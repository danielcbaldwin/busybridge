"""Canonical identity helpers for the ledger architecture.

Two ID schemes live here:

* **canonical_uid** — opaque per-user key for one logical event,
  derived from the source.  Stable across syncs, used for
  ``ledger_events.canonical_uid``.
* **deterministic Google ID** — the ``id`` we send to Google on
  ``events.insert`` so retries hit the unique-id constraint and
  return 409 Conflict instead of duplicating.  Derived from
  ``ledger_projections.id``.

Google's ID alphabet for client-supplied IDs is base32hex
(lowercase ``a-v`` plus ``0-9``), 5–1024 chars.  The encoder
below produces conforming IDs by base-32-hex-encoding the
projection ID and prefixing it with ``bb`` for traceability.
"""

from __future__ import annotations

import base64
import hashlib
import re
import struct
from datetime import datetime, timezone
from typing import Optional

# Stamp that marks a deterministic Google ID as ours.  Three
# letters keeps the per-id overhead small while making greps
# unambiguous.  See ``derive_google_event_id``.
_BB_PREFIX = "bb"

# Google's accepted alphabet for client-supplied event IDs.
_GOOGLE_ID_RE = re.compile(r"^[a-v0-9]{5,1024}$")


# ---------------------------------------------------------------------------
# canonical_uid
# ---------------------------------------------------------------------------
def canonical_uid_main_native(user_id: int, google_event_id: str) -> str:
    """Native main-calendar event (not a projection of any source)."""
    return f"main_native:{user_id}:{google_event_id}"


def canonical_uid_client(client_calendar_id: int, google_event_id: str) -> str:
    """Event sourced from an OAuth client calendar."""
    return f"client:{client_calendar_id}:{google_event_id}"


def canonical_uid_personal(personal_calendar_id: int, google_event_id: str) -> str:
    """Event sourced from a personal (read-only) calendar."""
    return f"personal:{personal_calendar_id}:{google_event_id}"


def canonical_uid_webcal_stable(subscription_id: int, ics_uid: str) -> str:
    """Event from a webcal feed whose UIDs survive across polls."""
    return f"webcal:{subscription_id}:{ics_uid}"


def canonical_uid_webcal_unstable(
    subscription_id: int,
    start_at: str,
    end_at: str,
    ordinal: int = 0,
) -> str:
    """Event from a webcal feed whose UIDs change every poll.

    The hash deliberately omits the summary, so an upstream rename
    of an event whose start/end stay the same does NOT generate a
    new canonical_uid (and therefore not a duplicate ledger row).
    This is the fix for the "Eventbrite renames event → BB
    creates duplicate" bug.

    ``ordinal`` disambiguates DISTINCT events that share the same
    start/end (which would otherwise collide on the same hash and
    silently overwrite one another).  ``ordinal=0`` yields the
    original, unsuffixed key so existing rows need no migration; the
    caller probes 1, 2, … only when a real collision is detected.
    """
    raw = f"{start_at}|{end_at}".encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    base = f"webcal:{subscription_id}:hash:{digest}"
    return base if ordinal == 0 else f"{base}:{ordinal}"


def canonical_uid_for_instance(
    parent_canonical_uid: str,
    original_start: str,
) -> str:
    """Canonical UID for a *modified* recurring instance.

    The parent series stores its own canonical_uid; modified
    instances get a per-occurrence UID derived from the parent +
    the original start time, so they survive parent re-keying
    (the `_R` reschedule case) by being re-parented rather than
    deleted.

    ``original_start`` is normalised to a canonical UTC instant before
    being embedded in the UID — Google returns the same occurrence's
    ``originalStartTime`` with different timezone offsets across reads
    (e.g. ``-04:00`` from a New-York-local user, ``+02:00`` after the
    user travels to Europe), and embedding the raw string would
    fingerprint the SAME occurrence into multiple distinct UIDs and
    DUPLICATE the ledger row.  Live regression: when the account moved
    between EDT and CEST, BB created a second row per modified instance
    and both rows fought over the main copy.  See
    test_instance_canonical_is_timezone_stable.  All-day occurrences
    keep their ``YYYY-MM-DD`` form (no offset to normalise).
    """
    return f"{parent_canonical_uid}:inst:{_canonical_instant_for_uid(original_start)}"


def _canonical_instant_for_uid(value: str) -> str:
    """UTC-normalised form of an ISO timestamp for use inside a UID.

    A timed value with any offset becomes ``YYYY-MM-DDTHH:MM:SSZ``; an
    all-day value (bare ``YYYY-MM-DD``) is returned unchanged; an
    unparsable value falls back to the raw string so malformed inputs
    still produce a deterministic (if non-normalised) UID.
    """
    if not value:
        return value
    if "T" not in value:
        # Bare date — all-day occurrence; already canonical.
        return value
    iso = (value[:-1] + "+00:00") if value.endswith("Z") else value
    try:
        dt = datetime.fromisoformat(iso)
    except (ValueError, TypeError):
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Deterministic Google event IDs
# ---------------------------------------------------------------------------
def derive_google_event_id(projection_id: int, generation: int = 0) -> str:
    """Encode a projection ID as a Google-acceptable event ID.

    Used as ``body['id']`` on ``events.insert``: the same input
    yields the same output, so a retried insert hits the per-
    calendar uniqueness constraint and returns 409 Conflict, which
    the outbox treats as success after a confirming GET.

    ``generation`` defaults to 0 and then encodes ``projection_id``
    directly — the original, stable scheme.  When a user deletes one
    of our events, Google keeps a *cancelled tombstone* at that id
    forever, so it can never be re-inserted.  The outbox then bumps
    the projection's generation and derives a fresh id: ``generation``
    > 0 hashes ``(projection_id, generation)`` into a distinct seed,
    still fully deterministic so a retry of that create is idempotent.

    Result: ``bb`` + base32hex(8-byte seed), lowercase, padding
    stripped — ~15 chars, well within Google's 5–1024 limit.
    """
    if projection_id <= 0:
        raise ValueError(f"projection_id must be positive, got {projection_id}")
    if projection_id >= (1 << 64):
        raise ValueError(f"projection_id exceeds 64 bits: {projection_id}")
    if generation < 0:
        raise ValueError(f"generation must be >= 0, got {generation}")
    if generation == 0:
        seed = struct.pack(">Q", projection_id)
    else:
        seed = hashlib.sha256(
            f"{projection_id}:{generation}".encode("ascii")
        ).digest()[:8]
    encoded = (
        base64.b32hexencode(seed)
        .decode("ascii")
        .lower()
        .rstrip("=")
    )
    out = f"{_BB_PREFIX}{encoded}"
    # Defence in depth: prove the id is well-formed.
    if not _GOOGLE_ID_RE.match(out):
        raise AssertionError(
            f"derive_google_event_id produced invalid id {out!r} "
            f"for projection_id={projection_id}"
        )
    return out


def derive_instance_google_event_id(
    parent_google_event_id: str,
    original_start_at: str,
    is_all_day: bool = False,
) -> str:
    """Build the Google ID for an exception/instance of a recurring event.

    Google's instance ID format is ``<parent>_<stamp>`` where the
    stamp is ``YYYYMMDD`` for all-day series or ``YYYYMMDDTHHMMSSZ``
    (UTC) for timed series.  Used by the diff step to pre-set
    ``ledger_projections.google_event_id`` on instance projections so
    the outbox can ``events.update`` / ``events.delete`` them
    directly.

    The stamp form is chosen from the SHAPE of ``original_start_at``
    itself — a bare ``YYYY-MM-DD`` means the ORIGINAL slot was
    all-day, anything with a time component means it was timed — and
    NOT from the caller's row-level ``is_all_day`` flag.  Google keys
    an instance id to the original occurrence slot, and the id does
    not change when the user converts that single occurrence between
    all-day and timed (both are ordinary Google UI/API actions).  The
    occurrence row's display flag follows the OVERRIDE, so deriving
    from it produced ``parent_20260310`` where Google actually held
    ``parent_20260310T130000Z`` (and vice versa): every UPDATE then
    404'd into a silent replan loop, and every DELETE was a
    404-treated-as-success that left a phantom busy block behind.

    ``is_all_day`` survives only as a fallback discriminator for an
    EMPTY ``original_start_at`` (no shape to inspect); it preserves
    the historical, degenerate-but-deterministic output for that
    input and is ignored otherwise.
    """
    if not original_start_at:
        # Empty input has no shape to inspect — keep the historical
        # flag-driven fallback outputs, byte-for-byte.
        if is_all_day:
            return f"{parent_google_event_id}_"
        return f"{parent_google_event_id}_Z"
    if "T" not in original_start_at:
        # Bare date: the original slot was all-day.
        stamp = original_start_at.replace("-", "")
        # Defensive: ensure 8 digits.
        stamp = stamp[:8]
        return f"{parent_google_event_id}_{stamp}"
    # Timed form: Google's instance ID stamp is always UTC.  The
    # source ``originalStartTime`` may carry any offset
    # (e.g. ``2024-03-10T14:30:00-05:00``), so parse it and convert
    # to UTC rather than stripping punctuation off whatever string
    # we were handed — a naive strip mangles a non-UTC offset into
    # an invalid id.
    s = original_start_at
    iso = (s[:-1] + "+00:00") if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError:
        # Unexpected format — best-effort strip, assume UTC.
        bare = s[:-1] if s.endswith("Z") else s
        if "." in bare:
            bare = bare.split(".", 1)[0]
        stamp = bare.replace("-", "").replace(":", "") + "Z"
        return f"{parent_google_event_id}_{stamp}"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return f"{parent_google_event_id}_{dt.strftime('%Y%m%dT%H%M%SZ')}"


def is_busybridge_event(
    event: dict,
    *,
    sync_tag: Optional[str] = None,
    managed_prefix: Optional[str] = None,
) -> bool:
    """One shared predicate: "was this Google event written by BusyBridge?"

    Used by both the backup snapshot (``app/sync/backup.py`` via the
    legacy client's ``is_our_event``) and the ICS "clean" export
    (``app/sync/ics_export.py``), which previously carried drifted
    copies and disagreed about legacy prefix-titled events.

    Four signals (any one is sufficient):

    * Deterministic ledger ID (:func:`is_managed_google_event_id`;
      the primary post-cutover signal).
    * ``extendedProperties.private.bb_proj_id`` (defence-in-depth
      stamp the ledger payload renderer applies; see
      ``app.ledger.payload.EP_PROJ_ID``).
    * Legacy sync-tag extended property — pass the configured tag
      name as ``sync_tag`` (``settings.calendar_sync_tag``); skipped
      when omitted.
    * Legacy summary prefix — pass the configured prefix as
      ``managed_prefix`` (``settings.managed_event_prefix``); skipped
      when omitted.  Recognises events from older versions that
      didn't stamp extended properties.

    The tag/prefix are parameters (not read from settings here) so
    this module stays configuration-free; callers supply them from
    their own settings object.
    """
    if is_managed_google_event_id(event.get("id")):
        return True

    ext_props = event.get("extendedProperties") or {}
    private_props = ext_props.get("private") or {}

    if private_props.get("bb_proj_id"):
        return True

    if sync_tag and private_props.get(sync_tag) == "true":
        return True

    prefix = (managed_prefix or "").strip().lower()
    if prefix:
        summary = (event.get("summary") or "").strip().lower()
        if summary.startswith(prefix):
            return True

    return False


def is_managed_google_event_id(event_id: Optional[str]) -> bool:
    """True if ``event_id`` looks like a deterministic ID we issued.

    Match shape: exactly 13 base32hex chars after the ``bb`` prefix
    (a 64-bit projection_id encoded with padding stripped).  Anything
    else — including user-chosen IDs that happen to start with
    ``bb`` — is NOT ours and must not be skipped by ingest.

    Used by the discovery / orphan scan to recognise our writes
    without hitting the database.  This is "is it ours?" by
    construction rather than by extended-property lookup.
    """
    if not event_id:
        return False
    if not event_id.startswith(_BB_PREFIX):
        return False
    rest = event_id[len(_BB_PREFIX) :]
    return bool(re.fullmatch(r"[a-v0-9]{13}", rest))
