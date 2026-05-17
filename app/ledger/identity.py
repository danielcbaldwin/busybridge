"""Canonical identity helpers for the ledger architecture.

Two ID schemes live here:

* **canonical_uid** — opaque per-user key for one logical event,
  derived from the source.  Stable across syncs, used for
  ``ledger_events.canonical_uid``.
* **deterministic Google ID** — the ``id`` we send to Google on
  ``events.insert`` so retries hit the unique-id constraint and
  return 409 Conflict instead of duplicating.  Derived from
  ``ledger_projections.id``.

Both schemes are described in REWRITE_PLAN.md §3 and §7.

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
    This is the fix for today's "Eventbrite renames event → BB
    creates duplicate" bug — REWRITE_PLAN.md §5.4.

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
    """
    return f"{parent_canonical_uid}:inst:{original_start}"


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
    is_all_day: bool,
) -> str:
    """Build the Google ID for an exception/instance of a recurring event.

    Google's instance ID format is ``<parent>_<stamp>`` where the
    stamp is ``YYYYMMDD`` for all-day events or
    ``YYYYMMDDTHHMMSSZ`` for timed events.  Used by the diff step
    to pre-set ``ledger_projections.google_event_id`` on instance
    projections so the outbox can ``events.update`` /
    ``events.delete`` them directly.
    """
    if is_all_day:
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
