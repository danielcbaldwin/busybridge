"""Client OAuth ingest.

For each event Google delivers, decide:

1. Is it one of our writes (a busy block / projection)?  If so,
   skip — we don't want to mirror our own output.
2. Is it a rescheduled-parent ``_R`` case?  Look up the existing
   ledger row by stripped base ID and re-key its
   ``source_event_id`` to the new one.
3. Otherwise: upsert the ledger row, bumping ``version`` only if a
   material field changed.

The connection runs in autocommit (``isolation_level=None``), so each
event is recorded as it is processed — there is no single enclosing
transaction.  Per-event failures are isolated: one poison event (e.g.
a ``canonical_uid`` UNIQUE collision) is logged and skipped rather than
aborting the pass.  This matters because aborting before the sync-token
write at the end would freeze the token and silently stop every later
event from reaching the main calendar.  The token write is the last
durable write of the pass, so a crash mid-pass simply re-ingests from
the old token next time (idempotent).
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Iterable, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiosqlite

from app.ledger.async_google import as_async_google
from app.ledger.google_client import GoogleClient
from app.ledger.identity import (
    canonical_uid_client,
    canonical_uid_for_instance,
    is_managed_google_event_id,
)

logger = logging.getLogger(__name__)
UTC = timezone.utc


async def ingest_client_calendar(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    client_calendar_id: int,
    google_calendar_id: str,
    user_email: str,
    owned_emails: Optional[Iterable[str]] = None,
) -> dict:
    """Run one ingest pass for one client calendar.

    Returns counters: ``{seen, created, updated, rekeyed, skipped, cancelled}``.
    Affected ``ledger_event.id``s are written into the
    ``reconcile_requests.sources_json`` so the reconciler picks
    them up for planning.
    """
    google = as_async_google(google)
    state = await _get_or_create_sync_state(db, client_calendar_id)
    sync_token: Optional[str] = state["sync_token"]
    counters = {
        "seen": 0, "created": 0, "updated": 0,
        "rekeyed": 0, "skipped": 0, "cancelled": 0, "failed": 0,
    }
    affected_ledger_ids: list[int] = []

    page_token: Optional[str] = None
    new_sync_token: Optional[str] = None
    full_sync = sync_token is None
    # Recurring parent event IDs seen this pass; on a full sync we
    # must scan their instances explicitly (see below).
    recurring_parent_ids: set[str] = set()

    while True:
        try:
            page = await google.list_events(
                google_calendar_id,
                # Google's sync guide: every page of an incremental
                # sync carries the SAME syncToken (plus pageToken for
                # pages 2+).  Dropping it on later pages can break a
                # sync that spans >250 changes.
                sync_token=sync_token,
                page_token=page_token,
                show_deleted=True,
                max_results=250,
            )
        except Exception as e:
            if getattr(e, "status", None) == 410:
                # Sync token expired — restart with full sync.
                logger.info(
                    "sync token expired for client_calendar_id=%s, "
                    "falling back to full sync",
                    client_calendar_id,
                )
                sync_token = None
                page_token = None
                full_sync = True
                recurring_parent_ids.clear()
                continue
            raise

        for event in page.get("items", []):
            counters["seen"] += 1
            if _is_recurring_parent(event):
                recurring_parent_ids.add(event["id"])
            try:
                outcome, ledger_id = await _ingest_one_event(
                    db,
                    user_id=user_id,
                    client_calendar_id=client_calendar_id,
                    user_email=user_email,
                    owned_emails=owned_emails,
                    event=event,
                )
            except Exception:
                # Isolate per-event failures.  A single poison event
                # (e.g. a canonical_uid UNIQUE collision) must NOT
                # abort the pass — that would strand the sync token and
                # silently stop every later event from reaching the
                # main calendar.  Log loudly, skip this one, keep going;
                # the token still advances so the calendar keeps syncing.
                logger.exception(
                    "client ingest: skipping event %s on client_calendar_id=%s "
                    "after error",
                    event.get("id"), client_calendar_id,
                )
                counters["failed"] = counters.get("failed", 0) + 1
                continue
            counters[outcome] = counters.get(outcome, 0) + 1
            if ledger_id is not None:
                affected_ledger_ids.append(ledger_id)
                await _stamp_ical_uid(db, ledger_id, event)

        if "nextPageToken" in page:
            page_token = page["nextPageToken"]
            continue
        new_sync_token = page.get("nextSyncToken")
        break

    # On a full sync, recover cancelled recurring instances that
    # ``events.list`` omits (see scan_full_sync_recurring_cancellations).
    if full_sync and recurring_parent_ids:
        async def _ingest(inst: dict) -> tuple[str, Optional[int]]:
            return await _ingest_one_event(
                db,
                user_id=user_id,
                client_calendar_id=client_calendar_id,
                user_email=user_email,
                owned_emails=owned_emails,
                event=inst,
            )
        scan_failures = await scan_full_sync_recurring_cancellations(
            db, google,
            google_calendar_id=google_calendar_id,
            recurring_parent_ids=recurring_parent_ids,
            counters=counters,
            affected_ledger_ids=affected_ledger_ids,
            ingest_one=_ingest,
        )
        if scan_failures:
            # Hold the sync token back so the next reconcile re-runs
            # a full sync and retries the scan — otherwise the
            # un-scanned cancellations are stranded.
            new_sync_token = None

    # Record affected ledger ids BEFORE advancing the sync token.
    # The DB runs in autocommit, so the token write must be the last
    # durable write of the pass: a crash after recording affected ids
    # but before the token advances simply re-ingests from the old
    # token next pass (idempotent).  The reverse order would advance
    # the token first and strand any not-yet-recorded affected ids.
    when = datetime.now(UTC).isoformat()
    if affected_ledger_ids:
        await _record_affected(db, user_id=user_id, ledger_ids=affected_ledger_ids)
    await db.execute(
        """UPDATE calendar_sync_state
              SET sync_token = ?,
                  last_full_sync = COALESCE(?, last_full_sync),
                  last_incremental_sync = ?,
                  consecutive_failures = 0,
                  last_error = NULL
            WHERE client_calendar_id = ?""",
        (
            new_sync_token,
            when if full_sync else None,
            when,
            client_calendar_id,
        ),
    )
    await db.commit()
    return counters


# ---------------------------------------------------------------------------
# Full-sync recurring-cancellation recovery (shared by every source)
# ---------------------------------------------------------------------------
def _is_recurring_parent(event: dict) -> bool:
    """True for a recurring *series master* we did not write: it
    carries a recurrence rule, is not itself an instance, is not
    cancelled, and is not one of our own managed copies."""
    return bool(
        event.get("recurrence")
        and not event.get("recurringEventId")
        and event.get("status") != "cancelled"
        and not is_managed_google_event_id(event.get("id"))
    )


async def scan_full_sync_recurring_cancellations(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    google_calendar_id: str,
    recurring_parent_ids: set[str],
    counters: dict,
    affected_ledger_ids: list[int],
    ingest_one: Callable[[dict], Awaitable[tuple[str, Optional[int]]]],
) -> int:
    """Recover cancelled recurring instances that a full sync omits.

    Full ``events.list`` does NOT return cancelled instance
    exceptions of recurring series (documented Google quirk — see
    tests/fakes/QUIRKS.md), so an instance cancelled during a
    sync-token gap is invisible to the full-sync page loop.
    ``events.instances(showDeleted=True)`` IS reliable: this scans
    every recurring parent seen this pass and routes the cancelled
    instances through ``ingest_one``.  Closes the
    recurring-cancellation-amnesia bug for
    every source type — client, personal, and native main.

    Returns the number of parents whose instance scan FAILED.  A
    non-zero return means the caller MUST NOT advance the sync
    token: the un-scanned cancellations would otherwise be stranded
    (invisible to the next incremental sync) until the next token
    expiry.  Keeping the token NULL forces a fresh full sync — which
    is idempotent — that re-attempts the scan.
    """
    failed = 0
    for parent_id in recurring_parent_ids:
        try:
            inst_resp = await google.list_instances(
                google_calendar_id, parent_id,
                show_deleted=True, max_results=2500,
            )
        except Exception as e:
            logger.warning(
                "instance scan failed for %s/%s: %s — sync token will "
                "be held back so the next full sync retries",
                google_calendar_id, parent_id, e,
            )
            failed += 1
            continue
        for inst in inst_resp.get("items", []):
            if inst.get("status") != "cancelled":
                continue
            if not inst.get("recurringEventId"):
                continue
            counters["seen"] += 1
            try:
                outcome, ledger_id = await ingest_one(inst)
            except Exception:
                # Isolate per-instance failures, same rationale as the
                # main page loop: one bad cancelled instance must not
                # abort the whole scan.
                logger.exception(
                    "instance scan: skipping cancelled instance %s of %s "
                    "after error",
                    inst.get("id"), parent_id,
                )
                counters["failed"] = counters.get("failed", 0) + 1
                continue
            counters[outcome] = counters.get(outcome, 0) + 1
            if ledger_id is not None:
                affected_ledger_ids.append(ledger_id)
                await _stamp_ical_uid(db, ledger_id, inst)
    return failed


async def _stamp_ical_uid(
    db: aiosqlite.Connection, ledger_event_id: int, event: dict,
) -> None:
    """Record Google's cross-calendar event identity (events.iCalUID) on
    an ingested ledger row when the event carries one.

    The SAME meeting shares one iCalUID across every calendar the user is
    on, so the planner uses this to recognise that a 'main_native'
    reflection of a meeting is the same event already ingested from a
    client/personal source — and suppress the duplicate busy block.  It is
    identity only and deliberately kept OUT of the content hash, so it can
    never trigger a spurious version bump.  Idempotent: the conditional
    UPDATE is a no-op once the value is already stored.
    """
    ical = event.get("iCalUID")
    if not ical:
        return
    await db.execute(
        "UPDATE ledger_events SET ical_uid = ? "
        "WHERE id = ? AND COALESCE(ical_uid, '') != ?",
        (ical, int(ledger_event_id), ical),
    )


# ---------------------------------------------------------------------------
# Per-event handling
# ---------------------------------------------------------------------------
async def _ingest_one_event(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    client_calendar_id: int,
    user_email: str,
    event: dict,
    owned_emails: Optional[Iterable[str]] = None,
    skip_if_older: bool = False,
) -> tuple[str, Optional[int]]:
    """Process one Google event.  Returns (outcome, ledger_event_id).

    ``skip_if_older`` (used by the content-audit pass): never apply a
    read whose ``updated`` is strictly older than what's already stored
    for that event — a stale replica we must not let revert fresher
    data.  Equal timestamps still apply (the create-race carries an
    identical ``updated`` and must be corrected)."""
    event_id = event["id"]
    status = event.get("status", "confirmed")

    # 1. Loop-prevention: skip events we wrote (deterministic IDs
    # plus an exact projection lookup as defence-in-depth).  Before
    # skipping, check whether one of our busy blocks has drifted —
    # the user moved or edited it on the client calendar — and if so
    # re-assert our canonical payload (revert-on-drift, now uniform
    # on client targets too).
    proj_match = await (await db.execute(
        """SELECT id, google_etag FROM ledger_projections
            WHERE google_event_id = ?
            LIMIT 1""",
        (event_id,),
    )).fetchone()
    if proj_match is not None or is_managed_google_event_id(event_id):
        if proj_match is not None:
            await _maybe_revert_client_drift(db, proj_match, event)
        return "skipped", None

    # 2. Recurring-event INSTANCE (modified or cancelled).  Route
    #    to the instance handler — these get their own ledger row
    #    with parent_canonical_uid set so cancellations are sticky.
    if event.get("recurringEventId"):
        parent_canonical = canonical_uid_client(
            client_calendar_id, event["recurringEventId"],
        )
        return await _ingest_instance(
            db,
            user_id=user_id,
            user_email=user_email,
            owned_emails=owned_emails,
            event=event,
            parent_canonical=parent_canonical,
            source_type="client",
            source_calendar_id=client_calendar_id,
            skip_if_older=skip_if_older,
        )

    canonical = canonical_uid_client(client_calendar_id, event_id)

    # A ``<base>_R<date>`` event is Google's "this and following" split:
    # an ADDITIVE new series segment that coexists with the (now
    # UNTIL-truncated) base series and any earlier ``_R`` segments, each
    # covering a distinct date range.  We deliberately do NOT re-key the
    # base onto it — each segment is ingested as its own recurring series
    # (the normal upsert below), and a modified instance stays parented to
    # whichever segment its ``recurringEventId`` names (handled above).
    # Re-keying collapsed coexisting segments and bulk-re-parented
    # pre-boundary instances onto a later segment whose expansion lacks
    # their date, so ``events.update`` on the derived instance id 404'd
    # forever (the MLC-meeting residual).  See
    # test_moved_instance_survives_this_and_following.

    existing = await (await db.execute(
        """SELECT * FROM ledger_events
            WHERE user_id = ? AND canonical_uid = ?""",
        (user_id, canonical),
    )).fetchone()

    # 3. Cancellations: flip to status=cancelled (don't delete the row).
    if status == "cancelled":
        if existing is None:
            return "skipped", None
        if existing["status"] == "cancelled":
            return "skipped", int(existing["id"])
        await _mark_cancelled(db, ledger_event_id=int(existing["id"]))
        return "cancelled", int(existing["id"])

    # 4. Upsert.
    if existing is None:
        new_id = await _insert_ledger_row(
            db,
            user_id=user_id,
            canonical_uid=canonical,
            client_calendar_id=client_calendar_id,
            event=event,
            user_email=user_email,
            owned_emails=owned_emails,
        )
        return "created", new_id
    changed = await _apply_event_to_ledger(
        db,
        ledger_event_id=int(existing["id"]),
        event=event,
        user_email=user_email,
        owned_emails=owned_emails,
        skip_if_older=skip_if_older,
    )
    return ("updated" if changed else "skipped"), int(existing["id"])


async def _maybe_revert_client_drift(
    db: aiosqlite.Connection, proj_match, event: dict,
) -> None:
    """Revert-on-drift for one of our own writes on a client calendar.

    A busy block we wrote carries the etag Google returned, stored on
    the projection.  If the etag delivered by a later sync differs,
    the user moved or edited the busy block — clear the projection's
    ``applied_ledger_version`` so the diff re-asserts our canonical
    payload, and refresh ``google_etag`` so the corrective
    ``events.update`` is not rejected by ``If-Match``
    (uniform with the main-copy revert).

    A user-deleted busy block (status=cancelled) resets the projection
    so the diff re-CREATEs it: a missing busy block is a real
    correctness failure for a calendar-as-truth system, and leaving
    the projection 'present' would make the next source change emit an
    ``events.update`` that 404s and poison-pills.
    """
    if event.get("status") == "cancelled":
        await db.execute(
            """UPDATE ledger_projections
                  SET current_state = 'absent',
                      google_event_id = NULL,
                      google_etag = NULL,
                      applied_ledger_version = NULL,
                      applied_payload_hash = NULL
                WHERE id = ?""",
            (int(proj_match["id"]),),
        )
        return
    ev_etag = event.get("etag")
    stored = proj_match["google_etag"]
    if ev_etag and stored and ev_etag != stored:
        await db.execute(
            """UPDATE ledger_projections
                  SET applied_ledger_version = NULL,
                      google_etag = ?
                WHERE id = ?""",
            (ev_etag, int(proj_match["id"])),
        )


# ---------------------------------------------------------------------------
# Instance handling
# ---------------------------------------------------------------------------
# Trailing RFC 3339 UTC-offset, e.g. ``-05:00`` / ``+05:30`` — the
# date portion's hyphens are never followed by a colon, so this only
# matches a genuine time-zone offset.
_OFFSET_RE = re.compile(r"[+-]\d{2}:\d{2}$")


def _resolve_original_start(time_dict: dict) -> str:
    """Normalize a recurring occurrence's start so the same occurrence
    always yields the same string.

    Google usually delivers ``dateTime`` with an explicit offset; some
    sources send a naive ``dateTime`` plus a separate IANA ``timeZone``.
    In the naive+zone case the wall time is resolved in that zone and
    converted to UTC, so the downstream instance-id derivation — which
    assumes a naive datetime is UTC — stays correct across DST.  A
    ``dateTime`` that already carries an offset, or that has no usable
    ``timeZone``, is returned unchanged.
    """
    dt_str = time_dict["dateTime"]
    if dt_str.endswith("Z") or _OFFSET_RE.search(dt_str):
        return dt_str
    tz_name = time_dict.get("timeZone")
    if not tz_name:
        return dt_str
    try:
        tz = ZoneInfo(tz_name)
        naive = datetime.fromisoformat(dt_str)
    except (ZoneInfoNotFoundError, ValueError):
        return dt_str
    return naive.replace(tzinfo=tz).astimezone(UTC).isoformat()


def _instance_original_start(event: dict) -> tuple[str, bool]:
    """Return ``(original_start, is_all_day)`` for a recurring instance.

    Prefers ``originalStartTime`` (the un-modified occurrence slot);
    falls back to the instance's own ``start`` when Google omitted it.
    A timed value is normalized via :func:`_resolve_original_start` so
    the same occurrence always yields the same string.
    """
    ost = event.get("originalStartTime", {}) or {}
    if "dateTime" in ost:
        return _resolve_original_start(ost), False
    if "date" in ost:
        return ost["date"], True
    start = event.get("start") or {}
    if "dateTime" in start:
        return _resolve_original_start(start), False
    if "date" in start:
        return start["date"], True
    return "", False


#: Sentinel for ``_ingest_instance``'s ``source_event_id`` parameter:
#: "store the event's own id".  A literal ``None`` is a distinct,
#: meaningful value (the instance has no source event id yet — used
#: by the main-side managed-instance edit path), so it cannot double
#: as the default.
_USE_EVENT_ID: Any = object()


async def _ingest_instance(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    user_email: str,
    owned_emails: Optional[Iterable[str]] = None,
    event: dict,
    parent_canonical: str,
    source_type: str,
    source_calendar_id: Optional[int],
    source_event_id: Any = _USE_EVENT_ID,
    fields: Optional[dict] = None,
    skip_if_older: bool = False,
) -> tuple[str, Optional[int]]:
    """Upsert a modified or cancelled instance of a recurring series.

    Source-neutral: ``source_type`` is ``'client'``, ``'personal'``,
    or ``'main_native'`` and ``source_calendar_id`` is the owning
    calendar id (``None`` for native main).  Instances are kept as
    separate ledger rows with ``parent_canonical_uid`` set, so a
    cancellation persists even if the recurring parent series later
    gets re-ingested via full sync (which omits cancelled
    exceptions — the documented "recurring-cancellation amnesia"
    bug).

    ``source_event_id`` defaults to the event's own id.  The main-side
    managed-instance edit path passes ``None``: a move made on one of
    our managed copies has no source event id yet (the source's own
    occurrence is still an un-exceptioned part of its series), and
    storing the main-copy instance id there would mis-identify it.

    ``fields`` overrides the extracted content fields.  The main-side
    edit path passes a dict whose edit-rights / organizer / attendee
    keys are taken from the SOURCE series rather than from the opaque
    managed copy the user dragged.  Passing it (rather than patching
    the row afterwards) keeps the content hash self-consistent so a
    re-ingest of the same edit still no-ops.
    """
    if source_event_id is _USE_EVENT_ID:
        source_event_id = event.get("id")
    original_start, instance_is_all_day = _instance_original_start(event)

    instance_canonical = canonical_uid_for_instance(
        parent_canonical, original_start,
    )
    status = event.get("status", "confirmed")
    when = datetime.now(UTC).isoformat()

    existing = await (await db.execute(
        """SELECT * FROM ledger_events
            WHERE user_id = ? AND canonical_uid = ?""",
        (user_id, instance_canonical),
    )).fetchone()

    # Cancelled instance — sticky ledger row that survives parent
    # full-sync (since incremental sync surfaces the cancellation
    # once, and our row persists across future passes).
    if status == "cancelled":
        if existing is not None and existing["status"] == "cancelled":
            return "skipped", int(existing["id"])
        if existing is None:
            cursor = await db.execute(
                """INSERT INTO ledger_events
                      (user_id, canonical_uid, parent_canonical_uid,
                       source_type, source_calendar_id, source_event_id,
                       recurrence_instance_original_start, is_all_day,
                       status, version, is_recurring,
                       created_at, updated_at, last_seen_at, cancelled_at)
                   VALUES (?, ?, ?,
                           ?, ?, ?, ?, ?,
                           'cancelled', 1, 0,
                           ?, ?, ?, ?)""",
                (
                    user_id, instance_canonical, parent_canonical,
                    source_type, source_calendar_id, source_event_id,
                    original_start, instance_is_all_day,
                    when, when, when, when,
                ),
            )
            return "cancelled", int(cursor.lastrowid)
        await db.execute(
            """UPDATE ledger_events
                  SET status = 'cancelled',
                      is_all_day = ?,
                      version = version + 1,
                      cancelled_at = ?, updated_at = ?, last_seen_at = ?
                WHERE id = ?""",
            (instance_is_all_day, when, when, when, int(existing["id"])),
        )
        return "cancelled", int(existing["id"])

    # Modified instance — single-instance override on the series.
    if skip_if_older and existing is not None and _read_is_stale(event, existing):
        await db.execute(
            "UPDATE ledger_events SET last_seen_at = ? WHERE id = ?",
            (when, int(existing["id"])),
        )
        return "skipped", int(existing["id"])
    if fields is None:
        fields = _extract_event_fields(
            event, user_email=user_email, owned_emails=owned_emails,
        )
    if existing is None:
        cursor = await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid, parent_canonical_uid,
                   source_type, source_calendar_id, source_event_id,
                   recurrence_instance_original_start,
                   source_etag, source_updated_at,
                   summary, description, location,
                   start_at, end_at, start_timezone, end_timezone,
                   is_all_day,
                   show_as, visibility, color_id,
                   organizer_email, user_can_edit, user_rsvp_status,
                   attendees_json, conference_data_json, source_html_link,
                   status, is_recurring, version,
                   created_at, updated_at, last_seen_at)
               VALUES (?, ?, ?,
                       ?, ?, ?, ?,
                       ?, ?,
                       ?, ?, ?,
                       ?, ?, ?, ?,
                       ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       'active', 0, 1,
                       ?, ?, ?)""",
            (
                user_id, instance_canonical, parent_canonical,
                source_type, source_calendar_id, source_event_id,
                original_start,
                event.get("etag"), event.get("updated"),
                fields["summary"], fields["description"], fields["location"],
                fields["start_at"], fields["end_at"],
                fields["start_timezone"], fields["end_timezone"],
                fields["is_all_day"],
                fields["show_as"], fields["visibility"], fields["color_id"],
                fields["organizer_email"], fields["user_can_edit"],
                fields["user_rsvp_status"],
                fields["attendees_json"], fields["conference_data_json"],
                fields["source_html_link"],
                when, when, when,
            ),
        )
        return "created", int(cursor.lastrowid)

    new_hash = _content_hash(fields)
    old_hash = _content_hash_from_row(existing)
    conf_json, conf_pending, conf_changed = _resolve_conference(existing, fields)
    if new_hash == old_hash and not conf_changed:
        # No content change.  Record the conference debounce candidate so
        # a genuine, settled room swap can confirm on the next read.
        await db.execute(
            "UPDATE ledger_events SET last_seen_at = ?, "
            "pending_conference_id = ? WHERE id = ?",
            (when, conf_pending, int(existing["id"])),
        )
        return "skipped", int(existing["id"])
    await db.execute(
        """UPDATE ledger_events
              SET source_etag = ?, source_updated_at = ?,
                  summary = ?, description = ?, location = ?,
                  start_at = ?, end_at = ?,
                  start_timezone = ?, end_timezone = ?, is_all_day = ?,
                  show_as = ?, visibility = ?, color_id = ?,
                  organizer_email = ?, user_can_edit = ?,
                  user_rsvp_status = ?,
                  attendees_json = ?,
                  conference_data_json = ?, source_html_link = ?,
                  pending_conference_id = ?,
                  status = 'active',
                  version = version + 1,
                  updated_at = ?, last_seen_at = ?
            WHERE id = ?""",
        (
            event.get("etag"), event.get("updated"),
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"],
            fields["start_timezone"], fields["end_timezone"],
            fields["is_all_day"],
            fields["show_as"], fields["visibility"], fields["color_id"],
            fields["organizer_email"], fields["user_can_edit"],
            fields["user_rsvp_status"],
            fields["attendees_json"],
            conf_json, fields["source_html_link"],
            conf_pending,
            when, when, int(existing["id"]),
        ),
    )
    return "updated", int(existing["id"])


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------
async def _insert_ledger_row(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    canonical_uid: str,
    client_calendar_id: int,
    event: dict,
    user_email: str,
    owned_emails: Optional[Iterable[str]] = None,
) -> int:
    fields = _extract_event_fields(
        event, user_email=user_email, owned_emails=owned_emails,
    )
    when = datetime.now(UTC).isoformat()
    cursor = await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid,
               source_type, source_calendar_id, source_event_id,
               source_etag, source_updated_at,
               summary, description, location,
               start_at, end_at, start_timezone, end_timezone, is_all_day,
               show_as, visibility, color_id,
               organizer_email, user_can_edit, user_rsvp_status,
               attendees_json, recurrence_rule_json,
               conference_data_json, source_html_link,
               status, is_recurring, version,
               created_at, updated_at, last_seen_at)
           VALUES (?, ?,
                   'client', ?, ?,
                   ?, ?,
                   ?, ?, ?,
                   ?, ?, ?, ?, ?,
                   ?, ?, ?,
                   ?, ?, ?,
                   ?, ?,
                   ?, ?,
                   'active', ?, 1,
                   ?, ?, ?)""",
        (
            user_id, canonical_uid,
            client_calendar_id, event["id"],
            event.get("etag"), event.get("updated"),
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"],
            fields["start_timezone"], fields["end_timezone"],
            fields["is_all_day"],
            fields["show_as"], fields["visibility"], fields["color_id"],
            fields["organizer_email"], fields["user_can_edit"],
            fields["user_rsvp_status"],
            fields["attendees_json"], fields["recurrence_rule_json"],
            fields["conference_data_json"], fields["source_html_link"],
            fields["is_recurring"],
            when, when, when,
        ),
    )
    return int(cursor.lastrowid)


def _parse_iso_utc(s: Optional[str]) -> Optional[datetime]:
    """Parse an RFC3339 timestamp to a tz-aware UTC datetime, or None."""
    if not s:
        return None
    try:
        t = (s[:-1] + "+00:00") if s.endswith("Z") else s
        dt = datetime.fromisoformat(t)
    except (ValueError, TypeError):
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def _read_is_stale(event: dict, existing) -> bool:
    """True if ``event``'s source ``updated`` is strictly OLDER than the
    stored ``source_updated_at`` — a stale replica read the audit must
    not apply (it would revert fresher data).  Equal or unparsable
    timestamps are NOT stale: the create-race carries an identical
    ``updated`` and must still be corrected."""
    if existing is None:
        return False
    a = _parse_iso_utc(event.get("updated"))
    b = _parse_iso_utc(existing["source_updated_at"])
    if a is None or b is None:
        return False
    return a < b


async def _apply_event_to_ledger(
    db: aiosqlite.Connection,
    *,
    ledger_event_id: int,
    event: dict,
    user_email: str,
    owned_emails: Optional[Iterable[str]] = None,
    skip_if_older: bool = False,
) -> bool:
    """Update an existing ledger row.  Returns True if any material
    field changed (and version was bumped).  A row resurrecting
    from ``cancelled`` to ``active`` always counts as changed.

    ``skip_if_older`` (audit pass): if this read's ``updated`` is
    strictly older than the stored ``source_updated_at``, treat it as a
    stale replica and do not apply (would revert fresher data)."""
    fields = _extract_event_fields(
        event, user_email=user_email, owned_emails=owned_emails,
    )
    existing = await (await db.execute(
        "SELECT * FROM ledger_events WHERE id = ?",
        (ledger_event_id,),
    )).fetchone()
    when = datetime.now(UTC).isoformat()

    if skip_if_older and _read_is_stale(event, existing):
        await db.execute(
            "UPDATE ledger_events SET last_seen_at = ? WHERE id = ?",
            (when, ledger_event_id),
        )
        return False

    new_hash = _content_hash(fields)
    old_hash = _content_hash_from_row(existing)
    resurrecting = existing["status"] == "cancelled"
    conf_json, conf_pending, conf_changed = _resolve_conference(existing, fields)
    changed = (new_hash != old_hash) or resurrecting or conf_changed

    if not changed:
        # No material content change.  Still record the conference
        # debounce candidate so a genuine, settled room swap can confirm
        # on the next read (and clear a candidate that went away).
        await db.execute(
            "UPDATE ledger_events SET last_seen_at = ?, "
            "pending_conference_id = ? WHERE id = ?",
            (when, conf_pending, ledger_event_id),
        )
        return False

    await db.execute(
        """UPDATE ledger_events
              SET source_etag = ?,
                  source_updated_at = ?,
                  summary = ?, description = ?, location = ?,
                  start_at = ?, end_at = ?,
                  start_timezone = ?, end_timezone = ?, is_all_day = ?,
                  show_as = ?, visibility = ?, color_id = ?,
                  organizer_email = ?, user_can_edit = ?,
                  user_rsvp_status = ?,
                  attendees_json = ?, recurrence_rule_json = ?,
                  conference_data_json = ?, source_html_link = ?,
                  pending_conference_id = ?,
                  is_recurring = ?,
                  status = 'active',
                  version = version + 1,
                  updated_at = ?,
                  last_seen_at = ?
            WHERE id = ?""",
        (
            event.get("etag"), event.get("updated"),
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"],
            fields["start_timezone"], fields["end_timezone"],
            fields["is_all_day"],
            fields["show_as"], fields["visibility"], fields["color_id"],
            fields["organizer_email"], fields["user_can_edit"],
            fields["user_rsvp_status"],
            fields["attendees_json"], fields["recurrence_rule_json"],
            conf_json, fields["source_html_link"],
            conf_pending,
            fields["is_recurring"],
            when, when, ledger_event_id,
        ),
    )
    return True


async def _mark_cancelled(
    db: aiosqlite.Connection, *, ledger_event_id: int,
) -> None:
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE ledger_events
              SET status = 'cancelled',
                  version = version + 1,
                  cancelled_at = ?,
                  updated_at = ?,
                  last_seen_at = ?
            WHERE id = ?""",
        (when, when, when, ledger_event_id),
    )


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------
def _extract_event_fields(
    event: dict,
    *,
    user_email: str,
    owned_emails: Optional[Iterable[str]] = None,
) -> dict:
    """Project a Google event dict into the columns the ledger stores.

    ``user_email`` is the user's primary (home) email.  ``owned_emails``
    optionally lists every email the user owns — home, client OAuth
    accounts, personal accounts.  Identity checks (organizer match,
    self-attendee match) treat ANY of those emails as 'you', so an
    event you organise under a non-home identity (e.g. via your
    mlcommons account) correctly reads as editable rather than landing
    with a lock prefix on the main copy.  Defaults to ``{user_email}``
    when the caller passes only the primary email."""
    owned = _normalise_owned_emails(user_email, owned_emails)
    start = event.get("start", {}) or {}
    end = event.get("end", {}) or {}
    is_all_day = "date" in start
    if is_all_day:
        start_at = start.get("date")
        end_at = end.get("date")
    else:
        start_at = start.get("dateTime")
        end_at = end.get("dateTime")

    show_as = "free" if event.get("transparency") == "transparent" else "busy"
    # User can edit if: they are the organizer (under ANY owned email),
    # OR the event explicitly marks guestsCanModify=True.  Solo events
    # (no attendees, no explicit organizer set) are also editable.
    has_attendees = bool(event.get("attendees"))
    user_can_edit = (
        _user_is_organizer(event, owned_emails=owned)
        or bool(event.get("guestsCanModify"))
        or not has_attendees
    )

    user_rsvp = None
    for att in (event.get("attendees") or []):
        if att.get("self") or (att.get("email", "").lower() in owned):
            user_rsvp = att.get("responseStatus")
            break

    organizer_email = (event.get("organizer") or {}).get("email")

    return {
        "summary": event.get("summary"),
        "description": event.get("description"),
        "location": event.get("location"),
        "start_at": start_at,
        "end_at": end_at,
        # IANA timezone the source expands its RRULE in.  Preserving it
        # keeps a "weekly 9am America/New_York" event correct across
        # DST instead of drifting onto a fixed UTC grid.
        "start_timezone": start.get("timeZone"),
        "end_timezone": end.get("timeZone"),
        "is_all_day": is_all_day,
        "show_as": show_as,
        "visibility": event.get("visibility"),
        "color_id": event.get("colorId"),
        "organizer_email": organizer_email,
        "user_can_edit": user_can_edit,
        "user_rsvp_status": user_rsvp,
        "attendees_json": json.dumps(event.get("attendees", [])),
        "recurrence_rule_json": (
            json.dumps(event["recurrence"]) if event.get("recurrence") else None
        ),
        "is_recurring": bool(event.get("recurrence")),
        # Video-call data (Meet/Zoom) and a link back to the real event,
        # carried onto the main copy.  In the content hash so a source
        # change (or first capture after the column was added) re-renders.
        "conference_data_json": (
            json.dumps(event["conferenceData"])
            if event.get("conferenceData") else None
        ),
        "source_html_link": event.get("htmlLink"),
    }


def _normalise_owned_emails(
    user_email: str, owned: Optional[Iterable[str]] = None,
) -> frozenset[str]:
    """Build a case-folded set of the user's owned emails.  Always
    includes the primary ``user_email``."""
    out = {user_email.lower()} if user_email else set()
    for e in (owned or ()):
        if e:
            out.add(e.lower())
    return frozenset(out)


def _user_is_organizer(
    event: dict,
    user_email: Optional[str] = None,
    *,
    owned_emails: Optional[Iterable[str]] = None,
) -> bool:
    """Is the event's organizer one of the user's owned identities?

    Either pass the primary ``user_email`` (back-compat single-identity
    check) or ``owned_emails`` (the full owned set — recognises the user
    as organizer under any of their accounts)."""
    organizer = (event.get("organizer") or {}).get("email", "").lower()
    if not organizer:
        return False
    owned = _normalise_owned_emails(user_email or "", owned_emails)
    return organizer in owned


# Fields hashed via a normalised form rather than their raw value.
# The raw conferenceData JSON varies between Google reads (entry-point
# order / volatile sub-fields), so hashing it verbatim makes every Meet
# event look "changed" on every sync — an endless version-bump →
# re-plan → re-send churn.  Instead we hash a stable SIGNATURE (the
# conference id + the actual entry-point URIs, sorted): a genuine
# Meet-link change IS detected and re-synced, the serialisation noise
# is not.  htmlLink is stable and display-only, so it is dropped from
# change detection entirely.  Both are still stored and rendered in full.
#
# start_at / end_at are excluded for the same reason: Google returns the
# same instant in different timezone offsets across reads / accounts (a
# user travelling will see "+02:00" one pass and "-04:00" the next), so
# the raw string churns even though the moment in time is unchanged.
# We hash a canonical UTC instant instead.  Display-time values stay
# whatever Google last delivered — only change DETECTION is normalised.
_HASH_EXCLUDE = frozenset({
    "conference_data_json", "source_html_link",
    "start_at", "end_at",
})


def _conference_signature(conf_json: Optional[str]) -> str:
    """Stable identity of a conferenceData blob — its conference id and
    the sorted set of entry-point URIs — ignoring ordering and volatile
    sub-fields.  Falls back to the raw value if it isn't parseable."""
    if not conf_json:
        return ""
    try:
        data = json.loads(conf_json)
    except (TypeError, ValueError):
        return conf_json
    uris = sorted(
        (ep.get("uri") or "") for ep in (data.get("entryPoints") or [])
    )
    return (data.get("conferenceId") or "") + "|" + "|".join(uris)


def _conference_id(conf_json: Optional[str]) -> Optional[str]:
    """The stable room identity (``conferenceId``) of a conferenceData
    blob, or ``None``.  Change-detection keys on this, NOT the whole
    blob, so entry-point ordering / phone-PIN noise that varies across
    reads is ignored — only an actual room swap counts."""
    if not conf_json:
        return None
    try:
        data = json.loads(conf_json)
    except (TypeError, ValueError):
        return None
    return (data or {}).get("conferenceId")


def _resolve_conference(existing, fields) -> tuple[Optional[str], Optional[str], bool]:
    """Debounced conference-link change detection.

    Returns ``(conference_data_json_to_store, pending_conference_id,
    changed)``.

    ``conference_data_json`` is deliberately excluded from the content
    hash (``_HASH_EXCLUDE``): a Meet-link swap must NEVER bump the version
    on its own, because Google returns a modified recurring instance's own
    link on one API surface and the inherited master link on another, and
    hashing that flip-flop once churned an instance to version 905.  This
    helper is the ONLY path that re-syncs a changed room to the mirror
    copies, and it is churn-proof: a new ``conferenceId`` is accepted only
    after the SAME value is seen on TWO consecutive ingests, so an
    alternating read never confirms.  A genuine, settled room change
    confirms on the next read and then propagates as a normal change.

    A REMOVAL (the source drops its conferenceData entirely) is debounced the
    same way: the candidate is the empty-string sentinel ``""`` — distinct from
    ``None`` ("no candidate") — so a settled removal confirms on the second
    read and clears the stale link.  Without the sentinel a removal's
    ``None`` candidate was indistinguishable from "no candidate" and never
    confirmed, leaving a dead Meet link on the mirror copy forever.
    """
    stored_json = existing["conference_data_json"]
    new_json = fields.get("conference_data_json")
    stored_cid = _conference_id(stored_json)
    new_cid = _conference_id(new_json)
    pending = (
        existing["pending_conference_id"]
        if "pending_conference_id" in existing.keys()
        else None
    )
    if new_cid == stored_cid:
        # Same room (or both have none): keep the accepted blob and drop
        # any outstanding candidate.
        return stored_json, None, False
    # A different room, or a removal (new_cid is None while a room was
    # stored).  Encode the candidate so a removal is distinguishable from
    # "no candidate": a real conferenceId, or "" meaning "pending removal".
    candidate = new_cid if new_cid is not None else ""
    if pending is not None and candidate == pending:
        # Confirmed on a second consecutive read: adopt the new state
        # (the new room, or — for a removal — drop the link entirely).
        return new_json, None, True
    # First sighting of the change: hold the accepted value and remember
    # the candidate until the next read confirms it.
    return stored_json, candidate, False


def _canonical_instant(value: Optional[str], is_all_day: bool) -> Optional[str]:
    """Hash-friendly form of an event start/end.

    For timed values: parse the offset-bearing ISO string to a UTC
    instant — same moment hashes the same regardless of which timezone
    Google chose to render it in.  For all-day values: the bare
    ``YYYY-MM-DD`` is already canonical.  Unparsable inputs fall back to
    the raw string so a malformed value still detects change."""
    if not value:
        return None
    if is_all_day:
        return value[:10]
    s = value
    iso = (s[:-1] + "+00:00") if s.endswith("Z") else s
    try:
        dt = datetime.fromisoformat(iso)
    except (ValueError, TypeError):
        return s
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _content_hash(fields: dict) -> str:
    hashable = {k: fields[k] for k in sorted(fields) if k not in _HASH_EXCLUDE}
    # conferenceData is dropped from change detection: even the
    # entry-point URI set varies across reads for the same event (Google
    # returns "uym-zdoy-vof" one pass and "vjs-kzyb-gkb" the next — both
    # valid links the recurring instance carries; the conference id /
    # URI normalisation we tried before couldn't see through that), so
    # any hash including it churned versions into the thousands.  The
    # blob is still STORED (raw) and rendered on the main copy, and an
    # actual Meet-link change re-renders the next time *anything else*
    # changes on the event or the next audit re-ingest.  We accept up to
    # one audit cycle of staleness on a Meet-link swap to keep the hash
    # deterministic.
    # UTC-normalised start/end so a timezone-only representation shift
    # doesn't masquerade as drift (the live churn that ran versions into
    # the thousands).  Original values still stored for display.
    is_all_day = bool(fields.get("is_all_day"))
    hashable["start_instant"] = _canonical_instant(fields.get("start_at"), is_all_day)
    hashable["end_instant"] = _canonical_instant(fields.get("end_at"), is_all_day)
    canonical = json.dumps(hashable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _content_hash_from_row(row) -> str:
    fields = {
        "summary": row["summary"],
        "description": row["description"],
        "location": row["location"],
        "start_at": row["start_at"],
        "end_at": row["end_at"],
        "start_timezone": row["start_timezone"],
        "end_timezone": row["end_timezone"],
        "is_all_day": bool(row["is_all_day"]),
        "show_as": row["show_as"],
        "visibility": row["visibility"],
        "color_id": row["color_id"],
        "organizer_email": row["organizer_email"],
        "user_can_edit": bool(row["user_can_edit"]),
        "user_rsvp_status": row["user_rsvp_status"],
        "attendees_json": row["attendees_json"],
        "recurrence_rule_json": row["recurrence_rule_json"],
        "is_recurring": bool(row["is_recurring"]),
        "conference_data_json": row["conference_data_json"],
        "source_html_link": row["source_html_link"],
    }
    return _content_hash(fields)


# ---------------------------------------------------------------------------
# Sync state + reconcile bookkeeping
# ---------------------------------------------------------------------------
async def _get_or_create_sync_state(
    db: aiosqlite.Connection, client_calendar_id: int,
) -> aiosqlite.Row:
    row = await (await db.execute(
        "SELECT * FROM calendar_sync_state WHERE client_calendar_id = ?",
        (client_calendar_id,),
    )).fetchone()
    if row is not None:
        return row
    await db.execute(
        """INSERT INTO calendar_sync_state (client_calendar_id) VALUES (?)""",
        (client_calendar_id,),
    )
    return await (await db.execute(
        "SELECT * FROM calendar_sync_state WHERE client_calendar_id = ?",
        (client_calendar_id,),
    )).fetchone()


async def _record_affected(
    db: aiosqlite.Connection, *, user_id: int, ledger_ids: list[int],
) -> None:
    """Mark affected ledger events for replan.

    Ingest already runs inside a reconcile pass that will plan these
    events in its own step 3, so this only records them — it does not
    schedule a further reconcile.
    """
    if not ledger_ids:
        return
    from app.ledger.triggers import record_affected_events
    await record_affected_events(
        db, user_id=user_id, ledger_event_ids=ledger_ids,
    )
