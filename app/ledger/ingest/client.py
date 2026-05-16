"""Client OAuth ingest (REWRITE_PLAN.md §5.1).

For each event Google delivers, decide:

1. Is it one of our writes (a busy block / projection)?  If so,
   skip — we don't want to mirror our own output.
2. Is it a rescheduled-parent ``_R`` case?  Look up the existing
   ledger row by stripped base ID and re-key its
   ``source_event_id`` to the new one.
3. Otherwise: upsert the ledger row, bumping ``version`` only if a
   material field changed.

The whole loop runs inside one transaction so the sync token can
only advance after every event was successfully recorded.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

import aiosqlite

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
) -> dict:
    """Run one ingest pass for one client calendar.

    Returns counters: ``{seen, created, updated, rekeyed, skipped, cancelled}``.
    Affected ``ledger_event.id``s are written into the
    ``reconcile_requests.sources_json`` so the reconciler picks
    them up for planning.
    """
    state = await _get_or_create_sync_state(db, client_calendar_id)
    sync_token: Optional[str] = state["sync_token"]
    counters = {
        "seen": 0, "created": 0, "updated": 0,
        "rekeyed": 0, "skipped": 0, "cancelled": 0,
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
            page = google.list_events(
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
            outcome, ledger_id = await _ingest_one_event(
                db,
                user_id=user_id,
                client_calendar_id=client_calendar_id,
                user_email=user_email,
                event=event,
            )
            counters[outcome] = counters.get(outcome, 0) + 1
            if ledger_id is not None:
                affected_ledger_ids.append(ledger_id)

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
    recurring-cancellation-amnesia bug (REWRITE_PLAN.md §8) for
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
            inst_resp = google.list_instances(
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
            outcome, ledger_id = await ingest_one(inst)
            counters[outcome] = counters.get(outcome, 0) + 1
            if ledger_id is not None:
                affected_ledger_ids.append(ledger_id)
    return failed


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
) -> tuple[str, Optional[int]]:
    """Process one Google event.  Returns (outcome, ledger_event_id)."""
    event_id = event["id"]
    status = event.get("status", "confirmed")

    # 1. Loop-prevention: skip events we wrote (deterministic IDs
    # plus an exact projection lookup as defence-in-depth).  Before
    # skipping, check whether one of our busy blocks has drifted —
    # the user moved or edited it on the client calendar — and if so
    # re-assert our canonical payload (revert-on-drift, now uniform
    # on client targets too; REWRITE_PLAN.md §9).
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
            event=event,
            parent_canonical=parent_canonical,
            source_type="client",
            source_calendar_id=client_calendar_id,
        )

    canonical = canonical_uid_client(client_calendar_id, event_id)

    # 3. Rescheduled-parent ``_R`` quirk.  Re-key existing ledger
    # row before treating this as a new event.
    if (
        status != "cancelled"
        and "_R" in event_id
        and event.get("recurrence")
    ):
        rekeyed = await _try_rekey_R_parent(
            db,
            user_id=user_id,
            source_type="client",
            source_calendar_id=client_calendar_id,
            new_event_id=event_id,
            canonical_for=lambda eid: canonical_uid_client(
                client_calendar_id, eid,
            ),
        )
        if rekeyed is not None:
            await _apply_event_to_ledger(
                db, ledger_event_id=rekeyed,
                event=event, user_email=user_email,
            )
            return "rekeyed", rekeyed

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
        )
        return "created", new_id
    changed = await _apply_event_to_ledger(
        db,
        ledger_event_id=int(existing["id"]),
        event=event,
        user_email=user_email,
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
    (REWRITE_PLAN.md §9; uniform with the main-copy revert).

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
async def _ingest_instance(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    user_email: str,
    event: dict,
    parent_canonical: str,
    source_type: str,
    source_calendar_id: Optional[int],
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
    """
    ost = event.get("originalStartTime", {}) or {}
    if "dateTime" in ost:
        original_start = ost["dateTime"]
    elif "date" in ost:
        original_start = ost["date"]
    else:
        original_start = (event.get("start") or {}).get("dateTime") or (
            event.get("start") or {}
        ).get("date") or ""

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
                       recurrence_instance_original_start,
                       status, version, is_recurring,
                       created_at, updated_at, last_seen_at, cancelled_at)
                   VALUES (?, ?, ?,
                           ?, ?, ?, ?,
                           'cancelled', 1, 0,
                           ?, ?, ?, ?)""",
                (
                    user_id, instance_canonical, parent_canonical,
                    source_type, source_calendar_id, event["id"],
                    original_start,
                    when, when, when, when,
                ),
            )
            return "cancelled", int(cursor.lastrowid)
        await db.execute(
            """UPDATE ledger_events
                  SET status = 'cancelled',
                      version = version + 1,
                      cancelled_at = ?, updated_at = ?, last_seen_at = ?
                WHERE id = ?""",
            (when, when, when, int(existing["id"])),
        )
        return "cancelled", int(existing["id"])

    # Modified instance — single-instance override on the series.
    fields = _extract_event_fields(event, user_email=user_email)
    if existing is None:
        cursor = await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid, parent_canonical_uid,
                   source_type, source_calendar_id, source_event_id,
                   recurrence_instance_original_start,
                   source_etag, source_updated_at,
                   summary, description, location,
                   start_at, end_at, is_all_day,
                   show_as, visibility, color_id,
                   organizer_email, user_can_edit, user_rsvp_status,
                   attendees_json,
                   status, is_recurring, version,
                   created_at, updated_at, last_seen_at)
               VALUES (?, ?, ?,
                       ?, ?, ?, ?,
                       ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?,
                       'active', 0, 1,
                       ?, ?, ?)""",
            (
                user_id, instance_canonical, parent_canonical,
                source_type, source_calendar_id, event["id"],
                original_start,
                event.get("etag"), event.get("updated"),
                fields["summary"], fields["description"], fields["location"],
                fields["start_at"], fields["end_at"], fields["is_all_day"],
                fields["show_as"], fields["visibility"], fields["color_id"],
                fields["organizer_email"], fields["user_can_edit"],
                fields["user_rsvp_status"],
                fields["attendees_json"],
                when, when, when,
            ),
        )
        return "created", int(cursor.lastrowid)

    new_hash = _content_hash(fields)
    old_hash = _content_hash_from_row(existing)
    if new_hash == old_hash:
        await db.execute(
            "UPDATE ledger_events SET last_seen_at = ? WHERE id = ?",
            (when, int(existing["id"])),
        )
        return "skipped", int(existing["id"])
    await db.execute(
        """UPDATE ledger_events
              SET source_etag = ?, source_updated_at = ?,
                  summary = ?, description = ?, location = ?,
                  start_at = ?, end_at = ?, is_all_day = ?,
                  show_as = ?, visibility = ?, color_id = ?,
                  organizer_email = ?, user_can_edit = ?,
                  user_rsvp_status = ?,
                  attendees_json = ?,
                  status = 'active',
                  version = version + 1,
                  updated_at = ?, last_seen_at = ?
            WHERE id = ?""",
        (
            event.get("etag"), event.get("updated"),
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"], fields["is_all_day"],
            fields["show_as"], fields["visibility"], fields["color_id"],
            fields["organizer_email"], fields["user_can_edit"],
            fields["user_rsvp_status"],
            fields["attendees_json"],
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
) -> int:
    fields = _extract_event_fields(event, user_email=user_email)
    when = datetime.now(UTC).isoformat()
    cursor = await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid,
               source_type, source_calendar_id, source_event_id,
               source_etag, source_updated_at,
               summary, description, location,
               start_at, end_at, is_all_day,
               show_as, visibility, color_id,
               organizer_email, user_can_edit, user_rsvp_status,
               attendees_json, recurrence_rule_json,
               status, is_recurring, version,
               created_at, updated_at, last_seen_at)
           VALUES (?, ?,
                   'client', ?, ?,
                   ?, ?,
                   ?, ?, ?,
                   ?, ?, ?,
                   ?, ?, ?,
                   ?, ?, ?,
                   ?, ?,
                   'active', ?, 1,
                   ?, ?, ?)""",
        (
            user_id, canonical_uid,
            client_calendar_id, event["id"],
            event.get("etag"), event.get("updated"),
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"], fields["is_all_day"],
            fields["show_as"], fields["visibility"], fields["color_id"],
            fields["organizer_email"], fields["user_can_edit"],
            fields["user_rsvp_status"],
            fields["attendees_json"], fields["recurrence_rule_json"],
            fields["is_recurring"],
            when, when, when,
        ),
    )
    return int(cursor.lastrowid)


async def _apply_event_to_ledger(
    db: aiosqlite.Connection,
    *,
    ledger_event_id: int,
    event: dict,
    user_email: str,
) -> bool:
    """Update an existing ledger row.  Returns True if any material
    field changed (and version was bumped).  A row resurrecting
    from ``cancelled`` to ``active`` always counts as changed."""
    fields = _extract_event_fields(event, user_email=user_email)
    existing = await (await db.execute(
        "SELECT * FROM ledger_events WHERE id = ?",
        (ledger_event_id,),
    )).fetchone()
    when = datetime.now(UTC).isoformat()

    new_hash = _content_hash(fields)
    old_hash = _content_hash_from_row(existing)
    resurrecting = existing["status"] == "cancelled"
    changed = (new_hash != old_hash) or resurrecting

    if not changed:
        await db.execute(
            "UPDATE ledger_events SET last_seen_at = ? WHERE id = ?",
            (when, ledger_event_id),
        )
        return False

    await db.execute(
        """UPDATE ledger_events
              SET source_etag = ?,
                  source_updated_at = ?,
                  summary = ?, description = ?, location = ?,
                  start_at = ?, end_at = ?, is_all_day = ?,
                  show_as = ?, visibility = ?, color_id = ?,
                  organizer_email = ?, user_can_edit = ?,
                  user_rsvp_status = ?,
                  attendees_json = ?, recurrence_rule_json = ?,
                  is_recurring = ?,
                  status = 'active',
                  version = version + 1,
                  updated_at = ?,
                  last_seen_at = ?
            WHERE id = ?""",
        (
            event.get("etag"), event.get("updated"),
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"], fields["is_all_day"],
            fields["show_as"], fields["visibility"], fields["color_id"],
            fields["organizer_email"], fields["user_can_edit"],
            fields["user_rsvp_status"],
            fields["attendees_json"], fields["recurrence_rule_json"],
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


async def _try_rekey_R_parent(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    source_type: str,
    source_calendar_id: Optional[int],
    new_event_id: str,
    canonical_for: Callable[[str], str],
) -> Optional[int]:
    """Look up an existing ledger row under the base ID (everything
    before ``_R``) and re-key it to ``new_event_id``.  Returns the
    ledger event id if a re-key happened, else None.

    Source-neutral: ``source_type`` / ``source_calendar_id`` scope
    the search, and ``canonical_for`` maps a source event id to its
    canonical_uid (``canonical_uid_client`` / ``_personal`` /
    ``_main_native``).  Searches for both the bare base and any
    prior ``_R<ts>`` variant.
    """
    base = new_event_id.split("_R")[0]
    bare_uid = canonical_for(base)
    rows = await (await db.execute(
        """SELECT id, canonical_uid, source_event_id
             FROM ledger_events
            WHERE user_id = ?
              AND source_type = ?
              AND COALESCE(source_calendar_id, -1) = COALESCE(?, -1)
              AND status = 'active'
              AND user_intentionally_deleted = 0
              AND (canonical_uid = ?
                   OR source_event_id = ?
                   OR source_event_id LIKE ?)
            ORDER BY updated_at DESC
            LIMIT 1""",
        (
            user_id, source_type, source_calendar_id,
            bare_uid, base, f"{base}_R%",
        ),
    )).fetchall()
    if not rows:
        return None
    target = rows[0]
    old_canonical = target["canonical_uid"]
    new_canonical = canonical_for(new_event_id)
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE ledger_events
              SET canonical_uid = ?,
                  source_event_id = ?,
                  updated_at = ?
            WHERE id = ?""",
        (new_canonical, new_event_id, when, int(target["id"])),
    )
    # Re-parent any modified-instance ledger rows that pointed at the
    # old series canonical, so they stay attached to the re-keyed
    # parent (REWRITE_PLAN.md §8 — "old instance rows get their
    # parent_canonical_uid updated").  Without this the diff's
    # parent-projection lookup resolves to nothing and the instance
    # is orphaned.  Post-boundary instance overrides that the source
    # cancels are handled by the normal cancelled-instance path.
    cur = await db.execute(
        """UPDATE ledger_events
              SET parent_canonical_uid = ?,
                  updated_at = ?
            WHERE user_id = ?
              AND parent_canonical_uid = ?""",
        (new_canonical, when, user_id, old_canonical),
    )
    logger.info(
        "re-keyed ledger_event %s from source_event_id=%s to %s "
        "(_R reschedule); re-parented %s instance row(s)",
        target["id"], target["source_event_id"], new_event_id,
        getattr(cur, "rowcount", "?"),
    )
    return int(target["id"])


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------
def _extract_event_fields(event: dict, *, user_email: str) -> dict:
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
    # User can edit if: they are the organizer, OR the event explicitly
    # marks guestsCanModify=True.  Solo events (no attendees, no
    # explicit organizer set) are also editable.
    has_attendees = bool(event.get("attendees"))
    user_can_edit = (
        _user_is_organizer(event, user_email)
        or bool(event.get("guestsCanModify"))
        or not has_attendees
    )

    user_rsvp = None
    for att in (event.get("attendees") or []):
        if att.get("self") or att.get("email", "").lower() == user_email.lower():
            user_rsvp = att.get("responseStatus")
            break

    organizer_email = (event.get("organizer") or {}).get("email")

    return {
        "summary": event.get("summary"),
        "description": event.get("description"),
        "location": event.get("location"),
        "start_at": start_at,
        "end_at": end_at,
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
    }


def _user_is_organizer(event: dict, user_email: str) -> bool:
    organizer = event.get("organizer") or {}
    return (organizer.get("email") or "").lower() == user_email.lower()


def _content_hash(fields: dict) -> str:
    canonical = json.dumps(
        {k: fields[k] for k in sorted(fields)},
        sort_keys=True, separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _content_hash_from_row(row) -> str:
    fields = {
        "summary": row["summary"],
        "description": row["description"],
        "location": row["location"],
        "start_at": row["start_at"],
        "end_at": row["end_at"],
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
