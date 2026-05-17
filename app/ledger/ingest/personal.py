"""Personal calendar ingest (REWRITE_PLAN.md §5.3).

Personal calendars are OAuth-connected like clients, but the
ledger marks their rows ``source_type='personal'``.  The planner
produces ``present_personal_busy`` projections everywhere (no
full-detail copy, no detail leaks across the personal/work
boundary).

Implementation reuses the client-ingest machinery directly —
the only difference is the source_type written to the ledger.
We carry that through by reaching into the client-ingest internals
rather than duplicating their entire body.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

from app.ledger.async_google import as_async_google
from app.ledger.google_client import GoogleClient
from app.ledger.identity import (
    canonical_uid_personal,
    is_managed_google_event_id,
)
from app.ledger.ingest.client import (
    _content_hash,
    _content_hash_from_row,
    _extract_event_fields,
    _ingest_instance,
    _is_recurring_parent,
    _record_affected,
    _try_rekey_R_parent,
    scan_full_sync_recurring_cancellations,
)

logger = logging.getLogger(__name__)
UTC = timezone.utc


async def ingest_personal_calendar(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    personal_calendar_id: int,
    google_calendar_id: str,
    user_email: str,
) -> dict:
    """Run one ingest pass for one personal calendar.

    Counters: ``{seen, created, updated, rekeyed, cancelled, skipped}``.
    The schema reuses ``client_calendars`` with
    ``calendar_type='personal'``; the sync state lives on the same
    ``calendar_sync_state`` row keyed by client_calendar_id.
    """
    google = as_async_google(google)
    state = await _get_or_create_sync_state(db, personal_calendar_id)
    sync_token: Optional[str] = state["sync_token"]
    counters: dict[str, int] = {
        "seen": 0, "created": 0, "updated": 0,
        "rekeyed": 0, "cancelled": 0, "skipped": 0,
    }
    affected_ledger_ids: list[int] = []
    page_token: Optional[str] = None
    new_sync_token: Optional[str] = None
    full_sync = sync_token is None
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
            outcome, ledger_id = await _ingest_one(
                db,
                user_id=user_id,
                personal_calendar_id=personal_calendar_id,
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
            return await _ingest_one(
                db,
                user_id=user_id,
                personal_calendar_id=personal_calendar_id,
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
            # a full sync and retries the scan.
            new_sync_token = None

    # Record affected ledger ids BEFORE advancing the sync token, so
    # the token write is the last durable write of the pass (the DB
    # runs in autocommit).  A crash after this point but before the
    # token advances just re-ingests idempotently next pass.
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
            when, personal_calendar_id,
        ),
    )
    await db.commit()
    return counters


async def _ingest_one(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    personal_calendar_id: int,
    user_email: str,
    event: dict,
) -> tuple[str, Optional[int]]:
    event_id = event["id"]
    status = event.get("status", "confirmed")

    if is_managed_google_event_id(event_id):
        return "skipped", None
    proj_match = await (await db.execute(
        """SELECT id FROM ledger_projections
            WHERE google_event_id = ? LIMIT 1""",
        (event_id,),
    )).fetchone()
    if proj_match is not None:
        return "skipped", None

    # Recurring-event INSTANCE — route to the shared instance
    # handler so cancellations get their own sticky ledger row.
    if event.get("recurringEventId"):
        parent_canonical = canonical_uid_personal(
            personal_calendar_id, event["recurringEventId"],
        )
        return await _ingest_instance(
            db,
            user_id=user_id,
            user_email=user_email,
            event=event,
            parent_canonical=parent_canonical,
            source_type="personal",
            source_calendar_id=personal_calendar_id,
        )

    # Rescheduled-parent ``_R`` quirk: re-key the existing series
    # ledger row to the new event id so its projections (and the
    # busy blocks they drive) are reused rather than orphaned.  The
    # normal upsert below then applies the new content to it.
    if (
        status != "cancelled"
        and "_R" in event_id
        and event.get("recurrence")
    ):
        await _try_rekey_R_parent(
            db,
            user_id=user_id,
            source_type="personal",
            source_calendar_id=personal_calendar_id,
            new_event_id=event_id,
            canonical_for=lambda eid: canonical_uid_personal(
                personal_calendar_id, eid,
            ),
        )

    canonical = canonical_uid_personal(personal_calendar_id, event_id)
    existing = await (await db.execute(
        """SELECT * FROM ledger_events
            WHERE user_id = ? AND canonical_uid = ?""",
        (user_id, canonical),
    )).fetchone()

    if status == "cancelled":
        if existing is None:
            return "skipped", None
        if existing["status"] == "cancelled":
            return "skipped", int(existing["id"])
        await _mark_cancelled(db, ledger_event_id=int(existing["id"]))
        return "cancelled", int(existing["id"])

    fields = _extract_event_fields(event, user_email=user_email)
    when = datetime.now(UTC).isoformat()
    if existing is None:
        cursor = await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid,
                   source_type, source_calendar_id, source_event_id,
                   source_etag, source_updated_at,
                   summary, description, location,
                   start_at, end_at, start_timezone, end_timezone,
                   is_all_day,
                   show_as, visibility, color_id,
                   organizer_email, user_can_edit, user_rsvp_status,
                   attendees_json, recurrence_rule_json,
                   status, is_recurring, version,
                   created_at, updated_at, last_seen_at)
               VALUES (?, ?,
                       'personal', ?, ?,
                       ?, ?,
                       ?, ?, ?,
                       ?, ?, ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?,
                       'active', ?, 1,
                       ?, ?, ?)""",
            (
                user_id, canonical,
                personal_calendar_id, event_id,
                event.get("etag"), event.get("updated"),
                fields["summary"], fields["description"], fields["location"],
                fields["start_at"], fields["end_at"],
                fields["start_timezone"], fields["end_timezone"],
                fields["is_all_day"],
                fields["show_as"], fields["visibility"], fields["color_id"],
                fields["organizer_email"], fields["user_can_edit"],
                fields["user_rsvp_status"],
                fields["attendees_json"], fields["recurrence_rule_json"],
                fields["is_recurring"],
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
                  start_at = ?, end_at = ?,
                  start_timezone = ?, end_timezone = ?, is_all_day = ?,
                  show_as = ?, visibility = ?, color_id = ?,
                  organizer_email = ?, user_can_edit = ?,
                  user_rsvp_status = ?,
                  attendees_json = ?, recurrence_rule_json = ?,
                  is_recurring = ?,
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
            fields["attendees_json"], fields["recurrence_rule_json"],
            fields["is_recurring"],
            when, when, int(existing["id"]),
        ),
    )
    return "updated", int(existing["id"])


async def _mark_cancelled(
    db: aiosqlite.Connection, *, ledger_event_id: int,
) -> None:
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE ledger_events
              SET status = 'cancelled',
                  version = version + 1,
                  cancelled_at = ?, updated_at = ?, last_seen_at = ?
            WHERE id = ?""",
        (when, when, when, ledger_event_id),
    )


async def _get_or_create_sync_state(
    db: aiosqlite.Connection, personal_calendar_id: int,
) -> aiosqlite.Row:
    row = await (await db.execute(
        "SELECT * FROM calendar_sync_state WHERE client_calendar_id = ?",
        (personal_calendar_id,),
    )).fetchone()
    if row is not None:
        return row
    await db.execute(
        "INSERT INTO calendar_sync_state (client_calendar_id) VALUES (?)",
        (personal_calendar_id,),
    )
    return await (await db.execute(
        "SELECT * FROM calendar_sync_state WHERE client_calendar_id = ?",
        (personal_calendar_id,),
    )).fetchone()
