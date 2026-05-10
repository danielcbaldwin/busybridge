"""Main calendar ingest (REWRITE_PLAN.md §5.2).

Same shape as the client ingest, with three differences:

1. Skip events whose ID matches one of our projections — those
   are our own writes.
2. Native main events (NOT projections of any other source) get
   ``source_type='main_native'``.
3. Cancelled-on-main events that ARE projections of another
   source flip the parent ledger row's
   ``user_intentionally_deleted`` flag, so the planner suppresses
   re-creation on every target.

Edit-back-propagation (RSVP from main → source, drag-on-main
revert) is intentionally not in this first pass — those land
once the basic main-ingest is exercised end-to-end.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

from app.ledger.google_client import GoogleClient
from app.ledger.identity import (
    canonical_uid_main_native,
    is_managed_google_event_id,
)
from app.ledger.ingest.client import (
    _content_hash,
    _content_hash_from_row,
    _extract_event_fields,
    _record_affected,
)

logger = logging.getLogger(__name__)
UTC = timezone.utc


async def ingest_main_calendar(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    google_main_calendar_id: str,
    user_email: str,
) -> dict:
    """Run one ingest pass for the user's main calendar.

    Returns counters: ``{seen, native_created, native_updated,
    user_deletes, our_writes_skipped, skipped, cancelled_native}``.
    """
    state = await _get_or_create_sync_state(db, user_id=user_id)
    sync_token: Optional[str] = state["sync_token"]
    counters: dict[str, int] = {
        "seen": 0, "native_created": 0, "native_updated": 0,
        "user_deletes": 0, "our_writes_skipped": 0, "skipped": 0,
        "cancelled_native": 0,
    }
    affected_ledger_ids: list[int] = []
    page_token: Optional[str] = None
    new_sync_token: Optional[str] = None
    full_sync = sync_token is None

    while True:
        try:
            page = google.list_events(
                google_main_calendar_id,
                sync_token=sync_token if page_token is None else None,
                page_token=page_token,
                show_deleted=True,
                max_results=250,
            )
        except Exception as e:
            if getattr(e, "status", None) == 410:
                logger.info(
                    "main sync token expired for user_id=%s, falling back",
                    user_id,
                )
                sync_token = None
                page_token = None
                full_sync = True
                continue
            raise

        for event in page.get("items", []):
            counters["seen"] += 1
            outcome, ledger_id = await _ingest_one_main_event(
                db,
                user_id=user_id,
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

    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE main_calendar_sync_state
              SET sync_token = ?,
                  last_full_sync = COALESCE(?, last_full_sync),
                  last_incremental_sync = ?,
                  consecutive_failures = 0,
                  last_error = NULL
            WHERE user_id = ?""",
        (
            new_sync_token,
            when if full_sync else None,
            when, user_id,
        ),
    )
    if affected_ledger_ids:
        await _record_affected(db, user_id=user_id, ledger_ids=affected_ledger_ids)
    await db.commit()
    return counters


async def _ingest_one_main_event(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    user_email: str,
    event: dict,
) -> tuple[str, Optional[int]]:
    event_id = event["id"]
    status = event.get("status", "confirmed")

    # Was this our write?  Two checks: deterministic ID prefix and
    # exact projection lookup.
    is_our_write = is_managed_google_event_id(event_id)
    proj_match = await (await db.execute(
        """SELECT p.id AS projection_id, p.ledger_event_id
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE p.google_event_id = ? AND e.user_id = ?
            LIMIT 1""",
        (event_id, user_id),
    )).fetchone()
    if proj_match is not None or is_our_write:
        if proj_match is not None and status == "cancelled":
            # User deleted our copy on main → flip
            # user_intentionally_deleted on the source ledger row.
            ledger_id = int(proj_match["ledger_event_id"])
            await _mark_user_intentionally_deleted(db, ledger_id)
            return "user_deletes", ledger_id
        return "our_writes_skipped", None

    # A native main event we haven't seen before, or seen previously.
    canonical = canonical_uid_main_native(user_id, event_id)
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
        await _mark_native_cancelled(db, ledger_event_id=int(existing["id"]))
        return "cancelled_native", int(existing["id"])

    fields = _extract_event_fields(event, user_email=user_email)
    when = datetime.now(UTC).isoformat()
    if existing is None:
        cursor = await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid,
                   source_type, source_event_id,
                   source_etag, source_updated_at,
                   summary, description, location,
                   start_at, end_at, is_all_day,
                   show_as, visibility, color_id,
                   organizer_email, user_can_edit, user_rsvp_status,
                   attendees_json, recurrence_rule_json,
                   status, is_recurring, version,
                   created_at, updated_at, last_seen_at)
               VALUES (?, ?,
                       'main_native', ?,
                       ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?,
                       'active', ?, 1,
                       ?, ?, ?)""",
            (
                user_id, canonical,
                event_id,
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
        return "native_created", int(cursor.lastrowid)

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
            when, when, int(existing["id"]),
        ),
    )
    return "native_updated", int(existing["id"])


async def _mark_user_intentionally_deleted(
    db: aiosqlite.Connection, ledger_event_id: int,
) -> None:
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE ledger_events
              SET user_intentionally_deleted = 1,
                  version = version + 1,
                  updated_at = ?
            WHERE id = ?""",
        (when, ledger_event_id),
    )


async def _mark_native_cancelled(
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


async def _get_or_create_sync_state(
    db: aiosqlite.Connection, *, user_id: int,
) -> aiosqlite.Row:
    row = await (await db.execute(
        "SELECT * FROM main_calendar_sync_state WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    if row is not None:
        return row
    await db.execute(
        "INSERT INTO main_calendar_sync_state (user_id) VALUES (?)",
        (user_id,),
    )
    return await (await db.execute(
        "SELECT * FROM main_calendar_sync_state WHERE user_id = ?",
        (user_id,),
    )).fetchone()
