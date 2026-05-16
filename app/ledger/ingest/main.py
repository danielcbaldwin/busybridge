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
    _ingest_instance,
    _is_recurring_parent,
    _record_affected,
    _try_rekey_R_parent,
    scan_full_sync_recurring_cancellations,
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
    recurring_parent_ids: set[str] = set()

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
                recurring_parent_ids.clear()
                continue
            raise

        for event in page.get("items", []):
            counters["seen"] += 1
            if _is_recurring_parent(event):
                recurring_parent_ids.add(event["id"])
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

    # On a full sync, recover cancelled recurring instances that
    # ``events.list`` omits (see scan_full_sync_recurring_cancellations).
    if full_sync and recurring_parent_ids:
        async def _ingest(inst: dict) -> tuple[str, Optional[int]]:
            return await _ingest_one_main_event(
                db,
                user_id=user_id,
                user_email=user_email,
                event=inst,
            )
        await scan_full_sync_recurring_cancellations(
            db, google,
            google_calendar_id=google_main_calendar_id,
            recurring_parent_ids=recurring_parent_ids,
            counters=counters,
            affected_ledger_ids=affected_ledger_ids,
            ingest_one=_ingest,
        )

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
        """SELECT p.id AS projection_id, p.ledger_event_id,
                  p.google_etag, p.applied_payload_hash
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
        # Edit-on-main detection: the user has changed our copy.
        # Two outcomes per REWRITE_PLAN.md §9:
        # * Editable event (client source, user_can_edit): propagate
        #   RSVP and/or time back to the source by bumping the
        #   ledger row.
        # * Non-editable (lock-emoji): the planner will revert
        #   automatically because the new etag invalidates our
        #   If-Match next time we try to update, AND because
        #   bumping the ledger row's version produces a fresh
        #   desired_payload_hash that re-asserts the canonical state.
        if proj_match is not None and status != "cancelled":
            outcome = await _maybe_apply_main_edit_back(
                db, user_email=user_email,
                ledger_event_id=int(proj_match["ledger_event_id"]),
                event=event,
            )
            if outcome is not None:
                return outcome, int(proj_match["ledger_event_id"])
        return "our_writes_skipped", None

    # Recurring-event INSTANCE of a *native* main series — route to
    # the shared instance handler so cancellations get their own
    # sticky ledger row.  Instances whose parent is one of our own
    # managed writes are left to the native path below (which
    # safely skips an unknown cancelled event).
    recurring_parent = event.get("recurringEventId")
    if recurring_parent and not is_managed_google_event_id(recurring_parent):
        parent_canonical = canonical_uid_main_native(user_id, recurring_parent)
        return await _ingest_instance(
            db,
            user_id=user_id,
            user_email=user_email,
            event=event,
            parent_canonical=parent_canonical,
            source_type="main_native",
            source_calendar_id=None,
        )

    # Rescheduled-parent ``_R`` quirk for a native main series:
    # re-key the existing series ledger row so its projections are
    # reused rather than orphaned.  The normal upsert below applies
    # the new content to it.
    if (
        status != "cancelled"
        and "_R" in event_id
        and event.get("recurrence")
    ):
        await _try_rekey_R_parent(
            db,
            user_id=user_id,
            source_type="main_native",
            source_calendar_id=None,
            new_event_id=event_id,
            canonical_for=lambda eid: canonical_uid_main_native(user_id, eid),
        )

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


async def _maybe_apply_main_edit_back(
    db: aiosqlite.Connection,
    *,
    user_email: str,
    ledger_event_id: int,
    event: dict,
) -> Optional[str]:
    """User edited our copy on main.  Decide what to do per
    REWRITE_PLAN.md §9:

    * RSVP changed → ALWAYS propagate.  A user may set their own
      response on any event, editable or not; the planner routes it
      back to the origin client via the ``present_full_rsvp_only``
      projection (the outbox writes it with an ``events.patch``).
    * Time changed + editable → propagate the new time to the source.
    * Time changed + non-editable → bump the version so the planner
      re-renders the canonical time and the outbox reverts the drift.
    """
    ledger = await (await db.execute(
        "SELECT * FROM ledger_events WHERE id = ?",
        (ledger_event_id,),
    )).fetchone()
    if ledger is None:
        return None

    new_rsvp = _extract_self_rsvp(event, user_email)
    new_start, new_end, is_all_day = _extract_start_end(event)

    user_can_edit = bool(ledger["user_can_edit"])
    rsvp_changed = (new_rsvp is not None and new_rsvp != ledger["user_rsvp_status"])
    time_changed = (
        new_start is not None
        and (new_start != ledger["start_at"] or new_end != ledger["end_at"])
    )

    if not (rsvp_changed or time_changed):
        return "our_writes_skipped"

    when = datetime.now(UTC).isoformat()
    # A time edit only propagates to the source when the user is
    # allowed to move the event; otherwise it is drift to revert.
    apply_time = time_changed and user_can_edit

    if rsvp_changed or apply_time:
        # Forward-edit: update the ledger so the planner re-renders
        # and the outbox pushes the change back.  RSVP routes to the
        # origin client; an editable time edit routes to every copy.
        # The version bump also reverts any *non-editable* time drift
        # bundled into the same edit (the planner re-renders the
        # canonical time, the diff sees the main copy diverged).
        await db.execute(
            """UPDATE ledger_events
                  SET user_rsvp_status = COALESCE(?, user_rsvp_status),
                      start_at = COALESCE(?, start_at),
                      end_at = COALESCE(?, end_at),
                      is_all_day = COALESCE(?, is_all_day),
                      version = version + 1,
                      updated_at = ?
                WHERE id = ?""",
            (
                new_rsvp if rsvp_changed else None,
                new_start if apply_time else None,
                new_end if apply_time else None,
                is_all_day if apply_time else None,
                when, ledger_event_id,
            ),
        )
        return "main_edit_propagated"

    # Only a non-editable time drift remains: bump the version
    # without changing canonical content.  The planner re-renders
    # the desired payload and the diff issues an update that reverts
    # the move on Google.
    await db.execute(
        """UPDATE ledger_events
              SET version = version + 1,
                  updated_at = ?
            WHERE id = ?""",
        (when, ledger_event_id),
    )
    return "main_drift_reverted"


def _extract_self_rsvp(event: dict, user_email: str) -> Optional[str]:
    for att in (event.get("attendees") or []):
        if att.get("self") or att.get("email", "").lower() == user_email.lower():
            return att.get("responseStatus")
    return None


def _extract_start_end(event: dict) -> tuple[Optional[str], Optional[str], Optional[bool]]:
    start = event.get("start") or {}
    end = event.get("end") or {}
    if "date" in start:
        return start.get("date"), end.get("date"), True
    if "dateTime" in start:
        return start.get("dateTime"), end.get("dateTime"), False
    return None, None, None


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
