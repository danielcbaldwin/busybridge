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

Edit-back-propagation (REWRITE_PLAN.md §9): a user edit to one of
our managed copies on the main calendar is classified by
:func:`_maybe_apply_main_edit_back` and either propagated to the
source event or reverted as drift.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

from app.ledger.async_google import as_async_google
from app.ledger.google_client import GoogleClient
from app.ledger.identity import (
    canonical_uid_main_native,
    derive_instance_google_event_id,
    is_managed_google_event_id,
)
from app.ledger.payload import render_payload
from app.ledger.ingest.client import (
    _content_hash,
    _content_hash_from_row,
    _extract_event_fields,
    _ingest_instance,
    _instance_original_start,
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
    google = as_async_google(google)
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
            page = await google.list_events(
                google_main_calendar_id,
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
        scan_failures = await scan_full_sync_recurring_cancellations(
            db, google,
            google_calendar_id=google_main_calendar_id,
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

    # A recurring-event INSTANCE (carries recurringEventId).
    recurring_parent = event.get("recurringEventId")
    if recurring_parent and is_managed_google_event_id(recurring_parent):
        # An instance — modified or cancelled — of one of OUR managed
        # recurring copies (the parent id is a bb-derived id).  It is
        # NOT a native main event: falling through to the native
        # upsert below would mint a phantom main_native ledger row and
        # project DUPLICATE busy blocks onto every client calendar.
        # A modified instance is the user moving/editing one
        # occurrence on the main calendar — map it back to the SOURCE
        # series and let the planner propagate it everywhere.
        return await _ingest_managed_recurring_instance(
            db,
            user_id=user_id,
            user_email=user_email,
            event=event,
            managed_parent_id=recurring_parent,
        )
    if recurring_parent and not is_managed_google_event_id(recurring_parent):
        # Instance of a *native* main series — route to the shared
        # instance handler so a cancellation gets its own sticky
        # ledger row.
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
                   start_at, end_at, start_timezone, end_timezone,
                   is_all_day,
                   show_as, visibility, color_id,
                   organizer_email, user_can_edit, user_rsvp_status,
                   attendees_json, recurrence_rule_json,
                   status, is_recurring, version,
                   created_at, updated_at, last_seen_at)
               VALUES (?, ?,
                       'main_native', ?,
                       ?, ?,
                       ?, ?, ?,
                       ?, ?, ?, ?,
                       ?,
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
                  start_at = ?, end_at = ?,
                  start_timezone = ?, end_timezone = ?, is_all_day = ?,
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
    return "native_updated", int(existing["id"])


async def _ingest_managed_recurring_instance(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    user_email: str,
    event: dict,
    managed_parent_id: str,
) -> tuple[str, Optional[int]]:
    """Handle an instance of one of our managed recurring main copies.

    The user moved or edited a single occurrence of a recurring event
    we mirror onto the main calendar.  Map it back to the SOURCE
    series' ledger row and upsert a canonical source-parented instance
    row, so the existing planner / diff propagate the change to the
    source occurrence and every peer copy (REWRITE_PLAN.md Option A).

    Never mints a ``main_native`` row.  Cancelled instances are a
    destructive operation handled separately; for now they are skipped.
    """
    status = event.get("status", "confirmed")
    if status == "cancelled":
        # Cancelling one occurrence of a managed copy is destructive
        # (it must delete that occurrence on the real source calendar).
        # Handled by the dedicated cancellation path; skip here.
        return "our_writes_skipped", None

    # Map the managed parent id back to the SOURCE series ledger row
    # via its 'main' projection.  The bb-id the instance carries as
    # recurringEventId is that projection's google_event_id.
    parent = await (await db.execute(
        """SELECT e.id AS ledger_event_id, e.canonical_uid,
                  e.source_type, e.source_calendar_id, e.source_event_id,
                  e.is_recurring, e.parent_canonical_uid,
                  e.user_can_edit, e.organizer_email, e.attendees_json
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE p.google_event_id = ?
              AND p.target_kind = 'main'
              AND e.user_id = ?
            LIMIT 1""",
        (managed_parent_id, user_id),
    )).fetchone()
    if parent is None:
        # No projection maps this id — cannot map it back to a source,
        # so there is nothing safe to do.  Skip (never mint a phantom).
        return "our_writes_skipped", None
    if not parent["is_recurring"] or parent["parent_canonical_uid"]:
        # The mapped row is not a recurring series master — defensive.
        return "our_writes_skipped", None

    # The dragged copy is opaque about edit-rights: its rendered shape
    # carries neither the real organizer nor guestsCanModify.  Take
    # edit-rights / organizer / attendees from the SOURCE series; take
    # the moved time and (for full copies) the detail from the event.
    fields = _extract_event_fields(event, user_email=user_email)
    fields["user_can_edit"] = bool(parent["user_can_edit"])
    fields["organizer_email"] = parent["organizer_email"]
    fields["attendees_json"] = parent["attendees_json"]

    # The origin writeback patches the source occurrence by id.  No
    # exception exists on the source yet, but Google addresses an
    # instance as ``<series>_<stamp>`` whether or not one has been
    # materialised — derive it from the source series id so the
    # writeback has a target (and so a later source-side ingest of
    # that same exception keys to the very same row).
    source_event_id: Optional[str] = None
    src_series_id = parent["source_event_id"]
    if parent["source_type"] in ("client", "personal") and src_series_id:
        original_start, instance_is_all_day = _instance_original_start(event)
        source_event_id = derive_instance_google_event_id(
            src_series_id, original_start, instance_is_all_day,
        )

    outcome, ledger_id = await _ingest_instance(
        db,
        user_id=user_id,
        user_email=user_email,
        event=event,
        parent_canonical=parent["canonical_uid"],
        source_type=parent["source_type"],
        source_calendar_id=parent["source_calendar_id"],
        source_event_id=source_event_id,
        fields=fields,
    )

    # The change was made on main; the source event does not have it.
    # Flag the row so the origin-writeback patch fires even though the
    # instance's writeback projection is brand new (NULL applied hash).
    # Only on a real create/update — a no-op re-ingest must not re-arm
    # a writeback that already drained.
    if ledger_id is not None and outcome in ("created", "updated"):
        await db.execute(
            "UPDATE ledger_events SET origin_writeback_pending = 1 "
            "WHERE id = ?",
            (ledger_id,),
        )
    return outcome, ledger_id


async def _maybe_apply_main_edit_back(
    db: aiosqlite.Connection,
    *,
    user_email: str,
    ledger_event_id: int,
    event: dict,
) -> Optional[str]:
    """The user edited our copy of an event on the main calendar.
    Classify the edit and either propagate it to the source event or
    revert it (REWRITE_PLAN.md §9).

    Three edit categories, each routed per source type:

    * RSVP — always propagated.  A user may set their own response
      on any event, editable or not; the planner's origin writeback
      projection writes it back to the source via an events.patch.
    * Time (start/end) — propagated for an editable client- or
      personal-sourced event; reverted otherwise (a webcal feed is
      read-only; a locked event the user is not allowed to move).
    * Detail (summary/description/location) — propagated for an
      editable client-sourced event only.  A personal main copy is
      an opaque "Busy (personal)" placeholder and a webcal copy is
      read-only, so a detail edit there is reverted.

    A propagated category is written into the ledger so the planner
    re-renders it onto every copy and the origin writeback patch
    carries it to the source.  An un-propagated (reverted) category
    is left untouched in the ledger; the version bump alone makes the
    planner re-render the canonical copy and the diff revert the
    drift on the main calendar.
    """
    ledger = await (await db.execute(
        "SELECT * FROM ledger_events WHERE id = ?",
        (ledger_event_id,),
    )).fetchone()
    if ledger is None:
        return None

    # Compare the incoming event against the payload we last rendered
    # for the main copy — NOT the raw ledger fields.  A personal copy
    # is an opaque placeholder whose rendered summary ("Busy
    # (personal)") never equals the ledger's real summary, so a
    # ledger-field comparison would read every personal copy as
    # permanently drifted.
    main_proj = await (await db.execute(
        """SELECT desired_state FROM ledger_projections
            WHERE ledger_event_id = ?
              AND target_kind = 'main'
              AND target_calendar_id IS NULL""",
        (ledger_event_id,),
    )).fetchone()
    canonical: dict = {}
    if main_proj is not None:
        rendered = render_payload(
            desired_state=main_proj["desired_state"],
            ledger_row={k: ledger[k] for k in ledger.keys()},
            target_kind="main",
        )
        canonical = rendered or {}

    new_rsvp = _extract_self_rsvp(event, user_email)
    new_start, new_end, new_start_tz, new_end_tz, is_all_day = (
        _extract_start_end(event)
    )

    source_type = ledger["source_type"]
    user_can_edit = bool(ledger["user_can_edit"])

    rsvp_changed = (
        new_rsvp is not None and new_rsvp != ledger["user_rsvp_status"]
    )
    time_changed = (
        new_start is not None
        and (new_start != ledger["start_at"] or new_end != ledger["end_at"])
    )
    detail_changed = _detail_differs(event, canonical)

    if not (rsvp_changed or time_changed or detail_changed):
        return "our_writes_skipped"

    # Which categories may be written back to this source type.
    time_writeback = source_type in ("client", "personal")
    detail_writeback = source_type == "client"

    apply_rsvp = rsvp_changed
    apply_time = time_changed and user_can_edit and time_writeback
    apply_detail = detail_changed and user_can_edit and detail_writeback

    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE ledger_events
              SET user_rsvp_status = COALESCE(?, user_rsvp_status),
                  start_at = COALESCE(?, start_at),
                  end_at = COALESCE(?, end_at),
                  start_timezone = CASE WHEN ? THEN ? ELSE start_timezone END,
                  end_timezone = CASE WHEN ? THEN ? ELSE end_timezone END,
                  is_all_day = COALESCE(?, is_all_day),
                  summary = CASE WHEN ? THEN ? ELSE summary END,
                  description = CASE WHEN ? THEN ? ELSE description END,
                  location = CASE WHEN ? THEN ? ELSE location END,
                  version = version + 1,
                  updated_at = ?
            WHERE id = ?""",
        (
            new_rsvp if apply_rsvp else None,
            new_start if apply_time else None,
            new_end if apply_time else None,
            apply_time, new_start_tz,
            apply_time, new_end_tz,
            is_all_day if apply_time else None,
            apply_detail, event.get("summary"),
            apply_detail, event.get("description"),
            apply_detail, event.get("location"),
            when, ledger_event_id,
        ),
    )
    if apply_rsvp or apply_time or apply_detail:
        return "main_edit_propagated"
    return "main_drift_reverted"


def _detail_differs(event: dict, canonical: dict) -> bool:
    """True when the incoming main-copy event's summary, description
    or location differs from the payload last rendered for it.  An
    absent field and an empty string are normalised to compare equal.
    """
    for key in ("summary", "description", "location"):
        if (event.get(key) or "") != (canonical.get(key) or ""):
            return True
    return False


def _extract_self_rsvp(event: dict, user_email: str) -> Optional[str]:
    for att in (event.get("attendees") or []):
        if att.get("self") or att.get("email", "").lower() == user_email.lower():
            return att.get("responseStatus")
    return None


def _extract_start_end(
    event: dict,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[bool]]:
    """Return ``(start, end, start_timezone, end_timezone, is_all_day)``
    for an incoming main-copy event.  All-day events carry no zone."""
    start = event.get("start") or {}
    end = event.get("end") or {}
    if "date" in start:
        return start.get("date"), end.get("date"), None, None, True
    if "dateTime" in start:
        return (
            start.get("dateTime"), end.get("dateTime"),
            start.get("timeZone"), end.get("timeZone"), False,
        )
    return None, None, None, None, None


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
