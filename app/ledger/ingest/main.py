"""Main calendar ingest.

Same shape as the client ingest, with three differences:

1. Skip events whose ID matches one of our projections — those
   are our own writes.
2. Native main events (NOT projections of any other source) get
   ``source_type='main_native'``.
3. Cancelled-on-main events that ARE projections of another
   source flip the parent ledger row's
   ``user_intentionally_deleted`` flag, so the planner suppresses
   re-creation on every target.

Edit-back-propagation: a user edit to one of
our managed copies on the main calendar is classified by
:func:`_maybe_apply_main_edit_back` and either propagated to the
source event or reverted as drift.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Iterable, Optional

import aiosqlite

from app.config import get_settings
from app.ledger.async_google import as_async_google
from app.ledger.google_client import GoogleClient
from app.ledger.identity import (
    canonical_uid_for_instance,
    canonical_uid_main_native,
    derive_instance_google_event_id,
    is_managed_google_event_id,
)
from app.ledger.recurrence import (
    looks_finite_recurrence,
    occurrence_in_series,
    parse_instant,
    parse_recurrence_lines,
    series_dtstart,
    strip_r_suffix,
)
from app.ledger.payload import (
    render_payload,
    strip_copy_summary_prefixes,
    strip_full_copy_metadata,
)
from app.ledger.ingest.client import (
    _canonical_instant,
    _content_hash,
    _content_hash_from_row,
    _extract_event_fields,
    _ingest_instance,
    _instance_original_start,
    _is_recurring_parent,
    _record_affected,
    _stamp_ical_uid,
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
    owned_emails: Optional[Iterable[str]] = None,
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
        "cancelled_native": 0, "failed": 0,
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
            try:
                outcome, ledger_id = await _ingest_one_main_event(
                    db,
                    user_id=user_id,
                    user_email=user_email,
                    owned_emails=owned_emails,
                    event=event,
                )
            except Exception:
                # Isolate per-event failures so a single poison event
                # cannot abort the pass and strand the main sync token
                # (which would silently stop ingesting main-side edits
                # and native events).  Log loudly, skip, keep going.
                logger.exception(
                    "main ingest: skipping event %s for user_id=%s after error",
                    event.get("id"), user_id,
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
            return await _ingest_one_main_event(
                db,
                user_id=user_id,
                user_email=user_email,
                owned_emails=owned_emails,
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
    owned_emails: Optional[Iterable[str]] = None,
) -> tuple[str, Optional[int]]:
    event_id = event["id"]
    status = event.get("status", "confirmed")

    # Was this our write?  Two checks: deterministic ID prefix and
    # exact projection lookup.
    is_our_write = is_managed_google_event_id(event_id)
    proj_match = await (await db.execute(
        """SELECT p.id AS projection_id, p.ledger_event_id,
                  p.google_etag, p.applied_payload_hash,
                  e.status AS ledger_status
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE p.google_event_id = ? AND e.user_id = ?
            LIMIT 1""",
        (event_id, user_id),
    )).fetchone()
    if proj_match is not None or is_our_write:
        # A 'released' event is retired from sync — its copy is frozen on
        # main on purpose.  Leave it fully alone: do not revert drift,
        # propagate a main-side edit, or (critically) arm a source delete
        # if the user removes the frozen copy.  Hands off.
        if proj_match is not None and proj_match["ledger_status"] == "released":
            return "our_writes_skipped", None
        if proj_match is not None and status == "cancelled":
            ledger_id = int(proj_match["ledger_event_id"])
            matched = await (await db.execute(
                """SELECT parent_canonical_uid, source_type, status,
                          user_can_edit, is_recurring
                     FROM ledger_events WHERE id = ?""",
                (ledger_id,),
            )).fetchone()
            if matched is not None and matched["source_type"] in (
                "personal", "webcal",
            ):
                # Personal calendars and webcal feeds are authoritative
                # read-only sources.  Deleting a Busy copy on main (whether a
                # whole series or a single modified occurrence) is drift to
                # revert, not an instruction to suppress or delete anything
                # from the source — there is nothing to propagate back, so a
                # still-live occurrence must be re-asserted.  (Without this,
                # a live modified webcal occurrence deleted on main was
                # wrongly cancelled, silently dropping its busy block.)
                await _mark_main_drift_reverted(db, ledger_id)
                return "main_drift_reverted", ledger_id
            if matched is not None and matched["parent_canonical_uid"]:
                # A cancelled occurrence of a managed recurring copy on
                # main.  CHURN-BREAKER + _R artifact guard: if the source
                # instance row is still ACTIVE (client ingest runs first
                # each pass, so this is the source's current view), the
                # occurrence is LIVE and this cancelled exception is an
                # _R-split artifact, not a real cancellation.  Cancelling
                # it oscillated with client ingest forever (version 344)
                # and deleted real source occurrences.  Re-assert (drift
                # revert) instead — the diff re-creates the mirror copies.
                if (
                    matched["status"] == "active"
                    and matched["source_type"] == "client"
                ):
                    await _mark_main_drift_reverted(db, ledger_id)
                    return "main_drift_reverted", ledger_id
                # Source occurrence not live → a genuine removal.  Cancel
                # the mirror only.  The conservative fix (2026-06-02)
                # already suppresses the destructive source delete: never
                # delete the authoritative source via this path.
                await _mark_managed_instance_cancelled(db, ledger_id)
                return "cancelled_instance", ledger_id
            # User deleted our copy on main → flip
            # user_intentionally_deleted on the source ledger row.
            await _mark_user_intentionally_deleted(db, ledger_id)
            await _maybe_arm_organizer_source_delete(db, matched, ledger_id)
            return "user_deletes", ledger_id
        # Edit-on-main detection: the user has changed our copy.
        # Two outcomes:
        # * Editable event (client source, user_can_edit): propagate
        #   RSVP and/or time back to the source by bumping the
        #   ledger row.
        # * Non-editable (lock-emoji): the planner will revert
        #   automatically because the new etag invalidates our
        #   If-Match next time we try to update, AND because
        #   bumping the ledger row's version produces a fresh
        #   desired_payload_hash that re-asserts the canonical state.
        if proj_match is not None and status != "cancelled":
            matched_id = int(proj_match["ledger_event_id"])
            if await _managed_instance_orphaned_by_split(
                db, user_id=user_id, ledger_event_id=matched_id,
            ):
                # "_R" this-and-following split race: the user edited an
                # occurrence on the base bb-series during the window before BB
                # truncated its own mirror, so BB adopted a base-parented copy
                # for a date the now-truncated base no longer covers.  The
                # covering _R segment already mirrors that date, so this base
                # copy is a stale duplicate — revert it (cancel → the diff
                # deletes the stray copy), leaving the segment's regular
                # occurrence as the single correct mirror.
                await _mark_managed_instance_cancelled(db, matched_id)
                return "main_drift_reverted", matched_id
            outcome = await _maybe_apply_main_edit_back(
                db, user_email=user_email,
                owned_emails=owned_emails,
                ledger_event_id=matched_id,
                event=event,
            )
            if outcome is not None:
                return outcome, matched_id
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
            owned_emails=owned_emails,
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
            owned_emails=owned_emails,
            event=event,
            parent_canonical=parent_canonical,
            source_type="main_native",
            source_calendar_id=None,
        )

    # A ``<base>_R<date>`` event is Google's "this and following" split —
    # an ADDITIVE new series segment, not a replacement.  Ingest it as its
    # own recurring series (normal upsert below) rather than re-keying the
    # base onto it; modified instances stay parented to the segment their
    # ``recurringEventId`` names.  See the client-ingest note and
    # test_moved_instance_survives_this_and_following.

    # A native main event we haven't seen before, or seen previously.
    canonical = canonical_uid_main_native(user_id, event_id)
    existing = await (await db.execute(
        """SELECT * FROM ledger_events
            WHERE user_id = ? AND canonical_uid = ?""",
        (user_id, canonical),
    )).fetchone()

    # A 'released' event was retired from sync by retention (frozen on the
    # calendars on purpose); never re-ingest, update, or un-release it.
    if existing is not None and existing["status"] == "released":
        return "skipped", None

    if status == "cancelled":
        if existing is None:
            return "skipped", None
        if existing["status"] == "cancelled":
            return "skipped", int(existing["id"])
        await _mark_native_cancelled(db, ledger_event_id=int(existing["id"]))
        return "cancelled_native", int(existing["id"])

    fields = _extract_event_fields(
        event, user_email=user_email, owned_emails=owned_emails,
    )
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
                   conference_data_json, source_html_link,
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
                fields["conference_data_json"], fields["source_html_link"],
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
                  conference_data_json = ?, source_html_link = ?,
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
            fields["conference_data_json"], fields["source_html_link"],
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
    owned_emails: Optional[Iterable[str]] = None,
) -> tuple[str, Optional[int]]:
    """Handle an instance of one of our managed recurring main copies.

    The user moved, edited or cancelled a single occurrence of a
    recurring event we mirror onto the main calendar.  Map it back to
    the SOURCE series' ledger row and upsert a canonical
    source-parented instance row, so the existing planner / diff
    propagate the change to the source occurrence and every peer copy.

    A move/edit for a writable client source arms an origin-writeback
    patch; a cancellation arms a destructive delete of that one source
    occurrence.  Personal sources are read-only and never get those
    source-write flags.  Never mints a ``main_native`` row, and never
    touches the parent series.
    """
    status = event.get("status", "confirmed")

    # Map the managed parent id back to the SOURCE series ledger row
    # via its 'main' projection.  The bb-id the instance carries as
    # recurringEventId is that projection's google_event_id.
    parent = await (await db.execute(
        """SELECT e.id AS ledger_event_id, e.canonical_uid,
                  e.source_type, e.source_calendar_id, e.source_event_id,
                  e.is_recurring, e.parent_canonical_uid,
                  e.user_can_edit, e.organizer_email, e.attendees_json,
                  e.user_rsvp_status, e.summary, e.description, e.location,
                  e.start_at, e.end_at, e.start_timezone, e.end_timezone,
                  e.is_all_day, e.show_as, e.visibility, e.color_id,
                  e.conference_data_json, e.source_html_link
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

    # The origin op addresses the source occurrence by id.  No
    # exception exists on the source yet, but Google addresses an
    # instance as ``<series>_<stamp>`` whether or not one has been
    # materialised — derive it from the source series id so the op has
    # a target (and so a later source-side ingest of that same
    # exception keys to the very same row).
    source_event_id: Optional[str] = None
    src_series_id = parent["source_event_id"]
    if parent["source_type"] == "client" and src_series_id:
        original_start, instance_is_all_day = _instance_original_start(event)
        source_event_id = derive_instance_google_event_id(
            src_series_id, original_start, instance_is_all_day,
        )

    if status == "cancelled":
        # CHURN-BREAKER + _R artifact guard.
        #
        # A cancelled instance-exception of our managed recurring copy on
        # main is, from BusyBridge's current state alone, ambiguous between
        # (a) the user genuinely cancelling that occurrence on main and
        # (b) an artifact of the "_R" this-and-following split that
        # BusyBridge's own machinery generates for a LIVE occurrence. Case
        # (b) looped forever (version 344 on one row) and, before the
        # conservative fix, destructively deleted ~170 real source
        # occurrences.
        #
        # Client ingest runs BEFORE main ingest in every reconcile pass, so
        # an ACTIVE source instance row for this occurrence reflects the
        # source's current view: the occurrence is LIVE.  A cancelled
        # exception on main for a live source occurrence is therefore an
        # artifact, not a real cancellation.  Don't cancel it (which would
        # loop); instead re-assert the mirror — clear applied state on any
        # non-present projection so the diff re-creates the BB-deleted
        # copies (status=confirmed revive).  In steady state every
        # projection is already present, so this is a no-op (no churn).
        original_start, _iad = _instance_original_start(event)
        inst_canonical = canonical_uid_for_instance(
            parent["canonical_uid"], original_start,
        )
        live = await (await db.execute(
            "SELECT id FROM ledger_events "
            "WHERE user_id = ? AND canonical_uid = ? AND status = 'active' "
            "LIMIT 1",
            (user_id, inst_canonical),
        )).fetchone()
        if live is not None:
            # Re-assert (drift revert): bump the source row so the diff
            # re-creates the BB-deleted mirror copies (status=confirmed
            # revive), the same mechanism used for any reverted main-side
            # edit.  Converges in a pass or two; a no-op once present.
            await _mark_main_drift_reverted(db, int(live["id"]))
            return "main_drift_reverted", int(live["id"])

        # No live source occurrence — a genuine removal.  Cancel the mirror
        # (a sticky cancelled instance row).  The CONSERVATIVE SAFETY FIX
        # (data-loss incident, 2026-06-02) means we still do NOT arm a
        # destructive delete of the real source occurrence: BusyBridge must
        # never reach over and delete the authoritative source calendar via
        # this path.  Only the mirror copies are removed.
        outcome, ledger_id = await _ingest_instance(
            db,
            user_id=user_id,
            user_email=user_email,
            owned_emails=owned_emails,
            event=event,
            parent_canonical=parent["canonical_uid"],
            source_type=parent["source_type"],
            source_calendar_id=parent["source_calendar_id"],
            source_event_id=source_event_id,
        )
        return outcome, ledger_id

    # Move / edit.  The dragged copy is opaque about edit-rights: its
    # rendered shape carries neither the real organizer nor
    # guestsCanModify.  Take edit-rights / organizer / attendees from
    # the SOURCE series, and mirror _maybe_apply_main_edit_back's
    # per-category policy EXACTLY:
    #
    #   * RSVP   — the user's own attendee response; always writable
    #              back to a client source, editable or not.
    #   * time   — written back only for an editable client source.
    #   * detail — likewise (summary/description/location).
    #
    # A disallowed category is NOT stored: the instance row keeps the
    # canonical (source) value for it, and the main projection is
    # explicitly un-converged so the diff re-delivers the canonical
    # occurrence and reverts the drift on main — the parent path's
    # revert semantics.  (Pre-fix this path stored the RENDERED copy's
    # fields with no gating: declining one occurrence of a locked
    # meeting wrote the lock-emoji title into the REAL source event —
    # and the main copy then rendered a double lock — while dragging
    # one occurrence of a locked meeting MOVED the real source meeting.)
    original_start, instance_is_all_day = _instance_original_start(event)
    inst_canonical = canonical_uid_for_instance(
        parent["canonical_uid"], original_start,
    )
    existing = await (await db.execute(
        """SELECT * FROM ledger_events
            WHERE user_id = ? AND canonical_uid = ?""",
        (user_id, inst_canonical),
    )).fetchone()

    # Baseline = what this occurrence canonically looks like: the
    # already-ingested instance row when one exists, else the parent
    # series' values at the occurrence's own slot.
    if existing is not None:
        base = existing
        base_start, base_end = existing["start_at"], existing["end_at"]
        base_all_day = bool(existing["is_all_day"])
    else:
        base = parent
        base_start, base_end = _occurrence_slot(
            parent, original_start, instance_is_all_day,
        )
        base_all_day = instance_is_all_day

    # Classify the edit against the baseline.  The copy's summary /
    # description carry rendered artifacts (lock prefix, guest-list
    # footer, managed tag) — strip them before comparing or storing,
    # so they can never leak into the ledger or back onto the source.
    new_rsvp = _extract_self_rsvp(event, user_email, owned_emails=owned_emails)
    new_start, new_end, new_start_tz, new_end_tz, new_all_day = (
        _extract_start_end(event)
    )
    new_is_all_day = bool(new_all_day)
    new_summary = strip_copy_summary_prefixes(event.get("summary"))
    new_description = strip_full_copy_metadata(event.get("description"))
    new_location = event.get("location")

    rsvp_changed = (
        new_rsvp is not None and new_rsvp != base["user_rsvp_status"]
    )
    time_changed = (
        new_start is not None
        and (
            _canonical_instant(new_start, new_is_all_day)
            != _canonical_instant(base_start, base_all_day)
            or _canonical_instant(new_end, new_is_all_day)
            != _canonical_instant(base_end, base_all_day)
        )
    )
    detail_changed = (
        (new_summary or "") != (base["summary"] or "")
        or (new_description or "") != (base["description"] or "")
        or (new_location or "") != (base["location"] or "")
    )
    if not (rsvp_changed or time_changed or detail_changed):
        # Nothing material — our own rendered copy read back.
        return "our_writes_skipped", None

    # Same gates as the parent path: RSVP for any client source;
    # time/detail only when the user can edit the source event.
    # Personal and webcal sources are read-only — nothing propagates,
    # and origin_writeback_pending must NEVER be armed for them.
    source_is_client = parent["source_type"] == "client"
    user_can_edit = bool(parent["user_can_edit"])
    apply_rsvp = rsvp_changed and source_is_client
    apply_time = time_changed and user_can_edit and source_is_client
    apply_detail = detail_changed and user_can_edit and source_is_client
    propagating = apply_rsvp or apply_time or apply_detail

    fields = _extract_event_fields(
        event, user_email=user_email, owned_emails=owned_emails,
    )
    fields["user_can_edit"] = user_can_edit
    fields["organizer_email"] = parent["organizer_email"]
    fields["attendees_json"] = parent["attendees_json"]
    fields["user_rsvp_status"] = (
        new_rsvp if apply_rsvp else base["user_rsvp_status"]
    )
    if apply_detail:
        fields["summary"] = new_summary
        fields["description"] = new_description
        fields["location"] = new_location
    else:
        fields["summary"] = base["summary"]
        fields["description"] = base["description"]
        fields["location"] = base["location"]
    if not apply_time:
        fields["start_at"] = base_start
        fields["end_at"] = base_end
        fields["start_timezone"] = base["start_timezone"]
        fields["end_timezone"] = base["end_timezone"]
        fields["is_all_day"] = base_all_day
    # Cosmetic fields the rendered copy cannot speak for — take them
    # from the source row so a later source-side ingest of the same
    # occurrence hashes identically (no churn), and so the main copy's
    # own htmlLink never overwrites the source's.
    for col in (
        "show_as", "visibility", "color_id",
        "conference_data_json", "source_html_link",
    ):
        fields[col] = base[col]

    outcome, ledger_id = await _ingest_instance(
        db,
        user_id=user_id,
        user_email=user_email,
        owned_emails=owned_emails,
        event=event,
        parent_canonical=parent["canonical_uid"],
        source_type=parent["source_type"],
        source_calendar_id=parent["source_calendar_id"],
        source_event_id=source_event_id,
        fields=fields,
    )

    # Disallowed drift on an EXISTING row stores values identical to
    # the baseline — a content-identical replan the planner now skips
    # (self-write-echo fix) — so the dragged copy on main would never
    # be re-delivered.  Signal the revert explicitly, exactly like the
    # parent path's disallowed-edit handling.  A NEW row needs no
    # signal: its projections are freshly created (NULL applied hash)
    # and diverge on their own.
    unpropagated_drift = (
        (time_changed and not apply_time)
        or (detail_changed and not apply_detail)
        or (rsvp_changed and not apply_rsvp)
    )
    if unpropagated_drift and existing is not None:
        await _mark_main_drift_reverted(db, int(existing["id"]))

    # The change was made on main; the source event does not have it.
    # Flag the row so the origin-writeback patch fires even though the
    # instance's writeback projection is brand new (NULL applied hash).
    # Only on a real create/update — a no-op re-ingest must not re-arm
    # a writeback that already drained — and only when a category is
    # actually being propagated (``propagating`` is only ever true for
    # a CLIENT source: personal/webcal rows must never carry the flag,
    # because no writeback ever fires for them to clear it).
    if (
        ledger_id is not None
        and outcome in ("created", "updated")
        and propagating
    ):
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
    owned_emails: Optional[Iterable[str]] = None,
) -> Optional[str]:
    """The user edited our copy of an event on the main calendar.
    Classify the edit and either propagate it to the source event or
    revert it.

    Three edit categories, each routed per source type:

    * RSVP — propagated for client-sourced events.  A user may set
      their own response on a client event, editable or not; the
      planner's origin writeback projection writes it back to the
      source via an events.patch.
    * Time (start/end) — propagated for an editable client-sourced
      event; reverted otherwise (personal calendars and webcal feeds
      are read-only; a locked event the user is not allowed to move).
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
    # Join the source calendar (color + label) so the canonical render
    # below matches the body we actually wrote to main — otherwise our
    # own footer ("Source: …") reads as a user edit and churns.  The
    # joins must mirror planner.py / diff.py exactly: client/personal go
    # to client_calendars; webcal goes to webcal_subscriptions (for the
    # display_prefix that's prepended to the title AND used as the
    # Source: label) plus an optional second join to client_calendars
    # for the placement target (color + Placement: footer line).
    ledger = await (await db.execute(
        """SELECT e.*,
                  COALESCE(
                      cc.display_name,
                      NULLIF(ws.display_prefix, '')
                  ) AS source_label,
                  CASE
                    WHEN e.source_type IN ('client', 'personal')
                      THEN cc.color_id
                    WHEN e.source_type = 'webcal'
                      AND ws.placement_kind = 'client'
                      THEN cc_placement.color_id
                    ELSE NULL
                  END AS calendar_color_id,
                  CASE
                    WHEN e.source_type = 'webcal'
                      AND ws.placement_kind = 'client'
                      THEN cc_placement.display_name
                    ELSE NULL
                  END AS placement_label
             FROM ledger_events e
             LEFT JOIN client_calendars cc
                    ON cc.id = e.source_calendar_id
                   AND e.source_type IN ('client', 'personal')
             LEFT JOIN webcal_subscriptions ws
                    ON ws.id = e.source_calendar_id
                   AND e.source_type = 'webcal'
             LEFT JOIN client_calendars cc_placement
                    ON cc_placement.id = ws.placement_client_calendar_id
                   AND cc_placement.is_active = 1
            WHERE e.id = ?""",
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

    new_rsvp = _extract_self_rsvp(event, user_email, owned_emails=owned_emails)
    new_start, new_end, new_start_tz, new_end_tz, is_all_day = (
        _extract_start_end(event)
    )

    source_type = ledger["source_type"]
    user_can_edit = bool(ledger["user_can_edit"])

    rsvp_changed = (
        new_rsvp is not None and new_rsvp != ledger["user_rsvp_status"]
    )
    # Compare canonical UTC instants, not raw ISO strings: Google rewrites
    # the offset on ``start.dateTime`` to match the ``timeZone`` we sent
    # (e.g. "11:35-04:00" submitted with timeZone=America/Los_Angeles
    # comes back as "08:35-07:00" — same instant, different string).
    # A raw string compare flagged every re-ingest as drift, bumping
    # version and looping the planner → write → webhook cycle.
    new_is_all_day = bool(is_all_day)
    new_start_inst = _canonical_instant(new_start, new_is_all_day)
    new_end_inst = _canonical_instant(new_end, new_is_all_day)
    old_is_all_day = bool(ledger["is_all_day"])
    old_start_inst = _canonical_instant(ledger["start_at"], old_is_all_day)
    old_end_inst = _canonical_instant(ledger["end_at"], old_is_all_day)
    time_changed = (
        new_start is not None
        and (new_start_inst != old_start_inst or new_end_inst != old_end_inst)
    )
    detail_changed = _detail_differs(event, canonical)

    if not (rsvp_changed or time_changed or detail_changed):
        return "our_writes_skipped"

    # Which categories may be written back to this source type.
    rsvp_writeback = source_type == "client"
    time_writeback = source_type == "client"
    detail_writeback = source_type == "client"

    apply_rsvp = rsvp_changed and rsvp_writeback
    apply_time = time_changed and user_can_edit and time_writeback
    apply_detail = detail_changed and user_can_edit and detail_writeback

    when = datetime.now(UTC).isoformat()
    # When ANY category is being propagated (rsvp/time/detail), flag the
    # ledger row so the diff's origin-writeback patch fires.  Without
    # this flag set, the diff treats a desired_hash bump as a
    # source-ingest echo and snaps a baseline — which is the correct
    # default to prevent the source-clobber regression
    # (test_rsvp_only_does_not_clobber_source_on_source_side_change).
    propagating = apply_rsvp or apply_time or apply_detail
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
                  origin_writeback_pending =
                      CASE WHEN ? THEN 1 ELSE origin_writeback_pending END,
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
            apply_detail, strip_full_copy_metadata(event.get("description")),
            apply_detail, event.get("location"),
            propagating,
            when, ledger_event_id,
        ),
    )
    # The main copy now differs from its canonical render (whether the
    # edit is being propagated, partially propagated, or reverted), so
    # it must be re-delivered.  Signal that EXPLICITLY by un-converging
    # the main projection: the planner skips content-identical replans,
    # so on the revert path (nothing applied → ledger content unchanged
    # → hash unchanged) the version bump above alone would never
    # diverge the row and the drifted copy would stay wrong forever.
    # When the edit is being PROPAGATED the origin-writeback projection
    # is un-converged too, so the diff re-evaluates it while the
    # pending flag is armed — either patching the source or, when the
    # rendered writeback is hash-identical (the edit netted out),
    # taking the converged no-op path that clears the flag.
    await db.execute(
        """UPDATE ledger_projections
              SET applied_ledger_version = NULL,
                  updated_at = ?
            WHERE ledger_event_id = ?
              AND (target_kind = 'main'
                   OR (? AND target_kind = 'client'
                       AND target_calendar_id =
                           (SELECT source_calendar_id FROM ledger_events
                             WHERE id = ?)))""",
        (when, ledger_event_id, propagating, ledger_event_id),
    )
    if propagating:
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


def _extract_self_rsvp(
    event: dict,
    user_email: str,
    *,
    owned_emails: Optional[Iterable[str]] = None,
) -> Optional[str]:
    """The user's RSVP on an event, matched against any of their owned
    identities (home + every connected OAuth account)."""
    from app.ledger.ingest.client import _normalise_owned_emails
    owned = _normalise_owned_emails(user_email, owned_emails)
    for att in (event.get("attendees") or []):
        if att.get("self") or att.get("email", "").lower() in owned:
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


def _occurrence_slot(
    parent, original_start: str, is_all_day: bool,
) -> tuple[str, str]:
    """The canonical ``(start_at, end_at)`` of one un-modified
    occurrence of ``parent``: the occurrence's original start plus the
    series' own duration.  Used as the revert baseline when a main-side
    instance edit's time category is not allowed to propagate.
    Falls back to ``original_start`` for both ends when the parent's
    stored times are unparsable (change detection still works — any
    moved time differs from the slot)."""
    start_dt = parse_instant(original_start, is_all_day=is_all_day)
    parent_all_day = bool(parent["is_all_day"])
    p_start = parse_instant(parent["start_at"], is_all_day=parent_all_day)
    p_end = parse_instant(parent["end_at"], is_all_day=parent_all_day)
    if start_dt is None or p_start is None or p_end is None:
        return original_start, original_start
    end_dt = start_dt + (p_end - p_start)
    if is_all_day:
        return original_start[:10], end_dt.date().isoformat()
    return (
        original_start,
        end_dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


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


async def _maybe_arm_organizer_source_delete(
    db: aiosqlite.Connection, row, ledger_event_id: int,
) -> None:
    """Phase-1 organizer-delete propagation (DELETE_PROPAGATION_PLAN.md).

    The user deleted our managed copy of an event on main (the caller already
    flagged ``user_intentionally_deleted``).  When that event is a NON-recurring
    CLIENT event the user can edit (organizer / guestsCanModify / solo), and
    delete-propagation is enabled, arm a destructive delete of the real source
    event so it is removed at the source too — mirroring how an RSVP/edit on
    main already writes back.

    Safety: default mode ``"off"`` never propagates; ``"shadow"`` only logs.
    Recurring events are NEVER armed here — the per-occurrence / "_R" case
    stays disarmed pending the Layer-1/2 work (it is the path that caused the
    2026-06-02 data loss).  The destructive op itself stays gated in
    ``diff._decide`` on ``source_delete_pending`` + an origin-writeback
    projection, so arming here is the only switch.
    """
    if not (
        row["source_type"] == "client"
        and row["user_can_edit"]
        and not row["is_recurring"]
    ):
        return
    mode = getattr(get_settings(), "delete_propagation_mode", "off")
    if mode == "on":
        await db.execute(
            "UPDATE ledger_events SET source_delete_pending = 1 WHERE id = ?",
            (ledger_event_id,),
        )
    elif mode == "shadow":
        logger.info(
            "delete-propagation [shadow]: WOULD delete the source event for "
            "ledger_event id=%s (non-recurring client event the organizer "
            "deleted on main); set DELETE_PROPAGATION_MODE=on to enable.",
            ledger_event_id,
        )


async def _mark_main_drift_reverted(
    db: aiosqlite.Connection, ledger_event_id: int,
) -> None:
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE ledger_events
              SET version = version + 1,
                  updated_at = ?
            WHERE id = ?""",
        (when, ledger_event_id),
    )
    # Re-assertion is signalled EXPLICITLY by un-converging the main
    # projection (mirroring client ingest's drift revert), not by the
    # version bump above: the planner deliberately skips replans whose
    # desired state/hash are unchanged, so a bare bump would no longer
    # diverge the row and the drifted copy would never be re-delivered.
    # Keeping google_event_id: a MOVED copy heals via events.update; a
    # DELETED copy's update 404s and the outbox 404 handler clears the
    # id and replans into a re-create.
    await db.execute(
        """UPDATE ledger_projections
              SET applied_ledger_version = NULL,
                  updated_at = ?
            WHERE ledger_event_id = ? AND target_kind = 'main'""",
        (when, ledger_event_id),
    )


async def _mark_managed_instance_cancelled(
    db: aiosqlite.Connection, ledger_event_id: int,
) -> None:
    """Cancel one occurrence (an instance ledger row) whose managed
    main copy the user deleted.

    Sets ``status='cancelled'`` — NOT ``user_intentionally_deleted``,
    which is a whole-series flag — so only this occurrence is affected.

    CONSERVATIVE SAFETY FIX (data-loss incident, 2026-06-02): this no
    longer arms ``source_delete_pending``.  A cancelled managed-copy
    occurrence on main cannot be reliably distinguished from an "_R"
    split artifact, and arming a destructive delete here deleted real
    source occurrences in a loop.  BusyBridge must not destructively
    delete the authoritative source calendar; only the mirror copies are
    removed.  The proper _R fix will restore safe propagation.
    """
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE ledger_events
              SET status = 'cancelled',
                  version = version + 1,
                  cancelled_at = ?, updated_at = ?, last_seen_at = ?
            WHERE id = ?""",
        (when, when, when, ledger_event_id),
    )


def _series_covers(parent_row, *, original_start: str, is_all_day: bool):
    """Tri-state: does ``parent_row``'s live RRULE cover ``original_start``?"""
    return occurrence_in_series(
        parse_recurrence_lines(parent_row["recurrence_rule_json"]),
        series_dtstart(
            parent_row["start_at"], parent_row["start_timezone"],
            is_all_day=is_all_day,
        ),
        parse_instant(original_start, is_all_day=is_all_day),
        is_all_day=is_all_day,
    )


async def _managed_instance_orphaned_by_split(
    db: aiosqlite.Connection, *, user_id: int, ledger_event_id: int,
) -> bool:
    """True when a managed recurring INSTANCE row is a stale duplicate left by
    an "_R" this-and-following split race, and is safe to revert.

    The race: a main-side edit landed on the base bb-series before BB truncated
    it, so BB minted a base-parented instance for a date the (now
    ``UNTIL``-truncated) base no longer contains.  The covering ``_R`` segment
    mirrors that date independently, so the base copy is a stale duplicate.

    Reverting cancels a mirror copy, so we demand POSITIVE proof, not merely
    "the parent stopped covering it":

      1. the parent series' live RRULE definitively does NOT cover the
         occurrence's original start, AND
      2. a DIFFERENT live segment of the SAME ``_R`` family (same base id after
         stripping ``_R<stamp>`` suffixes) definitively DOES cover it.

    Requiring (2) makes the check robust to a coverage-probe that wrongly reads
    a covered occurrence as uncovered (e.g. a timezone/offset edge): the same
    misread would also make the sibling look uncovered, so no revert fires —
    we only ever cancel a copy whose date another live segment provably owns.
    The parent's recurrence must also be structurally FINITE (carry an
    ``UNTIL``/``COUNT``/``RDATE`` — a real this-and-following truncation), so an
    infinite series can never be judged "stopped covering" by a probe miss.
    Any ambiguity (indeterminate coverage, parent gone/inactive/infinite, no
    covering sibling) yields False.  Scoped to ``client`` sources.

    LIMITATIONS (all deliberately conservative — a leftover busy block, never a
    wrongly-deleted live mirror or any source change):
      * If the split ALSO moved the time-of-day, the covering segment occupies a
        different instant than the orphan's original start, so (2) finds no
        positive proof and the stale base copy is left in place.
      * If the date is owned only by a modified-instance override (its segment
        master gone), (2) — which scans segment masters — won't match.
      * A genuinely user-modified occurrence re-split out from under its segment
        is cancelled here, but that matches Google's own semantics (a
        this-and-following split cancels post-boundary overrides), so the
        source already dropped the edit.
    """
    inst = await (await db.execute(
        """SELECT parent_canonical_uid, source_type, is_all_day,
                  recurrence_instance_original_start
             FROM ledger_events WHERE id = ?""",
        (ledger_event_id,),
    )).fetchone()
    if (
        inst is None
        or inst["source_type"] != "client"
        or not inst["parent_canonical_uid"]
        or not inst["recurrence_instance_original_start"]
    ):
        return False
    original_start = inst["recurrence_instance_original_start"]

    parent = await (await db.execute(
        """SELECT id, source_event_id, recurrence_rule_json,
                  start_at, start_timezone, is_recurring, status
             FROM ledger_events
            WHERE user_id = ? AND canonical_uid = ?""",
        (user_id, inst["parent_canonical_uid"]),
    )).fetchone()
    if (
        parent is None
        or not parent["is_recurring"]
        or parent["status"] != "active"
        or not parent["source_event_id"]
    ):
        return False

    # (1) The parent must be a FINITE (truncated) series that DEFINITIVELY does
    #     not cover the occurrence.  Requiring finiteness means "stopped
    #     covering" is structural (a this-and-following UNTIL/COUNT boundary),
    #     never an infinite series misjudged by a bounded probe.
    parent_lines = parse_recurrence_lines(parent["recurrence_rule_json"])
    if parent_lines is None or not looks_finite_recurrence(parent_lines):
        return False
    if _series_covers(
        parent, original_start=original_start, is_all_day=bool(inst["is_all_day"]),
    ) is not False:
        return False

    # (2) A different live segment of the same _R family must DEFINITIVELY
    #     cover it — positive proof that the occurrence belongs elsewhere.
    base_id = strip_r_suffix(parent["source_event_id"])
    siblings = await (await db.execute(
        """SELECT id, source_event_id, recurrence_rule_json,
                  start_at, start_timezone, is_all_day
             FROM ledger_events
            WHERE user_id = ? AND source_type = 'client'
              AND is_recurring = 1 AND parent_canonical_uid IS NULL
              AND status = 'active' AND id != ?""",
        (user_id, int(parent["id"])),
    )).fetchall()
    for sib in siblings:
        if not sib["source_event_id"]:
            continue
        if strip_r_suffix(sib["source_event_id"]) != base_id:
            continue
        if _series_covers(
            sib, original_start=original_start, is_all_day=bool(sib["is_all_day"]),
        ) is True:
            return True
    return False


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
