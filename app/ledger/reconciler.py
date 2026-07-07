"""Per-user reconciliation: ingest → plan → diff → drain.

The reconciler is the single writer per user.  External triggers
(webhook, periodic timer, manual) do not call ingest directly;
they upsert into ``reconcile_requests`` and notify the
reconciler.  Multiple notifications collapse into one run.

This module exposes :func:`reconcile_user`, which performs one
end-to-end pass synchronously.  The plumbing that drives it from
webhooks/timers will land later — for now, integration tests
call ``reconcile_user`` directly.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

import aiosqlite

from app.ledger.async_google import as_async_google
from app.ledger.diff import diff_and_enqueue_for_user
from app.ledger.google_client import GoogleClient
from app.ledger.ingest import (
    discover_orphans,
    ingest_client_calendar,
    ingest_main_calendar,
    ingest_personal_calendar,
    ingest_webcal_subscription,
)
from app.ledger.ingest.client import (
    _ingest_one_event,
    _record_affected,
    _stamp_ical_uid,
)
from app.ledger.outbox import drain_user
from app.ledger.planner import plan_for_ledger_event

logger = logging.getLogger(__name__)
UTC = timezone.utc


async def _pause_mode(
    db: aiosqlite.Connection, user_id: int,
) -> Optional[str]:
    """Return the active pause mode for a user, or ``None``.

    * ``"global"`` — the ``settings`` row keyed ``sync_paused``: a
      HARD freeze (the admin "pause everything" emergency stop).  No
      ingest, diff, or drain — nothing is written to Google.
    * ``"user"`` — ``users.sync_paused``: a SOFT pause (the circuit
      breaker, or ``cleanup_and_pause``).  Ingest is skipped, but the
      outbox still drains so staged cleanup work converges.

    A global pause outranks a per-user one.  Both flags are read from
    the passed connection (in production the single shared connection
    that ``settings`` lives on).
    """
    try:
        global_row = await (await db.execute(
            "SELECT value_plain FROM settings WHERE key = 'sync_paused'",
        )).fetchone()
    except sqlite3.OperationalError as e:
        # ONLY a missing settings table is tolerated — minimal test
        # databases omit it.  Any other failure (locked DB, disk I/O
        # error, corruption) must propagate: this read is the admin
        # "pause everything" emergency stop, and swallowing a real
        # error would silently fail OPEN — the pass would proceed to
        # ingest/diff/drain while the operator believes sync is frozen.
        if "no such table" not in str(e).lower():
            raise
        global_row = None
    if global_row and global_row["value_plain"] == "true":
        return "global"
    user_row = await (await db.execute(
        "SELECT sync_paused FROM users WHERE id = ?", (user_id,),
    )).fetchone()
    if user_row and user_row["sync_paused"]:
        return "user"
    return None


async def _sync_is_paused(db: aiosqlite.Connection, user_id: int) -> bool:
    """True when sync is paused in either mode — the plain-boolean
    view of :func:`_pause_mode`."""
    return await _pause_mode(db, user_id) is not None


async def reconcile_user(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    user_email: str,
    main_google_calendar_id: str,
    client_calendars: list[dict],
    all_known_client_calendars: Optional[list[dict]] = None,
    personal_calendars: Optional[list[dict]] = None,
    webcal_subscriptions: Optional[list[dict]] = None,
    webcal_fetch=None,
    include_main: bool = True,
    drain: bool = True,
    run_discovery: bool = False,
    dry_run: bool = False,
    now: Optional[datetime] = None,
    owned_emails: Optional["Iterable[str]"] = None,
) -> dict:
    """Run one full reconciliation pass for one user.

    Args:
        db: open aiosqlite connection.
        google: a GoogleClient (fake or real).
        user_id: the user to reconcile.
        user_email: needed by ingest to detect "self" attendees.
        main_google_calendar_id: the Google calendar ID for main.
        client_calendars: list of dicts, each with at least
            ``id`` (client_calendars.id) and
            ``google_calendar_id``.
        include_main: skip the main-ingest pass (useful for tests
            that only exercise client→main propagation).
        drain: skip the outbox drain (useful for assertions about
            queue contents before delivery).
        dry_run: ingest + plan + diff, but never deliver to Google.
            The outbox rows the diff enqueues are captured as a
            preview and then deleted (here, under the per-user
            reconcile lock) so no later pass can drain them.

    Returns a counters dict aggregating each phase.  When ``dry_run``
    is set it also carries ``preview_operations`` — the list of
    writes the pass would have sent to Google.
    """
    # Offload every Google call to a worker thread (see async_google):
    # the GoogleClient protocol is synchronous, so a blocking call
    # would otherwise freeze the event loop.  Idempotent if already
    # wrapped.
    google = as_async_google(google)

    # A dry-run never delivers: force the drain off so the captured
    # preview rows are the *only* thing the pass produces for Google.
    if dry_run:
        drain = False

    # Pause handling has two modes (see _pause_mode):
    #  * global  — a HARD freeze: skip ingest AND diff AND drain.
    #               Nothing is written to Google at all.
    #  * per-user — a SOFT pause: skip ingest, but still diff + drain
    #               so a paused user's staged cleanup converges.
    mode = await _pause_mode(db, user_id)
    if mode == "global":
        return {
            "ingest": {}, "ingest_errors": {},
            "planned": 0, "enqueued": 0, "drain": {},
            "paused": True,
        }
    paused = mode == "user"

    out: dict = {
        "ingest": {}, "ingest_errors": {},
        "planned": 0, "enqueued": 0, "drain": {},
        "paused": paused,
    }

    # Snapshot the outbox before any enqueue so a dry-run can undo
    # exactly what this pass does to it: the snapshot's high-water mark
    # identifies the rows the pass CREATES (discarded afterwards), and
    # the per-row statuses let the discard also restore any
    # PRE-EXISTING row that ``enqueue`` mutated in place (conflict
    # resurrection, supersede) — see _discard_dry_run_outbox.
    dry_run_snapshot = (
        await _snapshot_dry_run_outbox(db, user_id=user_id)
        if dry_run else {}
    )

    # Diff/outbox needs the Google ID for any calendar a projection
    # might target — including disconnected ones we still owe a
    # delete to.  Default to the active list if the caller didn't
    # supply a wider set.
    google_id_for: dict[int, str] = {
        int(c["id"]): c["google_calendar_id"]
        for c in (all_known_client_calendars or client_calendars)
    }
    # Personal calendars used to be origin writeback targets.  Keep
    # their IDs resolvable while legacy projections/outbox rows age
    # out, but new planning never writes to them.
    for c in (personal_calendars or []):
        google_id_for.setdefault(int(c["id"]), c["google_calendar_id"])

    if paused:
        # Skip ingest + plan (don't pull new state).  But DO run
        # diff+drain so admin-staged cleanup work (cleanup_and_pause
        # sets projections to absent) can converge.
        await _diff_drain_until_quiescent(
            db, google,
            user_id=user_id,
            main_calendar_id=main_google_calendar_id,
            google_calendar_id_for=google_id_for,
            drain=drain,
            now=now,
            out=out,
        )
        if dry_run:
            out["preview_operations"] = await _discard_dry_run_outbox(
                db, user_id=user_id, snapshot=dry_run_snapshot,
            )
        return out

    # 1. Ingest each client.  Failures are caught per-calendar so
    #    one bad calendar doesn't block planning the rest.  Per the
    #    plan, the per-calendar consecutive_failures counter (set by
    #    ingest itself on success/reset; bumped here on failure) is
    #    what feeds the circuit breaker.
    for cal in client_calendars:
        try:
            counters = await ingest_client_calendar(
                db, google,
                user_id=user_id,
                client_calendar_id=int(cal["id"]),
                google_calendar_id=cal["google_calendar_id"],
                user_email=user_email,
                owned_emails=owned_emails,
            )
            out["ingest"][f"client:{cal['id']}"] = counters
        except Exception as e:
            logger.warning(
                "client ingest failed user_id=%s client=%s: %s",
                user_id, cal["id"], e,
            )
            await _bump_failure(db, client_calendar_id=int(cal["id"]), error=str(e))
            out["ingest_errors"][f"client:{cal['id']}"] = str(e)

    # 1b. Ingest each personal calendar (same Google API surface).
    for cal in (personal_calendars or []):
        try:
            out["ingest"][f"personal:{cal['id']}"] = await ingest_personal_calendar(
                db, google,
                user_id=user_id,
                personal_calendar_id=int(cal["id"]),
                google_calendar_id=cal["google_calendar_id"],
                user_email=user_email,
                owned_emails=owned_emails,
            )
        except Exception as e:
            logger.warning(
                "personal ingest failed user_id=%s personal=%s: %s",
                user_id, cal["id"], e,
            )
            await _bump_failure(db, client_calendar_id=int(cal["id"]), error=str(e))
            out["ingest_errors"][f"personal:{cal['id']}"] = str(e)

    # 1c. Poll each webcal subscription (no Google API; uses the
    #     fetch hook supplied by the caller).
    for sub in (webcal_subscriptions or []):
        if webcal_fetch is None:
            logger.warning(
                "webcal subscription %s skipped: no fetch hook supplied",
                sub["id"],
            )
            continue
        try:
            out["ingest"][f"webcal:{sub['id']}"] = await ingest_webcal_subscription(
                db,
                user_id=user_id,
                subscription_id=int(sub["id"]),
                url=sub["url"],
                fetch=webcal_fetch,
            )
        except Exception as e:
            logger.warning(
                "webcal ingest failed user_id=%s sub=%s: %s",
                user_id, sub["id"], e,
            )
            out["ingest_errors"][f"webcal:{sub['id']}"] = str(e)

    # 2. Ingest main.
    if include_main:
        try:
            out["ingest"]["main"] = await ingest_main_calendar(
                db, google,
                user_id=user_id,
                google_main_calendar_id=main_google_calendar_id,
                user_email=user_email,
                owned_emails=owned_emails,
            )
        except Exception as e:
            logger.warning("main ingest failed user_id=%s: %s", user_id, e)
            await _bump_failure(db, user_id=user_id, error=str(e))
            out["ingest_errors"]["main"] = str(e)

    # 2b. Discovery / orphan scan (caller-controlled cadence).
    if run_discovery:
        try:
            out["discovery"] = await discover_orphans(
                db, google,
                user_id=user_id,
                main_google_calendar_id=main_google_calendar_id,
                client_google_calendar_ids=google_id_for,
                dry_run=dry_run,
                now=now,
            )
        except Exception as e:
            logger.warning("discovery scan failed user_id=%s: %s", user_id, e)
            out["ingest_errors"]["discovery"] = str(e)

    # 3. Plan affected ledger rows.  The queue rows are READ first and
    #    cleared (by row id) only AFTER planning succeeds: a crash
    #    mid-plan re-plans next pass, and a re-enqueue that lands
    #    mid-pass gets a new row id that this clear leaves alone.
    affected_rows = await _read_affected_ledger_rows(db, user_id=user_id)
    for ledger_id in sorted({lid for _, lid in affected_rows}):
        await plan_for_ledger_event(db, ledger_event_id=ledger_id)
        out["planned"] += 1
    if affected_rows:
        await _clear_affected_ledger_rows(
            db, row_ids=[rid for rid, _ in affected_rows],
        )

    # 4. Diff + drain + replan fixed-point loop (see
    #    _diff_drain_until_quiescent for the convergence rationale).
    await _diff_drain_until_quiescent(
        db, google,
        user_id=user_id,
        main_calendar_id=main_google_calendar_id,
        google_calendar_id_for=google_id_for,
        drain=drain,
        now=now,
        out=out,
    )

    if dry_run:
        out["preview_operations"] = await _discard_dry_run_outbox(
            db, user_id=user_id, snapshot=dry_run_snapshot,
        )

    return out


async def audit_user(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    user_email: str,
    main_google_calendar_id: str,
    client_calendars: list[dict],
    all_known_client_calendars: Optional[list[dict]] = None,
    window_back_days: int = 1,
    window_fwd_days: int = 90,
    drain: bool = True,
    now: Optional[datetime] = None,
    owned_emails: Optional[Iterable[str]] = None,
) -> dict:
    """Source-content audit (the periodic backstop the incremental sync
    cannot provide).

    For each client source calendar, re-``list`` events over a forward
    window and re-ingest any whose content has drifted from the ledger.
    This catches the create-race (and any edit) where Google folded a
    change into a revision without advancing the sync cursor, so
    incremental sync never re-delivered it.

    Safety invariants (see ONBOARDING / the incident notes):
      * Reads with ``timeMin/timeMax`` — never advances or resets a sync
        token, so it cannot trigger the full-sync cancellation scan that
        once bloated the ledger.
      * Never infers a deletion from the windowed list (a window
        legitimately omits past / far-future events); cancelled rows are
        skipped, deletions remain incremental sync's job.
      * Passes ``skip_if_older`` so a stale-replica read can never revert
        fresher ledger data.

    Scope: client calendars only for now (personal + native-main use the
    same pattern and can be added later).
    """
    google = as_async_google(google)
    mode = await _pause_mode(db, user_id)
    if mode == "global":
        return {"audited": 0, "reingested": 0, "planned": 0,
                "enqueued": 0, "drain": {}, "paused": True}

    now = now or datetime.now(UTC)
    time_min = now - timedelta(days=window_back_days)
    time_max = now + timedelta(days=window_fwd_days)
    out: dict = {
        "audited": 0, "reingested": 0, "planned": 0, "enqueued": 0,
        "drain": {"processed": 0, "succeeded": 0, "retried": 0,
                  "failed_permanent": 0, "superseded": 0},
    }

    for cal in client_calendars:
        try:
            await _audit_client_calendar(
                db, google,
                user_id=user_id,
                user_email=user_email,
                owned_emails=owned_emails,
                client_calendar_id=int(cal["id"]),
                google_calendar_id=cal["google_calendar_id"],
                time_min=time_min,
                time_max=time_max,
                out=out,
            )
        except Exception as e:
            logger.warning(
                "content audit failed user_id=%s cal=%s: %s",
                user_id, cal["id"], e,
            )

    # Plan re-ingested rows, then diff + drain (same convergence loop as
    # reconcile_user) so a corrected source propagates to its mirrors.
    affected_rows = await _read_affected_ledger_rows(db, user_id=user_id)
    for ledger_id in sorted({lid for _, lid in affected_rows}):
        await plan_for_ledger_event(db, ledger_event_id=ledger_id)
        out["planned"] += 1
    if affected_rows:
        await _clear_affected_ledger_rows(
            db, row_ids=[rid for rid, _ in affected_rows],
        )

    google_id_for = {
        int(c["id"]): c["google_calendar_id"]
        for c in (all_known_client_calendars or client_calendars)
    }
    await _diff_drain_until_quiescent(
        db, google,
        user_id=user_id,
        main_calendar_id=main_google_calendar_id,
        google_calendar_id_for=google_id_for,
        drain=drain,
        now=now,
        out=out,
    )
    return out


async def _audit_client_calendar(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    user_email: str,
    owned_emails: Optional[Iterable[str]] = None,
    client_calendar_id: int,
    google_calendar_id: str,
    time_min: datetime,
    time_max: datetime,
    out: dict,
) -> None:
    """List one client calendar over the forward window and re-ingest any
    drifted event.  Pagination follows ``nextPageToken``; cancellations
    are skipped (never inferred from a windowed list)."""
    page_token: Optional[str] = None
    while True:
        resp = await google.list_events(
            google_calendar_id,
            time_min=time_min,
            time_max=time_max,
            single_events=False,
            show_deleted=False,
            page_token=page_token,
            max_results=250,
        )
        affected: list[int] = []
        for ev in resp.get("items", []):
            if ev.get("status") == "cancelled":
                continue
            out["audited"] += 1
            outcome, ledger_id = await _ingest_one_event(
                db,
                user_id=user_id,
                client_calendar_id=client_calendar_id,
                user_email=user_email,
                owned_emails=owned_emails,
                event=ev,
                skip_if_older=True,
            )
            # Backfill the cross-calendar identity on every re-listed event
            # (even unchanged ones) so this safe, token-stable audit
            # gradually populates ical_uid on rows that predate the column
            # — feeding the main_native dedup without a full resync.
            if ledger_id is not None:
                await _stamp_ical_uid(db, ledger_id, ev)
            if outcome in ("updated", "created", "rekeyed", "cancelled"):
                out["reingested"] += 1
                if ledger_id is not None:
                    affected.append(ledger_id)
        # Queue only the events that actually changed for replan — a
        # no-op audit records nothing, so plan/diff/drain stay idle.
        await _record_affected(db, user_id=user_id, ledger_ids=affected)
        await db.commit()
        page_token = resp.get("nextPageToken")
        if not page_token:
            break


async def _diff_drain_until_quiescent(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    main_calendar_id: str,
    google_calendar_id_for: dict[int, str],
    drain: bool,
    now: Optional[datetime],
    out: dict,
    max_passes: int = 3,
) -> None:
    """Run the diff → drain → replan fixed-point loop until quiescent.

    An etag-mismatch on update marks an op superseded and clears the
    projection's ``applied_ledger_version`` (asking for a replan).
    Iterating until nothing was processed AND nothing was superseded
    (which would trigger another replan) lets revert-on-drift converge
    in one pass instead of waiting for the next caller.  ``max_passes``
    is a hard cap guarding against infinite bounce; soak tests cover
    the edge cases.

    Shared by ``reconcile_user`` — both the per-user-paused branch
    (where staged cleanup still has to converge) and the normal path —
    and ``audit_user``.  Accumulates into ``out["enqueued"]`` and
    ``out["drain"]`` in place; the drain counters dict is (re)seeded
    with all five keys so callers can index them unconditionally even
    when ``drain`` is off and no drain ever runs.
    """
    counters_out = out.setdefault("drain", {})
    for key in ("processed", "succeeded", "retried",
                "failed_permanent", "superseded"):
        counters_out.setdefault(key, 0)
    for _ in range(max_passes):
        enq = await diff_and_enqueue_for_user(
            db,
            user_id=user_id,
            main_calendar_id=main_calendar_id,
            google_calendar_id_for=google_calendar_id_for,
            now=now,
        )
        out["enqueued"] += enq
        await db.commit()
        if not drain:
            break
        drain_counters = await drain_user(db, google, user_id=user_id, now=now)
        for k, v in drain_counters.items():
            counters_out[k] = counters_out.get(k, 0) + v
        # If nothing was processed AND nothing was superseded
        # (which would trigger another replan), we're done.
        if drain_counters["processed"] == 0 and drain_counters["superseded"] == 0:
            break


# The outbox columns ``enqueue`` can rewrite on a PRE-EXISTING row: the
# conflict-resurrection UPDATE touches all of them; the supersede step
# touches status + completed_at.  A dry-run snapshot captures exactly
# this set so the discard can restore a mutated row byte-for-byte.
_OUTBOX_SNAPSHOT_COLUMNS = (
    "status", "attempts", "next_attempt_at", "last_error",
    "last_http_status", "payload_json", "started_at", "completed_at",
    "ledger_version_at_enqueue", "desired_payload_hash",
)


async def _snapshot_dry_run_outbox(
    db: aiosqlite.Connection, *, user_id: int,
) -> dict[int, aiosqlite.Row]:
    """Snapshot a user's outbox rows before a dry-run pass.

    The snapshot serves two purposes in :func:`_discard_dry_run_outbox`:
    its highest id is the high-water mark separating rows the pass
    CREATES (deleted on discard) from pre-existing real work, and the
    per-row column values let the discard restore any pre-existing row
    the pass MUTATED in place.  Taken under the per-user reconcile
    lock, so it cannot race a concurrent enqueue/drain for this user.
    """
    rows = await (await db.execute(
        f"""SELECT id, {', '.join(_OUTBOX_SNAPSHOT_COLUMNS)}
              FROM outbox_operations
             WHERE user_id = ?""",
        (user_id,),
    )).fetchall()
    return {int(r["id"]): r for r in rows}


async def _discard_dry_run_outbox(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    snapshot: dict[int, aiosqlite.Row],
) -> list[dict]:
    """Capture the writes a dry-run pass staged, then undo the outbox.

    The diff step always enqueues — there is no preview mode in it —
    so a dry-run leaves real ``pending`` work behind, in two shapes:

    * NEW rows (id above the snapshot's high-water mark).  Captured
      into the preview, then deleted.
    * PRE-EXISTING rows ``enqueue`` mutated in place.  Its conflict-
      resurrection path flips a done/superseded row (same idempotency
      key re-derived) back to ``pending`` — id at or below the
      watermark, so deleting above it both omits the op from the
      preview and leaves a live pending op a later pass would drain,
      violating the dry-run guarantee.  Its supersede step can also
      knock a pre-existing pending op (real queued work) to
      ``superseded``.  Both are detected by comparing statuses against
      the snapshot — every in-place mutation changes ``status`` —
      and restored to their snapshotted column values; resurrected
      rows (now pending) additionally join the preview, since they are
      writes the pass would have sent.

    This runs under the per-user reconcile lock, so no drain can claim
    a row between the capture and the delete/restore.
    """
    watermark = max(snapshot, default=0)
    changed = await (await db.execute(
        "SELECT id, status FROM outbox_operations WHERE user_id = ? AND id <= ?",
        (user_id, watermark),
    )).fetchall()
    changed_ids = [
        int(r["id"]) for r in changed
        if r["status"] != snapshot[int(r["id"])]["status"]
    ]
    # Resurrected = mutated back to pending (in_flight is impossible
    # here: a dry-run forces the drain off, so nothing claims rows).
    resurrected_ids = [
        int(r["id"]) for r in changed
        if r["status"] == "pending"
        and snapshot[int(r["id"])]["status"] != "pending"
    ]

    # Preview: everything this pass would have sent — new rows plus
    # resurrected pre-existing ones, in queue (id) order.
    id_marks = ",".join("?" * len(resurrected_ids))
    rows = await (await db.execute(
        f"""SELECT o.id, o.operation, o.target_google_calendar_id,
                   o.payload_json, e.summary, e.canonical_uid,
                   p.target_kind
              FROM outbox_operations o
              JOIN ledger_projections p ON p.id = o.projection_id
              JOIN ledger_events e ON e.id = p.ledger_event_id
             WHERE o.user_id = ?
               AND (o.id > ?{f' OR o.id IN ({id_marks})' if resurrected_ids else ''})
             ORDER BY o.id""",
        (user_id, watermark, *resurrected_ids),
    )).fetchall()
    preview: list[dict] = []
    for r in rows:
        payload = json.loads(r["payload_json"]) if r["payload_json"] else None
        preview.append({
            "outbox_id": int(r["id"]),
            "operation": r["operation"],
            "target_calendar": r["target_google_calendar_id"],
            "target_kind": r["target_kind"],
            "event_summary": r["summary"],
            "canonical_uid": r["canonical_uid"],
            "would_send_summary": (payload or {}).get("summary"),
        })

    await db.execute(
        "DELETE FROM outbox_operations WHERE user_id = ? AND id > ?",
        (user_id, watermark),
    )
    if changed_ids:
        set_clause = ", ".join(f"{c} = ?" for c in _OUTBOX_SNAPSHOT_COLUMNS)
        await db.executemany(
            f"UPDATE outbox_operations SET {set_clause} WHERE id = ?",
            [
                tuple(snapshot[i][c] for c in _OUTBOX_SNAPSHOT_COLUMNS) + (i,)
                for i in changed_ids
            ],
        )
    await db.commit()
    return preview


async def _bump_failure(
    db: aiosqlite.Connection,
    *,
    client_calendar_id: Optional[int] = None,
    user_id: Optional[int] = None,
    error: str,
) -> None:
    """Record an ingest failure on the matching sync_state row.

    Either ``client_calendar_id`` (per-client state) or ``user_id``
    (main-calendar state) must be supplied.
    """
    when = datetime.now(UTC).isoformat()
    if client_calendar_id is not None:
        await db.execute(
            """UPDATE calendar_sync_state
                  SET consecutive_failures = consecutive_failures + 1,
                      last_error = ?,
                      last_incremental_sync = ?
                WHERE client_calendar_id = ?""",
            (error[:1000], when, client_calendar_id),
        )
    elif user_id is not None:
        await db.execute(
            """UPDATE main_calendar_sync_state
                  SET consecutive_failures = consecutive_failures + 1,
                      last_error = ?,
                      last_incremental_sync = ?
                WHERE user_id = ?""",
            (error[:1000], when, user_id),
        )
    await db.commit()


async def _read_affected_ledger_rows(
    db: aiosqlite.Connection, *, user_id: int,
) -> list[tuple[int, int]]:
    """The replan queue for one user as ``(row_id, ledger_event_id)``
    pairs.

    Read-only: the rows are deleted by :func:`_clear_affected_ledger_rows`
    only AFTER planning succeeds, and only by row id — so a row
    appended mid-pass (a re-enqueue of an event being planned right
    now) keeps a fresh id, is not in the cleared set, and survives to
    the next reconcile.
    """
    rows = await (await db.execute(
        """SELECT id, ledger_event_id FROM affected_ledger_events
            WHERE user_id = ? ORDER BY id""",
        (user_id,),
    )).fetchall()
    return [(int(r["id"]), int(r["ledger_event_id"])) for r in rows]


# SQLite's default host-parameter ceiling is 999 (SQLITE_MAX_VARIABLE_
# NUMBER; raised to 32766 in 3.32+, but don't rely on the build).  A
# reconcile pass rarely queues more than a few hundred affected rows —
# a full-sync burst is the realistic worst case — so chunking below the
# classic limit keeps every DELETE valid on any SQLite while still
# issuing one statement per ~500 rows instead of one per row.
_DELETE_CHUNK = 500


async def _clear_affected_ledger_rows(
    db: aiosqlite.Connection, *, row_ids: list[int],
) -> None:
    """Delete affected-event rows by id once their planning succeeded."""
    for start in range(0, len(row_ids), _DELETE_CHUNK):
        chunk = row_ids[start:start + _DELETE_CHUNK]
        marks = ",".join("?" * len(chunk))
        await db.execute(
            f"DELETE FROM affected_ledger_events WHERE id IN ({marks})",
            [int(rid) for rid in chunk],
        )
    await db.commit()
