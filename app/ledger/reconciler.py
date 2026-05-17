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
from datetime import datetime, timezone
from typing import Optional

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
    except Exception:
        # Minimal test databases may omit the settings table.
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

    # Record the outbox high-water mark before any enqueue so a
    # dry-run can identify — and discard — exactly the rows this pass
    # creates, without touching real work queued by an earlier pass.
    dry_run_watermark = (
        await _outbox_watermark(db, user_id) if dry_run else 0
    )

    # Diff/outbox needs the Google ID for any calendar a projection
    # might target — including disconnected ones we still owe a
    # delete to.  Default to the active list if the caller didn't
    # supply a wider set.
    google_id_for: dict[int, str] = {
        int(c["id"]): c["google_calendar_id"]
        for c in (all_known_client_calendars or client_calendars)
    }
    # Personal calendars are origin writeback targets too — a
    # personal-sourced event's edits patch back to the personal
    # source — so the diff must be able to resolve their Google ids.
    for c in (personal_calendars or []):
        google_id_for.setdefault(int(c["id"]), c["google_calendar_id"])

    if paused:
        # Skip ingest + plan (don't pull new state).  But DO run
        # diff+drain so admin-staged cleanup work (cleanup_and_pause
        # sets projections to absent) can converge.  The fixed-point
        # loop below handles this uniformly.
        out["drain"] = {"processed": 0, "succeeded": 0, "retried": 0,
                        "failed_permanent": 0, "superseded": 0}
        for _ in range(3):
            enq = await diff_and_enqueue_for_user(
                db, user_id=user_id,
                main_calendar_id=main_google_calendar_id,
                google_calendar_id_for=google_id_for,
                now=now,
            )
            out["enqueued"] += enq
            await db.commit()
            if not drain:
                break
            counters = await drain_user(db, google, user_id=user_id, now=now)
            for k, v in counters.items():
                out["drain"][k] = out["drain"].get(k, 0) + v
            if counters["processed"] == 0 and counters["superseded"] == 0:
                break
        if dry_run:
            out["preview_operations"] = await _discard_dry_run_outbox(
                db, user_id=user_id, watermark=dry_run_watermark,
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

    # 4. Diff + drain + replan loop.  An etag-mismatch on update
    #    marks an op superseded and clears the projection's
    #    applied_ledger_version (asking for a replan).  Iterating
    #    until quiescent lets revert-on-drift converge in one
    #    reconcile pass instead of waiting for the next caller.
    #    A hard cap (3 inner passes) guards against infinite
    #    bounce; soak tests cover the edge cases.
    max_inner_passes = 3
    out["drain"] = {"processed": 0, "succeeded": 0, "retried": 0,
                    "failed_permanent": 0, "superseded": 0}
    for _ in range(max_inner_passes):
        enq = await diff_and_enqueue_for_user(
            db,
            user_id=user_id,
            main_calendar_id=main_google_calendar_id,
            google_calendar_id_for=google_id_for,
            now=now,
        )
        out["enqueued"] += enq
        await db.commit()
        if not drain:
            break
        drain_counters = await drain_user(db, google, user_id=user_id, now=now)
        for k, v in drain_counters.items():
            out["drain"][k] = out["drain"].get(k, 0) + v
        # If nothing was processed AND nothing was superseded
        # (which would trigger another replan), we're done.
        if drain_counters["processed"] == 0 and drain_counters["superseded"] == 0:
            break

    if dry_run:
        out["preview_operations"] = await _discard_dry_run_outbox(
            db, user_id=user_id, watermark=dry_run_watermark,
        )

    return out


async def _outbox_watermark(db: aiosqlite.Connection, user_id: int) -> int:
    """Highest outbox row id for a user, or 0 when the queue is empty."""
    row = await (await db.execute(
        "SELECT COALESCE(MAX(id), 0) AS m FROM outbox_operations WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    return int(row["m"])


async def _discard_dry_run_outbox(
    db: aiosqlite.Connection, *, user_id: int, watermark: int,
) -> list[dict]:
    """Capture, then delete, the outbox rows a dry-run pass enqueued.

    The diff step always enqueues — there is no preview mode in it —
    so a dry-run leaves real ``pending`` rows behind.  Left in place a
    later reconcile would drain them to Google.  Every row above
    ``watermark`` was created by this pass (rows at or below it are
    pre-existing real work and are left untouched).  This runs under
    the per-user reconcile lock, so no drain can claim a row between
    the capture and the delete.
    """
    rows = await (await db.execute(
        """SELECT o.id, o.operation, o.target_google_calendar_id,
                  o.payload_json, e.summary, e.canonical_uid,
                  p.target_kind
             FROM outbox_operations o
             JOIN ledger_projections p ON p.id = o.projection_id
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE o.user_id = ? AND o.id > ?
            ORDER BY o.id""",
        (user_id, watermark),
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


async def _clear_affected_ledger_rows(
    db: aiosqlite.Connection, *, row_ids: list[int],
) -> None:
    """Delete affected-event rows by id once their planning succeeded."""
    for rid in row_ids:
        await db.execute(
            "DELETE FROM affected_ledger_events WHERE id = ?", (int(rid),),
        )
    await db.commit()
    await db.commit()
