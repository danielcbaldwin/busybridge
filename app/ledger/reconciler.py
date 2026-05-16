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


async def _sync_is_paused(db: aiosqlite.Connection, user_id: int) -> bool:
    """True when sync must not pull new state for this user.

    Two independent pause switches exist and both must be honoured:

    * per-user — ``users.sync_paused``, flipped by the circuit breaker
      or an admin pausing one account;
    * global — the ``settings`` row keyed ``sync_paused``, flipped by
      the admin "pause everything" switch and, critically, by the
      backup job to freeze writes for a consistent snapshot.

    The reconciler historically read only the per-user flag, so the
    global switch silently did nothing to the ledger engine — backups
    ran concurrently with sync.  Both flags are read from the passed
    connection (in production the single shared connection ``settings``
    also lives on).
    """
    user_row = await (await db.execute(
        "SELECT sync_paused FROM users WHERE id = ?", (user_id,),
    )).fetchone()
    if user_row and user_row["sync_paused"]:
        return True
    try:
        global_row = await (await db.execute(
            "SELECT value_plain FROM settings WHERE key = 'sync_paused'",
        )).fetchone()
    except Exception:
        # Minimal test databases may omit the settings table; absence
        # of the table means no global pause is configured.
        return False
    return bool(global_row and global_row["value_plain"] == "true")


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

    Returns a counters dict aggregating each phase.
    """
    # If sync is paused, skip ingest/planning entirely — the outbox
    # should still drain (so a paused user's pending deletes complete)
    # but we don't pull new state in.
    paused = await _sync_is_paused(db, user_id)

    out: dict = {
        "ingest": {}, "ingest_errors": {},
        "planned": 0, "enqueued": 0, "drain": {},
        "paused": paused,
    }

    # Diff/outbox needs the Google ID for any calendar a projection
    # might target — including disconnected ones we still owe a
    # delete to.  Default to the active list if the caller didn't
    # supply a wider set.
    google_id_for: dict[int, str] = {
        int(c["id"]): c["google_calendar_id"]
        for c in (all_known_client_calendars or client_calendars)
    }

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

    # 3. Plan affected ledger rows.  We pull the affected list out
    #    of reconcile_requests and clear it inside this run.
    affected = await _consume_affected_ledger_ids(db, user_id=user_id)
    for ledger_id in affected:
        await plan_for_ledger_event(db, ledger_event_id=ledger_id)
        out["planned"] += 1

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

    return out


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


async def _consume_affected_ledger_ids(
    db: aiosqlite.Connection, *, user_id: int,
) -> list[int]:
    """Pull and clear the affected list for one user."""
    row = await (await db.execute(
        "SELECT sources_json FROM reconcile_requests WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    if row is None or not row["sources_json"]:
        return []
    ids = json.loads(row["sources_json"])
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE reconcile_requests
              SET sources_json = NULL,
                  in_flight = 0,
                  last_run_at = ?
            WHERE user_id = ?""",
        (when, user_id),
    )
    await db.commit()
    # The list might contain non-int legacy junk if ingest evolves;
    # belt-and-braces filter.
    return [int(i) for i in ids if isinstance(i, int) or str(i).isdigit()]
