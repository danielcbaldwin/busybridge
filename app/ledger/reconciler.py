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
from app.ledger.ingest import ingest_client_calendar, ingest_main_calendar
from app.ledger.outbox import drain_user
from app.ledger.planner import plan_for_ledger_event

logger = logging.getLogger(__name__)
UTC = timezone.utc


async def reconcile_user(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    user_email: str,
    main_google_calendar_id: str,
    client_calendars: list[dict],
    include_main: bool = True,
    drain: bool = True,
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
    out: dict = {"ingest": {}, "ingest_errors": {}, "planned": 0, "enqueued": 0, "drain": {}}

    google_id_for: dict[int, str] = {
        int(c["id"]): c["google_calendar_id"] for c in client_calendars
    }

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

    # 3. Plan affected ledger rows.  We pull the affected list out
    #    of reconcile_requests and clear it inside this run.
    affected = await _consume_affected_ledger_ids(db, user_id=user_id)
    for ledger_id in affected:
        await plan_for_ledger_event(db, ledger_event_id=ledger_id)
        out["planned"] += 1

    # 4. Diff projections, enqueue outbox ops.
    out["enqueued"] = await diff_and_enqueue_for_user(
        db,
        user_id=user_id,
        main_calendar_id=main_google_calendar_id,
        google_calendar_id_for=google_id_for,
    )
    await db.commit()

    # 5. Drain the outbox (one pass; caller may loop for retries
    #    via clock-advance + re-call).
    if drain:
        out["drain"] = await drain_user(db, google, user_id=user_id)

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
