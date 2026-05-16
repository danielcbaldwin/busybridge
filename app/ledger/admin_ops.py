"""Administrative operations expressed as ledger mutations.

REWRITE_PLAN.md §9 (color recolor) and §10 (cleanup / disconnect /
pause / full re-sync).  Every admin button maps to one of these
functions — no special "two-pass cleanup" path, no
prefix-sweep-versus-DB-mismatch dance.  The ledger is the truth;
the planner + outbox carry the truth to Google.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)
UTC = timezone.utc


# ---------------------------------------------------------------------------
# Color recolor (§9)
# ---------------------------------------------------------------------------
async def recolor_client_calendar(
    db: aiosqlite.Connection,
    *,
    client_calendar_id: int,
    new_color_id: Optional[str],
) -> int:
    """Change a client calendar's color and bump every ledger row
    sourced from it so the next reconcile re-renders the colorId
    on its projections.  Returns the number of rows touched."""
    when = datetime.now(UTC).isoformat()
    await db.execute(
        "UPDATE client_calendars SET color_id = ? WHERE id = ?",
        (new_color_id, client_calendar_id),
    )
    cursor = await db.execute(
        """UPDATE ledger_events
              SET color_id = ?,
                  version = version + 1,
                  updated_at = ?
            WHERE source_type IN ('client', 'personal')
              AND source_calendar_id = ?
              AND status = 'active'""",
        (new_color_id, when, client_calendar_id),
    )
    # Affected ledger rows: enqueue them for replan.  The source_type
    # filter matches the UPDATE above — client/personal calendars and
    # webcal subscriptions are numbered in separate tables, so without
    # it a colliding webcal id would replan unrelated webcal rows.
    affected = await (await db.execute(
        """SELECT id, user_id FROM ledger_events
            WHERE source_type IN ('client', 'personal')
              AND source_calendar_id = ?
              AND status = 'active'""",
        (client_calendar_id,),
    )).fetchall()
    by_user: dict[int, list[int]] = {}
    for row in affected:
        by_user.setdefault(int(row["user_id"]), []).append(int(row["id"]))
    for user_id, ids in by_user.items():
        await _append_affected(db, user_id=user_id, ledger_ids=ids)
    await db.commit()
    return cursor.rowcount or 0


# ---------------------------------------------------------------------------
# Cleanup (§10)
# ---------------------------------------------------------------------------
async def cleanup_one_calendar(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    client_calendar_id: int,
) -> dict:
    """Mark a single client calendar for full removal: every event
    sourced from it goes to ``status='cancelled'``; every
    projection targeting it goes to ``desired_state='absent'``.
    The next reconcile drains the deletes.
    """
    when = datetime.now(UTC).isoformat()

    # 1. Source-side: cancel every ledger row that came from this calendar.
    #    The source_type filter is essential: client/personal calendars
    #    and webcal subscriptions are numbered in SEPARATE tables, so a
    #    webcal subscription id can equal this client_calendar_id.
    #    Without the filter the cancel would also hit unrelated webcal
    #    events that merely share the numeric id.
    await db.execute(
        """UPDATE ledger_events
              SET status = 'cancelled',
                  version = version + 1,
                  cancelled_at = ?, updated_at = ?
            WHERE user_id = ?
              AND source_type IN ('client', 'personal')
              AND source_calendar_id = ?
              AND status = 'active'""",
        (when, when, user_id, client_calendar_id),
    )

    # 2. Target-side: any projection that targets this client must
    #    go absent regardless of source — busy blocks from other
    #    calendars also need to vanish.  The divergence the diff acts
    #    on is the payload-hash change (-> 'absent'); desired_ledger_version
    #    stays pinned to the event's real version rather than a
    #    fabricated version+1, which the next planner pass would
    #    overwrite with the real value anyway.
    await db.execute(
        """UPDATE ledger_projections
              SET desired_state = 'absent',
                  desired_payload_hash = 'absent',
                  desired_ledger_version = (
                      SELECT version FROM ledger_events
                       WHERE id = ledger_projections.ledger_event_id
                  ),
                  updated_at = ?
            WHERE target_kind = 'client'
              AND target_calendar_id = ?
              AND ledger_event_id IN (
                  SELECT id FROM ledger_events WHERE user_id = ?
              )""",
        (when, client_calendar_id, user_id),
    )

    # 3. Clear sync token so resume does a full sync.
    await db.execute(
        """UPDATE calendar_sync_state
              SET sync_token = NULL,
                  last_full_sync = NULL,
                  last_incremental_sync = NULL
            WHERE client_calendar_id = ?""",
        (client_calendar_id,),
    )
    # 4. Mark every user ledger row dirty so the reconciler picks
    #    them up.
    affected = await (await db.execute(
        """SELECT id FROM ledger_events WHERE user_id = ?""",
        (user_id,),
    )).fetchall()
    await _append_affected(
        db, user_id=user_id,
        ledger_ids=[int(r["id"]) for r in affected],
    )
    await db.commit()
    return {"ledger_rows_cancelled": -1}  # caller asks the reconciler


async def cleanup_and_pause(
    db: aiosqlite.Connection,
    *,
    user_id: int,
) -> None:
    """Global cleanup: every projection for the user goes absent,
    sync is paused.  After the outbox drains, the user's calendars
    contain none of our writes.
    """
    when = datetime.now(UTC).isoformat()
    # The diff acts on the payload-hash change to 'absent';
    # desired_ledger_version stays at the event's real version
    # instead of a fabricated version+1.
    await db.execute(
        """UPDATE ledger_projections
              SET desired_state = 'absent',
                  desired_payload_hash = 'absent',
                  desired_ledger_version = (
                      SELECT version FROM ledger_events
                       WHERE id = ledger_projections.ledger_event_id
                  ),
                  updated_at = ?
            WHERE ledger_event_id IN (
                SELECT id FROM ledger_events WHERE user_id = ?
            )""",
        (when, user_id),
    )
    await db.execute(
        "UPDATE users SET sync_paused = 1 WHERE id = ?",
        (user_id,),
    )
    affected = await (await db.execute(
        "SELECT id FROM ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    await _append_affected(
        db, user_id=user_id,
        ledger_ids=[int(r["id"]) for r in affected],
    )
    await db.commit()


async def disconnect_calendar(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    client_calendar_id: int,
) -> None:
    """Cleanup + soft-delete the client_calendars row."""
    await cleanup_one_calendar(
        db, user_id=user_id, client_calendar_id=client_calendar_id,
    )
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE client_calendars
              SET is_active = 0, disconnected_at = ?
            WHERE id = ?""",
        (when, client_calendar_id),
    )
    await db.commit()


async def full_resync(
    db: aiosqlite.Connection,
    *,
    user_id: int,
) -> None:
    """Wipe sync tokens for every calendar the user owns.

    Idempotent.  The next reconcile re-fetches everything;
    ledger upserts dedupe so no actual writes are issued unless
    content changed.  Useful for "calendars look out of sync" or
    suspected drift.
    """
    await db.execute(
        """UPDATE calendar_sync_state
              SET sync_token = NULL,
                  last_full_sync = NULL,
                  last_incremental_sync = NULL
            WHERE client_calendar_id IN (
                SELECT id FROM client_calendars WHERE user_id = ?
            )""",
        (user_id,),
    )
    await db.execute(
        """UPDATE main_calendar_sync_state
              SET sync_token = NULL,
                  last_full_sync = NULL,
                  last_incremental_sync = NULL
            WHERE user_id = ?""",
        (user_id,),
    )
    await db.commit()


async def resume_sync(
    db: aiosqlite.Connection, *, user_id: int,
) -> None:
    """Un-pause a previously paused user."""
    await db.execute(
        "UPDATE users SET sync_paused = 0 WHERE id = ?",
        (user_id,),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Poison-pill recovery
# ---------------------------------------------------------------------------
async def retry_permanent_failures(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    projection_id: Optional[int] = None,
) -> int:
    """Clear the poison-pill flag on permanently-failed projections so
    the diff step re-enqueues them.

    A poison-pilled projection is excluded from the diff
    (``permanently_failed = 1``); clearing the flag is the only way
    back in.  ``applied_*`` still diverges from ``desired_*`` (the
    failed op never advanced it), so the next diff pass enqueues a
    fresh op with ``attempts = 0``.  Without an explicit
    ``projection_id`` every permanently-failed projection for the user
    is retried.  Returns the number of projections un-stuck.
    """
    when = datetime.now(UTC).isoformat()
    params: list = [user_id]
    clause = ""
    if projection_id is not None:
        clause = " AND p.id = ?"
        params.append(projection_id)
    rows = await (await db.execute(
        f"""SELECT p.id, p.ledger_event_id
              FROM ledger_projections p
              JOIN ledger_events e ON e.id = p.ledger_event_id
             WHERE e.user_id = ?
               AND p.permanently_failed = 1{clause}""",
        params,
    )).fetchall()
    if not rows:
        return 0
    proj_ids = [int(r["id"]) for r in rows]
    placeholders = ",".join("?" for _ in proj_ids)
    await db.execute(
        f"""UPDATE ledger_projections
               SET permanently_failed = 0,
                   attempts = 0,
                   next_attempt_at = NULL,
                   last_error = NULL,
                   updated_at = ?
             WHERE id IN ({placeholders})""",
        [when, *proj_ids],
    )
    await _append_affected(
        db, user_id=user_id,
        ledger_ids=[int(r["ledger_event_id"]) for r in rows],
    )
    await db.commit()
    return len(proj_ids)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _append_affected(
    db: aiosqlite.Connection, *, user_id: int, ledger_ids: list[int],
) -> None:
    import json
    if not ledger_ids:
        return
    when = datetime.now(UTC).isoformat()
    existing = await (await db.execute(
        "SELECT sources_json FROM reconcile_requests WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    if existing is None:
        await db.execute(
            """INSERT INTO reconcile_requests
                  (user_id, sources_json, enqueued_at, scheduled_for)
               VALUES (?, ?, ?, ?)""",
            (user_id, json.dumps(list(set(ledger_ids))), when, when),
        )
        return
    prior = json.loads(existing["sources_json"] or "[]")
    merged = list({*prior, *ledger_ids})
    await db.execute(
        """UPDATE reconcile_requests
              SET sources_json = ?, enqueued_at = ?
            WHERE user_id = ?""",
        (json.dumps(merged), when, user_id),
    )
