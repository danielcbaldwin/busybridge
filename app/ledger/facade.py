"""Read-only facade for UI / API / admin code.

The UI doesn't need to know about projections or outbox queues —
it just wants "what's on my schedule?", "how many busy blocks are
on calendar X?", "are there sync failures right now?".  This
module is the friendly contract.

All functions take an open ``aiosqlite.Connection``; none mutate
state.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

import aiosqlite

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Schedule queries
# ---------------------------------------------------------------------------
async def list_active_events_for_user(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    time_min: Optional[str] = None,
    time_max: Optional[str] = None,
) -> list[dict]:
    """Return every active (non-cancelled, non-user-deleted) ledger
    row for the user, sorted by start time."""
    sql = [
        "SELECT * FROM ledger_events",
        "WHERE user_id = ?",
        "  AND status = 'active'",
        "  AND user_intentionally_deleted = 0",
    ]
    params: list[Any] = [user_id]
    if time_min is not None:
        sql.append("  AND (end_at IS NULL OR end_at >= ?)")
        params.append(time_min)
    if time_max is not None:
        sql.append("  AND (start_at IS NULL OR start_at <= ?)")
        params.append(time_max)
    sql.append("ORDER BY start_at, id")
    rows = await (await db.execute(" ".join(sql), params)).fetchall()
    return [_row_to_dict(r) for r in rows]


async def count_events_for_user(
    db: aiosqlite.Connection, *, user_id: int,
) -> dict[str, int]:
    """Per-source counts for the dashboard."""
    rows = await (await db.execute(
        """SELECT source_type, COUNT(*) AS n
             FROM ledger_events
            WHERE user_id = ? AND status = 'active'
              AND user_intentionally_deleted = 0
            GROUP BY source_type""",
        (user_id,),
    )).fetchall()
    return {row["source_type"]: int(row["n"]) for row in rows}


async def count_busy_blocks_per_calendar(
    db: aiosqlite.Connection, *, user_id: int,
) -> dict[int, int]:
    """Count of present busy-block projections per client calendar."""
    rows = await (await db.execute(
        """SELECT target_calendar_id, COUNT(*) AS n
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND p.target_kind = 'client'
              AND p.current_state = 'present'
              AND p.desired_state IN ('present_busy', 'present_personal_busy')
            GROUP BY target_calendar_id""",
        (user_id,),
    )).fetchall()
    return {int(r["target_calendar_id"]): int(r["n"]) for r in rows}


async def count_main_copies(
    db: aiosqlite.Connection, *, user_id: int,
) -> int:
    """Count of present full-detail copies on the user's main calendar."""
    row = await (await db.execute(
        """SELECT COUNT(*) AS n
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND p.target_kind = 'main'
              AND p.current_state = 'present'""",
        (user_id,),
    )).fetchone()
    return int(row["n"]) if row else 0


async def count_active_ledger_events(
    db: aiosqlite.Connection, *, user_id: int,
) -> int:
    """Total active (non-cancelled, non-user-deleted) ledger rows."""
    row = await (await db.execute(
        """SELECT COUNT(*) AS n FROM ledger_events
            WHERE user_id = ? AND status = 'active'
              AND user_intentionally_deleted = 0""",
        (user_id,),
    )).fetchone()
    return int(row["n"]) if row else 0


async def count_active_events_per_source_calendar(
    db: aiosqlite.Connection, *, user_id: int,
) -> dict[int, int]:
    """``{client_calendars.id: count}`` of active ledger rows
    sourced from each client or personal calendar."""
    rows = await (await db.execute(
        """SELECT source_calendar_id, COUNT(*) AS n
             FROM ledger_events
            WHERE user_id = ? AND status = 'active'
              AND user_intentionally_deleted = 0
              AND source_type IN ('client', 'personal')
              AND source_calendar_id IS NOT NULL
            GROUP BY source_calendar_id""",
        (user_id,),
    )).fetchall()
    return {int(r["source_calendar_id"]): int(r["n"]) for r in rows}


async def count_active_events_per_webcal_subscription(
    db: aiosqlite.Connection, *, user_id: int,
) -> dict[int, int]:
    """``{webcal_subscriptions.id: count}`` of active ledger rows
    sourced from each webcal subscription."""
    rows = await (await db.execute(
        """SELECT source_calendar_id, COUNT(*) AS n
             FROM ledger_events
            WHERE user_id = ? AND status = 'active'
              AND user_intentionally_deleted = 0
              AND source_type = 'webcal'
              AND source_calendar_id IS NOT NULL
            GROUP BY source_calendar_id""",
        (user_id,),
    )).fetchall()
    return {int(r["source_calendar_id"]): int(r["n"]) for r in rows}


# ---------------------------------------------------------------------------
# Operational health
# ---------------------------------------------------------------------------
async def outbox_summary(
    db: aiosqlite.Connection, *, user_id: int,
) -> dict[str, int]:
    """Return ``{status: count}`` over the user's outbox queue."""
    rows = await (await db.execute(
        """SELECT status, COUNT(*) AS n
             FROM outbox_operations
            WHERE user_id = ?
            GROUP BY status""",
        (user_id,),
    )).fetchall()
    return {row["status"]: int(row["n"]) for row in rows}


async def integrity_status_for_user(
    db: aiosqlite.Connection, *, user_id: int,
) -> dict:
    """Compute integrity health from live ledger state.

    Replaces the legacy ``integrity_status`` table, which the old
    consistency-check job populated.  Under the ledger architecture
    that job is a no-op, so the table is never written — any dashboard
    still reading it shows a permanently blank integrity panel.

    "Issues" map onto ledger reality: a ``permanently_failed``
    projection is a hard error; a merely diverged projection
    (``applied`` behind ``desired``) is in-progress work, surfaced as
    a warning.
    """
    ob = await outbox_summary(db, user_id=user_id)
    permanent_failures = int(ob.get("permanent_failure", 0))
    diverged_row = await (await db.execute(
        """SELECT COUNT(*) AS n
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND p.permanently_failed = 0
              AND (p.applied_ledger_version IS NULL
                   OR p.applied_ledger_version != p.desired_ledger_version)""",
        (user_id,),
    )).fetchone()
    diverged = int(diverged_row["n"] or 0)
    if permanent_failures > 0:
        status = "error"
    elif diverged > 0:
        status = "warning"
    else:
        status = "ok"
    return {
        "status": status,
        "diverged": diverged,
        "permanent_failures": permanent_failures,
        "unresolved_issues": permanent_failures,
        "issues_found": diverged + permanent_failures,
    }


async def list_permanent_failures(
    db: aiosqlite.Connection, *, user_id: int, limit: int = 50,
) -> list[dict]:
    """Projections that hit the poison-pill threshold."""
    rows = await (await db.execute(
        """SELECT p.id, p.ledger_event_id, p.target_kind, p.target_calendar_id,
                  p.last_error, p.last_attempt_at,
                  e.summary, e.canonical_uid
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND p.permanently_failed = 1
            ORDER BY p.last_attempt_at DESC
            LIMIT ?""",
        (user_id, int(limit)),
    )).fetchall()
    return [_row_to_dict(r) for r in rows]


async def sync_failure_status(
    db: aiosqlite.Connection, *, user_id: int,
) -> dict:
    """Return the per-calendar sync failure state for the dashboard."""
    main_row = await (await db.execute(
        """SELECT consecutive_failures, last_error, last_incremental_sync
             FROM main_calendar_sync_state
            WHERE user_id = ?""",
        (user_id,),
    )).fetchone()
    client_rows = await (await db.execute(
        """SELECT c.id, c.display_name,
                  s.consecutive_failures, s.last_error, s.last_incremental_sync
             FROM client_calendars c
             LEFT JOIN calendar_sync_state s ON s.client_calendar_id = c.id
            WHERE c.user_id = ? AND c.is_active = 1""",
        (user_id,),
    )).fetchall()
    return {
        "main": _row_to_dict(main_row) if main_row else None,
        "clients": [_row_to_dict(r) for r in client_rows],
    }


def _row_to_dict(row) -> dict:
    if row is None:
        return {}
    return {k: row[k] for k in row.keys()}
