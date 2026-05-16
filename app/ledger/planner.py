"""Planner: ledger row → desired projections per target calendar.

Implements REWRITE_PLAN.md §6.1: for each ledger event, compute
which calendars should hold a copy and in what shape.

The planner is *pure* given ledger state + active client list.
It writes desired_state, desired_payload_hash, and
desired_ledger_version into ``ledger_projections``; the diff
step is what actually enqueues outbox ops.

Universal overrides (cancelled, user_intentionally_deleted,
show_as=free for client targets) live here so every source type
respects them uniformly.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

from app.ledger.payload import (
    ABSENT,
    PRESENT_BUSY,
    PRESENT_FULL,
    PRESENT_FULL_RSVP_ONLY,
    PRESENT_PERSONAL_BUSY,
    hash_payload,
    render_payload,
)

logger = logging.getLogger(__name__)
UTC = timezone.utc

# Target kinds
TARGET_MAIN = "main"
TARGET_CLIENT = "client"


async def plan_for_ledger_event(
    db: aiosqlite.Connection,
    *,
    ledger_event_id: int,
) -> int:
    """Recompute desired projections for one ledger row.

    Returns the number of projection rows written or updated.
    """
    ledger = await _get_ledger_row(db, ledger_event_id)
    user_id = int(ledger["user_id"])

    # A modified-instance row's desired state also depends on its
    # parent series: if the whole series was cancelled or the user
    # deleted it, the instance must go absent too (REWRITE_PLAN.md
    # §6) — otherwise it lingers as a ghost.
    parent_inactive = await _parent_is_inactive(db, ledger)
    desired = _compute_desired_projections(ledger, parent_inactive=parent_inactive)

    active_clients = await _active_client_calendars(db, user_id)
    targets = _resolve_targets(ledger, desired, active_clients)

    written = 0
    for target_kind, target_calendar_id, desired_state in targets:
        await _upsert_projection(
            db,
            ledger_event_id=ledger_event_id,
            target_kind=target_kind,
            target_calendar_id=target_calendar_id,
            desired_state=desired_state,
            ledger_row=ledger,
        )
        written += 1

    # Anything in projections that we didn't just write is now
    # implicit-absent (the calendar list shrank, or the source
    # changed type).  Mark it absent so the diff step deletes it.
    written_targets = {(k, c) for (k, c, _) in targets}
    await _mark_implicit_absent(
        db,
        ledger_event_id=ledger_event_id,
        keep=written_targets,
        ledger_version=int(ledger["version"]),
    )

    # Cascade: re-plan this series' modified-instance rows so a
    # change to the parent's lifecycle (e.g. the whole series being
    # cancelled) propagates to every instance.
    written += await _replan_instance_children(db, ledger)
    return written


async def _parent_is_inactive(db: aiosqlite.Connection, ledger) -> bool:
    """For a modified-instance row, True when its parent series has
    been cancelled or intentionally deleted."""
    parent_uid = ledger["parent_canonical_uid"]
    if not parent_uid:
        return False
    parent = await (await db.execute(
        """SELECT status, user_intentionally_deleted
             FROM ledger_events
            WHERE user_id = ? AND canonical_uid = ?""",
        (int(ledger["user_id"]), parent_uid),
    )).fetchone()
    if parent is None:
        return False
    return (
        parent["status"] == "cancelled"
        or bool(parent["user_intentionally_deleted"])
    )


async def _replan_instance_children(db: aiosqlite.Connection, ledger) -> int:
    """Re-plan every modified-instance row of this series master.

    A no-op for instance rows themselves (no grandchildren) and for
    non-recurring events.  Depth is bounded at one level.
    """
    if ledger["parent_canonical_uid"] is not None:
        return 0  # this IS an instance — it has no children
    if not ledger["is_recurring"]:
        return 0
    children = await (await db.execute(
        """SELECT id FROM ledger_events
            WHERE user_id = ? AND parent_canonical_uid = ?""",
        (int(ledger["user_id"]), ledger["canonical_uid"]),
    )).fetchall()
    written = 0
    for child in children:
        written += await plan_for_ledger_event(
            db, ledger_event_id=int(child["id"]),
        )
    return written


# ---------------------------------------------------------------------------
# Internal: compute desired
# ---------------------------------------------------------------------------
def _compute_desired_projections(
    ledger, *, parent_inactive: bool = False,
) -> dict[str, str]:
    """Return ``{role: desired_state}`` keys: 'main', 'peer_clients',
    'origin_client'.  The caller resolves 'peer_clients' against
    the active client list.

    ``parent_inactive`` carries the lifecycle of a modified
    instance's parent series — when the series is gone, the
    instance is too.
    """
    if (
        bool(ledger["user_intentionally_deleted"])
        or ledger["status"] == "cancelled"
        or parent_inactive
    ):
        return {"main": ABSENT, "peer_clients": ABSENT, "origin_client": ABSENT}

    source = ledger["source_type"]
    show_as = ledger["show_as"] or "busy"

    if source == "main_native":
        # Lives natively on main; just place busy blocks on clients.
        peer = PRESENT_BUSY if show_as != "free" else ABSENT
        return {"main": ABSENT, "peer_clients": peer, "origin_client": ABSENT}

    if source == "client":
        peer = PRESENT_BUSY if show_as != "free" else ABSENT
        # The origin client calendar holds the event natively, so it
        # gets no busy block.  But when the user is an attendee with
        # an RSVP, the origin gets a "phantom" rsvp-only projection
        # (REWRITE_PLAN.md §9): an RSVP the user sets on the main
        # copy is written back to the source event.  Rendered as an
        # events.patch — it never creates or deletes the source.
        origin = (
            PRESENT_FULL_RSVP_ONLY
            if ledger["user_rsvp_status"]
            else ABSENT
        )
        return {
            "main": PRESENT_FULL,
            "peer_clients": peer,
            "origin_client": origin,
        }

    if source == "personal":
        return {
            "main": PRESENT_PERSONAL_BUSY,
            "peer_clients": PRESENT_PERSONAL_BUSY,
            "origin_client": ABSENT,
        }

    if source == "webcal":
        peer = PRESENT_BUSY if show_as != "free" else ABSENT
        return {"main": PRESENT_FULL, "peer_clients": peer, "origin_client": ABSENT}

    raise ValueError(f"unknown source_type: {source!r}")


def _resolve_targets(
    ledger,
    desired: dict[str, str],
    active_clients: list[aiosqlite.Row],
) -> list[tuple[str, Optional[int], str]]:
    """Convert role-keyed desired states to concrete (kind, cal_id, state) triples.

    Origin-exclusion (the client calendar that *sourced* the event
    does not receive a busy block) only applies when the source is
    a client calendar.  Personal / webcal / main_native sources do
    not share an ID space with ``client_calendars.id``, so we must
    NOT compare numeric IDs across spaces — that would accidentally
    exclude a client whose ID matched, e.g., a webcal subscription's
    ID.
    """
    out: list[tuple[str, Optional[int], str]] = []

    main_state = desired["main"]
    out.append((TARGET_MAIN, None, main_state))

    source_type = ledger["source_type"]
    origin_cal_id = (
        int(ledger["source_calendar_id"])
        if ledger["source_calendar_id"] is not None
        else None
    )
    same_id_space = source_type in ("client",)
    peer_state = desired["peer_clients"]
    origin_state = desired["origin_client"]

    for cal in active_clients:
        cal_id = int(cal["id"])
        if same_id_space and cal_id == origin_cal_id:
            state = origin_state
        else:
            state = peer_state
        out.append((TARGET_CLIENT, cal_id, state))
    return out


# ---------------------------------------------------------------------------
# Internal: persist
# ---------------------------------------------------------------------------
async def _upsert_projection(
    db: aiosqlite.Connection,
    *,
    ledger_event_id: int,
    target_kind: str,
    target_calendar_id: Optional[int],
    desired_state: str,
    ledger_row,
) -> None:
    """Create or update one projection row with the latest desired
    state and payload hash."""
    rendered = render_payload(
        desired_state=desired_state,
        ledger_row=_row_to_dict(ledger_row),
        projection_id=None,
        ledger_version=int(ledger_row["version"]),
        target_kind=target_kind,
    )
    desired_hash = hash_payload(rendered)
    when = datetime.now(UTC).isoformat()

    existing = await (await db.execute(
        """SELECT id, applied_ledger_version, desired_ledger_version,
                  desired_payload_hash
             FROM ledger_projections
            WHERE ledger_event_id = ?
              AND target_kind = ?
              AND COALESCE(target_calendar_id, -1) = COALESCE(?, -1)""",
        (ledger_event_id, target_kind, target_calendar_id),
    )).fetchone()

    if existing is None:
        await db.execute(
            """INSERT INTO ledger_projections
                  (ledger_event_id, target_kind, target_calendar_id,
                   desired_state, desired_payload_hash,
                   desired_ledger_version,
                   created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                ledger_event_id, target_kind, target_calendar_id,
                desired_state, desired_hash, int(ledger_row["version"]),
                when, when,
            ),
        )
    else:
        # Bump desired_ledger_version regardless of whether the hash
        # changed; that way the diff step can see "ledger has moved
        # past what's applied" and re-evaluate.
        await db.execute(
            """UPDATE ledger_projections
                  SET desired_state = ?,
                      desired_payload_hash = ?,
                      desired_ledger_version = ?,
                      updated_at = ?
                WHERE id = ?""",
            (
                desired_state, desired_hash, int(ledger_row["version"]),
                when, int(existing["id"]),
            ),
        )


async def _mark_implicit_absent(
    db: aiosqlite.Connection,
    *,
    ledger_event_id: int,
    keep: set[tuple[str, Optional[int]]],
    ledger_version: int,
) -> None:
    rows = await (await db.execute(
        """SELECT id, target_kind, target_calendar_id
             FROM ledger_projections
            WHERE ledger_event_id = ?""",
        (ledger_event_id,),
    )).fetchall()
    when = datetime.now(UTC).isoformat()
    for row in rows:
        key = (row["target_kind"], row["target_calendar_id"])
        if key in keep:
            continue
        await db.execute(
            """UPDATE ledger_projections
                  SET desired_state = ?,
                      desired_payload_hash = 'absent',
                      desired_ledger_version = ?,
                      updated_at = ?
                WHERE id = ?""",
            (ABSENT, ledger_version, when, int(row["id"])),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _get_ledger_row(db: aiosqlite.Connection, ledger_event_id: int):
    row = await (await db.execute(
        "SELECT * FROM ledger_events WHERE id = ?",
        (int(ledger_event_id),),
    )).fetchone()
    if row is None:
        raise ValueError(f"ledger_event {ledger_event_id} not found")
    return row


async def _active_client_calendars(
    db: aiosqlite.Connection, user_id: int,
) -> list[aiosqlite.Row]:
    """Active CLIENT calendars — personal calendars are deliberately
    excluded, per REWRITE_PLAN.md §6.1: personal calendars are
    read-only origin sources, never busy-block targets."""
    cursor = await db.execute(
        """SELECT id FROM client_calendars
            WHERE user_id = ?
              AND is_active = 1
              AND calendar_type = 'client'""",
        (int(user_id),),
    )
    return await cursor.fetchall()


def _row_to_dict(row) -> dict:
    """Turn an aiosqlite.Row into a plain dict.

    Tolerates both Row objects (from real connections) and dicts
    (from in-memory test factories that prefer not to involve a
    Row factory).
    """
    if isinstance(row, dict):
        return dict(row)
    return {k: row[k] for k in row.keys()}
