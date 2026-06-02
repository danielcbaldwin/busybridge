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
from itertools import islice
from typing import Optional

import aiosqlite
from dateutil.rrule import rrulestr

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
    no_live_occurrences = await _recurring_parent_has_no_live_occurrences(
        db, ledger,
    )
    main_native_redundant = await _main_native_is_redundant(db, ledger)
    desired = _compute_desired_projections(
        ledger,
        parent_inactive=parent_inactive,
        no_live_occurrences=no_live_occurrences,
        main_native_redundant=main_native_redundant,
    )

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


async def _main_native_is_redundant(db: aiosqlite.Connection, ledger) -> bool:
    """True when this ``main_native`` row is just the main-calendar
    reflection of a meeting already ingested from a client/personal/webcal
    source (matched by Google's cross-calendar ``iCalUID``).

    The user attends many client meetings that Google also drops natively
    onto their main calendar, producing a parallel ``main_native`` ledger
    lineage for the SAME meeting.  Both lineages then cast busy blocks onto
    the other client calendars — a visible duplicate.  The source lineage
    is authoritative, so the redundant ``main_native`` copy must project
    nothing.  Matching on iCalUID (not time) means this only ever
    collapses the genuinely-same meeting — never two distinct events that
    merely share a start time (which must each keep their busy block).
    """
    if ledger["source_type"] != "main_native":
        return False
    ical = ledger["ical_uid"]
    if not ical:
        return False
    sibling = await (await db.execute(
        """SELECT 1 FROM ledger_events
            WHERE user_id = ?
              AND ical_uid = ?
              AND source_type != 'main_native'
              AND status = 'active'
            LIMIT 1""",
        (int(ledger["user_id"]), ical),
    )).fetchone()
    return sibling is not None


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


async def _recurring_parent_has_no_live_occurrences(
    db: aiosqlite.Connection, ledger,
) -> bool:
    """True when a finite recurring parent has zero live occurrences.

    Google represents a cancelled occurrence as a child instance row.
    If every occurrence generated by a finite parent series has an
    inactive child, projecting the parent and then deleting its child
    exceptions creates a self-loop: the child delete cancels the only
    remaining occurrence, ingest clears the parent projection, and diff
    recreates it.  Infinite or unparseable recurrence rules are treated
    conservatively as live.
    """
    if ledger["parent_canonical_uid"] is not None:
        return False
    if not bool(ledger["is_recurring"]):
        return False
    if not ledger["recurrence_rule_json"] or not ledger["start_at"]:
        return False

    recurrence = _parse_recurrence_lines(ledger["recurrence_rule_json"])
    if recurrence is None:
        return False
    if not _looks_finite_recurrence(recurrence):
        return False

    start = _parse_datetime_for_recurrence(
        ledger["start_at"], is_all_day=bool(ledger["is_all_day"]),
    )
    if start is None:
        return False

    try:
        rule = rrulestr("\n".join(recurrence), dtstart=start, forceset=True)
        occurrences = list(islice(rule, 1001))
    except Exception as e:
        logger.warning(
            "could not expand recurrence for ledger event %s: %s",
            ledger["id"], e,
        )
        return False

    if len(occurrences) > 1000:
        logger.warning(
            "recurrence for ledger event %s expanded to %s occurrences; "
            "leaving parent live",
            ledger["id"], len(occurrences),
        )
        return False

    child_rows = await (await db.execute(
        """SELECT recurrence_instance_original_start,
                  status, user_intentionally_deleted
             FROM ledger_events
            WHERE user_id = ?
              AND parent_canonical_uid = ?""",
        (int(ledger["user_id"]), ledger["canonical_uid"]),
    )).fetchall()
    inactive_by_key: dict[str, bool] = {}
    for child in child_rows:
        key = _occurrence_key(
            child["recurrence_instance_original_start"],
            is_all_day=bool(ledger["is_all_day"]),
        )
        if key is None:
            continue
        inactive = (
            child["status"] == "cancelled"
            or bool(child["user_intentionally_deleted"])
        )
        inactive_by_key[key] = inactive_by_key.get(key, True) and inactive

    for occurrence in occurrences:
        key = _occurrence_key_from_datetime(
            occurrence,
            is_all_day=bool(ledger["is_all_day"]),
        )
        if not inactive_by_key.get(key, False):
            return False
    return True


def _parse_recurrence_lines(value: str) -> Optional[list[str]]:
    try:
        parsed = json.loads(value)
    except Exception:
        return None
    if not isinstance(parsed, list):
        return None
    lines = [str(item) for item in parsed if item]
    return lines or None


def _looks_finite_recurrence(lines: list[str]) -> bool:
    for line in lines:
        upper = line.upper()
        if upper.startswith("RRULE:") and (
            "COUNT=" in upper or "UNTIL=" in upper
        ):
            return True
        if upper.startswith("RDATE"):
            return True
    return False


def _parse_datetime_for_recurrence(
    value: str, *, is_all_day: bool,
) -> Optional[datetime]:
    try:
        if is_all_day:
            return datetime.fromisoformat(value[:10])
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _occurrence_key(value: str | None, *, is_all_day: bool) -> Optional[str]:
    if not value:
        return None
    dt = _parse_datetime_for_recurrence(value, is_all_day=is_all_day)
    if dt is None:
        return None
    return _occurrence_key_from_datetime(dt, is_all_day=is_all_day)


def _occurrence_key_from_datetime(dt: datetime, *, is_all_day: bool) -> str:
    if is_all_day:
        return dt.date().isoformat()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).replace(microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Internal: compute desired
# ---------------------------------------------------------------------------
def _compute_desired_projections(
    ledger,
    *,
    parent_inactive: bool = False,
    no_live_occurrences: bool = False,
    main_native_redundant: bool = False,
) -> dict[str, str]:
    """Return ``{role: desired_state}`` keys: 'main', 'peer_clients',
    'origin_client'.  The caller resolves 'peer_clients' against
    the active client list.

    ``parent_inactive`` carries the lifecycle of a modified
    instance's parent series — when the series is gone, the
    instance is too.

    ``main_native_redundant`` marks a ``main_native`` row that is the
    main-calendar reflection of a meeting already mirrored from a
    client/personal/webcal source (same iCalUID); it projects nothing so
    the authoritative source owns the busy blocks (no duplicate).
    """
    if (
        bool(ledger["user_intentionally_deleted"])
        or ledger["status"] == "cancelled"
        or parent_inactive
        or no_live_occurrences
        or main_native_redundant
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
        # gets no busy block.  It gets a "phantom" writeback
        # projection (REWRITE_PLAN.md §9) whenever there is something
        # to push back to the source: the user's RSVP, or — for an
        # editable event — a time/detail edit made on the main copy.
        # Rendered as an events.patch; it never creates or deletes
        # the source event.
        origin = (
            PRESENT_FULL_RSVP_ONLY
            if (ledger["user_rsvp_status"] or ledger["user_can_edit"])
            else ABSENT
        )
        return {
            "main": PRESENT_FULL,
            "peer_clients": peer,
            "origin_client": origin,
        }

    if source == "personal":
        # Personal calendars are read-only sources.  They cast opaque
        # busy blocks onto main and client calendars, but they never
        # receive a projection or writeback target themselves.
        return {
            "main": PRESENT_PERSONAL_BUSY,
            "peer_clients": PRESENT_PERSONAL_BUSY,
            "origin_client": ABSENT,
        }

    if source == "webcal":
        # Placement (see webcal.md §Planner Rules):
        #   main      -> full on main, busy/absent on every client
        #   client    -> full on main AND the placement client,
        #                busy/absent on every other client
        #   client+stale (target null/inactive) -> behaves like main
        peer = PRESENT_BUSY if show_as != "free" else ABSENT
        placement_kind = ledger["placement_kind"] or "main"
        placement_resolved = ledger["placement_client_resolved_id"]
        if placement_kind == "client" and placement_resolved is not None:
            # The placement target gets a real full copy, not a
            # PRESENT_FULL_RSVP_ONLY writeback — webcal feeds are
            # read-only sources, so there is no edit to push back.
            return {
                "main": PRESENT_FULL,
                "peer_clients": peer,
                "origin_client": PRESENT_FULL,
            }
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

    # WebCal placement: the placement client_calendar IS the origin for
    # routing purposes, *only* when the target row resolved (active and
    # belongs-to-user, per the planner's JOIN gating).  This is what
    # promotes the selected client from peer-busy to origin-full.  The
    # placement_client_calendar_id space is client_calendars.id, so
    # same_id_space is True for this branch — the existing collision
    # warning (webcal_subscriptions.id ≠ client_calendars.id) does NOT
    # apply here because we are using the placement id, not the source
    # id.
    if (
        source_type == "webcal"
        and ledger["placement_client_resolved_id"] is not None
    ):
        origin_cal_id = int(ledger["placement_client_resolved_id"])
        same_id_space = True

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
        #
        # A genuine desired change (hash differs) also un-sticks a
        # poison-pilled projection: the failed payload is now moot, so
        # clear permanently_failed and let the diff retry.  Without
        # this a permanently-failed projection is excluded from the
        # diff forever, even after the user edits the event.
        hash_changed = existing["desired_payload_hash"] != desired_hash
        await db.execute(
            """UPDATE ledger_projections
                  SET desired_state = ?,
                      desired_payload_hash = ?,
                      desired_ledger_version = ?,
                      permanently_failed = CASE WHEN ? THEN 0
                                                ELSE permanently_failed END,
                      last_error = CASE WHEN ? THEN NULL
                                        ELSE last_error END,
                      updated_at = ?
                WHERE id = ?""",
            (
                desired_state, desired_hash, int(ledger_row["version"]),
                hash_changed, hash_changed,
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
                      permanently_failed = 0,
                      updated_at = ?
                WHERE id = ?""",
            (ABSENT, ledger_version, when, int(row["id"])),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _get_ledger_row(db: aiosqlite.Connection, ledger_event_id: int):
    # Join the source calendar so the renderer can colour the copy by
    # the calendar the user picked in the UI (calendar_color_id) and
    # label its description with the source name (source_label).  Both
    # are read at render time so they stay consistent with the diff's
    # send body (which joins the same way) — keeping the payload hash
    # stable.
    #
    # For source_type='webcal' the JOIN target is webcal_subscriptions
    # (its display_prefix becomes the Source: label) plus an optional
    # second JOIN to client_calendars via placement_client_calendar_id
    # — that's the "placement target" that drives the color, the
    # Placement: footer line, and the per-target full-copy fan-out
    # (see webcal.md §Label, Footer, Color).  cc_placement is gated
    # on is_active=1 so a stale placement (target disconnected) falls
    # through to the main-placed render (no color, no Placement
    # line).
    row = await (await db.execute(
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
                  END AS placement_label,
                  ws.placement_kind                AS placement_kind,
                  ws.placement_client_calendar_id  AS placement_client_calendar_id,
                  cc_placement.id                  AS placement_client_resolved_id
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
