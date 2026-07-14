"""Busy-block coalescer.

Merges overlapping / adjacent source-event intervals into the minimal
set of maximal contiguous "busy" ranges per target calendar.  The
ledger already sees every source event across every connected
calendar, so the planner can compute an optimal cover instead of
naively emitting one busy block per source event.

Design invariants:

* **Pure function.**  ``coalesce_intervals`` operates on plain dicts
  and returns plain dataclass instances.  No DB access, no I/O.  This
  makes it unit-testable and easy to reason about.
* **Deterministic carrier.**  Each merged group designates the source
  event with the smallest ledger id as its "carrier"; the carrier is
  what the planner promotes into a ``present_*_busy`` projection with a
  ``payload_override`` covering the union.  Deterministic choice keeps
  the group stable across runs — as long as the same events belong to
  the group, the same carrier projection stays live, so the diff sees
  no churn.
* **Timed vs. all-day are never merged.**  An all-day event has date
  semantics; a timed event has instant semantics.  Overlaying them
  would create ambiguous free/busy.  We keep them in separate groups.
* **Recurrence is dropped on merge.**  A merged interval represents a
  concrete one-off time range; it cannot carry an RRULE (there is no
  meaningful pattern spanning a mixed group).  The planner nulls
  ``recurrence_rule_json`` in the override.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable, Optional


@dataclass(frozen=True)
class CoalesceInput:
    """One source event's contribution to the coalescer.

    Fields mirror the columns the planner reads from
    ``ledger_events`` when rendering a busy block, so a
    ``CoalesceInput`` can be built directly from a projection-join
    row without extra lookup.
    """

    ledger_event_id: int
    start_at: str           # ISO datetime (with tz offset) OR YYYY-MM-DD for all-day
    end_at: str
    start_timezone: Optional[str]
    end_timezone: Optional[str]
    is_all_day: bool


@dataclass
class MergedInterval:
    """One maximal contiguous busy range produced by the coalescer."""

    start_at: str
    end_at: str
    start_timezone: Optional[str]
    end_timezone: Optional[str]
    is_all_day: bool
    carrier_ledger_event_id: int
    member_ledger_event_ids: list[int] = field(default_factory=list)


def _parse(s: str, is_all_day: bool) -> datetime:
    """Parse an ISO date-or-datetime string into a naive datetime
    that supports ``<``/``==`` comparison within the group.

    All-day dates are compared as date-only (converted to midnight
    UTC).  Timed events must carry a timezone offset in ``start_at``
    for the naive-utc comparison to be correct; ingest already stores
    them that way.  A trailing 'Z' is normalized to '+00:00' because
    Python's ``datetime.fromisoformat`` on 3.10 chokes on it.
    """
    if is_all_day:
        # Date-only comparison: convert to midnight so ``<``/``==`` work
        # against other all-day rows.
        d = datetime.strptime(s[:10], "%Y-%m-%d")
        return d
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # A best-effort fallback for the rare stored string without a
        # tz offset — parse naively.  Comparison stays consistent as
        # long as all events in the group use the same tz.
        dt = datetime.fromisoformat(s.split("+")[0].split("Z")[0])
    return dt


def coalesce_intervals(
    events: Iterable[CoalesceInput],
) -> list[MergedInterval]:
    """Merge overlapping / adjacent source events into a minimal cover.

    Events are grouped into two disjoint sets first — all-day events
    and timed events — because their instant vs. date semantics cannot
    be mixed.  Within each set the algorithm is a standard sort-by-start
    then linear sweep: current group grows while the next event's start
    lies at or before the current group's running maximum end; a strictly
    later start closes the group and opens a new one.
    """
    all_events = list(events)
    if not all_events:
        return []

    timed = [e for e in all_events if not e.is_all_day]
    all_day = [e for e in all_events if e.is_all_day]

    return _merge_bucket(timed) + _merge_bucket(all_day)


def _merge_bucket(bucket: list[CoalesceInput]) -> list[MergedInterval]:
    """Merge one same-kind (timed OR all-day) bucket."""
    if not bucket:
        return []

    # Sort by (start, ledger_id): the ledger_id tiebreak means equal
    # starts pick the smaller-id event first, so the deterministic
    # carrier choice stays predictable in ties.
    sorted_events = sorted(
        bucket, key=lambda e: (_parse(e.start_at, e.is_all_day), e.ledger_event_id),
    )

    groups: list[list[CoalesceInput]] = []
    current: list[CoalesceInput] = []
    current_end: Optional[datetime] = None

    for e in sorted_events:
        e_start = _parse(e.start_at, e.is_all_day)
        e_end = _parse(e.end_at, e.is_all_day)
        if not current:
            current = [e]
            current_end = e_end
            continue
        # <= means "touch or overlap".  Adjacent events (end == next
        # start) merge into a single interval — from a free/busy
        # perspective there is no gap between them and rendering two
        # abutting blocks would be strictly worse than one longer one.
        if e_start <= current_end:  # type: ignore[operator]
            current.append(e)
            if e_end > current_end:  # type: ignore[operator]
                current_end = e_end
        else:
            groups.append(current)
            current = [e]
            current_end = e_end
    if current:
        groups.append(current)

    return [_group_to_interval(g) for g in groups]


def _group_to_interval(group: list[CoalesceInput]) -> MergedInterval:
    """Reduce one merged group to its interval and carrier."""
    # Carrier: smallest ledger id in the group.  Deterministic across
    # runs — as long as the same events belong to the group, the same
    # carrier stays live, so the diff sees no churn.
    carrier = min(group, key=lambda e: e.ledger_event_id)

    # Interval bounds: the union.  All-day 'start_at' is a date; we
    # keep it as-is (no time component) so the payload renders as an
    # all-day block.  Timed events carry ISO datetimes; taking the
    # min/max of their raw strings works when they share a timezone,
    # but the safe path is to compare via parsed datetimes and then
    # write back the string form of the winner.
    is_all_day = carrier.is_all_day
    start_owner = min(group, key=lambda e: _parse(e.start_at, e.is_all_day))
    end_owner = max(group, key=lambda e: _parse(e.end_at, e.is_all_day))

    return MergedInterval(
        start_at=start_owner.start_at,
        end_at=end_owner.end_at,
        start_timezone=start_owner.start_timezone,
        end_timezone=end_owner.end_timezone,
        is_all_day=is_all_day,
        carrier_ledger_event_id=carrier.ledger_event_id,
        member_ledger_event_ids=[e.ledger_event_id for e in group],
    )


# ---------------------------------------------------------------------------
# DB-integrated pass
# ---------------------------------------------------------------------------
async def apply_coalescing_for_user(db, *, user_id: int) -> int:
    """Recompute the coalesced busy-block cover for every active
    client target of one user.

    Reads the current per-event ``present_personal_busy`` projections
    that the planner has already written, computes maximal contiguous
    intervals, and updates ``ledger_projections`` in place so that the
    diff step then converges Google to the merged shape:

    * A **carrier** projection (per interval) keeps its ``desired_state
      = present_personal_busy`` but gains ``payload_override_json`` that
      spans the union of its group's start/end times.  The carrier's
      identity is the smallest-id ledger event in the group — a
      deterministic choice that keeps carriers stable across runs.
    * **Non-carrier members** in a group of 2+ have their
      ``desired_state`` set to ``absent`` and their
      ``coalesce_carrier_id`` linked to the carrier's projection id.
      Diff sees the absent + present-with-hash-change, issues one
      delete per member and one update on the carrier.
    * **Singletons** (a group of one) are left untouched: no coalesce
      metadata, no override — they render identically to un-coalesced
      per-event projections.

    Idempotent: rows are only UPDATEd when their desired state / hash /
    override / carrier link actually differs from what the coalescing
    computation says they should be.

    Returns the number of projection rows written.
    """
    import json

    from app.ledger.payload import PRESENT_PERSONAL_BUSY, hash_payload, render_payload

    active_clients = await (await db.execute(
        """SELECT id FROM client_calendars
            WHERE user_id = ? AND is_active = 1 AND calendar_type = 'client'""",
        (user_id,),
    )).fetchall()

    written = 0
    for cal_row in active_clients:
        target_calendar_id = int(cal_row["id"])
        written += await _apply_coalescing_for_target(
            db, user_id=user_id, target_calendar_id=target_calendar_id,
            render_payload=render_payload,
            hash_payload=hash_payload,
            present_state=PRESENT_PERSONAL_BUSY,
            json=json,
        )
    return written


async def _apply_coalescing_for_target(
    db, *, user_id: int, target_calendar_id: int,
    render_payload, hash_payload, present_state, json,
) -> int:
    """Coalesce one target's personal-busy projections.  Kept separate
    for clarity — every target is independent."""

    # Candidate projections are those that the per-event planner has
    # already decided should carry a personal-busy block on this target
    # OR that a PRIOR coalesce pass folded into a group (absent +
    # coalesce_carrier_id set).  A group's constituent projections are
    # exactly what we need to re-evaluate against the current ledger
    # state.
    rows = await (await db.execute(
        """SELECT p.id AS projection_id,
                  p.ledger_event_id,
                  p.desired_state,
                  p.desired_payload_hash,
                  p.payload_override_json,
                  p.coalesce_carrier_id,
                  p.desired_ledger_version,
                  e.version AS ledger_version,
                  e.start_at, e.end_at,
                  e.start_timezone, e.end_timezone,
                  e.is_all_day,
                  e.status,
                  e.source_type,
                  e.recurrence_rule_json
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE p.target_kind = 'client'
              AND p.target_calendar_id = ?
              AND e.user_id = ?
              AND e.source_type = 'personal'
              AND (
                   p.desired_state = ?
                OR (p.desired_state = 'absent' AND p.coalesce_carrier_id IS NOT NULL)
              )""",
        (target_calendar_id, user_id, present_state),
    )).fetchall()

    # A projection whose source event has since gone inactive is not a
    # coalesce candidate — the planner will have separately moved it to
    # desired=absent and we must not resurrect it here.
    candidates = [r for r in rows if r["status"] == "active"]

    # Reduce to CoalesceInputs; feed the coalescer.
    inputs = [
        CoalesceInput(
            ledger_event_id=int(r["ledger_event_id"]),
            start_at=r["start_at"],
            end_at=r["end_at"],
            start_timezone=r["start_timezone"],
            end_timezone=r["end_timezone"],
            is_all_day=bool(r["is_all_day"]),
        )
        for r in candidates
    ]
    intervals = coalesce_intervals(inputs)

    # Index projections by ledger_event_id for quick lookup.
    proj_by_ledger: dict[int, dict] = {
        int(r["ledger_event_id"]): dict(r) for r in candidates
    }

    # Compute the desired end state for every candidate projection.
    # Missing entries (i.e. a projection that USED to be part of a
    # coalesce group but whose source event is no longer active) are
    # dropped — the per-event planner has already handled them.
    #
    # For every interval:
    #   - singleton: keep the projection at desired=present with no
    #     coalesce metadata and no override.
    #   - group of 2+: carrier keeps present + override; members go
    #     absent + coalesce_carrier_id pointing back to the carrier's
    #     projection row.
    desired_per_projection: dict[int, dict] = {}
    for interval in intervals:
        carrier_ledger_id = interval.carrier_ledger_event_id
        carrier_projection = proj_by_ledger.get(carrier_ledger_id)
        if carrier_projection is None:  # defensive; carrier came from candidates
            continue

        if len(interval.member_ledger_event_ids) == 1:
            desired_per_projection[int(carrier_projection["projection_id"])] = {
                "desired_state": present_state,
                "payload_override_json": None,
                "coalesce_carrier_id": None,
                "carrier_ledger_row": carrier_projection,
            }
            continue

        # Group of 2+: build the override for the carrier.
        override_dict = {
            "start_at": interval.start_at,
            "end_at": interval.end_at,
            "start_timezone": interval.start_timezone,
            "end_timezone": interval.end_timezone,
            "is_all_day": 1 if interval.is_all_day else 0,
            # A merged interval represents concrete time ranges; if the
            # carrier's source event was recurring, the merged shape is
            # not — drop the RRULE so the rendered block is a one-off.
            "recurrence_rule_json": None,
        }
        override_json = json.dumps(override_dict, sort_keys=True)
        desired_per_projection[int(carrier_projection["projection_id"])] = {
            "desired_state": present_state,
            "payload_override_json": override_json,
            "coalesce_carrier_id": None,
            "carrier_ledger_row": carrier_projection,
            "override_dict": override_dict,
        }
        for member_id in interval.member_ledger_event_ids:
            if member_id == carrier_ledger_id:
                continue
            member_proj = proj_by_ledger.get(member_id)
            if member_proj is None:
                continue
            desired_per_projection[int(member_proj["projection_id"])] = {
                "desired_state": "absent",
                "payload_override_json": None,
                "coalesce_carrier_id": int(carrier_projection["projection_id"]),
            }

    # Apply diffs.  Skip rows that already match.
    when = _now_iso()
    written = 0
    for r in candidates:
        pid = int(r["projection_id"])
        target = desired_per_projection.get(pid)
        if target is None:
            continue
        new_state = target["desired_state"]
        new_override = target["payload_override_json"]
        new_carrier = target["coalesce_carrier_id"]

        # Compute the new payload hash.
        if new_state == "absent":
            new_hash = "absent"
        else:
            override = target.get("override_dict")
            rendered = render_payload(
                desired_state=new_state,
                ledger_row=target["carrier_ledger_row"],
                projection_id=None,
                ledger_version=int(target["carrier_ledger_row"]["ledger_version"]),
                target_kind="client",
                payload_override=override,
            )
            new_hash = hash_payload(rendered)

        # Idempotence: skip rows already matching.
        if (
            r["desired_state"] == new_state
            and (r["payload_override_json"] or None) == new_override
            and r["coalesce_carrier_id"] == new_carrier
            and r["desired_payload_hash"] == new_hash
        ):
            continue

        await db.execute(
            """UPDATE ledger_projections
                  SET desired_state = ?,
                      desired_payload_hash = ?,
                      payload_override_json = ?,
                      coalesce_carrier_id = ?,
                      updated_at = ?
                WHERE id = ?""",
            (new_state, new_hash, new_override, new_carrier, when, pid),
        )
        written += 1
    if written:
        await db.commit()
    return written


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()

