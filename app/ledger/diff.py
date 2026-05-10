"""Diff: turn diverged projections into outbox ops.

Walks every projection where the planner has bumped
``desired_ledger_version`` past ``applied_ledger_version`` (or
where it diverges by hash) and enqueues exactly one outbox op
per projection.

Operation choice:

* desired = absent, current = present  → delete
* desired = present_*, current = absent → create
* desired = present_*, current = present → update
* desired = absent, current = absent  → no-op (clear divergence)

The op's payload is rendered fresh each time — never cached —
so a payload-rendering bug cannot persist a stale body.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Iterable

import aiosqlite

from app.ledger.google_client import GoogleClient
from app.ledger.outbox import OP_CREATE, OP_DELETE, OP_UPDATE, enqueue
from app.ledger.payload import ABSENT, render_payload

logger = logging.getLogger(__name__)
UTC = timezone.utc


async def diff_and_enqueue_for_user(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    main_calendar_id: str,
    google_calendar_id_for: dict[int, str],
) -> int:
    """Scan diverged projections and enqueue outbox ops.

    Args:
        user_id: only process this user's projections.
        main_calendar_id: the Google calendar ID for the user's main.
        google_calendar_id_for: map from ``client_calendars.id``
            to its Google calendar ID.

    Returns the number of ops enqueued.
    """
    rows = await _diverged_projections(db, user_id=user_id)
    enqueued = 0
    for proj in rows:
        op_kind, payload, target_cal = _decide(
            proj=proj,
            main_calendar_id=main_calendar_id,
            google_calendar_id_for=google_calendar_id_for,
        )
        if op_kind is None:
            # absent/absent — clear divergence by snapping applied
            # forward.
            await _snap_applied(db, proj["id"], int(proj["desired_ledger_version"]))
            continue
        await enqueue(
            db,
            user_id=user_id,
            projection_id=int(proj["id"]),
            operation=op_kind,
            ledger_version=int(proj["desired_ledger_version"]),
            target_google_calendar_id=target_cal,
            payload=payload,
        )
        enqueued += 1
    await db.commit()
    return enqueued


# ---------------------------------------------------------------------------
# Internals
# ---------------------------------------------------------------------------
async def _diverged_projections(
    db: aiosqlite.Connection, *, user_id: int,
) -> list[aiosqlite.Row]:
    """All projections for ``user_id`` whose desired != applied."""
    cursor = await db.execute(
        """SELECT p.*, e.user_id AS user_id_from_ledger,
                  e.summary, e.description, e.location,
                  e.start_at, e.end_at, e.is_all_day, e.show_as,
                  e.color_id, e.user_can_edit, e.user_rsvp_status,
                  e.recurrence_rule_json, e.version AS ledger_version
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND (p.applied_ledger_version IS NULL
                   OR p.applied_ledger_version != p.desired_ledger_version
                   OR p.applied_payload_hash IS NULL
                   OR p.applied_payload_hash != p.desired_payload_hash)
              AND p.permanently_failed = 0""",
        (int(user_id),),
    )
    return await cursor.fetchall()


def _decide(
    *,
    proj,
    main_calendar_id: str,
    google_calendar_id_for: dict[int, str],
) -> tuple[str | None, dict | None, str]:
    """Return ``(op, payload, target_google_calendar)``."""
    desired = proj["desired_state"]
    current = proj["current_state"]
    target_kind = proj["target_kind"]

    if target_kind == "main":
        target_cal = main_calendar_id
    else:
        client_cal_id = int(proj["target_calendar_id"])
        if client_cal_id not in google_calendar_id_for:
            raise ValueError(
                f"projection {proj['id']} targets client_calendar_id "
                f"{client_cal_id} but no Google ID mapping was provided"
            )
        target_cal = google_calendar_id_for[client_cal_id]

    payload = render_payload(
        desired_state=desired,
        ledger_row=_proj_row_to_ledger_dict(proj),
        projection_id=int(proj["id"]),
        ledger_version=int(proj["desired_ledger_version"]),
        target_kind=target_kind,
    )

    if desired == ABSENT:
        # No google_event_id ever assigned → nothing to delete.
        if not proj["google_event_id"] or current in ("absent", "unknown"):
            return None, None, target_cal
        return OP_DELETE, None, target_cal

    # desired is present-of-some-kind
    if current == "absent" or current == "unknown" or not proj["google_event_id"]:
        return OP_CREATE, payload, target_cal
    return OP_UPDATE, payload, target_cal


def _proj_row_to_ledger_dict(proj) -> dict:
    """Re-shape the joined diff row into the dict ``render_payload`` expects."""
    return {
        "summary": proj["summary"],
        "description": proj["description"],
        "location": proj["location"],
        "start_at": proj["start_at"],
        "end_at": proj["end_at"],
        "is_all_day": proj["is_all_day"],
        "show_as": proj["show_as"],
        "color_id": proj["color_id"],
        "user_can_edit": proj["user_can_edit"],
        "user_rsvp_status": proj["user_rsvp_status"],
        "recurrence_rule_json": proj["recurrence_rule_json"],
    }


async def _snap_applied(
    db: aiosqlite.Connection, projection_id: int, ledger_version: int,
) -> None:
    """Mark divergence as resolved without an outbox op."""
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE ledger_projections
              SET applied_ledger_version = ?,
                  applied_payload_hash = desired_payload_hash,
                  current_state = 'absent',
                  last_attempt_at = ?,
                  updated_at = ?
            WHERE id = ?""",
        (int(ledger_version), when, when, int(projection_id)),
    )
