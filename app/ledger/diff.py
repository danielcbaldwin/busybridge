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
from typing import Iterable, Optional

import aiosqlite

from app.ledger.google_client import GoogleClient
from app.ledger.identity import derive_instance_google_event_id
from app.ledger.outbox import OP_CREATE, OP_DELETE, OP_PATCH, OP_UPDATE, enqueue
from app.ledger.payload import ABSENT, PRESENT_FULL_RSVP_ONLY, render_payload

logger = logging.getLogger(__name__)
UTC = timezone.utc


async def diff_and_enqueue_for_user(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    main_calendar_id: str,
    google_calendar_id_for: dict[int, str],
    now: Optional[datetime] = None,
) -> int:
    """Scan diverged projections and enqueue outbox ops.

    Args:
        user_id: only process this user's projections.
        main_calendar_id: the Google calendar ID for the user's main.
        google_calendar_id_for: map from ``client_calendars.id``
            to its Google calendar ID.
        now: clock for outbox timestamps; defaults to wall-clock.
            Tests pass a simulated clock so backoff is deterministic.

    Returns the number of ops enqueued.
    """
    rows = await _diverged_projections(db, user_id=user_id)
    enqueued = 0
    # Process parents (non-instance rows) before instances so a
    # parent's google_event_id is settled by the time we look it
    # up for instance derivation.
    rows_sorted = sorted(rows, key=lambda r: bool(r["parent_canonical_uid"]))

    for proj in rows_sorted:
        # Instance projections: derive google_event_id from the
        # parent's projection on the same target.  Origin writeback
        # projections are skipped here — they deliberately keep
        # google_event_id NULL (see _is_origin_writeback / _do_patch).
        if proj["parent_canonical_uid"] and not _is_origin_writeback(proj):
            parent_proj_google_id = await _parent_projection_google_id(
                db,
                user_id=user_id,
                parent_canonical_uid=proj["parent_canonical_uid"],
                target_kind=proj["target_kind"],
                target_calendar_id=proj["target_calendar_id"],
            )
            if not parent_proj_google_id:
                # Parent hasn't been written yet; defer this instance
                # to the next reconcile pass.
                continue
            derived = derive_instance_google_event_id(
                parent_proj_google_id,
                proj["recurrence_instance_original_start"] or "",
                bool(proj["is_all_day"]),
            )
            # Pre-set google_event_id on the instance projection so
            # the outbox's update/delete code path can find it.
            if not proj["google_event_id"]:
                await db.execute(
                    """UPDATE ledger_projections
                          SET google_event_id = ?
                        WHERE id = ?""",
                    (derived, int(proj["id"])),
                )
                # Re-read; otherwise _decide sees the stale row.
                proj = await (await db.execute(
                    """SELECT p.*, e.user_id AS user_id_from_ledger,
                              e.summary, e.description, e.location,
                              e.start_at, e.end_at,
                              e.start_timezone, e.end_timezone,
                              e.origin_writeback_pending,
                              e.is_all_day, e.show_as,
                              e.color_id, e.user_can_edit, e.user_rsvp_status,
                              e.recurrence_rule_json, e.version AS ledger_version,
                              e.parent_canonical_uid,
                              e.recurrence_instance_original_start,
                              e.source_type, e.source_calendar_id,
                              e.attendees_json
                         FROM ledger_projections p
                         JOIN ledger_events e ON e.id = p.ledger_event_id
                        WHERE p.id = ?""",
                    (int(proj["id"]),),
                )).fetchone()

        op_kind, payload, target_cal = _decide(
            proj=proj,
            main_calendar_id=main_calendar_id,
            google_calendar_id_for=google_calendar_id_for,
        )
        if op_kind is None:
            await _snap_applied(db, proj["id"], int(proj["desired_ledger_version"]))
            continue
        # Instances are applied via UPDATE on the derived ID (the
        # fake — and real Google — materialise the override
        # transparently).  No INSERT path for instances.
        if proj["parent_canonical_uid"] and op_kind == OP_CREATE:
            op_kind = OP_UPDATE
        await enqueue(
            db,
            user_id=user_id,
            projection_id=int(proj["id"]),
            operation=op_kind,
            ledger_version=int(proj["desired_ledger_version"]),
            target_google_calendar_id=target_cal,
            payload=payload,
            desired_payload_hash=proj["desired_payload_hash"],
            now=now,
        )
        enqueued += 1
    await db.commit()
    return enqueued


async def _parent_projection_google_id(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    parent_canonical_uid: str,
    target_kind: str,
    target_calendar_id: Optional[int],
) -> Optional[str]:
    """Look up the parent ledger row's projection on the same
    target, and return its ``google_event_id`` if set."""
    row = await (await db.execute(
        """SELECT p.google_event_id
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND e.canonical_uid = ?
              AND p.target_kind = ?
              AND COALESCE(p.target_calendar_id, -1) = COALESCE(?, -1)
            LIMIT 1""",
        (user_id, parent_canonical_uid, target_kind, target_calendar_id),
    )).fetchone()
    if row is None:
        return None
    return row["google_event_id"]


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
                  e.start_at, e.end_at,
                  e.start_timezone, e.end_timezone,
                  e.origin_writeback_pending,
                  e.is_all_day, e.show_as,
                  e.color_id, e.user_can_edit, e.user_rsvp_status,
                  e.recurrence_rule_json, e.version AS ledger_version,
                  e.parent_canonical_uid,
                  e.recurrence_instance_original_start,
                  e.source_type, e.source_calendar_id, e.attendees_json
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


def _is_origin_writeback(proj) -> bool:
    """True for the phantom projection that writes the user's edits
    — RSVP, time, and (for client sources) detail — back to the
    calendar that *sourced* the event (target calendar == origin
    calendar).  It is rendered as an ``events.patch`` and never
    creates or deletes the source event."""
    if proj["target_kind"] != "client":
        return False
    if proj["source_type"] not in ("client", "personal"):
        return False
    sc = proj["source_calendar_id"]
    tc = proj["target_calendar_id"]
    return sc is not None and tc is not None and int(sc) == int(tc)


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

    # Origin writeback projection: the target is the user's real
    # source event.  It may only ever be PATCHed (write the user's
    # edits) or be a no-op — never created or deleted.  When the
    # event is cancelled / intentionally-deleted the planner sets
    # desired to ABSENT; that must NOT delete the source event.
    if _is_origin_writeback(proj):
        if desired != PRESENT_FULL_RSVP_ONLY:
            return None, None, target_cal
        # The writeback patches an event we do NOT own, so it must
        # fire ONLY for a genuine change to the user's edits — never
        # on a bare version bump (e.g. a recolor, which the writeback
        # does not carry).  A no-op patch would still touch the
        # source event and echo back through that calendar's
        # incremental feed as a spurious change.  A NULL
        # applied_payload_hash means "not yet baselined": the
        # projection was just planned from state ingested *from* the
        # source, so it already matches — snap a baseline, no patch.
        applied = proj["applied_payload_hash"]
        if applied is not None and applied == proj["desired_payload_hash"]:
            # Already written back — nothing to do.
            return None, None, target_cal
        if applied is None and not bool(proj["origin_writeback_pending"]):
            # First sight with a NULL baseline normally means the
            # projection was just planned from state ingested FROM the
            # source, so the source already matches — snap a baseline,
            # no patch.  origin_writeback_pending overrides this: a
            # main-side edit produced a change the source has not seen.
            return None, None, target_cal
        return OP_PATCH, payload, target_cal

    if desired == ABSENT:
        # No google_event_id ever assigned → nothing to delete,
        # unless this is an instance projection (whose google_event_id
        # is derived from the parent — the delete materialises a
        # cancelled exception on the series).
        is_instance = bool(proj["parent_canonical_uid"])
        if not proj["google_event_id"]:
            return None, None, target_cal
        if current in ("absent",) and not is_instance:
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
        "start_timezone": proj["start_timezone"],
        "end_timezone": proj["end_timezone"],
        "is_all_day": proj["is_all_day"],
        "show_as": proj["show_as"],
        "color_id": proj["color_id"],
        "user_can_edit": proj["user_can_edit"],
        "user_rsvp_status": proj["user_rsvp_status"],
        "recurrence_rule_json": proj["recurrence_rule_json"],
        "attendees_json": proj["attendees_json"],
        "source_type": proj["source_type"],
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
