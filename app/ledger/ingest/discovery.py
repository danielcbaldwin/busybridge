"""Discovery / orphan scan (REWRITE_PLAN.md §5.5).

Periodically (every 6 hours in production) walks every calendar
the user owns and asks Google: "show me events that look like
ours that we don't know about."  Two signals:

* Our deterministic Google ID prefix (``bb``-prefixed base32hex
  encoding of a projection ID).  This is the primary signal —
  cheap to compute, no extended-property lookup needed.
* Events with our ``extendedProperties.private.bb_proj_id`` that
  doesn't match any known projection.  Defence-in-depth for
  legacy events created before deterministic IDs landed.

Anything matched against a stale projection is queued for
deletion via the outbox.  Events matched against a still-live
projection get their ``current_state``/``google_event_id``
re-linked so the next reconcile is consistent.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Iterable, Optional

import aiosqlite

from app.ledger.async_google import as_async_google
from app.ledger.google_client import GoogleClient
from app.ledger.identity import is_managed_google_event_id
from app.ledger.outbox import OP_DELETE, enqueue
from app.ledger.payload import EP_PROJ_ID

logger = logging.getLogger(__name__)
UTC = timezone.utc


async def discover_orphans(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    main_google_calendar_id: str,
    client_google_calendar_ids: dict[int, str],
    now: Optional[datetime] = None,
) -> dict:
    """Scan every connected calendar for events that look like ours.

    Returns counters: ``{scanned_calendars, candidates_seen,
    relinked, orphans_deleted}``.
    """
    google = as_async_google(google)
    counters = {
        "scanned_calendars": 0,
        "candidates_seen": 0,
        "relinked": 0,
        "orphans_deleted": 0,
    }

    targets = [(main_google_calendar_id, None)] + [
        (gid, cid) for cid, gid in client_google_calendar_ids.items()
    ]

    for google_cal, client_cal_db_id in targets:
        counters["scanned_calendars"] += 1
        candidates = [
            c async for c in _iter_candidates_on_calendar(google, google_cal)
        ]
        counters["candidates_seen"] += len(candidates)

        for event in candidates:
            outcome = await _classify_and_handle(
                db, google,
                user_id=user_id,
                target_google_calendar_id=google_cal,
                target_calendar_db_id=client_cal_db_id,
                event=event,
                now=now,
            )
            counters[outcome] = counters.get(outcome, 0) + 1

    return counters


async def _iter_candidates_on_calendar(
    google, calendar_id: str,
):
    """Pull all events from ``calendar_id`` that match either of
    our two ownership signals (deterministic ID prefix OR
    ``bb_proj_id`` private extended property).

    An async generator: ``google`` is the awaitable client adapter."""
    page_token = None
    while True:
        try:
            page = await google.list_events(
                calendar_id,
                page_token=page_token,
                show_deleted=False,
                max_results=250,
            )
        except Exception as e:
            logger.warning("discovery list_events failed on %s: %s", calendar_id, e)
            return
        for ev in page.get("items", []):
            if _looks_like_ours(ev):
                yield ev
        page_token = page.get("nextPageToken")
        if not page_token:
            return


def _looks_like_ours(event: dict) -> bool:
    if is_managed_google_event_id(event.get("id")):
        return True
    private = (event.get("extendedProperties") or {}).get("private") or {}
    return EP_PROJ_ID in private


async def _classify_and_handle(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    target_google_calendar_id: str,
    target_calendar_db_id: Optional[int],
    event: dict,
    now: Optional[datetime] = None,
) -> str:
    """Decide if a candidate is live, re-linkable, or orphaned.

    Returns one of ``relinked``, ``orphans_deleted``, ``live``.
    """
    eid = event["id"]
    private = (event.get("extendedProperties") or {}).get("private") or {}
    claimed_proj_id = private.get(EP_PROJ_ID)

    # First: is it already linked to a live projection?
    proj = await (await db.execute(
        """SELECT p.* FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE p.google_event_id = ? AND e.user_id = ?
            LIMIT 1""",
        (eid, user_id),
    )).fetchone()
    if proj is not None:
        return "live"

    # Second: extended-property claim → re-link if the claimed
    # projection still exists.
    if claimed_proj_id is not None:
        try:
            claimed_int = int(claimed_proj_id)
        except (TypeError, ValueError):
            claimed_int = None
        if claimed_int is not None:
            claimed = await (await db.execute(
                """SELECT p.* FROM ledger_projections p
                     JOIN ledger_events e ON e.id = p.ledger_event_id
                    WHERE p.id = ? AND e.user_id = ?""",
                (claimed_int, user_id),
            )).fetchone()
            if claimed is not None and claimed["google_event_id"] is None:
                await db.execute(
                    """UPDATE ledger_projections
                          SET google_event_id = ?,
                              google_etag = ?,
                              current_state = 'present'
                        WHERE id = ?""",
                    (eid, event.get("etag", ""), int(claimed["id"])),
                )
                await db.commit()
                return "relinked"

    # Third: not live, not re-linkable → orphan.  Schedule deletion.
    await _schedule_orphan_delete(
        db, google,
        user_id=user_id,
        target_google_calendar_id=target_google_calendar_id,
        target_calendar_db_id=target_calendar_db_id,
        event_id=eid,
        now=now,
    )
    return "orphans_deleted"


async def _schedule_orphan_delete(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    target_google_calendar_id: str,
    target_calendar_db_id: Optional[int],
    event_id: str,
    now: Optional[datetime] = None,
) -> None:
    """Insert a synthetic ledger row + projection so the outbox
    can issue a clean idempotent delete.

    We create a "tombstone" ledger event with ``status='cancelled'``
    and a projection whose ``google_event_id`` is set to the orphan
    we want gone.  The next reconcile diff produces a delete.
    """
    when = datetime.now(UTC).isoformat()
    canonical = f"orphan:{user_id}:{target_google_calendar_id}:{event_id}"
    cursor = await db.execute(
        """INSERT OR IGNORE INTO ledger_events
              (user_id, canonical_uid, source_type,
               status, version,
               created_at, updated_at, last_seen_at)
           VALUES (?, ?, 'main_native',
                   'cancelled', 1,
                   ?, ?, ?)""",
        (user_id, canonical, when, when, when),
    )
    if cursor.rowcount == 0:
        existing = await (await db.execute(
            "SELECT id FROM ledger_events WHERE user_id=? AND canonical_uid=?",
            (user_id, canonical),
        )).fetchone()
        ledger_id = int(existing["id"])
    else:
        ledger_id = int(cursor.lastrowid)

    target_kind = "main" if target_calendar_db_id is None else "client"
    # Insert projection (idempotent on unique constraint).
    await db.execute(
        """INSERT OR IGNORE INTO ledger_projections
              (ledger_event_id, target_kind, target_calendar_id,
               desired_state, desired_payload_hash,
               desired_ledger_version,
               current_state, google_event_id, google_etag,
               created_at, updated_at)
           VALUES (?, ?, ?,
                   'absent', 'absent', 1,
                   'present', ?, '',
                   ?, ?)""",
        (
            ledger_id, target_kind, target_calendar_db_id,
            event_id, when, when,
        ),
    )
    proj_row = await (await db.execute(
        """SELECT id FROM ledger_projections
            WHERE ledger_event_id = ? AND target_kind = ?
              AND COALESCE(target_calendar_id, -1) = COALESCE(?, -1)""",
        (ledger_id, target_kind, target_calendar_db_id),
    )).fetchone()
    if proj_row is None:
        return
    await enqueue(
        db,
        user_id=user_id,
        projection_id=int(proj_row["id"]),
        operation=OP_DELETE,
        ledger_version=1,
        target_google_calendar_id=target_google_calendar_id,
        payload=None,
        now=now,
    )
    await db.commit()
