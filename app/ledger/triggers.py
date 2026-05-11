"""Reconcile-request trigger plumbing (REWRITE_PLAN.md §11).

All three sync entry points (webhook, periodic timer, manual
sync) funnel into ``reconcile_requests``.  A single per-user
reconciler coroutine drains.  This module owns the upsert
semantics that collapse multiple notifications into one run.

Debounce values match the existing code:

* Webhook: 5s (let bursts coalesce).
* Manual: 25s (Google's eventual-consistency window).
* Periodic: now (no debounce).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

import aiosqlite

logger = logging.getLogger(__name__)
UTC = timezone.utc

WEBHOOK_DEBOUNCE = timedelta(seconds=5)
MANUAL_SETTLING_DELAY = timedelta(seconds=25)


# ---------------------------------------------------------------------------
# Enqueue
# ---------------------------------------------------------------------------
async def enqueue_webhook(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    source_hint: str,
    now: Optional[datetime] = None,
) -> None:
    """A webhook just told us about a change.  Upsert a debounced
    request — multiple webhooks within the debounce window collapse
    into one reconcile pass.

    ``source_hint`` is a short string like ``"client:7"`` or
    ``"main"`` the reconciler can pass to selective ingest.
    """
    await _upsert_request(
        db,
        user_id=user_id,
        source_hint=source_hint,
        scheduled_for=(now or datetime.now(UTC)) + WEBHOOK_DEBOUNCE,
        prefer_later_schedule=True,
    )


async def enqueue_periodic(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    now: Optional[datetime] = None,
) -> None:
    """The periodic timer fired for this user.  Reconcile every
    source."""
    await _upsert_request(
        db,
        user_id=user_id,
        source_hint="all",
        scheduled_for=now or datetime.now(UTC),
        prefer_later_schedule=False,
    )


async def enqueue_manual(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    source_hint: str = "all",
    now: Optional[datetime] = None,
) -> None:
    """User clicked the Sync button.  Wait the settling delay so
    any in-flight Google writes have time to land."""
    await _upsert_request(
        db,
        user_id=user_id,
        source_hint=source_hint,
        scheduled_for=(now or datetime.now(UTC)) + MANUAL_SETTLING_DELAY,
        prefer_later_schedule=True,
    )


# ---------------------------------------------------------------------------
# Claim
# ---------------------------------------------------------------------------
async def claim_due_request(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    now: Optional[datetime] = None,
) -> Optional[dict]:
    """Atomically grab the user's due reconcile request and mark
    it in-flight.  Returns the source list, or None if nothing is
    due yet.
    """
    now = now or datetime.now(UTC)
    row = await (await db.execute(
        """SELECT user_id, sources_json, scheduled_for, in_flight, last_run_at
             FROM reconcile_requests
            WHERE user_id = ?""",
        (user_id,),
    )).fetchone()
    if row is None or row["in_flight"]:
        return None
    sched = row["scheduled_for"]
    if sched:
        try:
            sched_dt = datetime.fromisoformat(sched)
            if sched_dt.tzinfo is None:
                sched_dt = sched_dt.replace(tzinfo=UTC)
        except ValueError:
            sched_dt = None
        if sched_dt is not None and sched_dt > now:
            return None
    sources = json.loads(row["sources_json"] or "[]") if row["sources_json"] else []
    await db.execute(
        """UPDATE reconcile_requests
              SET in_flight = 1, sources_json = NULL, last_run_at = ?
            WHERE user_id = ?""",
        (now.isoformat(), user_id),
    )
    await db.commit()
    return {"sources": sources}


async def release_request(
    db: aiosqlite.Connection, *, user_id: int,
) -> None:
    """Mark the user's reconcile request as no longer in-flight."""
    await db.execute(
        "UPDATE reconcile_requests SET in_flight = 0 WHERE user_id = ?",
        (user_id,),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Internal
# ---------------------------------------------------------------------------
async def _upsert_request(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    source_hint: str,
    scheduled_for: datetime,
    prefer_later_schedule: bool,
) -> None:
    """Upsert one row in reconcile_requests, merging source hints
    and respecting the debounce window.

    ``prefer_later_schedule=True`` is the debounce path — repeated
    notifications push the scheduled_for further out so a burst
    collapses into one run.  ``prefer_later_schedule=False`` (the
    periodic-timer path) preserves the earliest schedule so we
    don't postpone work indefinitely.
    """
    when_iso = scheduled_for.isoformat()
    existing = await (await db.execute(
        "SELECT * FROM reconcile_requests WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    if existing is None:
        await db.execute(
            """INSERT INTO reconcile_requests
                  (user_id, sources_json, enqueued_at, scheduled_for)
               VALUES (?, ?, ?, ?)""",
            (user_id, json.dumps([source_hint]), when_iso, when_iso),
        )
        await db.commit()
        return

    prior = json.loads(existing["sources_json"] or "[]") if existing["sources_json"] else []
    if "all" in prior or source_hint == "all":
        merged_sources: list = ["all"]
    else:
        merged_sources = sorted(set(prior + [source_hint]))

    new_sched = scheduled_for
    if not prefer_later_schedule and existing["scheduled_for"]:
        try:
            cur = datetime.fromisoformat(existing["scheduled_for"])
            if cur.tzinfo is None:
                cur = cur.replace(tzinfo=UTC)
            if cur < new_sched:
                new_sched = cur
        except ValueError:
            pass
    elif prefer_later_schedule and existing["scheduled_for"]:
        try:
            cur = datetime.fromisoformat(existing["scheduled_for"])
            if cur.tzinfo is None:
                cur = cur.replace(tzinfo=UTC)
            if cur > new_sched:
                new_sched = cur
        except ValueError:
            pass

    await db.execute(
        """UPDATE reconcile_requests
              SET sources_json = ?, scheduled_for = ?, enqueued_at = ?
            WHERE user_id = ?""",
        (json.dumps(merged_sources), new_sched.isoformat(), when_iso, user_id),
    )
    await db.commit()
