"""Reconcile-request trigger plumbing.

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

# An in-flight reconcile claim older than this was almost certainly
# abandoned by a crashed process: a single reconcile pass (ingest +
# plan + diff + bounded outbox drain) never runs this long.
STALE_CLAIM_TIMEOUT = timedelta(minutes=15)


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
    ``"main"``.  It is accepted for the caller's benefit only and is
    never persisted or acted on — the reconciler always reconciles
    every source (see ``_upsert_request`` for why).
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
    any in-flight Google writes have time to land.

    ``source_hint`` is accepted for the caller's benefit only and is
    never persisted or acted on — the reconciler always reconciles
    every source (see ``_upsert_request`` for why)."""
    await _upsert_request(
        db,
        user_id=user_id,
        source_hint=source_hint,
        scheduled_for=(now or datetime.now(UTC)) + MANUAL_SETTLING_DELAY,
        prefer_later_schedule=True,
    )


# ---------------------------------------------------------------------------
# Affected-event recording
# ---------------------------------------------------------------------------
async def record_affected_events(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    ledger_event_ids: Iterable[int],
) -> None:
    """Mark ledger events as needing a replan.

    Every call appends fresh rows to ``affected_ledger_events`` — a
    plain ``INSERT``, never a merge or an INSERT-OR-IGNORE.  So two
    concurrent callers cannot clobber each other (the old
    ``sources_json`` JSON blob did), and re-enqueuing an event that is
    *already* queued is NOT swallowed: it gets a new row with a higher
    id, which the reconciler's "delete only the ids I read" clear
    leaves untouched.  The reconciler reads the rows, plans the
    distinct events, and deletes only the row ids it read.
    """
    for lid in {int(x) for x in ledger_event_ids}:
        await db.execute(
            """INSERT INTO affected_ledger_events
                  (user_id, ledger_event_id) VALUES (?, ?)""",
            (user_id, lid),
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

    The claim is a *conditional* UPDATE checked via ``rowcount`` — its
    WHERE clause itself enforces "claimable" (not in-flight, or an
    abandoned/stale claim).  Two drains racing for the same user —
    in this process or, with SQLite serialising the write, another —
    cannot therefore both win: the loser sees ``rowcount == 0``.  A
    plain select-then-update would let both proceed.
    """
    now = now or datetime.now(UTC)
    row = await (await db.execute(
        """SELECT user_id, sources_json, scheduled_for, in_flight, last_run_at
             FROM reconcile_requests
            WHERE user_id = ?""",
        (user_id,),
    )).fetchone()
    if row is None:
        return None
    was_in_flight = bool(row["in_flight"])
    stale_cutoff = (now - STALE_CLAIM_TIMEOUT).isoformat()
    stale_claim = (
        was_in_flight
        and (row["last_run_at"] is None or row["last_run_at"] < stale_cutoff)
    )

    # Not yet due — a future scheduled_for is a hard pre-condition,
    # independent of any race.  A NULL scheduled_for means idle, except
    # for an abandoned in-flight claim that predates this lifecycle and
    # needs to be taken over.
    sched = row["scheduled_for"]
    if sched is None and not stale_claim:
        return None
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
    # Compare-and-claim: the row is claimable when it is not in-flight,
    # or its in-flight claim is older than STALE_CLAIM_TIMEOUT (a
    # crashed process never released it).
    #
    # sources_json is deliberately NOT cleared here.  It is a legacy
    # column that may still carry integer ledger-event ids staged by
    # pre-cutover code; new writes go to the affected_ledger_events
    # table instead (see record_affected_events), which the
    # reconciler's _read_affected_ledger_rows / _clear_affected_
    # ledger_rows consume once the rows are planned.  Clearing
    # sources_json at claim time silently dropped scheduled admin work.
    cursor = await db.execute(
        """UPDATE reconcile_requests
              SET in_flight = 1,
                  last_run_at = ?,
                  scheduled_for = NULL
            WHERE user_id = ?
              AND (in_flight = 0
                   OR last_run_at IS NULL
                   OR last_run_at < ?)
              AND ((? IS NULL AND scheduled_for IS NULL)
                   OR scheduled_for = ?)""",
        (now.isoformat(), user_id, stale_cutoff, sched, sched),
    )
    await db.commit()
    if (cursor.rowcount or 0) == 0:
        # Either an in-flight claim is still fresh, or another drain
        # won the race.  Nothing claimed.
        return None
    if was_in_flight:
        logger.warning(
            "reclaimed stale in-flight reconcile request for user %s "
            "(claimed at %s)",
            user_id, row["last_run_at"],
        )
    return {"sources": sources}


async def release_request(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    retry_at: Optional[datetime] = None,
) -> None:
    """Mark the user's reconcile request as no longer in-flight.

    ``claim_due_request`` consumes the scheduled timestamp by setting it
    to NULL.  A clean release leaves the row idle unless a webhook or
    manual sync re-enqueued it while the reconcile was running.  On a
    failed reconcile, callers pass ``retry_at`` so an otherwise-idle
    row is retried.
    """
    if retry_at is None:
        await db.execute(
            "UPDATE reconcile_requests SET in_flight = 0 WHERE user_id = ?",
            (user_id,),
        )
    else:
        when_iso = retry_at.isoformat()
        await db.execute(
            """UPDATE reconcile_requests
                  SET in_flight = 0,
                      scheduled_for = COALESCE(scheduled_for, ?),
                      enqueued_at = CASE
                          WHEN scheduled_for IS NULL THEN ?
                          ELSE enqueued_at
                      END
                WHERE user_id = ?""",
            (when_iso, when_iso, user_id),
        )
    await db.commit()


async def reclaim_stale_requests(
    db: aiosqlite.Connection, *, now: Optional[datetime] = None,
) -> int:
    """Reset in-flight reconcile requests abandoned by a crashed
    process so the drain loop can pick them up again.

    ``claim_due_request`` sets ``in_flight = 1``; a clean run clears it
    via ``release_request``.  A process that dies mid-reconcile leaves
    the row stuck — and ``drain_all_due_users`` only ever SELECTs
    ``in_flight = 0`` rows, so that user would never reconcile again.
    This sweeper clears any claim older than ``STALE_CLAIM_TIMEOUT``.
    Returns the number of rows reclaimed.
    """
    now = now or datetime.now(UTC)
    cutoff = (now - STALE_CLAIM_TIMEOUT).isoformat()
    cursor = await db.execute(
        """UPDATE reconcile_requests
              SET in_flight = 0,
                  scheduled_for = COALESCE(scheduled_for, ?)
            WHERE in_flight = 1
              AND (last_run_at IS NULL OR last_run_at < ?)""",
        (now.isoformat(), cutoff),
    )
    await db.commit()
    n = cursor.rowcount or 0
    if n:
        logger.warning("reclaimed %s stale reconcile request(s)", n)
    return n


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
    """Upsert one row in reconcile_requests, respecting the debounce
    window.

    ``prefer_later_schedule=True`` is the debounce path — repeated
    notifications push the scheduled_for further out so a burst
    collapses into one run.  ``prefer_later_schedule=False`` (the
    periodic-timer path) preserves the earliest schedule so we
    don't postpone work indefinitely.

    ``sources_json`` is deliberately NOT written here.  That legacy
    column once carried the *integer ledger-event ids* staged by the
    ingest layer and admin ops (both now append to the
    ``affected_ledger_events`` table via ``record_affected_events``
    instead).  This trigger path used to merge a *string*
    ``source_hint`` into the same column — and a later
    ``sorted(set(prior + [hint]))`` would then raise ``TypeError`` the
    moment ints and strings coexisted.  The hint is kept in the
    signature for callers / logging but is never persisted; the
    reconciler always re-plans every dirty ledger row regardless.
    """
    when_iso = scheduled_for.isoformat()
    existing = await (await db.execute(
        "SELECT scheduled_for FROM reconcile_requests WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    if existing is None:
        await db.execute(
            """INSERT INTO reconcile_requests
                  (user_id, sources_json, enqueued_at, scheduled_for)
               VALUES (?, NULL, ?, ?)""",
            (user_id, when_iso, when_iso),
        )
        await db.commit()
        return

    new_sched = scheduled_for
    if existing["scheduled_for"]:
        try:
            cur = datetime.fromisoformat(existing["scheduled_for"])
            if cur.tzinfo is None:
                cur = cur.replace(tzinfo=UTC)
            if not prefer_later_schedule and cur < new_sched:
                new_sched = cur
            elif prefer_later_schedule and cur > new_sched:
                new_sched = cur
        except ValueError:
            pass

    await db.execute(
        """UPDATE reconcile_requests
              SET scheduled_for = ?, enqueued_at = ?
            WHERE user_id = ?""",
        (new_sched.isoformat(), when_iso, user_id),
    )
    await db.commit()
