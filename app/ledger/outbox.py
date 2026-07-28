"""Outbox drain: pending writes → Google, with idempotent retry.

The drain is deliberately simple: pick the oldest pending op,
try it once, update state, repeat.  Idempotency is provided by
two structural choices:

1. **Inserts** carry a deterministic ``id`` derived from the
   projection ID (see :func:`identity.derive_google_event_id`).
   A retry hits the per-calendar uniqueness constraint and
   returns 409 Conflict.  We treat 409 as success after a
   confirming GET that the stored event matches our hash.

2. **Updates** carry the ``If-Match`` etag we last observed.
   A retry that races with a concurrent edit returns 412
   Precondition Failed; we mark the op superseded and let the
   planner re-derive from fresh state.

Errors are routed by class:

* :class:`Exception` whose status is 400/401/403/404 → permanent
  failure after ``POISON_PILL_THRESHOLD`` attempts.
* deterministic local failures — :class:`ValueError` (including
  ``json.JSONDecodeError`` from a malformed ``payload_json``) with
  no HTTP status — → same permanent-failure ceiling; retrying an
  op that is structurally broken can never succeed.
* status 408/429/500/502/503/504 or any non-HTTP transport
  exception → retry with exponential backoff, unbounded (a network
  blip must never poison-pill real work).
* status 412 (etag mismatch) → mark superseded, schedule replan.

The retry timing matches the existing
``app/sync/google_calendar.py`` curve so production behaviour
under load doesn't change at cutover.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiosqlite

from app.config import get_settings
from app.ledger.async_google import as_async_google
from app.ledger.google_client import GoogleClient
from app.ledger.identity import (
    derive_google_event_id,
    derive_instance_google_event_id,
)

logger = logging.getLogger(__name__)

UTC = timezone.utc

# Statuses
STATUS_PENDING = "pending"
STATUS_IN_FLIGHT = "in_flight"
STATUS_DONE = "done"
STATUS_PERMANENT_FAILURE = "permanent_failure"
STATUS_SUPERSEDED = "superseded"

# Operations
OP_CREATE = "create"
OP_UPDATE = "update"
OP_DELETE = "delete"
# Field-scoped patch (events.patch) of the user's edits back to the
# calendar that sourced the event.
OP_PATCH = "patch"
# Destructive delete of a single source occurrence — the user
# cancelled one occurrence of a managed recurring copy on the main
# calendar.  Distinct from OP_DELETE (which removes our own writes by
# projection google_event_id): this addresses the user's REAL source
# event by the ledger row's source_event_id, so it is kept explicit
# rather than overloading the ordinary writeback or delete path.
OP_DELETE_SOURCE = "delete_source"

# Failure handling
POISON_PILL_THRESHOLD = 5
PERMANENT_FAILURE_STATUSES = frozenset({400, 401, 403, 404})

# Google encodes quota / rate-limit errors as one of these reason
# codes — carried in an HTTP 403 (classic) or 429 response.  They
# are always transient and must never be poison-pilled.
_RATE_LIMIT_TOKENS = (
    "ratelimitexceeded",
    "userratelimitexceeded",
    "quotaexceeded",
    "dailylimitexceeded",
)


def _is_rate_limited(error: Exception) -> bool:
    """True for a Google quota / rate-limit response.  Status alone
    is not enough: real Google returns 403 with a structured reason
    for quota as often as 429.  ``RealGoogleClient`` surfaces the
    reason code in the error message; the fake uses 429."""
    if getattr(error, "status", None) == 429:
        return True
    haystack = (
        str(error) + " " + str(getattr(error, "reason", "") or "")
    ).lower()
    return any(tok in haystack for tok in _RATE_LIMIT_TOKENS)

# Backoff schedule (seconds): roughly the curve used today in
# app/sync/google_calendar.py.  Capped at 60s.
_BACKOFF_SECONDS = (1, 2, 4, 8, 16, 32, 60)

# An op in_flight longer than this was abandoned by a crashed drain:
# a single op is one Google API call.  Past it the op is reclaimed.
_STALE_OP_TIMEOUT = timedelta(minutes=15)

# Upper bound on how many fresh deterministic ids _do_create will
# derive past cancelled tombstones within a SINGLE drain attempt.
_MAX_ID_GENERATIONS = 8

# Absolute ceiling on a projection's *persisted* google_id_generation.
# _MAX_ID_GENERATIONS only bounds one drain attempt's inner loop, but the
# generation is persisted and resumed across attempts.  Without a global
# cap, a projection whose every derived id collides with a cancelled
# tombstone burns ids forever — observed climbing into the thousands,
# re-burning ~hundreds of insert+GET calls per drain while the real event
# is never mirrored.  Past this cap we stop, mark the op a permanent
# failure, and alert: a real event that cannot be created needs operator
# attention, not an unbounded loop.  Recover via the admin retry-failed
# action once the root cause is addressed.
_MAX_TOTAL_ID_GENERATIONS = 50


class OutboxDrainError(Exception):
    """Raised when the drain could not even read the queue."""


async def _global_sync_paused(db: aiosqlite.Connection) -> bool:
    """True when the admin 'pause everything' emergency stop is set.

    Mirrors the GLOBAL branch of reconciler._pause_mode (the ``settings``
    row keyed ``sync_paused``).  Read directly here so the drain enforces
    the kill switch on the actual write path; per-user soft pauses are
    intentionally not consulted (they keep draining).  Tolerant of a
    minimal test DB without the settings table — and of NOTHING else.
    """
    try:
        row = await (await db.execute(
            "SELECT value_plain FROM settings WHERE key = 'sync_paused'",
        )).fetchone()
    except sqlite3.OperationalError as e:
        # ONLY the missing-table case is tolerated (minimal test DBs
        # omit the settings table).  Any other failure — locked DB,
        # disk I/O error, corruption — must propagate: this read guards
        # the admin emergency stop on the one path that writes to
        # Google, and swallowing a real error would silently fail OPEN,
        # draining the backlog out while the operator believes sync is
        # paused.  Aborting the drain (fail closed) is the safe answer.
        if "no such table" not in str(e).lower():
            raise
        return False
    return bool(row and row["value_plain"] == "true")


# ---------------------------------------------------------------------------
# Enqueue
# ---------------------------------------------------------------------------
async def enqueue(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    projection_id: int,
    operation: str,
    ledger_version: int,
    target_google_calendar_id: str,
    payload: Optional[dict],
    desired_payload_hash: Optional[str] = None,
    now: Optional[datetime] = None,
) -> int:
    """Insert one outbox row, superseding any older pending ops
    for the same projection.

    The idempotency key is shaped so a re-derivation of the same
    (projection, version, operation) produces the same key — the
    UNIQUE constraint then prevents a second copy.

    Returns the new outbox row id.
    """
    if operation not in (
        OP_CREATE, OP_UPDATE, OP_DELETE, OP_PATCH, OP_DELETE_SOURCE,
    ):
        raise ValueError(f"unknown operation: {operation!r}")
    when = (now or datetime.now(UTC)).isoformat()
    idem = f"proj:{projection_id}:v{ledger_version}:{operation}"

    # If an op with this exact idempotency key is already pending,
    # the caller is re-asking for the same work — no-op.  Without
    # this check the supersede-below would knock it out of the
    # queue without a successor.
    same_op = await (await db.execute(
        "SELECT id, status FROM outbox_operations WHERE idempotency_key = ?",
        (idem,),
    )).fetchone()
    if same_op is not None and same_op["status"] in (STATUS_PENDING, STATUS_IN_FLIGHT):
        return int(same_op["id"])

    # Supersede any pending op for the same projection with a
    # different idempotency key.  In-flight ops are left alone —
    # the drain will discover their work is stale via etag
    # mismatch on the next attempt.
    await db.execute(
        """UPDATE outbox_operations
              SET status = ?, completed_at = ?
            WHERE projection_id = ?
              AND status = ?
              AND idempotency_key != ?""",
        (STATUS_SUPERSEDED, when, projection_id, STATUS_PENDING, idem),
    )

    payload_json = json.dumps(payload, sort_keys=True) if payload is not None else None

    cursor = await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, desired_payload_hash,
               target_google_calendar_id,
               payload_json, status, attempts, next_attempt_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?)
           ON CONFLICT(idempotency_key) DO NOTHING""",
        (
            user_id, projection_id, operation, idem,
            ledger_version, desired_payload_hash, target_google_calendar_id,
            payload_json, STATUS_PENDING, when, when,
        ),
    )
    if cursor.rowcount > 0:
        return cursor.lastrowid
    # Conflict resurrection: a prior op with the same key exists
    # but is done/superseded.  Bring it back to pending so the
    # drain retries.
    await db.execute(
        """UPDATE outbox_operations
              SET status = ?, attempts = 0, next_attempt_at = ?,
                  last_error = NULL, last_http_status = NULL,
                  payload_json = ?, started_at = NULL, completed_at = NULL,
                  ledger_version_at_enqueue = ?, desired_payload_hash = ?
            WHERE idempotency_key = ?""",
        (STATUS_PENDING, when, payload_json, ledger_version,
         desired_payload_hash, idem),
    )
    row = await (await db.execute(
        "SELECT id FROM outbox_operations WHERE idempotency_key = ?",
        (idem,),
    )).fetchone()
    return int(row["id"])


# ---------------------------------------------------------------------------
# Drain
# ---------------------------------------------------------------------------
async def drain_user(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    now: Optional[datetime] = None,
    max_ops: int = 1000,
) -> dict:
    """Drain all due outbox ops for one user.

    Stops when:
    * no pending op is due (``next_attempt_at <= now``), OR
    * ``max_ops`` ops have been processed (a guard for soak tests),
      OR
    * an unrecoverable error reading the queue is hit.

    Returns a dict of counters: ``{processed, succeeded, retried,
    failed_permanent, superseded}``.
    """
    # Google calls are offloaded to worker threads (see async_google)
    # so a slow request cannot block the event loop.  Idempotent.
    google = as_async_google(google)
    now = now or datetime.now(UTC)
    counters = {
        "processed": 0,
        "succeeded": 0,
        "retried": 0,
        "failed_permanent": 0,
        "superseded": 0,
    }

    # Kill-switch, defence in depth.  Every current caller already runs
    # through reconcile_user, which returns early on a GLOBAL pause before
    # reaching here — but the drain is the one place that actually writes
    # to Google, so it enforces the global "pause everything" stop itself
    # too.  No future caller can then bypass the emergency stop, and if
    # the operator flips the pause to halt a runaway, queued ops stop
    # flowing at the very next drain rather than draining the backlog out.
    # Per-USER soft pauses are deliberately NOT consulted here: those keep
    # draining so staged cleanup converges (see reconciler._pause_mode).
    if await _global_sync_paused(db):
        logger.info(
            "drain_user: skipping user %s — global sync pause is active",
            user_id,
        )
        return counters

    # Free any op a crashed drain left stuck in_flight; _claim_next
    # only ever selects pending rows, so without this such an op is
    # never retried.
    await _reclaim_stale_operations(db, user_id=user_id, now=now)

    for _ in range(max_ops):
        op = await _claim_next(db, user_id=user_id, now=now)
        if op is None:
            break
        counters["processed"] += 1
        try:
            outcome = await _execute_op(db, google, op, now=now)
        except Exception as e:  # pragma: no cover - defensive
            logger.exception("outbox drain crashed on op %s", op["id"])
            await _mark_retry(db, op, error=str(e), http_status=None, now=now)
            counters["retried"] += 1
            continue
        counters[outcome] = counters.get(outcome, 0) + 1
    return counters


async def _reclaim_stale_operations(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    now: datetime,
) -> int:
    """Reset outbox ops stuck ``in_flight`` by a crashed drain back to
    ``pending`` so they are retried.

    ``_claim_next`` only ever selects ``pending`` rows; an op claimed
    (status ``in_flight``, ``started_at`` stamped) whose drain then
    died is otherwise stranded forever.  Anything in-flight longer
    than ``_STALE_OP_TIMEOUT`` — far longer than one Google call — is
    treated as abandoned.  ``attempts`` was already bumped at claim
    time, so the reclaimed op simply re-enters the queue.  Returns the
    number reclaimed.
    """
    cutoff = (now - _STALE_OP_TIMEOUT).isoformat()
    cursor = await db.execute(
        """UPDATE outbox_operations
              SET status = ?
            WHERE user_id = ?
              AND status = ?
              AND (started_at IS NULL OR started_at < ?)""",
        (STATUS_PENDING, user_id, STATUS_IN_FLIGHT, cutoff),
    )
    await db.commit()
    n = cursor.rowcount or 0
    if n:
        logger.warning(
            "reclaimed %s stale in-flight outbox op(s) for user %s", n, user_id,
        )
    return n


async def _claim_next(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    now: datetime,
) -> Optional[aiosqlite.Row]:
    """Atomically pull the oldest due pending op and mark it in-flight.

    The claim is a *conditional* UPDATE (``WHERE id = ? AND status =
    'pending'``) checked via ``rowcount`` — not a select-then-update.
    That makes it safe even if two drains race for the same op, in
    this process or (with SQLite serialising the write) another: the
    loser sees ``rowcount == 0`` and claims nothing.
    """
    row = await (await db.execute(
        """SELECT * FROM outbox_operations
            WHERE user_id = ?
              AND status = ?
              AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
            ORDER BY id LIMIT 1""",
        (user_id, STATUS_PENDING, now.isoformat()),
    )).fetchone()
    if row is None:
        return None
    cursor = await db.execute(
        """UPDATE outbox_operations
              SET status = ?, started_at = ?, attempts = attempts + 1
            WHERE id = ? AND status = ?""",
        (STATUS_IN_FLIGHT, now.isoformat(), row["id"], STATUS_PENDING),
    )
    await db.commit()
    if (cursor.rowcount or 0) == 0:
        # Another claimant won the row between our SELECT and UPDATE.
        return None
    return row


async def _execute_op(
    db: aiosqlite.Connection,
    google: GoogleClient,
    op: aiosqlite.Row,
    *,
    now: datetime,
) -> str:
    """Run one op against Google.  Returns one of:
    'succeeded', 'retried', 'failed_permanent', 'superseded'.

    Every ``_do_*`` handler returns the outcome it recorded, and that
    return value is propagated verbatim — there is deliberately no
    blanket ``return "succeeded"`` fall-through.  A handler can resolve
    an op *internally* to something other than success (``_do_create``
    marks the op superseded on the etag_mismatch_on_409 race, and a
    permanent failure when it gives up on burned ids); flattening those
    to "succeeded" made drain counters lie — ``failed_permanent``
    stayed 0 for the burned-id give-up (the case that should alert
    operators) and the reconciler's ``superseded`` convergence signal
    undercounted, so its fixed-point loop could stop a pass early.
    """
    operation = op["operation"]
    cal_id = op["target_google_calendar_id"]

    try:
        # Parsed INSIDE the try: a malformed payload_json (corrupt row,
        # partial write) must become a *classified* failure — routed to
        # the deterministic-failure ceiling in _classify_and_retry —
        # rather than escape _execute_op into drain_user's defensive
        # catch-all, which retries with no attempts ceiling forever.
        payload = json.loads(op["payload_json"]) if op["payload_json"] else None
        if operation == OP_CREATE:
            return await _do_create(db, google, op, cal_id, payload, now=now)
        if operation == OP_UPDATE:
            return await _do_update(db, google, op, cal_id, payload, now=now)
        if operation == OP_DELETE:
            return await _do_delete(db, google, op, cal_id, now=now)
        if operation == OP_PATCH:
            return await _do_patch(db, google, op, cal_id, payload, now=now)
        if operation == OP_DELETE_SOURCE:
            return await _do_delete_source(db, google, op, cal_id, now=now)
        raise ValueError(f"unknown operation: {operation!r}")
    except Exception as e:
        return await _classify_and_retry(db, op, e, now=now)


async def _do_create(
    db: aiosqlite.Connection,
    google: GoogleClient,
    op: aiosqlite.Row,
    cal_id: str,
    payload: Optional[dict],
    *,
    now: datetime,
) -> str:
    """Insert with a deterministic ID; treat 409 as success.

    If the 409 turns out to be a *cancelled tombstone* — a user
    deleted one of our events, so Google keeps that id permanently
    reserved — the deterministic id is burned: it can never be
    re-inserted and ``events.update`` on it is unreliable.  We then
    bump the projection's ``google_id_generation``, which yields a
    fresh deterministic id, and retry the insert.  Bumping is
    persisted before each retry so a crash mid-recreate is idempotent.

    Returns the outcome it recorded ('succeeded', 'superseded', or
    'failed_permanent') so drain counters reflect what actually
    happened — the give-up and supersede paths resolve the op
    internally and return normally, so a caller cannot infer the
    outcome from "didn't raise".
    """
    if payload is None:
        raise ValueError(f"create op {op['id']} has no payload")
    proj = await _get_projection(db, op["projection_id"])
    generation = int(proj["google_id_generation"] or 0)
    # The ceiling is PER EPISODE: (generation - floor), where a
    # successful create advances the floor.  Routine absent->present
    # toggles burn one generation each by design (our own delete leaves
    # a cancelled tombstone at the old id, so the recreate collides once
    # and bumps) — a lifetime cap therefore falsely bricked long-lived
    # flapping events (observed at generation 3000+ in production, with
    # the admin retry insta-failing on the entry check below).
    floor = int(proj["google_id_generation_floor"] or 0)
    body = dict(payload)

    # Ceiling: a resumed op whose persisted generation already burned a
    # whole episode's worth of ids stops here rather than burning more.
    if generation - floor >= _MAX_TOTAL_ID_GENERATIONS:
        await _give_up_burned_ids(db, op, generation=generation, now=now)
        return "failed_permanent"

    for _ in range(_MAX_ID_GENERATIONS):
        google_id = derive_google_event_id(
            int(op["projection_id"]), generation,
        )
        body["id"] = google_id
        try:
            result = await google.insert_event(cal_id, body)
        except Exception as e:
            if getattr(e, "status", None) != 409:
                raise
            # The ID is already on Google.  Fetch it to tell the cases
            # apart — but the GET itself may 404/410 if the id is a
            # burned/reserved tombstone Google will not surface.
            burned = False
            try:
                existing = await google.get_event(cal_id, google_id)
            except Exception as ge:
                if getattr(ge, "status", None) not in (404, 410):
                    raise
                # insert said the id is taken, yet GET says it is gone
                # — a reserved tombstone.  Treat exactly like a
                # cancelled one: burn this generation.
                existing = {}
                burned = True
            if burned or existing.get("status") == "cancelled":
                # The id is burned (cancelled tombstone, or reserved).
                # Move to a fresh deterministic id and retry — unless we
                # have hit the absolute ceiling, in which case give up and
                # alert instead of burning ids forever.
                generation += 1
                if generation - floor >= _MAX_TOTAL_ID_GENERATIONS:
                    await _give_up_burned_ids(
                        db, op, generation=generation, now=now,
                    )
                    return "failed_permanent"
                await db.execute(
                    """UPDATE ledger_projections
                          SET google_id_generation = ?
                        WHERE id = ?""",
                    (generation, int(op["projection_id"])),
                )
                await db.commit()
                continue
            # A live event sits at our id — a retry of our own write,
            # or a concurrent edit of our copy.  We own this id by
            # construction; UPDATE to restore the canonical payload.
            try:
                result = await google.update_event(
                    cal_id, google_id, payload,
                    if_match=existing.get("etag"),
                )
            except Exception as e2:
                if getattr(e2, "status", None) == 412:
                    # Lost the race; supersede + replan.
                    await _mark_superseded(
                        db, op,
                        error="etag_mismatch_on_409",
                        http_status=412,
                        now=now,
                    )
                    await _request_projection_replan(db, op["projection_id"])
                    return "superseded"
                raise
        # A converged create ends the ceiling episode: advance the floor
        # to the generation that actually landed, so the NEXT episode
        # (the next delete/recreate toggle) gets its own full budget
        # instead of inheriting this one's burn count.
        await db.execute(
            """UPDATE ledger_projections
                  SET google_id_generation = ?,
                      google_id_generation_floor = ?
                WHERE id = ?""",
            (generation, generation, int(op["projection_id"])),
        )
        await _record_success(
            db, op,
            google_event_id=result["id"],
            google_etag=result.get("etag", ""),
            now=now,
        )
        return "succeeded"

    raise RuntimeError(
        f"create op {op['id']}: exhausted {_MAX_ID_GENERATIONS} id "
        f"generations for projection {op['projection_id']}"
    )


async def _do_update(
    db: aiosqlite.Connection,
    google: GoogleClient,
    op: aiosqlite.Row,
    cal_id: str,
    payload: Optional[dict],
    *,
    now: datetime,
) -> str:
    """Etag-gated update; on 412 mark superseded for replan."""
    if payload is None:
        raise ValueError(f"update op {op['id']} has no payload")
    proj = await _get_projection(db, op["projection_id"])
    if not proj["google_event_id"]:
        raise ValueError(
            f"update op {op['id']} has no google_event_id on its projection — "
            "the create must succeed first"
        )
    try:
        result = await google.update_event(
            cal_id,
            proj["google_event_id"],
            payload,
            if_match=proj["google_etag"] or None,
        )
    except Exception as e:
        if getattr(e, "status", None) == 412:
            # Refresh our etag from Google so the next attempt sends
            # the correct If-Match.  Without this, we'd ping-pong on
            # 412 forever.
            try:
                fresh = await google.get_event(cal_id, proj["google_event_id"])
                await db.execute(
                    """UPDATE ledger_projections
                          SET google_etag = ?
                        WHERE id = ?""",
                    (fresh.get("etag", ""), int(proj["id"])),
                )
                await db.commit()
            except Exception:
                pass  # best-effort; drain will retry
            await _mark_superseded(
                db, op,
                error="etag_mismatch",
                http_status=412,
                now=now,
            )
            await _request_projection_replan(db, op["projection_id"])
            return "superseded"
        if getattr(e, "status", None) in (404, 410):
            # The event we meant to update is gone — a user deleted
            # our managed copy.  Reset the projection so the next diff
            # re-CREATEs it, rather than poison-pilling the update
            # (404 is otherwise a permanent-failure status).
            await db.execute(
                """UPDATE ledger_projections
                      SET current_state = 'absent',
                          google_event_id = NULL,
                          google_etag = NULL,
                          applied_ledger_version = NULL,
                          applied_payload_hash = NULL
                    WHERE id = ?""",
                (int(proj["id"]),),
            )
            await db.commit()
            await _mark_superseded(
                db, op,
                error="target_event_gone",
                http_status=int(getattr(e, "status", 404)),
                now=now,
            )
            await _request_projection_replan(db, op["projection_id"])
            return "superseded"
        if getattr(e, "status", None) == 400:
            # A status:confirmed "revive" UPDATE on a derived instance id
            # can hit an out-of-range, DETACHED cancelled tombstone.  When
            # a recurring source is split "this and following", BusyBridge
            # shrinks the old managed master's RRULE and creates a newer
            # keeper segment that now owns the later dates — but the old
            # exception-instance projection still derives
            # <old_master>_<date> for a date the bounded master no longer
            # generates.  Google keeps that id only as a cancelled
            # exception with no recurringEventId, and rejects un-cancelling
            # an occurrence the parent no longer contains with 400 — every
            # retry, forever (poison-pilling it as a permanent failure).
            # Retire the orphaned projection instead: the keeper segment
            # already holds the correct copy, so its true desired state is
            # absent.  Any OTHER 400 (a genuinely malformed payload) falls
            # through to the poison-pill path so real bugs still surface.
            if await _retire_orphaned_instance_tombstone(
                db, google, proj, cal_id, op, now=now,
            ):
                return "superseded"
        raise
    await _record_success(
        db, op,
        google_event_id=result["id"],
        google_etag=result.get("etag", ""),
        now=now,
    )
    return "succeeded"


async def _retire_orphaned_instance_tombstone(
    db: aiosqlite.Connection,
    google: GoogleClient,
    proj: aiosqlite.Row,
    cal_id: str,
    op: aiosqlite.Row,
    *,
    now: datetime,
) -> bool:
    """Converge an orphaned out-of-range instance projection to absent.

    Returns ``True`` (and supersedes the op) only when a 400 on an
    instance UPDATE matches the un-revivable tombstone signature:

    * the projection is a recurring-instance row (its ledger event has a
      ``parent_canonical_uid``), AND
    * the target Google event is a DETACHED cancelled tombstone — its
      ``status`` is ``cancelled`` and it has no ``recurringEventId`` (the
      now-bounded master no longer generates this occurrence), which is
      why Google rejects the status:confirmed revive with 400.

    Otherwise returns ``False`` so the caller re-raises and the existing
    poison-pill path handles a genuinely-bad-payload 400.  The keeper
    segment created by the split already holds the correct copy for this
    date, so the orphaned projection's true desired state is ``absent``;
    converging it (rather than poison-pilling) stops the endless retry.
    The convergence uses the codebase's absent-projection convention —
    ``desired_payload_hash`` and ``applied_payload_hash`` both set to the
    ``'absent'`` sentinel (cf. ``planner._mark_implicit_absent``) with
    ``applied_ledger_version == desired_ledger_version`` — so the row is
    immediately quiescent: the diff's divergence query does not re-select
    it, no follow-up op is enqueued, and ``google_event_id`` is cleared so
    no live Google event is ever touched (the cancelled tombstone simply
    stays cancelled).
    """
    gid = proj["google_event_id"]
    if not gid:
        return False
    # Only an instance projection can hit the _R-split id-drift tombstone.
    row = await (await db.execute(
        """SELECT e.id AS ledger_event_id, e.user_id, e.status,
                  e.parent_canonical_uid
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE p.id = ?""",
        (int(proj["id"]),),
    )).fetchone()
    if row is None or not row["parent_canonical_uid"]:
        return False
    # Confirm the target really is a detached cancelled tombstone before
    # giving up on it; a 400 on a still-live event is a different bug.
    try:
        target = await google.get_event(cal_id, gid)
    except Exception:
        return False  # cannot confirm — let the normal 400 path decide
    if target.get("status") != "cancelled" or target.get("recurringEventId"):
        return False
    await db.execute(
        """UPDATE ledger_projections
              SET desired_state = 'absent',
                  current_state = 'absent',
                  desired_payload_hash = 'absent',
                  applied_payload_hash = 'absent',
                  applied_ledger_version = desired_ledger_version,
                  google_event_id = NULL,
                  google_etag = NULL,
                  permanently_failed = 0,
                  last_error = NULL,
                  updated_at = ?
            WHERE id = ?""",
        (now.isoformat(), int(proj["id"])),
    )
    # Persist the verdict on the LEDGER ROW too, not just this projection.
    # The tombstone GET is positive proof the bounded master no longer
    # generates this occurrence, so the instance row's true state is
    # cancelled.  Leaving it 'active' re-armed the loop this function
    # exists to stop: the next parent replan overwrote desired_state back
    # to present (planner._upsert_projection), the diff re-derived the id,
    # and every parent edit cost UPDATE(400) + confirming GET + retire —
    # per target calendar, forever (observed daily in production).  With
    # the row cancelled the planner forces ABSENT for every target on all
    # future replans.  Safety: if the source ever re-delivers this
    # occurrence live under the same parent, client ingest's
    # modified-instance path resurrects status='active' and the mirror
    # returns — a wrong retire self-heals.
    if row["status"] == "active":
        await db.execute(
            """UPDATE ledger_events
                  SET status = 'cancelled',
                      cancelled_at = ?,
                      updated_at = ?,
                      version = version + 1
                WHERE id = ? AND status = 'active'""",
            (now.isoformat(), now.isoformat(), int(row["ledger_event_id"])),
        )
        # Queue the row for replan so the SIBLING projections (other
        # target calendars churning on the same orphaned occurrence)
        # converge to absent on the next pass instead of each burning
        # their own 400+GET+retire round.
        await db.execute(
            """INSERT INTO affected_ledger_events
                  (user_id, ledger_event_id) VALUES (?, ?)""",
            (int(row["user_id"]), int(row["ledger_event_id"])),
        )
    await db.commit()
    await _mark_superseded(
        db, op,
        error="instance_tombstone_out_of_range",
        http_status=400,
        now=now,
    )
    logger.info(
        "outbox: retired orphaned out-of-range instance projection %s — "
        "google_event_id %s is a detached cancelled tombstone (its managed "
        "master was re-bounded by a this-and-following split; the keeper "
        "segment owns this date), converged to absent instead of poison-pill",
        int(proj["id"]), gid,
    )
    return True


async def _do_delete(
    db: aiosqlite.Connection,
    google: GoogleClient,
    op: aiosqlite.Row,
    cal_id: str,
    *,
    now: datetime,
) -> str:
    """Idempotent delete: 404/410 are treated as success.

    The delete is intentionally *unconditional* — no ``If-Match``.
    The planner has decided this projection's desired state is
    ``absent``; the event must go regardless of whatever revision
    currently sits on Google.  Sending the stored ETag would only
    invite a 412 ping-pong (event changed since we last saw it) for
    no benefit, since the outcome we want is "gone" either way.

    Always returns 'succeeded' — unlike ``_do_create``/``_do_update``
    there is no internal supersede / give-up path here; every
    non-success raises and is classified by the caller.
    """
    proj = await _get_projection(db, op["projection_id"])
    if not proj["google_event_id"]:
        # Never created — nothing to delete.  Mark projection
        # absent and consider done.
        await _record_absent(db, op, now=now)
        return "succeeded"
    try:
        await google.delete_event(cal_id, proj["google_event_id"])
    except Exception as e:
        if getattr(e, "status", None) in (404, 410):
            pass  # already gone
        else:
            raise
    await _record_absent(db, op, now=now)
    return "succeeded"


async def _do_delete_source(
    db: aiosqlite.Connection,
    google: GoogleClient,
    op: aiosqlite.Row,
    cal_id: str,
    *,
    now: datetime,
) -> str:
    """Destructively delete on the user's real source calendar — either a
    single occurrence of a managed recurring copy, or a whole non-recurring
    event (Phase-1 organizer-delete propagation).

    The target is addressed by the ledger row's ``source_event_id``: for a
    recurring instance that is ``<series>_<stamp>`` (exactly that occurrence,
    never the parent series); for a non-recurring event it is the event's own
    id (the whole event).  Only ever reached behind the
    ``source_delete_pending`` gate in ``diff._decide``.  Idempotent: a 404/410
    means it is already gone, which is the goal.  Personal sources are
    read-only and short-circuit without calling Google.

    Always returns 'succeeded' — like ``_do_delete``, every non-success
    path raises and is classified by the caller.
    """
    proj = await _get_projection(db, op["projection_id"])
    led = await (await db.execute(
        "SELECT id, source_type, source_event_id FROM ledger_events WHERE id = ?",
        (int(proj["ledger_event_id"]),),
    )).fetchone()
    if led is not None and led["source_type"] == "personal":
        # Personal calendars are read-only sources.  Older versions
        # could enqueue source-delete work for them; drain those rows
        # as satisfied without calling Google.
        await db.execute(
            "UPDATE ledger_events SET source_delete_pending = 0 WHERE id = ?",
            (int(led["id"]),),
        )
        await _record_absent(db, op, now=now)
        return "succeeded"
    if led is not None and led["source_event_id"]:
        try:
            await google.delete_event(cal_id, led["source_event_id"])
        except Exception as e:
            if getattr(e, "status", None) not in (404, 410):
                raise
    # The destructive delete has landed (or the occurrence was
    # already gone) — clear the flag so a later reconcile does not
    # re-delete.
    if led is not None:
        await db.execute(
            "UPDATE ledger_events SET source_delete_pending = 0 WHERE id = ?",
            (int(led["id"]),),
        )
    await _record_absent(db, op, now=now)
    return "succeeded"


async def _do_patch(
    db: aiosqlite.Connection,
    google: GoogleClient,
    op: aiosqlite.Row,
    cal_id: str,
    payload: Optional[dict],
    *,
    now: datetime,
) -> str:
    """Write the user's edits back onto the calendar that sourced
    the event.

    The target is the user's real source event, addressed by the
    ledger row's ``source_event_id`` — NOT a projection
    ``google_event_id``.  The origin writeback projection
    deliberately keeps ``google_event_id`` NULL so ingest's
    loop-prevention does not mistake the source event for one of our
    writes.

    ``events.patch`` is field-scoped (only the keys in the payload
    are sent), so no other field of the source event is clobbered.
    A 404/410 (source event gone) is treated as success — there is
    nothing left to write back.

    Notifications: this is the ONE write that may notify.  When the
    ``writeback_notifications`` setting is on, the patch carries
    ``sendUpdates='all'`` so the organizer receives Google's standard
    accepted/declined email (and guests are notified of time/detail
    changes on editable events) — exactly as if the user had responded
    on the client calendar directly.  Every other outbox write targets
    our own managed copies and stays silent unconditionally.
    """
    if payload is None:
        raise ValueError(f"patch op {op['id']} has no payload")
    proj = await _get_projection(db, op["projection_id"])
    led = await (await db.execute(
        "SELECT source_type, source_event_id FROM ledger_events WHERE id = ?",
        (int(proj["ledger_event_id"]),),
    )).fetchone()
    if led is not None and led["source_type"] == "personal":
        # Legacy pending personal writebacks must not hit Google: the
        # personal token intentionally has calendar.readonly scope.
        await _record_origin_writeback_applied(db, op, now=now)
        return "succeeded"
    if led is None or not led["source_event_id"]:
        await _record_origin_writeback_applied(db, op, now=now)
        return "succeeded"
    # Read the setting at EXECUTION time, not enqueue time: flipping
    # it takes effect for already-queued writebacks on the next drain.
    # A retried patch re-sends the notification — acceptable and
    # bounded (the retry schedule is backed off, and a duplicate
    # organizer email is far better than a silently-dropped RSVP), so
    # no dedup machinery here.
    send_updates = "all" if get_settings().writeback_notifications else None
    try:
        await google.patch_event(
            cal_id, led["source_event_id"], payload,
            send_updates=send_updates,
        )
    except Exception as e:
        if getattr(e, "status", None) in (404, 410):
            await _record_origin_writeback_applied(db, op, now=now)
            return "succeeded"
        raise
    await _record_origin_writeback_applied(db, op, now=now)
    return "succeeded"


# ---------------------------------------------------------------------------
# State updates
# ---------------------------------------------------------------------------
async def _applied_hash_for(db: aiosqlite.Connection, op: aiosqlite.Row) -> str:
    """The projection desired-hash this op was enqueued for.

    Recorded as the projection's ``applied_payload_hash`` on success —
    deliberately NOT the projection's *current* ``desired_payload_hash``.
    A projection's desired state can change (e.g. cleanup / pause sets
    it to absent, at the same ledger version) while an older op is
    still in flight; recording the current desired hash would then
    mark the projection "applied" at a state Google never received, so
    the diff sees no divergence and never enqueues the corrective
    delete.  The diff stamps each op with the desired hash it targeted
    at enqueue time — that is what the op applied.

    The op's own ``payload_json`` cannot be hashed for this: it is
    rendered with the projection id baked in, so its hash would never
    equal the planner's projection-id-free ``desired_payload_hash``.
    """
    stamped = op["desired_payload_hash"]
    if stamped is not None:
        return stamped
    # Op enqueued before the desired_payload_hash column existed —
    # fall back to the projection's current desired hash.
    proj = await _get_projection(db, op["projection_id"])
    return proj["desired_payload_hash"]


async def _record_origin_writeback_applied(
    db: aiosqlite.Connection,
    op: aiosqlite.Row,
    *,
    now: datetime,
) -> None:
    """Mark an origin writeback patch done.

    Unlike :func:`_record_success` this does NOT write
    ``google_event_id`` onto the projection — the origin writeback
    projection keeps it NULL so the next ingest of the source
    calendar still processes the source event normally.
    """
    when = now.isoformat()
    await db.execute(
        """UPDATE ledger_projections
              SET current_state = 'present',
                  applied_payload_hash = ?,
                  applied_ledger_version = ?,
                  last_attempt_at = ?,
                  next_attempt_at = NULL,
                  attempts = attempts + 1,
                  last_error = NULL,
                  permanently_failed = 0,
                  updated_at = ?
            WHERE id = ?""",
        (
            await _applied_hash_for(db, op),
            int(op["ledger_version_at_enqueue"]),
            when, when, int(op["projection_id"]),
        ),
    )
    # The pending main-side change has now reached the source — clear
    # the flag so a later reconcile does not re-patch.
    await db.execute(
        """UPDATE ledger_events
              SET origin_writeback_pending = 0
            WHERE id = (SELECT ledger_event_id FROM ledger_projections
                         WHERE id = ?)""",
        (int(op["projection_id"]),),
    )
    await db.execute(
        """UPDATE outbox_operations
              SET status = ?, completed_at = ?, last_http_status = 200
            WHERE id = ?""",
        (STATUS_DONE, when, op["id"]),
    )
    await db.commit()


async def _record_success(
    db: aiosqlite.Connection,
    op: aiosqlite.Row,
    *,
    google_event_id: str,
    google_etag: str,
    now: datetime,
) -> None:
    when = now.isoformat()
    await _invalidate_instances_on_parent_id_change(
        db, op, new_google_event_id=google_event_id, now=now,
    )
    await db.execute(
        """UPDATE ledger_projections
              SET current_state = 'present',
                  google_event_id = ?,
                  google_etag = ?,
                  applied_payload_hash = ?,
                  applied_ledger_version = ?,
                  last_attempt_at = ?,
                  next_attempt_at = NULL,
                  attempts = attempts + 1,
                  last_error = NULL,
                  permanently_failed = 0,
                  updated_at = ?
            WHERE id = ?""",
        (
            google_event_id, google_etag,
            await _applied_hash_for(db, op),
            int(op["ledger_version_at_enqueue"]),
            when, when, int(op["projection_id"]),
        ),
    )
    await db.execute(
        """UPDATE outbox_operations
              SET status = ?, completed_at = ?, last_http_status = 200
            WHERE id = ?""",
        (STATUS_DONE, when, op["id"]),
    )
    await db.commit()


async def _invalidate_instances_on_parent_id_change(
    db: aiosqlite.Connection,
    op: aiosqlite.Row,
    *,
    new_google_event_id: str,
    now: datetime,
) -> None:
    """Repoint a recurring parent's instance projections at the id the
    parent series actually landed on.

    ``_do_create`` burns a deterministic id whenever it collides with a
    cancelled tombstone (the user deleted our managed copy, so Google
    keeps the id reserved) and retries under a bumped
    ``google_id_generation``.  A mirrored series therefore legitimately
    changes Google id over its lifetime.  Instance projections address
    their occurrence as ``<parent_google_event_id>_<stamp>``, and nothing
    used to tell them the prefix had moved:

    * A *present* occurrence override kept pointing at the burned
      parent's stamp, so every subsequent UPDATE 404s.
    * A *cancelled* occurrence was worse.  Its projection is
      ``desired_state='absent'`` and quiescent — ``applied`` equals
      ``desired`` — so the diff never re-selects it.  The recreated
      series carries the source RRULE verbatim (no EXDATE), Google
      expands the very occurrence the source had cancelled, and the
      resulting busy block is owned by nobody: no reconcile, drain,
      drift-revert or content-audit pass revisits it (the audit is
      source-side and skips cancelled rows).

    Live regression: a personal-source "Bi-Weekly All-Hands" series
    reached ``google_id_generation = 3``; its cancelled 2026-07-28
    occurrence stayed converged with a cleared ``google_event_id`` while
    the recreated series cast a permanent 2pm "Busy" block on a client
    calendar.

    Keyed off the DERIVED id per instance rather than off the parent's
    previous ``google_event_id``: the drift path that notices our copy
    was deleted clears the parent's id *before* the recreate, so an
    old-vs-new comparison sees NULL and misses the very case this exists
    for.  Deriving is also self-correcting for rows stranded by an
    earlier build.

    The new id is *stamped* (not cleared): ``diff._decide`` treats an
    absent projection with no ``google_event_id`` as "nothing to delete"
    and would converge it again.  ``current_state`` becomes ``unknown``
    because what we knew about the old id says nothing about the new
    series.  Scoped to the SAME target as the parent — a sibling
    calendar's copy has its own, unaffected id.  Instances whose derived
    id is already correct are left completely alone, so a routine
    same-id update never churns occurrence overrides.
    """
    # Deliberately not _get_projection: that raises when the row is
    # gone, and this runs AFTER the Google write has landed — a
    # vanished projection (cascade delete mid-drain) must not turn a
    # successful write into a drain error.
    proj = await (await db.execute(
        """SELECT id, ledger_event_id, target_kind, target_calendar_id
             FROM ledger_projections WHERE id = ?""",
        (int(op["projection_id"]),),
    )).fetchone()
    if proj is None or not new_google_event_id:
        return
    parent = await (await db.execute(
        """SELECT canonical_uid, user_id, parent_canonical_uid,
                  recurrence_rule_json
             FROM ledger_events WHERE id = ?""",
        (int(proj["ledger_event_id"]),),
    )).fetchone()
    # Only a recurring series MASTER has instance overrides hanging off it.
    if parent is None or parent["parent_canonical_uid"]:
        return
    if not parent["recurrence_rule_json"]:
        return
    instances = await (await db.execute(
        """SELECT p.id, p.google_event_id,
                  e.recurrence_instance_original_start, e.is_all_day
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE p.target_kind = ?
              AND COALESCE(p.target_calendar_id, -1) = COALESCE(?, -1)
              AND e.user_id = ?
              AND e.parent_canonical_uid = ?""",
        (
            proj["target_kind"], proj["target_calendar_id"],
            int(parent["user_id"]), parent["canonical_uid"],
        ),
    )).fetchall()
    repointed = 0
    for inst in instances:
        derived = derive_instance_google_event_id(
            new_google_event_id,
            inst["recurrence_instance_original_start"] or "",
            is_all_day=bool(inst["is_all_day"]),
        )
        if inst["google_event_id"] == derived:
            continue
        await db.execute(
            """UPDATE ledger_projections
                  SET google_event_id = ?,
                      google_etag = NULL,
                      current_state = 'unknown',
                      applied_ledger_version = NULL,
                      applied_payload_hash = NULL,
                      next_attempt_at = NULL,
                      updated_at = ?
                WHERE id = ?""",
            (derived, now.isoformat(), int(inst["id"])),
        )
        repointed += 1
    if repointed:
        logger.info(
            "outbox: parent projection %s landed on google_event_id %s — "
            "repointed and re-diverged %s instance projection(s) on the same "
            "target so their occurrence overrides are re-asserted against the "
            "new series",
            int(proj["id"]), new_google_event_id, repointed,
        )


async def _record_absent(
    db: aiosqlite.Connection,
    op: aiosqlite.Row,
    *,
    now: datetime,
) -> None:
    when = now.isoformat()
    await db.execute(
        """UPDATE ledger_projections
              SET current_state = 'absent',
                  applied_ledger_version = ?,
                  applied_payload_hash = 'absent',
                  last_attempt_at = ?,
                  next_attempt_at = NULL,
                  attempts = attempts + 1,
                  last_error = NULL,
                  permanently_failed = 0,
                  updated_at = ?
            WHERE id = ?""",
        (
            int(op["ledger_version_at_enqueue"]),
            when, when, int(op["projection_id"]),
        ),
    )
    await db.execute(
        """UPDATE outbox_operations
              SET status = ?, completed_at = ?, last_http_status = 200
            WHERE id = ?""",
        (STATUS_DONE, when, op["id"]),
    )
    await db.commit()


async def _mark_superseded(
    db: aiosqlite.Connection,
    op: aiosqlite.Row,
    *,
    error: str,
    http_status: Optional[int],
    now: datetime,
) -> None:
    await db.execute(
        """UPDATE outbox_operations
              SET status = ?, completed_at = ?,
                  last_error = ?, last_http_status = ?
            WHERE id = ?""",
        (STATUS_SUPERSEDED, now.isoformat(), error, http_status, op["id"]),
    )
    await db.commit()


async def _mark_retry(
    db: aiosqlite.Connection,
    op: aiosqlite.Row,
    *,
    error: str,
    http_status: Optional[int],
    now: datetime,
) -> None:
    attempts = int(op["attempts"]) + 1  # we already bumped on claim
    backoff_idx = min(attempts - 1, len(_BACKOFF_SECONDS) - 1)
    next_at = now + timedelta(seconds=_BACKOFF_SECONDS[backoff_idx])
    await db.execute(
        """UPDATE outbox_operations
              SET status = ?, next_attempt_at = ?,
                  last_error = ?, last_http_status = ?
            WHERE id = ?""",
        (STATUS_PENDING, next_at.isoformat(), error, http_status, op["id"]),
    )
    await db.commit()


async def _mark_permanent_failure(
    db: aiosqlite.Connection,
    op: aiosqlite.Row,
    *,
    error: str,
    http_status: Optional[int],
    now: datetime,
) -> None:
    when = now.isoformat()
    await db.execute(
        """UPDATE outbox_operations
              SET status = ?, completed_at = ?,
                  last_error = ?, last_http_status = ?
            WHERE id = ?""",
        (STATUS_PERMANENT_FAILURE, when, error, http_status, op["id"]),
    )
    await db.execute(
        """UPDATE ledger_projections
              SET current_state = 'errored',
                  permanently_failed = 1,
                  last_attempt_at = ?,
                  last_error = ?,
                  updated_at = ?
            WHERE id = ?""",
        (when, error, when, int(op["projection_id"])),
    )
    await db.commit()


async def _give_up_burned_ids(
    db: aiosqlite.Connection,
    op: aiosqlite.Row,
    *,
    generation: int,
    now: datetime,
) -> None:
    """Stop burning deterministic ids and surface the projection.

    Every derived id for this projection has collided with a cancelled
    tombstone up to the absolute ceiling.  Persist the generation (so a
    later admin retry resumes past the burned ids rather than re-colliding
    from a low generation), mark the op a permanent failure, and alert —
    a real event that cannot be mirrored needs operator attention, not an
    unbounded id-burning loop.
    """
    # Alert once per failure episode: a flapping event whose planner
    # hash change clears permanently_failed and immediately re-fails
    # would otherwise email the operator on every flap.
    prior = await (await db.execute(
        "SELECT permanently_failed FROM ledger_projections WHERE id = ?",
        (int(op["projection_id"]),),
    )).fetchone()
    already_failed = bool(prior and prior["permanently_failed"])
    await db.execute(
        "UPDATE ledger_projections SET google_id_generation = ? WHERE id = ?",
        (int(generation), int(op["projection_id"])),
    )
    error = (
        f"create op {op['id']}: gave up after {generation} burned id "
        f"generations for projection {op['projection_id']} — every derived "
        f"id collides with a cancelled tombstone on Google"
    )
    logger.error(error)
    await _mark_permanent_failure(db, op, error=error, http_status=None, now=now)
    if already_failed:
        return  # same episode, operator already alerted
    try:  # alerting must never break the drain
        from app.alerts.email import queue_alert
        await queue_alert(
            alert_type="event_unmirrorable",
            user_id=int(op["user_id"]),
            details=(
                "A calendar event could not be mirrored as a busy block after "
                f"exhausting {generation} id generations (projection "
                f"{op['projection_id']}). It will not appear as busy until "
                "resolved; use the admin retry-failed action after "
                "investigating the underlying recurring event."
            ),
        )
    except Exception as e:
        logger.warning("could not queue event_unmirrorable alert: %s", e)


def _is_deterministic_local_failure(error: Exception) -> bool:
    """True for a failure that is local and deterministic — retrying
    the identical op can never succeed.

    Covers the ``ValueError``s raised before any network I/O for
    structurally-broken ops (``_do_create``/``_do_update``/``_do_patch``
    "has no payload" / "has no google_event_id", the unknown-operation
    guard) and ``json.JSONDecodeError`` from a malformed
    ``payload_json`` (a ``ValueError`` subclass).  Genuine transients
    never look like this: HTTP failures arrive as ``GoogleApiError`` /
    ``HttpError``-shaped exceptions carrying a ``status`` (and are
    routed by status before this check), and transport failures are
    ``OSError`` / timeout types — none of them ``ValueError``.
    """
    return isinstance(error, ValueError)


async def _classify_and_retry(
    db: aiosqlite.Connection,
    op: aiosqlite.Row,
    error: Exception,
    *,
    now: datetime,
) -> str:
    """Decide whether to retry, give up, or supersede an op."""
    status = getattr(error, "status", None)
    msg = str(error)
    if _is_rate_limited(error):
        # Quota / rate-limit responses are ALWAYS transient.  Google
        # Calendar returns these as HTTP 403 (with a structured
        # reason) at least as often as 429, so they must be caught
        # BEFORE the 403-is-permanent rule below — otherwise a quota
        # blip poison-pills a real event.
        await _mark_retry(db, op, error=msg, http_status=status, now=now)
        return "retried"
    if status in PERMANENT_FAILURE_STATUSES or (
        status is None and _is_deterministic_local_failure(error)
    ):
        # Deterministic failures — a 4xx from Google, or a local
        # ValueError / JSON-decode error that no retry can fix — share
        # one attempts ceiling.  Without it, a non-HTTP deterministic
        # failure (malformed payload_json, an update whose projection
        # never got a google_event_id) fell through to the transient
        # branch below and retried every drain forever.
        if int(op["attempts"]) >= POISON_PILL_THRESHOLD:
            await _mark_permanent_failure(
                db, op, error=msg, http_status=status, now=now,
            )
            return "failed_permanent"
        # First few failures: retry slowly; the planner may produce a
        # corrected payload after the next ingest.
        await _mark_retry(db, op, error=msg, http_status=status, now=now)
        return "retried"

    # Retryable: 408/429/5xx and any non-HTTP transport exception
    # (network).  Deliberately unbounded — an outage must never
    # poison-pill real work.
    await _mark_retry(db, op, error=msg, http_status=status, now=now)
    return "retried"


async def _get_projection(db: aiosqlite.Connection, projection_id: int) -> aiosqlite.Row:
    row = await (await db.execute(
        "SELECT * FROM ledger_projections WHERE id = ?",
        (int(projection_id),),
    )).fetchone()
    if row is None:
        raise OutboxDrainError(f"projection {projection_id} vanished")
    return row


async def _request_projection_replan(
    db: aiosqlite.Connection, projection_id: int,
) -> None:
    """Mark the projection as needing fresh planner attention.

    Clears ``applied_ledger_version`` so the diff filter picks it
    up again, but leaves ``current_state`` as ``'present'`` (if it
    was) so the next diff resolves to UPDATE rather than CREATE —
    the event still exists on Google; we just need to re-assert
    our desired payload against a freshly-fetched etag.
    """
    await db.execute(
        """UPDATE ledger_projections
              SET applied_ledger_version = NULL
            WHERE id = ?""",
        (int(projection_id),),
    )
    await db.commit()
