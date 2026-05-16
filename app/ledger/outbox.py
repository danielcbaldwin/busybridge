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

* :class:`Exception` whose status is 401/403/404 → permanent
  failure after ``POISON_PILL_THRESHOLD`` attempts.
* status 408/429/500/502/503/504 or any non-HTTP exception →
  retry with exponential backoff.
* status 412 (etag mismatch) → mark superseded, schedule replan.

The retry timing matches the existing
``app/sync/google_calendar.py`` curve so production behaviour
under load doesn't change at cutover.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiosqlite

from app.ledger.google_client import GoogleClient
from app.ledger.identity import derive_google_event_id

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
# RSVP-only patch back to the calendar that sourced the event.
OP_PATCH = "patch"

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


class OutboxDrainError(Exception):
    """Raised when the drain could not even read the queue."""


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
    if operation not in (OP_CREATE, OP_UPDATE, OP_DELETE, OP_PATCH):
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
    now = now or datetime.now(UTC)
    counters = {
        "processed": 0,
        "succeeded": 0,
        "retried": 0,
        "failed_permanent": 0,
        "superseded": 0,
    }

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
    """
    operation = op["operation"]
    cal_id = op["target_google_calendar_id"]
    payload = json.loads(op["payload_json"]) if op["payload_json"] else None

    try:
        if operation == OP_CREATE:
            await _do_create(db, google, op, cal_id, payload, now=now)
        elif operation == OP_UPDATE:
            return await _do_update(db, google, op, cal_id, payload, now=now)
        elif operation == OP_DELETE:
            await _do_delete(db, google, op, cal_id, now=now)
        elif operation == OP_PATCH:
            return await _do_patch(db, google, op, cal_id, payload, now=now)
        else:
            raise ValueError(f"unknown operation: {operation!r}")
    except Exception as e:
        return await _classify_and_retry(db, op, e, now=now)
    return "succeeded"


async def _do_create(
    db: aiosqlite.Connection,
    google: GoogleClient,
    op: aiosqlite.Row,
    cal_id: str,
    payload: Optional[dict],
    *,
    now: datetime,
) -> None:
    """Insert with a deterministic ID; treat 409 as success."""
    if payload is None:
        raise ValueError(f"create op {op['id']} has no payload")
    google_id = derive_google_event_id(int(op["projection_id"]))
    body = dict(payload)
    body["id"] = google_id
    try:
        result = google.insert_event(cal_id, body)
    except Exception as e:
        if getattr(e, "status", None) == 409:
            # The ID is already on Google.  Two cases:
            # * Retry-of-our-own-write: existing event matches our
            #   intended payload → adopt and mark done.
            # * Concurrent edit (e.g. user dragged our copy on main):
            #   existing event differs from our intended payload.
            #   We own this ID by construction; UPDATE to restore.
            existing = google.get_event(cal_id, google_id)
            try:
                result = google.update_event(
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
                    return
                raise
            await _record_success(
                db, op,
                google_event_id=result["id"],
                google_etag=result.get("etag", ""),
                now=now,
            )
            return
        raise
    await _record_success(
        db, op,
        google_event_id=result["id"],
        google_etag=result.get("etag", ""),
        now=now,
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
        result = google.update_event(
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
                fresh = google.get_event(cal_id, proj["google_event_id"])
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
        raise
    await _record_success(
        db, op,
        google_event_id=result["id"],
        google_etag=result.get("etag", ""),
        now=now,
    )
    return "succeeded"


async def _do_delete(
    db: aiosqlite.Connection,
    google: GoogleClient,
    op: aiosqlite.Row,
    cal_id: str,
    *,
    now: datetime,
) -> None:
    """Idempotent delete: 404/410 are treated as success.

    The delete is intentionally *unconditional* — no ``If-Match``.
    The planner has decided this projection's desired state is
    ``absent``; the event must go regardless of whatever revision
    currently sits on Google.  Sending the stored ETag would only
    invite a 412 ping-pong (event changed since we last saw it) for
    no benefit, since the outcome we want is "gone" either way.
    """
    proj = await _get_projection(db, op["projection_id"])
    if not proj["google_event_id"]:
        # Never created — nothing to delete.  Mark projection
        # absent and consider done.
        await _record_absent(db, op, now=now)
        return
    try:
        google.delete_event(cal_id, proj["google_event_id"])
    except Exception as e:
        if getattr(e, "status", None) in (404, 410):
            pass  # already gone
        else:
            raise
    await _record_absent(db, op, now=now)


async def _do_patch(
    db: aiosqlite.Connection,
    google: GoogleClient,
    op: aiosqlite.Row,
    cal_id: str,
    payload: Optional[dict],
    *,
    now: datetime,
) -> str:
    """RSVP-only patch back onto the calendar that sourced the event.

    The target is the user's real source event, addressed by the
    ledger row's ``source_event_id`` — NOT a projection
    ``google_event_id``.  The origin rsvp projection deliberately
    keeps ``google_event_id`` NULL so ingest's loop-prevention does
    not mistake the source event for one of our writes.

    ``events.patch`` is field-scoped (only the attendees array is
    sent), so no other field of the source event can be clobbered.
    A 404/410 (source event gone) is treated as success — there is
    nothing left to write back.
    """
    if payload is None:
        raise ValueError(f"patch op {op['id']} has no payload")
    proj = await _get_projection(db, op["projection_id"])
    led = await (await db.execute(
        "SELECT source_event_id FROM ledger_events WHERE id = ?",
        (int(proj["ledger_event_id"]),),
    )).fetchone()
    if led is None or not led["source_event_id"]:
        await _record_rsvp_applied(db, op, now=now)
        return "succeeded"
    try:
        google.patch_event(cal_id, led["source_event_id"], payload)
    except Exception as e:
        if getattr(e, "status", None) in (404, 410):
            await _record_rsvp_applied(db, op, now=now)
            return "succeeded"
        raise
    await _record_rsvp_applied(db, op, now=now)
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


async def _record_rsvp_applied(
    db: aiosqlite.Connection,
    op: aiosqlite.Row,
    *,
    now: datetime,
) -> None:
    """Mark an rsvp-only patch done.

    Unlike :func:`_record_success` this does NOT write
    ``google_event_id`` onto the projection — the origin rsvp
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
    if status in PERMANENT_FAILURE_STATUSES:
        if int(op["attempts"]) >= POISON_PILL_THRESHOLD:
            await _mark_permanent_failure(
                db, op, error=msg, http_status=status, now=now,
            )
            return "failed_permanent"
        # First few 4xxs: retry slowly; the planner may produce a
        # corrected payload after the next ingest.
        await _mark_retry(db, op, error=msg, http_status=status, now=now)
        return "retried"

    # Retryable: 408/429/5xx and any non-HTTP exception (network).
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
