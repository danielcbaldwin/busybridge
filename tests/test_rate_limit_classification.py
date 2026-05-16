"""Rate-limit responses must never be poison-pilled.

Google Calendar returns quota / rate-limit errors as HTTP 403 with
a structured reason (``rateLimitExceeded`` / ``userRateLimitExceeded``
/ ``quotaExceeded``) at least as often as 429.  The outbox's
PERMANENT_FAILURE_STATUSES includes 403, so without special-casing
a routine quota blip would be retried 5 times and then permanently
poison-pilled.  These tests pin that a rate-limit is always
classified as transient.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from app.ledger.outbox import _classify_and_retry, _is_rate_limited
from tests.fakes.google_calendar import GoogleApiError
from tests.integration.framework import Scenario

UTC = timezone.utc


def test_is_rate_limited_recognises_429_and_403_reasons():
    assert _is_rate_limited(GoogleApiError(429, "Too Many Requests", "slow down"))
    assert _is_rate_limited(GoogleApiError(
        403, "Forbidden", "Rate Limit Exceeded [rateLimitExceeded]",
    ))
    assert _is_rate_limited(GoogleApiError(
        403, "Forbidden", "quota [userRateLimitExceeded]",
    ))
    assert _is_rate_limited(GoogleApiError(403, "Forbidden", "[quotaExceeded]"))
    # A genuine permission 403 is NOT a rate limit.
    assert not _is_rate_limited(GoogleApiError(
        403, "Forbidden", "insufficient calendar permissions",
    ))
    assert not _is_rate_limited(GoogleApiError(404, "Not Found", "gone"))


def test_wrap_surfaces_the_structured_rate_limit_reason():
    from app.ledger.real_google_client import _wrap

    class _Resp:
        status = 403
        reason = "Forbidden"

    class _Err(Exception):
        resp = _Resp()
        content = json.dumps({
            "error": {
                "errors": [{"reason": "rateLimitExceeded"}],
                "message": "Rate Limit Exceeded",
            },
        }).encode("utf-8")

        def _get_reason(self):
            return "Rate Limit Exceeded"

    wrapped = _wrap(_Err())
    assert wrapped.status == 403
    # The structured reason code is now in the message, so the
    # outbox classifier can see it.
    assert "rateLimitExceeded" in str(wrapped)
    assert _is_rate_limited(wrapped)


@pytest.mark.asyncio
async def test_rate_limited_op_retries_instead_of_poison_pilling():
    """A 403-rateLimitExceeded outbox op, even well past the
    poison-pill threshold, is retried — never marked permanent."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, 'client:1:x', 'client', 'active', 1,
                   '2026-01-01', '2026-01-01') RETURNING id""",
        (user.user_id,),
    )).fetchone()
    proj = await (await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, desired_state,
               desired_payload_hash, desired_ledger_version, current_state)
           VALUES (?, 'main', 'present_full', 'h', 1, 'present')
           RETURNING id""",
        (int(ev["id"]),),
    )).fetchone()
    op_row = await (await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, target_google_calendar_id,
               status, attempts)
           VALUES (?, ?, 'update', 'k1', 1, 'main@cal', 'in_flight', 9)
           RETURNING *""",
        (user.user_id, int(proj["id"])),
    )).fetchone()
    await db.commit()

    # attempts=9 is well past POISON_PILL_THRESHOLD (5).
    outcome = await _classify_and_retry(
        db, op_row,
        GoogleApiError(403, "Forbidden", "Rate Limit Exceeded [rateLimitExceeded]"),
        now=datetime.now(UTC),
    )
    assert outcome == "retried"

    op = await (await db.execute(
        "SELECT status FROM outbox_operations WHERE id = ?", (op_row["id"],),
    )).fetchone()
    assert op["status"] == "pending", "rate-limited op was not left retryable"
    pr = await (await db.execute(
        "SELECT permanently_failed FROM ledger_projections WHERE id = ?",
        (int(proj["id"]),),
    )).fetchone()
    assert not pr["permanently_failed"], "rate limit poison-pilled the projection"
    await s.close()


@pytest.mark.asyncio
async def test_genuine_permission_403_still_poison_pills():
    """A real (non-rate-limit) 403 past the threshold is still a
    permanent failure — the fix must not make 403 always-retryable."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, 'client:1:y', 'client', 'active', 1,
                   '2026-01-01', '2026-01-01') RETURNING id""",
        (user.user_id,),
    )).fetchone()
    proj = await (await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, desired_state,
               desired_payload_hash, desired_ledger_version, current_state)
           VALUES (?, 'main', 'present_full', 'h', 1, 'present')
           RETURNING id""",
        (int(ev["id"]),),
    )).fetchone()
    op_row = await (await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, target_google_calendar_id,
               status, attempts)
           VALUES (?, ?, 'update', 'k2', 1, 'main@cal', 'in_flight', 9)
           RETURNING *""",
        (user.user_id, int(proj["id"])),
    )).fetchone()
    await db.commit()

    outcome = await _classify_and_retry(
        db, op_row,
        GoogleApiError(403, "Forbidden", "insufficient calendar permissions"),
        now=datetime.now(UTC),
    )
    assert outcome == "failed_permanent"
    pr = await (await db.execute(
        "SELECT permanently_failed FROM ledger_projections WHERE id = ?",
        (int(proj["id"]),),
    )).fetchone()
    assert pr["permanently_failed"]
    await s.close()
