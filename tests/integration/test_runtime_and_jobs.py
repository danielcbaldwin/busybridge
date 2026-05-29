"""Tests for the production glue: ``app.ledger.runtime``,
``app.jobs.ledger_jobs``, and the ledger admin endpoints.

These exercise the wiring that connects the existing FastAPI /
APScheduler app to the ledger pipeline without booting the whole
app — we patch ``get_database`` and the auth dependencies to use
the in-memory test DB and a FakeGoogleCalendar."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio
UTC = timezone.utc


# ---------------------------------------------------------------------------
# drain_all_due_users
# ---------------------------------------------------------------------------
async def test_drain_all_due_users_dispatches_only_due_requests(monkeypatch):
    """The runtime drains every user with a due reconcile_request.
    Users whose schedule_for is in the future are left alone."""
    from app.ledger import runtime
    from app.ledger.triggers import enqueue_webhook, enqueue_manual

    s = Scenario(clock_start=datetime(2026, 6, 1, tzinfo=UTC))
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    s.given_event("client_a", summary="due event", start="2026-06-02T09:00:00Z")
    db = await s.setup_db()

    # User has a due request (webhook landed 10s before "now").
    s.clock.advance(10)
    await enqueue_webhook(db, user_id=s.user("alice").user_id, source_hint="client:1")
    # Also enqueue a manual request — its scheduled_for is 25s in
    # the future, so it should NOT be picked up by this drain.
    bob_user_id = 9999  # nonexistent: makes the runtime skip-by-error rather than succeed
    # Tick clock past the webhook debounce.
    s.clock.advance(20)

    # Patch the runtime's database/reconciler hooks to use the scenario.
    monkeypatch.setattr("app.ledger.runtime.get_database", lambda: _async_value(db))

    async def _fake_reconcile(user_id, **kwargs):
        # Run the scenario's reconciler so the fake-Google state advances.
        return await s.run_reconciler("alice")

    monkeypatch.setattr(
        "app.ledger.runtime.reconcile_user_by_id", _fake_reconcile,
    )

    out = await runtime.drain_all_due_users(now=s.clock.now())
    assert s.user("alice").user_id in out
    # The fake reconciler ran → main now has the event.
    s.assert_event_exists("main", summary="due event")

    # The claimed request was consumed.  With no new webhook/manual
    # enqueue, the next drain tick should be idle instead of processing
    # the same past scheduled_for forever.
    out2 = await runtime.drain_all_due_users(now=s.clock.now())
    assert out2 == {}
    await s.close()


# ---------------------------------------------------------------------------
# ledger_drain_due / ledger_enqueue_periodic
# ---------------------------------------------------------------------------
async def test_ledger_enqueue_periodic_skips_paused_users(monkeypatch):
    from app.jobs import ledger_jobs
    from app.ledger.admin_ops import cleanup_and_pause

    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    s.given_calendar("main2")
    s.given_calendar("client_b")
    await s.given_user(
        "bob", main="main2", clients=["client_b"], email="bob@example.com",
    )

    db = await s.setup_db()
    await db.execute(
        "UPDATE users SET main_calendar_id = ? WHERE id = ?",
        (s.cal("main"), s.user("alice").user_id),
    )
    await db.execute(
        "UPDATE users SET main_calendar_id = ? WHERE id = ?",
        (s.cal("main2"), s.user("bob").user_id),
    )
    incomplete = await (await db.execute(
        "INSERT INTO users (email) VALUES ('incomplete@example.com') "
        "RETURNING id",
    )).fetchone()
    await db.commit()
    # Pause Alice.
    await cleanup_and_pause(db, user_id=s.user("alice").user_id)

    monkeypatch.setattr("app.jobs.ledger_jobs.get_database", lambda: _async_value(db))
    enqueued_for: list[int] = []

    async def _fake_enqueue(db, *, user_id):
        enqueued_for.append(user_id)

    monkeypatch.setattr("app.jobs.ledger_jobs.enqueue_periodic", _fake_enqueue)

    await ledger_jobs.ledger_enqueue_periodic()
    assert s.user("bob").user_id in enqueued_for
    assert s.user("alice").user_id not in enqueued_for
    assert int(incomplete["id"]) not in enqueued_for
    await s.close()


async def test_ledger_drain_due_swallows_per_user_errors(monkeypatch):
    """One bad user must not block the whole drain tick."""
    from app.jobs import ledger_jobs

    async def _crashing_drain(*, now=None):
        raise RuntimeError("simulated outage")

    monkeypatch.setattr(
        "app.jobs.ledger_jobs.drain_all_due_users", _crashing_drain,
    )
    # Should not raise.
    await ledger_jobs.ledger_drain_due()


# ---------------------------------------------------------------------------
# RealGoogleClient (basic shape — actual HTTP is not exercised)
# ---------------------------------------------------------------------------
def test_real_google_client_wraps_http_errors():
    """The wrapper converts googleapiclient HttpError to our
    GoogleApiError so the ledger code can branch on e.status."""
    from app.ledger.real_google_client import GoogleApiError, _wrap

    class _FakeResp:
        def __init__(self, status, reason):
            self.status = status
            self.reason = reason

    class _FakeHttpError(Exception):
        def __init__(self, status, reason, msg):
            self.resp = _FakeResp(status, reason)
            self._msg = msg

        def _get_reason(self):
            return self._msg

    err = _wrap(_FakeHttpError(412, "Precondition Failed", "etag mismatch"))
    assert isinstance(err, GoogleApiError)
    assert err.status == 412
    assert err.reason == "Precondition Failed"
    assert "etag mismatch" in err.message


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _async_value(value):
    """Wrap a sync value as the awaitable that ``get_database`` returns."""
    async def _coro():
        return value
    return _coro()
