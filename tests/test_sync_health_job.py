"""Scheduler wrapper for sync health checks."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.asyncio


async def test_run_sync_health_checks_calls_breaker_and_calendar_alerts(
    monkeypatch,
):
    from app.jobs import sync_job

    calls: list[str] = []

    async def fake_get_setting(key):
        assert key == "sync_paused"
        return None

    async def fake_acquire_job_lock(name):
        calls.append(f"lock:{name}")
        return "owner-token"

    async def fake_release_job_lock(name, owner):
        calls.append(f"release:{name}:{owner}")

    async def fake_check_circuit_breaker():
        calls.append("breaker")

    async def fake_alert_failing_calendars():
        calls.append("alerts")

    monkeypatch.setattr(sync_job, "get_setting", fake_get_setting)
    monkeypatch.setattr(sync_job, "acquire_job_lock", fake_acquire_job_lock)
    monkeypatch.setattr(sync_job, "release_job_lock", fake_release_job_lock)
    monkeypatch.setattr(
        sync_job, "_check_circuit_breaker", fake_check_circuit_breaker,
    )
    monkeypatch.setattr(
        sync_job, "_alert_failing_calendars", fake_alert_failing_calendars,
    )

    await sync_job.run_sync_health_checks()

    assert calls == [
        "lock:sync_health_checks",
        "breaker",
        "alerts",
        "release:sync_health_checks:owner-token",
    ]


async def test_run_sync_health_checks_respects_global_pause(monkeypatch):
    from app.jobs import sync_job

    calls: list[str] = []

    async def fake_get_setting(key):
        assert key == "sync_paused"
        return {"value_plain": "true"}

    async def fake_acquire_job_lock(name):
        calls.append(name)
        return "owner-token"

    monkeypatch.setattr(sync_job, "get_setting", fake_get_setting)
    monkeypatch.setattr(sync_job, "acquire_job_lock", fake_acquire_job_lock)

    await sync_job.run_sync_health_checks()

    assert calls == []
