"""Additional coverage tests for jobs.scheduler."""

from __future__ import annotations

from types import SimpleNamespace


class _FakeScheduler:
    def __init__(self):
        self.jobs: dict[str, dict] = {}
        self.started = False
        self.stopped = False

    def add_job(self, func, *_args, **kwargs):
        self.jobs[kwargs["id"]] = {
            "func": func,
            "name": kwargs.get("name"),
            "trigger": kwargs.get("trigger"),
        }

    def start(self):
        self.started = True

    def shutdown(self, wait: bool = False):
        self.stopped = True


def test_get_scheduler_returns_current_instance(monkeypatch):
    """get_scheduler should return the module's current scheduler reference."""
    import app.jobs.scheduler as scheduler

    sentinel = object()
    monkeypatch.setattr(scheduler, "_scheduler", sentinel)
    assert scheduler.get_scheduler() is sentinel


def _settings(**overrides):
    base = {
        "sync_interval_minutes": 5,
        "enable_webhooks": True,
        "webhook_renewal_hours": 6,
        "content_audit_minutes": 10,
        "token_refresh_minutes": 30,
        "alert_process_minutes": 1,
        "enable_ledger_jobs": False,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def test_setup_scheduler_registers_webhook_job_when_enabled(monkeypatch):
    """Scheduler setup should include webhook renewal when ENABLE_WEBHOOKS is true."""
    import app.jobs.scheduler as scheduler

    monkeypatch.setattr(scheduler, "AsyncIOScheduler", _FakeScheduler)
    monkeypatch.setattr(scheduler, "get_settings", lambda: _settings())

    sched = scheduler.setup_scheduler()
    assert sched.started is True
    assert "periodic_sync" in sched.jobs
    assert "webhook_renewal" in sched.jobs
    assert "token_refresh" in sched.jobs

    scheduler.shutdown_scheduler()


def test_setup_scheduler_registers_vacuum_job(monkeypatch):
    """Scheduler setup should register the weekly database VACUUM
    job so the DB reclaims space after retention deletes."""
    import app.jobs.scheduler as scheduler

    monkeypatch.setattr(scheduler, "AsyncIOScheduler", _FakeScheduler)
    monkeypatch.setattr(scheduler, "get_settings", lambda: _settings())

    sched = scheduler.setup_scheduler()
    assert "database_vacuum" in sched.jobs
    assert "retention_cleanup" in sched.jobs

    scheduler.shutdown_scheduler()


def test_setup_scheduler_skips_webhook_job_when_disabled(monkeypatch):
    """Scheduler setup should skip webhook renewal when ENABLE_WEBHOOKS is false."""
    import app.jobs.scheduler as scheduler

    monkeypatch.setattr(scheduler, "AsyncIOScheduler", _FakeScheduler)
    monkeypatch.setattr(
        scheduler, "get_settings", lambda: _settings(enable_webhooks=False),
    )

    sched = scheduler.setup_scheduler()
    assert sched.started is True
    assert "periodic_sync" in sched.jobs
    assert "webhook_renewal" not in sched.jobs
    assert "token_refresh" in sched.jobs

    scheduler.shutdown_scheduler()


def test_setup_scheduler_legacy_mode_keeps_periodic_sync_only(monkeypatch):
    """Without ledger jobs, keep the rollback-compatible periodic shim."""
    import app.jobs.scheduler as scheduler

    monkeypatch.setattr(scheduler, "AsyncIOScheduler", _FakeScheduler)
    monkeypatch.setattr(
        scheduler, "get_settings", lambda: _settings(enable_ledger_jobs=False),
    )

    sched = scheduler.setup_scheduler()
    assert "periodic_sync" in sched.jobs
    assert sched.jobs["periodic_sync"]["func"] == (
        "app.jobs.sync_job:run_periodic_sync"
    )
    assert "ledger_enqueue_periodic" not in sched.jobs
    assert "ledger_drain_due" not in sched.jobs
    assert "sync_health_checks" not in sched.jobs

    scheduler.shutdown_scheduler()


def test_setup_scheduler_ledger_mode_uses_ledger_jobs_not_periodic_shim(
    monkeypatch,
):
    """Ledger mode must not schedule both the old shim and the ledger jobs.

    ``run_periodic_sync`` delegates to ledger enqueue + drain, so scheduling
    it alongside the ledger jobs creates duplicate periodic reconciles.
    """
    import app.jobs.scheduler as scheduler

    monkeypatch.setattr(scheduler, "AsyncIOScheduler", _FakeScheduler)
    monkeypatch.setattr(
        scheduler, "get_settings", lambda: _settings(enable_ledger_jobs=True),
    )

    sched = scheduler.setup_scheduler()
    targets = {job["func"] for job in sched.jobs.values()}
    assert "periodic_sync" not in sched.jobs
    assert "app.jobs.sync_job:run_periodic_sync" not in targets
    assert sched.jobs["ledger_enqueue_periodic"]["func"] == (
        "app.jobs.ledger_jobs:ledger_enqueue_periodic"
    )
    assert sched.jobs["ledger_drain_due"]["func"] == (
        "app.jobs.ledger_jobs:ledger_drain_due"
    )
    assert sched.jobs["sync_health_checks"]["func"] == (
        "app.jobs.sync_job:run_sync_health_checks"
    )

    scheduler.shutdown_scheduler()
