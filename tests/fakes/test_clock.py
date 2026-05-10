"""Tests for ``tests.fakes.clock.SimulatedClock``."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.fakes.clock import SimulatedClock

UTC = timezone.utc


def test_default_start_is_stable_epoch():
    clock = SimulatedClock()
    assert clock.now() == datetime(2026, 1, 1, tzinfo=UTC)


def test_set_start_explicitly():
    start = datetime(2030, 6, 15, 12, 0, 0, tzinfo=UTC)
    clock = SimulatedClock(start=start)
    assert clock.now() == start


def test_naive_start_is_treated_as_utc():
    clock = SimulatedClock(start=datetime(2030, 1, 1, 0, 0, 0))
    assert clock.now() == datetime(2030, 1, 1, tzinfo=UTC)


def test_advance_with_seconds():
    clock = SimulatedClock()
    before = clock.now()
    clock.advance(60)
    assert clock.now() == before + timedelta(seconds=60)


def test_advance_with_timedelta():
    clock = SimulatedClock()
    before = clock.now()
    clock.advance(timedelta(hours=2))
    assert clock.now() == before + timedelta(hours=2)


def test_advance_negative_raises():
    clock = SimulatedClock()
    with pytest.raises(ValueError):
        clock.advance(timedelta(seconds=-1))


def test_set_time_backwards_raises():
    clock = SimulatedClock()
    earlier = clock.now() - timedelta(seconds=1)
    with pytest.raises(ValueError):
        clock.set_time(earlier)


def test_utcnow_is_naive():
    clock = SimulatedClock()
    assert clock.utcnow().tzinfo is None
    assert clock.utcnow() == clock.now().replace(tzinfo=None)


def test_monotonic_advances_with_clock():
    clock = SimulatedClock()
    t0 = clock.monotonic()
    clock.advance(60)
    assert clock.monotonic() == pytest.approx(t0 + 60.0)


def test_schedule_fires_callback_on_advance():
    clock = SimulatedClock()
    fired: list[datetime] = []
    clock.schedule(30, lambda: fired.append(clock.now()))
    clock.advance(60)
    assert len(fired) == 1
    # Callback observed the *scheduled* time, not the post-advance time.
    assert fired[0] == datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC)


def test_schedule_does_not_fire_if_not_yet_due():
    clock = SimulatedClock()
    fired: list[int] = []
    clock.schedule(60, lambda: fired.append(1))
    clock.advance(30)
    assert fired == []


def test_multiple_callbacks_fire_in_chronological_order():
    clock = SimulatedClock()
    log: list[str] = []
    clock.schedule(30, lambda: log.append("middle"))
    clock.schedule(10, lambda: log.append("first"))
    clock.schedule(50, lambda: log.append("last"))
    clock.advance(120)
    assert log == ["first", "middle", "last"]


def test_callbacks_at_same_time_fire_in_insertion_order():
    clock = SimulatedClock()
    log: list[str] = []
    clock.schedule(10, lambda: log.append("a"))
    clock.schedule(10, lambda: log.append("b"))
    clock.schedule(10, lambda: log.append("c"))
    clock.advance(60)
    assert log == ["a", "b", "c"]


def test_cancelled_callback_does_not_fire():
    clock = SimulatedClock()
    fired: list[int] = []
    handle = clock.schedule(10, lambda: fired.append(1))
    clock.cancel(handle)
    clock.advance(60)
    assert fired == []


def test_schedule_at_absolute_time():
    clock = SimulatedClock()
    fire_at = clock.now() + timedelta(seconds=42)
    fired: list[datetime] = []
    clock.schedule_at(fire_at, lambda: fired.append(clock.now()))
    clock.advance(60)
    assert fired == [fire_at]


def test_schedule_at_in_past_raises():
    clock = SimulatedClock()
    with pytest.raises(ValueError):
        clock.schedule_at(clock.now() - timedelta(seconds=1), lambda: None)


def test_run_for_returns_callback_count():
    clock = SimulatedClock()
    clock.schedule(10, lambda: None)
    clock.schedule(20, lambda: None)
    clock.schedule(40, lambda: None)
    fired = clock.run_for(30)
    assert fired == 2
    assert clock.pending_callbacks() == 1


def test_run_until_idle_processes_already_due_callbacks():
    clock = SimulatedClock()
    fired: list[int] = []
    # Schedule and then jump past the fire time without firing,
    # by setting now directly (not allowed) — instead, schedule
    # a callback that schedules another one.
    def outer():
        fired.append(1)
        clock.schedule(0, lambda: fired.append(2))
    clock.schedule(10, outer)
    clock.advance(20)
    # The inner callback was scheduled at sim_now=10 with delay 0, so
    # it's due immediately; advance(20) processes it too.
    assert fired == [1, 2]


def test_run_until_predicate():
    clock = SimulatedClock()
    counter = [0]
    def tick():
        counter[0] += 1
        clock.schedule(60, tick)
    clock.schedule(60, tick)
    steps = clock.run_until(lambda: counter[0] >= 5, step=60)
    assert counter[0] >= 5
    # Should have advanced about 5 minutes.
    assert clock.now() >= datetime(2026, 1, 1, 0, 5, 0, tzinfo=UTC)
    assert steps >= 5


def test_run_until_max_steps_guard():
    clock = SimulatedClock()
    with pytest.raises(RuntimeError):
        clock.run_until(lambda: False, step=60, max_steps=10)


def test_pending_callbacks_excludes_cancelled():
    clock = SimulatedClock()
    h1 = clock.schedule(10, lambda: None)
    clock.schedule(20, lambda: None)
    assert clock.pending_callbacks() == 2
    clock.cancel(h1)
    assert clock.pending_callbacks() == 1


def test_callback_observes_scheduled_time_not_target():
    """Callbacks see the clock at their fire time, mid-advance."""
    clock = SimulatedClock()
    seen: list[datetime] = []
    clock.schedule(15, lambda: seen.append(clock.now()))
    clock.schedule(45, lambda: seen.append(clock.now()))
    clock.advance(120)
    assert seen[0] == datetime(2026, 1, 1, 0, 0, 15, tzinfo=UTC)
    assert seen[1] == datetime(2026, 1, 1, 0, 0, 45, tzinfo=UTC)
    # After advance returns, clock is at the target.
    assert clock.now() == datetime(2026, 1, 1, 0, 2, 0, tzinfo=UTC)
