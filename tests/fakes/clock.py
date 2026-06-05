"""Simulated clock for deterministic time-travel in tests.

The clock advances under explicit control: calls to ``advance()`` move
simulated time forward; ``run_for()`` advances time while yielding
control to scheduled callbacks.  No relationship to wall-clock time
unless you wire one up via ``run_in_realtime()``.

Default rate (when ``run_in_realtime()`` is used) is 1 simulated day
per real second, matching the soak-harness target.

Threading note: the clock is not thread-safe.  Tests must drive it
from a single coroutine or from a single thread.
"""

from __future__ import annotations

import bisect
import heapq
import itertools
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Optional

UTC = timezone.utc

# 1 simulated day per 1 real second is the default soak rate.
DEFAULT_SIM_SECONDS_PER_REAL_SECOND = 86_400.0

# Global counter so scheduled-callback ordering is deterministic when
# two callbacks fire at the exact same simulated instant.
_seq_counter = itertools.count()


@dataclass(order=True)
class _ScheduledCallback:
    """One pending invocation on the simulated timeline."""
    fire_at: datetime
    seq: int = field(compare=True)
    callback: Callable[[], None] = field(compare=False)
    cancelled: bool = field(default=False, compare=False)


class SimulatedClock:
    """A controllable clock that returns a configurable ``datetime``.

    Use ``now()`` everywhere production code would call
    ``datetime.utcnow()`` / ``datetime.now(timezone.utc)``.  Tests
    drive the clock via ``advance()`` / ``set_time()``.

    Optional callback scheduling lets the integration framework run
    work "later in simulated time" without sleeping.
    """

    def __init__(self, start: Optional[datetime] = None):
        if start is None:
            # Pin to a stable epoch so test failures are reproducible
            # without depending on the wall clock at run time.
            start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)
        self._now = _ensure_aware(start)
        self._heap: list[_ScheduledCallback] = []
        self._sim_seconds_per_real_second = DEFAULT_SIM_SECONDS_PER_REAL_SECOND

    # ------------------------------------------------------------------
    # Reading time
    # ------------------------------------------------------------------
    def now(self) -> datetime:
        """Return the current simulated time as an aware UTC datetime."""
        return self._now

    def utcnow(self) -> datetime:
        """Return the current simulated time as a *naive* UTC datetime.

        Provided so production code that still uses ``datetime.utcnow()``
        can be patched against this clock without behavioural change.
        """
        return self._now.replace(tzinfo=None)

    def monotonic(self) -> float:
        """Return a monotonic timestamp (seconds since epoch).

        Useful where production code uses ``time.monotonic()`` for
        backoff timers; under simulation we just project ``now`` onto
        a real-number axis.
        """
        return self._now.timestamp()

    # ------------------------------------------------------------------
    # Setting time
    # ------------------------------------------------------------------
    def set_time(self, when: datetime) -> None:
        """Jump to ``when``.  Must not move backwards."""
        when = _ensure_aware(when)
        if when < self._now:
            raise ValueError(
                f"Refusing to move clock backwards: "
                f"now={self._now.isoformat()} requested={when.isoformat()}"
            )
        self._fire_through(when)
        self._now = when

    def advance(self, delta: timedelta | float) -> None:
        """Advance simulated time by ``delta``.

        ``delta`` may be a ``timedelta`` or a number of seconds.  Any
        scheduled callbacks whose fire time falls within the window
        will run, in chronological order, with ``self._now`` set to
        each callback's fire time at invocation.
        """
        if not isinstance(delta, timedelta):
            delta = timedelta(seconds=float(delta))
        if delta < timedelta(0):
            raise ValueError("Cannot advance the clock backwards")
        self.set_time(self._now + delta)

    # ------------------------------------------------------------------
    # Callback scheduling
    # ------------------------------------------------------------------
    def schedule(
        self,
        delay: timedelta | float,
        callback: Callable[[], None],
    ) -> _ScheduledCallback:
        """Run ``callback`` after ``delay`` simulated seconds have passed.

        Returns the scheduled-callback handle so it can be cancelled.
        """
        if not isinstance(delay, timedelta):
            delay = timedelta(seconds=float(delay))
        item = _ScheduledCallback(
            fire_at=self._now + delay,
            seq=next(_seq_counter),
            callback=callback,
        )
        heapq.heappush(self._heap, item)
        return item

    def schedule_at(
        self,
        when: datetime,
        callback: Callable[[], None],
    ) -> _ScheduledCallback:
        """Like ``schedule`` but with an absolute fire time."""
        when = _ensure_aware(when)
        if when < self._now:
            raise ValueError("Cannot schedule a callback in the past")
        item = _ScheduledCallback(
            fire_at=when,
            seq=next(_seq_counter),
            callback=callback,
        )
        heapq.heappush(self._heap, item)
        return item

    @staticmethod
    def cancel(handle: _ScheduledCallback) -> None:
        """Cancel a previously scheduled callback (idempotent)."""
        handle.cancelled = True

    # ------------------------------------------------------------------
    # Driving the loop
    # ------------------------------------------------------------------
    def run_until_idle(self) -> int:
        """Fire any callbacks whose time has already arrived.

        Returns the number of callbacks invoked.
        """
        return self._fire_through(self._now)

    def run_for(self, delta: timedelta | float) -> int:
        """Advance by ``delta``, firing any callbacks along the way.

        Returns the number of callbacks invoked.
        """
        if not isinstance(delta, timedelta):
            delta = timedelta(seconds=float(delta))
        target = self._now + delta
        fired = self._fire_through(target)
        self._now = target
        return fired

    def run_until(self, predicate: Callable[[], bool], step: timedelta | float = 60.0,
                  max_steps: int = 100_000) -> int:
        """Step forward repeatedly until ``predicate()`` returns truthy.

        Each step advances by ``step`` simulated seconds.  Raises
        ``RuntimeError`` if ``max_steps`` is exhausted; this prevents
        a flaky test from spinning forever.
        """
        if not isinstance(step, timedelta):
            step = timedelta(seconds=float(step))
        steps = 0
        while not predicate():
            if steps >= max_steps:
                raise RuntimeError(
                    f"run_until: predicate never became true after "
                    f"{max_steps} steps of {step}"
                )
            self.run_for(step)
            steps += 1
        return steps

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _fire_through(self, target: datetime) -> int:
        """Fire callbacks scheduled at or before ``target``.

        Updates ``self._now`` to each callback's scheduled time as it
        fires, so callbacks observe the correct ``now()``.  Leaves
        ``self._now`` at the latest callback time; the caller is
        responsible for moving past ``target`` afterwards.
        """
        fired = 0
        while self._heap and self._heap[0].fire_at <= target:
            item = heapq.heappop(self._heap)
            if item.cancelled:
                continue
            self._now = item.fire_at
            item.callback()
            fired += 1
        return fired

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    def pending_callbacks(self) -> int:
        """Number of un-cancelled callbacks still on the timeline."""
        return sum(1 for item in self._heap if not item.cancelled)

    def __repr__(self) -> str:
        return f"SimulatedClock(now={self._now.isoformat()}, pending={self.pending_callbacks()})"


def _ensure_aware(dt: datetime) -> datetime:
    """Return a UTC-aware datetime; assume naive datetimes are already UTC."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)
