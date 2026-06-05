"""Failure injection for the fake Google Calendar.

Rates are independent per failure mode and are evaluated in
deterministic order; with the same ``seed`` the same sequence of
operations yields the same sequence of failures.

Failure modes:

* **network_error_rate** — raise :class:`NetworkError` (transport
  layer; the API call never reached Google).
* **rate_limit_rate** — raise 429 with the ``rateLimitExceeded``
  reason that production code parses out of the message.
* **server_error_rate** — raise 5xx (default 503).
* **sync_token_expiry_rate** — only applies to ``list_events`` with
  a sync token; raises 410 so the consumer falls back to full
  sync.  Independent of, and in addition to, the time-based TTL
  expiry implemented in :mod:`tests.fakes.google_calendar`.
* **mid_write_crash_rate** — only applies to write operations;
  raises :class:`NetworkError` *after* the state has been mutated,
  simulating "Google persisted the change but the response was
  lost in transit."  This is the most subtle injection mode and
  is what exercises idempotency on retry.

Tests can also force a specific failure on the next call via
:meth:`FailureInjector.force_next`, which is useful for adversarial
unit tests where probabilistic injection is too coarse.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from typing import Optional

from tests.fakes.google_calendar import GoogleApiError


class NetworkError(Exception):
    """Transport-level failure (DNS/TCP/TLS/connection-reset).

    Distinct from :class:`GoogleApiError`, which represents an HTTP
    response with a status code.  Production code that catches
    network failures separately from HTTP errors should catch this
    type plus any equivalent third-party exception (e.g.
    ``socket.timeout`` in real code).
    """


def _rate_limit_error() -> GoogleApiError:
    return GoogleApiError(
        429, "Too Many Requests",
        "rateLimitExceeded: simulated rate limit",
    )


def _server_error() -> GoogleApiError:
    return GoogleApiError(
        503, "Service Unavailable",
        "simulated transient server error",
    )


def _sync_token_expired() -> GoogleApiError:
    return GoogleApiError(
        410, "Gone",
        "simulated sync-token expiry (failure injection)",
    )


# Operations recognised by the injector.  Used to scope which
# failure modes apply to which calls.
_READ_OPERATIONS = frozenset({
    "list", "get", "instances", "list_calendars", "get_calendar",
})
_WRITE_OPERATIONS = frozenset({
    "insert", "update", "patch", "delete",
})
_ALL_OPERATIONS = _READ_OPERATIONS | _WRITE_OPERATIONS


@dataclass
class FailureInjector:
    """Probabilistic / forced failure controller.

    All rates are independent; setting all of them to ``0`` is the
    no-op default.  Rates apply *per-operation* — a single
    ``insert_event`` rolls one dice per failure mode.
    """

    seed: int = 0
    network_error_rate: float = 0.0
    rate_limit_rate: float = 0.0
    server_error_rate: float = 0.0
    sync_token_expiry_rate: float = 0.0
    mid_write_crash_rate: float = 0.0

    # Test introspection counters.
    network_error_count: int = field(default=0, init=False)
    rate_limit_count: int = field(default=0, init=False)
    server_error_count: int = field(default=0, init=False)
    sync_token_expiry_count: int = field(default=0, init=False)
    mid_write_crash_count: int = field(default=0, init=False)
    forced_failure_count: int = field(default=0, init=False)

    _rng: random.Random = field(default=None, init=False, repr=False)
    _next_force: Optional[Exception] = field(default=None, init=False, repr=False)
    _crash_after_writes: bool = field(default=False, init=False, repr=False)

    def __post_init__(self) -> None:
        for name in (
            "network_error_rate", "rate_limit_rate", "server_error_rate",
            "sync_token_expiry_rate", "mid_write_crash_rate",
        ):
            v = getattr(self, name)
            if not (0.0 <= v <= 1.0):
                raise ValueError(f"{name} must be in [0, 1], got {v!r}")
        self._rng = random.Random(self.seed)

    # ------------------------------------------------------------------
    # Forced failures
    # ------------------------------------------------------------------
    def force_next(self, error: Exception) -> None:
        """Make the very next ``maybe_fail`` call raise ``error``.

        Useful for tests that need a specific 412 / 409 / 500 etc.
        without fiddling with rates.  Cleared after one use.
        """
        self._next_force = error

    def force_next_crash_after_write(self) -> None:
        """Make the very next write succeed-then-crash.

        Equivalent to ``mid_write_crash_rate=1.0`` for one call.
        """
        self._crash_after_writes = True

    # ------------------------------------------------------------------
    # Pre-operation
    # ------------------------------------------------------------------
    def maybe_fail(self, operation: str, *, has_sync_token: bool = False) -> None:
        """Roll for failures that should preempt the operation.

        Raises:
            NetworkError, GoogleApiError: if a failure mode triggers.
        """
        if operation not in _ALL_OPERATIONS:
            raise ValueError(f"unknown operation: {operation!r}")

        if self._next_force is not None:
            err = self._next_force
            self._next_force = None
            self.forced_failure_count += 1
            raise err

        if self._rng.random() < self.network_error_rate:
            self.network_error_count += 1
            raise NetworkError(f"simulated network failure during {operation}")

        if self._rng.random() < self.rate_limit_rate:
            self.rate_limit_count += 1
            raise _rate_limit_error()

        if self._rng.random() < self.server_error_rate:
            self.server_error_count += 1
            raise _server_error()

        if (
            operation == "list"
            and has_sync_token
            and self._rng.random() < self.sync_token_expiry_rate
        ):
            self.sync_token_expiry_count += 1
            raise _sync_token_expired()

    # ------------------------------------------------------------------
    # Post-write
    # ------------------------------------------------------------------
    def maybe_crash_after_write(self, operation: str) -> None:
        """Roll for a "Google persisted, response lost" crash.

        Called by the fake AFTER mutating state but BEFORE returning
        the result.  If it fires, the change is visible to the next
        call but the current call raises a transport failure.
        """
        if operation not in _WRITE_OPERATIONS:
            return
        if self._crash_after_writes:
            self._crash_after_writes = False
            self.mid_write_crash_count += 1
            raise NetworkError(
                f"simulated mid-write crash during {operation}: "
                "the change WAS persisted by Google, but the "
                "response did not reach the caller"
            )
        if self._rng.random() < self.mid_write_crash_rate:
            self.mid_write_crash_count += 1
            raise NetworkError(
                f"simulated mid-write crash during {operation}: "
                "the change WAS persisted by Google, but the "
                "response did not reach the caller"
            )

    # ------------------------------------------------------------------
    # Counters
    # ------------------------------------------------------------------
    @property
    def total_failures(self) -> int:
        return (
            self.network_error_count
            + self.rate_limit_count
            + self.server_error_count
            + self.sync_token_expiry_count
            + self.mid_write_crash_count
            + self.forced_failure_count
        )

    def reset_counters(self) -> None:
        self.network_error_count = 0
        self.rate_limit_count = 0
        self.server_error_count = 0
        self.sync_token_expiry_count = 0
        self.mid_write_crash_count = 0
        self.forced_failure_count = 0
