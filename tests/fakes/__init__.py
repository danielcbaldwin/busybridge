"""Test fakes for the BusyBridge rewrite.

This package provides drop-in fake implementations of external
dependencies (Google Calendar API, real-time clocks, ICS feeds, ...)
so the new ledger/projection/outbox code can be exercised
end-to-end without contacting any network service.

The fakes are deliberately faithful to the documented quirks of the
real services they stand in for; see ``REWRITE_PLAN.md`` §13 Stage 1
for the full list of behaviours covered.
"""

from tests.fakes.clock import SimulatedClock

__all__ = ["SimulatedClock"]
