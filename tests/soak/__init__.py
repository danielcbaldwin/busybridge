"""Soak-test harness.

The soak layer runs the full pipeline against the fakes
across simulated weeks/months and verifies invariants after every
reconciliation cycle.  It catches the bugs unit tests can't see:
slow accumulation, long-cycle effects (sync-token expiry,
retention, recurrence-month-boundary), and compounding failure
spirals.

Structure:

* ``Oracle`` — the ground-truth model of what should be on each
  calendar, updated as the simulated user takes actions.
* ``InvariantChecker`` — the 11 invariants evaluated
  after every reconcile cycle.
* ``SoakHarness`` — orchestrates a simulated persona, drives the
  reconciler, and dispatches the checker.
"""

from tests.soak.oracle import Oracle
from tests.soak.invariants import InvariantChecker, InvariantViolation
from tests.soak.harness import SoakHarness

__all__ = ["Oracle", "InvariantChecker", "InvariantViolation", "SoakHarness"]
