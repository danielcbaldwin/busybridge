"""Soak-test harness (REWRITE_PLAN.md §13 Stage 3 + §14 Layer 5).

The soak layer runs the full pipeline against the Stage-1 fakes
across simulated weeks/months and verifies invariants after every
reconciliation cycle.  It catches the bugs unit tests can't see:
slow accumulation, long-cycle effects (sync-token expiry,
retention, recurrence-month-boundary), and compounding failure
spirals.

Structure:

* ``Oracle`` — the ground-truth model of what should be on each
  calendar, updated as the simulated user takes actions.
* ``InvariantChecker`` — the 11 invariants from §14 evaluated
  after every reconcile cycle.
* ``SoakHarness`` — orchestrates a simulated persona, drives the
  reconciler, and dispatches the checker.
"""

from tests.soak.oracle import Oracle
from tests.soak.invariants import InvariantChecker, InvariantViolation
from tests.soak.harness import SoakHarness

__all__ = ["Oracle", "InvariantChecker", "InvariantViolation", "SoakHarness"]
