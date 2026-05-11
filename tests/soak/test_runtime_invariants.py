"""Tests for the three runtime invariants (9, 10, 11).

These check the soak harness's analyzers — given a sequence of
samples or a recovery-cycle count, they decide pass/fail.  The
soak loop itself produces the samples; tests here exercise the
analyzers directly with synthetic inputs.
"""

from __future__ import annotations

import pytest

from tests.soak.invariants import (
    DbSizeSample,
    InvariantChecker,
    LatencySample,
)


# ---------------------------------------------------------------------------
# Invariant 9: latency-per-event bounded
# ---------------------------------------------------------------------------
def test_inv9_linear_growth_passes():
    """Per-event cost stays constant → no violation."""
    samples = [
        LatencySample(event_count=n, wall_seconds=0.001 * n)
        for n in (10, 50, 100, 500, 1000)
    ]
    assert InvariantChecker.check_9_reconcile_latency_bounded(samples) == []


def test_inv9_exponential_growth_fails():
    """Per-event cost balloons on the last sample → violation."""
    samples = [
        LatencySample(event_count=10, wall_seconds=0.01),
        LatencySample(event_count=50, wall_seconds=0.05),
        LatencySample(event_count=100, wall_seconds=0.10),
        LatencySample(event_count=200, wall_seconds=0.20),
        # Spike: 10× per-event cost.
        LatencySample(event_count=200, wall_seconds=2.00),
    ]
    violations = InvariantChecker.check_9_reconcile_latency_bounded(samples)
    assert len(violations) == 1
    assert "INV-9" in violations[0]


def test_inv9_too_few_samples_is_undecided():
    samples = [LatencySample(event_count=10, wall_seconds=0.01)]
    assert InvariantChecker.check_9_reconcile_latency_bounded(samples) == []


# ---------------------------------------------------------------------------
# Invariant 10: db-size grows sub-linearly
# ---------------------------------------------------------------------------
def test_inv10_reasonable_size_passes():
    """1 KB per ledger row is well within the ceiling."""
    samples = [
        DbSizeSample(event_count=100, ledger_row_count=100, db_bytes=100_000),
    ]
    assert InvariantChecker.check_10_db_size_sub_linear(samples) == []


def test_inv10_unbounded_growth_fails():
    """100 KB per ledger row → unbounded outbox / sync-log retention."""
    samples = [
        DbSizeSample(event_count=100, ledger_row_count=100, db_bytes=10_000_000),
    ]
    violations = InvariantChecker.check_10_db_size_sub_linear(samples)
    assert len(violations) == 1
    assert "INV-10" in violations[0]


def test_inv10_empty_samples_no_op():
    assert InvariantChecker.check_10_db_size_sub_linear([]) == []


# ---------------------------------------------------------------------------
# Invariant 11: recovery within bounded cycles
# ---------------------------------------------------------------------------
def test_inv11_quick_recovery_passes():
    assert InvariantChecker.check_11_recovery_within_cycles(2) == []


def test_inv11_slow_recovery_fails():
    violations = InvariantChecker.check_11_recovery_within_cycles(7)
    assert len(violations) == 1
    assert "INV-11" in violations[0]


def test_inv11_no_recovery_fails():
    violations = InvariantChecker.check_11_recovery_within_cycles(None)
    assert len(violations) == 1
    assert "INV-11" in violations[0]
    assert "did not converge" in violations[0]
