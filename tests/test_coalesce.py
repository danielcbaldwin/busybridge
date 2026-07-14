"""Unit tests for the busy-block coalescer.

Interval merging is a pure function on plain dicts; these tests are
synchronous and exercise the algorithm directly, not through the
planner.  Coverage: no overlap, exact same time, adjacent, partial
overlap, containment, mixed all-day, single event, empty input,
deterministic carrier.
"""

from __future__ import annotations

from app.ledger.coalesce import CoalesceInput, coalesce_intervals


def _e(id: int, start: str, end: str, all_day: bool = False, tz: str = "UTC"):
    return CoalesceInput(
        ledger_event_id=id,
        start_at=start,
        end_at=end,
        start_timezone=None if all_day else tz,
        end_timezone=None if all_day else tz,
        is_all_day=all_day,
    )


def test_empty_input_returns_empty_list():
    assert coalesce_intervals([]) == []


def test_single_event_returns_single_interval():
    intervals = coalesce_intervals([
        _e(1, "2026-07-15T09:00:00+00:00", "2026-07-15T10:00:00+00:00"),
    ])
    assert len(intervals) == 1
    iv = intervals[0]
    assert iv.carrier_ledger_event_id == 1
    assert iv.member_ledger_event_ids == [1]
    assert iv.start_at == "2026-07-15T09:00:00+00:00"
    assert iv.end_at == "2026-07-15T10:00:00+00:00"


def test_non_overlapping_stay_separate():
    intervals = coalesce_intervals([
        _e(1, "2026-07-15T09:00:00+00:00", "2026-07-15T10:00:00+00:00"),
        _e(2, "2026-07-15T14:00:00+00:00", "2026-07-15T15:00:00+00:00"),
    ])
    assert len(intervals) == 2
    assert {iv.carrier_ledger_event_id for iv in intervals} == {1, 2}


def test_exact_same_time_merges():
    """Same meeting on two personal calendars produces one block."""
    intervals = coalesce_intervals([
        _e(1, "2026-07-15T09:00:00+00:00", "2026-07-15T10:00:00+00:00"),
        _e(2, "2026-07-15T09:00:00+00:00", "2026-07-15T10:00:00+00:00"),
    ])
    assert len(intervals) == 1
    iv = intervals[0]
    # Carrier is the smaller ledger id.
    assert iv.carrier_ledger_event_id == 1
    assert sorted(iv.member_ledger_event_ids) == [1, 2]


def test_partial_overlap_merges_and_unions_time():
    """9:00-10:00 and 9:30-11:00 merge into 9:00-11:00."""
    intervals = coalesce_intervals([
        _e(1, "2026-07-15T09:00:00+00:00", "2026-07-15T10:00:00+00:00"),
        _e(2, "2026-07-15T09:30:00+00:00", "2026-07-15T11:00:00+00:00"),
    ])
    assert len(intervals) == 1
    iv = intervals[0]
    assert iv.start_at == "2026-07-15T09:00:00+00:00"
    assert iv.end_at == "2026-07-15T11:00:00+00:00"


def test_adjacent_events_merge():
    """Back-to-back meetings (end == next start) coalesce into one block."""
    intervals = coalesce_intervals([
        _e(1, "2026-07-15T09:00:00+00:00", "2026-07-15T10:00:00+00:00"),
        _e(2, "2026-07-15T10:00:00+00:00", "2026-07-15T11:00:00+00:00"),
    ])
    assert len(intervals) == 1
    assert intervals[0].start_at == "2026-07-15T09:00:00+00:00"
    assert intervals[0].end_at == "2026-07-15T11:00:00+00:00"


def test_containment_merges():
    """Long event contains a shorter one; still one merged interval."""
    intervals = coalesce_intervals([
        _e(1, "2026-07-15T09:00:00+00:00", "2026-07-15T12:00:00+00:00"),
        _e(2, "2026-07-15T10:00:00+00:00", "2026-07-15T11:00:00+00:00"),
    ])
    assert len(intervals) == 1
    assert intervals[0].start_at == "2026-07-15T09:00:00+00:00"
    assert intervals[0].end_at == "2026-07-15T12:00:00+00:00"


def test_carrier_is_smallest_id_in_group():
    """Determinism: given the same set of events, the same carrier
    stays the carrier across runs — no diff churn."""
    intervals = coalesce_intervals([
        _e(5, "2026-07-15T09:00:00+00:00", "2026-07-15T10:00:00+00:00"),
        _e(3, "2026-07-15T09:30:00+00:00", "2026-07-15T11:00:00+00:00"),
        _e(9, "2026-07-15T10:30:00+00:00", "2026-07-15T12:00:00+00:00"),
    ])
    assert len(intervals) == 1
    assert intervals[0].carrier_ledger_event_id == 3


def test_three_events_all_merge_into_one():
    intervals = coalesce_intervals([
        _e(1, "2026-07-15T09:00:00+00:00", "2026-07-15T10:00:00+00:00"),
        _e(2, "2026-07-15T09:30:00+00:00", "2026-07-15T11:00:00+00:00"),
        _e(3, "2026-07-15T10:30:00+00:00", "2026-07-15T12:00:00+00:00"),
    ])
    assert len(intervals) == 1
    iv = intervals[0]
    assert iv.start_at == "2026-07-15T09:00:00+00:00"
    assert iv.end_at == "2026-07-15T12:00:00+00:00"
    assert sorted(iv.member_ledger_event_ids) == [1, 2, 3]


def test_gap_splits_groups():
    """A gap between events preserves the split; no forced merge."""
    intervals = coalesce_intervals([
        _e(1, "2026-07-15T09:00:00+00:00", "2026-07-15T10:00:00+00:00"),
        _e(2, "2026-07-15T10:00:01+00:00", "2026-07-15T11:00:00+00:00"),  # 1s gap
    ])
    assert len(intervals) == 2


def test_all_day_and_timed_are_not_merged():
    """All-day and timed events stay in separate groups even on the
    same date — different semantics."""
    intervals = coalesce_intervals([
        _e(1, "2026-07-15", "2026-07-16", all_day=True),
        _e(2, "2026-07-15T09:00:00+00:00", "2026-07-15T10:00:00+00:00"),
    ])
    assert len(intervals) == 2
    all_day = [iv for iv in intervals if iv.is_all_day]
    timed = [iv for iv in intervals if not iv.is_all_day]
    assert len(all_day) == 1
    assert len(timed) == 1


def test_all_day_events_on_same_day_merge():
    """Two all-day events (e.g. from two personal cals) on the same
    day still produce a single all-day block."""
    intervals = coalesce_intervals([
        _e(1, "2026-07-15", "2026-07-16", all_day=True),
        _e(2, "2026-07-15", "2026-07-16", all_day=True),
    ])
    assert len(intervals) == 1
    assert intervals[0].is_all_day is True


def test_zulu_time_normalizes():
    """A stored 'Z'-suffixed datetime (some ingest paths emit that)
    parses and merges correctly with a +00:00-suffixed one."""
    intervals = coalesce_intervals([
        _e(1, "2026-07-15T09:00:00Z", "2026-07-15T10:00:00Z"),
        _e(2, "2026-07-15T09:30:00+00:00", "2026-07-15T11:00:00+00:00"),
    ])
    assert len(intervals) == 1
