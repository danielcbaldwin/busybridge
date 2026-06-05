"""Unit contract for app/ledger/recurrence.py — the shared coverage probe.

Locks the two behaviours a code review flagged as destructive-if-wrong, since
``occurrence_in_series`` returning a confident-but-wrong ``False`` causes the
main-ingest orphan check to cancel a live mirror:

* naive-wall-time + IANA ``start_timezone`` must anchor IN the zone (not be
  read as UTC then re-zoned), or a covered occurrence reads as uncovered.
* an all-day series with a Z-stamped ``UNTIL`` must yield a determinate
  answer, not ``None`` (which silently disables the orphan revert).
"""

from __future__ import annotations

from app.ledger.recurrence import (
    occurrence_in_series,
    parse_instant,
    parse_recurrence_lines,
    series_dtstart,
    strip_r_suffix,
)

_WEEKLY = parse_recurrence_lines('["RRULE:FREQ=WEEKLY;COUNT=6"]')


def _cov(lines, start_at, tz, target_iso, *, all_day=False, start_all_day=None):
    ds = series_dtstart(start_at, tz, is_all_day=start_all_day if start_all_day is not None else all_day)
    return occurrence_in_series(lines, ds, parse_instant(target_iso, is_all_day=all_day), is_all_day=all_day)


# --- strip_r_suffix --------------------------------------------------------
def test_strip_r_suffix():
    assert strip_r_suffix("abc") == "abc"
    assert strip_r_suffix("abc_R20260302T090000Z") == "abc"
    assert strip_r_suffix("abc_R20260302T090000Z_R20260406T110000Z") == "abc"
    # A literal "_R" not followed by a full server timestamp is NOT stripped.
    assert strip_r_suffix("ab_Rxyz") == "ab_Rxyz"
    assert strip_r_suffix("ab_R2026") == "ab_R2026"


# --- finding #2: naive wall-time + IANA zone ------------------------------
def test_series_dtstart_naive_walltime_anchors_in_zone():
    # 09:00 naive + America/New_York (June → EDT, -04:00).
    ds = series_dtstart("2024-06-04T09:00:00", "America/New_York", is_all_day=False)
    assert ds.utcoffset().total_seconds() == -4 * 3600
    # The correctly-resolved occurrence instant (09:00 EDT == 13:00Z) is covered.
    assert _cov(_WEEKLY, "2024-06-04T09:00:00", "America/New_York", "2024-06-04T13:00:00Z") is True
    # Reading the naive value as UTC (the bug) would anchor at 09:00Z and miss it.
    assert _cov(_WEEKLY, "2024-06-04T09:00:00", "America/New_York", "2024-06-04T09:00:00Z") is False


def test_series_dtstart_offset_aware_dst_correct():
    # Weekly NY series crossing 2026-03-08 DST: pre 14:00Z (EST), post 13:00Z (EDT).
    assert _cov(parse_recurrence_lines('["RRULE:FREQ=WEEKLY;COUNT=5"]'),
                "2026-03-05T09:00:00-05:00", "America/New_York", "2026-03-05T14:00:00Z") is True
    assert _cov(parse_recurrence_lines('["RRULE:FREQ=WEEKLY;COUNT=5"]'),
                "2026-03-05T09:00:00-05:00", "America/New_York", "2026-03-12T13:00:00Z") is True
    # The wrong (fixed-EST) grid instant must NOT match.
    assert _cov(parse_recurrence_lines('["RRULE:FREQ=WEEKLY;COUNT=5"]'),
                "2026-03-05T09:00:00-05:00", "America/New_York", "2026-03-12T14:00:00Z") is False


def test_series_dtstart_naive_no_zone_is_utc():
    assert _cov(_WEEKLY, "2026-02-02T09:00:00", "UTC", "2026-02-09T09:00:00Z") is True


# --- finding #3: all-day Z-stamped UNTIL must be determinate ----------------
def test_all_day_zulu_until_is_determinate():
    lines = parse_recurrence_lines('["RRULE:FREQ=DAILY;UNTIL=20260205T235959Z"]')
    assert _cov(lines, "2026-02-02", None, "2026-02-05", all_day=True) is True
    assert _cov(lines, "2026-02-02", None, "2026-02-06", all_day=True) is False


def test_all_day_date_form_until_also_works():
    lines = parse_recurrence_lines('["RRULE:FREQ=DAILY;UNTIL=20260205"]')
    assert _cov(lines, "2026-02-02", None, "2026-02-05", all_day=True) is True
    assert _cov(lines, "2026-02-02", None, "2026-02-06", all_day=True) is False


# --- tri-state: indeterminate inputs -> None (never a confident answer) -----
def test_indeterminate_inputs_return_none():
    ds = series_dtstart("2026-02-02T09:00:00Z", "UTC", is_all_day=False)
    assert occurrence_in_series(None, ds, parse_instant("2026-02-09T09:00:00Z", is_all_day=False), is_all_day=False) is None
    assert occurrence_in_series(_WEEKLY, None, parse_instant("2026-02-09T09:00:00Z", is_all_day=False), is_all_day=False) is None
    assert occurrence_in_series(_WEEKLY, ds, None, is_all_day=False) is None


def test_truncated_base_excludes_post_boundary():
    base = parse_recurrence_lines('["RRULE:FREQ=WEEKLY;UNTIL=20260302T085959Z"]')
    assert _cov(base, "2026-02-02T09:00:00Z", "UTC", "2026-02-23T09:00:00Z") is True
    assert _cov(base, "2026-02-02T09:00:00Z", "UTC", "2026-03-09T09:00:00Z") is False
