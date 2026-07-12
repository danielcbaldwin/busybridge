"""``derive_instance_google_event_id`` — timezone correctness.

Google's per-instance event ID is ``<parent>_<YYYYMMDDTHHMMSSZ>``
with the timestamp always in UTC.  The source ``originalStartTime``
can carry any offset, so it must be parsed and converted — not
string-stripped, which mangles a non-UTC offset into an invalid id.

NOTE: the stamp form (``_YYYYMMDD`` vs ``_YYYYMMDDTHHMMSSZ``) is now
chosen from the SHAPE of the original-start string itself; the
``is_all_day`` argument these tests pass is only the fallback for an
empty input.  Shape-vs-flag disagreement cases (a single occurrence
converted between all-day and timed) live in
tests/test_allday_instance_id_shape.py.
"""

from __future__ import annotations

from app.ledger.identity import derive_instance_google_event_id as derive


def test_utc_z_timestamp_unchanged():
    assert derive("bbp", "2024-03-10T14:30:00Z", False) == "bbp_20240310T143000Z"


def test_non_utc_offset_is_converted_to_utc():
    # 14:30 at -05:00 is 19:30 UTC.
    assert derive("bbp", "2024-03-10T14:30:00-05:00", False) == (
        "bbp_20240310T193000Z"
    )
    # 09:00 at +02:00 is 07:00 UTC.
    assert derive("bbp", "2024-03-10T09:00:00+02:00", False) == (
        "bbp_20240310T070000Z"
    )


def test_naive_timestamp_assumed_utc():
    assert derive("bbp", "2024-03-10T14:30:00", False) == "bbp_20240310T143000Z"


def test_fractional_seconds_dropped():
    assert derive("bbp", "2024-03-10T14:30:00.123456Z", False) == (
        "bbp_20240310T143000Z"
    )


def test_all_day_uses_the_date_stamp():
    assert derive("bbp", "2024-03-10", True) == "bbp_20240310"


def test_unparseable_falls_back_without_crashing():
    # Garbage in → best-effort, but never an exception.
    out = derive("bbp", "not-a-timestamp", False)
    assert out.startswith("bbp_")
