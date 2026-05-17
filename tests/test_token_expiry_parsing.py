"""token_expiry parsing must normalize tz-aware values to naive UTC
so they never raise TypeError against datetime.utcnow().
"""

from __future__ import annotations

from app.auth.google import _parse_expiry_naive_utc


def test_naive_value_is_returned_naive():
    dt = _parse_expiry_naive_utc("2026-05-17T10:00:00")
    assert dt.tzinfo is None


def test_aware_utc_value_is_folded_to_naive():
    dt = _parse_expiry_naive_utc("2026-05-17T10:00:00+00:00")
    assert dt.tzinfo is None
    assert dt == _parse_expiry_naive_utc("2026-05-17T10:00:00")


def test_aware_offset_value_is_converted_to_utc():
    # 05:00 at -05:00 is 10:00 UTC.
    dt = _parse_expiry_naive_utc("2026-05-17T05:00:00-05:00")
    assert dt.tzinfo is None
    assert dt == _parse_expiry_naive_utc("2026-05-17T10:00:00")
