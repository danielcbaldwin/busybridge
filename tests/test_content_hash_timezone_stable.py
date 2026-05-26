"""Content hash is timezone-representation independent.

Google returns the same instant in time with different offset strings
across reads — e.g. "+02:00" while you're in Berlin, "-04:00" while
back in NYC.  If the content hash treats those as different values,
every audit/reconcile pass thinks the event drifted and re-ingests it
in an infinite loop (versions ran into the thousands live).
"""

from __future__ import annotations

from app.ledger.ingest.client import _content_hash, _extract_event_fields


def _ev(start: str, end: str) -> dict:
    return {
        "id": "x",
        "summary": "S",
        "start": {"dateTime": start, "timeZone": "America/New_York"},
        "end": {"dateTime": end, "timeZone": "America/New_York"},
    }


def test_same_instant_different_offsets_hashes_equal():
    h_edt = _content_hash(_extract_event_fields(
        _ev("2026-05-28T11:30:00-04:00", "2026-05-28T12:30:00-04:00"),
        user_email="u@example.com",
    ))
    h_cedt = _content_hash(_extract_event_fields(
        _ev("2026-05-28T17:30:00+02:00", "2026-05-28T18:30:00+02:00"),
        user_email="u@example.com",
    ))
    h_utc = _content_hash(_extract_event_fields(
        _ev("2026-05-28T15:30:00Z", "2026-05-28T16:30:00Z"),
        user_email="u@example.com",
    ))
    assert h_edt == h_cedt == h_utc, (
        "same instant in different timezone representations must hash "
        f"identically; got EDT={h_edt[:8]} CEDT={h_cedt[:8]} UTC={h_utc[:8]}"
    )


def test_genuinely_different_times_hash_differently():
    """Sanity: don't over-normalise away a real time change."""
    h1 = _content_hash(_extract_event_fields(
        _ev("2026-05-28T11:30:00-04:00", "2026-05-28T12:30:00-04:00"),
        user_email="u@example.com",
    ))
    h2 = _content_hash(_extract_event_fields(
        _ev("2026-05-28T12:30:00-04:00", "2026-05-28T13:30:00-04:00"),
        user_email="u@example.com",
    ))
    assert h1 != h2, "moving the event by an hour must change the hash"


def test_all_day_event_hash_stable():
    """All-day events use ``date`` not ``dateTime``; the YYYY-MM-DD form
    has no offset to normalise, so this is just a guard against the
    canonicalizer mangling it."""
    def _all_day(date: str) -> dict:
        return {
            "id": "x",
            "summary": "S",
            "start": {"date": date},
            "end": {"date": date},
        }
    h1 = _content_hash(_extract_event_fields(_all_day("2026-05-28"), user_email="u@e"))
    h2 = _content_hash(_extract_event_fields(_all_day("2026-05-28"), user_email="u@e"))
    h3 = _content_hash(_extract_event_fields(_all_day("2026-05-29"), user_email="u@e"))
    assert h1 == h2
    assert h1 != h3
