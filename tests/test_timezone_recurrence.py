"""Phase 0 — timezone correctness for recurring events.

A recurring series declares an IANA timezone its RRULE expands in.
A "weekly 9 AM America/New_York" event keeps a fixed WALL-CLOCK
time and therefore shifts its UTC instant by an hour across a DST
transition.  BusyBridge used to render every mirrored copy with a
hardcoded ``timeZone: "UTC"``, so the mirror expanded on a fixed
UTC grid and drifted ±1h against the source after every DST change.

These tests pin the fix: the source timezone is captured at ingest,
stored on the ledger row, and rendered onto every mirrored copy.
"""

from __future__ import annotations

import json
from datetime import datetime

import pytest
from icalendar import Calendar

from app.ledger.ingest.client import _extract_event_fields, _resolve_original_start
from app.ledger.ingest.webcal import _vevent_to_dict
from app.ledger.payload import _end_dict, _start_dict, render_payload
from tests.integration.framework import Scenario


# ---------------------------------------------------------------------------
# Capture — _extract_event_fields
# ---------------------------------------------------------------------------
def test_extract_captures_start_and_end_timezone():
    event = {
        "id": "e1",
        "summary": "NY weekly",
        "start": {"dateTime": "2026-02-02T09:00:00-05:00",
                  "timeZone": "America/New_York"},
        "end": {"dateTime": "2026-02-02T09:30:00-05:00",
                "timeZone": "America/New_York"},
    }
    fields = _extract_event_fields(event, user_email="u@e.com")
    assert fields["start_timezone"] == "America/New_York"
    assert fields["end_timezone"] == "America/New_York"


def test_extract_all_day_event_has_no_timezone():
    event = {
        "id": "e2",
        "summary": "All day",
        "start": {"date": "2026-02-02"},
        "end": {"date": "2026-02-03"},
    }
    fields = _extract_event_fields(event, user_email="u@e.com")
    assert fields["start_timezone"] is None
    assert fields["end_timezone"] is None


# ---------------------------------------------------------------------------
# Render — payload start/end dicts
# ---------------------------------------------------------------------------
def test_start_end_dict_render_stored_timezone():
    row = {
        "is_all_day": False,
        "start_at": "2026-02-02T09:00:00-05:00",
        "end_at": "2026-02-02T09:30:00-05:00",
        "start_timezone": "America/New_York",
        "end_timezone": "America/New_York",
    }
    assert _start_dict(row)["timeZone"] == "America/New_York"
    assert _end_dict(row)["timeZone"] == "America/New_York"


def test_start_end_dict_fall_back_to_utc_without_timezone():
    row = {
        "is_all_day": False,
        "start_at": "2026-02-02T09:00:00Z",
        "end_at": "2026-02-02T09:30:00Z",
        "start_timezone": None,
        "end_timezone": None,
    }
    assert _start_dict(row)["timeZone"] == "UTC"
    assert _end_dict(row)["timeZone"] == "UTC"


def test_start_dict_tolerates_a_row_missing_the_timezone_column():
    # A legacy row read before the migration ran has no tz key at all.
    row = {"is_all_day": False, "start_at": "2026-02-02T09:00:00Z",
           "end_at": "2026-02-02T09:30:00Z"}
    assert _start_dict(row)["timeZone"] == "UTC"
    assert _end_dict(row)["timeZone"] == "UTC"


def test_busy_block_render_carries_source_timezone():
    row = {
        "is_all_day": False,
        "summary": "NY weekly",
        "start_at": "2026-02-02T09:00:00-05:00",
        "end_at": "2026-02-02T09:30:00-05:00",
        "start_timezone": "America/New_York",
        "end_timezone": "America/New_York",
        "recurrence_rule_json": json.dumps(["RRULE:FREQ=WEEKLY;COUNT=8"]),
    }
    body = render_payload(desired_state="present_busy", ledger_row=row)
    assert body["start"]["timeZone"] == "America/New_York"
    assert body["end"]["timeZone"] == "America/New_York"
    assert body["recurrence"] == ["RRULE:FREQ=WEEKLY;COUNT=8"]


# ---------------------------------------------------------------------------
# originalStartTime resolution
# ---------------------------------------------------------------------------
def test_resolve_original_start_keeps_explicit_offset():
    assert _resolve_original_start(
        {"dateTime": "2026-03-15T09:00:00-04:00"}
    ) == "2026-03-15T09:00:00-04:00"


def test_resolve_original_start_keeps_z_suffix():
    assert _resolve_original_start(
        {"dateTime": "2026-03-15T13:00:00Z"}
    ) == "2026-03-15T13:00:00Z"


def test_resolve_original_start_resolves_naive_plus_timezone():
    # 09:00 wall in New York on 2026-03-15 (EDT, -04:00) is 13:00 UTC.
    out = _resolve_original_start(
        {"dateTime": "2026-03-15T09:00:00", "timeZone": "America/New_York"}
    )
    assert datetime.fromisoformat(out).utctimetuple()[:5] == (
        2026, 3, 15, 13, 0,
    )


def test_resolve_original_start_winter_offset_differs_from_summer():
    # Same wall time, opposite sides of the DST boundary: the UTC
    # instant must differ by exactly an hour (EST -05:00 vs EDT -04:00).
    winter = _resolve_original_start(
        {"dateTime": "2026-02-15T09:00:00", "timeZone": "America/New_York"}
    )
    summer = _resolve_original_start(
        {"dateTime": "2026-07-15T09:00:00", "timeZone": "America/New_York"}
    )
    assert datetime.fromisoformat(winter).hour == 14  # 09:00 EST
    assert datetime.fromisoformat(summer).hour == 13  # 09:00 EDT


def test_resolve_original_start_naive_without_timezone_unchanged():
    assert _resolve_original_start(
        {"dateTime": "2026-03-15T09:00:00"}
    ) == "2026-03-15T09:00:00"


def test_resolve_original_start_bad_timezone_unchanged():
    assert _resolve_original_start(
        {"dateTime": "2026-03-15T09:00:00", "timeZone": "Not/AZone"}
    ) == "2026-03-15T09:00:00"


# ---------------------------------------------------------------------------
# Webcal — IANA timezone capture
# ---------------------------------------------------------------------------
def _vevent(*lines: str):
    body = "\r\n".join(
        ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//T//EN", "BEGIN:VEVENT"]
        + list(lines)
        + ["END:VEVENT", "END:VCALENDAR"]
    ) + "\r\n"
    cal = Calendar.from_ical(body.encode("utf-8"))
    return next(c for c in cal.walk() if c.name == "VEVENT")


def test_webcal_captures_iana_timezone_from_tzid():
    comp = _vevent(
        "UID:tz@e.com",
        "SUMMARY:NY weekly",
        "DTSTART;TZID=America/New_York:20260202T090000",
        "DTEND;TZID=America/New_York:20260202T093000",
        "RRULE:FREQ=WEEKLY;COUNT=8",
    )
    d = _vevent_to_dict(comp)
    assert d["start_timezone"] == "America/New_York"
    assert d["end_timezone"] == "America/New_York"


def test_webcal_utc_dtstart_has_no_iana_timezone():
    comp = _vevent(
        "UID:utc@e.com",
        "SUMMARY:UTC weekly",
        "DTSTART:20260202T090000Z",
        "DTEND:20260202T093000Z",
        "RRULE:FREQ=WEEKLY;COUNT=8",
    )
    d = _vevent_to_dict(comp)
    # A UTC / fixed-offset time has no IANA key — render falls back to UTC.
    assert d["start_timezone"] is None


def test_webcal_all_day_event_has_no_timezone():
    comp = _vevent(
        "UID:allday@e.com",
        "SUMMARY:All day",
        "DTSTART;VALUE=DATE:20260202",
        "DTEND;VALUE=DATE:20260203",
    )
    d = _vevent_to_dict(comp)
    assert d["is_all_day"] is True
    assert d["start_timezone"] is None


# ---------------------------------------------------------------------------
# End-to-end — a DST-crossing recurring series does not drift
# ---------------------------------------------------------------------------
pytestmark_async = pytest.mark.asyncio


def _utc_hour(dt_str: str) -> int:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).utctimetuple()[3]


@pytest.mark.asyncio
async def test_dst_crossing_recurring_series_mirrors_without_drift():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])

    # Weekly 9 AM America/New_York, starting before the 2026-03-08 DST
    # transition and running well past it.
    s.given_recurring_event(
        "client_a",
        summary="NY Standup",
        start="2026-02-02T09:00:00-05:00",
        timezone="America/New_York",
        rrule="RRULE:FREQ=WEEKLY;COUNT=12",
    )

    await s.run_reconciler("alice")

    # The ledger row carries the source's IANA zone.
    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT start_timezone, end_timezone, recurrence_rule_json
             FROM ledger_events
            WHERE user_id = ? AND source_type = 'client'""",
        (user.user_id,),
    )).fetchone()
    assert row["start_timezone"] == "America/New_York"
    assert row["end_timezone"] == "America/New_York"

    # The mirrored busy block on main is a single recurring event that
    # carries the source zone (NOT a hardcoded "UTC").
    mirror = [
        e for e in s.list_events("main") if e.get("recurrence")
    ]
    assert len(mirror) == 1, f"expected one recurring mirror, got {mirror}"
    assert mirror[0]["start"]["timeZone"] == "America/New_York"

    # Expanded, the mirror's occurrences line up instant-for-instant
    # with the source — no DST drift.  A pre-DST occurrence is 09:00
    # EST (14:00 UTC); a post-DST one is 09:00 EDT (13:00 UTC).
    src = {
        e["start"]["dateTime"][:10]: e["start"]["dateTime"]
        for e in s.list_events("client_a", single_events=True)
    }
    dst = {
        e["start"]["dateTime"][:10]: e["start"]["dateTime"]
        for e in s.list_events("main", single_events=True)
    }
    assert src == dst, "mirrored occurrences drifted from the source"
    assert _utc_hour(src["2026-02-09"]) == 14  # EST
    assert _utc_hour(src["2026-03-09"]) == 13  # EDT — DST already applied

    await s.close()


@pytest.mark.asyncio
async def test_all_day_recurring_series_still_mirrors():
    """An all-day recurring series carries no timezone and must keep
    working unchanged after the timezone plumbing was added."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])

    s.given_recurring_event(
        "client_a",
        summary="Daily all-day",
        start="2026-02-02",
        rrule="RRULE:FREQ=DAILY;COUNT=5",
    )

    await s.run_reconciler("alice")

    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT is_all_day, start_timezone FROM ledger_events
            WHERE user_id = ? AND source_type = 'client'""",
        (user.user_id,),
    )).fetchone()
    assert bool(row["is_all_day"]) is True
    assert row["start_timezone"] is None

    mirror = [e for e in s.list_events("main") if e.get("recurrence")]
    assert len(mirror) == 1
    assert "date" in mirror[0]["start"]

    await s.close()
