"""Timezone-correctness fixes for webcal ingest + recurrence expansion.

Four verified bugs, all with the same failure mode — the user shows
FREE during real meetings (or ghost busy blocks are never pruned):

1. A custom/localized TZID (``Mitteleuropaeische Zeit`` from a German
   Outlook export) produced ``start_timezone = None`` — the mirrored
   series expanded on a fixed UTC grid and drifted an hour at every
   DST change.  ``_iana_tz_name`` now resolves such TZIDs via
   icalendar's Windows->Olson table, a small localized-name alias
   table, and (last resort) the VTIMEZONE's actual January/July UTC
   offsets.

2. A non-UTC ``UNTIL`` (naive datetime, or date-only on a timed
   event) was forwarded verbatim to Google, which 400s the whole
   series insert.  ``_extract_recurrence`` now rewrites UNTIL to a
   compliant UTC ``...Z`` stamp without moving the final occurrence.

3. A date-only or naive-datetime ``UNTIL`` (or awareness-mismatched
   EXDATE/RDATE) under an AWARE dtstart made dateutil raise inside
   ``expand_occurrences`` / ``occurrence_in_series``, which swallowed
   it into a permanent ``None`` — disabling cancel-all pruning and
   split-orphan detection for the series forever.

4. A floating (naive) RECURRENCE-ID was read as UTC, so the override
   or cancellation targeted an instant off by the zone offset and was
   silently lost.  It is now interpreted in the parent DTSTART's zone.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest
from dateutil.rrule import rrulestr
from icalendar import Calendar

from app.database import get_database
from app.ledger.ingest.webcal import (
    _vevent_to_dict,
    ingest_webcal_subscription,
)
from app.ledger.recurrence import expand_occurrences, occurrence_in_series

UTC = timezone.utc
BERLIN = ZoneInfo("Europe/Berlin")
URL = "webcal://outlook.example/feed.ics"

# A real Europe/Berlin VTIMEZONE block, renamed to the German Outlook
# display name (the shape German Exchange/Outlook exports actually use).
VTZ_MEZ = """BEGIN:VTIMEZONE
TZID:Mitteleuropaeische Zeit
BEGIN:DAYLIGHT
TZOFFSETFROM:+0100
TZOFFSETTO:+0200
TZNAME:CEST
DTSTART:19700329T020000
RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU
END:DAYLIGHT
BEGIN:STANDARD
TZOFFSETFROM:+0200
TZOFFSETTO:+0100
TZNAME:CET
DTSTART:19701025T030000
RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU
END:STANDARD
END:VTIMEZONE"""


def _ics(*vevents: str, vtimezone: str = "") -> bytes:
    body = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Test//EN"]
    if vtimezone:
        body.extend(vtimezone.strip().splitlines())
    for v in vevents:
        body.append("BEGIN:VEVENT")
        body.extend(v.strip().splitlines())
        body.append("END:VEVENT")
    body.append("END:VCALENDAR")
    return ("\r\n".join(body) + "\r\n").encode("utf-8")


def _vevent(body: bytes):
    cal = Calendar.from_ical(body)
    return next(c for c in cal.walk() if c.name == "VEVENT")


def _recurrence(d: dict) -> list[str]:
    return json.loads(d["recurrence_rule_json"])


# ---------------------------------------------------------------------------
# Fix 1: non-IANA TZIDs resolve to a real IANA zone
# ---------------------------------------------------------------------------
def test_localized_german_tzid_resolves_to_berlin():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:mez-1@example.com\n"
        "SUMMARY:Wochenmeeting\n"
        "DTSTART;TZID=Mitteleuropaeische Zeit:20260601T090000\n"
        "DTEND;TZID=Mitteleuropaeische Zeit:20260601T100000\n"
        "RRULE:FREQ=WEEKLY;BYDAY=MO",
        vtimezone=VTZ_MEZ,
    )))
    assert d["start_timezone"] == "Europe/Berlin", (
        f"custom TZID degraded to {d['start_timezone']!r} — the weekly "
        "grid would anchor at fixed UTC and drift an hour across DST"
    )
    assert d["end_timezone"] == "Europe/Berlin"
    # The instant itself is unchanged: 09:00 CEST == 07:00Z.
    assert d["start_at"] == "2026-06-01T07:00:00Z"


def test_windows_tzid_resolves_via_windows_to_olson():
    vtz = VTZ_MEZ.replace("Mitteleuropaeische Zeit", "W. Europe Standard Time")
    d = _vevent_to_dict(_vevent(_ics(
        "UID:win-1@example.com\n"
        "DTSTART;TZID=W. Europe Standard Time:20260601T090000\n"
        "DTEND;TZID=W. Europe Standard Time:20260601T100000\n"
        "RRULE:FREQ=WEEKLY;BYDAY=MO",
        vtimezone=vtz,
    )))
    assert d["start_timezone"] == "Europe/Berlin"


def test_unknown_tzid_derives_zone_from_vtimezone_offsets():
    # Not in any mapping table — only the VTIMEZONE's actual offsets
    # (+01:00 winter / +02:00 summer) identify it as the Berlin grid.
    vtz = VTZ_MEZ.replace("Mitteleuropaeische Zeit", "Totally Custom Zeit")
    d = _vevent_to_dict(_vevent(_ics(
        "UID:probe-1@example.com\n"
        "DTSTART;TZID=Totally Custom Zeit:20260601T090000\n"
        "DTEND;TZID=Totally Custom Zeit:20260601T100000\n"
        "RRULE:FREQ=WEEKLY;BYDAY=MO",
        vtimezone=vtz,
    )))
    assert d["start_timezone"] == "Europe/Berlin"


def test_unresolvable_tzid_warns_and_keeps_utc_fallback(caplog):
    # Nepal's +05:45 (no DST) matches nothing in the probe shortlist —
    # the failure must be VISIBLE (warning naming the TZID), not a
    # silent drift.
    vtz = """BEGIN:VTIMEZONE
TZID:Weird Custom Zone
BEGIN:STANDARD
TZOFFSETFROM:+0545
TZOFFSETTO:+0545
TZNAME:WCZ
DTSTART:19700101T000000
END:STANDARD
END:VTIMEZONE"""
    with caplog.at_level(logging.WARNING):
        d = _vevent_to_dict(_vevent(_ics(
            "UID:weird-1@example.com\n"
            "DTSTART;TZID=Weird Custom Zone:20260601T090000\n"
            "DTEND;TZID=Weird Custom Zone:20260601T100000",
            vtimezone=vtz,
        )))
    assert d["start_timezone"] is None
    assert any("Weird Custom Zone" in r.message for r in caplog.records), (
        "unresolved TZID must be logged, not silently dropped"
    )


def test_plain_utc_dtstart_still_has_no_timezone():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:utc-1@example.com\n"
        "DTSTART:20260601T090000Z\n"
        "DTEND:20260601T100000Z",
    )))
    assert d["start_timezone"] is None  # UTC fallback stays NULL


@pytest.mark.asyncio
async def test_ingested_series_stores_resolved_timezone(test_db):
    db = await get_database()
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')")
    uid = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO webcal_subscriptions (user_id, url) VALUES (?, ?)",
        (uid, URL))
    sid = int(cur.lastrowid)
    await db.commit()

    body = _ics(
        "UID:mez-db@example.com\n"
        "SUMMARY:Wochenmeeting\n"
        "DTSTART;TZID=Mitteleuropaeische Zeit:20260601T090000\n"
        "DTEND;TZID=Mitteleuropaeische Zeit:20260601T100000\n"
        "RRULE:FREQ=WEEKLY;BYDAY=MO",
        vtimezone=VTZ_MEZ,
    )

    async def fetch(url, if_none_match):
        return {"status": 200, "etag": "e1", "body": body}

    c = await ingest_webcal_subscription(
        db, user_id=uid, subscription_id=sid, url=URL, fetch=fetch,
        now=datetime(2026, 6, 1, 6, 0, tzinfo=UTC),
    )
    assert c["created"] == 1
    row = await (await db.execute(
        "SELECT start_timezone, end_timezone FROM ledger_events "
        "WHERE user_id = ? AND source_event_id = 'mez-db@example.com'",
        (uid,),
    )).fetchone()
    assert row["start_timezone"] == "Europe/Berlin"
    assert row["end_timezone"] == "Europe/Berlin"


# ---------------------------------------------------------------------------
# Fix 2: UNTIL rewritten to RFC 5545 / Google-compliant UTC
# ---------------------------------------------------------------------------
def _last_occurrence(lines: list[str], dtstart: datetime) -> datetime:
    rule = rrulestr("\n".join(lines), dtstart=dtstart, forceset=True)
    return list(rule)[-1]


def test_naive_until_rewritten_to_utc_z():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:until-1@example.com\n"
        "DTSTART;TZID=Europe/Berlin:20260105T090000\n"
        "DTEND;TZID=Europe/Berlin:20260105T100000\n"
        "RRULE:FREQ=WEEKLY;UNTIL=20261221T090000",
    )))
    (line,) = _recurrence(d)
    # 09:00 CET on 2026-12-21 == 08:00Z.
    assert line == "RRULE:FREQ=WEEKLY;UNTIL=20261221T080000Z"
    # The final occurrence is preserved: still 2026-12-21 09:00 Berlin.
    dtstart = datetime(2026, 1, 5, 9, 0, tzinfo=BERLIN)
    last = _last_occurrence([line], dtstart)
    assert last.astimezone(BERLIN) == datetime(
        2026, 12, 21, 9, 0, tzinfo=BERLIN)


def test_date_only_until_on_timed_event_expands_to_end_of_local_day():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:until-2@example.com\n"
        "DTSTART;TZID=Europe/Berlin:20260105T090000\n"
        "DTEND;TZID=Europe/Berlin:20260105T100000\n"
        "RRULE:FREQ=WEEKLY;UNTIL=20261221",
    )))
    (line,) = _recurrence(d)
    # End of 2026-12-21 in Berlin (23:59:59 CET) == 22:59:59Z; a bare
    # midnight reading would have truncated the Dec 21 occurrence.
    assert line == "RRULE:FREQ=WEEKLY;UNTIL=20261221T225959Z"
    dtstart = datetime(2026, 1, 5, 9, 0, tzinfo=BERLIN)
    last = _last_occurrence([line], dtstart)
    assert last.astimezone(BERLIN) == datetime(
        2026, 12, 21, 9, 0, tzinfo=BERLIN), (
        "the UNTIL-day occurrence itself was wrongly truncated"
    )


def test_compliant_utc_until_is_untouched():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:until-3@example.com\n"
        "DTSTART;TZID=Europe/Berlin:20260105T090000\n"
        "DTEND;TZID=Europe/Berlin:20260105T100000\n"
        "RRULE:FREQ=WEEKLY;UNTIL=20261221T080000Z",
    )))
    assert _recurrence(d) == ["RRULE:FREQ=WEEKLY;UNTIL=20261221T080000Z"]


def test_all_day_event_keeps_date_only_until():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:until-4@example.com\n"
        "DTSTART;VALUE=DATE:20260105\n"
        "RRULE:FREQ=WEEKLY;UNTIL=20261221",
    )))
    assert d["is_all_day"] is True
    assert _recurrence(d) == ["RRULE:FREQ=WEEKLY;UNTIL=20261221"]


def test_naive_until_under_floating_dtstart_treated_as_utc():
    # A floating DTSTART is normalised as UTC, so its floating UNTIL
    # must resolve in the same (UTC) frame to stay on the series grid.
    d = _vevent_to_dict(_vevent(_ics(
        "UID:until-5@example.com\n"
        "DTSTART:20260105T090000\n"
        "DTEND:20260105T100000\n"
        "RRULE:FREQ=WEEKLY;UNTIL=20261221T090000",
    )))
    assert _recurrence(d) == ["RRULE:FREQ=WEEKLY;UNTIL=20261221T090000Z"]


def test_custom_tzid_until_converts_with_the_resolved_zone():
    # Fix 1 + fix 2 together: the zone resolved from the VTIMEZONE is
    # also the frame the naive UNTIL converts in.
    d = _vevent_to_dict(_vevent(_ics(
        "UID:until-6@example.com\n"
        "DTSTART;TZID=Mitteleuropaeische Zeit:20260105T090000\n"
        "DTEND;TZID=Mitteleuropaeische Zeit:20260105T100000\n"
        "RRULE:FREQ=WEEKLY;UNTIL=20261221T090000",
        vtimezone=VTZ_MEZ,
    )))
    assert _recurrence(d) == ["RRULE:FREQ=WEEKLY;UNTIL=20261221T080000Z"]


# ---------------------------------------------------------------------------
# Fix 3: awareness-mismatched recurrence lines no longer kill expansion
# ---------------------------------------------------------------------------
AWARE_DTSTART = datetime(2026, 1, 6, 9, 0, tzinfo=BERLIN)


def test_date_only_until_with_aware_dtstart_expands():
    occs = expand_occurrences(
        ["RRULE:FREQ=WEEKLY;UNTIL=20260421"], AWARE_DTSTART)
    assert occs is not None, "expansion swallowed into permanent None"
    assert len(occs) == 16  # Jan 6 .. Apr 21 inclusive, weekly
    assert occs[-1] == datetime(2026, 4, 21, 9, 0, tzinfo=BERLIN), (
        "the UNTIL-day occurrence must be included (end-of-day bound)"
    )


def test_naive_datetime_until_with_aware_dtstart_expands():
    occs = expand_occurrences(
        ["RRULE:FREQ=WEEKLY;UNTIL=20260421T090000"], AWARE_DTSTART)
    assert occs is not None
    assert occs[-1] == datetime(2026, 4, 21, 9, 0, tzinfo=BERLIN)


def test_utc_z_until_with_aware_dtstart_still_expands():
    occs = expand_occurrences(
        ["RRULE:FREQ=WEEKLY;UNTIL=20260421T080000Z"], AWARE_DTSTART)
    assert occs is not None
    assert occs[-1] == datetime(2026, 4, 21, 9, 0, tzinfo=BERLIN)


def test_occurrence_in_series_decides_for_date_only_until():
    lines = ["RRULE:FREQ=WEEKLY;UNTIL=20260421"]
    covered = occurrence_in_series(
        lines, AWARE_DTSTART,
        datetime(2026, 1, 13, 8, 0, tzinfo=UTC),  # Jan 13 09:00 Berlin
        is_all_day=False,
    )
    uncovered = occurrence_in_series(
        lines, AWARE_DTSTART,
        datetime(2026, 1, 13, 9, 0, tzinfo=UTC),  # not on the grid
        is_all_day=False,
    )
    assert covered is True, "must decide, not return indeterminate None"
    assert uncovered is False


def test_naive_exdate_with_aware_dtstart_excludes_that_occurrence():
    occs = expand_occurrences(
        [
            "RRULE:FREQ=WEEKLY;UNTIL=20260421T080000Z",
            "EXDATE:20260113T090000",  # naive: Berlin wall clock
        ],
        AWARE_DTSTART,
    )
    assert occs is not None, "one mismatched EXDATE killed expansion"
    assert len(occs) == 15
    assert datetime(2026, 1, 13, 9, 0, tzinfo=BERLIN) not in occs


def test_date_only_exdate_with_aware_dtstart_excludes_that_day():
    occs = expand_occurrences(
        [
            "RRULE:FREQ=WEEKLY;UNTIL=20260421T080000Z",
            "EXDATE;VALUE=DATE:20260113",
        ],
        AWARE_DTSTART,
    )
    assert occs is not None
    assert all(o.date() != datetime(2026, 1, 13).date() for o in occs)


def test_aware_exdate_with_naive_all_day_dtstart_expands():
    # The reverse mismatch: aware (Z-stamped) EXDATE under a naive
    # all-day dtstart previously raised inside dateutil too.
    occs = expand_occurrences(
        [
            "RRULE:FREQ=WEEKLY;UNTIL=20260421",
            "EXDATE:20260113T000000Z",
        ],
        datetime(2026, 1, 6),
    )
    assert occs is not None
    assert len(occs) == 15
    assert datetime(2026, 1, 13) not in occs


def test_tzid_exdate_with_aware_dtstart_still_works():
    # Already-working shape must stay working: TZID param is aware.
    occs = expand_occurrences(
        [
            "RRULE:FREQ=WEEKLY;UNTIL=20260421T080000Z",
            "EXDATE;TZID=Europe/Berlin:20260113T090000",
        ],
        AWARE_DTSTART,
    )
    assert occs is not None
    assert len(occs) == 15


def test_naive_rdate_with_aware_dtstart_localizes():
    occs = expand_occurrences(
        [
            "RRULE:FREQ=WEEKLY;UNTIL=20260127T080000Z",
            "RDATE:20260201T090000",  # naive: Berlin wall clock
        ],
        AWARE_DTSTART,
    )
    assert occs is not None
    assert datetime(2026, 2, 1, 9, 0, tzinfo=BERLIN).astimezone(UTC) in [
        o.astimezone(UTC) for o in occs
    ]


# ---------------------------------------------------------------------------
# Fix 4: floating RECURRENCE-ID interpreted in the parent's zone
# ---------------------------------------------------------------------------
def test_floating_recurrence_id_uses_parent_zone():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:rid-1@example.com\n"
        "SUMMARY:Moved instance\n"
        "DTSTART;TZID=Europe/Berlin:20261102T090000\n"
        "DTEND;TZID=Europe/Berlin:20261102T100000\n"
        "RECURRENCE-ID:20261102T090000",
    )))
    # 09:00 CET (winter) == 08:00Z; reading the floating value as UTC
    # would target 09:00Z — an occurrence that does not exist, so the
    # override/cancellation would be silently lost.
    assert d["recurrence_id"] == "2026-11-02T08:00:00Z"


def test_floating_recurrence_id_in_summer_uses_dst_offset():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:rid-2@example.com\n"
        "DTSTART;TZID=Europe/Berlin:20260706T090000\n"
        "DTEND;TZID=Europe/Berlin:20260706T100000\n"
        "RECURRENCE-ID:20260706T090000",
    )))
    assert d["recurrence_id"] == "2026-07-06T07:00:00Z"  # 09:00 CEST


def test_utc_recurrence_id_unchanged():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:rid-3@example.com\n"
        "DTSTART;TZID=Europe/Berlin:20261102T090000\n"
        "DTEND;TZID=Europe/Berlin:20261102T100000\n"
        "RECURRENCE-ID:20261102T080000Z",
    )))
    assert d["recurrence_id"] == "2026-11-02T08:00:00Z"


def test_tzid_recurrence_id_unchanged():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:rid-4@example.com\n"
        "DTSTART;TZID=Europe/Berlin:20261102T093000\n"
        "DTEND;TZID=Europe/Berlin:20261102T103000\n"
        "RECURRENCE-ID;TZID=Europe/Berlin:20261102T090000",
    )))
    assert d["recurrence_id"] == "2026-11-02T08:00:00Z"


def test_floating_recurrence_id_without_zone_context_stays_utc():
    # Fully floating event (no zone anywhere): historical UTC reading
    # is the only consistent choice and must not change.
    d = _vevent_to_dict(_vevent(_ics(
        "UID:rid-5@example.com\n"
        "DTSTART:20261102T090000\n"
        "DTEND:20261102T100000\n"
        "RECURRENCE-ID:20261102T090000",
    )))
    assert d["recurrence_id"] == "2026-11-02T09:00:00Z"


def test_custom_tzid_recurrence_id_converts_with_resolved_zone():
    # Fix 1 + fix 4 together: floating RECURRENCE-ID under a
    # localized-TZID DTSTART converts in the VTIMEZONE-derived zone.
    d = _vevent_to_dict(_vevent(_ics(
        "UID:rid-6@example.com\n"
        "DTSTART;TZID=Mitteleuropaeische Zeit:20261102T090000\n"
        "DTEND;TZID=Mitteleuropaeische Zeit:20261102T100000\n"
        "RECURRENCE-ID:20261102T090000",
        vtimezone=VTZ_MEZ,
    )))
    assert d["recurrence_id"] == "2026-11-02T08:00:00Z"
