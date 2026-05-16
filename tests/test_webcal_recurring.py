"""Webcal recurring-event handling (REWRITE_PLAN.md §5.4 / §8).

Two ICS recurrence features that were previously dropped:

* ``EXDATE`` — excluded occurrences.  Carried into the Google
  ``recurrence`` array so excluded dates are not materialised as
  ghost busy blocks.
* ``RECURRENCE-ID`` overrides — a modified/cancelled single
  occurrence.  Routed to its own instance ledger row instead of
  colliding with the parent series (both share the feed UID).
"""

from __future__ import annotations

import json

import pytest
from icalendar import Calendar

from app.ledger.ingest.webcal import _vevent_to_dict
from tests.integration.framework import Scenario


def _ics(*vevents: str) -> bytes:
    body = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Test//EN"]
    for v in vevents:
        body.append("BEGIN:VEVENT")
        body.extend(line for line in v.strip().splitlines())
        body.append("END:VEVENT")
    body.append("END:VCALENDAR")
    return ("\r\n".join(body) + "\r\n").encode("utf-8")


def _vevent(body: bytes):
    cal = Calendar.from_ical(body)
    return next(c for c in cal.walk() if c.name == "VEVENT")


def test_exdate_is_carried_into_the_recurrence_array():
    comp = _vevent(_ics(
        "UID:series-1@example.com\n"
        "SUMMARY:Weekly standup\n"
        "DTSTART:20260202T090000Z\n"
        "DTEND:20260202T093000Z\n"
        "RRULE:FREQ=WEEKLY;COUNT=10\n"
        "EXDATE:20260216T090000Z"
    ))
    d = _vevent_to_dict(comp)
    assert d["is_recurring"] is True
    rec = json.loads(d["recurrence_rule_json"])
    assert any(line.startswith("RRULE:") for line in rec)
    assert any(
        line.startswith("EXDATE:") and "20260216" in line for line in rec
    ), f"EXDATE dropped from recurrence array: {rec}"


def test_recurrence_id_override_is_detected():
    comp = _vevent(_ics(
        "UID:series-1@example.com\n"
        "SUMMARY:Weekly standup\n"
        "DTSTART:20260216T090000Z\n"
        "DTEND:20260216T093000Z\n"
        "RECURRENCE-ID:20260216T090000Z\n"
        "STATUS:CANCELLED"
    ))
    d = _vevent_to_dict(comp)
    assert d["recurrence_id"] == "2026-02-16T09:00:00Z"
    assert d["status"] == "CANCELLED"


@pytest.mark.asyncio
async def test_recurrence_id_override_does_not_collide_with_parent():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    await s.given_webcal("alice", sub_nick="feed", url="https://e.test/f.ics")

    body = _ics(
        # The recurring series master.
        "UID:weekly@example.com\n"
        "SUMMARY:Weekly\n"
        "DTSTART:20260202T090000Z\n"
        "DTEND:20260202T093000Z\n"
        "RRULE:FREQ=WEEKLY;COUNT=8",
        # A cancelled-occurrence override carrying the SAME UID.
        "UID:weekly@example.com\n"
        "SUMMARY:Weekly\n"
        "DTSTART:20260216T090000Z\n"
        "DTEND:20260216T093000Z\n"
        "RECURRENCE-ID:20260216T090000Z\n"
        "STATUS:CANCELLED",
    )

    async def fetch(url, if_none_match):
        return {"status": 200, "etag": '"v1"', "body": body}

    await s.run_reconciler("alice", webcal_fetch=fetch)

    db = await s.setup_db()
    rows = await (await db.execute(
        """SELECT canonical_uid, parent_canonical_uid, status, is_recurring
             FROM ledger_events
            WHERE user_id = ? AND source_type = 'webcal'
            ORDER BY id""",
        (user.user_id,),
    )).fetchall()

    # Two distinct rows — the parent series and the cancelled
    # instance — not one row the two VEVENTs fought over.
    assert len(rows) == 2, f"expected parent + instance, got {len(rows)}"
    parents = [r for r in rows if r["parent_canonical_uid"] is None]
    instances = [r for r in rows if r["parent_canonical_uid"] is not None]
    assert len(parents) == 1 and len(instances) == 1
    assert bool(parents[0]["is_recurring"]) is True
    assert instances[0]["status"] == "cancelled"
    assert instances[0]["parent_canonical_uid"] == parents[0]["canonical_uid"]
    await s.close()
