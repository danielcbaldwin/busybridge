"""Webcal ingest fixes: DTSTART+DURATION support and per-event
poison isolation in the poll loop.

* ``DURATION`` — RFC 5545 allows DTSTART + DURATION instead of DTEND.
  Previously the duration was ignored and the event shrank to the
  30-minute default end.
* Poison isolation — one VEVENT raising out of ``_ingest_ics_event``
  used to abort the poll before ``_record_fetch_success``: the etag
  never advanced, the identical body refailed every poll, and later
  VEVENTs never ingested.  Now the poison event is counted as
  ``failed`` and skipped; and because its canonical UID never reaches
  the seen set, stale-cancellation is suppressed on polls with
  failures so its existing busy block is not misread as "gone".
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import app.ledger.ingest.webcal as webcal_ingest
from app.database import get_database
from app.ledger.ingest.webcal import (
    _vevent_to_dict,
    ingest_webcal_subscription,
)
from icalendar import Calendar

UTC = timezone.utc
URL = "webcal://tripit.example/feed.ics"


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


# ---------------------------------------------------------------------------
# DTSTART + DURATION (no DTEND)
# ---------------------------------------------------------------------------
def test_timed_duration_sets_the_end():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:dur-1@example.com\n"
        "SUMMARY:Two hour block\n"
        "DTSTART:20260710T090000Z\n"
        "DURATION:PT2H"
    )))
    assert d["start_at"] == "2026-07-10T09:00:00Z"
    assert d["end_at"] == "2026-07-10T11:00:00Z", (
        f"DURATION ignored, got {d['end_at']}"
    )
    assert d["is_all_day"] is False


def test_all_day_duration_sets_the_end_date():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:dur-2@example.com\n"
        "SUMMARY:Two day offsite\n"
        "DTSTART;VALUE=DATE:20260710\n"
        "DURATION:P2D"
    )))
    assert d["is_all_day"] is True
    assert d["start_at"] == "2026-07-10"
    assert d["end_at"] == "2026-07-12", f"DURATION ignored, got {d['end_at']}"


def test_thirty_minute_fallback_only_without_dtend_and_duration():
    d = _vevent_to_dict(_vevent(_ics(
        "UID:dur-3@example.com\n"
        "SUMMARY:Endless\n"
        "DTSTART:20260710T090000Z"
    )))
    assert d["end_at"] == "2026-07-10T09:30:00Z"


# ---------------------------------------------------------------------------
# Poison-event isolation in the poll loop
# ---------------------------------------------------------------------------
EV1 = ("UID:trip-1@tripit\nSUMMARY:Trip A\n"
       "DTSTART:20260710T090000Z\nDTEND:20260710T100000Z")
EV2 = ("UID:trip-2@tripit\nSUMMARY:Trip B\n"
       "DTSTART:20260712T090000Z\nDTEND:20260712T100000Z")
EV3 = ("UID:trip-3@tripit\nSUMMARY:Trip C\n"
       "DTSTART:20260714T090000Z\nDTEND:20260714T100000Z")


def _fetch(body: bytes, etag: str):
    async def fetch(url, if_none_match):
        return {"status": 200, "etag": etag, "body": body}
    return fetch


async def _seed(db):
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')")
    uid = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO webcal_subscriptions (user_id, url) VALUES (?, ?)",
        (uid, URL))
    sid = int(cur.lastrowid)
    await db.commit()
    return uid, sid


async def _poll(db, uid, sid, body, etag, now):
    return await ingest_webcal_subscription(
        db, user_id=uid, subscription_id=sid, url=URL,
        fetch=_fetch(body, etag), now=now)


async def _statuses(db, sid):
    rows = await (await db.execute(
        "SELECT source_event_id, status FROM ledger_events "
        "WHERE source_type='webcal' AND source_calendar_id=? "
        "ORDER BY source_event_id",
        (sid,))).fetchall()
    return {r["source_event_id"]: r["status"] for r in rows}


async def _etag(db, sid):
    return (await (await db.execute(
        "SELECT last_etag FROM webcal_subscriptions WHERE id=?",
        (sid,))).fetchone())["last_etag"]


def _poison(monkeypatch, poison_uid: str):
    real = webcal_ingest._ingest_ics_event

    async def flaky(db, *, event, **kw):
        if event.get("uid") == poison_uid:
            raise RuntimeError("simulated UNIQUE collision")
        return await real(db, event=event, **kw)

    monkeypatch.setattr(webcal_ingest, "_ingest_ics_event", flaky)


@pytest.mark.asyncio
async def test_poison_event_skipped_others_ingest_etag_advances(
    test_db, monkeypatch,
):
    db = await get_database()
    uid, sid = await _seed(db)
    t0 = datetime(2026, 6, 10, 8, 0, tzinfo=UTC)
    _poison(monkeypatch, "trip-2@tripit")

    c = await _poll(db, uid, sid, _ics(EV1, EV2, EV3), "e1", t0)
    assert c["seen"] == 3
    assert c["failed"] == 1
    assert c["created"] == 2  # the two good events still ingested
    assert c["stale_cancelled"] == 0
    assert set((await _statuses(db, sid))) == {
        "trip-1@tripit", "trip-3@tripit",
    }
    # The etag still advances despite the poison event, so the next
    # poll is not doomed to refail the identical body.
    assert await _etag(db, sid) == "e1"


@pytest.mark.asyncio
async def test_failed_event_is_not_stale_cancelled(test_db, monkeypatch):
    db = await get_database()
    uid, sid = await _seed(db)
    t0 = datetime(2026, 6, 10, 8, 0, tzinfo=UTC)

    # Clean first poll — all three ingest.
    c = await _poll(db, uid, sid, _ics(EV1, EV2, EV3), "e1", t0)
    assert c["created"] == 3

    # Second poll well past 2 poll intervals with the middle event
    # poison-failing.  Its canonical UID never reaches the seen set,
    # so without the failed-poll guard it would be misread as "gone
    # from feed" and stale-cancelled — a false busy-block wipe.
    _poison(monkeypatch, "trip-2@tripit")
    c = await _poll(db, uid, sid, _ics(EV1, EV2, EV3), "e2", t0 + timedelta(minutes=11))
    assert c["failed"] == 1
    assert c["stale_cancelled"] == 0
    statuses = await _statuses(db, sid)
    assert statuses["trip-2@tripit"] == "active", (
        "failed event's busy block was falsely stale-cancelled"
    )
    assert statuses["trip-1@tripit"] == "active"
    assert statuses["trip-3@tripit"] == "active"
    assert await _etag(db, sid) == "e2"
