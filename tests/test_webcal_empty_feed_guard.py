"""An empty-but-valid ICS feed (provider reset/outage: HTTP 200 + a
parseable but eventless VCALENDAR) must NOT mass-cancel the busy blocks a
subscription produced — that would make the user show FREE for real
commitments. Individual events dropping out of a NON-empty feed must
still cancel normally.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.database import get_database
from app.ledger.ingest.webcal import ingest_webcal_subscription

UTC = timezone.utc
URL = "webcal://tripit.example/feed.ics"

EV1 = ("UID:trip-1@tripit\nSUMMARY:Trip A\n"
       "DTSTART:20260710T090000Z\nDTEND:20260710T100000Z")
EV2 = ("UID:trip-2@tripit\nSUMMARY:Trip B\n"
       "DTSTART:20260712T090000Z\nDTEND:20260712T100000Z")
EMPTY_ICS = b"BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nEND:VCALENDAR\r\n"


def _ics(*vevents: str) -> bytes:
    body = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Test//EN"]
    for v in vevents:
        body.append("BEGIN:VEVENT")
        body.extend(line for line in v.strip().splitlines())
        body.append("END:VEVENT")
    body.append("END:VCALENDAR")
    return ("\r\n".join(body) + "\r\n").encode("utf-8")


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


async def _active(db, sid):
    return int((await (await db.execute(
        "SELECT COUNT(*) c FROM ledger_events "
        "WHERE source_type='webcal' AND source_calendar_id=? AND status='active'",
        (sid,))).fetchone())["c"])


async def _empties(db, sid):
    return int((await (await db.execute(
        "SELECT consecutive_empty_polls c FROM webcal_subscriptions WHERE id=?",
        (sid,))).fetchone())["c"])


async def _poll(db, uid, sid, body, etag, now):
    return await ingest_webcal_subscription(
        db, user_id=uid, subscription_id=sid, url=URL,
        fetch=_fetch(body, etag), now=now)


@pytest.mark.asyncio
async def test_empty_feed_never_wipes_blocks_and_recovers(test_db):
    db = await get_database()
    uid, sid = await _seed(db)
    t0 = datetime(2026, 6, 10, 8, 0, tzinfo=UTC)

    c = await _poll(db, uid, sid, _ics(EV1, EV2), "e1", t0)
    assert c["created"] == 2
    assert await _active(db, sid) == 2

    # Empty poll, well past 2 poll intervals — would normally stale-cancel.
    c = await _poll(db, uid, sid, EMPTY_ICS, "e2", t0 + timedelta(hours=2))
    assert c["empty_skipped"] == 1
    assert c["stale_cancelled"] == 0
    assert await _active(db, sid) == 2, "empty feed must not wipe busy blocks"
    assert await _empties(db, sid) == 1

    # Still empty hours later — still protected, counter climbs.
    c = await _poll(db, uid, sid, EMPTY_ICS, "e3", t0 + timedelta(hours=4))
    assert c["empty_skipped"] == 1
    assert await _active(db, sid) == 2
    assert await _empties(db, sid) == 2

    # Feed recovers: events still present, empty streak resets.
    c = await _poll(db, uid, sid, _ics(EV1, EV2), "e4", t0 + timedelta(hours=6))
    assert c["empty_skipped"] == 0
    assert await _active(db, sid) == 2
    assert await _empties(db, sid) == 0


@pytest.mark.asyncio
async def test_dropped_event_in_nonempty_feed_still_cancels(test_db):
    # The guard must be surgical: a NON-empty feed that drops one event
    # still cancels that one event (normal stale-detection).
    db = await get_database()
    uid, sid = await _seed(db)
    t0 = datetime(2026, 6, 10, 8, 0, tzinfo=UTC)

    await _poll(db, uid, sid, _ics(EV1, EV2), "e1", t0)
    assert await _active(db, sid) == 2

    # Re-poll with only EV1, > 2 poll intervals later → EV2 goes stale.
    c = await _poll(db, uid, sid, _ics(EV1), "e2", t0 + timedelta(minutes=11))
    assert c["empty_skipped"] == 0
    assert c["stale_cancelled"] == 1
    assert await _active(db, sid) == 1, "the genuinely-dropped event should cancel"
