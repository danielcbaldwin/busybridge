"""Webcal UID-stability classification is per event, not per feed.

A feed mixing UUIDv4 UIDs with a domain-anchored UID must not let the
domain UID flip every other event's canonical_uid scheme — that would
mass-cancel and recreate the whole feed's busy blocks.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.database import get_database
from app.ledger.ingest.webcal import _ingest_ics_event

pytestmark = pytest.mark.asyncio


def _event(event_uid: str) -> dict:
    return {
        "uid": event_uid,
        "start_at": "2026-03-01T09:00:00+00:00",
        "end_at": "2026-03-01T10:00:00+00:00",
        "status": "CONFIRMED",
        "summary": "Event",
    }


async def test_uid_classification_is_per_event(test_db):
    db = await get_database()
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id, display_name) "
        "VALUES ('u@example.com', 'g-u', 'U') RETURNING id"
    )
    uid = int((await cur.fetchone())["id"])
    now = datetime.now(timezone.utc)

    _o1, _l1, canon_uuid = await _ingest_ics_event(
        db, user_id=uid, subscription_id=1,
        event=_event("550e8400-e29b-41d4-a716-446655440000"), now=now,
    )
    _o2, _l2, canon_domain = await _ingest_ics_event(
        db, user_id=uid, subscription_id=1,
        event=_event("event-123@eventbrite.com"), now=now,
    )

    # A UUIDv4 UID is classified unstable (hash-keyed); a domain UID
    # is classified stable (UID-keyed) — independently of each other.
    assert ":hash:" in canon_uuid, canon_uuid
    assert canon_domain == "webcal:1:event-123@eventbrite.com", canon_domain


async def test_distinct_unstable_events_same_time_do_not_collide(test_db):
    """Two genuinely-distinct unstable events sharing a start/end must
    get distinct canonical_uids — neither may overwrite the other."""
    from datetime import datetime, timezone

    db = await get_database()
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id, display_name) "
        "VALUES ('c@example.com', 'g-c', 'C') RETURNING id"
    )
    uid = int((await cur.fetchone())["id"])
    now = datetime.now(timezone.utc)

    def _ev(summary: str) -> dict:
        return {
            "uid": None,  # no UID -> unstable
            "start_at": "2026-03-01T09:00:00+00:00",
            "end_at": "2026-03-01T10:00:00+00:00",
            "status": "CONFIRMED",
            "summary": summary,
        }

    o1, l1, c1 = await _ingest_ics_event(
        db, user_id=uid, subscription_id=1, event=_ev("Track 1"), now=now,
    )
    o2, l2, c2 = await _ingest_ics_event(
        db, user_id=uid, subscription_id=1, event=_ev("Track 2"), now=now,
    )
    assert o1 == "created" and o2 == "created"
    assert c1 != c2, "distinct same-time events must not share a canonical_uid"
    assert l1 != l2
    rows = await (await db.execute(
        "SELECT summary FROM ledger_events WHERE user_id = ? ORDER BY id",
        (uid,),
    )).fetchall()
    assert sorted(r["summary"] for r in rows) == ["Track 1", "Track 2"]


async def test_unstable_event_reingested_next_poll_is_not_duplicated(test_db):
    """The same unstable event seen in a later poll reuses its row —
    the collision probe must not treat it as a new distinct event."""
    from datetime import datetime, timedelta, timezone

    db = await get_database()
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id, display_name) "
        "VALUES ('d@example.com', 'g-d', 'D') RETURNING id"
    )
    uid = int((await cur.fetchone())["id"])
    ev = {
        "uid": None,
        "start_at": "2026-03-01T09:00:00+00:00",
        "end_at": "2026-03-01T10:00:00+00:00",
        "status": "CONFIRMED",
        "summary": "Webinar",
    }
    poll1 = datetime(2026, 3, 1, 8, 0, tzinfo=timezone.utc)
    poll2 = poll1 + timedelta(minutes=10)

    o1, l1, _c1 = await _ingest_ics_event(
        db, user_id=uid, subscription_id=1, event=ev, now=poll1,
    )
    o2, l2, _c2 = await _ingest_ics_event(
        db, user_id=uid, subscription_id=1, event=ev, now=poll2,
    )
    assert o1 == "created"
    assert o2 != "created", "a re-poll of the same event must not create a row"
    assert l1 == l2
    count = await (await db.execute(
        "SELECT COUNT(*) AS n FROM ledger_events WHERE user_id = ?", (uid,),
    )).fetchone()
    assert count["n"] == 1
