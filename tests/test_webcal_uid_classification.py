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
