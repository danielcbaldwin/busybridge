"""Regression: cancelled all-day recurring instances must record
is_all_day so the diff derives the correct Google instance ID.

Google's instance ID stamp is ``YYYYMMDD`` for an all-day series and
``YYYYMMDDTHHMMSSZ`` for a timed one.  A cancelled-instance ledger
row that failed to record ``is_all_day`` defaulted to 0, so the diff
built the timed-form stamp for an all-day series — the delete then
targeted an ID Google never had and the busy block was never removed.
"""

from __future__ import annotations

import pytest

from app.database import get_database
from app.ledger.ingest.client import _ingest_instance

pytestmark = pytest.mark.asyncio


async def _user(db) -> int:
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id, display_name) "
        "VALUES ('u@example.com', 'g-u', 'U') RETURNING id"
    )
    return int((await cur.fetchone())["id"])


@pytest.mark.parametrize("source_type", ["client", "personal", "main_native"])
async def test_cancelled_all_day_instance_records_is_all_day(test_db, source_type):
    """_ingest_instance is source-neutral — client, personal, and
    main-native cancelled all-day instances must all record is_all_day."""
    db = await get_database()
    uid = await _user(db)
    result, led_id = await _ingest_instance(
        db,
        user_id=uid,
        user_email="u@example.com",
        event={
            "id": "series_20260315",
            "status": "cancelled",
            "originalStartTime": {"date": "2026-03-15"},
        },
        parent_canonical=f"{source_type}:1:series",
        source_type=source_type,
        source_calendar_id=None if source_type == "main_native" else 1,
    )
    assert result == "cancelled"
    row = await (await db.execute(
        "SELECT is_all_day, status, recurrence_instance_original_start "
        "FROM ledger_events WHERE id = ?",
        (led_id,),
    )).fetchone()
    assert row["status"] == "cancelled"
    assert row["is_all_day"] == 1, (
        "an all-day cancelled instance must record is_all_day=1"
    )
    assert row["recurrence_instance_original_start"] == "2026-03-15"


async def test_cancelled_timed_instance_records_not_all_day(test_db):
    db = await get_database()
    uid = await _user(db)
    _result, led_id = await _ingest_instance(
        db,
        user_id=uid,
        user_email="u@example.com",
        event={
            "id": "series_20260315T090000Z",
            "status": "cancelled",
            "originalStartTime": {"dateTime": "2026-03-15T09:00:00Z"},
        },
        parent_canonical="client:1:series",
        source_type="client",
        source_calendar_id=1,
    )
    row = await (await db.execute(
        "SELECT is_all_day FROM ledger_events WHERE id = ?", (led_id,),
    )).fetchone()
    assert row["is_all_day"] == 0


async def test_cancelling_an_existing_modified_instance_keeps_is_all_day(test_db):
    """When a previously-modified all-day instance is later cancelled,
    the UPDATE path must also keep is_all_day set."""
    db = await get_database()
    uid = await _user(db)
    ev_modified = {
        "id": "series_20260315",
        "status": "confirmed",
        "originalStartTime": {"date": "2026-03-15"},
        "start": {"date": "2026-03-16"},
        "end": {"date": "2026-03-17"},
        "summary": "moved",
    }
    _r, led_id = await _ingest_instance(
        db, user_id=uid, user_email="u@example.com", event=ev_modified,
        parent_canonical="client:1:series", source_type="client",
        source_calendar_id=1,
    )
    ev_cancelled = {
        "id": "series_20260315",
        "status": "cancelled",
        "originalStartTime": {"date": "2026-03-15"},
    }
    result, led_id2 = await _ingest_instance(
        db, user_id=uid, user_email="u@example.com", event=ev_cancelled,
        parent_canonical="client:1:series", source_type="client",
        source_calendar_id=1,
    )
    assert result == "cancelled" and led_id2 == led_id
    row = await (await db.execute(
        "SELECT is_all_day, status FROM ledger_events WHERE id = ?", (led_id,),
    )).fetchone()
    assert row["status"] == "cancelled"
    assert row["is_all_day"] == 1


@pytest.mark.parametrize("recurrence_id,expect_all_day", [
    ("2026-03-15", 1),                  # RECURRENCE-ID;VALUE=DATE
    ("2026-03-15T09:00:00Z", 0),        # timed RECURRENCE-ID
])
async def test_webcal_cancelled_instance_records_is_all_day(
    test_db, recurrence_id, expect_all_day,
):
    """A cancelled webcal RECURRENCE-ID override must record is_all_day
    so the diff derives the correct Google instance ID — the webcal
    twin of the client-path fix."""
    from datetime import datetime, timezone

    from app.ledger.ingest.webcal import _ingest_ics_instance

    db = await get_database()
    uid = await _user(db)
    outcome, led_id, _canon = await _ingest_ics_instance(
        db,
        user_id=uid,
        subscription_id=1,
        event={"status": "CANCELLED", "recurrence_id": recurrence_id},
        parent_uid="series@example.com",
        now=datetime.now(timezone.utc),
    )
    assert outcome == "cancelled"
    row = await (await db.execute(
        "SELECT is_all_day FROM ledger_events WHERE id = ?", (led_id,),
    )).fetchone()
    assert row["is_all_day"] == expect_all_day
