"""A modified or cancelled INSTANCE of one of BusyBridge's own
managed recurring main copies must not be mis-ingested as a native
main event — that would mint a phantom ledger row and duplicate busy
blocks onto every client calendar.
"""

from __future__ import annotations

import pytest

from app.database import get_database
from app.ledger.ingest.main import _ingest_one_main_event

pytestmark = pytest.mark.asyncio

# bb + 13 base32hex chars == a well-formed managed (series) id.
_MANAGED_PARENT = "bb" + "a" * 13


async def _user(db) -> int:
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id, display_name) "
        "VALUES ('u@example.com', 'g-u', 'U') RETURNING id"
    )
    return int((await cur.fetchone())["id"])


async def test_modified_instance_of_managed_copy_creates_no_phantom(test_db):
    db = await get_database()
    uid = await _user(db)
    event = {
        "id": f"{_MANAGED_PARENT}_20260315T090000Z",
        "status": "confirmed",
        "recurringEventId": _MANAGED_PARENT,
        "start": {"dateTime": "2026-03-15T11:00:00Z"},
        "end": {"dateTime": "2026-03-15T12:00:00Z"},
        "summary": "user dragged this occurrence",
    }
    outcome, ledger_id = await _ingest_one_main_event(
        db, user_id=uid, user_email="u@example.com", event=event,
    )
    assert outcome == "our_writes_skipped"
    assert ledger_id is None
    n = await (await db.execute(
        "SELECT COUNT(*) AS n FROM ledger_events WHERE user_id = ?", (uid,),
    )).fetchone()
    assert n["n"] == 0, "a modified managed-copy instance must not mint a row"


async def test_cancelled_instance_of_managed_copy_is_skipped(test_db):
    db = await get_database()
    uid = await _user(db)
    event = {
        "id": f"{_MANAGED_PARENT}_20260315T090000Z",
        "status": "cancelled",
        "recurringEventId": _MANAGED_PARENT,
    }
    outcome, ledger_id = await _ingest_one_main_event(
        db, user_id=uid, user_email="u@example.com", event=event,
    )
    assert outcome == "our_writes_skipped"
    assert ledger_id is None
    n = await (await db.execute(
        "SELECT COUNT(*) AS n FROM ledger_events WHERE user_id = ?", (uid,),
    )).fetchone()
    assert n["n"] == 0
