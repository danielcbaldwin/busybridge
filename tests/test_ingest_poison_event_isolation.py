"""Regression for the pre-cutover ingest blocker.

A single poison event in a sync page (e.g. a ``canonical_uid`` UNIQUE
collision) must NOT abort the whole ingest pass.  If it did, the
sync-token write at the end of the pass would be skipped, the token
would freeze, and every later event would silently stop reaching the
main calendar — the worst-case "missing busy block" failure mode.

These tests assert the per-event isolation added to
``ingest_client_calendar`` / ``ingest_main_calendar`` /
``ingest_personal_calendar``: the poison event
is counted as ``failed`` and skipped, the surrounding events still
ingest, and the sync token still advances.
"""

import pytest

import app.ledger.ingest.client as client_ingest
import app.ledger.ingest.main as main_ingest
import app.ledger.ingest.personal as personal_ingest
from app.ledger.ingest.client import ingest_client_calendar
from app.ledger.ingest.main import ingest_main_calendar
from app.ledger.ingest.personal import ingest_personal_calendar


class _FakeGoogle:
    """Minimal synchronous GoogleClient: one page + a next sync token."""

    def __init__(self, items, next_sync_token="TOK1"):
        self._items = items
        self._next = next_sync_token

    def list_events(self, calendar_id, *, sync_token=None, page_token=None,
                    show_deleted=False, max_results=250):
        return {"items": self._items, "nextSyncToken": self._next}


def _ev(eid, summary):
    return {
        "id": eid,
        "status": "confirmed",
        "summary": summary,
        "start": {"dateTime": "2026-06-10T10:00:00Z"},
        "end": {"dateTime": "2026-06-10T11:00:00Z"},
    }


async def _seed_user(db, *, with_client=True):
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id, main_calendar_id) "
        "VALUES ('u@x.com', 'g1', 'main-google-id')"
    )
    user_id = int(cur.lastrowid)
    await db.execute(
        "INSERT INTO main_calendar_sync_state (user_id) VALUES (?)", (user_id,)
    )
    ccid = None
    if with_client:
        cur = await db.execute(
            "INSERT INTO oauth_tokens "
            "(user_id, account_type, google_account_email, "
            " access_token_encrypted, refresh_token_encrypted) "
            "VALUES (?, 'client', 'u@x.com', ?, ?)",
            (user_id, b"x", b"y"),
        )
        tok_id = int(cur.lastrowid)
        cur = await db.execute(
            "INSERT INTO client_calendars "
            "(user_id, oauth_token_id, google_calendar_id, display_name) "
            "VALUES (?, ?, 'cal-google-id', 'Client')",
            (user_id, tok_id),
        )
        ccid = int(cur.lastrowid)
    await db.commit()
    return user_id, ccid


@pytest.mark.asyncio
async def test_client_poison_event_skipped_and_token_advances(test_db, monkeypatch):
    db = test_db
    user_id, ccid = await _seed_user(db)

    events = [_ev("ev-ok-1", "A"), _ev("ev-poison", "B"), _ev("ev-ok-2", "C")]

    real = client_ingest._ingest_one_event

    async def flaky(db, *, event, **kw):
        if event["id"] == "ev-poison":
            raise RuntimeError("simulated UNIQUE collision")
        return await real(db, event=event, **kw)

    monkeypatch.setattr(client_ingest, "_ingest_one_event", flaky)

    counters = await ingest_client_calendar(
        db, _FakeGoogle(events),
        user_id=user_id, client_calendar_id=ccid,
        google_calendar_id="cal-google-id", user_email="u@x.com",
    )

    assert counters["seen"] == 3
    assert counters["failed"] == 1
    assert counters["created"] == 2  # the two good events still ingested

    row = await (await db.execute(
        "SELECT sync_token FROM calendar_sync_state WHERE client_calendar_id=?",
        (ccid,),
    )).fetchone()
    assert row["sync_token"] == "TOK1"  # token advanced despite the poison event


@pytest.mark.asyncio
async def test_main_poison_event_skipped_and_token_advances(test_db, monkeypatch):
    db = test_db
    user_id, _ = await _seed_user(db, with_client=False)

    events = [_ev("ev-ok-1", "A"), _ev("ev-poison", "B"), _ev("ev-ok-2", "C")]

    real = main_ingest._ingest_one_main_event

    async def flaky(db, *, event, **kw):
        if event["id"] == "ev-poison":
            raise RuntimeError("simulated collision")
        return await real(db, event=event, **kw)

    monkeypatch.setattr(main_ingest, "_ingest_one_main_event", flaky)

    counters = await ingest_main_calendar(
        db, _FakeGoogle(events),
        user_id=user_id, google_main_calendar_id="main-google-id",
        user_email="u@x.com",
    )

    assert counters["seen"] == 3
    assert counters["failed"] == 1

    row = await (await db.execute(
        "SELECT sync_token FROM main_calendar_sync_state WHERE user_id=?",
        (user_id,),
    )).fetchone()
    assert row["sync_token"] == "TOK1"


@pytest.mark.asyncio
async def test_personal_poison_event_skipped_and_token_advances(test_db, monkeypatch):
    db = test_db
    user_id, _ = await _seed_user(db, with_client=False)
    # Personal calendars reuse client_calendars with
    # calendar_type='personal' (see app/ledger/ingest/personal.py).
    cur = await db.execute(
        "INSERT INTO oauth_tokens "
        "(user_id, account_type, google_account_email, "
        " access_token_encrypted, refresh_token_encrypted) "
        "VALUES (?, 'personal', 'p@x.com', ?, ?)",
        (user_id, b"x", b"y"),
    )
    tok_id = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO client_calendars "
        "(user_id, oauth_token_id, google_calendar_id, display_name, "
        " calendar_type) "
        "VALUES (?, ?, 'personal-google-id', 'Personal', 'personal')",
        (user_id, tok_id),
    )
    pcid = int(cur.lastrowid)
    await db.commit()

    events = [_ev("ev-ok-1", "A"), _ev("ev-poison", "B"), _ev("ev-ok-2", "C")]

    real = personal_ingest._ingest_one

    async def flaky(db, *, event, **kw):
        if event["id"] == "ev-poison":
            raise RuntimeError("simulated UNIQUE collision")
        return await real(db, event=event, **kw)

    monkeypatch.setattr(personal_ingest, "_ingest_one", flaky)

    counters = await ingest_personal_calendar(
        db, _FakeGoogle(events),
        user_id=user_id, personal_calendar_id=pcid,
        google_calendar_id="personal-google-id", user_email="u@x.com",
    )

    assert counters["seen"] == 3
    assert counters["failed"] == 1
    assert counters["created"] == 2  # the two good events still ingested

    row = await (await db.execute(
        "SELECT sync_token FROM calendar_sync_state WHERE client_calendar_id=?",
        (pcid,),
    )).fetchone()
    assert row["sync_token"] == "TOK1"  # token advanced despite the poison event
