"""Disconnect / reauth / delete must STOP a user's Google push channels,
not just drop the local rows. Otherwise Google keeps POSTing to the dead
channel for its ~7-day TTL (the 'Unknown webhook channel' storm) and the
leftover row blocks retention's client_calendars delete via a RESTRICT FK.
"""

from __future__ import annotations

import pytest

import app.api.webhooks as webhooks
import app.auth.google as google_auth
from app.api.webhooks import stop_channels_for_user
from app.database import get_database

pytestmark = pytest.mark.asyncio


async def _seed(db, *, calendar_type="client"):
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')")
    uid = int(cur.lastrowid)
    ccid = None
    if calendar_type == "client":
        cur = await db.execute(
            "INSERT INTO oauth_tokens "
            "(user_id, account_type, google_account_email, "
            " access_token_encrypted, refresh_token_encrypted) "
            "VALUES (?, 'client', 'client@x.com', ?, ?)", (uid, b"a", b"b"))
        tok = int(cur.lastrowid)
        cur = await db.execute(
            "INSERT INTO client_calendars "
            "(user_id, oauth_token_id, google_calendar_id, display_name) "
            "VALUES (?, ?, 'cal@g', 'C')", (uid, tok))
        ccid = int(cur.lastrowid)
        await db.execute(
            "INSERT INTO webhook_channels "
            "(user_id, calendar_type, client_calendar_id, channel_id, "
            " resource_id, expiration) "
            "VALUES (?, 'client', ?, 'chan-c', 'res-c', '2026-12-01T00:00:00')",
            (uid, ccid))
    else:  # main
        await db.execute(
            "INSERT INTO webhook_channels "
            "(user_id, calendar_type, client_calendar_id, channel_id, "
            " resource_id, expiration) "
            "VALUES (?, 'main', NULL, 'chan-m', 'res-m', '2026-12-01T00:00:00')",
            (uid,))
    await db.commit()
    return uid, ccid


def _patch_stop(monkeypatch, *, recorder, succeed=True):
    async def fake_stop(channel_id, resource_id, access_token):
        recorder.append((channel_id, resource_id, access_token))
        if succeed:
            db = await get_database()
            await db.execute(
                "DELETE FROM webhook_channels WHERE channel_id = ?", (channel_id,))
            await db.commit()
            return True
        return False
    monkeypatch.setattr(webhooks, "stop_webhook_channel", fake_stop)


async def _channel_count(db):
    return int((await (await db.execute(
        "SELECT COUNT(*) c FROM webhook_channels")).fetchone())["c"])


async def test_stops_channel_on_google_and_removes_row(test_db, monkeypatch):
    db = await get_database()
    uid, ccid = await _seed(db)
    calls = []
    _patch_stop(monkeypatch, recorder=calls, succeed=True)

    async def fake_token(user_id, email):
        return "tok-123"
    monkeypatch.setattr(google_auth, "get_valid_access_token", fake_token)

    stopped = await stop_channels_for_user(db, user_id=uid, client_calendar_id=ccid)

    assert stopped == 1
    assert calls == [("chan-c", "res-c", "tok-123")]
    assert await _channel_count(db) == 0


async def test_falls_back_to_row_delete_when_token_unavailable(test_db, monkeypatch):
    # Revoked token: can't stop on Google, but the local row must still be
    # removed so it isn't left dangling and the FK is cleared.
    db = await get_database()
    uid, ccid = await _seed(db)
    calls = []
    _patch_stop(monkeypatch, recorder=calls, succeed=True)

    async def boom(user_id, email):
        raise RuntimeError("invalid_grant")
    monkeypatch.setattr(google_auth, "get_valid_access_token", boom)

    stopped = await stop_channels_for_user(db, user_id=uid, client_calendar_id=ccid)

    assert stopped == 0
    assert calls == []  # never reached stop (token fetch failed first)
    assert await _channel_count(db) == 0  # row removed anyway


async def test_main_channel_resolves_user_email(test_db, monkeypatch):
    db = await get_database()
    uid, _ = await _seed(db, calendar_type="main")
    calls = []
    _patch_stop(monkeypatch, recorder=calls, succeed=True)

    seen_emails = []

    async def fake_token(user_id, email):
        seen_emails.append(email)
        return "tok-m"
    monkeypatch.setattr(google_auth, "get_valid_access_token", fake_token)

    stopped = await stop_channels_for_user(db, user_id=uid)  # no filter → all

    assert stopped == 1
    assert seen_emails == ["u@x.com"]  # main resolves to the user's home email
    assert calls == [("chan-m", "res-m", "tok-m")]
    assert await _channel_count(db) == 0


async def test_disconnect_calendar_tears_down_the_channel(test_db, monkeypatch):
    # Integration: admin_ops.disconnect_calendar must invoke the teardown.
    from app.ledger.admin_ops import disconnect_calendar
    db = await get_database()
    uid, ccid = await _seed(db)
    calls = []
    _patch_stop(monkeypatch, recorder=calls, succeed=True)

    async def fake_token(user_id, email):
        return "tok-d"
    monkeypatch.setattr(google_auth, "get_valid_access_token", fake_token)

    await disconnect_calendar(db, user_id=uid, client_calendar_id=ccid)

    assert calls == [("chan-c", "res-c", "tok-d")]
    assert await _channel_count(db) == 0
