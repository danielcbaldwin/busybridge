"""Verify the BB_FAKE_GOOGLE=1 boot hook + debug endpoints work.

These are the same flows I ran manually against a uvicorn process
to prove the app boots and serves traffic with the fake Google
plugged in.  Captured here so the regression is automated.
"""

from __future__ import annotations

import importlib
import os

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from app.auth.session import create_session_token


@pytest_asyncio.fixture
async def app_with_fake_google(test_db, monkeypatch):
    """Set BB_FAKE_GOOGLE=1, reload app.main so the debug endpoints
    are registered, return the configured TestClient.
    """
    monkeypatch.setenv("BB_FAKE_GOOGLE", "1")
    # Reload the module so the `if os.environ.get("BB_FAKE_GOOGLE") == "1"`
    # branch at import time fires this time.
    import app.main as main_mod
    importlib.reload(main_mod)

    from app.database import get_database
    db = await get_database()

    # Seed an admin user + 2 client calendars.
    await db.execute(
        """INSERT OR IGNORE INTO organization
              (id, google_workspace_domain, google_client_id_encrypted,
               google_client_secret_encrypted)
           VALUES (1, ?, ?, ?)""",
        ("example.com", b"x", b"x"),
    )
    await db.execute(
        "INSERT OR REPLACE INTO settings (key, value_plain, is_sensitive) "
        "VALUES ('oobe_completed', 'true', 0)",
    )
    cur = await db.execute(
        """INSERT INTO users
              (email, google_user_id, display_name, main_calendar_id, is_admin)
           VALUES ('alice@example.com', 'g-alice', 'Alice',
                   'alice@example.com', 1)
           RETURNING id""",
    )
    user_id = int((await cur.fetchone())["id"])
    cur = await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'home', 'alice@example.com', x'00', x'00')
           RETURNING id""",
        (user_id,),
    )
    token_id = int((await cur.fetchone())["id"])
    for gid, name in [
        ("client_a@cal.test", "Client A"),
        ("client_b@cal.test", "Client B"),
    ]:
        await db.execute(
            """INSERT INTO client_calendars
                  (user_id, oauth_token_id, google_calendar_id,
                   display_name, calendar_type, is_active)
               VALUES (?, ?, ?, ?, 'client', 1)""",
            (user_id, token_id, gid, name),
        )
    await db.commit()

    with TestClient(main_mod.app) as client:
        client.cookies.set(
            "session",
            create_session_token(
                user_id=user_id, email="alice@example.com", is_admin=True,
            ),
        )
        yield client, user_id


def test_bb_fake_google_end_to_end(app_with_fake_google):
    """The exact flow we ran by hand: plant events, reconcile,
    verify state."""
    client, user_id = app_with_fake_google

    # /health
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"

    # Plant 2 events via the debug endpoint.
    for summary, start, end in [
        ("Project sync", "2026-06-02T09:00:00Z", "2026-06-02T09:30:00Z"),
        ("Strategy review", "2026-06-02T14:00:00Z", "2026-06-02T15:00:00Z"),
    ]:
        r = client.post(
            "/_fake/calendars/client_a@cal.test/events",
            json={
                "summary": summary,
                "start": {"dateTime": start, "timeZone": "UTC"},
                "end": {"dateTime": end, "timeZone": "UTC"},
            },
        )
        assert r.status_code == 200, r.text

    # Reconcile via the admin endpoint.
    r = client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")
    assert r.status_code == 200, r.text
    result = r.json()["result"]
    assert result["planned"] == 2
    assert result["enqueued"] == 4
    assert result["drain"]["succeeded"] == 4

    # Health endpoint reflects new state.
    r = client.get(f"/api/admin/ledger/health/{user_id}")
    assert r.status_code == 200
    h = r.json()
    assert h["main_copies"] == 2
    assert sum(h["busy_blocks_by_calendar"].values()) == 2

    # Dashboard renders.
    r = client.get("/app", follow_redirects=False)
    assert r.status_code == 200

    # Cancel one source event + reconcile + verify cancellation.
    src = next(
        e for e in client.get("/_fake/calendars/client_a@cal.test/events").json()["items"]
        if e["summary"] == "Project sync"
    )
    r = client.delete(f"/_fake/calendars/client_a@cal.test/events/{src['id']}")
    assert r.status_code == 200

    client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")
    remaining = {
        e["summary"]
        for e in client.get("/_fake/calendars/alice@example.com/events").json()["items"]
    }
    assert "Project sync" not in remaining
    assert "Strategy review" in remaining


def test_bb_fake_google_recurring_cancellation_propagates(app_with_fake_google):
    """The headline architectural claim: cancelling one instance of
    a recurring series on the source produces a cancelled instance
    exception on the main copy.  Verified via the
    events.instances(show_deleted=True) endpoint (the reliable
    retrieval path; events.list with show_deleted is the BUGGY
    path that we faithfully reproduce — see QUIRKS.md)."""
    client, user_id = app_with_fake_google

    # Plant a weekly recurring series with 4 occurrences.
    r = client.post(
        "/_fake/calendars/client_a@cal.test/events",
        json={
            "id": "bbstandupv01",  # base32hex
            "summary": "Weekly standup",
            "start": {"dateTime": "2026-06-08T09:00:00Z", "timeZone": "UTC"},
            "end":   {"dateTime": "2026-06-08T09:30:00Z", "timeZone": "UTC"},
            "recurrence": ["RRULE:FREQ=WEEKLY;COUNT=4;BYDAY=MO"],
        },
    )
    assert r.status_code == 200, r.text
    parent_id_on_source = r.json()["id"]

    # Reconcile creates the series on main + busy block on client_b.
    r = client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")
    assert r.status_code == 200, r.text
    main_items = client.get("/_fake/calendars/alice@example.com/events").json()["items"]
    recurring_on_main = [e for e in main_items if e.get("recurrence")]
    assert len(recurring_on_main) == 1
    main_parent_id = recurring_on_main[0]["id"]

    # Cancel ONE instance on the source (the Jun 15 occurrence).
    instance_id_on_source = f"{parent_id_on_source}_20260615T090000Z"
    r = client.delete(
        f"/_fake/calendars/client_a@cal.test/events/{instance_id_on_source}",
    )
    assert r.status_code == 200, r.text

    # Reconcile again.
    client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")

    # Verify on main via events.instances(showDeleted=True) — the
    # reliable retrieval path.
    r = client.get(
        f"/_fake/calendars/alice@example.com/events/{main_parent_id}/instances",
        params={"show_deleted": "true"},
    )
    assert r.status_code == 200
    statuses = [
        (i["status"], (i.get("originalStartTime") or i.get("start") or {}).get("dateTime"))
        for i in r.json()["items"]
    ]
    # 4 weekly instances, exactly one cancelled at 2026-06-15.
    assert len(statuses) == 4
    cancelled_15th = [
        s for s in statuses
        if s[0] == "cancelled" and s[1] and s[1].startswith("2026-06-15")
    ]
    assert len(cancelled_15th) == 1, (
        f"expected exactly one cancelled instance on 2026-06-15; got {statuses}"
    )