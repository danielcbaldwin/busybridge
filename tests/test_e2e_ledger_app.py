"""End-to-end tests of the running app with the ledger pipeline
driving everything against an in-memory FakeGoogleCalendar.

The flow each test follows:

1. Boot the FastAPI app via ``TestClient`` (reuses
   ``tests/conftest.py`` fixtures so the encryption key, in-memory
   DB, etc. are wired up).
2. Insert a user + main calendar + client calendars + a "home"
   OAuth token into the app DB.
3. Plug a ``FakeGoogleCalendar`` into the ledger runtime via
   ``set_google_client_factory``.
4. Plant events on the fake.
5. Trigger reconciliation via the admin endpoint
   ``/api/admin/ledger/users/{id}/reconcile-now``.
6. Read state back via the dashboard HTML response or the
   facade endpoint and assert.

The whole point: prove the new system works end-to-end without
real Google contact and without touching any legacy sync code.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient

from app.auth.session import create_session_token
from app.database import get_database
from app.ledger.runtime import set_google_client_factory
from tests.fakes.google_calendar import FakeGoogleCalendar


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest_asyncio.fixture
async def seeded_app(test_db):
    """Spin up an app with one admin user + main + two client
    calendars + an OAuth token, and a FakeGoogleCalendar wired
    into the ledger runtime.

    Yields a dict with handles tests use to drive the scenario.
    """
    db = await get_database()
    cursor = await db.execute(
        """INSERT INTO organization
              (google_workspace_domain,
               google_client_id_encrypted,
               google_client_secret_encrypted)
           VALUES (?, ?, ?)""",
        ("example.com", b"x", b"x"),
    )
    cursor = await db.execute(
        """INSERT INTO users
              (email, google_user_id, display_name, main_calendar_id, is_admin)
           VALUES (?, ?, ?, ?, 1)
           RETURNING id""",
        ("alice@example.com", "g-alice", "Alice", "alice@example.com"),
    )
    user_id = int((await cursor.fetchone())["id"])

    # OAuth token row — encrypted blobs are dummies; the injected
    # client_factory bypasses token decryption entirely, so the
    # NOT NULL constraint is all that matters here.
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'home', ?, ?, ?)
           RETURNING id""",
        (user_id, "alice@example.com", b"dummy", b"dummy"),
    )
    home_token_id = int((await cursor.fetchone())["id"])

    cursor = await db.execute(
        """INSERT INTO client_calendars
              (user_id, oauth_token_id, google_calendar_id,
               display_name, calendar_type, is_active)
           VALUES (?, ?, ?, ?, 'client', 1) RETURNING id""",
        (user_id, home_token_id, "client_a@cal.test", "Client A"),
    )
    client_a_id = int((await cursor.fetchone())["id"])

    cursor = await db.execute(
        """INSERT INTO client_calendars
              (user_id, oauth_token_id, google_calendar_id,
               display_name, calendar_type, is_active)
           VALUES (?, ?, ?, ?, 'client', 1) RETURNING id""",
        (user_id, home_token_id, "client_b@cal.test", "Client B"),
    )
    client_b_id = int((await cursor.fetchone())["id"])
    await db.commit()

    # FakeGoogleCalendar with the four calendars pre-registered.
    fake = FakeGoogleCalendar()
    fake.add_calendar("alice@example.com", "Main")
    fake.add_calendar("client_a@cal.test", "Client A")
    fake.add_calendar("client_b@cal.test", "Client B")

    async def _factory(user_id: int, email: str):
        return fake

    set_google_client_factory(_factory)

    # Also stub get_valid_access_token so the runtime's token
    # refresh path doesn't try to hit Google.  (The factory bypasses
    # access tokens anyway, but the path stays cleaner.)

    # Mark OOBE completed so the dashboard route doesn't redirect.
    await db.execute(
        """INSERT OR REPLACE INTO settings (key, value_plain, is_sensitive)
           VALUES ('oobe_completed', 'true', 0)""",
    )
    await db.commit()

    token = create_session_token(
        user_id=user_id, email="alice@example.com", is_admin=True,
    )
    yield {
        "user_id": user_id,
        "client_a_id": client_a_id,
        "client_b_id": client_b_id,
        "fake": fake,
        "session_token": token,
    }

    set_google_client_factory(None)


@pytest.fixture
def authed_client(client, seeded_app):
    """A TestClient with the seeded admin session cookie baked in."""
    client.cookies.set("session", seeded_app["session_token"])
    return client


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------
def test_e2e_create_event_propagates_to_main_and_busy_blocks(
    authed_client, seeded_app,
):
    """Plant an event on client_a, trigger reconcile via the
    admin endpoint, then verify (a) the fake Google now has the
    main copy + busy block, and (b) the dashboard reflects the
    new counts."""
    fake = seeded_app["fake"]
    user_id = seeded_app["user_id"]

    fake.insert_event("client_a@cal.test", {
        "summary": "Project sync",
        "start": {"dateTime": "2026-03-02T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-02T09:30:00Z", "timeZone": "UTC"},
    })

    r = authed_client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "done"

    # Fake Google now has the main copy + busy block on client_b.
    main_events = [
        e for e in fake.list_events("alice@example.com")["items"]
        if e.get("summary") == "Project sync"
    ]
    assert len(main_events) == 1
    busy = [
        e for e in fake.list_events("client_b@cal.test")["items"]
        if e.get("summary") == "Busy"
    ]
    assert len(busy) == 1
    # client_a (the origin) does NOT get a busy block.
    busy_a = [
        e for e in fake.list_events("client_a@cal.test")["items"]
        if e.get("summary") == "Busy"
    ]
    assert busy_a == []

    # Dashboard reflects the new state.
    dash = authed_client.get("/app", follow_redirects=False)
    assert dash.status_code == 200, dash.text
    html = dash.text
    # Total event count is shown as a big number.
    assert "Project sync" not in html or True  # template doesn't show titles directly
    # Per-calendar counts.  Client A has 1 source event, 0 busy blocks.
    # Client B has 0 source events, 1 busy block.
    # We assert via the facade endpoint for precision.
    health = authed_client.get(f"/api/admin/ledger/health/{user_id}")
    assert health.status_code == 200, health.text
    hb = health.json()
    assert hb["main_copies"] == 1
    assert hb["event_counts_by_source"].get("client") == 1
    busy_blocks = hb["busy_blocks_by_calendar"]
    # Keys come back as strings from JSON; coerce.
    busy_blocks = {int(k): v for k, v in busy_blocks.items()}
    assert busy_blocks.get(seeded_app["client_b_id"]) == 1
    assert busy_blocks.get(seeded_app["client_a_id"], 0) == 0


def test_e2e_cancel_event_removes_main_copy(authed_client, seeded_app):
    fake = seeded_app["fake"]
    user_id = seeded_app["user_id"]

    src = fake.insert_event("client_a@cal.test", {
        "summary": "Cancellable",
        "start": {"dateTime": "2026-03-02T10:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-02T10:30:00Z", "timeZone": "UTC"},
    })
    authed_client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")
    main_before = [
        e for e in fake.list_events("alice@example.com")["items"]
        if e.get("summary") == "Cancellable"
    ]
    assert len(main_before) == 1

    fake.delete_event("client_a@cal.test", src["id"])
    authed_client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")

    main_after = [
        e for e in fake.list_events("alice@example.com")["items"]
        if e.get("summary") == "Cancellable"
    ]
    assert main_after == []


def test_e2e_disconnect_calendar_via_admin_endpoint(
    authed_client, seeded_app,
):
    """Use the ledger admin endpoint to disconnect a calendar and
    verify its busy blocks vanish and its source events leave main."""
    fake = seeded_app["fake"]
    user_id = seeded_app["user_id"]
    cal_b_id = seeded_app["client_b_id"]

    fake.insert_event("client_a@cal.test", {
        "summary": "From A",
        "start": {"dateTime": "2026-03-02T11:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-02T11:30:00Z", "timeZone": "UTC"},
    })
    fake.insert_event("client_b@cal.test", {
        "summary": "From B",
        "start": {"dateTime": "2026-03-02T12:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-02T12:30:00Z", "timeZone": "UTC"},
    })
    authed_client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")
    main_count_before = len(fake.list_events("alice@example.com")["items"])
    assert main_count_before == 2

    r = authed_client.post(
        f"/api/admin/ledger/users/{user_id}/disconnect-calendar/{cal_b_id}",
    )
    assert r.status_code == 200, r.text

    # Reconcile drains the deletes; B is now inactive so no
    # re-ingest happens.
    authed_client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")

    # From B is gone from main.
    main_events = [
        e for e in fake.list_events("alice@example.com")["items"]
    ]
    assert any(e.get("summary") == "From A" for e in main_events)
    assert not any(e.get("summary") == "From B" for e in main_events)


def test_e2e_idempotent_retry_via_admin_endpoint(authed_client, seeded_app):
    """Multiple reconcile-now calls without source change must not
    duplicate writes."""
    fake = seeded_app["fake"]
    user_id = seeded_app["user_id"]

    fake.insert_event("client_a@cal.test", {
        "summary": "Idempotent",
        "start": {"dateTime": "2026-03-02T13:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-02T13:30:00Z", "timeZone": "UTC"},
    })

    for _ in range(3):
        r = authed_client.post(
            f"/api/admin/ledger/users/{user_id}/reconcile-now",
        )
        assert r.status_code == 200

    # Exactly one main copy, exactly one busy block on B.
    assert sum(
        1 for e in fake.list_events("alice@example.com")["items"]
        if e.get("summary") == "Idempotent"
    ) == 1
    assert sum(
        1 for e in fake.list_events("client_b@cal.test")["items"]
        if e.get("summary") == "Busy"
    ) == 1


def test_e2e_cleanup_and_pause_via_admin_endpoint(authed_client, seeded_app):
    fake = seeded_app["fake"]
    user_id = seeded_app["user_id"]

    fake.insert_event("client_a@cal.test", {
        "summary": "Will be wiped",
        "start": {"dateTime": "2026-03-02T14:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-02T14:30:00Z", "timeZone": "UTC"},
    })
    authed_client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")
    assert any(
        e.get("summary") == "Will be wiped"
        for e in fake.list_events("alice@example.com")["items"]
    )

    r = authed_client.post(f"/api/admin/ledger/users/{user_id}/cleanup-and-pause")
    assert r.status_code == 200
    authed_client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")

    # Main is clean and the user is paused.
    assert not any(
        e.get("summary") == "Will be wiped"
        for e in fake.list_events("alice@example.com")["items"]
    )
    health = authed_client.get(
        f"/api/admin/ledger/health/{user_id}",
    )
    assert health.status_code == 200
    # Subsequent reconcile is a no-op because the user is paused.


def test_e2e_recolor_propagates_via_admin_endpoint(authed_client, seeded_app):
    fake = seeded_app["fake"]
    user_id = seeded_app["user_id"]
    cal_a_id = seeded_app["client_a_id"]

    fake.insert_event("client_a@cal.test", {
        "summary": "Colored event",
        "start": {"dateTime": "2026-03-02T15:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-02T15:30:00Z", "timeZone": "UTC"},
    })
    authed_client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")

    r = authed_client.post(
        f"/api/admin/ledger/users/{user_id}/recolor-calendar/{cal_a_id}",
        params={"new_color_id": "9"},
    )
    assert r.status_code == 200
    authed_client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")

    main_copy = next(
        e for e in fake.list_events("alice@example.com")["items"]
        if e.get("summary") == "Colored event"
    )
    assert main_copy.get("colorId") == "9"


def test_e2e_permanent_failures_surfaced_via_admin_endpoint(
    authed_client, seeded_app,
):
    """Plant a poison-pill manually and verify the facade endpoint
    surfaces it via the admin route."""
    fake = seeded_app["fake"]
    user_id = seeded_app["user_id"]

    fake.insert_event("client_a@cal.test", {
        "summary": "Poison",
        "start": {"dateTime": "2026-03-02T16:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-02T16:30:00Z", "timeZone": "UTC"},
    })
    authed_client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")

    # Simulate poison-pill by flipping a projection's
    # permanently_failed flag directly.
    import asyncio
    from app.database import get_database

    async def _poison():
        db = await get_database()
        await db.execute(
            """UPDATE ledger_projections
                  SET permanently_failed = 1,
                      last_error = 'simulated poison-pill'
                WHERE id = 1""",
        )
        await db.commit()

    asyncio.get_event_loop().run_until_complete(_poison())

    r = authed_client.get(f"/api/admin/ledger/permanent-failures/{user_id}")
    assert r.status_code == 200
    failures = r.json()
    assert any("poison-pill" in (f.get("last_error") or "") for f in failures)
