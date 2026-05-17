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

    # A configured instance has a real encryption key and an org row
    # whose credentials are encrypted with it — the lifespan decrypts
    # those at startup, so the fixture must be internally consistent.
    from app.config import get_settings
    from app.encryption import init_encryption_manager
    key_file = get_settings().encryption_key_file
    if os.path.dirname(key_file):
        os.makedirs(os.path.dirname(key_file), exist_ok=True)
    with open(key_file, "wb") as f:
        f.write(b"0" * 32)
    enc = init_encryption_manager(b"0" * 32)

    # Seed an admin user + 2 client calendars.
    await db.execute(
        """INSERT OR IGNORE INTO organization
              (id, google_workspace_domain, google_client_id_encrypted,
               google_client_secret_encrypted)
           VALUES (1, ?, ?, ?)""",
        ("example.com", enc.encrypt("test-client-id"),
         enc.encrypt("test-client-secret")),
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

    try:
        with TestClient(main_mod.app) as client:
            client.cookies.set(
                "session",
                create_session_token(
                    user_id=user_id, email="alice@example.com", is_admin=True,
                ),
            )
            yield client, user_id
    finally:
        for path in (key_file, os.path.join(
            os.path.dirname(key_file) or ".", "session_secret",
        )):
            try:
                os.remove(path)
            except FileNotFoundError:
                pass


def test_security_headers_and_vendored_assets(app_with_fake_google):
    """Every response carries defensive headers, and front-end assets
    are served locally rather than from a third-party CDN."""
    client, _user_id = app_with_fake_google

    r = client.get("/health")
    assert r.headers["X-Content-Type-Options"] == "nosniff"
    assert r.headers["X-Frame-Options"] == "DENY"
    assert r.headers["Referrer-Policy"] == "no-referrer"
    csp = r.headers["Content-Security-Policy"]
    assert "default-src 'self'" in csp
    assert "frame-ancestors 'none'" in csp
    # The policy must not whitelist any third-party CDN origin.
    assert "unpkg.com" not in csp
    assert "cdnjs" not in csp
    assert "tailwindcss.com" not in csp

    # The vendored assets are actually served from /static.
    for path in (
        "/static/vendor/htmx.min.js",
        "/static/vendor/htmx-ext-json-enc.js",
        "/static/vendor/alpine.min.js",
        "/static/vendor/tailwind.js",
        "/static/vendor/fontawesome/css/all.min.css",
        "/static/vendor/fontawesome/webfonts/fa-solid-900.woff2",
    ):
        rr = client.get(path)
        assert rr.status_code == 200, path


def test_cross_site_post_is_blocked(app_with_fake_google):
    """A state-changing request carrying a foreign Origin header is
    refused by the CSRF origin check before it reaches the route."""
    client, user_id = app_with_fake_google
    r = client.post(
        f"/api/admin/ledger/users/{user_id}/reconcile-now",
        headers={"Origin": "https://evil.example.com"},
    )
    assert r.status_code == 403
    assert "Cross-site" in r.json()["detail"]


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

def test_dry_run_previews_without_writing(app_with_fake_google):
    """POST /dry-run ingests + plans + diffs but drains nothing.
    The pending-operations preview lists what WOULD be written;
    the main calendar stays empty."""
    client, user_id = app_with_fake_google

    # Plant a source event.
    r = client.post(
        "/_fake/calendars/client_a@cal.test/events",
        json={
            "summary": "Dry run candidate",
            "start": {"dateTime": "2026-07-01T09:00:00Z", "timeZone": "UTC"},
            "end": {"dateTime": "2026-07-01T09:30:00Z", "timeZone": "UTC"},
        },
    )
    assert r.status_code == 200, r.text

    # Dry-run.
    r = client.post(f"/api/admin/ledger/users/{user_id}/dry-run")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "done"
    # The system intends to write: 1 main copy + 1 busy block on client_b.
    assert body["pending_count"] == 2, body
    ops = {p["operation"] for p in body["pending_operations"]}
    assert ops == {"create"}
    summaries = {p["would_send_summary"] for p in body["pending_operations"]}
    assert "Dry run candidate" in summaries  # the main copy
    assert "Busy" in summaries               # the busy block

    # Crucially: nothing was actually written to the main calendar.
    main = client.get("/_fake/calendars/alice@example.com/events").json()["items"]
    assert main == [], f"dry-run wrote to Google: {main}"
    busy = client.get("/_fake/calendars/client_b@cal.test/events").json()["items"]
    assert busy == [], f"dry-run wrote to Google: {busy}"


def test_verify_reports_consistency_after_reconcile(app_with_fake_google):
    """After a real reconcile, /verify reports zero divergences —
    the ledger's model matches what's on the fake Google."""
    client, user_id = app_with_fake_google

    client.post(
        "/_fake/calendars/client_a@cal.test/events",
        json={
            "summary": "Verify me",
            "start": {"dateTime": "2026-07-02T09:00:00Z", "timeZone": "UTC"},
            "end": {"dateTime": "2026-07-02T09:30:00Z", "timeZone": "UTC"},
        },
    )
    client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")

    r = client.get(f"/api/admin/ledger/users/{user_id}/verify")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "done"
    assert body["consistent"] is True, body["divergences"]
    assert body["checked"] >= 2  # main copy + busy block
    assert body["ok"] == body["checked"]


def test_verify_detects_divergence(app_with_fake_google):
    """If something deletes a ledger-tracked event behind the
    system's back, /verify catches it."""
    client, user_id = app_with_fake_google

    client.post(
        "/_fake/calendars/client_a@cal.test/events",
        json={
            "summary": "Will be tampered",
            "start": {"dateTime": "2026-07-03T09:00:00Z", "timeZone": "UTC"},
            "end": {"dateTime": "2026-07-03T09:30:00Z", "timeZone": "UTC"},
        },
    )
    client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")

    # Sabotage: delete the main copy directly on Google.
    main = client.get("/_fake/calendars/alice@example.com/events").json()["items"]
    victim = next(e for e in main if e["summary"] == "Will be tampered")
    client.delete(f"/_fake/calendars/alice@example.com/events/{victim['id']}")

    r = client.get(f"/api/admin/ledger/users/{user_id}/verify")
    assert r.status_code == 200
    body = r.json()
    assert body["consistent"] is False
    # The fake's delete marks status=cancelled (doesn't drop the row),
    # so the divergence surfaces as "is CANCELLED"; a real hard-delete
    # would surface as "MISSING".  Either is a caught divergence.
    assert any(
        ("MISSING" in d or "CANCELLED" in d) for d in body["divergences"]
    ), body["divergences"]


def test_ledger_dry_run_env_var_blocks_all_writes(app_with_fake_google, monkeypatch):
    """With LEDGER_DRY_RUN=1, even a normal /reconcile-now ingests +
    plans + diffs but never drains — nothing reaches Google.  This
    is the Stage-4 shadow-mode deployment switch."""
    client, user_id = app_with_fake_google

    # Flip the dry-run flag and bust the settings cache.
    monkeypatch.setenv("LEDGER_DRY_RUN", "1")
    from app.config import get_settings
    get_settings.cache_clear()
    assert get_settings().ledger_dry_run is True

    try:
        client.post(
            "/_fake/calendars/client_a@cal.test/events",
            json={
                "summary": "Shadow-mode event",
                "start": {"dateTime": "2026-07-04T09:00:00Z", "timeZone": "UTC"},
                "end": {"dateTime": "2026-07-04T09:30:00Z", "timeZone": "UTC"},
            },
        )
        r = client.post(f"/api/admin/ledger/users/{user_id}/reconcile-now")
        assert r.status_code == 200, r.text
        result = r.json()["result"]
        # Ingest + plan + diff still ran...
        assert result["planned"] >= 1
        assert result["enqueued"] >= 1
        # ...but the drain was a no-op.
        assert result["drain"].get("processed", 0) == 0

        # Nothing on Google.
        main = client.get("/_fake/calendars/alice@example.com/events").json()["items"]
        assert main == [], f"dry-run mode wrote to Google: {main}"

        # The pending outbox is the preview.
        h = client.get(f"/api/admin/ledger/health/{user_id}").json()
        assert h["outbox"].get("pending", 0) >= 1
    finally:
        monkeypatch.delenv("LEDGER_DRY_RUN", raising=False)
        get_settings.cache_clear()
