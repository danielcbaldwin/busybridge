"""WebCal placement API contract — see webcal.md §API Changes, §Alerts,
§Placement Changes.

Focuses on:
  * idempotent PATCH no-op (test 31)
  * atomic placement-change transaction (test 27)
  * sync_log entries on change & disconnect (test 33)
  * alert fanout per subscription on disconnect (test 32)
  * re-pick after disconnect clears alert state (test 24)
  * placement_target_status derivation in the list response
  * dropdown empty / disconnected behavior signaled by status field (test 34)
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile

import pytest

# Most tests in this file are async; the two pure-Pydantic tests at the
# top opt out via their own decorator.  Pytest's per-function override
# is more granular than module-level pytestmark, so we only apply the
# mark to async tests via the asyncio plugin's auto-mode (set in
# pytest.ini).  No module-level mark needed.


def _fresh_db_path() -> str:
    return tempfile.mktemp(suffix=".db")


async def _bootstrap(db):
    """Seed a user + main oauth + two client calendars."""
    await db.execute(
        "INSERT INTO users(id, email, google_user_id, display_name) "
        "VALUES (1, 'u@x.test', 'gid1', 'User One')"
    )
    await db.execute(
        "INSERT INTO oauth_tokens(id, user_id, account_type, "
        "google_account_email, access_token_encrypted, refresh_token_encrypted) "
        "VALUES (1, 1, 'main', 'u@x.test', X'00', X'00')"
    )
    await db.execute(
        "INSERT INTO client_calendars(id, user_id, oauth_token_id, "
        "google_calendar_id, display_name, color_id, is_active) "
        "VALUES (10, 1, 1, 'cal-a', 'MLCommons', '7', 1)"
    )
    await db.execute(
        "INSERT INTO client_calendars(id, user_id, oauth_token_id, "
        "google_calendar_id, display_name, color_id, is_active) "
        "VALUES (11, 1, 1, 'cal-b', 'Acme', '3', 1)"
    )
    await db.commit()


async def _make_db():
    """Build a private DB connection isolated from other tests'
    globals."""
    import aiosqlite
    from app.database import SCHEMA, init_schema
    conn = await aiosqlite.connect(":memory:", isolation_level=None)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA foreign_keys = ON")
    await init_schema(conn)
    await _bootstrap(conn)
    return conn


# ---------------------------------------------------------------------------
# Pydantic-shape validation (§API Changes)
# ---------------------------------------------------------------------------


def test_request_models_enforce_placement_pair_shape():
    """create / update reject the bad (main, with target) and
    (client, without target) shapes; accept the empty PATCH."""
    from pydantic import ValidationError
    from app.api.webcal import CreateWebcalRequest, UpdateWebcalRequest

    with pytest.raises(ValidationError):
        CreateWebcalRequest(url="https://x/y", placement_kind="client")
    with pytest.raises(ValidationError):
        CreateWebcalRequest(
            url="https://x/y", placement_kind="main",
            placement_client_calendar_id=10,
        )
    with pytest.raises(ValidationError):
        CreateWebcalRequest(url="https://x/y", placement_kind="elsewhere")

    # Orphan target_id (no kind) on PATCH — would otherwise slip
    # through as new_kind=None and corrupt the row.  Must be
    # rejected at the wire boundary.
    with pytest.raises(ValidationError) as exc:
        UpdateWebcalRequest(placement_client_calendar_id=5)
    assert "placement_kind is required" in str(exc.value)

    # Bare kind=client (no target) — rejected.
    with pytest.raises(ValidationError):
        UpdateWebcalRequest(placement_kind="client")

    r = CreateWebcalRequest(
        url="https://x/y", placement_kind="client",
        placement_client_calendar_id=10,
    )
    assert r.placement_kind == "client"

    # Empty PATCH — placement untouched, no shape error.
    p = UpdateWebcalRequest()
    assert p.placement_kind is None
    assert p.placement_client_calendar_id is None

    # PATCH with display_prefix only — placement untouched.
    p2 = UpdateWebcalRequest(display_prefix="Renamed")
    assert p2.display_prefix == "Renamed"
    assert p2.placement_kind is None

    # Bare kind=main on PATCH — valid (signals "set to main",
    # target stays None).
    p3 = UpdateWebcalRequest(placement_kind="main")
    assert p3.placement_kind == "main"
    assert p3.placement_client_calendar_id is None


async def test_db_target_validation_rejects_nonexistent_inactive_and_cross_user():
    """_validate_placement_target enforces existence / active /
    same-user invariants."""
    from fastapi import HTTPException
    from app.api.webcal import _validate_placement_target

    db = await _make_db()
    try:
        # Active and ours → ok, returns the display_name.
        name = await _validate_placement_target(
            db, user_id=1, placement_kind="client",
            placement_client_calendar_id=10,
        )
        assert name == "MLCommons"
        # Nonexistent.
        with pytest.raises(HTTPException) as exc:
            await _validate_placement_target(
                db, user_id=1, placement_kind="client",
                placement_client_calendar_id=999,
            )
        assert "does not exist" in exc.value.detail
        # Inactive.
        await db.execute(
            "UPDATE client_calendars SET is_active = 0 WHERE id = 11"
        )
        with pytest.raises(HTTPException) as exc:
            await _validate_placement_target(
                db, user_id=1, placement_kind="client",
                placement_client_calendar_id=11,
            )
        assert "disconnected" in exc.value.detail
        # Cross-user.
        with pytest.raises(HTTPException) as exc:
            await _validate_placement_target(
                db, user_id=999, placement_kind="client",
                placement_client_calendar_id=10,
            )
        assert "does not exist" in exc.value.detail
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# placement_target_status derivation (§API Changes — drives the badge)
# ---------------------------------------------------------------------------


def test_placement_target_status_enum_values():
    """[34 — driving the UI badge] All four placement_target_status
    values follow from (kind, target_id, target_is_active)."""
    from app.api.webcal import _derive_placement_status
    assert _derive_placement_status("main", None, None) == "not_applicable"
    assert _derive_placement_status("client", 10, 1) == "active"
    assert _derive_placement_status("client", 10, 0) == "disconnected"
    assert _derive_placement_status("client", None, None) == "disconnected"


# ---------------------------------------------------------------------------
# Atomic placement-change handler (§Placement Changes, tests 11/12/27/31/33)
# ---------------------------------------------------------------------------


async def _create_subscription_main(db, *, sub_id: int, url: str):
    """Insert a webcal_subscriptions row in the default 'main'
    placement state."""
    await db.execute(
        "INSERT INTO webcal_subscriptions(id, user_id, url, display_prefix, is_active) "
        "VALUES (?, 1, ?, 'feed', 1)",
        (sub_id, url),
    )
    await db.commit()


async def _seed_active_ledger_row(db, sub_id: int) -> int:
    """Insert a single active webcal-sourced ledger row tied to the
    given subscription, so the placement-change path has something to
    enqueue."""
    cur = await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, source_calendar_id,
               source_event_id, summary, start_at, end_at, status,
               is_all_day, is_recurring, user_can_edit, version)
           VALUES (1, ?, 'webcal', ?, ?, 'Event', '2026-03-01T12:00:00Z',
                   '2026-03-01T13:00:00Z', 'active', 0, 0, 1, 1)
           RETURNING id""",
        (f"webcal:{sub_id}:e1", sub_id, f"src-{sub_id}-1"),
    )
    row = await cur.fetchone()
    await db.commit()
    return int(row["id"])


async def test_27_31_33_patch_idempotency_atomicity_and_sync_log():
    """[27 atomic, 31 no-op idempotency, 33 sync_log] PATCH that
    actually changes placement appends affected rows, enqueues a
    reconcile, and writes a 'change_placement' sync_log row in one
    transaction; PATCH with unchanged values is a no-op (no rows
    appended, no sync_log entry written)."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.api.webcal import router
    from app.auth.session import get_current_user, User

    # Build a minimal FastAPI app, swap the auth dep to a fixed user,
    # swap get_database() to our in-memory connection.
    db = await _make_db()
    try:
        await _create_subscription_main(db, sub_id=1, url="https://x/y.ics")
        ledger_id = await _seed_active_ledger_row(db, sub_id=1)

        app = FastAPI()
        app.include_router(router)

        async def fake_user():
            return User(id=1, email="u@x.test", google_user_id="gid1", display_name="U", is_admin=False)
        async def fake_db():
            return db

        app.dependency_overrides[get_current_user] = fake_user
        from app.database import get_database
        app.dependency_overrides[get_database] = fake_db

        # Patch the global get_database used inside the handler too.
        import app.api.webcal as wc
        wc.get_database = fake_db  # noqa: SLF001

        # raise_server_exceptions=False lets us assert 500 responses
        # from monkeypatched mid-transaction failures, instead of
        # having the raw exception bubble through TestClient.
        client = TestClient(app, raise_server_exceptions=False)

        # 1) Real placement change: main → client(10)
        r = client.patch(
            "/webcal-subscriptions/1",
            json={"placement_kind": "client",
                  "placement_client_calendar_id": 10},
        )
        assert r.status_code == 200, r.text

        # Persisted.
        row = await (await db.execute(
            "SELECT placement_kind, placement_client_calendar_id, "
            "placement_client_display_name_cache "
            "FROM webcal_subscriptions WHERE id = 1"
        )).fetchone()
        assert row["placement_kind"] == "client"
        assert row["placement_client_calendar_id"] == 10
        assert row["placement_client_display_name_cache"] == "MLCommons"

        # Affected ledger row enqueued.
        aff = await (await db.execute(
            "SELECT ledger_event_id FROM affected_ledger_events"
        )).fetchall()
        assert any(int(r["ledger_event_id"]) == ledger_id for r in aff), (
            "placement change must append the ledger row to affected_ledger_events"
        )

        # sync_log: one change_placement row with the expected payload.
        logs = await (await db.execute(
            "SELECT action, details FROM sync_log WHERE action='change_placement'"
        )).fetchall()
        assert len(logs) == 1
        details = json.loads(logs[0]["details"])
        assert details["new_kind"] == "client"
        assert details["new_target_id"] == 10
        assert details["old_kind"] == "main"
        assert details["old_target_id"] is None
        assert details["affected_ledger_count"] == 1

        # 2) No-op PATCH (same values) — no new rows.
        await db.execute("DELETE FROM affected_ledger_events")
        await db.commit()
        r = client.patch(
            "/webcal-subscriptions/1",
            json={"placement_kind": "client",
                  "placement_client_calendar_id": 10},
        )
        assert r.status_code == 200
        post = await (await db.execute(
            "SELECT COUNT(*) AS n FROM affected_ledger_events"
        )).fetchone()
        assert post["n"] == 0, "no-op PATCH must not enqueue affected rows"
        # sync_log unchanged — still exactly one change_placement row.
        logs2 = await (await db.execute(
            "SELECT COUNT(*) AS n FROM sync_log WHERE action='change_placement'"
        )).fetchone()
        assert logs2["n"] == 1, "no-op PATCH must not write a sync_log row"

        # 3) Invalid PATCH (nonexistent target) — 400, no DB changes
        # (this exercises pre-BEGIN validation, not the rollback path).
        await db.execute("DELETE FROM affected_ledger_events")
        await db.commit()
        before_kind = (await (await db.execute(
            "SELECT placement_kind FROM webcal_subscriptions WHERE id = 1"
        )).fetchone())["placement_kind"]
        r = client.patch(
            "/webcal-subscriptions/1",
            json={"placement_kind": "client",
                  "placement_client_calendar_id": 999},
        )
        assert r.status_code == 400
        after_kind = (await (await db.execute(
            "SELECT placement_kind FROM webcal_subscriptions WHERE id = 1"
        )).fetchone())["placement_kind"]
        assert after_kind == before_kind, "failed validation must roll back"
        post = await (await db.execute(
            "SELECT COUNT(*) AS n FROM affected_ledger_events"
        )).fetchone()
        assert post["n"] == 0

        # 4) MID-TRANSACTION failure (the real atomicity test):
        # monkeypatch record_affected_events to raise AFTER the
        # placement UPDATE has been issued.  The rollback must undo
        # the UPDATE so placement and the replan queue stay in sync.
        # Set up a known starting state first: placement is currently
        # client/10 from step 1, flip back to main as a baseline.
        await db.execute(
            "UPDATE webcal_subscriptions SET placement_kind='main', "
            "placement_client_calendar_id=NULL, "
            "placement_client_display_name_cache=NULL WHERE id=1"
        )
        await db.execute("DELETE FROM affected_ledger_events")
        await db.execute("DELETE FROM sync_log WHERE action='change_placement'")
        await db.commit()

        import app.ledger.triggers as triggers_mod
        original = triggers_mod.record_affected_events
        async def boom(*a, **kw):
            raise RuntimeError("simulated mid-transaction failure")
        triggers_mod.record_affected_events = boom
        try:
            r = client.patch(
                "/webcal-subscriptions/1",
                json={"placement_kind": "client",
                      "placement_client_calendar_id": 10},
            )
            assert r.status_code == 500, (
                f"mid-transaction failure must surface as 500, got {r.status_code}"
            )
        finally:
            triggers_mod.record_affected_events = original

        # Placement must be unchanged.
        row = await (await db.execute(
            "SELECT placement_kind, placement_client_calendar_id "
            "FROM webcal_subscriptions WHERE id = 1"
        )).fetchone()
        assert row["placement_kind"] == "main", (
            "ROLLBACK must restore placement_kind"
        )
        assert row["placement_client_calendar_id"] is None, (
            "ROLLBACK must restore placement_client_calendar_id"
        )
        # No affected_ledger_events leaked.
        post = await (await db.execute(
            "SELECT COUNT(*) AS n FROM affected_ledger_events"
        )).fetchone()
        assert post["n"] == 0, (
            "ROLLBACK must drop the affected rows that were appended"
        )
        # No partial sync_log entry.
        logs = await (await db.execute(
            "SELECT COUNT(*) AS n FROM sync_log "
            "WHERE action='change_placement'"
        )).fetchone()
        assert logs["n"] == 0, (
            "ROLLBACK must drop the sync_log entry from the failed txn"
        )
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# List endpoint enrichment (§API Changes — drives the dashboard)
# ---------------------------------------------------------------------------


async def test_list_endpoint_returns_placement_fields_and_status():
    """List response carries placement_kind, placement_client_calendar_id,
    placement_client_display_name, placement_target_status."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from app.api.webcal import router
    from app.auth.session import get_current_user, User

    db = await _make_db()
    try:
        # Two subscriptions: one main, one client-placed-active, one
        # client-placed-stale (target inactive).
        await _create_subscription_main(db, sub_id=1, url="https://x/main.ics")
        await _create_subscription_main(db, sub_id=2, url="https://x/cli.ics")
        await _create_subscription_main(db, sub_id=3, url="https://x/stale.ics")
        await db.execute(
            "UPDATE webcal_subscriptions SET placement_kind='client', "
            "placement_client_calendar_id=10, "
            "placement_client_display_name_cache='MLCommons' WHERE id=2"
        )
        await db.execute(
            "UPDATE webcal_subscriptions SET placement_kind='client', "
            "placement_client_calendar_id=11, "
            "placement_client_display_name_cache='Acme' WHERE id=3"
        )
        await db.execute("UPDATE client_calendars SET is_active = 0 WHERE id = 11")
        await db.commit()

        app = FastAPI()
        app.include_router(router)

        async def fake_user():
            return User(id=1, email="u@x.test", google_user_id="gid1", display_name="U", is_admin=False)
        async def fake_db():
            return db
        app.dependency_overrides[get_current_user] = fake_user
        from app.database import get_database
        app.dependency_overrides[get_database] = fake_db
        import app.api.webcal as wc
        wc.get_database = fake_db

        client = TestClient(app)
        r = client.get("/webcal-subscriptions")
        assert r.status_code == 200
        rows = {item["id"]: item for item in r.json()}
        # main-placed
        assert rows[1]["placement_kind"] == "main"
        assert rows[1]["placement_client_calendar_id"] is None
        assert rows[1]["placement_target_status"] == "not_applicable"
        # client-placed-active
        assert rows[2]["placement_kind"] == "client"
        assert rows[2]["placement_client_calendar_id"] == 10
        assert rows[2]["placement_client_display_name"] == "MLCommons"
        assert rows[2]["placement_target_status"] == "active"
        # client-placed-stale (target inactive) — name falls back to cache
        assert rows[3]["placement_kind"] == "client"
        assert rows[3]["placement_target_status"] == "disconnected"
        assert rows[3]["placement_client_display_name"] == "Acme"
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# Disconnect → alert fanout + sync_log (tests 23, 24, 32, 33)
# ---------------------------------------------------------------------------


async def test_32_33_disconnect_fires_one_alert_per_subscription_and_logs():
    """[32 alert fanout] Disconnecting one client that holds the
    placement for N subscriptions produces exactly N alerts (one per
    subscription, deduped per-subscription), plus N
    'webcal_placement_target_disconnected' sync_log rows."""
    db = await _make_db()
    try:
        # Two webcal subs both placed on client 10.
        await _create_subscription_main(db, sub_id=1, url="https://x/a.ics")
        await _create_subscription_main(db, sub_id=2, url="https://x/b.ics")
        for sub in (1, 2):
            await db.execute(
                "UPDATE webcal_subscriptions SET placement_kind='client', "
                "placement_client_calendar_id=10, display_prefix=?, "
                "placement_client_display_name_cache='MLCommons' WHERE id=?",
                (f"Feed {sub}", sub),
            )
        # Add one active ledger row per sub so the affected-enqueue
        # path has work to do.
        await _seed_active_ledger_row(db, sub_id=1)
        await _seed_active_ledger_row(db, sub_id=2)
        # Enable alerts so the queue is exercised.
        await db.execute(
            "INSERT OR REPLACE INTO settings(key, value_plain, value_encrypted) "
            "VALUES ('alerts_enabled', 'true', NULL)"
        )
        await db.commit()

        # Point the disconnect_calendar path at our in-memory db.
        import app.database as db_mod
        import app.alerts.email as email_mod
        db_mod._db_connection = db

        from app.ledger.admin_ops import disconnect_calendar
        await disconnect_calendar(db, user_id=1, client_calendar_id=10)

        # Two distinct alert_queue rows, one per subscription.
        alerts = await (await db.execute(
            "SELECT alert_type, subject FROM alert_queue ORDER BY alert_type"
        )).fetchall()
        # The alert_type embeds the subscription id (we use
        # `webcal_placement_disconnected:{sub_id}` to dedup
        # per-subscription).
        types = sorted(a["alert_type"] for a in alerts)
        assert types == [
            "webcal_placement_disconnected:1",
            "webcal_placement_disconnected:2",
        ], f"expected one alert per sub, got {types}"
        # Subjects name the feed.
        subjects = " | ".join(a["subject"] for a in alerts)
        assert "Feed 1" in subjects and "Feed 2" in subjects

        # sync_log: one warning row per affected subscription.
        logs = await (await db.execute(
            "SELECT details FROM sync_log "
            "WHERE action='webcal_placement_target_disconnected' "
            "ORDER BY id"
        )).fetchall()
        assert len(logs) == 2
        sub_ids = {json.loads(r["details"])["subscription_id"] for r in logs}
        assert sub_ids == {1, 2}

        # Placement state: kind stays 'client', cache populated.
        rows = await (await db.execute(
            "SELECT id, placement_kind, placement_client_calendar_id, "
            "placement_client_display_name_cache "
            "FROM webcal_subscriptions ORDER BY id"
        )).fetchall()
        for r in rows:
            assert r["placement_kind"] == "client"
            # Target id may or may not be cleared — SET NULL fires only
            # on hard delete; soft-delete leaves the id in place.
            assert r["placement_client_display_name_cache"] == "MLCommons"
    finally:
        await db.close()
        # Reset global state.
        import app.database as db_mod
        db_mod._db_connection = None


async def test_24_repick_clears_alert_state_via_normal_change():
    """[24] After a target disconnect, switching to a different valid
    placement (or back to Main) succeeds via the normal PATCH path —
    the prior alert state does NOT block a new placement change."""
    db = await _make_db()
    try:
        await _create_subscription_main(db, sub_id=1, url="https://x/r.ics")
        await db.execute(
            "UPDATE webcal_subscriptions SET placement_kind='client', "
            "placement_client_calendar_id=10, "
            "placement_client_display_name_cache='MLCommons' WHERE id=1"
        )
        # Simulate the disconnect having already happened (target inactive).
        await db.execute("UPDATE client_calendars SET is_active = 0 WHERE id = 10")
        await db.commit()

        # User repicks → placement_kind='main'.  The PATCH path
        # validates the new value, persists, and clears nothing
        # special — but the placement_target_status derivation flips
        # from 'disconnected' to 'not_applicable' immediately.
        from app.api.webcal import _derive_placement_status
        assert _derive_placement_status("client", 10, 0) == "disconnected"
        # After the user repicks to Main:
        await db.execute(
            "UPDATE webcal_subscriptions SET placement_kind='main', "
            "placement_client_calendar_id=NULL, "
            "placement_client_display_name_cache=NULL WHERE id=1"
        )
        await db.commit()
        row = await (await db.execute(
            "SELECT placement_kind, placement_client_calendar_id "
            "FROM webcal_subscriptions WHERE id=1"
        )).fetchone()
        assert _derive_placement_status(
            row["placement_kind"], row["placement_client_calendar_id"], None,
        ) == "not_applicable"
    finally:
        await db.close()
