"""Release mode for expired one-off events.

Default retention (``release_expired_events=True``) RELEASES a single
event past ``event_retention_days`` instead of deleting it: its copies
are left frozen on main + clients and it is retired from sync (the
planner, diff, and ingest all skip a ``released`` row).  This preserves
old calendar history past the retention window.

Genuine cancellations are still pruned; only age-based expiry changes.
The legacy delete mode is covered in test_ledger_retention.py.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.database import get_database
from app.jobs.cleanup import run_retention_cleanup
from app.ledger.planner import plan_for_ledger_event
from tests.integration.framework import Scenario
from tests.test_ledger_retention import _pin_release_mode, _seed_user

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Retention-level (release mode)
# ---------------------------------------------------------------------------
async def test_release_mode_freezes_expired_single_event(test_db, monkeypatch):
    """An expired one-off event is RELEASED, not deleted: its live
    projection stays present and is never driven to absent."""
    _pin_release_mode(monkeypatch, True)
    db = await get_database()
    user_id = await _seed_user(db, "rel@example.com")
    long_ago = (datetime.utcnow() - timedelta(days=60)).isoformat()

    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, is_recurring,
               start_at, end_at, status, version, created_at, updated_at)
           VALUES (?, ?, 'main_native', 0, ?, ?, 'active', 1, ?, ?)
           RETURNING id""",
        (user_id, "main_native:1:relpast", long_ago, long_ago,
         long_ago, long_ago),
    )).fetchone()
    ev_id = int(ev["id"])
    await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, target_calendar_id,
               desired_state, desired_payload_hash, desired_ledger_version,
               current_state, google_event_id, applied_ledger_version,
               applied_payload_hash)
           VALUES (?, 'main', NULL, 'present_full', 'h', 1,
                   'present', 'bbreleasevt001', 1, 'h')""",
        (ev_id,),
    )
    await db.commit()

    summary = await run_retention_cleanup()
    assert summary["expired_events_released"] >= 1
    assert summary["expired_events_cancelled"] == 0

    row = await (await db.execute(
        "SELECT status FROM ledger_events WHERE id = ?", (ev_id,),
    )).fetchone()
    assert row is not None, "released event must not be hard-deleted"
    assert row["status"] == "released"
    # The copy stays — projection frozen, NOT driven to absent.
    proj = await (await db.execute(
        """SELECT desired_state, current_state
             FROM ledger_projections WHERE ledger_event_id = ?""",
        (ev_id,),
    )).fetchone()
    assert proj["desired_state"] == "present_full"
    assert proj["current_state"] == "present"


async def test_genuine_cancelled_event_still_pruned_in_release_mode(
    test_db, monkeypatch,
):
    """Release mode only changes age-based expiry — a genuinely cancelled
    event whose projections have drained is still hard-deleted."""
    _pin_release_mode(monkeypatch, True)
    db = await get_database()
    user_id = await _seed_user(db, "relcancel@example.com")
    long_ago = (datetime.utcnow() - timedelta(days=60)).isoformat()

    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, is_recurring,
               start_at, end_at, status, version, created_at, updated_at)
           VALUES (?, ?, 'main_native', 0, ?, ?, 'cancelled', 2, ?, ?)
           RETURNING id""",
        (user_id, "main_native:1:reldrained", long_ago, long_ago,
         long_ago, long_ago),
    )).fetchone()
    ev_id = int(ev["id"])
    await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, target_calendar_id,
               desired_state, desired_payload_hash, desired_ledger_version,
               current_state, applied_ledger_version)
           VALUES (?, 'main', NULL, 'absent', 'absent', 2, 'absent', 2)""",
        (ev_id,),
    )
    await db.commit()

    summary = await run_retention_cleanup()
    assert summary["expired_ledger_events"] >= 1
    row = await (await db.execute(
        "SELECT id FROM ledger_events WHERE id = ?", (ev_id,),
    )).fetchone()
    assert row is None, "drained cancelled event should still be hard-deleted"


async def test_released_event_is_never_hard_deleted(test_db, monkeypatch):
    """The hard-delete bucket only removes status='cancelled' rows — a
    'released' row survives even with no live projection."""
    _pin_release_mode(monkeypatch, True)
    db = await get_database()
    user_id = await _seed_user(db, "relkeep@example.com")
    long_ago = (datetime.utcnow() - timedelta(days=90)).isoformat()

    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, is_recurring,
               start_at, end_at, status, version, created_at, updated_at)
           VALUES (?, ?, 'main_native', 0, ?, ?, 'released', 2, ?, ?)
           RETURNING id""",
        (user_id, "main_native:1:relforever", long_ago, long_ago,
         long_ago, long_ago),
    )).fetchone()
    ev_id = int(ev["id"])
    await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, target_calendar_id,
               desired_state, desired_payload_hash, desired_ledger_version,
               current_state, applied_ledger_version)
           VALUES (?, 'main', NULL, 'absent', 'absent', 2, 'absent', 2)""",
        (ev_id,),
    )
    await db.commit()

    await run_retention_cleanup()
    row = await (await db.execute(
        "SELECT id FROM ledger_events WHERE id = ?", (ev_id,),
    )).fetchone()
    assert row is not None, "released rows must never be hard-deleted"


# ---------------------------------------------------------------------------
# Planner
# ---------------------------------------------------------------------------
async def test_planner_skips_released_event(test_db):
    """plan_for_ledger_event is a no-op for a released event — its frozen
    projection is left exactly as-is (never recomputed to absent)."""
    db = await get_database()
    user_id = await _seed_user(db, "relplan@example.com")
    long_ago = (datetime.utcnow() - timedelta(days=60)).isoformat()

    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, is_recurring,
               start_at, end_at, status, version, created_at, updated_at)
           VALUES (?, ?, 'main_native', 0, ?, ?, 'released', 5, ?, ?)
           RETURNING id""",
        (user_id, "main_native:1:relplan", long_ago, long_ago,
         long_ago, long_ago),
    )).fetchone()
    ev_id = int(ev["id"])
    await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, target_calendar_id,
               desired_state, desired_payload_hash, desired_ledger_version,
               current_state, google_event_id, applied_ledger_version,
               applied_payload_hash)
           VALUES (?, 'main', NULL, 'present_full', 'h', 5,
                   'present', 'bbrelplan00001', 5, 'h')""",
        (ev_id,),
    )
    await db.commit()

    written = await plan_for_ledger_event(db, ledger_event_id=ev_id)
    assert written == 0
    proj = await (await db.execute(
        "SELECT desired_state FROM ledger_projections WHERE ledger_event_id = ?",
        (ev_id,),
    )).fetchone()
    assert proj["desired_state"] == "present_full"


# ---------------------------------------------------------------------------
# Ingest (end-to-end)
# ---------------------------------------------------------------------------
async def test_released_event_survives_full_resync():
    """A released event's copy stays on main and is not resurrected,
    re-deleted, or un-released by a full re-sync (which re-lists it)."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    s.given_event(
        "client_a", summary="Old call",
        start="2026-01-05T09:00:00Z", event_id="oldcall0001",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)
    assert s.google.event_count(s.cal("main")) == 1  # managed full copy

    db = await s.setup_db()
    # Simulate retention having released the aged event.
    await db.execute(
        "UPDATE ledger_events SET status = 'released' WHERE source_type = 'client'",
    )
    # Force a full re-sync so ingest re-lists the (old) source event.
    await db.execute("UPDATE calendar_sync_state SET sync_token = NULL")
    await db.commit()

    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    # Copy still present; row still released; no churn / resurrection.
    assert s.google.event_count(s.cal("main")) == 1
    row = await (await db.execute(
        "SELECT status FROM ledger_events WHERE source_type = 'client'",
    )).fetchone()
    assert row["status"] == "released"
    await s.close()


async def test_released_main_native_event_survives_full_resync():
    """A released main-native event is not un-released by a full re-sync
    of the main calendar; its busy block on clients stays frozen."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])
    s.given_event(
        "main", summary="Old native",
        start="2026-01-06T09:00:00Z", event_id="oldnative001",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)
    assert s.google.event_count(s.cal("client_a")) == 1  # busy block

    db = await s.setup_db()
    await db.execute(
        "UPDATE ledger_events SET status = 'released' WHERE source_type = 'main_native'",
    )
    await db.execute("UPDATE main_calendar_sync_state SET sync_token = NULL")
    await db.commit()

    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    assert s.google.event_count(s.cal("client_a")) == 1
    row = await (await db.execute(
        "SELECT status FROM ledger_events WHERE source_type = 'main_native'",
    )).fetchone()
    assert row["status"] == "released"
    await s.close()


def _ics(*vevents: str) -> bytes:
    body = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//Test//EN"]
    for v in vevents:
        body.append("BEGIN:VEVENT")
        body.extend(line for line in v.strip().splitlines())
        body.append("END:VEVENT")
    body.append("END:VCALENDAR")
    return ("\r\n".join(body) + "\r\n").encode("utf-8")


async def test_released_webcal_event_not_un_released_on_repoll(test_db):
    """A webcal re-poll must not resurrect a released event back to active
    — even when the feed edits it (the bug found in live verification)."""
    from app.ledger.ingest.webcal import ingest_webcal_subscription

    db = await get_database()
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('wc@x.com', 'gwc')",
    )
    uid = int(cur.lastrowid)
    url = "webcal://feed.example/f.ics"
    cur = await db.execute(
        "INSERT INTO webcal_subscriptions (user_id, url) VALUES (?, ?)",
        (uid, url),
    )
    sid = int(cur.lastrowid)
    await db.commit()
    now = datetime(2026, 6, 27, tzinfo=UTC)

    body1 = _ics(
        "UID:wc-1@x\nSUMMARY:Old trip\n"
        "DTSTART:20260101T090000Z\nDTEND:20260101T100000Z",
    )

    async def fetch1(u, inm):
        return {"status": 200, "etag": "e1", "body": body1}

    await ingest_webcal_subscription(
        db, user_id=uid, subscription_id=sid, url=url, fetch=fetch1, now=now,
    )
    row = await (await db.execute(
        "SELECT id, status FROM ledger_events WHERE source_type = 'webcal'",
    )).fetchone()
    assert row["status"] == "active"
    led_id = int(row["id"])

    # Retire it (as retention would).
    await db.execute(
        "UPDATE ledger_events SET status = 'released' WHERE id = ?", (led_id,),
    )
    await db.commit()

    # Re-poll with the SAME UID but EDITED content (would normally
    # UPDATE the row back to status='active').
    body2 = _ics(
        "UID:wc-1@x\nSUMMARY:Old trip EDITED\n"
        "DTSTART:20260101T090000Z\nDTEND:20260101T100000Z",
    )

    async def fetch2(u, inm):
        return {"status": 200, "etag": "e2", "body": body2}

    await ingest_webcal_subscription(
        db, user_id=uid, subscription_id=sid, url=url, fetch=fetch2,
        now=now + timedelta(minutes=1),
    )
    row2 = await (await db.execute(
        "SELECT status, summary FROM ledger_events WHERE id = ?", (led_id,),
    )).fetchone()
    assert row2["status"] == "released", "webcal re-poll must not un-release"
    assert row2["summary"] != "Old trip EDITED", "released row must not be updated"
