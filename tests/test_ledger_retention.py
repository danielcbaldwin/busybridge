"""Retention-cleanup tests covering the ledger-backed paths.

Covers the retention semantics:

* Past single events fall out after ``event_retention_days``.
* Cancelled recurring series fall out after
  ``recurring_soft_delete_days``.
* Settled outbox rows older than 7 days are pruned.
* Disconnected calendars + old sync logs continue to age out.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.database import get_database
from app.jobs.cleanup import run_retention_cleanup

UTC = timezone.utc

pytestmark = pytest.mark.asyncio


def _pin_release_mode(monkeypatch, release: bool) -> None:
    """Force the retention cleanup's release_expired_events flag.

    Replaces cleanup's get_settings with a SimpleNamespace carrying the
    real retention windows plus the chosen release flag, so a test does
    not depend on the live default (which is release=True)."""
    from types import SimpleNamespace
    import app.jobs.cleanup as cleanup_mod
    from app.config import get_settings as _gs
    real = _gs()
    fake = SimpleNamespace(
        event_retention_days=real.event_retention_days,
        recurring_soft_delete_days=real.recurring_soft_delete_days,
        audit_log_retention_days=real.audit_log_retention_days,
        disconnected_calendar_retention_days=real.disconnected_calendar_retention_days,
        release_expired_events=release,
    )
    monkeypatch.setattr(cleanup_mod, "get_settings", lambda: fake)


async def _seed_user(db, email: str = "ret@example.com") -> int:
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES (?, ?, 'Ret')""",
        (email, email),
    )
    return int(cursor.lastrowid)


async def test_expired_single_ledger_events_are_pruned(test_db):
    db = await get_database()
    user_id = await _seed_user(db)

    long_ago = (datetime.utcnow() - timedelta(days=60)).isoformat()
    recent = (datetime.utcnow() - timedelta(days=1)).isoformat()
    await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, is_recurring,
               start_at, end_at, status, version,
               created_at, updated_at)
           VALUES (?, ?, 'main_native', 0, ?, ?, 'cancelled', 1, ?, ?)""",
        (user_id, "main_native:1:old",
         long_ago, long_ago, long_ago, long_ago),
    )
    await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, is_recurring,
               start_at, end_at, status, version,
               created_at, updated_at)
           VALUES (?, ?, 'main_native', 0, ?, ?, 'active', 1, ?, ?)""",
        (user_id, "main_native:1:new",
         recent, recent, recent, recent),
    )
    await db.commit()

    summary = await run_retention_cleanup()
    assert summary["expired_ledger_events"] >= 1

    rows = await (await db.execute(
        "SELECT canonical_uid FROM ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    remaining = {r["canonical_uid"] for r in rows}
    assert "main_native:1:old" not in remaining
    assert "main_native:1:new" in remaining


async def test_settled_outbox_rows_are_pruned(test_db):
    db = await get_database()
    user_id = await _seed_user(db, "outbox@example.com")

    # Seed a ledger event + projection so the FK is satisfied.
    cursor = await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, 'main_native:2:e', 'main_native', 'active', 1,
                   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id""",
        (user_id,),
    )
    ledger_id = int((await cursor.fetchone())["id"])
    cursor = await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, desired_state,
               desired_payload_hash, desired_ledger_version)
           VALUES (?, 'main', 'present_full', 'h', 1) RETURNING id""",
        (ledger_id,),
    )
    proj_id = int((await cursor.fetchone())["id"])

    old_done = (datetime.utcnow() - timedelta(days=14)).isoformat()
    recent_done = (datetime.utcnow() - timedelta(days=1)).isoformat()

    await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, target_google_calendar_id,
               status, completed_at, created_at)
           VALUES (?, ?, 'create', 'k-old', 1, 'main', 'done', ?, ?)""",
        (user_id, proj_id, old_done, old_done),
    )
    await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, target_google_calendar_id,
               status, completed_at, created_at)
           VALUES (?, ?, 'create', 'k-new', 1, 'main', 'done', ?, ?)""",
        (user_id, proj_id, recent_done, recent_done),
    )
    await db.commit()

    summary = await run_retention_cleanup()
    assert summary["settled_outbox_rows"] >= 1

    rows = await (await db.execute(
        "SELECT idempotency_key FROM outbox_operations WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    keys = {r["idempotency_key"] for r in rows}
    assert "k-old" not in keys
    assert "k-new" in keys


async def test_disconnected_calendars_age_out(test_db):
    db = await get_database()
    user_id = await _seed_user(db, "discon@example.com")

    long_ago = (datetime.utcnow() - timedelta(days=40)).isoformat()
    # OAuth token (required FK)
    cursor = await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', 'c@example.com', x'00', x'00') RETURNING id""",
        (user_id,),
    )
    token_id = int((await cursor.fetchone())["id"])
    await db.execute(
        """INSERT INTO client_calendars
              (user_id, oauth_token_id, google_calendar_id, display_name,
               is_active, disconnected_at)
           VALUES (?, ?, 'old@cal', 'Old', 0, ?)""",
        (user_id, token_id, long_ago),
    )
    await db.execute(
        """INSERT INTO client_calendars
              (user_id, oauth_token_id, google_calendar_id, display_name,
               is_active, disconnected_at)
           VALUES (?, ?, 'recent@cal', 'Recent', 0, ?)""",
        (user_id, token_id, datetime.utcnow().isoformat()),
    )
    await db.commit()

    summary = await run_retention_cleanup()
    assert summary["disconnected_calendars"] >= 1

    rows = await (await db.execute(
        "SELECT google_calendar_id FROM client_calendars WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    ids = {r["google_calendar_id"] for r in rows}
    assert "old@cal" not in ids
    assert "recent@cal" in ids


# ---------------------------------------------------------------------------
# Orphan-safe retention
# ---------------------------------------------------------------------------
async def test_active_expired_event_with_live_projection_is_cancelled_not_deleted(
    test_db, monkeypatch,
):
    """An expired event whose busy block is still live on Google must
    NOT be hard-deleted — that would orphan the Google copy.  It is
    cancelled and re-planned; the next reconcile drains the delete.

    This is the legacy DELETE mode (release_expired_events=False); the
    default release mode is covered in test_expired_event_release.py."""
    _pin_release_mode(monkeypatch, False)
    db = await get_database()
    user_id = await _seed_user(db, "reta@example.com")
    long_ago = (datetime.utcnow() - timedelta(days=60)).isoformat()

    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, is_recurring,
               start_at, end_at, status, version, created_at, updated_at)
           VALUES (?, ?, 'main_native', 0, ?, ?, 'active', 1, ?, ?)
           RETURNING id""",
        (user_id, "main_native:1:livepast", long_ago, long_ago,
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
                   'present', 'bbliveevt00001', 1, 'h')""",
        (ev_id,),
    )
    await db.commit()

    summary = await run_retention_cleanup()

    row = await (await db.execute(
        "SELECT status FROM ledger_events WHERE id = ?", (ev_id,),
    )).fetchone()
    assert row is not None, "event was hard-deleted despite a live projection"
    assert row["status"] == "cancelled"
    assert summary["expired_events_cancelled"] >= 1
    # Re-planned: the projection now wants the Google copy gone.
    proj = await (await db.execute(
        "SELECT desired_state FROM ledger_projections WHERE ledger_event_id = ?",
        (ev_id,),
    )).fetchone()
    assert proj["desired_state"] == "absent"


async def test_drained_cancelled_event_is_hard_deleted(test_db):
    """Once every projection has drained ('present' nowhere) the
    cancelled row is safe to hard-delete."""
    db = await get_database()
    user_id = await _seed_user(db, "retb@example.com")
    long_ago = (datetime.utcnow() - timedelta(days=60)).isoformat()

    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, is_recurring,
               start_at, end_at, status, version, created_at, updated_at)
           VALUES (?, ?, 'main_native', 0, ?, ?, 'cancelled', 2, ?, ?)
           RETURNING id""",
        (user_id, "main_native:1:drained", long_ago, long_ago,
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

    row = await (await db.execute(
        "SELECT id FROM ledger_events WHERE id = ?", (ev_id,),
    )).fetchone()
    assert row is None, "fully-drained cancelled event should be hard-deleted"
    assert summary["expired_ledger_events"] >= 1


async def test_orphan_guard_trigger_blocks_deleting_a_live_ledger_event(test_db):
    """The DB trigger refuses a direct delete of a ledger_event whose
    projection is still 'present' on Google."""
    db = await get_database()
    user_id = await _seed_user(db, "retc@example.com")

    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, 'main_native:1:guard', 'main_native', 'active', 1,
                   '2026-01-01', '2026-01-01')
           RETURNING id""",
        (user_id,),
    )).fetchone()
    ev_id = int(ev["id"])
    await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, desired_state,
               desired_payload_hash, desired_ledger_version, current_state)
           VALUES (?, 'main', 'present_full', 'h', 1, 'present')""",
        (ev_id,),
    )
    await db.commit()

    with pytest.raises(Exception) as exc:
        await db.execute("DELETE FROM ledger_events WHERE id = ?", (ev_id,))
    assert "refusing to delete" in str(exc.value)

    # The row survives the blocked delete.
    row = await (await db.execute(
        "SELECT id FROM ledger_events WHERE id = ?", (ev_id,),
    )).fetchone()
    assert row is not None
