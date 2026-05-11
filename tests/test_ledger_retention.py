"""Retention-cleanup tests covering the ledger-backed paths.

Mirrors REWRITE_PLAN.md §10 retention semantics:

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
