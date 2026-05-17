"""A calendar's sync-progress must reflect only that calendar's own
work — not main-copy work driven by the user's other calendars.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.api.calendars import get_calendar_sync_progress
from app.auth.session import User
from app.database import get_database

pytestmark = pytest.mark.asyncio


async def _seed_failed_op(db, user_id: int, source_calendar_id: int) -> None:
    """A permanently-failed main-copy outbox op for an event sourced
    from ``source_calendar_id``."""
    led = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, source_calendar_id,
               status, version, created_at, updated_at)
           VALUES (?, ?, 'client', ?, 'active', 1, '2026-01-01', '2026-01-01')
           RETURNING id""",
        (user_id, f"client:{source_calendar_id}:ev", source_calendar_id),
    )).fetchone()
    proj = await (await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, target_calendar_id,
               desired_state, desired_ledger_version, current_state)
           VALUES (?, 'main', NULL, 'present', 1, 'errored')
           RETURNING id""",
        (int(led["id"]),),
    )).fetchone()
    now = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, target_google_calendar_id,
               status, completed_at)
           VALUES (?, ?, 'create', ?, 1, 'main@cal',
                   'permanent_failure', ?)""",
        (user_id, int(proj["id"]), f"k-{source_calendar_id}", now),
    )


async def test_progress_excludes_other_calendars_failed_work(test_db):
    db = await get_database()
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id, display_name, main_calendar_id) "
        "VALUES ('u@example.com', 'g-u', 'U', 'main@cal') RETURNING id"
    )
    uid = int((await cur.fetchone())["id"])
    tok = await (await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', 'u@example.com', x'00', x'00')
           RETURNING id""",
        (uid,),
    )).fetchone()
    token_id = int(tok["id"])
    cals = {}
    for name in ("A", "B"):
        c = await (await db.execute(
            """INSERT INTO client_calendars
                  (user_id, oauth_token_id, google_calendar_id, display_name)
               VALUES (?, ?, ?, ?) RETURNING id""",
            (uid, token_id, f"{name}@cal.test", name),
        )).fetchone()
        cals[name] = int(c["id"])
    # A permanently-failed main-copy op driven only by calendar B.
    await _seed_failed_op(db, uid, cals["B"])
    await db.commit()

    user = User(id=uid, email="u@example.com", google_user_id="g-u")

    # Calendar A has no work of its own — its progress must not show
    # calendar B's failure.
    a_status = await get_calendar_sync_progress(cals["A"], user=user)
    assert a_status["status"] != "error", a_status

    # Calendar B's own progress does surface its failure.
    b_status = await get_calendar_sync_progress(cals["B"], user=user)
    assert b_status["status"] == "error", b_status
