"""ledger.verify must not report 'consistent' while projections sit
in a permanently-failed or otherwise unresolved state.
"""

from __future__ import annotations

import pytest

from app.database import get_database
from app.ledger.verify import verify_user
from tests.fakes.google_calendar import FakeGoogleCalendar

pytestmark = pytest.mark.asyncio


async def _seed_projection(db, user_id: int, *, current_state: str,
                           permanently_failed: int) -> None:
    led = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, ?, 'client', 'active', 1, '2026-01-01', '2026-01-01')
           RETURNING id""",
        (user_id, f"client:1:{current_state}-{permanently_failed}"),
    )).fetchone()
    await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, target_calendar_id,
               desired_state, desired_ledger_version,
               current_state, permanently_failed)
           VALUES (?, 'main', NULL, 'present_busy', 1, ?, ?)""",
        (int(led["id"]), current_state, permanently_failed),
    )


async def test_verify_flags_failed_and_unresolved_projections(test_db):
    db = await get_database()
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id, display_name) "
        "VALUES ('u@example.com', 'g-u', 'U') RETURNING id"
    )
    uid = int((await cur.fetchone())["id"])

    await _seed_projection(db, uid, current_state="errored", permanently_failed=1)
    await _seed_projection(db, uid, current_state="unknown", permanently_failed=0)
    await db.commit()

    result = await verify_user(
        db, FakeGoogleCalendar(),
        user_id=uid,
        main_google_calendar_id="main@cal.test",
        google_calendar_id_for={},
    )

    assert result["consistent"] is False, (
        "verify must not report consistent while bad-state projections exist"
    )
    blob = " ".join(result["divergences"]).lower()
    assert "permanently" in blob
    assert "unresolved" in blob
