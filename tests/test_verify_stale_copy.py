"""verify_user must not call a 'present' projection consistent when the
ledger itself knows it hasn't applied its desired state.

The Google copy can EXIST and not be cancelled while still showing the
wrong time/title — because a later update (e.g. a reschedule) failed to
land, leaving applied_* behind desired_*.  Existence-only verification
misses exactly the failure mode this tool is meant to catch, so it would
give false confidence at cutover.
"""

from __future__ import annotations

import pytest

from app.database import get_database
from app.ledger.verify import verify_user
from tests.fakes.google_calendar import FakeGoogleCalendar

pytestmark = pytest.mark.asyncio

MAIN = "main@cal.test"
GID = "abcde12345"  # valid base32hex event id


async def _mk_user_and_google(db, *, with_event=True):
    await db.execute(
        "INSERT INTO users (id, email, google_user_id) "
        "VALUES (1, 'u@x.com', 'g1')"
    )
    await db.commit()
    g = FakeGoogleCalendar()
    g.add_calendar(MAIN)
    if with_event:
        g.insert_event(MAIN, {
            "id": GID, "summary": "Busy",
            "start": {"dateTime": "2026-06-10T10:00:00Z"},
            "end": {"dateTime": "2026-06-10T11:00:00Z"},
        })
    return g


async def _seed_present(db, *, applied_version, desired_version,
                        applied_hash, desired_hash, gid=GID):
    led = await (await db.execute(
        "INSERT INTO ledger_events "
        "(user_id, canonical_uid, source_type, status, version, summary, "
        " created_at, updated_at) "
        "VALUES (1, 'client:1:x', 'client', 'active', 1, 'Client Mtg', "
        " '2026-01-01', '2026-01-01') RETURNING id"
    )).fetchone()
    await db.execute(
        "INSERT INTO ledger_projections "
        "(ledger_event_id, target_kind, target_calendar_id, desired_state, "
        " desired_payload_hash, desired_ledger_version, current_state, "
        " google_event_id, applied_payload_hash, applied_ledger_version) "
        "VALUES (?, 'main', NULL, 'present_busy', ?, ?, 'present', ?, ?, ?)",
        (int(led["id"]), desired_hash, desired_version, gid,
         applied_hash, applied_version),
    )
    await db.commit()


async def _verify(db, g):
    return await verify_user(
        db, g, user_id=1, main_google_calendar_id=MAIN,
        google_calendar_id_for={},
    )


async def test_converged_present_copy_is_ok(test_db):
    db = await get_database()
    g = await _mk_user_and_google(db)
    await _seed_present(db, applied_version=3, desired_version=3,
                        applied_hash="hashAAAA", desired_hash="hashAAAA")
    res = await _verify(db, g)
    assert res["consistent"] is True, res["divergences"]
    assert res["ok"] == 1
    assert res["divergences"] == []


async def test_stale_by_payload_hash_is_flagged(test_db):
    db = await get_database()
    g = await _mk_user_and_google(db)
    # Same version, but the applied content predates the desired content
    # (a reschedule that never reached Google).
    await _seed_present(db, applied_version=3, desired_version=3,
                        applied_hash="OLDhash0", desired_hash="NEWhash0")
    res = await _verify(db, g)
    assert res["consistent"] is False
    blob = " ".join(res["divergences"])
    assert "STALE" in blob
    assert res["ok"] == 0
    assert res["checked"] == 1


async def test_stale_by_ledger_version_is_flagged(test_db):
    db = await get_database()
    g = await _mk_user_and_google(db)
    await _seed_present(db, applied_version=2, desired_version=3,
                        applied_hash="sameHash", desired_hash="sameHash")
    res = await _verify(db, g)
    assert res["consistent"] is False
    assert "STALE" in " ".join(res["divergences"])


async def test_never_applied_present_is_flagged(test_db):
    db = await get_database()
    g = await _mk_user_and_google(db)
    await _seed_present(db, applied_version=None, desired_version=1,
                        applied_hash=None, desired_hash="h")
    res = await _verify(db, g)
    assert res["consistent"] is False
    assert "STALE" in " ".join(res["divergences"])


async def test_converged_but_missing_on_google_still_flagged(test_db):
    # Existence check must still fire: converged ledger, but Google lost it.
    db = await get_database()
    g = await _mk_user_and_google(db, with_event=False)  # no event inserted
    await _seed_present(db, applied_version=3, desired_version=3,
                        applied_hash="h", desired_hash="h")
    res = await _verify(db, g)
    assert res["consistent"] is False
    assert "MISSING" in " ".join(res["divergences"])


async def test_converged_but_cancelled_on_google_still_flagged(test_db):
    db = await get_database()
    g = await _mk_user_and_google(db)
    g.delete_event(MAIN, GID)  # leaves a cancelled tombstone
    await _seed_present(db, applied_version=3, desired_version=3,
                        applied_hash="h", desired_hash="h")
    res = await _verify(db, g)
    assert res["consistent"] is False
    assert "CANCELLED" in " ".join(res["divergences"])
