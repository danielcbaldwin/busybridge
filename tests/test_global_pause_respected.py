"""The ledger engine must honour the global pause switch.

Two pause switches exist:

* per-user — ``users.sync_paused`` (circuit breaker, per-account
  admin pause);
* global — the ``settings`` row keyed ``sync_paused`` (admin "pause
  everything", and the backup job, which flips it to freeze writes
  for a consistent snapshot).

The reconciler used to read only the per-user flag, so the global
switch silently did nothing — backups ran concurrently with sync.
``_sync_is_paused`` now reads both.
"""

from __future__ import annotations

import pytest

from app.ledger.reconciler import _sync_is_paused
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _set_global_pause(db, value: str) -> None:
    await db.execute(
        "INSERT INTO settings (key, value_plain) VALUES ('sync_paused', ?) "
        "ON CONFLICT(key) DO UPDATE SET value_plain = excluded.value_plain",
        (value,),
    )
    await db.commit()


async def test_sync_is_paused_honours_the_global_switch():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    assert await _sync_is_paused(db, user.user_id) is False

    await _set_global_pause(db, "true")
    assert await _sync_is_paused(db, user.user_id) is True, \
        "global sync_paused setting was ignored by the engine"

    await _set_global_pause(db, "false")
    assert await _sync_is_paused(db, user.user_id) is False
    await s.close()


async def test_sync_is_paused_honours_the_per_user_flag():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    await db.execute(
        "UPDATE users SET sync_paused = 1 WHERE id = ?", (user.user_id,),
    )
    await db.commit()
    assert await _sync_is_paused(db, user.user_id) is True
    await s.close()


async def test_global_pause_makes_the_reconciler_skip_ingest():
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    await _set_global_pause(db, "true")
    out = await s.run_reconciler("alice")
    assert out["paused"] is True, \
        "reconciler ingested despite the global pause being on"
    await s.close()
