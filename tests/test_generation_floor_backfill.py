"""The generation-floor backfill must start legacy high-generation
projections on a fresh ceiling episode.

Databases that predate google_id_generation_floor carry generations
accumulated under the old LIFETIME-cap semantics (observed at 3000+ in
production, grown by routine absent->present toggles).  The ALTER adds
the floor with DEFAULT 0, so without the backfill every healthy legacy
row would insta-fail the per-episode ceiling (generation - floor >= 50)
on its next present toggle — a false permanent failure and an operator
alert per row.  Rows already marked permanently_failed are left alone:
recovering those is the admin retry action's job, and it resets the
floor itself.
"""

from __future__ import annotations

import aiosqlite
import pytest

from app.ledger.schema import init_ledger_schema

pytestmark = pytest.mark.asyncio


_next_cal = iter(range(1, 100))


async def _projection(db, *, generation, floor=0, failed=0):
    # Distinct target_calendar_id per row: (ledger_event_id, target_kind,
    # target_calendar_id) is UNIQUE.
    cur = await db.execute(
        """INSERT INTO ledger_projections
               (ledger_event_id, target_kind, target_calendar_id,
                desired_state, desired_ledger_version,
                google_id_generation, google_id_generation_floor,
                permanently_failed)
           VALUES (1, 'client', ?, 'present_busy', 1, ?, ?, ?)""",
        (next(_next_cal), generation, floor, failed),
    )
    return cur.lastrowid


async def _floor_of(db, proj_id):
    row = await (await db.execute(
        "SELECT google_id_generation_floor FROM ledger_projections "
        "WHERE id = ?",
        (proj_id,),
    )).fetchone()
    return int(row[0])


async def test_legacy_generations_get_a_fresh_episode_floor():
    db = await aiosqlite.connect(":memory:")
    try:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA foreign_keys = OFF")
        await init_ledger_schema(db)

        # The production shapes from the operator's diagnostics:
        healthy_legacy = await _projection(db, generation=3260)
        healthy_small = await _projection(db, generation=1124)
        bricked = await _projection(db, generation=3492, failed=1)
        mid_episode = await _projection(db, generation=10, floor=5)
        fresh = await _projection(db, generation=0)

        # Re-run the schema init — the migration path an upgraded
        # database takes at startup.
        await init_ledger_schema(db)

        # Healthy legacy rows start a fresh episode (gap 0 < cap): the
        # next present toggle burns one generation and succeeds instead
        # of insta-failing the entry check.
        assert await _floor_of(db, healthy_legacy) == 3260
        assert await _floor_of(db, healthy_small) == 1124
        # A permanently-failed row is the admin retry action's to
        # recover — the backfill must not mask it.
        assert await _floor_of(db, bricked) == 0
        # A row mid-episode under the NEW semantics keeps its burn
        # budget: the backfill only matches the impossible legacy gap.
        assert await _floor_of(db, mid_episode) == 5
        assert await _floor_of(db, fresh) == 0

        # Idempotent: a third init changes nothing.
        await init_ledger_schema(db)
        assert await _floor_of(db, healthy_legacy) == 3260
        assert await _floor_of(db, bricked) == 0
    finally:
        await db.close()
