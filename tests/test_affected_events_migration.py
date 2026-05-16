"""The affected_ledger_events reshape migration must not lose work.

The table was first shipped with a (user_id, ledger_event_id) primary
key and is rebuilt as an append-only autoincrement-id table.  A row in
it is an event ingest has queued but not yet planned — dropping it
would strand that change — so the migration copies existing rows
across rather than dropping the table.
"""

from __future__ import annotations

import aiosqlite
import pytest

from app.ledger.schema import init_ledger_schema

pytestmark = pytest.mark.asyncio


async def test_queued_rows_survive_the_affected_table_rebuild():
    db = await aiosqlite.connect(":memory:")
    try:
        db.row_factory = aiosqlite.Row
        # FK enforcement off for the test so the OLD table can hold a
        # queued row without standing up every parent table; the
        # migration's copy logic is what is under test.
        await db.execute("PRAGMA foreign_keys = OFF")

        # The OLD-shape affected table with a queued row — the state a
        # pre-migration database is in.
        await db.execute(
            """CREATE TABLE affected_ledger_events (
                   user_id INTEGER NOT NULL,
                   ledger_event_id INTEGER NOT NULL,
                   enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                   PRIMARY KEY (user_id, ledger_event_id)
               )"""
        )
        await db.execute(
            """INSERT INTO affected_ledger_events
                  (user_id, ledger_event_id, enqueued_at)
               VALUES (1, 42, '2026-01-01T00:00:00')"""
        )
        await db.commit()

        await init_ledger_schema(db)

        # The table now has the append-only autoincrement id...
        cols = {
            c["name"]
            for c in await (await db.execute(
                "PRAGMA table_info(affected_ledger_events)"
            )).fetchall()
        }
        assert "id" in cols
        # ...and the queued row was carried across, not dropped.
        rows = await (await db.execute(
            """SELECT user_id, ledger_event_id, enqueued_at
                 FROM affected_ledger_events"""
        )).fetchall()
        assert len(rows) == 1
        assert rows[0]["user_id"] == 1
        assert rows[0]["ledger_event_id"] == 42
        assert rows[0]["enqueued_at"] == "2026-01-01T00:00:00"
        # The scratch table is gone.
        leftover = await (await db.execute(
            "SELECT name FROM sqlite_master "
            "WHERE name = 'affected_ledger_events_old'"
        )).fetchone()
        assert leftover is None

        # The idx_affected_user index lives on the NEW table — the
        # rename carried it onto the _old table, so it must be
        # re-created after the drop or the migrated table has none.
        idx = await (await db.execute(
            """SELECT tbl_name FROM sqlite_master
                WHERE type = 'index' AND name = 'idx_affected_user'"""
        )).fetchone()
        assert idx is not None, "idx_affected_user was lost in the migration"
        assert idx["tbl_name"] == "affected_ledger_events"
    finally:
        await db.close()
