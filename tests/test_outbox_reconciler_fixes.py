"""Regressions for outbox/reconciler review findings.

1. Drain counters must report what actually happened: a create that
   resolved internally to superseded (etag race on the 409-confirm
   path) or permanent failure (burned-id give-up; covered in
   test_outbox_generation_ceiling.py) must not be counted 'succeeded'.
2. A dry-run must capture ops that ``enqueue`` RESURRECTS in place (a
   done/superseded row flipped back to pending under the same
   idempotency key) — previously those were both missing from
   ``preview_operations`` and left pending for the next real pass to
   drain, violating the documented dry-run guarantee.
3. The ``sync_paused`` kill-switch reads must fail CLOSED: only the
   minimal-test-DB "no such table: settings" case is tolerated; a real
   DB error propagates instead of silently disabling the admin
   emergency stop on the Google write path.
4. ``_clear_affected_ledger_rows`` deletes in chunked ``IN (...)``
   statements (bounded by SQLite's classic 999-variable limit) rather
   than one DELETE per row.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

import aiosqlite
import pytest

from app.ledger import outbox
from app.ledger.outbox import drain_user
from app.ledger.reconciler import (
    _clear_affected_ledger_rows,
    _pause_mode,
    _read_affected_ledger_rows,
)
from tests.integration.framework import Scenario

UTC = timezone.utc
NOW = datetime(2026, 7, 1, tzinfo=UTC)


class _HttpErr(Exception):
    def __init__(self, status):
        super().__init__(f"http {status}")
        self.status = status


# ---------------------------------------------------------------------------
# 1. Drain counters: the 409→confirm→412 etag race counts as superseded
# ---------------------------------------------------------------------------
class _EtagRaceGoogle:
    """insert 409s (id taken by a LIVE event), the confirming GET finds
    it confirmed, and the restoring update loses the etag race with 412
    — _do_create's etag_mismatch_on_409 supersede path.  Synchronous:
    drain_user wraps its client with as_async_google/to_thread."""

    def insert_event(self, cal_id, body):
        raise _HttpErr(409)

    def get_event(self, cal_id, gid):
        return {"id": gid, "status": "confirmed", "etag": '"e1"'}

    def update_event(self, cal_id, gid, body, if_match=None):
        raise _HttpErr(412)


async def _seed_pending_create(db):
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')")
    uid = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_events (user_id, canonical_uid, source_type) "
        "VALUES (?, 'uid-1', 'client')", (uid,))
    le = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_projections "
        "(ledger_event_id, target_kind, desired_state, desired_ledger_version, "
        " current_state) VALUES (?, 'client', 'present', 1, 'absent')", (le,))
    proj = int(cur.lastrowid)
    body = {"summary": "Busy",
            "start": {"dateTime": "2026-07-10T09:00:00Z"},
            "end": {"dateTime": "2026-07-10T10:00:00Z"}}
    cur = await db.execute(
        "INSERT INTO outbox_operations "
        "(user_id, projection_id, operation, idempotency_key, "
        " ledger_version_at_enqueue, target_google_calendar_id, payload_json, "
        " status, next_attempt_at) "
        "VALUES (?, ?, 'create', 'idem-1', 1, 'cal-x', ?, 'pending', "
        " '2020-01-01T00:00:00+00:00')",
        (uid, proj, json.dumps(body)))
    op_id = int(cur.lastrowid)
    await db.commit()
    return uid, proj, op_id


@pytest.mark.asyncio
async def test_create_409_etag_race_counts_as_superseded(test_db):
    """Regression: _execute_op fell through to a blanket 'succeeded'
    for creates, so this supersede was invisible to drain counters —
    undercounting the reconciler's fixed-point convergence signal."""
    db = test_db
    uid, proj_id, op_id = await _seed_pending_create(db)

    counters = await drain_user(db, _EtagRaceGoogle(), user_id=uid, now=NOW)

    assert counters["processed"] == 1
    assert counters["superseded"] == 1
    assert counters["succeeded"] == 0
    op_row = await (await db.execute(
        "SELECT status, last_error FROM outbox_operations WHERE id=?",
        (op_id,))).fetchone()
    assert op_row["status"] == "superseded"
    assert op_row["last_error"] == "etag_mismatch_on_409"
    # And the projection was asked to replan.
    proj_row = await (await db.execute(
        "SELECT applied_ledger_version FROM ledger_projections WHERE id=?",
        (proj_id,))).fetchone()
    assert proj_row["applied_ledger_version"] is None


# ---------------------------------------------------------------------------
# 2. Dry-run captures and restores resurrected ops
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_dry_run_previews_and_restores_a_resurrected_op():
    """A dry-run whose diff re-derives an already-completed op's
    idempotency key hits enqueue's conflict-resurrection path, which
    UPDATEs the existing done row back to pending IN PLACE (id at or
    below the pre-pass watermark).  The dry run must (a) include that
    op in preview_operations, (b) restore the row to its original
    status, so (c) a later real drain has nothing to send."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    s.given_event("client_a", summary="Standup")

    # Real pass: the busy block lands on main; its create op is done.
    await s.run_reconciler("alice")
    db = await s.setup_db()
    done_op = await (await db.execute(
        """SELECT * FROM outbox_operations
            WHERE user_id = ? AND operation = 'create' AND status = 'done'""",
        (user.user_id,),
    )).fetchone()
    assert done_op is not None

    # Reset the projection exactly as the update-404 (target event
    # gone) path does: desired unchanged (same ledger version), current
    # absent.  The next diff re-derives OP_CREATE under the SAME
    # idempotency key -> enqueue resurrects the done row in place.
    await db.execute(
        """UPDATE ledger_projections
              SET current_state = 'absent', google_event_id = NULL,
                  google_etag = NULL, applied_ledger_version = NULL,
                  applied_payload_hash = NULL
            WHERE id = ?""",
        (int(done_op["projection_id"]),),
    )
    await db.commit()

    out = await s.run_reconciler("alice", include_main=False, dry_run=True)

    # (a) The resurrected op IS the preview.
    preview = out["preview_operations"]
    assert [p["outbox_id"] for p in preview] == [int(done_op["id"])], preview
    assert preview[0]["operation"] == "create"
    assert preview[0]["event_summary"] == "Standup"

    # (b) The row is back to its pre-dry-run state.
    restored = await (await db.execute(
        "SELECT status, attempts, completed_at FROM outbox_operations WHERE id=?",
        (int(done_op["id"]),),
    )).fetchone()
    assert restored["status"] == "done"
    assert restored["attempts"] == done_op["attempts"]
    assert restored["completed_at"] == done_op["completed_at"]

    # (c) Nothing pending is left for a later pass to drain.
    counters = await drain_user(
        db, s.google, user_id=user.user_id, now=s.clock.now(),
    )
    assert counters["processed"] == 0, (
        "dry-run left a live pending op behind"
    )
    await s.close()


# ---------------------------------------------------------------------------
# 3. Kill switches fail closed
# ---------------------------------------------------------------------------
class _RaisingDB:
    """Stands in for a connection whose very first execute fails."""

    def __init__(self, exc: Exception):
        self._exc = exc

    async def execute(self, *args, **kwargs):
        raise self._exc


@pytest.mark.asyncio
async def test_kill_switch_reads_tolerate_only_a_missing_settings_table():
    # A minimal DB without the settings table (but with users, which
    # _pause_mode reads next) — the one tolerated failure.
    db = await aiosqlite.connect(":memory:")
    db.row_factory = aiosqlite.Row
    await db.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, "
        " sync_paused BOOLEAN DEFAULT 0)")
    await db.execute("INSERT INTO users (id, sync_paused) VALUES (1, 0)")
    assert await outbox._global_sync_paused(db) is False
    assert await _pause_mode(db, 1) is None
    await db.close()


@pytest.mark.asyncio
async def test_kill_switch_reads_propagate_real_db_errors():
    """A real DB failure (locked, I/O error, ...) must abort the pass
    (fail closed), not silently report 'not paused' (fail open) while
    the operator believes the emergency stop is holding."""
    locked = sqlite3.OperationalError("database is locked")
    with pytest.raises(sqlite3.OperationalError):
        await outbox._global_sync_paused(_RaisingDB(locked))
    with pytest.raises(sqlite3.OperationalError):
        await _pause_mode(_RaisingDB(locked), 1)

    # Non-OperationalError failures propagate too.
    boom = RuntimeError("connection lost")
    with pytest.raises(RuntimeError):
        await outbox._global_sync_paused(_RaisingDB(boom))
    with pytest.raises(RuntimeError):
        await _pause_mode(_RaisingDB(boom), 1)


# ---------------------------------------------------------------------------
# 4. Chunked affected-row clear
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_clear_affected_rows_handles_more_than_999_ids():
    """More ids than SQLite's classic 999-host-parameter ceiling must
    clear in one call (the delete is chunked internally)."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    uid = user.user_id

    row = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, 'client:1:a', 'client', 'active', 1,
                   '2026-01-01', '2026-01-01')
           RETURNING id""",
        (uid,),
    )).fetchone()
    ledger_id = int(row["id"])
    await db.executemany(
        "INSERT INTO affected_ledger_events (user_id, ledger_event_id) "
        "VALUES (?, ?)",
        [(uid, ledger_id)] * 1200,
    )
    await db.commit()

    rows = await _read_affected_ledger_rows(db, user_id=uid)
    assert len(rows) == 1200
    await _clear_affected_ledger_rows(db, row_ids=[rid for rid, _ in rows])

    left = await (await db.execute(
        "SELECT COUNT(*) AS n FROM affected_ledger_events WHERE user_id = ?",
        (uid,),
    )).fetchone()
    assert int(left["n"]) == 0
    await s.close()
