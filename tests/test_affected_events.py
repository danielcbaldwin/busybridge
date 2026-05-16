"""Affected ledger events are tracked in their own table.

The reconciler plans the events the ingest layer and admin ops flag
as changed.  Those ids live in ``affected_ledger_events`` — one row
per (user, event), written ``INSERT OR IGNORE`` — not in a shared
JSON blob.  So concurrent writers cannot lose each other's ids, a
claim no longer clears them out from under the planner, and a crash
mid-plan re-plans next pass instead of stranding the work.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.ledger.triggers import (
    claim_due_request,
    enqueue_webhook,
    record_affected_events,
)
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _event(db, user_id, canonical_uid):
    row = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, ?, 'client', 'active', 1, '2026-01-01', '2026-01-01')
           RETURNING id""",
        (user_id, canonical_uid),
    )).fetchone()
    return int(row["id"])


async def _affected(db, user_id):
    rows = await (await db.execute(
        "SELECT ledger_event_id FROM affected_ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    return sorted(int(r["ledger_event_id"]) for r in rows)


async def test_record_affected_events_merges_without_losing_ids():
    """Two overlapping recordings keep the union — the old JSON
    read-merge-write could drop one writer's ids under a race."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    uid = user.user_id

    e1 = await _event(db, uid, "client:1:a")
    e2 = await _event(db, uid, "client:1:b")
    e3 = await _event(db, uid, "client:1:c")
    await db.commit()

    await record_affected_events(db, user_id=uid, ledger_event_ids=[e1, e2])
    await record_affected_events(db, user_id=uid, ledger_event_ids=[e2, e3])
    # Recording a duplicate is a no-op (INSERT OR IGNORE).
    await record_affected_events(db, user_id=uid, ledger_event_ids=[e1])
    await db.commit()

    assert await _affected(db, uid) == [e1, e2, e3]
    await s.close()


async def test_affected_events_survive_a_claim():
    """claim_due_request must not clear affected_ledger_events — the
    reconciler consumes them only after planning succeeds."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    uid = user.user_id

    e1 = await _event(db, uid, "client:1:a")
    await db.commit()
    await record_affected_events(db, user_id=uid, ledger_event_ids=[e1])
    await enqueue_webhook(db, user_id=uid, source_hint="client:7")
    await db.commit()

    # Claim past the webhook debounce window.
    claimed = await claim_due_request(
        db, user_id=uid, now=datetime(2099, 1, 1, tzinfo=timezone.utc),
    )
    assert claimed is not None
    assert await _affected(db, uid) == [e1], "claim wrongly cleared the list"
    await s.close()


async def test_reconcile_clears_affected_events_after_planning():
    """A reconcile pass plans the affected events and then clears
    their rows — so a clean pass leaves the table empty."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    uid = user.user_id

    e1 = await _event(db, uid, "client:1:a")
    await db.commit()
    await record_affected_events(db, user_id=uid, ledger_event_ids=[e1])
    await db.commit()

    await s.run_reconciler("alice")

    assert await _affected(db, uid) == [], \
        "affected events were not cleared after a successful reconcile"
    await s.close()


async def test_trigger_path_does_not_write_sources_json():
    """The trigger path manages scheduling only — never the (now
    legacy) sources_json column."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    await enqueue_webhook(db, user_id=user.user_id, source_hint="main")

    row = await (await db.execute(
        "SELECT sources_json FROM reconcile_requests WHERE user_id = ?",
        (user.user_id,),
    )).fetchone()
    assert row is not None
    assert row["sources_json"] is None
    await s.close()
