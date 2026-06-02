"""Guard the outbox drain/prune indexes.

The drain's _claim_next runs every 30s per due user and the nightly
retention prune deletes settled rows; both must use an index rather than
full-scanning outbox_operations (175K rows when backlogged). These
indexes were added after the pre-cutover review found the claim doing a
full table SCAN.  This test fails if either is dropped.
"""

import pytest


@pytest.mark.asyncio
async def test_outbox_claim_and_settled_indexes_exist(test_db):
    db = test_db
    rows = await (await db.execute(
        "SELECT name FROM sqlite_master WHERE type='index' "
        "AND tbl_name='outbox_operations'"
    )).fetchall()
    names = {r["name"] for r in rows}
    assert "idx_outbox_claim" in names
    assert "idx_outbox_settled" in names


@pytest.mark.asyncio
async def test_claim_query_uses_an_index_not_a_full_scan(test_db):
    db = test_db
    # Seed a user + projection so a handful of outbox rows can exist; the
    # plan check below does not depend on row count, only on index match.
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')")
    user_id = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_events (user_id, canonical_uid, source_type) "
        "VALUES (?, 'uid-1', 'client')", (user_id,))
    le_id = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_projections "
        "(ledger_event_id, target_kind, desired_state, desired_ledger_version) "
        "VALUES (?, 'client', 'present', 1)", (le_id,))
    proj_id = int(cur.lastrowid)
    for i in range(50):
        await db.execute(
            "INSERT INTO outbox_operations "
            "(user_id, projection_id, operation, idempotency_key, "
            " ledger_version_at_enqueue, target_google_calendar_id, status, "
            " next_attempt_at) "
            "VALUES (?, ?, 'create', ?, 1, 'cal', 'pending', '2026-06-01T00:00:00')",
            (user_id, proj_id, f"k{i}"))
    await db.commit()

    plan = await (await db.execute(
        "EXPLAIN QUERY PLAN "
        "SELECT * FROM outbox_operations "
        "WHERE user_id=? AND status=? "
        "AND (next_attempt_at IS NULL OR next_attempt_at<=?) "
        "ORDER BY id LIMIT 1",
        (user_id, "pending", "2026-06-02T00:00:00"),
    )).fetchall()
    detail = " ".join(str(r[-1]) for r in plan)
    assert "USING INDEX" in detail, detail
    assert "SCAN outbox_operations" not in detail, detail
