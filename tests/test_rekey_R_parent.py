"""`_R` "this-and-following" reschedule re-keying — all source types.

When a recurring series is rescheduled via Google's "this and
following" UI, Google creates a new event whose id is
``<base>_R<timestamp>`` (REWRITE_PLAN.md §8).  Ingest must re-key
the EXISTING series ledger row to the new id rather than orphan it
and create a duplicate.

``_try_rekey_R_parent`` was originally client-only; it is now
source-neutral and called from the client, personal, and native-main
ingest paths.  These tests exercise the function directly for each
source type — including the ``source_calendar_id IS NULL`` path that
native-main events take.
"""

from __future__ import annotations

import pytest

from app.ledger.identity import (
    canonical_uid_client,
    canonical_uid_main_native,
    canonical_uid_personal,
)
from app.ledger.ingest.client import _try_rekey_R_parent
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


async def _insert_series(
    db, *, user_id, canonical_uid, source_type, source_calendar_id,
    source_event_id,
):
    """Insert an active recurring series ledger row, return its id."""
    cur = await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, source_calendar_id,
               source_event_id, status, is_recurring, version,
               created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, 'active', 1, 1,
                   '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')""",
        (user_id, canonical_uid, source_type, source_calendar_id,
         source_event_id),
    )
    await db.commit()
    return int(cur.lastrowid)


async def _row(db, ledger_id):
    return await (await db.execute(
        "SELECT canonical_uid, source_event_id FROM ledger_events WHERE id = ?",
        (ledger_id,),
    )).fetchone()


async def test_rekey_R_parent_personal():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("personal_a")
    user = await s.given_user(
        "alice", main="main", personals=["personal_a"],
    )
    db = await s.setup_db()
    pcid = user.personal_calendar_ids["personal_a"]
    base = "persrec00001"
    lid = await _insert_series(
        db, user_id=user.user_id,
        canonical_uid=canonical_uid_personal(pcid, base),
        source_type="personal", source_calendar_id=pcid,
        source_event_id=base,
    )

    new_id = f"{base}_R20260216T090000Z"
    rekeyed = await _try_rekey_R_parent(
        db,
        user_id=user.user_id,
        source_type="personal",
        source_calendar_id=pcid,
        new_event_id=new_id,
        canonical_for=lambda eid: canonical_uid_personal(pcid, eid),
    )
    # The SAME ledger row is re-keyed — not a fresh one.
    assert rekeyed == lid
    row = await _row(db, lid)
    assert row["source_event_id"] == new_id
    assert row["canonical_uid"] == canonical_uid_personal(pcid, new_id)
    await s.close()


async def test_rekey_R_parent_main_native():
    """Native-main series carry ``source_calendar_id IS NULL`` — the
    re-key lookup must match on that via COALESCE, not skip it."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    base = "mainrec00001"
    lid = await _insert_series(
        db, user_id=user.user_id,
        canonical_uid=canonical_uid_main_native(user.user_id, base),
        source_type="main_native", source_calendar_id=None,
        source_event_id=base,
    )

    new_id = f"{base}_R20260216T090000Z"
    rekeyed = await _try_rekey_R_parent(
        db,
        user_id=user.user_id,
        source_type="main_native",
        source_calendar_id=None,
        new_event_id=new_id,
        canonical_for=lambda eid: canonical_uid_main_native(user.user_id, eid),
    )
    assert rekeyed == lid
    row = await _row(db, lid)
    assert row["source_event_id"] == new_id
    assert row["canonical_uid"] == canonical_uid_main_native(user.user_id, new_id)
    await s.close()


async def test_rekey_R_parent_client_regression():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    db = await s.setup_db()
    ccid = user.client_calendar_ids["client_a"]
    base = "clirec000001"
    lid = await _insert_series(
        db, user_id=user.user_id,
        canonical_uid=canonical_uid_client(ccid, base),
        source_type="client", source_calendar_id=ccid,
        source_event_id=base,
    )

    new_id = f"{base}_R20260216T090000Z"
    rekeyed = await _try_rekey_R_parent(
        db,
        user_id=user.user_id,
        source_type="client",
        source_calendar_id=ccid,
        new_event_id=new_id,
        canonical_for=lambda eid: canonical_uid_client(ccid, eid),
    )
    assert rekeyed == lid
    row = await _row(db, lid)
    assert row["source_event_id"] == new_id
    await s.close()


async def test_rekey_R_parent_reparents_instance_rows():
    """A re-key must move modified-instance rows onto the new parent
    canonical (REWRITE_PLAN.md §8) — otherwise the diff's parent
    lookup resolves to nothing and the instance is orphaned."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    db = await s.setup_db()
    ccid = user.client_calendar_ids["client_a"]
    base = "clirec000099"
    parent_canonical = canonical_uid_client(ccid, base)
    await _insert_series(
        db, user_id=user.user_id,
        canonical_uid=parent_canonical, source_type="client",
        source_calendar_id=ccid, source_event_id=base,
    )
    # A modified-instance ledger row attached to that parent.
    inst_canonical = f"{parent_canonical}:inst:2026-02-09T09:00:00Z"
    cur = await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, parent_canonical_uid,
               source_type, source_calendar_id, source_event_id,
               recurrence_instance_original_start, status, is_recurring,
               version, created_at, updated_at)
           VALUES (?, ?, ?, 'client', ?, ?, '2026-02-09T09:00:00Z',
                   'active', 0, 1,
                   '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')""",
        (user.user_id, inst_canonical, parent_canonical, ccid,
         f"{base}_20260209T090000Z"),
    )
    inst_id = int(cur.lastrowid)
    await db.commit()

    new_id = f"{base}_R20260216T090000Z"
    await _try_rekey_R_parent(
        db,
        user_id=user.user_id,
        source_type="client",
        source_calendar_id=ccid,
        new_event_id=new_id,
        canonical_for=lambda eid: canonical_uid_client(ccid, eid),
    )

    inst = await (await db.execute(
        "SELECT parent_canonical_uid FROM ledger_events WHERE id = ?",
        (inst_id,),
    )).fetchone()
    assert inst["parent_canonical_uid"] == canonical_uid_client(ccid, new_id)
    await s.close()


async def test_rekey_R_parent_no_match_returns_none():
    """No existing series at the base id → no re-key, returns None."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("personal_a")
    user = await s.given_user(
        "alice", main="main", personals=["personal_a"],
    )
    db = await s.setup_db()
    pcid = user.personal_calendar_ids["personal_a"]

    rekeyed = await _try_rekey_R_parent(
        db,
        user_id=user.user_id,
        source_type="personal",
        source_calendar_id=pcid,
        new_event_id="nonesuch_R20260216T090000Z",
        canonical_for=lambda eid: canonical_uid_personal(pcid, eid),
    )
    assert rekeyed is None
    await s.close()


async def test_rekey_R_parent_does_not_cross_source_types():
    """A personal series must not be re-keyed by a client `_R` event
    that happens to share the base id."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("personal_a")
    user = await s.given_user(
        "alice", main="main", personals=["personal_a"],
    )
    db = await s.setup_db()
    pcid = user.personal_calendar_ids["personal_a"]
    base = "shared0000001"
    lid = await _insert_series(
        db, user_id=user.user_id,
        canonical_uid=canonical_uid_personal(pcid, base),
        source_type="personal", source_calendar_id=pcid,
        source_event_id=base,
    )

    # A client re-key attempt with the same base id and a client
    # calendar id must NOT touch the personal row.
    rekeyed = await _try_rekey_R_parent(
        db,
        user_id=user.user_id,
        source_type="client",
        source_calendar_id=999,
        new_event_id=f"{base}_R20260216T090000Z",
        canonical_for=lambda eid: canonical_uid_client(999, eid),
    )
    assert rekeyed is None
    row = await _row(db, lid)
    assert row["source_event_id"] == base  # untouched
    await s.close()
