"""Regression for the HTTP-400 "revive" loop on orphaned recurring instances.

When a recurring source is split "this and following", BusyBridge shrinks the
old managed master's RRULE and creates a newer keeper segment.  Old
exception-instance projections still derive ``<old_master>_<date>`` for dates
the bounded master no longer generates; on Google that id survives only as a
DETACHED cancelled tombstone (``status=cancelled``, no ``recurringEventId``),
and the status:confirmed revive UPDATE is rejected 400 forever.

``_do_update`` retires such an orphaned projection to absent (the keeper
segment already holds the correct copy) instead of poison-pilling it — but
ONLY for that exact signature.  A 400 on a still-live event, an attached
cancelled occurrence, a non-instance projection, or a GET that can't confirm
the tombstone must still raise so the existing poison-pill path can surface a
genuine bug.
"""

from datetime import datetime, timezone

import pytest

from app.ledger.diff import _diverged_projections
from app.ledger.outbox import _do_update

UTC = timezone.utc


class _HttpErr(Exception):
    def __init__(self, status):
        super().__init__(f"http {status}")
        self.status = status


class _FakeGoogle:
    """update_event always 400s; get_event returns a scripted dict (the
    tombstone-or-not the guard inspects), or raises if given an Exception."""

    def __init__(self, get_result):
        self._get_result = get_result
        self.update_calls = 0
        self.get_calls = 0

    async def update_event(self, cal_id, gid, body, if_match=None):
        self.update_calls += 1
        raise _HttpErr(400)

    async def get_event(self, cal_id, gid):
        self.get_calls += 1
        if isinstance(self._get_result, Exception):
            raise self._get_result
        return self._get_result


async def _seed(
    db,
    *,
    parent_canonical_uid,
    target_kind="main",
    target_calendar_id=None,
    desired_state="present_full",
    google_event_id="bbmaster_20270324T110000Z",
):
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')"
    )
    user_id = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_events (user_id, canonical_uid, source_type, "
        " source_calendar_id, parent_canonical_uid) "
        "VALUES (?, 'uid-1', 'client', NULL, ?)",
        (user_id, parent_canonical_uid),
    )
    le_id = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO ledger_projections "
        "(ledger_event_id, target_kind, target_calendar_id, desired_state, "
        " desired_ledger_version, current_state, google_event_id, google_etag, "
        " desired_payload_hash, applied_payload_hash, applied_ledger_version, "
        " permanently_failed) "
        "VALUES (?, ?, ?, ?, 5, 'errored', ?, 'etag-1', 'hash-desired', "
        " 'hash-old', 4, 0)",
        (le_id, target_kind, target_calendar_id, desired_state, google_event_id),
    )
    proj_id = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO outbox_operations "
        "(user_id, projection_id, operation, idempotency_key, "
        " ledger_version_at_enqueue, target_google_calendar_id, status, attempts) "
        "VALUES (?, ?, 'update', 'idem-1', 5, 'cal-google-id', 'in_flight', 1)",
        (user_id, proj_id),
    )
    op_id = int(cur.lastrowid)
    await db.commit()
    op = await (await db.execute(
        "SELECT * FROM outbox_operations WHERE id=?", (op_id,))).fetchone()
    return op, proj_id, user_id


async def _proj(db, proj_id):
    return await (await db.execute(
        "SELECT * FROM ledger_projections WHERE id=?", (proj_id,))).fetchone()


@pytest.mark.asyncio
async def test_400_detached_cancelled_tombstone_retires_to_absent(test_db):
    """The exact orphaned-instance signature → converge to absent, no
    poison-pill, no Google write — and the row is immediately QUIESCENT."""
    db = test_db
    op, proj_id, user_id = await _seed(
        db, parent_canonical_uid="client:6:base_R20260519T150000")
    g = _FakeGoogle({"id": "x", "status": "cancelled", "recurringEventId": None})

    result = await _do_update(
        db, g, op, "cal-google-id", {"summary": "X", "status": "confirmed"},
        now=datetime.now(UTC),
    )

    assert result == "superseded"
    assert g.update_calls == 1 and g.get_calls == 1  # one update, one confirm GET
    proj = await _proj(db, proj_id)
    assert proj["desired_state"] == "absent"
    assert proj["current_state"] == "absent"
    assert proj["google_event_id"] is None
    assert proj["permanently_failed"] == 0
    # Converged per the 'absent' convention: applied == desired (both the
    # 'absent' sentinel, equal version) so the diff will NOT re-select it.
    assert proj["desired_payload_hash"] == "absent"
    assert proj["applied_payload_hash"] == "absent"
    assert proj["applied_ledger_version"] == proj["desired_ledger_version"]
    op_row = await (await db.execute(
        "SELECT status, last_error FROM outbox_operations WHERE id=?", (op["id"],)
    )).fetchone()
    assert op_row["status"] == "superseded"
    assert op_row["last_error"] == "instance_tombstone_out_of_range"

    # Quiescence: the retired row must NOT come back as diverged work — i.e.
    # no follow-up OP_DELETE / re-enqueue. This is the property that proves
    # the loop is actually stopped (not merely relabelled).
    diverged = await _diverged_projections(db, user_id=user_id)
    assert proj_id not in {int(r["id"]) for r in diverged}


@pytest.mark.asyncio
async def test_400_retires_client_busy_instance_too(test_db):
    """The guard is target-agnostic: a client peer busy-block instance
    (present_busy) hits the same orphan signature and is retired."""
    db = test_db
    op, proj_id, _ = await _seed(
        db,
        parent_canonical_uid="client:6:base_R20260519T150000",
        target_kind="client",
        target_calendar_id=4,
        desired_state="present_busy",
        google_event_id="bbpeer_20270324T110000Z",
    )
    g = _FakeGoogle({"id": "x", "status": "cancelled", "recurringEventId": None})

    result = await _do_update(
        db, g, op, "cal-google-id", {"summary": "Busy", "status": "confirmed"},
        now=datetime.now(UTC),
    )

    assert result == "superseded"
    proj = await _proj(db, proj_id)
    assert proj["desired_state"] == "absent"
    assert proj["google_event_id"] is None


@pytest.mark.asyncio
async def test_400_on_live_event_is_not_retired(test_db):
    """A 400 whose target is a live (confirmed) event is a real bad-payload
    bug — it must raise so the poison-pill path still fires."""
    db = test_db
    op, proj_id, _ = await _seed(
        db, parent_canonical_uid="client:6:base_R20260519T150000")
    g = _FakeGoogle({"id": "x", "status": "confirmed"})

    with pytest.raises(_HttpErr):
        await _do_update(
            db, g, op, "cal-google-id", {"summary": "X", "status": "confirmed"},
            now=datetime.now(UTC),
        )
    proj = await _proj(db, proj_id)
    assert proj["desired_state"] == "present_full"  # untouched
    assert proj["google_event_id"] is not None


@pytest.mark.asyncio
async def test_400_on_attached_cancelled_occurrence_is_not_retired(test_db):
    """A cancelled occurrence that is STILL attached to a live master
    (recurringEventId set) is a different scenario — be conservative, raise."""
    db = test_db
    op, proj_id, _ = await _seed(
        db, parent_canonical_uid="client:6:base_R20260519T150000")
    g = _FakeGoogle({"id": "x", "status": "cancelled", "recurringEventId": "bbmaster"})

    with pytest.raises(_HttpErr):
        await _do_update(
            db, g, op, "cal-google-id", {"summary": "X", "status": "confirmed"},
            now=datetime.now(UTC),
        )
    proj = await _proj(db, proj_id)
    assert proj["desired_state"] == "present_full"
    assert proj["google_event_id"] is not None


@pytest.mark.asyncio
async def test_400_on_non_instance_projection_is_not_retired(test_db):
    """A non-instance (top-level) projection can't be an _R-split orphan;
    the guard must not even GET, and the 400 must raise."""
    db = test_db
    op, proj_id, _ = await _seed(db, parent_canonical_uid=None)
    g = _FakeGoogle({"id": "x", "status": "cancelled", "recurringEventId": None})

    with pytest.raises(_HttpErr):
        await _do_update(
            db, g, op, "cal-google-id", {"summary": "X", "status": "confirmed"},
            now=datetime.now(UTC),
        )
    assert g.get_calls == 0  # short-circuited before confirming
    proj = await _proj(db, proj_id)
    assert proj["desired_state"] == "present_full"


@pytest.mark.asyncio
async def test_400_when_confirming_get_fails_does_not_retire(test_db):
    """If the confirming GET cannot run (transient error), fail closed:
    the original 400 re-raises into the normal retry/poison-pill path
    rather than mis-retiring an unconfirmed projection."""
    db = test_db
    op, proj_id, _ = await _seed(
        db, parent_canonical_uid="client:6:base_R20260519T150000")
    g = _FakeGoogle(_HttpErr(503))  # GET raises

    with pytest.raises(_HttpErr):
        await _do_update(
            db, g, op, "cal-google-id", {"summary": "X", "status": "confirmed"},
            now=datetime.now(UTC),
        )
    assert g.get_calls == 1  # it tried to confirm, then failed closed
    proj = await _proj(db, proj_id)
    assert proj["desired_state"] == "present_full"
    assert proj["google_event_id"] is not None


@pytest.mark.asyncio
async def test_retire_is_durable_across_parent_replans(test_db):
    """The retire must survive the next replan of the instance row.

    Regression: the retire converged only the PROJECTION while the
    instance ledger row stayed 'active', so the next parent edit
    replanned the child back to desired=present and the whole
    UPDATE(400) -> confirming GET -> retire round repeated — per target
    calendar, on every parent change, forever (observed daily in
    production logs).  The retire now cancels the ledger row itself, so
    the planner forces ABSENT on every future replan.
    """
    from app.ledger.planner import plan_for_ledger_event

    db = test_db
    op, proj_id, user_id = await _seed(
        db, parent_canonical_uid="client:6:base_R20260519T150000")
    g = _FakeGoogle({"id": "x", "status": "cancelled", "recurringEventId": None})

    result = await _do_update(
        db, g, op, "cal-google-id", {"summary": "X", "status": "confirmed"},
        now=datetime.now(UTC),
    )
    assert result == "superseded"

    proj = await _proj(db, proj_id)
    le = await (await db.execute(
        "SELECT status, cancelled_at, version FROM ledger_events WHERE id=?",
        (proj["ledger_event_id"],),
    )).fetchone()
    assert le["status"] == "cancelled"
    assert le["cancelled_at"] is not None

    # The row is queued so sibling projections replan to absent too.
    affected = await (await db.execute(
        "SELECT ledger_event_id FROM affected_ledger_events WHERE user_id=?",
        (user_id,),
    )).fetchall()
    assert int(proj["ledger_event_id"]) in {int(r["ledger_event_id"]) for r in affected}

    # THE regression: replanning the row (what a parent edit triggers via
    # _replan_instance_children) must NOT resurrect desired=present.
    await plan_for_ledger_event(db, ledger_event_id=int(proj["ledger_event_id"]))
    proj = await _proj(db, proj_id)
    assert proj["desired_state"] == "absent"
    # And it stays quiescent — no diverged work, no new op next pass.
    diverged = await _diverged_projections(db, user_id=user_id)
    assert proj_id not in {int(r["id"]) for r in diverged}


@pytest.mark.asyncio
async def test_retire_leaves_already_cancelled_row_alone(test_db):
    """A retire on a row that is already cancelled must not bump its
    version or re-queue it (idempotent across repeated 400 rounds)."""
    db = test_db
    op, proj_id, user_id = await _seed(
        db, parent_canonical_uid="client:6:base_R20260519T150000")
    proj = await _proj(db, proj_id)
    await db.execute(
        "UPDATE ledger_events SET status='cancelled', version=7 WHERE id=?",
        (proj["ledger_event_id"],),
    )
    await db.commit()
    g = _FakeGoogle({"id": "x", "status": "cancelled", "recurringEventId": None})

    result = await _do_update(
        db, g, op, "cal-google-id", {"summary": "X", "status": "confirmed"},
        now=datetime.now(UTC),
    )
    assert result == "superseded"
    le = await (await db.execute(
        "SELECT version FROM ledger_events WHERE id=?",
        (proj["ledger_event_id"],),
    )).fetchone()
    assert le["version"] == 7  # untouched
    affected = await (await db.execute(
        "SELECT COUNT(*) n FROM affected_ledger_events WHERE user_id=?",
        (user_id,),
    )).fetchone()
    assert affected["n"] == 0
