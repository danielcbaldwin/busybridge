"""Regression tests for verified review findings in the ledger core.

Covers five fixes across ``app/ledger/diff.py``, ``app/ledger/runtime.py``
and ``app/ledger/outbox.py``:

1. diff — a projection whose ``target_calendar_id`` no longer has a
   Google-ID mapping (retention hard-deleted the ``client_calendars``
   row; projections deliberately keep the bare integer, no FK) must be
   skipped with a warning, NOT abort the entire reconcile with a
   ValueError that the scheduler then retries every tick forever.
2. runtime — a failed reconcile is released with an ESCALATING backoff
   (1m, 2m, 4m … capped), not ``retry_at=now`` (zero backoff = full
   ingest re-run every 30s tick forever).  Reset on success.
3. outbox — non-HTTP deterministic failures (malformed ``payload_json``,
   an update op whose projection never got a ``google_event_id``) hit
   the same POISON_PILL_THRESHOLD ceiling as HTTP 4xxs instead of
   retrying forever; genuine network errors stay uncapped.
4. diff — the delivered payload carries ``placement_label`` (the
   "Placement:" footer) exactly as the planner hashed it, including on
   the webcal recurring-instance re-read path (which once omitted the
   webcal joins entirely, dropping the feed prefix/footer/color).
5. diff — ``origin_writeback_pending`` is cleared on the
   applied==desired no-op path (no patch will ever run to clear it
   while the hashes match; a flag left set arms a stale-clobber
   writeback on the next source-side change).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio
UTC = timezone.utc


def _async_value(value):
    """Wrap a sync value as the awaitable ``get_database`` returns."""
    async def _coro():
        return value
    return _coro()


# ---------------------------------------------------------------------------
# 1. diff: orphaned projection target must not crash the reconcile
# ---------------------------------------------------------------------------
async def test_orphaned_projection_target_skips_instead_of_crashing():
    """A projection targeting a hard-deleted client_calendars row (no
    Google-ID mapping) converges as a no-op; the rest of the pass —
    including other projections of the same event — still runs."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    user = await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )
    # A locked (non-editable) source event: organised by someone else,
    # alice not an attendee.  Deliberate — an *editable* event's stale
    # main copy would trip the edit-on-main propagation path and mask
    # what this test is about (the orphaned busy-block target).
    ev = s.google.insert_event(s.cal("client_a"), {
        "id": "orphantarget01",
        "summary": "Team sync",
        "start": {"dateTime": "2026-03-02T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-03-02T09:30:00Z", "timeZone": "UTC"},
        "organizer": {"email": "boss@example.com"},
        "attendees": [
            {"email": "carol@example.com", "responseStatus": "accepted"},
        ],
    })
    await s.run_reconciler_until_quiescent("alice", max_passes=4)
    s.assert_event_exists("main", summary_contains="Team sync")
    s.assert_event_exists("client_b", summary="Busy")

    # Retention hard-deletes the disconnected client_b row; the busy
    # projection keeps the integer id with nothing to join against.
    db = await s.setup_db()
    client_b_id = user.client_calendar_ids.pop("client_b")
    await db.execute(
        "DELETE FROM client_calendars WHERE id = ?", (client_b_id,),
    )
    await db.commit()

    # A source edit replans the event, diverging EVERY projection —
    # including the orphaned busy block on client_b.
    s.update_event("client_a", ev["id"], summary="Team sync v2")

    # Must not raise (the old code aborted the whole reconcile here).
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    # The healthy projections still converged.
    s.assert_event_exists("main", summary_contains="Team sync v2")

    # The orphaned projection was snapped applied, so the next pass
    # does not re-select (and re-warn about) it forever.
    row = await (await db.execute(
        """SELECT p.applied_ledger_version, p.desired_ledger_version
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ? AND p.target_kind = 'client'
              AND p.target_calendar_id = ?""",
        (user.user_id, client_b_id),
    )).fetchone()
    assert row is not None
    assert row["applied_ledger_version"] == row["desired_ledger_version"], (
        "orphaned projection was not converged — it would re-diverge "
        "every reconcile pass"
    )
    await s.close()


# ---------------------------------------------------------------------------
# 2. runtime: failed reconciles back off instead of retrying every tick
# ---------------------------------------------------------------------------
async def test_reconcile_retry_delay_curve():
    from app.ledger.runtime import (
        _RECONCILE_RETRY_BASE,
        _RECONCILE_RETRY_MAX,
        _reconcile_retry_delay,
    )
    assert _reconcile_retry_delay(1) == _RECONCILE_RETRY_BASE
    assert _reconcile_retry_delay(2) == 2 * _RECONCILE_RETRY_BASE
    assert _reconcile_retry_delay(3) == 4 * _RECONCILE_RETRY_BASE
    # Capped, including for absurdly long streaks.
    assert _reconcile_retry_delay(10) == _RECONCILE_RETRY_MAX
    assert _reconcile_retry_delay(9999) == _RECONCILE_RETRY_MAX


async def test_failed_reconcile_backs_off_and_resets_on_success(monkeypatch):
    from app.ledger import runtime
    from app.ledger.triggers import enqueue_periodic

    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    uid = user.user_id

    monkeypatch.setattr(
        "app.ledger.runtime.get_database", lambda: _async_value(db),
    )
    fail = {"on": True}

    async def _fake_reconcile(user_id, **kwargs):
        if fail["on"]:
            raise RuntimeError("simulated ingest outage")
        return {"planned": 0}

    monkeypatch.setattr(
        "app.ledger.runtime.reconcile_user_by_id", _fake_reconcile,
    )
    # Process-lifetime state; isolate this test from any other.
    runtime._reconcile_failure_counts.clear()

    async def _scheduled_for():
        row = await (await db.execute(
            "SELECT scheduled_for FROM reconcile_requests WHERE user_id = ?",
            (uid,),
        )).fetchone()
        return row["scheduled_for"]

    now = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    await enqueue_periodic(db, user_id=uid, now=now)

    # First failure: retried after the BASE backoff, not immediately.
    out = await runtime.drain_all_due_users(now=now)
    assert "error" in out[uid]
    assert await _scheduled_for() == (
        now + runtime._RECONCILE_RETRY_BASE
    ).isoformat()

    # A tick inside the backoff window does nothing — no zero-backoff
    # re-run every 30s.
    assert await runtime.drain_all_due_users(
        now=now + timedelta(seconds=30),
    ) == {}

    # Second consecutive failure escalates (base * 2).
    now2 = now + runtime._RECONCILE_RETRY_BASE
    out = await runtime.drain_all_due_users(now=now2)
    assert "error" in out[uid]
    assert await _scheduled_for() == (
        now2 + 2 * runtime._RECONCILE_RETRY_BASE
    ).isoformat()

    # A clean pass resets the streak…
    fail["on"] = False
    now3 = now2 + 2 * runtime._RECONCILE_RETRY_BASE
    out = await runtime.drain_all_due_users(now=now3)
    assert "error" not in out[uid]
    assert uid not in runtime._reconcile_failure_counts

    # …so the next failure starts from the small end again.
    fail["on"] = True
    now4 = now3 + timedelta(minutes=5)
    await enqueue_periodic(db, user_id=uid, now=now4)
    out = await runtime.drain_all_due_users(now=now4)
    assert "error" in out[uid]
    assert await _scheduled_for() == (
        now4 + runtime._RECONCILE_RETRY_BASE
    ).isoformat()

    runtime._reconcile_failure_counts.clear()
    await s.close()


async def test_failure_backoff_is_capped(monkeypatch):
    from app.ledger import runtime
    from app.ledger.triggers import enqueue_periodic

    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()
    uid = user.user_id

    monkeypatch.setattr(
        "app.ledger.runtime.get_database", lambda: _async_value(db),
    )

    async def _always_fail(user_id, **kwargs):
        raise RuntimeError("persistent failure")

    monkeypatch.setattr(
        "app.ledger.runtime.reconcile_user_by_id", _always_fail,
    )
    runtime._reconcile_failure_counts.clear()
    # A long-running streak…
    runtime._reconcile_failure_counts[uid] = 50

    now = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    await enqueue_periodic(db, user_id=uid, now=now)
    await runtime.drain_all_due_users(now=now)
    row = await (await db.execute(
        "SELECT scheduled_for FROM reconcile_requests WHERE user_id = ?",
        (uid,),
    )).fetchone()
    # …retries at the cap, never further out.
    assert row["scheduled_for"] == (
        now + runtime._RECONCILE_RETRY_MAX
    ).isoformat()

    runtime._reconcile_failure_counts.clear()
    await s.close()


# ---------------------------------------------------------------------------
# 3. outbox: deterministic non-HTTP failures hit the poison-pill ceiling
# ---------------------------------------------------------------------------
async def _seed_op(db, user_id, *, operation, payload_json, google_event_id):
    """Insert a ledger event + projection + one pending outbox op."""
    ev = await (await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               summary, start_at, end_at, created_at, updated_at)
           VALUES (?, 'client:1:det', 'client', 'active', 1,
                   'Det', '2026-03-01T09:00:00Z', '2026-03-01T09:30:00Z',
                   '2026-01-01', '2026-01-01') RETURNING id""",
        (user_id,),
    )).fetchone()
    proj = await (await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, desired_state,
               desired_payload_hash, desired_ledger_version,
               current_state, google_event_id)
           VALUES (?, 'main', 'present_full', 'h', 1, 'present', ?)
           RETURNING id""",
        (int(ev["id"]), google_event_id),
    )).fetchone()
    op = await (await db.execute(
        """INSERT INTO outbox_operations
              (user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, desired_payload_hash,
               target_google_calendar_id, payload_json,
               status, attempts, next_attempt_at, created_at)
           VALUES (?, ?, ?, 'det-key', 1, 'h', 'main@cal.test', ?,
                   'pending', 0, '2026-01-01', '2026-01-01')
           RETURNING id""",
        (user_id, int(proj["id"]), operation, payload_json),
    )).fetchone()
    await db.commit()
    return int(op["id"]), int(proj["id"])


async def _drain_until_settled(s, db, user_id, op_id):
    """Drain repeatedly, advancing the clock past the retry backoff,
    until the op leaves pending/in_flight (or the loop bound trips)."""
    from app.ledger.outbox import POISON_PILL_THRESHOLD, drain_user

    t = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)
    for _ in range(POISON_PILL_THRESHOLD + 3):
        await drain_user(db, s.google, user_id=user_id, now=t)
        t += timedelta(minutes=2)  # past the 60s backoff cap
        row = await (await db.execute(
            "SELECT status, attempts, last_error FROM outbox_operations "
            "WHERE id = ?",
            (op_id,),
        )).fetchone()
        if row["status"] not in ("pending", "in_flight"):
            return row
    return row


async def test_malformed_payload_json_goes_permanent_failure():
    """A corrupt payload_json is a deterministic local failure: it must
    reach permanent_failure after the attempts ceiling, not escape
    _execute_op and retry forever."""
    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    op_id, proj_id = await _seed_op(
        db, user.user_id,
        operation="create",
        payload_json="{this is not json",
        google_event_id=None,
    )
    row = await _drain_until_settled(s, db, user.user_id, op_id)
    assert row["status"] == "permanent_failure", (
        f"malformed payload retried forever: status={row['status']!r}, "
        f"attempts={row['attempts']}, last_error={row['last_error']!r}"
    )
    pr = await (await db.execute(
        "SELECT permanently_failed FROM ledger_projections WHERE id = ?",
        (proj_id,),
    )).fetchone()
    assert pr["permanently_failed"], (
        "projection not surfaced as permanently failed"
    )
    await s.close()


async def test_update_without_google_event_id_goes_permanent_failure():
    """The 'update op has no google_event_id' ValueError is
    deterministic — same op, same failure, every retry — so it must hit
    the poison-pill ceiling like an HTTP 400 does."""
    import json as _json

    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    op_id, proj_id = await _seed_op(
        db, user.user_id,
        operation="update",
        payload_json=_json.dumps({"summary": "Det"}),
        google_event_id=None,  # the create never ran
    )
    row = await _drain_until_settled(s, db, user.user_id, op_id)
    assert row["status"] == "permanent_failure", (
        f"no-google_event_id update retried forever: "
        f"status={row['status']!r}, attempts={row['attempts']}"
    )
    assert "google_event_id" in (row["last_error"] or "")
    pr = await (await db.execute(
        "SELECT permanently_failed FROM ledger_projections WHERE id = ?",
        (proj_id,),
    )).fetchone()
    assert pr["permanently_failed"]
    await s.close()


async def test_network_errors_are_never_capped():
    """A transport failure (no HTTP status, not a ValueError) past the
    poison-pill threshold is still retried — the deterministic-failure
    ceiling must not swallow genuine transients."""
    from app.ledger.outbox import _classify_and_retry

    s = Scenario()
    s.given_calendar("main")
    user = await s.given_user("alice", main="main")
    db = await s.setup_db()

    op_id, proj_id = await _seed_op(
        db, user.user_id,
        operation="update",
        payload_json='{"summary": "x"}',
        google_event_id="gid1",
    )
    # Push the row well past the threshold.
    await db.execute(
        "UPDATE outbox_operations SET status = 'in_flight', attempts = 9 "
        "WHERE id = ?",
        (op_id,),
    )
    await db.commit()
    op = await (await db.execute(
        "SELECT * FROM outbox_operations WHERE id = ?", (op_id,),
    )).fetchone()

    outcome = await _classify_and_retry(
        db, op, ConnectionError("connection reset by peer"),
        now=datetime(2026, 6, 1, 12, 0, tzinfo=UTC),
    )
    assert outcome == "retried"
    row = await (await db.execute(
        "SELECT status FROM outbox_operations WHERE id = ?", (op_id,),
    )).fetchone()
    assert row["status"] == "pending", "network error was poison-pilled"
    await s.close()


# ---------------------------------------------------------------------------
# 4. diff: the DELIVERED payload carries the Placement footer
# ---------------------------------------------------------------------------
async def test_placement_footer_is_delivered_to_google():
    """test_webcal_placement asserts the planner-side render; this pins
    the outbox-delivery side — what the fake Google actually stores
    after a full reconcile must carry the Placement footer too (the
    diff's send body once silently dropped placement_label while the
    stamped applied hash masked the mismatch)."""
    from tests.test_webcal_placement import _fetcher, _ics, _setup_with_placement

    s, sub_id, ids = await _setup_with_placement(
        placement_target_nick="client_a",
    )
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:MLPerf review\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))

    # Delivered main copy: feed prefix in the title, Source: +
    # Placement: in the footer, colored by the placement client.
    main_ev = s.assert_event_exists("main", summary_contains="MLPerf review")
    desc = main_ev.get("description") or ""
    assert "Source: ISO Events" in desc
    assert "Placement: client_a" in desc, (
        f"delivered main copy is missing the Placement footer; "
        f"description={desc!r}"
    )
    assert main_ev.get("colorId") == "7"

    # Delivered selected-client copy: same footer.
    client_ev = s.assert_event_exists(
        "client_a", summary_contains="MLPerf review",
    )
    cdesc = client_ev.get("description") or ""
    assert "Placement: client_a" in cdesc
    assert "Source: ISO Events" in cdesc
    await s.close()


async def test_webcal_recurring_instance_delivery_carries_prefix_and_footer():
    """The recurring-INSTANCE path re-reads the projection row after
    deriving its google_event_id; that re-read once omitted the webcal
    joins, so instance copies rendered without their feed prefix,
    Placement footer, or color.  Pin the delivered instance payload."""
    from tests.test_webcal_placement import _fetcher, _ics, _setup_with_placement

    s, sub_id, ids = await _setup_with_placement(
        placement_target_nick="client_a",
    )
    body = _ics(
        # The recurring series master.
        "UID:weekly@x\n"
        "SUMMARY:Weekly\n"
        "DTSTART:20260202T090000Z\n"
        "DTEND:20260202T093000Z\n"
        "RRULE:FREQ=WEEKLY;COUNT=4",
        # A moved-occurrence override (RECURRENCE-ID) — becomes an
        # instance projection whose payload flows through the re-read.
        "UID:weekly@x\n"
        "SUMMARY:Weekly\n"
        "DTSTART:20260209T100000Z\n"
        "DTEND:20260209T103000Z\n"
        "RECURRENCE-ID:20260209T090000Z",
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    # A second pass in case the instance deferred behind its parent.
    s.advance(61)
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))

    events = s.list_events("main")
    master = next((e for e in events if e.get("recurrence")), None)
    assert master is not None, "recurring master copy missing on main"
    instance = next(
        (e for e in events if e["id"].startswith(master["id"] + "_")),
        None,
    )
    assert instance is not None, (
        f"instance override copy missing on main; "
        f"ids={[e['id'] for e in events]}"
    )
    assert (instance.get("summary") or "").startswith("ISO Events "), (
        f"instance copy lost its feed prefix: {instance.get('summary')!r}"
    )
    idesc = instance.get("description") or ""
    assert "Source: ISO Events" in idesc, (
        f"instance copy lost its Source footer; description={idesc!r}"
    )
    assert "Placement: client_a" in idesc, (
        f"instance copy lost its Placement footer; description={idesc!r}"
    )
    assert instance.get("colorId") == "7", (
        f"instance copy lost its placement color: {instance.get('colorId')!r}"
    )
    await s.close()


# ---------------------------------------------------------------------------
# 5. diff: origin_writeback_pending cleared on the converged no-op path
# ---------------------------------------------------------------------------
async def test_converged_writeback_noop_clears_pending_flag():
    """When the flag is set but the rendered writeback payload is
    unchanged (applied hash == desired hash), no patch ever runs — so
    the outbox can never clear the flag.  The diff's no-op path must
    clear it, otherwise the next genuine source-side change fires a
    stale writeback that clobbers the source."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    s.google.insert_event(s.cal("client_a"), {
        "id": "originflag001",
        "summary": "Team sync",
        "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
        "organizer": {"email": "alice@example.com"},
        "attendees": [
            {"email": "alice@example.com", "self": True,
             "responseStatus": "needsAction"},
            {"email": "bob@example.com", "responseStatus": "accepted"},
        ],
    })
    await s.run_reconciler_until_quiescent("alice", max_passes=4)

    db = await s.setup_db()
    led = await (await db.execute(
        """SELECT id, source_calendar_id FROM ledger_events
            WHERE user_id = ? AND source_type = 'client'""",
        (user.user_id,),
    )).fetchone()
    wb = await (await db.execute(
        """SELECT id, applied_payload_hash, desired_payload_hash
             FROM ledger_projections
            WHERE ledger_event_id = ? AND target_kind = 'client'
              AND target_calendar_id = ?
              AND desired_state = 'present_full_rsvp_only'""",
        (int(led["id"]), int(led["source_calendar_id"])),
    )).fetchone()
    assert wb is not None, "no origin writeback projection was planned"
    assert wb["applied_payload_hash"] == wb["desired_payload_hash"], (
        "writeback projection should be converged before the flag test"
    )

    # Simulate a main-side edit that arms the flag WITHOUT changing the
    # rendered writeback payload (e.g. an edit classified propagatable
    # whose writeback-visible fields are identical): flag + version
    # bump, then a replan.
    await db.execute(
        """UPDATE ledger_events
              SET origin_writeback_pending = 1, version = version + 1
            WHERE id = ?""",
        (int(led["id"]),),
    )
    await db.commit()
    from app.ledger.planner import plan_for_ledger_event
    await plan_for_ledger_event(db, ledger_event_id=int(led["id"]))
    await db.commit()

    await s.run_reconciler("alice")

    row = await (await db.execute(
        "SELECT origin_writeback_pending FROM ledger_events WHERE id = ?",
        (int(led["id"]),),
    )).fetchone()
    assert not row["origin_writeback_pending"], (
        "converged no-op left origin_writeback_pending set — it would "
        "fire a stale writeback on the next source-side change"
    )
    # And no patch was ever sent for it — the hashes matched throughout.
    ops = await (await db.execute(
        """SELECT COUNT(*) AS n FROM outbox_operations
            WHERE projection_id = ? AND operation = 'patch'""",
        (int(wb["id"]),),
    )).fetchone()
    assert ops["n"] == 0, "an unnecessary patch was enqueued"
    await s.close()
