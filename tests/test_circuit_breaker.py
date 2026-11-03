"""Circuit breaker.

When EVERY active calendar for a user has failed 3+ times in a row,
the breaker auto-pauses THAT USER's sync (the per-user
``users.sync_paused`` flag) and emails them.  It is per-user: one
user's dead calendars must never pause sync for everyone else, and
a single healthy calendar keeps a user's breaker open.
"""

from __future__ import annotations

import pytest

from app.database import get_database, get_setting
from app.jobs.sync_job import _check_circuit_breaker

pytestmark = pytest.mark.asyncio


async def _user(db, email: str) -> int:
    row = await (await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES (?, ?, 'CB') RETURNING id""",
        (email, email),
    )).fetchone()
    return int(row["id"])


async def _calendar(db, user_id: int, gid: str, *, failures: int) -> None:
    tok = await (await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', ?, ?, ?) RETURNING id""",
        (user_id, f"{gid}.tok", b"x", b"y"),
    )).fetchone()
    cal = await (await db.execute(
        """INSERT INTO client_calendars
              (user_id, oauth_token_id, google_calendar_id, display_name,
               calendar_type, is_active)
           VALUES (?, ?, ?, ?, 'client', 1) RETURNING id""",
        (user_id, int(tok["id"]), gid, gid),
    )).fetchone()
    await db.execute(
        """INSERT INTO calendar_sync_state
              (client_calendar_id, consecutive_failures)
           VALUES (?, ?)""",
        (int(cal["id"]), failures),
    )


async def _webcal(db, user_id: int, url: str, *, failures: int) -> None:
    await db.execute(
        """INSERT INTO webcal_subscriptions
              (user_id, url, is_active, consecutive_failures)
           VALUES (?, ?, 1, ?)""",
        (user_id, url, failures),
    )


async def _is_paused(db, user_id: int) -> bool:
    row = await (await db.execute(
        "SELECT sync_paused FROM users WHERE id = ?", (user_id,),
    )).fetchone()
    return bool(row and row["sync_paused"])


async def test_circuit_breaker_pauses_only_the_failing_user(
    test_db, monkeypatch,
):
    db = await get_database()
    uid = await _user(db, "cb@example.com")
    await _calendar(db, uid, "c1@g", failures=3)
    await _calendar(db, uid, "c2@g", failures=5)
    await db.commit()

    alerts: list[dict] = []

    async def fake_queue_alert(**kwargs):
        alerts.append(kwargs)

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _check_circuit_breaker()

    assert await _is_paused(db, uid)
    assert len(alerts) == 1
    assert alerts[0]["alert_type"] == "circuit_breaker"
    assert alerts[0]["user_id"] == uid
    # The GLOBAL pause switch must NOT be flipped — only this user.
    assert await get_setting("sync_paused") is None


async def test_circuit_breaker_does_not_pause_other_users(
    test_db, monkeypatch,
):
    """One user's all-failing calendars must not pause anyone else —
    the per-user-detection / global-effect bug."""
    db = await get_database()
    failing = await _user(db, "failing@example.com")
    await _calendar(db, failing, "f1@g", failures=4)
    await _calendar(db, failing, "f2@g", failures=4)
    healthy = await _user(db, "healthy@example.com")
    await _calendar(db, healthy, "h1@g", failures=0)
    await _calendar(db, healthy, "h2@g", failures=0)
    await db.commit()

    async def fake_queue_alert(**kwargs):
        pass

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _check_circuit_breaker()

    assert await _is_paused(db, failing)
    assert not await _is_paused(db, healthy), (
        "a healthy user was paused by another user's circuit breaker"
    )
    assert await get_setting("sync_paused") is None


async def test_circuit_breaker_stays_open_when_one_calendar_is_healthy(
    test_db, monkeypatch,
):
    db = await get_database()
    uid = await _user(db, "ok@example.com")
    await _calendar(db, uid, "c1@g", failures=5)   # failing
    await _calendar(db, uid, "c2@g", failures=0)   # healthy
    await db.commit()

    alerts: list[dict] = []

    async def fake_queue_alert(**kwargs):
        alerts.append(kwargs)

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _check_circuit_breaker()

    assert not await _is_paused(db, uid)
    assert alerts == []


async def test_webcal_only_user_trips_the_breaker(test_db, monkeypatch):
    """A user whose only sync sources are webcal feeds must still be
    caught — webcal feeds carry their own failure counter, and the
    breaker used to count Google calendars exclusively."""
    db = await get_database()
    uid = await _user(db, "webcal-only@example.com")
    await _webcal(db, uid, "https://feed.example/a.ics", failures=3)
    await _webcal(db, uid, "https://feed.example/b.ics", failures=7)
    await db.commit()

    alerts: list[dict] = []

    async def fake_queue_alert(**kwargs):
        alerts.append(kwargs)

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _check_circuit_breaker()

    assert await _is_paused(db, uid)
    assert len(alerts) == 1 and alerts[0]["alert_type"] == "circuit_breaker"


async def test_healthy_webcal_keeps_the_breaker_open(test_db, monkeypatch):
    """One healthy webcal feed counts as a working source — the user's
    breaker must stay open even though every Google calendar fails."""
    db = await get_database()
    uid = await _user(db, "mixed@example.com")
    await _calendar(db, uid, "c1@g", failures=9)   # failing
    await _webcal(db, uid, "https://feed.example/ok.ics", failures=0)  # healthy
    await db.commit()

    alerts: list[dict] = []

    async def fake_queue_alert(**kwargs):
        alerts.append(kwargs)

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _check_circuit_breaker()

    assert not await _is_paused(db, uid), (
        "a working webcal feed should keep the breaker open"
    )
    assert alerts == []


async def test_failing_webcal_trips_breaker_with_failing_calendars(
    test_db, monkeypatch,
):
    """Calendars and feeds are tallied together: all failing → pause."""
    db = await get_database()
    uid = await _user(db, "alldead@example.com")
    await _calendar(db, uid, "c1@g", failures=4)
    await _webcal(db, uid, "https://feed.example/dead.ics", failures=4)
    await db.commit()

    async def fake_queue_alert(**kwargs):
        pass

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    await _check_circuit_breaker()

    assert await _is_paused(db, uid)


# ---------------------------------------------------------------------------
# Transient failures, resume scoping, and self-healing.
#
# Regression: a ~5 minute DNS outage ("Unable to find the server at
# www.googleapis.com") failed every calendar 3 times, the breaker
# paused the user, and nothing ever un-paused them — sync stayed dead
# for days with only DEBUG-level evidence.
# ---------------------------------------------------------------------------
from datetime import datetime, timedelta, timezone

import httplib2

from app.jobs.sync_job import (
    CIRCUIT_BREAKER_PAUSE_REASON,
    _probe_circuit_breaker_pauses,
)
from app.ledger.reconciler import _bump_failure, is_transient_ingest_error
from app.ledger.real_google_client import GoogleApiError


async def _no_alerts(monkeypatch) -> list[dict]:
    alerts: list[dict] = []

    async def fake_queue_alert(**kwargs):
        alerts.append(kwargs)

    monkeypatch.setattr("app.alerts.email.queue_alert", fake_queue_alert)
    return alerts


async def _calendar_ids(db, user_id: int) -> list[int]:
    rows = await (await db.execute(
        "SELECT id FROM client_calendars WHERE user_id = ?", (user_id,),
    )).fetchall()
    return [int(r["id"]) for r in rows]


async def _user_row(db, user_id: int):
    return await (await db.execute(
        """SELECT sync_paused, sync_pause_reason, sync_paused_at,
                  sync_resumed_at, breaker_trips
             FROM users WHERE id = ?""",
        (user_id,),
    )).fetchone()


@pytest.mark.parametrize("error, transient", [
    (httplib2.ServerNotFoundError(
        "Unable to find the server at www.googleapis.com"), True),
    (ConnectionResetError("reset"), True),
    (TimeoutError("timed out"), True),
    (GoogleApiError(503, "backendError"), True),
    (GoogleApiError(429, "rateLimitExceeded"), True),
    (GoogleApiError(403, "rateLimitExceeded"), True),
    (GoogleApiError(404, "notFound"), False),
    (GoogleApiError(401, "authError"), False),
    (GoogleApiError(403, "forbidden"), False),
    (KeyError("items"), False),
])
async def test_transient_ingest_error_classification(error, transient):
    assert is_transient_ingest_error(error) is transient


async def test_dns_outage_does_not_trip_the_breaker(test_db, monkeypatch):
    db = await get_database()
    uid = await _user(db, "dns@example.com")
    await _calendar(db, uid, "c1@g", failures=0)
    await _calendar(db, uid, "c2@g", failures=0)
    await db.commit()
    for cid in await _calendar_ids(db, uid):
        for _ in range(5):
            await _bump_failure(
                db, client_calendar_id=cid,
                error=httplib2.ServerNotFoundError(
                    "Unable to find the server at www.googleapis.com",
                ),
            )
    alerts = await _no_alerts(monkeypatch)

    await _check_circuit_breaker()

    assert not await _is_paused(db, uid)
    assert alerts == []


async def test_real_failures_still_trip_and_tag_the_pause(test_db, monkeypatch):
    db = await get_database()
    uid = await _user(db, "dead@example.com")
    await _calendar(db, uid, "c1@g", failures=0)
    await db.commit()
    for cid in await _calendar_ids(db, uid):
        for _ in range(3):
            await _bump_failure(
                db, client_calendar_id=cid,
                error=GoogleApiError(404, "notFound"),
            )
    alerts = await _no_alerts(monkeypatch)

    await _check_circuit_breaker()

    row = await _user_row(db, uid)
    assert row["sync_paused"]
    assert row["sync_pause_reason"] == CIRCUIT_BREAKER_PAUSE_REASON
    assert row["breaker_trips"] == 1
    assert len(alerts) == 1


async def test_failures_from_before_a_resume_do_not_retrip(test_db, monkeypatch):
    """Resuming must not be undone by the counters that caused the
    pause before the resumed sync has had a chance to run."""
    from app.ledger.admin_ops import resume_sync

    db = await get_database()
    uid = await _user(db, "resumed@example.com")
    await _calendar(db, uid, "c1@g", failures=0)
    await db.commit()
    for cid in await _calendar_ids(db, uid):
        for _ in range(3):
            await _bump_failure(
                db, client_calendar_id=cid, error=GoogleApiError(404, "nf"),
            )
    await _no_alerts(monkeypatch)
    await _check_circuit_breaker()
    assert await _is_paused(db, uid)

    await resume_sync(db, user_id=uid)
    await _check_circuit_breaker()

    assert not await _is_paused(db, uid)
    req = await (await db.execute(
        "SELECT scheduled_for FROM reconcile_requests WHERE user_id = ?",
        (uid,),
    )).fetchone()
    assert req is not None, "resume must queue a reconcile immediately"

    # A failure observed AFTER the resume counts again.
    for cid in await _calendar_ids(db, uid):
        await _bump_failure(
            db, client_calendar_id=cid, error=GoogleApiError(404, "nf"),
        )
    await _check_circuit_breaker()
    assert await _is_paused(db, uid)


async def test_breaker_pause_self_heals_after_backoff(test_db, monkeypatch):
    db = await get_database()
    uid = await _user(db, "heal@example.com")
    await _calendar(db, uid, "c1@g", failures=0)
    await db.commit()
    for cid in await _calendar_ids(db, uid):
        for _ in range(3):
            await _bump_failure(
                db, client_calendar_id=cid, error=GoogleApiError(401, "auth"),
            )
    alerts = await _no_alerts(monkeypatch)
    # Simulated clock runs in the past so failures stamped by
    # _bump_failure (real time) land after the simulated resume.
    t0 = datetime.now(timezone.utc) - timedelta(hours=2)
    await _check_circuit_breaker(now=t0)
    assert await _is_paused(db, uid)

    # Before the first backoff (15 min) elapses: still paused.
    await _probe_circuit_breaker_pauses(now=t0 + timedelta(minutes=10))
    assert await _is_paused(db, uid)

    # After it: resumed, a reconcile is queued, trip count kept.
    await _probe_circuit_breaker_pauses(now=t0 + timedelta(minutes=16))
    row = await _user_row(db, uid)
    assert not row["sync_paused"]
    assert row["sync_pause_reason"] is None
    assert row["breaker_trips"] == 1
    assert await (await db.execute(
        "SELECT 1 FROM reconcile_requests WHERE user_id = ?", (uid,),
    )).fetchone()

    # The probe sync fails again → re-trip with a doubled backoff and
    # no second alert email.
    for cid in await _calendar_ids(db, uid):
        await _bump_failure(
            db, client_calendar_id=cid, error=GoogleApiError(401, "auth"),
        )
    t1 = datetime.now(timezone.utc)
    await _check_circuit_breaker(now=t1)
    row = await _user_row(db, uid)
    assert row["sync_paused"] and row["breaker_trips"] == 2
    assert len(alerts) == 1
    await _probe_circuit_breaker_pauses(now=t1 + timedelta(minutes=20))
    assert await _is_paused(db, uid), "second trip waits 30 minutes"
    await _probe_circuit_breaker_pauses(now=t1 + timedelta(minutes=31))
    assert not await _is_paused(db, uid)

    # A successful ingest resets the counter; the next health check
    # forgets the trip history.
    await db.execute(
        "UPDATE calendar_sync_state SET consecutive_failures = 0",
    )
    await db.commit()
    await _probe_circuit_breaker_pauses()
    assert (await _user_row(db, uid))["breaker_trips"] == 0


async def test_manual_pause_never_self_heals(test_db, monkeypatch):
    db = await get_database()
    uid = await _user(db, "manual@example.com")
    await _calendar(db, uid, "c1@g", failures=0)
    await db.execute(
        """UPDATE users SET sync_paused = 1, sync_pause_reason = 'user',
                  sync_paused_at = '2020-01-01T00:00:00+00:00'
            WHERE id = ?""",
        (uid,),
    )
    await db.commit()

    await _probe_circuit_breaker_pauses()

    assert await _is_paused(db, uid)
