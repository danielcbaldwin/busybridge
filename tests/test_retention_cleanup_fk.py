"""Regression for the pre-cutover retention-cleanup blocker.

The nightly cleanup aborted every night on a FOREIGN KEY violation:
``DELETE FROM client_calendars`` is blocked by RESTRICT FKs from
``sync_log.calendar_id`` and ``webhook_channels.client_calendar_id``,
and the old guard only checked ``ledger_projections``.  Because all
buckets ran as one pass, the abort stranded the outbox/sync_log prunes
and the DB grew without bound.

These tests assert (1) a disconnected calendar with orphaned sync_log /
webhook_channels references is now deleted cleanly (refs cleared first),
and (2) a failure in one bucket no longer strands the others.
"""

import pytest

import app.jobs.cleanup as cleanup
from app.jobs.cleanup import run_retention_cleanup


async def _seed_disconnected_calendar_with_refs(db):
    cur = await db.execute(
        "INSERT INTO users (email, google_user_id) VALUES ('u@x.com', 'g1')"
    )
    user_id = int(cur.lastrowid)
    cur = await db.execute(
        "INSERT INTO oauth_tokens "
        "(user_id, account_type, google_account_email, "
        " access_token_encrypted, refresh_token_encrypted) "
        "VALUES (?, 'client', 'u@x.com', ?, ?)",
        (user_id, b"x", b"y"),
    )
    tok_id = int(cur.lastrowid)
    # Disconnected long ago (past the 30-day retention window).
    cur = await db.execute(
        "INSERT INTO client_calendars "
        "(user_id, oauth_token_id, google_calendar_id, display_name, "
        " is_active, disconnected_at) "
        "VALUES (?, ?, 'cal-google-id', 'Old Client', 0, '2000-01-01T00:00:00')",
        (user_id, tok_id),
    )
    ccid = int(cur.lastrowid)
    # A RECENT audit row referencing the calendar — survives the
    # audit-log prune, so it would block the calendar DELETE unless the
    # cleanup clears it first.
    await db.execute(
        "INSERT INTO sync_log (user_id, calendar_id, action, status, "
        " created_at) VALUES (?, ?, 'sync', 'success', "
        " strftime('%Y-%m-%dT%H:%M:%S','now'))",
        (user_id, ccid),
    )
    # A stale webhook channel still referencing the calendar.
    await db.execute(
        "INSERT INTO webhook_channels "
        "(user_id, calendar_type, client_calendar_id, channel_id, "
        " resource_id, expiration) "
        "VALUES (?, 'client', ?, 'chan-1', 'res-1', '2000-01-08T00:00:00')",
        (user_id, ccid),
    )
    await db.commit()
    return user_id, ccid


@pytest.mark.asyncio
async def test_disconnected_calendar_with_orphan_refs_is_deleted(test_db):
    db = test_db
    _user_id, ccid = await _seed_disconnected_calendar_with_refs(db)

    summary = await run_retention_cleanup()  # must not raise

    assert summary["disconnected_calendars"] == 1
    cal = await (await db.execute(
        "SELECT id FROM client_calendars WHERE id=?", (ccid,))).fetchone()
    assert cal is None  # calendar deleted
    wh = await (await db.execute(
        "SELECT id FROM webhook_channels WHERE client_calendar_id=?", (ccid,)
    )).fetchone()
    assert wh is None  # stale channel row removed
    # The recent audit row survived but its dangling FK was NULLed.
    orphan = await (await db.execute(
        "SELECT calendar_id FROM sync_log WHERE action='sync'")).fetchone()
    assert orphan is not None and orphan["calendar_id"] is None


@pytest.mark.asyncio
async def test_one_bucket_failure_does_not_strand_the_rest(test_db, monkeypatch):
    db = test_db
    _user_id, ccid = await _seed_disconnected_calendar_with_refs(db)

    async def boom(*a, **k):
        raise RuntimeError("simulated bucket failure")

    # Fail an earlier bucket; the disconnected-calendar bucket runs after
    # it and must still complete.
    monkeypatch.setattr(cleanup, "_prune_cancelled_recurring", boom)

    summary = await run_retention_cleanup()  # must not raise

    assert summary["disconnected_calendars"] == 1
    cal = await (await db.execute(
        "SELECT id FROM client_calendars WHERE id=?", (ccid,))).fetchone()
    assert cal is None
