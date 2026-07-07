"""Timestamp-format correctness for the retention cleanup cutoffs.

Four timestamp string formats coexist in the database:

* ``sync_log.created_at`` — CURRENT_TIMESTAMP default:
  ``YYYY-MM-DD HH:MM:SS`` (space separator).
* ``outbox_operations.completed_at`` — aware isoformat (``...+00:00``).
* ``ledger_events.end_at`` — raw Google dateTime strings with arbitrary
  UTC offsets (or bare ``YYYY-MM-DD`` for all-day).
* ``client_calendars.disconnected_at`` — mixed: aware isoformat from
  admin_ops AND space-format from the database.py startup migration.

The old cleanup built one naive ``datetime.utcnow().isoformat()``
('T'-separator) cutoff for all of them; ``' ' < 'T'`` made same-day
space-format rows sort below the cutoff (deleted up to a day early),
and offset-bearing ``end_at`` strings compare by their LOCAL rendering
rather than the instant they denote.  Each test here places rows just
inside/outside the retention window in the column's real on-disk
format; the same-day space-format cases and the offset-bearing end_at
cases fail against the old string comparisons.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from app.config import get_settings
from app.database import get_database
from app.jobs.cleanup import run_retention_cleanup

UTC = timezone.utc

pytestmark = pytest.mark.asyncio


def _at_offset(instant: datetime, hours: float, minutes: int = 0) -> str:
    """Render an aware UTC instant as an ISO string at a given UTC offset,
    the way Google renders event dateTimes in the event's local zone."""
    tz = timezone(timedelta(hours=hours, minutes=minutes))
    return instant.astimezone(tz).isoformat()


async def _seed_user(db, email: str) -> int:
    cursor = await db.execute(
        """INSERT INTO users (email, google_user_id, display_name)
           VALUES (?, ?, 'TS')""",
        (email, email),
    )
    return int(cursor.lastrowid)


def _pin_release_mode(monkeypatch, release: bool) -> None:
    """Pin cleanup's release_expired_events flag (mirrors
    test_ledger_retention._pin_release_mode)."""
    from types import SimpleNamespace
    import app.jobs.cleanup as cleanup_mod
    real = get_settings()
    fake = SimpleNamespace(
        event_retention_days=real.event_retention_days,
        recurring_soft_delete_days=real.recurring_soft_delete_days,
        audit_log_retention_days=real.audit_log_retention_days,
        disconnected_calendar_retention_days=(
            real.disconnected_calendar_retention_days
        ),
        release_expired_events=release,
    )
    monkeypatch.setattr(cleanup_mod, "get_settings", lambda: fake)


# ---------------------------------------------------------------------------
# sync_log.created_at — CURRENT_TIMESTAMP space format
# ---------------------------------------------------------------------------
async def test_sync_log_space_format_same_day_row_survives(test_db):
    """A space-format row on the SAME DAY as the cutoff but at/after the
    cutoff time is inside the window and must be kept.

    Old code: cutoff was naive isoformat ``...T...``; since ``' ' < 'T'``
    every space-format row sharing the cutoff's date sorted BELOW the
    cutoff and was deleted up to a day early.  This test fails there.
    """
    db = await get_database()
    days = get_settings().audit_log_retention_days
    now = datetime.now(UTC)
    cutoff_day = (now - timedelta(days=days)).date()

    # End of the cutoff's calendar day: >= the cutoff instant, so inside
    # the retention window — regardless of what time of day the cleanup
    # runs.  Written in the column's real CURRENT_TIMESTAMP format.
    inside = f"{cutoff_day} 23:59:59"
    outside = (
        now - timedelta(days=days, hours=1)
    ).strftime("%Y-%m-%d %H:%M:%S")

    await db.execute(
        """INSERT INTO sync_log (action, status, created_at)
           VALUES ('ts_inside', 'success', ?)""",
        (inside,),
    )
    await db.execute(
        """INSERT INTO sync_log (action, status, created_at)
           VALUES ('ts_outside', 'success', ?)""",
        (outside,),
    )
    await db.commit()

    await run_retention_cleanup()

    rows = await (await db.execute(
        "SELECT action FROM sync_log WHERE action LIKE 'ts_%'",
    )).fetchall()
    actions = {r["action"] for r in rows}
    assert "ts_inside" in actions, (
        "same-day space-format row was deleted early (space-vs-T cutoff bug)"
    )
    assert "ts_outside" not in actions


async def test_sync_log_default_current_timestamp_rows_kept(test_db):
    """A row written through the real column default (no explicit
    created_at) is brand new and must survive the prune."""
    db = await get_database()
    await db.execute(
        "INSERT INTO sync_log (action, status) VALUES ('ts_default', 'success')",
    )
    await db.commit()

    await run_retention_cleanup()

    row = await (await db.execute(
        "SELECT id FROM sync_log WHERE action = 'ts_default'",
    )).fetchone()
    assert row is not None


# ---------------------------------------------------------------------------
# outbox_operations.completed_at — aware isoformat (+00:00)
# ---------------------------------------------------------------------------
async def test_outbox_completed_at_aware_format_boundary(test_db):
    """Rows written in the outbox's real aware-isoformat format, just
    inside/outside the fixed 7-day window."""
    db = await get_database()
    user_id = await _seed_user(db, "ts-outbox@example.com")

    cursor = await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, status, version,
               created_at, updated_at)
           VALUES (?, 'main_native:1:tsob', 'main_native', 'active', 1,
                   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id""",
        (user_id,),
    )
    ledger_id = int((await cursor.fetchone())["id"])
    cursor = await db.execute(
        """INSERT INTO ledger_projections
              (ledger_event_id, target_kind, desired_state,
               desired_payload_hash, desired_ledger_version)
           VALUES (?, 'main', 'present_full', 'h', 1) RETURNING id""",
        (ledger_id,),
    )
    proj_id = int((await cursor.fetchone())["id"])

    now = datetime.now(UTC)
    # The outbox writers use datetime.now(UTC).isoformat() — aware,
    # '+00:00' suffix.  Place one row just outside and one just inside
    # the 7-day window in exactly that format.
    outside = (now - timedelta(days=7, hours=2)).isoformat()
    inside = (now - timedelta(days=6, hours=22)).isoformat()

    for key, completed in (("ts-ob-old", outside), ("ts-ob-new", inside)):
        await db.execute(
            """INSERT INTO outbox_operations
                  (user_id, projection_id, operation, idempotency_key,
                   ledger_version_at_enqueue, target_google_calendar_id,
                   status, completed_at, created_at)
               VALUES (?, ?, 'create', ?, 1, 'main', 'done', ?, ?)""",
            (user_id, proj_id, key, completed, completed),
        )
    await db.commit()

    await run_retention_cleanup()

    rows = await (await db.execute(
        "SELECT idempotency_key FROM outbox_operations WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    keys = {r["idempotency_key"] for r in rows}
    assert "ts-ob-old" not in keys
    assert "ts-ob-new" in keys


# ---------------------------------------------------------------------------
# ledger_events.end_at — raw Google dateTime strings with arbitrary offsets
# ---------------------------------------------------------------------------
async def _insert_single_event(
    db, user_id: int, uid: str, end_at: str, status: str = "active",
) -> int:
    cursor = await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid, source_type, is_recurring,
               start_at, end_at, status, version, created_at, updated_at)
           VALUES (?, ?, 'main_native', 0, ?, ?, ?, 1,
                   CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING id""",
        (user_id, uid, end_at, end_at, status),
    )
    return int((await cursor.fetchone())["id"])


async def test_end_at_offset_rendering_is_compared_as_an_instant(
    test_db, monkeypatch,
):
    """Google stores end_at verbatim in the event's local offset.  The
    retention decision must be about the INSTANT, not the local
    rendering.

    Old code compared the raw string against a naive-UTC cutoff:

    * an in-window instant rendered at ``-08:00`` reads 8 hours "older"
      → released a day early (this row must stay active);
    * an out-of-window instant rendered at ``+05:30`` reads 5.5 hours
      "newer" → wrongly kept (this row must be released).

    Both assertions fail against the old lexicographic comparison.
    """
    _pin_release_mode(monkeypatch, True)
    db = await get_database()
    user_id = await _seed_user(db, "ts-endat@example.com")
    retention = get_settings().event_retention_days
    now = datetime.now(UTC)

    inside_instant = now - timedelta(days=retention) + timedelta(hours=2)
    outside_instant = now - timedelta(days=retention) - timedelta(hours=2)

    await _insert_single_event(
        db, user_id, "main_native:1:neg-offset-inside",
        _at_offset(inside_instant, -8),          # e.g. ...T04:00:00-08:00
    )
    await _insert_single_event(
        db, user_id, "main_native:1:pos-offset-outside",
        _at_offset(outside_instant, 5, 30),       # e.g. ...T15:30:00+05:30
    )
    await db.commit()

    await run_retention_cleanup()

    rows = await (await db.execute(
        "SELECT canonical_uid, status FROM ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    status = {r["canonical_uid"]: r["status"] for r in rows}
    assert status["main_native:1:neg-offset-inside"] == "active", (
        "in-window event released early: -08:00 rendering compared as a "
        "string instead of an instant"
    )
    assert status["main_native:1:pos-offset-outside"] == "released", (
        "expired event kept: +05:30 rendering compared as a string "
        "instead of an instant"
    )


async def test_end_at_offset_hard_delete_of_drained_cancelled_rows(test_db):
    """The hard-delete of drained cancelled singles uses the same
    normalised comparison: an out-of-window ``+05:30`` rendering is
    deleted (old code kept it), an in-window ``-08:00`` rendering is
    kept (old code deleted it)."""
    db = await get_database()
    user_id = await _seed_user(db, "ts-endat-del@example.com")
    retention = get_settings().event_retention_days
    now = datetime.now(UTC)

    await _insert_single_event(
        db, user_id, "main_native:1:del-outside",
        _at_offset(now - timedelta(days=retention, hours=2), 5, 30),
        status="cancelled",
    )
    await _insert_single_event(
        db, user_id, "main_native:1:del-inside",
        _at_offset(now - timedelta(days=retention) + timedelta(hours=2), -8),
        status="cancelled",
    )
    await db.commit()

    await run_retention_cleanup()

    rows = await (await db.execute(
        "SELECT canonical_uid FROM ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    remaining = {r["canonical_uid"] for r in rows}
    assert "main_native:1:del-outside" not in remaining
    assert "main_native:1:del-inside" in remaining


async def test_end_at_all_day_and_unparsable_values(test_db):
    """Bare ``YYYY-MM-DD`` all-day end_at values are handled at day
    precision; an unparsable end_at is retained (datetime() yields NULL
    → not matched) rather than mis-deleted."""
    db = await get_database()
    user_id = await _seed_user(db, "ts-allday@example.com")
    retention = get_settings().event_retention_days
    now = datetime.now(UTC)

    old_day = (now - timedelta(days=retention + 2)).date().isoformat()
    recent_day = (now - timedelta(days=1)).date().isoformat()

    await _insert_single_event(
        db, user_id, "main_native:1:allday-old", old_day, status="cancelled",
    )
    await _insert_single_event(
        db, user_id, "main_native:1:allday-new", recent_day, status="cancelled",
    )
    await _insert_single_event(
        db, user_id, "main_native:1:garbage", "not-a-timestamp",
        status="cancelled",
    )
    await db.commit()

    await run_retention_cleanup()

    rows = await (await db.execute(
        "SELECT canonical_uid FROM ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    remaining = {r["canonical_uid"] for r in rows}
    assert "main_native:1:allday-old" not in remaining
    assert "main_native:1:allday-new" in remaining
    assert "main_native:1:garbage" in remaining


# ---------------------------------------------------------------------------
# ledger_events.cancelled_at — aware isoformat (plus legacy naive)
# ---------------------------------------------------------------------------
async def test_recurring_cancelled_at_boundary_in_both_formats(test_db):
    """Cancelled recurring series just inside/outside
    ``recurring_soft_delete_days`` in the writers' aware format, plus a
    legacy naive-isoformat row (written by pre-fix cleanup passes)."""
    db = await get_database()
    user_id = await _seed_user(db, "ts-rec@example.com")
    days = get_settings().recurring_soft_delete_days
    now = datetime.now(UTC)

    cases = (
        # (uid, cancelled_at, expect_kept)
        ("rec-aware-old", (now - timedelta(days=days, hours=2)).isoformat(),
         False),
        ("rec-aware-new",
         (now - timedelta(days=days) + timedelta(hours=2)).isoformat(), True),
        # Legacy naive-UTC isoformat (old cleanup's own writes).
        ("rec-naive-old",
         (now - timedelta(days=days, hours=2)).replace(tzinfo=None)
         .isoformat(), False),
    )
    for uid, cancelled_at, _ in cases:
        await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid, source_type, is_recurring,
                   status, cancelled_at, version, created_at, updated_at)
               VALUES (?, ?, 'main_native', 1, 'cancelled', ?, 1,
                       CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)""",
            (user_id, f"main_native:1:{uid}", cancelled_at),
        )
    await db.commit()

    await run_retention_cleanup()

    rows = await (await db.execute(
        "SELECT canonical_uid FROM ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    remaining = {r["canonical_uid"] for r in rows}
    for uid, _, expect_kept in cases:
        assert (f"main_native:1:{uid}" in remaining) == expect_kept, uid


# ---------------------------------------------------------------------------
# client_calendars.disconnected_at — mixed aware isoformat + space format
# ---------------------------------------------------------------------------
async def test_disconnected_at_mixed_formats(test_db):
    """disconnected_at has two real writers: admin_ops (aware isoformat)
    and the database.py startup migration (CURRENT_TIMESTAMP space
    format).  A space-format row on the cutoff's own day but at/after
    the cutoff time must be kept — old code deleted it (space-vs-T);
    old rows in either format must be pruned."""
    db = await get_database()
    user_id = await _seed_user(db, "ts-cal@example.com")
    days = get_settings().disconnected_calendar_retention_days
    now = datetime.now(UTC)

    cursor = await db.execute(
        """INSERT INTO oauth_tokens
              (user_id, account_type, google_account_email,
               access_token_encrypted, refresh_token_encrypted)
           VALUES (?, 'client', 'ts@example.com', x'00', x'00')
           RETURNING id""",
        (user_id,),
    )
    token_id = int((await cursor.fetchone())["id"])

    cutoff_day = (now - timedelta(days=days)).date()
    cases = (
        # (google_calendar_id, disconnected_at, expect_kept)
        # Same-day space-format, at/after the cutoff instant → inside
        # the window.  Old code: ' ' < 'T' → deleted early.
        ("sameday-space@cal", f"{cutoff_day} 23:59:59", True),
        # Space-format, genuinely past retention → pruned.
        ("old-space@cal",
         (now - timedelta(days=days, hours=1)).strftime("%Y-%m-%d %H:%M:%S"),
         False),
        # Aware isoformat (admin_ops format), past retention → pruned.
        ("old-aware@cal", (now - timedelta(days=days, hours=1)).isoformat(),
         False),
        # Aware isoformat, inside the window → kept.
        ("new-aware@cal", (now - timedelta(days=1)).isoformat(), True),
    )
    for cal_id, disconnected_at, _ in cases:
        await db.execute(
            """INSERT INTO client_calendars
                  (user_id, oauth_token_id, google_calendar_id,
                   display_name, is_active, disconnected_at)
               VALUES (?, ?, ?, ?, 0, ?)""",
            (user_id, token_id, cal_id, cal_id, disconnected_at),
        )
    await db.commit()

    await run_retention_cleanup()

    rows = await (await db.execute(
        "SELECT google_calendar_id FROM client_calendars WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    remaining = {r["google_calendar_id"] for r in rows}
    for cal_id, _, expect_kept in cases:
        assert (cal_id in remaining) == expect_kept, cal_id
