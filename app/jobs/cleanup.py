"""Retention cleanup job — ledger-backed.

Retention policy (unchanged at the API contract level):

* Single-occurrence ledger events past ``event_retention_days``
  since their end time → CANCELLED if still active (so the outbox
  drains their Google copies), then hard-deleted once every
  projection has drained.
* Cancelled (soft-deleted) recurring series past
  ``recurring_soft_delete_days`` since cancellation → hard-deleted
  once every projection has drained.
* Old ``sync_log`` rows past ``audit_log_retention_days`` → deleted.
* Disconnected client_calendars past
  ``disconnected_calendar_retention_days`` → deleted.
* Settled outbox rows (``done`` / ``superseded`` / ``permanent_failure``)
  older than 7 days are pruned so the table doesn't grow without bound.

A ledger_event is never hard-deleted while a projection of it is
still ``present`` on Google — that would orphan the Google copy
(enforced by a DB trigger).  Expired active
events are cancelled and re-planned here; the next reconcile drains
the deletes, and a subsequent retention pass removes the row.

**Per-bucket isolation.**  Each retention bucket runs inside its own
``try``/``except`` and commits independently.  A failure in one bucket
(historically: the ``client_calendars`` delete tripping a RESTRICT FK
from ``sync_log`` / ``webhook_channels``) must NOT abort the whole pass
and strand the others — when it did, the ``outbox`` and ``sync_log``
prunes never ran and those tables grew without bound (the DB ballooned
from ~34MB to >300MB on the Pi).  Buckets are ordered cheapest-and-
unbounded-first so the highest-value prunes happen even if a later
bucket fails, and the ``client_calendars`` delete clears its blocking
FK references first.  A ``wal_checkpoint(TRUNCATE)`` at the end reclaims
the write-ahead-log high-water mark (WAL mode never shrinks the -wal
file on its own).
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from app.config import get_settings
from app.database import get_database

logger = logging.getLogger(__name__)

UTC = timezone.utc

# ---------------------------------------------------------------------------
# Timestamp formats.  FOUR string formats coexist in this database, and
# every retention cutoff below is compared against a TEXT column, so the
# cutoff must be built in (or normalised to) the format the column's
# writers actually use — otherwise the lexicographic comparison silently
# misorders rows:
#
# * ``sync_log.created_at`` — written ONLY by its CURRENT_TIMESTAMP
#   column default: ``YYYY-MM-DD HH:MM:SS`` (space separator, UTC).
#   ``' '`` sorts before ``'T'``, so a Python ``isoformat()`` cutoff
#   misorders same-day rows.  Cutoff built SQL-side with
#   ``datetime('now', '-N days')`` — same format, same clock, immune to
#   Python-side format drift (matching the admin/alerts precedent).
# * ``outbox_operations.completed_at`` — written by app/ledger/outbox.py
#   as ``datetime.now(UTC).isoformat()``:
#   ``YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00``.  An aware-isoformat cutoff
#   is in the same format and compares correctly.
# * ``ledger_events.end_at`` — RAW Google ``dateTime`` strings stored
#   verbatim by ingest, with ARBITRARY UTC offsets
#   (``2026-07-07T10:00:00+05:30``), bare ``YYYY-MM-DD`` for all-day
#   events, or ``+00:00`` isoformat from the webcal ingest.  String
#   comparison across different offsets is simply wrong, so both sides
#   are normalised in the query with SQLite's ``datetime()``, which
#   parses ISO offsets to the UTC instant (``...T10:00:00+05:30`` →
#   ``... 04:30:00``); an unparsable value yields NULL and the row is
#   retained (the safe failure mode).  Day-scale retention windows lose
#   nothing to ``datetime()``'s seconds precision.
# * ``ledger_events.cancelled_at`` / ``client_calendars.disconnected_at``
#   — mixed: aware isoformat from the app writers (ingest, admin_ops),
#   ``YYYY-MM-DD HH:MM:SS`` from the database.py startup migration
#   (disconnected_at), and legacy naive isoformat written by older
#   cleanup passes.  Normalised via ``datetime()`` the same way.
# ---------------------------------------------------------------------------


async def run_retention_cleanup() -> dict:
    """Run one retention-cleanup pass and return per-bucket counts."""
    settings = get_settings()
    db = await get_database()
    # Aware UTC: cutoffs and the ``updated_at``/``cancelled_at`` values
    # written below must match the ledger's aware-isoformat convention
    # (see the timestamp-format note above).
    now = datetime.now(UTC)

    summary = {
        "expired_events_cancelled": 0,
        "expired_events_released": 0,
        "expired_ledger_events": 0,
        "deleted_recurring_series": 0,
        "old_sync_logs": 0,
        "disconnected_calendars": 0,
        "settled_outbox_rows": 0,
        # Legacy buckets (kept in the response shape for back-compat;
        # always zero post-cutover):
        "expired_event_mappings": 0,
        "old_busy_blocks": 0,
    }

    # Cheapest, unbounded-growth prunes first (sync_log, outbox), then the
    # event/series/calendar buckets.  Each is isolated: one failing bucket
    # logs and the rest still run.  The connection is autocommit, so a
    # raise mid-bucket cannot roll back an earlier bucket.
    buckets = (
        ("old_sync_logs", _prune_old_sync_logs),
        ("settled_outbox_rows", _prune_settled_outbox),
        ("expired_single_events", _expire_single_events),
        ("deleted_recurring_series", _prune_cancelled_recurring),
        ("disconnected_calendars", _prune_disconnected_calendars),
    )
    for name, fn in buckets:
        try:
            await fn(db, now, settings, summary)
            await db.commit()
        except Exception:
            logger.exception(
                "retention cleanup bucket %r failed; continuing with the rest",
                name,
            )

    # Reclaim the WAL high-water mark.  WAL mode never shrinks the -wal
    # file on its own; without this a one-off bloat event (e.g. a write
    # storm) leaves hundreds of MB of -wal on disk forever.  Best-effort:
    # a TRUNCATE checkpoint that can't complete (a concurrent reader) just
    # returns busy without erroring.
    try:
        await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except Exception:
        logger.exception("wal_checkpoint(TRUNCATE) failed; continuing")

    logger.info(f"Retention cleanup completed: {summary}")
    try:
        await db.execute(
            """INSERT INTO sync_log (action, status, details)
               VALUES ('retention_cleanup', 'success', ?)""",
            (json.dumps(summary),),
        )
        await db.commit()
    except Exception:
        logger.exception("retention cleanup: failed to record summary row")
    return summary


# ---------------------------------------------------------------------------
# Individual retention buckets.  Each mutates ``summary`` in place and is
# called in isolation by run_retention_cleanup so a single failure cannot
# strand the others.
# ---------------------------------------------------------------------------
async def _prune_old_sync_logs(db, now, settings, summary) -> None:
    """Delete audit-log rows past ``audit_log_retention_days``.

    ``created_at`` is written only by its CURRENT_TIMESTAMP default —
    ``YYYY-MM-DD HH:MM:SS`` (space separator, UTC).  The cutoff is built
    SQL-side with ``datetime('now', '-N days')`` so it is in exactly
    that format; a Python ``isoformat()`` cutoff ('T' separator) would
    misorder same-day rows (``' ' < 'T'``).  The bare column compare
    keeps ``idx_sync_log_created`` usable.
    """
    cursor = await db.execute(
        """DELETE FROM sync_log
            WHERE created_at < datetime('now', ?)
            RETURNING id""",
        (f"-{settings.audit_log_retention_days} days",),
    )
    summary["old_sync_logs"] = len(await cursor.fetchall())


async def _prune_settled_outbox(db, now, settings, summary) -> None:
    """Delete settled outbox rows older than 7 days.

    This is the fastest-growing table (one settled row per Google
    write), so it is pruned first — even if a later bucket fails, the
    outbox does not grow without bound.

    ``completed_at`` is written by app/ledger/outbox.py as
    ``datetime.now(UTC).isoformat()`` (aware, ``...+00:00``).  ``now``
    is aware UTC, so this cutoff is in the same format and the string
    comparison is correct.
    """
    outbox_cutoff = (now - timedelta(days=7)).isoformat()
    cursor = await db.execute(
        """DELETE FROM outbox_operations
            WHERE status IN ('done', 'superseded', 'permanent_failure')
              AND completed_at IS NOT NULL
              AND completed_at < ?
            RETURNING id""",
        (outbox_cutoff,),
    )
    summary["settled_outbox_rows"] = len(await cursor.fetchall())


async def _expire_single_events(db, now, settings, summary) -> None:
    """Age out expired single events, then hard-delete genuinely-cancelled
    rows whose projections have all drained.

    Two retention modes for an active one-off event past
    ``event_retention_days`` (by end time):

    * ``release_expired_events`` True (default) — RELEASE it: flip status
      to 'released' and leave its copies frozen on main + every client.
      The planner, diff, and ingest all skip a 'released' row, so it is
      retired from sync without deleting anything — old calendar history
      is preserved.  No re-plan: the projections stay exactly as they are.
    * ``release_expired_events`` False — CANCEL it and re-plan, so the
      planner drives every projection to 'absent' and the next reconcile's
      diff drains the Google deletes (the legacy behavior).

    The final hard-delete only ever removes status='cancelled' rows whose
    projections have all drained ('present' nowhere) — genuine user
    cancellations and legacy delete-mode expiry.  'released' rows are
    never hard-deleted: deleting them would either orphan their live copy
    or (once forgotten) let a full re-sync recreate them.

    ``end_at`` holds RAW Google ``dateTime`` strings with arbitrary UTC
    offsets (e.g. ``...T10:00:00+05:30``), bare ``YYYY-MM-DD`` for
    all-day events, and ``+00:00`` isoformat from webcal ingest.  A
    plain string compare across offsets is wrong (a ``-08:00`` rendering
    of an in-window instant sorts as if it were 8 hours older), so both
    sides are normalised with SQLite's ``datetime()``, which converts
    offset-bearing values to their UTC instant; unparsable values yield
    NULL and the row is retained.
    """
    from app.ledger.planner import plan_for_ledger_event

    nowiso = now.isoformat()
    event_cutoff = (
        now - timedelta(days=settings.event_retention_days)
    ).isoformat()

    if getattr(settings, "release_expired_events", True):
        released_rows = await (await db.execute(
            """UPDATE ledger_events
                  SET status = 'released',
                      version = version + 1,
                      updated_at = ?
                WHERE is_recurring = 0
                  AND status = 'active'
                  AND end_at IS NOT NULL
                  AND datetime(end_at) < datetime(?)
                RETURNING id""",
            (nowiso, event_cutoff),
        )).fetchall()
        summary["expired_events_released"] = len(released_rows)
        # Deliberately NOT re-planned: the projections must stay frozen
        # (present) so the copies remain on the calendars.
    else:
        cancelled_rows = await (await db.execute(
            """UPDATE ledger_events
                  SET status = 'cancelled',
                      cancelled_at = COALESCE(cancelled_at, ?),
                      version = version + 1,
                      updated_at = ?
                WHERE is_recurring = 0
                  AND status = 'active'
                  AND end_at IS NOT NULL
                  AND datetime(end_at) < datetime(?)
                RETURNING id""",
            (nowiso, nowiso, event_cutoff),
        )).fetchall()
        summary["expired_events_cancelled"] = len(cancelled_rows)
        # Re-plan each freshly-cancelled row so its projections flip to
        # 'absent' and become diverged; the next reconcile drains them.
        for row in cancelled_rows:
            await plan_for_ledger_event(db, ledger_event_id=int(row["id"]))

    cursor = await db.execute(
        """DELETE FROM ledger_events
            WHERE is_recurring = 0
              AND status = 'cancelled'
              AND end_at IS NOT NULL
              AND datetime(end_at) < datetime(?)
              AND NOT EXISTS (
                  SELECT 1 FROM ledger_projections p
                   WHERE p.ledger_event_id = ledger_events.id
                     AND p.current_state = 'present')
            RETURNING id""",
        (event_cutoff,),
    )
    summary["expired_ledger_events"] = len(await cursor.fetchall())


async def _prune_cancelled_recurring(db, now, settings, summary) -> None:
    """Hard-delete cancelled recurring series past retention — only once
    every projection has drained (no Google copy left to orphan).

    ``cancelled_at`` is mixed-format: aware isoformat (``...+00:00``)
    from the ingest/admin_ops writers, plus legacy naive isoformat from
    older cleanup passes.  ``datetime()`` normalises both sides.
    """
    recurring_cutoff = (
        now - timedelta(days=settings.recurring_soft_delete_days)
    ).isoformat()
    cursor = await db.execute(
        """DELETE FROM ledger_events
            WHERE is_recurring = 1
              AND status = 'cancelled'
              AND cancelled_at IS NOT NULL
              AND datetime(cancelled_at) < datetime(?)
              AND NOT EXISTS (
                  SELECT 1 FROM ledger_projections p
                   WHERE p.ledger_event_id = ledger_events.id
                     AND p.current_state = 'present')
            RETURNING id""",
        (recurring_cutoff,),
    )
    summary["deleted_recurring_series"] = len(await cursor.fetchall())


async def _prune_disconnected_calendars(db, now, settings, summary) -> None:
    """Hard-delete disconnected client_calendars past retention.

    Only purge a calendar once nothing in the ledger still needs its
    google_calendar_id mapping: a projection still present on Google,
    permanently failed, or diverged still owes a delete the outbox
    routes via this calendar.  Purging early would strand that delete.

    The DELETE is blocked by two RESTRICT foreign keys —
    ``sync_log.calendar_id`` and ``webhook_channels.client_calendar_id``
    (the ledger's ``ledger_projections.target_calendar_id`` is NOT a
    foreign key).  This historically aborted the whole nightly pass.
    Clear those references first: NULL out the audit rows (the column is
    nullable) and drop the webhook rows — a calendar disconnected past
    retention has long-expired Google push channels (≤7-day TTL), so the
    channel rows are dead weight and safe to delete here.

    ``disconnected_at`` is mixed-format: aware isoformat (``...+00:00``)
    from app/ledger/admin_ops.py, but ``YYYY-MM-DD HH:MM:SS`` (space
    separator) from the database.py startup migration's
    CURRENT_TIMESTAMP.  ``datetime()`` normalises both sides so neither
    format misorders against the cutoff.
    """
    calendar_cutoff = (
        now - timedelta(days=settings.disconnected_calendar_retention_days)
    ).isoformat()
    eligible = await (await db.execute(
        """SELECT id FROM client_calendars
            WHERE is_active = FALSE
              AND disconnected_at IS NOT NULL
              AND datetime(disconnected_at) < datetime(?)
              AND NOT EXISTS (
                  SELECT 1 FROM ledger_projections p
                   WHERE p.target_calendar_id = client_calendars.id
                     AND (p.current_state = 'present'
                          OR p.permanently_failed = 1
                          OR p.applied_ledger_version IS NULL
                          OR p.applied_ledger_version != p.desired_ledger_version
                          OR p.applied_payload_hash != p.desired_payload_hash)
              )""",
        (calendar_cutoff,),
    )).fetchall()
    ids = [int(r["id"]) for r in eligible]
    if not ids:
        summary["disconnected_calendars"] = 0
        return

    placeholders = ",".join("?" for _ in ids)
    await db.execute(
        f"UPDATE sync_log SET calendar_id = NULL "
        f"WHERE calendar_id IN ({placeholders})",
        ids,
    )
    await db.execute(
        f"DELETE FROM webhook_channels "
        f"WHERE client_calendar_id IN ({placeholders})",
        ids,
    )
    cursor = await db.execute(
        f"DELETE FROM client_calendars WHERE id IN ({placeholders}) RETURNING id",
        ids,
    )
    summary["disconnected_calendars"] = len(await cursor.fetchall())


async def vacuum_database() -> None:
    """Run VACUUM on the database to reclaim space.

    VACUUM rewrites the whole file under an exclusive lock that can
    outlast ``busy_timeout`` on a large database.  Maintenance mode is
    held for its duration so the reconciler, webhook, and drain paths
    freeze rather than collide with it, and any reconcile pass already
    in flight is drained out first.

    VACUUM rewrites the main database file but does NOT shrink the WAL
    in WAL mode, so a ``wal_checkpoint(TRUNCATE)`` follows to reclaim the
    -wal high-water mark too.
    """
    from app.maintenance import (
        enter_maintenance,
        exit_maintenance,
        wait_for_reconcile_quiescence,
    )

    db = await get_database()
    logger.info("Running database VACUUM")
    enter_maintenance()
    try:
        await wait_for_reconcile_quiescence()
        await db.execute("VACUUM")
        try:
            await db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            logger.exception("wal_checkpoint(TRUNCATE) after VACUUM failed")
    finally:
        exit_maintenance()
    logger.info("Database VACUUM completed")
