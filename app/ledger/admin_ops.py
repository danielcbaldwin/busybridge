"""Administrative operations expressed as ledger mutations.

Covers color recolor and cleanup / disconnect / pause / full
re-sync.  Every admin button maps to one of these functions — no
special "two-pass cleanup" path, no prefix-sweep-versus-DB-mismatch
dance.  The ledger is the truth; the planner + outbox carry the
truth to Google.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)
UTC = timezone.utc


# ---------------------------------------------------------------------------
# Color recolor
# ---------------------------------------------------------------------------
async def recolor_client_calendar(
    db: aiosqlite.Connection,
    *,
    client_calendar_id: int,
    new_color_id: Optional[str],
) -> int:
    """Change a client calendar's color and bump every ledger row
    sourced from it so the next reconcile re-renders the colorId
    on its projections.  Returns the number of rows touched.

    Also re-enqueues every WebCal subscription PLACED on this client —
    placed-webcal main and selected-client copies are coloured by the
    placement client's color_id at render time (see webcal.md §Label,
    Footer, Color), so a recolor here must replan those rows too.
    """
    when = datetime.now(UTC).isoformat()
    await db.execute(
        "UPDATE client_calendars SET color_id = ? WHERE id = ?",
        (new_color_id, client_calendar_id),
    )
    cursor = await db.execute(
        """UPDATE ledger_events
              SET color_id = ?,
                  version = version + 1,
                  updated_at = ?
            WHERE source_type IN ('client', 'personal')
              AND source_calendar_id = ?
              AND status = 'active'""",
        (new_color_id, when, client_calendar_id),
    )
    direct_rows_touched = cursor.rowcount or 0

    # Placed-webcal pass: every webcal subscription where the placement
    # client is THIS client gets its ledger rows version-bumped so the
    # next planner pass re-fetches the JOIN (and the new color).  The
    # ledger row's own color_id is NOT touched — webcal projections
    # read color from cc_placement at render time, not from the row.
    await db.execute(
        """UPDATE ledger_events
              SET version = version + 1,
                  updated_at = ?
            WHERE source_type = 'webcal'
              AND status = 'active'
              AND source_calendar_id IN (
                SELECT id FROM webcal_subscriptions
                 WHERE placement_client_calendar_id = ?
                   AND placement_kind = 'client'
              )""",
        (when, client_calendar_id),
    )

    # Affected ledger rows: enqueue them for replan.  The source_type
    # filter matches the UPDATEs above — client/personal calendars and
    # webcal subscriptions are numbered in separate tables, so without
    # it a colliding webcal id would replan unrelated webcal rows.
    affected = await (await db.execute(
        """SELECT id, user_id FROM ledger_events
            WHERE status = 'active'
              AND (
                (source_type IN ('client', 'personal')
                  AND source_calendar_id = ?)
                OR
                (source_type = 'webcal'
                  AND source_calendar_id IN (
                    SELECT id FROM webcal_subscriptions
                     WHERE placement_client_calendar_id = ?
                       AND placement_kind = 'client'
                  ))
              )""",
        (client_calendar_id, client_calendar_id),
    )).fetchall()
    by_user: dict[int, list[int]] = {}
    for row in affected:
        by_user.setdefault(int(row["user_id"]), []).append(int(row["id"]))
    for user_id, ids in by_user.items():
        await _append_affected(db, user_id=user_id, ledger_ids=ids)
    await db.commit()
    return direct_rows_touched


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
async def cleanup_one_calendar(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    client_calendar_id: int,
) -> dict:
    """Mark a single client calendar for full removal: every event
    sourced from it goes to ``status='cancelled'``; every
    projection targeting it goes to ``desired_state='absent'``.
    The next reconcile drains the deletes.
    """
    when = datetime.now(UTC).isoformat()

    # 1. Source-side: cancel every ledger row that came from this calendar.
    #    The source_type filter is essential: client/personal calendars
    #    and webcal subscriptions are numbered in SEPARATE tables, so a
    #    webcal subscription id can equal this client_calendar_id.
    #    Without the filter the cancel would also hit unrelated webcal
    #    events that merely share the numeric id.
    await db.execute(
        """UPDATE ledger_events
              SET status = 'cancelled',
                  version = version + 1,
                  cancelled_at = ?, updated_at = ?
            WHERE user_id = ?
              AND source_type IN ('client', 'personal')
              AND source_calendar_id = ?
              AND status = 'active'""",
        (when, when, user_id, client_calendar_id),
    )

    # 2. Target-side: any projection that targets this client must
    #    go absent regardless of source — busy blocks from other
    #    calendars also need to vanish.  The divergence the diff acts
    #    on is the payload-hash change (-> 'absent'); desired_ledger_version
    #    stays pinned to the event's real version rather than a
    #    fabricated version+1, which the next planner pass would
    #    overwrite with the real value anyway.
    await db.execute(
        """UPDATE ledger_projections
              SET desired_state = 'absent',
                  desired_payload_hash = 'absent',
                  desired_ledger_version = (
                      SELECT version FROM ledger_events
                       WHERE id = ledger_projections.ledger_event_id
                  ),
                  updated_at = ?
            WHERE target_kind = 'client'
              AND target_calendar_id = ?
              AND ledger_event_id IN (
                  SELECT id FROM ledger_events WHERE user_id = ?
              )""",
        (when, client_calendar_id, user_id),
    )

    # 3. Clear sync token so resume does a full sync.
    await db.execute(
        """UPDATE calendar_sync_state
              SET sync_token = NULL,
                  last_full_sync = NULL,
                  last_incremental_sync = NULL
            WHERE client_calendar_id = ?""",
        (client_calendar_id,),
    )
    # 4. Mark every user ledger row dirty so the reconciler picks
    #    them up.
    affected = await (await db.execute(
        """SELECT id FROM ledger_events WHERE user_id = ?""",
        (user_id,),
    )).fetchall()
    await _append_affected(
        db, user_id=user_id,
        ledger_ids=[int(r["id"]) for r in affected],
    )
    await db.commit()
    return {"ledger_rows_cancelled": -1}  # caller asks the reconciler


async def cleanup_and_pause(
    db: aiosqlite.Connection,
    *,
    user_id: int,
) -> None:
    """Global cleanup: every projection for the user goes absent,
    sync is paused.  After the outbox drains, the user's calendars
    contain none of our writes.
    """
    when = datetime.now(UTC).isoformat()
    # The diff acts on the payload-hash change to 'absent';
    # desired_ledger_version stays at the event's real version
    # instead of a fabricated version+1.
    await db.execute(
        """UPDATE ledger_projections
              SET desired_state = 'absent',
                  desired_payload_hash = 'absent',
                  desired_ledger_version = (
                      SELECT version FROM ledger_events
                       WHERE id = ledger_projections.ledger_event_id
                  ),
                  updated_at = ?
            WHERE ledger_event_id IN (
                SELECT id FROM ledger_events WHERE user_id = ?
            )""",
        (when, user_id),
    )
    await db.execute(
        "UPDATE users SET sync_paused = 1 WHERE id = ?",
        (user_id,),
    )
    affected = await (await db.execute(
        "SELECT id FROM ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchall()
    await _append_affected(
        db, user_id=user_id,
        ledger_ids=[int(r["id"]) for r in affected],
    )
    await db.commit()


async def disconnect_calendar(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    client_calendar_id: int,
) -> None:
    """Cleanup + soft-delete the client_calendars row.

    If this client is the placement target of one or more WebCal
    subscriptions, additionally:

    * snapshot ``placement_client_display_name_cache`` from the current
      ``display_name`` so the alert email can name the lost target even
      after the row is deactivated or later hard-deleted;
    * enqueue every active webcal ledger row from those subscriptions
      for replan (so projections fall back to the stale-placement
      render path on the next reconcile);
    * raise a ``webcal_placement_disconnected`` alert per affected
      subscription (deduped per-subscription within 1h);
    * write one sync_log row per affected subscription describing the
      stale-placement transition.

    See webcal.md §Placement Target Lifecycle.
    """
    # Capture placement-affected subscriptions BEFORE flipping
    # client_calendars.is_active or running cleanup, so the JOIN to
    # display_name still resolves and so we can decide whether to
    # alert before any side effect has fired.
    placement_affected = await (await db.execute(
        """SELECT ws.id            AS subscription_id,
                  ws.url            AS feed_url,
                  ws.display_prefix AS feed_name,
                  cc.display_name   AS placement_target_name
             FROM webcal_subscriptions ws
             JOIN client_calendars cc
                  ON cc.id = ws.placement_client_calendar_id
            WHERE ws.placement_client_calendar_id = ?
              AND ws.placement_kind = 'client'
              AND ws.is_active = 1
              AND ws.user_id = ?""",
        (client_calendar_id, user_id),
    )).fetchall()

    await cleanup_one_calendar(
        db, user_id=user_id, client_calendar_id=client_calendar_id,
    )
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE client_calendars
              SET is_active = 0, disconnected_at = ?
            WHERE id = ?""",
        (when, client_calendar_id),
    )
    # Tear down the calendar's webhook channels: a disconnected calendar
    # is no longer ingested, so the renewal job must stop renewing its
    # webhooks (it does not filter on is_active).  Actively STOP the
    # channel on Google (best-effort) rather than just dropping the local
    # row — otherwise Google keeps POSTing to the dead channel for its
    # ~7-day TTL (the "Unknown webhook channel" storm) and the leftover
    # row blocks retention's client_calendars delete via the RESTRICT FK.
    from app.api.webhooks import stop_channels_for_user
    await stop_channels_for_user(
        db, user_id=user_id, client_calendar_id=client_calendar_id,
    )

    # Per-subscription placement transitions: snapshot the display
    # name, mark every active webcal ledger row from this sub for
    # replan, log it.  All-or-nothing across subscriptions — a
    # partial loop failure must not leave some subs replanned but
    # others not (each sub's sync_log + affected_ledger_events rows
    # must be consistent with its placement state).  Uses the
    # no-commit primitive record_affected_events so the whole loop
    # participates in one transaction.
    #
    # enqueue_periodic is deliberately OUTSIDE the BEGIN: it calls
    # _upsert_request which COMMITs internally.  The wake-up signal
    # is best-effort — the periodic reconciler will pick up the
    # affected_ledger_events rows regardless, so a missed wake-up
    # is a one-cycle delay, never a state inconsistency.
    if placement_affected:
        from app.ledger.triggers import (
            record_affected_events, enqueue_periodic,
        )
        try:
            await db.execute("BEGIN")
            for row in placement_affected:
                sub_id = int(row["subscription_id"])
                target_name = row["placement_target_name"] or ""
                await db.execute(
                    """UPDATE webcal_subscriptions
                          SET placement_client_display_name_cache = ?,
                              updated_at = ?
                        WHERE id = ?""",
                    (target_name, when, sub_id),
                )
                affected_rows = await (await db.execute(
                    """SELECT id FROM ledger_events
                        WHERE user_id = ? AND source_type = 'webcal'
                          AND source_calendar_id = ? AND status = 'active'""",
                    (user_id, sub_id),
                )).fetchall()
                ledger_ids = [int(r["id"]) for r in affected_rows]
                if ledger_ids:
                    await record_affected_events(
                        db, user_id=user_id, ledger_event_ids=ledger_ids,
                    )
                # One sync_log row per affected subscription — NOT per
                # ledger event — matches the spec's alert-fanout rule.
                await db.execute(
                    """INSERT INTO sync_log (user_id, action, status, details)
                       VALUES (?, 'webcal_placement_target_disconnected', 'warning', ?)""",
                    (
                        user_id,
                        json.dumps({
                            "subscription_id": sub_id,
                            "client_calendar_id": client_calendar_id,
                            "placement_client_display_name": target_name,
                            "feed_url": row["feed_url"],
                            "feed_name": row["feed_name"] or "",
                            "affected_ledger_count": len(ledger_ids),
                        }),
                    ),
                )
            await db.execute("COMMIT")
        except BaseException:
            await db.execute("ROLLBACK")
            raise
        # Wake the reconciler once for all affected subs.  Best-effort.
        await enqueue_periodic(db, user_id=user_id)
    else:
        await db.commit()

    # Queue alerts after the commit so a transient SMTP/alert-queue
    # failure cannot roll back the disconnect.  Each affected
    # subscription gets its own alert (deduped per-subscription
    # within 1h by queue_placement_disconnected_alert).
    if placement_affected:
        from app.alerts.email import queue_placement_disconnected_alert
        for row in placement_affected:
            try:
                await queue_placement_disconnected_alert(
                    user_id=user_id,
                    subscription_id=int(row["subscription_id"]),
                    feed_name=row["feed_name"] or "",
                    feed_url=row["feed_url"] or "",
                    placement_target_name=row["placement_target_name"] or "",
                )
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "failed to queue placement-disconnected alert for sub %s",
                    row["subscription_id"],
                )


async def full_resync(
    db: aiosqlite.Connection,
    *,
    user_id: int,
) -> None:
    """Wipe sync tokens for every calendar the user owns.

    Idempotent.  The next reconcile re-fetches everything;
    ledger upserts dedupe so no actual writes are issued unless
    content changed.  Useful for "calendars look out of sync" or
    suspected drift.
    """
    await db.execute(
        """UPDATE calendar_sync_state
              SET sync_token = NULL,
                  last_full_sync = NULL,
                  last_incremental_sync = NULL
            WHERE client_calendar_id IN (
                SELECT id FROM client_calendars WHERE user_id = ?
            )""",
        (user_id,),
    )
    await db.execute(
        """UPDATE main_calendar_sync_state
              SET sync_token = NULL,
                  last_full_sync = NULL,
                  last_incremental_sync = NULL
            WHERE user_id = ?""",
        (user_id,),
    )
    await db.commit()


async def replan_all_active_events(
    db: aiosqlite.Connection,
    *,
    user_id: int,
) -> int:
    """Queue every active source event for a fresh planner pass.

    The planner emits projections against the CURRENT set of active
    client-target calendars; when a new client calendar becomes active
    (a user connects a second bidi work calendar mid-life), previously-
    ingested events still hold their old projection set — no targets
    for the new calendar, no busy blocks written to it.  ``full_resync``
    doesn't fix this: it clears sync tokens, so ingest RE-FETCHES, but
    ingest short-circuits on unchanged content — the planner is not
    called and no new projections get created.

    Enqueueing every active event into ``affected_ledger_events``
    forces the reconciler's plan step to run each one against the
    current target set, adding projections for the new calendar (and
    dropping any that reference a since-disconnected one).

    Returns the number of events queued.  Idempotent — enqueueing an
    event already in the queue is a cheap no-op (the reconciler
    dedupes).  Callers still need to trigger a reconcile after; the
    connect flow already does this via ``enqueue_manual``.
    """
    rows = await (await db.execute(
        """SELECT id FROM ledger_events
            WHERE user_id = ?
              AND status = 'active'
              AND user_intentionally_deleted = 0""",
        (user_id,),
    )).fetchall()
    if not rows:
        return 0
    now = datetime.now(UTC).isoformat()
    for r in rows:
        await db.execute(
            "INSERT INTO affected_ledger_events (user_id, ledger_event_id, enqueued_at) VALUES (?, ?, ?)",
            (user_id, int(r["id"]), now),
        )
    await db.commit()
    return len(rows)


async def resume_sync(
    db: aiosqlite.Connection, *, user_id: int,
) -> None:
    """Un-pause a previously paused user."""
    await db.execute(
        "UPDATE users SET sync_paused = 0 WHERE id = ?",
        (user_id,),
    )
    await db.commit()


# ---------------------------------------------------------------------------
# Poison-pill recovery
# ---------------------------------------------------------------------------
async def retry_permanent_failures(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    projection_id: Optional[int] = None,
) -> int:
    """Clear the poison-pill flag on permanently-failed projections so
    the diff step re-enqueues them.

    A poison-pilled projection is excluded from the diff
    (``permanently_failed = 1``); clearing the flag is the only way
    back in.  ``applied_*`` still diverges from ``desired_*`` (the
    failed op never advanced it), so the next diff pass enqueues a
    fresh op with ``attempts = 0``.  Without an explicit
    ``projection_id`` every permanently-failed projection for the user
    is retried.  Returns the number of projections un-stuck.
    """
    when = datetime.now(UTC).isoformat()
    params: list = [user_id]
    clause = ""
    if projection_id is not None:
        clause = " AND p.id = ?"
        params.append(projection_id)
    rows = await (await db.execute(
        f"""SELECT p.id, p.ledger_event_id
              FROM ledger_projections p
              JOIN ledger_events e ON e.id = p.ledger_event_id
             WHERE e.user_id = ?
               AND p.permanently_failed = 1{clause}""",
        params,
    )).fetchall()
    if not rows:
        return 0
    proj_ids = [int(r["id"]) for r in rows]
    placeholders = ",".join("?" for _ in proj_ids)
    await db.execute(
        f"""UPDATE ledger_projections
               SET permanently_failed = 0,
                   attempts = 0,
                   next_attempt_at = NULL,
                   last_error = NULL,
                   -- Reset the id-generation ceiling episode too: a
                   -- give-up marked this projection failed BECAUSE
                   -- (generation - floor) hit the cap, so a retry that
                   -- left the floor behind would insta-fail on the
                   -- _do_create entry check and re-alert, making this
                   -- admin action a no-op loop.
                   google_id_generation_floor = google_id_generation,
                   updated_at = ?
             WHERE id IN ({placeholders})""",
        [when, *proj_ids],
    )
    await _append_affected(
        db, user_id=user_id,
        ledger_ids=[int(r["ledger_event_id"]) for r in rows],
    )
    await db.commit()
    return len(proj_ids)


# ---------------------------------------------------------------------------
# Personal all-day cleanup
# ---------------------------------------------------------------------------
async def cleanup_personal_all_day_blocks(
    db: aiosqlite.Connection,
    *,
    user_id: Optional[int] = None,
) -> int:
    """Re-plan every active all-day personal event so the next reconcile
    removes the now-suppressed busy blocks from main and every client.

    Turning ``SYNC_PERSONAL_ALL_DAY_EVENTS`` off changes the *desired*
    projection for all-day personal events to absent, but the reconciler
    only re-plans events that changed since the last pass — a static
    all-day personal event (birthday, vacation, OOO) never re-ingests, so
    its already-written busy blocks would linger.  Marking those rows
    affected makes the planner recompute them to absent; the diff then
    deletes the stale main/client copies (it diverges on the payload
    hash, so no ledger-version bump is needed).

    Scoped to one user when ``user_id`` is given; otherwise sweeps every
    user.  Idempotent — a second run finds nothing new diverged.  Returns
    the number of ledger events enqueued for replan.
    """
    params: list = []
    clause = ""
    if user_id is not None:
        clause = " AND user_id = ?"
        params.append(user_id)
    rows = await (await db.execute(
        f"""SELECT id, user_id FROM ledger_events
              WHERE source_type = 'personal'
                AND is_all_day = 1
                AND status = 'active'{clause}""",
        params,
    )).fetchall()
    by_user: dict[int, list[int]] = {}
    for row in rows:
        by_user.setdefault(int(row["user_id"]), []).append(int(row["id"]))
    for uid, ids in by_user.items():
        await _append_affected(db, user_id=uid, ledger_ids=ids)
    await db.commit()
    return sum(len(ids) for ids in by_user.values())


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
async def _append_affected(
    db: aiosqlite.Connection, *, user_id: int, ledger_ids: list[int],
) -> None:
    """Mark affected ledger events for replan AND schedule a reconcile.

    Admin ops run outside any reconcile pass, so — unlike ingest's
    ``_record_affected`` — this also upserts a ``reconcile_requests``
    row so the drain loop actually picks the user up.
    """
    if not ledger_ids:
        return
    from app.ledger.triggers import enqueue_periodic, record_affected_events
    await record_affected_events(
        db, user_id=user_id, ledger_event_ids=ledger_ids,
    )
    await enqueue_periodic(db, user_id=user_id)
