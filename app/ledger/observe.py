"""Observation audit: read Google back and verify converged projections.

Convergence in this system is certified by internal bookkeeping only —
``applied_ledger_version`` / ``applied_payload_hash`` are stamped from
our own write results, and several paths deliberately define a 404 as
success (deletes, origin writebacks).  Nothing ever *reads Google back*
to confirm the stamps match reality, so a projection whose write lied
(a delete that 404'd against a wrong id, an update that landed on the
wrong occurrence, a copy Google lost) stays "converged" — and therefore
silently wrong — forever.

This module is the backstop: a slow, sampled pass that GETs the actual
Google state for N converged projections per cycle and, on divergence,
marks the projection dirty using the codebase's established
explicit-unconverge signals (null the ``applied_*`` stamps — the same
signal client ingest's drift revert, main ingest's
``_mark_main_drift_reverted``, and the outbox 404 handler use), so the
next reconcile re-asserts canonical state.  Every "silently wrong
forever" class becomes "wrong for at most one audit sweep".

Design constraints, in rough order of importance:

* **Never amplify a false positive into churn.**  The planner skips
  content-identical replans, so a nulled stamp unconditionally re-sends
  one write.  Divergence is therefore only ever concluded from signals
  that round-trip exactly: existence, ``status``, and the event
  ``etag`` versus the etag stored from our own last write.  No
  payload-field comparison — field normalisation differences between
  what we send and what Google returns would dirty rows every cycle.
* **Only converged, quiescent rows are sampled.**  A diverged row is
  already scheduled for correction; auditing it is redundant and racy.
  Rows with pending outbox work, permanently-failed rows (operator
  surface), released events (frozen on purpose), and rows updated
  within the settle window (Google read replicas lag writes) are all
  skipped.
* **Instance projections need special care.**  ``events.get`` on a
  derived ``<parent>_<stamp>`` id *synthesises* a live occurrence from
  the parent whenever the slot is in range — so "the ledger says absent
  but Google returned a live event" is NOT evidence of a phantom for an
  instance (deleting it would cancel a live occurrence of our own
  series).  Absent-direction instance verification runs only through
  the parent's ``events.instances`` scan, gated on the instance ledger
  row being ``cancelled`` (source truth says the occurrence must not
  exist).
* **Observation beats derivation** (the residual wrong-derived-id
  class): derived instance ids
  (:func:`app.ledger.identity.derive_instance_google_event_id`) are
  computed and never confirmed, while ``events.instances`` returns the
  *real* ids.  When a sampled recurring parent checks out live, its
  instances are listed and each observed id is recorded onto the
  matching instance projection — the diff only derives when
  ``google_event_id`` is NULL, so a recorded observation wins and
  derivation remains the fallback.

Writeback safety: for MAIN-target copies the stored etag is left
STALE when content drift is detected (mirroring
``_mark_main_drift_reverted``), so the corrective update 412s, is
superseded, and re-runs on the next pass — after main ingest has had a
full cycle to classify any un-ingested user edit (RSVP propagation)
first.  Client-target copies carry no user intent, so their etag is
refreshed for a direct heal (mirroring client ingest's
``_maybe_revert_client_drift``).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiosqlite

from app.ledger.async_google import as_async_google
from app.ledger.google_client import GoogleClient
from app.ledger.identity import canonical_uid_for_instance
from app.ledger.ingest.client import _instance_original_start
from app.ledger.triggers import enqueue_periodic

logger = logging.getLogger(__name__)
UTC = timezone.utc

#: Skip rows whose projection changed within this window: our own write
#: may not be visible to a read replica yet (the same lag class as the
#: create-race the content audit exists for).  Tests pass 0 to disable.
DEFAULT_MIN_QUIET_SECONDS = 15 * 60

#: At most this many ``events.instances`` scans per user per cycle.
#: Each scan is 1+ paged API calls; sampled recurring parents beyond
#: the cap simply get their scan on a later rotation.
MAX_INSTANCE_SCANS_PER_CYCLE = 20

#: Follow at most this many pages per instances scan (2500 items each).
#: The scan acts only on POSITIVE observations, so truncating a
#: pathologically long series is safe — unseen occurrences are simply
#: not corrected this cycle.
MAX_INSTANCE_PAGES = 4


async def observe_user(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    main_google_calendar_id: str,
    google_calendar_id_for: dict[int, str],
    sample_size: int = 100,
    min_quiet_seconds: int = DEFAULT_MIN_QUIET_SECONDS,
    now: Optional[datetime] = None,
) -> dict:
    """Verify up to ``sample_size`` converged projections against live
    Google state; mark divergent ones dirty for the next reconcile.

    Rotation: every inspected row is stamped ``last_observed_at`` and
    candidates are taken oldest-observation-first, so the whole
    converged set is swept in ``ceil(rows / sample_size)`` cycles.

    Returns a counters dict.  ``divergent`` counts projections marked
    dirty this pass; the per-class counters break that down.  When
    anything was marked, a reconcile request is enqueued so the heal
    lands on the next drain tick rather than the next periodic sweep.
    """
    google = as_async_google(google)
    now = now or datetime.now(UTC)
    counters: dict[str, Any] = {
        "checked": 0,
        "consistent": 0,
        "divergent": 0,
        "missing_reset": 0,          # present-desired copy gone/cancelled
        "drift_marked": 0,           # live copy, etag differs
        "phantom_marked": 0,         # absent-desired copy still live
        "instance_ids_corrected": 0,  # observed id recorded over stored
        "instance_revive_marked": 0,  # observed cancelled, desired present
        "instance_delete_marked": 0,  # observed live, ledger row cancelled
        "tampered_unmatched": 0,     # override with no ledger row (visibility only)
        "instance_scans": 0,
        "instance_scans_deferred": 0,
        "unresolved_target": 0,
        "errors": 0,
    }

    rows = await _sample_converged(
        db,
        user_id=user_id,
        sample_size=sample_size,
        min_quiet_seconds=min_quiet_seconds,
        now=now,
    )

    marked = 0
    # (target_cal, parent_gid) pairs scanned this cycle, so several
    # sampled children of one parent don't re-list the same series.
    scanned: set[tuple[str, str]] = set()

    for row in rows:
        target_cal = _resolve_target(
            row, main_google_calendar_id, google_calendar_id_for,
        )
        await _stamp_observed(db, int(row["id"]), now)
        if target_cal is None:
            # No Google-ID mapping (calendar row hard-deleted after a
            # disconnect).  The diff converges these as no-ops; there
            # is nothing left to observe.
            counters["unresolved_target"] += 1
            continue

        is_instance = bool(row["parent_canonical_uid"])
        try:
            if row["current_state"] == "present":
                marked += await _check_present(db, google, row, target_cal, counters)
            elif not is_instance:
                marked += await _check_absent(db, google, row, target_cal, counters)
            else:
                # Absent-direction instance rows are verified via the
                # parent scan only (GET synthesises live occurrences).
                continue
            counters["checked"] += 1
        except Exception as e:
            counters["errors"] += 1
            logger.warning(
                "observation audit: projection %s GET %s on %s errored: %s",
                row["id"], row["google_event_id"], target_cal, e,
            )
            continue

        # Observation of instance ids (the wrong-derived-id killer):
        # a recurring parent that checked out live gets its instances
        # listed and every observed id recorded onto the matching
        # instance projection.
        if (
            row["recurrence_rule_json"]
            and not is_instance
            and row["current_state"] == "present"
            and (target_cal, row["google_event_id"]) not in scanned
        ):
            if counters["instance_scans"] >= MAX_INSTANCE_SCANS_PER_CYCLE:
                counters["instance_scans_deferred"] += 1
                continue
            scanned.add((target_cal, row["google_event_id"]))
            counters["instance_scans"] += 1
            try:
                marked += await _scan_parent_instances(
                    db, google,
                    user_id=user_id,
                    parent_row=row,
                    target_cal=target_cal,
                    counters=counters,
                )
            except Exception as e:
                counters["errors"] += 1
                logger.warning(
                    "observation audit: instance scan for %s on %s failed: %s",
                    row["google_event_id"], target_cal, e,
                )

    counters["divergent"] = marked
    if marked:
        # The nulled stamps are picked up by the diff on ANY reconcile;
        # enqueue one now so the heal lands on the next 30s drain tick
        # instead of the next periodic sweep.
        await enqueue_periodic(db, user_id=user_id, now=now)
        logger.warning(
            "observation audit user=%s: %d divergent projection(s) marked "
            "for re-assertion (missing=%d drift=%d phantom=%d "
            "instance_id=%d revive=%d instance_delete=%d) out of %d checked",
            user_id, marked,
            counters["missing_reset"], counters["drift_marked"],
            counters["phantom_marked"], counters["instance_ids_corrected"],
            counters["instance_revive_marked"], counters["instance_delete_marked"],
            counters["checked"],
        )
    await db.commit()
    return counters


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------
async def _sample_converged(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    sample_size: int,
    min_quiet_seconds: int,
    now: datetime,
) -> list[aiosqlite.Row]:
    """The next ``sample_size`` converged, quiescent projections, oldest
    observation first.

    Converged means the ledger itself claims nothing is owed: applied
    stamps present and equal to desired.  Excluded shapes:

    * permanently-failed rows (the operator alert surface — the admin
      retry action owns those);
    * rows with pending/in-flight outbox work (stamps about to move);
    * released ledger events (frozen on the calendars on purpose);
    * origin-writeback projections (``google_event_id`` deliberately
      NULL — nothing to GET) and legacy personal-source targets;
    * rows updated within the settle window (replica lag).
    """
    conditions = """
          e.user_id = ?
      AND p.google_event_id IS NOT NULL
      AND p.permanently_failed = 0
      AND p.applied_ledger_version IS NOT NULL
      AND p.applied_ledger_version = p.desired_ledger_version
      AND p.applied_payload_hash IS NOT NULL
      AND p.applied_payload_hash = p.desired_payload_hash
      AND e.status != 'released'
      AND NOT (p.target_kind = 'client'
               AND e.source_type IN ('client', 'personal')
               AND e.source_calendar_id IS NOT NULL
               AND p.target_calendar_id = e.source_calendar_id)
      AND NOT EXISTS (SELECT 1 FROM outbox_operations o
                       WHERE o.projection_id = p.id
                         AND o.status IN ('pending', 'in_flight'))
    """
    params: list[Any] = [int(user_id)]
    if min_quiet_seconds > 0:
        cutoff = (now - timedelta(seconds=min_quiet_seconds)).isoformat()
        conditions += " AND (p.updated_at IS NULL OR p.updated_at < ?)"
        params.append(cutoff)
    params.append(int(sample_size))
    return await (await db.execute(
        f"""SELECT p.id, p.target_kind, p.target_calendar_id,
                   p.current_state, p.google_event_id, p.google_etag,
                   p.applied_ledger_version, p.desired_ledger_version,
                   e.id AS ledger_event_id, e.canonical_uid,
                   e.parent_canonical_uid, e.status AS ledger_status,
                   e.recurrence_rule_json,
                   e.recurrence_instance_original_start, e.summary
              FROM ledger_projections p
              JOIN ledger_events e ON e.id = p.ledger_event_id
             WHERE {conditions}
             ORDER BY COALESCE(p.last_observed_at, '') ASC, p.id ASC
             LIMIT ?""",
        params,
    )).fetchall()


def _resolve_target(
    row,
    main_google_calendar_id: str,
    google_calendar_id_for: dict[int, str],
) -> Optional[str]:
    if row["target_kind"] == "main":
        return main_google_calendar_id
    if row["target_calendar_id"] is None:
        return None
    return google_calendar_id_for.get(int(row["target_calendar_id"]))


async def _stamp_observed(
    db: aiosqlite.Connection, projection_id: int, now: datetime,
) -> None:
    await db.execute(
        "UPDATE ledger_projections SET last_observed_at = ? WHERE id = ?",
        (now.isoformat(), projection_id),
    )


# ---------------------------------------------------------------------------
# Present-direction: the ledger says our copy is live on Google
# ---------------------------------------------------------------------------
async def _check_present(
    db: aiosqlite.Connection,
    google,
    row,
    target_cal: str,
    counters: dict,
) -> int:
    """Verify a present-converged projection.  Returns 1 if marked."""
    gid = row["google_event_id"]
    is_instance = bool(row["parent_canonical_uid"])
    try:
        live = await google.get_event(target_cal, gid)
    except Exception as e:
        if getattr(e, "status", None) in (404, 410):
            await _reset_missing(db, row)
            counters["missing_reset"] += 1
            logger.warning(
                "observation audit: projection %s (%r) says present but %s "
                "is MISSING from %s — marked for re-assertion",
                row["id"], row["summary"], gid, target_cal,
            )
            return 1
        raise

    if live.get("status") == "cancelled":
        await _reset_missing(db, row)
        counters["missing_reset"] += 1
        logger.warning(
            "observation audit: projection %s (%r) says present but %s on "
            "%s is CANCELLED — marked for re-assertion",
            row["id"], row["summary"], gid, target_cal,
        )
        return 1

    # For an instance, a live event at the WRONG original slot means the
    # stored (derived) id points at a different occurrence — our applied
    # state never reached the intended one.  Do NOT null stamps here:
    # re-asserting against the wrong id would clobber a neighbouring
    # occurrence.  The parent instances scan records the true id first;
    # marking happens there.
    if is_instance and _original_start_mismatch(row, live):
        logger.warning(
            "observation audit: instance projection %s stored id %s points "
            "at original slot %r but the ledger row's slot is %r — "
            "deferring to the parent instances scan for id correction",
            row["id"], gid,
            _canonical_slot(_observed_original_start(live)),
            _canonical_slot(row["recurrence_instance_original_start"]),
        )
        return 0

    stored_etag = row["google_etag"]
    live_etag = live.get("etag")
    if not stored_etag:
        # Nothing stored to compare — record the observed etag so the
        # NEXT cycle can compare, but never conclude divergence from an
        # absent baseline.
        if live_etag:
            await db.execute(
                "UPDATE ledger_projections SET google_etag = ? WHERE id = ?",
                (live_etag, int(row["id"])),
            )
        counters["consistent"] += 1
        return 0
    if live_etag == stored_etag:
        counters["consistent"] += 1
        return 0

    # The copy changed since our last write: user move/edit (or an
    # exception Google folded in).  Null the applied version — the
    # explicit unconverge signal — so the diff re-asserts canonical
    # payload.  Etag handling mirrors the two established reverts:
    # client targets refresh (direct heal, no user intent to protect);
    # main targets keep the stale etag so the corrective update 412s
    # and re-runs AFTER main ingest has classified any pending user
    # edit (RSVP writeback ordering).
    if row["target_kind"] == "client":
        await db.execute(
            """UPDATE ledger_projections
                  SET applied_ledger_version = NULL,
                      google_etag = ?
                WHERE id = ?""",
            (live_etag, int(row["id"])),
        )
    else:
        await db.execute(
            """UPDATE ledger_projections
                  SET applied_ledger_version = NULL
                WHERE id = ?""",
            (int(row["id"]),),
        )
    counters["drift_marked"] += 1
    logger.warning(
        "observation audit: projection %s (%r) on %s drifted — etag %s != "
        "stored %s; marked for re-assertion",
        row["id"], row["summary"], target_cal, live_etag, stored_etag,
    )
    return 1


async def _reset_missing(db: aiosqlite.Connection, row) -> None:
    """A present-desired copy is gone (404/410/cancelled tombstone).

    Non-instance rows get the full client-ingest reset (clear the id so
    the diff re-CREATEs; the create walks past the tombstone via the
    409 generation bump).  Instance rows only null the applied stamps —
    their id is derived from the parent and the diff's UPDATE carries
    ``status: confirmed``, the established revive idiom (a truly
    unrevivable tombstone lands in the outbox's 400/404 handlers).
    """
    if row["parent_canonical_uid"]:
        await db.execute(
            """UPDATE ledger_projections
                  SET applied_ledger_version = NULL
                WHERE id = ?""",
            (int(row["id"]),),
        )
        return
    await db.execute(
        """UPDATE ledger_projections
              SET current_state = 'absent',
                  google_event_id = NULL,
                  google_etag = NULL,
                  applied_ledger_version = NULL,
                  applied_payload_hash = NULL
            WHERE id = ?""",
        (int(row["id"]),),
    )


# ---------------------------------------------------------------------------
# Absent-direction: the ledger says our copy was deleted
# ---------------------------------------------------------------------------
async def _check_absent(
    db: aiosqlite.Connection,
    google,
    row,
    target_cal: str,
    counters: dict,
) -> int:
    """Verify an absent-converged NON-instance projection.

    Instance rows never reach here — ``events.get`` on a derived id
    synthesises a live occurrence whenever the parent covers the slot,
    which would read as a phantom and cancel a live occurrence of our
    own series.  See ``_scan_parent_instances`` for the safe instance
    path.
    """
    gid = row["google_event_id"]
    try:
        live = await google.get_event(target_cal, gid)
    except Exception as e:
        if getattr(e, "status", None) in (404, 410):
            counters["consistent"] += 1
            return 0
        raise
    if live.get("status") == "cancelled":
        counters["consistent"] += 1
        return 0

    # The delete this projection converged on never actually removed
    # the event (a 404-as-success against a wrong id, or the user
    # restored it from trash).  Record the observed truth and let the
    # diff re-issue the delete.
    await db.execute(
        """UPDATE ledger_projections
              SET current_state = 'present',
                  applied_ledger_version = NULL
            WHERE id = ?""",
        (int(row["id"]),),
    )
    counters["phantom_marked"] += 1
    logger.warning(
        "observation audit: projection %s (%r) says absent but %s on %s "
        "still EXISTS — marked for re-deletion",
        row["id"], row["summary"], gid, target_cal,
    )
    return 1


# ---------------------------------------------------------------------------
# Instance observation (events.instances): observed ids beat derived ids
# ---------------------------------------------------------------------------
async def _scan_parent_instances(
    db: aiosqlite.Connection,
    google,
    *,
    user_id: int,
    parent_row,
    target_cal: str,
    counters: dict,
) -> int:
    """List a live managed parent's real instances and reconcile the
    matching instance projections against what was OBSERVED.

    * Observed id != stored id → record the observed id (observation as
      truth; derivation stays the fallback for rows never scanned).  A
      row that claimed convergence against the wrong id is also marked
      dirty so the true occurrence receives our payload.
    * Observed ``cancelled`` while we want the occurrence present →
      mark dirty (the diff's ``status: confirmed`` update revives it).
    * Observed live while the instance LEDGER ROW is ``cancelled`` (the
      source says this occurrence must not exist) → flip the projection
      to observed-present and mark dirty so the delete re-fires.
      Structurally-absent instance rows (parent-driven) are left alone:
      a live item there is usually the parent's own synthesis, not an
      override.
    * Overrides with no matching ledger row are counted (tampering
      visibility) but never healed here — minting rows for them is the
      instance-level-heal work this codebase deliberately defers.

    Only POSITIVE observations are acted on: pagination is capped, so
    "my occurrence was not in the listing" proves nothing.
    """
    children = await (await db.execute(
        """SELECT p.id, p.google_event_id, p.google_etag,
                  p.applied_ledger_version, p.desired_state,
                  p.current_state,
                  e.id AS ledger_event_id, e.canonical_uid,
                  e.status AS ledger_status,
                  e.recurrence_instance_original_start, e.summary
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?
              AND e.parent_canonical_uid = ?
              AND e.status != 'released'
              AND p.target_kind = ?
              AND COALESCE(p.target_calendar_id, -1) = COALESCE(?, -1)
              AND p.permanently_failed = 0""",
        (
            user_id, parent_row["canonical_uid"],
            parent_row["target_kind"], parent_row["target_calendar_id"],
        ),
    )).fetchall()
    # No early-out when there are no tracked instance rows: a series
    # with zero exceptions is exactly where single-occurrence tampering
    # hides, and the unmatched-override counter below is its only
    # visibility.
    by_uid = {c["canonical_uid"]: c for c in children}

    observed: dict[str, dict] = {}
    page_token: Optional[str] = None
    pages = 0
    while pages < MAX_INSTANCE_PAGES:
        resp = await google.list_instances(
            target_cal, parent_row["google_event_id"],
            show_deleted=True, max_results=2500, page_token=page_token,
        )
        pages += 1
        for item in resp.get("items", []):
            original_start, _ = _instance_original_start(item)
            if not original_start:
                continue
            uid = canonical_uid_for_instance(
                parent_row["canonical_uid"], original_start,
            )
            # Overrides take precedence over synthesised occurrences in
            # the listing; last write wins if both somehow appear.
            observed[uid] = item
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    marked = 0
    for uid, item in observed.items():
        child = by_uid.get(uid)
        if child is None:
            # An override we have no ledger row for: a client
            # deleting/moving one occurrence of our busy series.
            # Cancelled items are overrides by definition; live ones
            # are only counted when visibly detached from the parent's
            # own synthesis (a moved occurrence).
            if item.get("status") == "cancelled" or _looks_moved(item):
                counters["tampered_unmatched"] += 1
            continue
        marked += await _reconcile_observed_instance(db, child, item, counters)
    return marked


async def _reconcile_observed_instance(
    db: aiosqlite.Connection, child, item: dict, counters: dict,
) -> int:
    """Apply one positive observation to one instance projection."""
    observed_id = item.get("id")
    observed_etag = item.get("etag")
    if not observed_id:
        return 0

    desired_present = child["desired_state"] != "absent"
    # A desired-absent row whose LEDGER ROW is still active is
    # structurally absent (parent-driven, target unresolvable, a
    # retired out-of-range slot) — and a live item at its slot is
    # usually the parent's own SYNTHESIS, not an override.  Acting on
    # it (recording the synthesized id, re-firing a delete) would
    # cancel a live occurrence of our own series.  Only a CANCELLED
    # ledger row carries source-truth justification to touch the
    # target here; everything else is hands-off.
    if not desired_present and child["ledger_status"] != "cancelled":
        return 0

    if child["google_event_id"] != observed_id:
        was_converged = child["applied_ledger_version"] is not None
        # Observation as truth: the real id replaces whatever was
        # stored/derived.  A projection that claimed convergence
        # against a DIFFERENT id never delivered to the real
        # occurrence — null the stamp so the diff re-asserts there.
        await db.execute(
            """UPDATE ledger_projections
                  SET google_event_id = ?,
                      google_etag = NULL,
                      applied_ledger_version = CASE WHEN ? THEN NULL
                          ELSE applied_ledger_version END
                WHERE id = ?""",
            (observed_id, was_converged, int(child["id"])),
        )
        counters["instance_ids_corrected"] += 1
        logger.warning(
            "observation audit: instance projection %s (%r) stored id %s "
            "but Google holds this occurrence at %s — recorded the "
            "observed id%s",
            child["id"], child["summary"], child["google_event_id"],
            observed_id,
            " and marked for re-assertion" if was_converged else "",
        )
        return 1 if was_converged else 0

    converged = child["applied_ledger_version"] is not None
    if item.get("status") == "cancelled" and desired_present and converged:
        # Tampered occurrence of a copy we want present: the diff's
        # instance UPDATE carries status:confirmed — the revive idiom.
        await db.execute(
            """UPDATE ledger_projections
                  SET applied_ledger_version = NULL
                WHERE id = ?""",
            (int(child["id"]),),
        )
        counters["instance_revive_marked"] += 1
        logger.warning(
            "observation audit: instance projection %s (%r) occurrence %s "
            "was cancelled on the target — marked for revive",
            child["id"], child["summary"], observed_id,
        )
        return 1

    if (
        item.get("status") != "cancelled"
        and not desired_present
        and converged
        and child["ledger_status"] == "cancelled"
    ):
        # The source cancelled this occurrence, we recorded our delete
        # as applied — yet the occurrence is live on the target.  Only
        # a CANCELLED ledger row is safe to act on here: for it, the
        # correct target state is a cancelled override regardless of
        # whether the live item is an override or the parent's own
        # synthesis (the delete materialises the cancellation either
        # way).  Structurally-absent rows (parent-driven) skip.
        await db.execute(
            """UPDATE ledger_projections
                  SET current_state = 'present',
                      applied_ledger_version = NULL
                WHERE id = ?""",
            (int(child["id"]),),
        )
        counters["instance_delete_marked"] += 1
        logger.warning(
            "observation audit: instance projection %s (%r) says the "
            "cancelled occurrence %s was deleted, but it is LIVE on the "
            "target — marked for re-deletion",
            child["id"], child["summary"], observed_id,
        )
        return 1

    # Id agrees; refresh drift detection the same way _check_present
    # does — but only when the row is converged-present with a stored
    # etag (an override we wrote).  A mismatch here is the occurrence
    # being edited in place on the target calendar.
    if (
        desired_present
        and converged
        and child["current_state"] == "present"
        and child["google_etag"]
        and observed_etag
        and observed_etag != child["google_etag"]
    ):
        await db.execute(
            """UPDATE ledger_projections
                  SET applied_ledger_version = NULL
                WHERE id = ?""",
            (int(child["id"]),),
        )
        counters["drift_marked"] += 1
        logger.warning(
            "observation audit: instance projection %s (%r) occurrence %s "
            "drifted on the target — marked for re-assertion",
            child["id"], child["summary"], observed_id,
        )
        return 1
    return 0


# ---------------------------------------------------------------------------
# Slot / shape helpers
# ---------------------------------------------------------------------------
def _observed_original_start(event: dict) -> str:
    start, _ = _instance_original_start(event)
    return start


def _canonical_slot(value: Optional[str]) -> str:
    """Timezone-stable comparison form of an original-start value —
    the same normalisation canonical instance UIDs use, so two
    representations of one slot always compare equal."""
    from app.ledger.identity import _canonical_instant_for_uid
    return _canonical_instant_for_uid(value or "")


def _original_start_mismatch(row, live: dict) -> bool:
    """True when a GET on the stored instance id returned an occurrence
    at a DIFFERENT original slot than the ledger row's — the stored id
    points at the wrong occurrence."""
    stored = row["recurrence_instance_original_start"]
    observed = _observed_original_start(live)
    if not stored or not observed:
        return False
    return _canonical_slot(stored) != _canonical_slot(observed)


def _looks_moved(item: dict) -> bool:
    """True when an instances-listing item is visibly an override that
    moved: its start no longer equals its original slot.  Used only for
    the tampering counter — synthesised occurrences always have
    start == originalStartTime."""
    ost = item.get("originalStartTime") or {}
    start = item.get("start") or {}
    o = ost.get("dateTime") or ost.get("date")
    s = start.get("dateTime") or start.get("date")
    if not o or not s:
        return False
    return _canonical_slot(o) != _canonical_slot(s)
