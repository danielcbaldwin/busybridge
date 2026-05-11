"""Client OAuth ingest (REWRITE_PLAN.md §5.1).

For each event Google delivers, decide:

1. Is it one of our writes (a busy block / projection)?  If so,
   skip — we don't want to mirror our own output.
2. Is it a rescheduled-parent ``_R`` case?  Look up the existing
   ledger row by stripped base ID and re-key its
   ``source_event_id`` to the new one.
3. Otherwise: upsert the ledger row, bumping ``version`` only if a
   material field changed.

The whole loop runs inside one transaction so the sync token can
only advance after every event was successfully recorded.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

import aiosqlite

from app.ledger.google_client import GoogleClient
from app.ledger.identity import (
    canonical_uid_client,
    canonical_uid_for_instance,
    is_managed_google_event_id,
)

logger = logging.getLogger(__name__)
UTC = timezone.utc


async def ingest_client_calendar(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    client_calendar_id: int,
    google_calendar_id: str,
    user_email: str,
) -> dict:
    """Run one ingest pass for one client calendar.

    Returns counters: ``{seen, created, updated, rekeyed, skipped, cancelled}``.
    Affected ``ledger_event.id``s are written into the
    ``reconcile_requests.sources_json`` so the reconciler picks
    them up for planning.
    """
    state = await _get_or_create_sync_state(db, client_calendar_id)
    sync_token: Optional[str] = state["sync_token"]
    counters = {
        "seen": 0, "created": 0, "updated": 0,
        "rekeyed": 0, "skipped": 0, "cancelled": 0,
    }
    affected_ledger_ids: list[int] = []

    page_token: Optional[str] = None
    new_sync_token: Optional[str] = None
    full_sync = sync_token is None

    while True:
        try:
            page = google.list_events(
                google_calendar_id,
                sync_token=sync_token if page_token is None else None,
                page_token=page_token,
                show_deleted=True,
                max_results=250,
            )
        except Exception as e:
            if getattr(e, "status", None) == 410:
                # Sync token expired — restart with full sync.
                logger.info(
                    "sync token expired for client_calendar_id=%s, "
                    "falling back to full sync",
                    client_calendar_id,
                )
                sync_token = None
                page_token = None
                full_sync = True
                continue
            raise

        for event in page.get("items", []):
            counters["seen"] += 1
            outcome, ledger_id = await _ingest_one_event(
                db,
                user_id=user_id,
                client_calendar_id=client_calendar_id,
                user_email=user_email,
                event=event,
            )
            counters[outcome] = counters.get(outcome, 0) + 1
            if ledger_id is not None:
                affected_ledger_ids.append(ledger_id)

        if "nextPageToken" in page:
            page_token = page["nextPageToken"]
            continue
        new_sync_token = page.get("nextSyncToken")
        break

    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE calendar_sync_state
              SET sync_token = ?,
                  last_full_sync = COALESCE(?, last_full_sync),
                  last_incremental_sync = ?,
                  consecutive_failures = 0,
                  last_error = NULL
            WHERE client_calendar_id = ?""",
        (
            new_sync_token,
            when if full_sync else None,
            when,
            client_calendar_id,
        ),
    )
    if affected_ledger_ids:
        await _record_affected(db, user_id=user_id, ledger_ids=affected_ledger_ids)
    await db.commit()
    return counters


# ---------------------------------------------------------------------------
# Per-event handling
# ---------------------------------------------------------------------------
async def _ingest_one_event(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    client_calendar_id: int,
    user_email: str,
    event: dict,
) -> tuple[str, Optional[int]]:
    """Process one Google event.  Returns (outcome, ledger_event_id)."""
    event_id = event["id"]
    status = event.get("status", "confirmed")

    # 1. Loop-prevention: skip events we wrote (deterministic IDs
    # plus an exact projection lookup as defence-in-depth).
    if is_managed_google_event_id(event_id):
        # Even if we wrote it, ingest needs to see status=cancelled
        # for our own deletes — but those should already be removed
        # from the source by definition.  Skip silently.
        return "skipped", None
    proj_match = await (await db.execute(
        """SELECT id FROM ledger_projections
            WHERE google_event_id = ?
            LIMIT 1""",
        (event_id,),
    )).fetchone()
    if proj_match is not None:
        return "skipped", None

    # 2. Recurring-event INSTANCE (modified or cancelled).  Route
    #    to the instance handler — these get their own ledger row
    #    with parent_canonical_uid set so cancellations are sticky.
    if event.get("recurringEventId"):
        return await _ingest_instance(
            db,
            user_id=user_id,
            client_calendar_id=client_calendar_id,
            user_email=user_email,
            event=event,
        )

    canonical = canonical_uid_client(client_calendar_id, event_id)

    # 3. Rescheduled-parent ``_R`` quirk.  Re-key existing ledger
    # row before treating this as a new event.
    if (
        status != "cancelled"
        and "_R" in event_id
        and event.get("recurrence")
    ):
        rekeyed = await _try_rekey_R_parent(
            db,
            user_id=user_id,
            client_calendar_id=client_calendar_id,
            new_event_id=event_id,
        )
        if rekeyed is not None:
            await _apply_event_to_ledger(
                db, ledger_event_id=rekeyed,
                event=event, user_email=user_email,
            )
            return "rekeyed", rekeyed

    existing = await (await db.execute(
        """SELECT * FROM ledger_events
            WHERE user_id = ? AND canonical_uid = ?""",
        (user_id, canonical),
    )).fetchone()

    # 3. Cancellations: flip to status=cancelled (don't delete the row).
    if status == "cancelled":
        if existing is None:
            return "skipped", None
        if existing["status"] == "cancelled":
            return "skipped", int(existing["id"])
        await _mark_cancelled(db, ledger_event_id=int(existing["id"]))
        return "cancelled", int(existing["id"])

    # 4. Upsert.
    if existing is None:
        new_id = await _insert_ledger_row(
            db,
            user_id=user_id,
            canonical_uid=canonical,
            client_calendar_id=client_calendar_id,
            event=event,
            user_email=user_email,
        )
        return "created", new_id
    changed = await _apply_event_to_ledger(
        db,
        ledger_event_id=int(existing["id"]),
        event=event,
        user_email=user_email,
    )
    return ("updated" if changed else "skipped"), int(existing["id"])


# ---------------------------------------------------------------------------
# Instance handling
# ---------------------------------------------------------------------------
async def _ingest_instance(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    client_calendar_id: int,
    user_email: str,
    event: dict,
) -> tuple[str, Optional[int]]:
    """Upsert a modified or cancelled instance of a recurring series.

    Instances are kept as separate ledger rows with
    ``parent_canonical_uid`` set, so a cancellation persists even
    if the recurring parent series later gets re-ingested via
    full sync (which omits cancelled exceptions — the documented
    "recurring-cancellation amnesia" bug).
    """
    parent_event_id = event["recurringEventId"]
    parent_canonical = canonical_uid_client(client_calendar_id, parent_event_id)
    ost = event.get("originalStartTime", {}) or {}
    if "dateTime" in ost:
        original_start = ost["dateTime"]
    elif "date" in ost:
        original_start = ost["date"]
    else:
        original_start = (event.get("start") or {}).get("dateTime") or (
            event.get("start") or {}
        ).get("date") or ""

    instance_canonical = canonical_uid_for_instance(
        parent_canonical, original_start,
    )
    status = event.get("status", "confirmed")
    when = datetime.now(UTC).isoformat()

    existing = await (await db.execute(
        """SELECT * FROM ledger_events
            WHERE user_id = ? AND canonical_uid = ?""",
        (user_id, instance_canonical),
    )).fetchone()

    # Cancelled instance — sticky ledger row that survives parent
    # full-sync (since incremental sync surfaces the cancellation
    # once, and our row persists across future passes).
    if status == "cancelled":
        if existing is not None and existing["status"] == "cancelled":
            return "skipped", int(existing["id"])
        if existing is None:
            cursor = await db.execute(
                """INSERT INTO ledger_events
                      (user_id, canonical_uid, parent_canonical_uid,
                       source_type, source_calendar_id, source_event_id,
                       recurrence_instance_original_start,
                       status, version, is_recurring,
                       created_at, updated_at, last_seen_at, cancelled_at)
                   VALUES (?, ?, ?,
                           'client', ?, ?, ?,
                           'cancelled', 1, 0,
                           ?, ?, ?, ?)""",
                (
                    user_id, instance_canonical, parent_canonical,
                    client_calendar_id, event["id"], original_start,
                    when, when, when, when,
                ),
            )
            return "cancelled", int(cursor.lastrowid)
        await db.execute(
            """UPDATE ledger_events
                  SET status = 'cancelled',
                      version = version + 1,
                      cancelled_at = ?, updated_at = ?, last_seen_at = ?
                WHERE id = ?""",
            (when, when, when, int(existing["id"])),
        )
        return "cancelled", int(existing["id"])

    # Modified instance — single-instance override on the series.
    fields = _extract_event_fields(event, user_email=user_email)
    if existing is None:
        cursor = await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid, parent_canonical_uid,
                   source_type, source_calendar_id, source_event_id,
                   recurrence_instance_original_start,
                   source_etag, source_updated_at,
                   summary, description, location,
                   start_at, end_at, is_all_day,
                   show_as, visibility, color_id,
                   organizer_email, user_can_edit, user_rsvp_status,
                   attendees_json,
                   status, is_recurring, version,
                   created_at, updated_at, last_seen_at)
               VALUES (?, ?, ?,
                       'client', ?, ?, ?,
                       ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?, ?, ?,
                       ?,
                       'active', 0, 1,
                       ?, ?, ?)""",
            (
                user_id, instance_canonical, parent_canonical,
                client_calendar_id, event["id"], original_start,
                event.get("etag"), event.get("updated"),
                fields["summary"], fields["description"], fields["location"],
                fields["start_at"], fields["end_at"], fields["is_all_day"],
                fields["show_as"], fields["visibility"], fields["color_id"],
                fields["organizer_email"], fields["user_can_edit"],
                fields["user_rsvp_status"],
                fields["attendees_json"],
                when, when, when,
            ),
        )
        return "created", int(cursor.lastrowid)

    new_hash = _content_hash(fields)
    old_hash = _content_hash_from_row(existing)
    if new_hash == old_hash:
        await db.execute(
            "UPDATE ledger_events SET last_seen_at = ? WHERE id = ?",
            (when, int(existing["id"])),
        )
        return "skipped", int(existing["id"])
    await db.execute(
        """UPDATE ledger_events
              SET source_etag = ?, source_updated_at = ?,
                  summary = ?, description = ?, location = ?,
                  start_at = ?, end_at = ?, is_all_day = ?,
                  show_as = ?, visibility = ?, color_id = ?,
                  organizer_email = ?, user_can_edit = ?,
                  user_rsvp_status = ?,
                  attendees_json = ?,
                  status = 'active',
                  version = version + 1,
                  updated_at = ?, last_seen_at = ?
            WHERE id = ?""",
        (
            event.get("etag"), event.get("updated"),
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"], fields["is_all_day"],
            fields["show_as"], fields["visibility"], fields["color_id"],
            fields["organizer_email"], fields["user_can_edit"],
            fields["user_rsvp_status"],
            fields["attendees_json"],
            when, when, int(existing["id"]),
        ),
    )
    return "updated", int(existing["id"])


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------
async def _insert_ledger_row(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    canonical_uid: str,
    client_calendar_id: int,
    event: dict,
    user_email: str,
) -> int:
    fields = _extract_event_fields(event, user_email=user_email)
    when = datetime.now(UTC).isoformat()
    cursor = await db.execute(
        """INSERT INTO ledger_events
              (user_id, canonical_uid,
               source_type, source_calendar_id, source_event_id,
               source_etag, source_updated_at,
               summary, description, location,
               start_at, end_at, is_all_day,
               show_as, visibility, color_id,
               organizer_email, user_can_edit, user_rsvp_status,
               attendees_json, recurrence_rule_json,
               status, is_recurring, version,
               created_at, updated_at, last_seen_at)
           VALUES (?, ?,
                   'client', ?, ?,
                   ?, ?,
                   ?, ?, ?,
                   ?, ?, ?,
                   ?, ?, ?,
                   ?, ?, ?,
                   ?, ?,
                   'active', ?, 1,
                   ?, ?, ?)""",
        (
            user_id, canonical_uid,
            client_calendar_id, event["id"],
            event.get("etag"), event.get("updated"),
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"], fields["is_all_day"],
            fields["show_as"], fields["visibility"], fields["color_id"],
            fields["organizer_email"], fields["user_can_edit"],
            fields["user_rsvp_status"],
            fields["attendees_json"], fields["recurrence_rule_json"],
            fields["is_recurring"],
            when, when, when,
        ),
    )
    return int(cursor.lastrowid)


async def _apply_event_to_ledger(
    db: aiosqlite.Connection,
    *,
    ledger_event_id: int,
    event: dict,
    user_email: str,
) -> bool:
    """Update an existing ledger row.  Returns True if any material
    field changed (and version was bumped).  A row resurrecting
    from ``cancelled`` to ``active`` always counts as changed."""
    fields = _extract_event_fields(event, user_email=user_email)
    existing = await (await db.execute(
        "SELECT * FROM ledger_events WHERE id = ?",
        (ledger_event_id,),
    )).fetchone()
    when = datetime.now(UTC).isoformat()

    new_hash = _content_hash(fields)
    old_hash = _content_hash_from_row(existing)
    resurrecting = existing["status"] == "cancelled"
    changed = (new_hash != old_hash) or resurrecting

    if not changed:
        await db.execute(
            "UPDATE ledger_events SET last_seen_at = ? WHERE id = ?",
            (when, ledger_event_id),
        )
        return False

    await db.execute(
        """UPDATE ledger_events
              SET source_etag = ?,
                  source_updated_at = ?,
                  summary = ?, description = ?, location = ?,
                  start_at = ?, end_at = ?, is_all_day = ?,
                  show_as = ?, visibility = ?, color_id = ?,
                  organizer_email = ?, user_can_edit = ?,
                  user_rsvp_status = ?,
                  attendees_json = ?, recurrence_rule_json = ?,
                  is_recurring = ?,
                  status = 'active',
                  version = version + 1,
                  updated_at = ?,
                  last_seen_at = ?
            WHERE id = ?""",
        (
            event.get("etag"), event.get("updated"),
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"], fields["is_all_day"],
            fields["show_as"], fields["visibility"], fields["color_id"],
            fields["organizer_email"], fields["user_can_edit"],
            fields["user_rsvp_status"],
            fields["attendees_json"], fields["recurrence_rule_json"],
            fields["is_recurring"],
            when, when, ledger_event_id,
        ),
    )
    return True


async def _mark_cancelled(
    db: aiosqlite.Connection, *, ledger_event_id: int,
) -> None:
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE ledger_events
              SET status = 'cancelled',
                  version = version + 1,
                  cancelled_at = ?,
                  updated_at = ?,
                  last_seen_at = ?
            WHERE id = ?""",
        (when, when, when, ledger_event_id),
    )


async def _try_rekey_R_parent(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    client_calendar_id: int,
    new_event_id: str,
) -> Optional[int]:
    """Look up an existing ledger row under the base ID (everything
    before ``_R``) and re-key it to ``new_event_id``.  Returns the
    ledger event id if a re-key happened, else None.

    Mirrors ``app/sync/rules.py:82-118``: searches for both the
    bare base and any prior ``_R<ts>`` variant.
    """
    base = new_event_id.split("_R")[0]
    bare_uid = canonical_uid_client(client_calendar_id, base)
    rows = await (await db.execute(
        """SELECT id, canonical_uid, source_event_id
             FROM ledger_events
            WHERE user_id = ?
              AND source_type = 'client'
              AND source_calendar_id = ?
              AND status = 'active'
              AND user_intentionally_deleted = 0
              AND (canonical_uid = ?
                   OR source_event_id = ?
                   OR source_event_id LIKE ?)
            ORDER BY updated_at DESC
            LIMIT 1""",
        (
            user_id, client_calendar_id,
            bare_uid, base, f"{base}_R%",
        ),
    )).fetchall()
    if not rows:
        return None
    target = rows[0]
    new_canonical = canonical_uid_client(client_calendar_id, new_event_id)
    when = datetime.now(UTC).isoformat()
    await db.execute(
        """UPDATE ledger_events
              SET canonical_uid = ?,
                  source_event_id = ?,
                  updated_at = ?
            WHERE id = ?""",
        (new_canonical, new_event_id, when, int(target["id"])),
    )
    logger.info(
        "re-keyed ledger_event %s from source_event_id=%s to %s "
        "(_R reschedule)",
        target["id"], target["source_event_id"], new_event_id,
    )
    return int(target["id"])


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------
def _extract_event_fields(event: dict, *, user_email: str) -> dict:
    start = event.get("start", {}) or {}
    end = event.get("end", {}) or {}
    is_all_day = "date" in start
    if is_all_day:
        start_at = start.get("date")
        end_at = end.get("date")
    else:
        start_at = start.get("dateTime")
        end_at = end.get("dateTime")

    show_as = "free" if event.get("transparency") == "transparent" else "busy"
    # User can edit if: they are the organizer, OR the event explicitly
    # marks guestsCanModify=True.  Solo events (no attendees, no
    # explicit organizer set) are also editable.
    has_attendees = bool(event.get("attendees"))
    user_can_edit = (
        _user_is_organizer(event, user_email)
        or bool(event.get("guestsCanModify"))
        or not has_attendees
    )

    user_rsvp = None
    for att in (event.get("attendees") or []):
        if att.get("self") or att.get("email", "").lower() == user_email.lower():
            user_rsvp = att.get("responseStatus")
            break

    organizer_email = (event.get("organizer") or {}).get("email")

    return {
        "summary": event.get("summary"),
        "description": event.get("description"),
        "location": event.get("location"),
        "start_at": start_at,
        "end_at": end_at,
        "is_all_day": is_all_day,
        "show_as": show_as,
        "visibility": event.get("visibility"),
        "color_id": event.get("colorId"),
        "organizer_email": organizer_email,
        "user_can_edit": user_can_edit,
        "user_rsvp_status": user_rsvp,
        "attendees_json": json.dumps(event.get("attendees", [])),
        "recurrence_rule_json": (
            json.dumps(event["recurrence"]) if event.get("recurrence") else None
        ),
        "is_recurring": bool(event.get("recurrence")),
    }


def _user_is_organizer(event: dict, user_email: str) -> bool:
    organizer = event.get("organizer") or {}
    return (organizer.get("email") or "").lower() == user_email.lower()


def _content_hash(fields: dict) -> str:
    canonical = json.dumps(
        {k: fields[k] for k in sorted(fields)},
        sort_keys=True, separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _content_hash_from_row(row) -> str:
    fields = {
        "summary": row["summary"],
        "description": row["description"],
        "location": row["location"],
        "start_at": row["start_at"],
        "end_at": row["end_at"],
        "is_all_day": bool(row["is_all_day"]),
        "show_as": row["show_as"],
        "visibility": row["visibility"],
        "color_id": row["color_id"],
        "organizer_email": row["organizer_email"],
        "user_can_edit": bool(row["user_can_edit"]),
        "user_rsvp_status": row["user_rsvp_status"],
        "attendees_json": row["attendees_json"],
        "recurrence_rule_json": row["recurrence_rule_json"],
        "is_recurring": bool(row["is_recurring"]),
    }
    return _content_hash(fields)


# ---------------------------------------------------------------------------
# Sync state + reconcile bookkeeping
# ---------------------------------------------------------------------------
async def _get_or_create_sync_state(
    db: aiosqlite.Connection, client_calendar_id: int,
) -> aiosqlite.Row:
    row = await (await db.execute(
        "SELECT * FROM calendar_sync_state WHERE client_calendar_id = ?",
        (client_calendar_id,),
    )).fetchone()
    if row is not None:
        return row
    await db.execute(
        """INSERT INTO calendar_sync_state (client_calendar_id) VALUES (?)""",
        (client_calendar_id,),
    )
    return await (await db.execute(
        "SELECT * FROM calendar_sync_state WHERE client_calendar_id = ?",
        (client_calendar_id,),
    )).fetchone()


async def _record_affected(
    db: aiosqlite.Connection, *, user_id: int, ledger_ids: list[int],
) -> None:
    """Append affected ledger_event ids to the user's reconcile_request."""
    when = datetime.now(UTC).isoformat()
    existing = await (await db.execute(
        "SELECT sources_json FROM reconcile_requests WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    if existing is None:
        sources = list(set(ledger_ids))
        await db.execute(
            """INSERT INTO reconcile_requests
                  (user_id, sources_json, enqueued_at, scheduled_for)
               VALUES (?, ?, ?, ?)""",
            (user_id, json.dumps(sources), when, when),
        )
        return
    prior = json.loads(existing["sources_json"] or "[]")
    merged = list({*prior, *ledger_ids})
    await db.execute(
        """UPDATE reconcile_requests
              SET sources_json = ?, enqueued_at = ?
            WHERE user_id = ?""",
        (json.dumps(merged), when, user_id),
    )
