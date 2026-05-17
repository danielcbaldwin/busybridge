"""Webcal/ICS ingest (REWRITE_PLAN.md §5.4).

ICS feeds give us a list of events on each poll.  Three quirks
matter:

1. **Unstable UIDs.**  Some providers (Eventbrite, Luma, ...)
   regenerate UIDs on every fetch.  We detect this and fall back
   to a content-hash canonical UID over (start, end) — NOT over
   the summary, so a rename does not produce a duplicate ledger
   row.

2. **Stale upstream removal.**  If a row's ``last_seen_at`` falls
   behind two poll intervals, treat it as cancelled.

3. **Out-of-window events.**  Past or far-future events stay in
   the ledger; the planner decides whether their projections
   should be present.

The fetch step is parametrised: a `fetch_ics` callable accepts
``(url, if_none_match)`` and returns ``(status, etag, body)``.
In tests we drive it from an in-memory dict; in production we'd
use httpx with SSRF guards from the existing
``app/sync/webcal_sync.py``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Optional

import aiosqlite
from icalendar import Calendar as ICalCalendar

from app.ledger.identity import (
    canonical_uid_for_instance,
    canonical_uid_webcal_stable,
    canonical_uid_webcal_unstable,
)
from app.ledger.ingest.client import _content_hash, _record_affected

logger = logging.getLogger(__name__)
UTC = timezone.utc

# UUID v4 pattern; webcal feeds that emit these and change them
# per fetch are flagged as unstable.
_UUID_V4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


# Type alias for the fetch hook (lets tests inject a fake fetcher).
# An async callable (url, if_none_match) -> dict with keys
#   'status' (int), 'etag' (Optional[str]), 'body' (Optional[bytes]).
# Async so the SSRF-safe HTTP fetch does not block the event loop.
FetchHook = Callable[[str, Optional[str]], Awaitable[dict]]


async def ingest_webcal_subscription(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    subscription_id: int,
    url: str,
    fetch: FetchHook,
    now: Optional[datetime] = None,
) -> dict:
    """Poll one webcal subscription and reconcile its events into
    the ledger."""
    now = now or datetime.now(UTC)
    counters = {
        "seen": 0, "created": 0, "updated": 0,
        "stale_cancelled": 0, "skipped": 0, "not_modified": 0,
        "errors": 0,
    }
    state = await _get_subscription(db, subscription_id=subscription_id)
    if state is None:
        raise ValueError(f"webcal subscription {subscription_id} not found")
    if not state["is_active"]:
        return counters

    try:
        response = await fetch(url, state["last_etag"])
    except Exception as e:
        logger.warning(
            "webcal fetch failed user_id=%s sub=%s: %s",
            user_id, subscription_id, e,
        )
        counters["errors"] += 1
        await _record_fetch_failure(db, subscription_id, str(e))
        return counters

    status = int(response.get("status", 200))
    if status == 304:
        counters["not_modified"] = 1
        await _record_fetch_success(
            db, subscription_id, etag=state["last_etag"], now=now,
        )
        return counters
    if status >= 400:
        counters["errors"] += 1
        await _record_fetch_failure(
            db, subscription_id, f"HTTP {status}",
        )
        return counters

    body = response.get("body") or b""
    etag = response.get("etag")
    try:
        cal = ICalCalendar.from_ical(body)
    except Exception as e:
        logger.warning("webcal parse failed sub=%s: %s", subscription_id, e)
        counters["errors"] += 1
        await _record_fetch_failure(db, subscription_id, f"parse: {e}")
        return counters

    parsed_events = list(_iter_ics_events(cal))
    affected_ledger_ids: list[int] = []
    seen_canonical_uids: set[str] = set()

    for ev in parsed_events:
        counters["seen"] += 1
        outcome, ledger_id, canonical = await _ingest_ics_event(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            event=ev,
            now=now,
        )
        counters[outcome] = counters.get(outcome, 0) + 1
        if ledger_id is not None:
            affected_ledger_ids.append(ledger_id)
            seen_canonical_uids.add(canonical)

    # Stale-detection: anything previously sourced from this
    # subscription but not in this poll's seen set, AND not seen
    # for >= 2 poll intervals, flips to cancelled.
    # Production schema stores poll_interval_minutes; some test
    # schemas use poll_interval_seconds.  Tolerate either.
    keys = state.keys() if hasattr(state, "keys") else []
    if "poll_interval_minutes" in keys and state["poll_interval_minutes"]:
        poll_interval = int(state["poll_interval_minutes"]) * 60
    elif "poll_interval_seconds" in keys and state["poll_interval_seconds"]:
        poll_interval = int(state["poll_interval_seconds"])
    else:
        poll_interval = 3600
    stale_cutoff = (now - timedelta(seconds=2 * poll_interval)).isoformat()
    # A stale row — not in this poll's seen set and not seen for >= 2
    # poll intervals — flips to cancelled.  This INCLUDES RECURRENCE-ID
    # override rows (parent_canonical_uid set): a modified override
    # dropped from the feed must be handled intentionally, not left
    # lingering at its old moved/edited time forever.  Cancelling the
    # override removes that occurrence's busy block; a true
    # revert-to-series-default is a richer behaviour the model does
    # not yet express.
    rows = await (await db.execute(
        """SELECT id, canonical_uid FROM ledger_events
            WHERE user_id = ?
              AND source_type = 'webcal'
              AND source_calendar_id = ?
              AND status = 'active'
              AND user_intentionally_deleted = 0
              AND (last_seen_at IS NULL OR last_seen_at < ?)""",
        (user_id, subscription_id, stale_cutoff),
    )).fetchall()
    for row in rows:
        if row["canonical_uid"] in seen_canonical_uids:
            continue
        await _mark_cancelled(db, ledger_event_id=int(row["id"]), now=now)
        counters["stale_cancelled"] += 1
        affected_ledger_ids.append(int(row["id"]))

    # Record affected ledger ids BEFORE marking the fetch successful.
    # _record_fetch_success advances last_etag; the DB is autocommit,
    # so a crash between the two would leave the etag advanced (next
    # poll gets 304) with the affected ids never recorded — stranding
    # those ICS changes.  Recording affected first makes the etag
    # write the last durable write of the poll.
    if affected_ledger_ids:
        await _record_affected(db, user_id=user_id, ledger_ids=affected_ledger_ids)
    await _record_fetch_success(db, subscription_id, etag=etag, now=now)
    await db.commit()
    return counters


# ---------------------------------------------------------------------------
# ICS parsing
# ---------------------------------------------------------------------------
def _iter_ics_events(cal: ICalCalendar):
    """Yield ``dict`` rows extracted from a parsed ICS calendar."""
    for comp in cal.walk():
        if comp.name != "VEVENT":
            continue
        yield _vevent_to_dict(comp)


def _vevent_to_dict(comp) -> dict:
    """Pull the fields we care about out of an icalendar VEVENT."""
    uid = str(comp.get("UID", "")) or None
    summary = _first_str(comp, "SUMMARY")
    description = _first_str(comp, "DESCRIPTION")
    location = _first_str(comp, "LOCATION")
    status = (str(comp.get("STATUS", "")) or "CONFIRMED").upper()

    dtstart = comp.get("DTSTART")
    dtend = comp.get("DTEND")
    start_at, end_at, start_tz, end_tz, is_all_day = _normalize_times(
        dtstart, dtend,
    )

    # RRULE + EXDATE + RDATE all belong in the `recurrence` array
    # Google materialises instances from.  Dropping EXDATE would
    # leave a ghost busy block on every excluded occurrence.
    recurrence_rule = _extract_recurrence(comp)

    # A VEVENT carrying RECURRENCE-ID is a single-occurrence override
    # of the series with the same UID — a modified or cancelled
    # instance, not a standalone event.
    recurrence_id = _format_recurrence_id(comp.get("RECURRENCE-ID"))

    transparency = (str(comp.get("TRANSP", "OPAQUE")) or "OPAQUE").upper()
    show_as = "free" if transparency == "TRANSPARENT" else "busy"

    return {
        "uid": uid,
        "summary": summary,
        "description": description,
        "location": location,
        "start_at": start_at,
        "end_at": end_at,
        "start_timezone": start_tz,
        "end_timezone": end_tz,
        "is_all_day": is_all_day,
        "show_as": show_as,
        "recurrence_rule_json": (
            json.dumps(recurrence_rule) if recurrence_rule else None
        ),
        "is_recurring": recurrence_rule is not None,
        "recurrence_id": recurrence_id,
        "status": status,
    }


def _extract_recurrence(comp) -> Optional[list[str]]:
    """Collect RRULE / EXDATE / RDATE lines for the Google
    ``recurrence`` array.  Each property may appear more than once
    (common for EXDATE)."""
    lines: list[str] = []
    for key in ("RRULE", "EXDATE", "RDATE"):
        val = comp.get(key)
        if val is None:
            continue
        items = val if isinstance(val, list) else [val]
        for item in items:
            if hasattr(item, "to_ical"):
                try:
                    lines.append(f"{key}:" + item.to_ical().decode("ascii"))
                except Exception:
                    lines.append(f"{key}:" + str(item))
            else:
                lines.append(f"{key}:" + str(item))
    return lines or None


def _format_recurrence_id(rid) -> Optional[str]:
    """Normalise a RECURRENCE-ID property to the same string shape
    used for ``recurrence_instance_original_start``."""
    if rid is None:
        return None
    dt = getattr(rid, "dt", None)
    if dt is None:
        return None
    if isinstance(dt, datetime):
        d = dt.astimezone(UTC) if dt.tzinfo else dt.replace(tzinfo=UTC)
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt.isoformat()  # date-only → YYYY-MM-DD


def _first_str(comp, key: str) -> Optional[str]:
    v = comp.get(key)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _normalize_times(
    dtstart, dtend,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str], bool]:
    """Return ``(start_at, end_at, start_timezone, end_timezone,
    is_all_day)``.  All-day events keep their ``YYYY-MM-DD`` shape and
    carry no zone; timed events get an ISO-8601 UTC string plus the
    IANA zone their RRULE expands in (so a mirrored recurring webcal
    event stays correct across DST instead of drifting onto a fixed
    UTC grid)."""
    if dtstart is None:
        return None, None, None, None, False
    sdt = dtstart.dt
    edt = dtend.dt if dtend is not None else None
    if isinstance(sdt, datetime):
        s = sdt.astimezone(UTC) if sdt.tzinfo else sdt.replace(tzinfo=UTC)
        e = (
            (edt.astimezone(UTC) if edt.tzinfo else edt.replace(tzinfo=UTC))
            if isinstance(edt, datetime)
            else (s + timedelta(minutes=30))
        )
        return (
            s.strftime("%Y-%m-%dT%H:%M:%SZ"),
            e.strftime("%Y-%m-%dT%H:%M:%SZ"),
            _iana_tz_name(sdt),
            _iana_tz_name(edt) if isinstance(edt, datetime) else _iana_tz_name(sdt),
            False,
        )
    # date-only → all-day
    e = edt if edt is not None else (sdt + timedelta(days=1))
    return sdt.isoformat(), e.isoformat(), None, None, True


def _iana_tz_name(dt) -> Optional[str]:
    """The IANA zone key of a datetime's tzinfo, or ``None``.

    icalendar parses ``DTSTART;TZID=America/New_York:...`` into a
    ``zoneinfo.ZoneInfo`` whose ``.key`` is the IANA name.  A UTC /
    fixed-offset / floating time has no ``.key`` — return ``None`` so
    the renderer falls back to UTC."""
    tz = getattr(dt, "tzinfo", None)
    if tz is None:
        return None
    key = getattr(tz, "key", None)
    # "UTC" is not a drift-prone zone — treat it as "no zone" so the
    # renderer's UTC fallback applies and the column stays NULL.
    return None if key == "UTC" else key


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------
async def _ingest_ics_event(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    subscription_id: int,
    event: dict,
    now: datetime,
) -> tuple[str, Optional[int], str]:
    """Upsert one ICS event into the ledger.

    Returns ``(outcome, ledger_event_id, canonical_uid)``.
    """
    uid = event.get("uid")
    # Classify per event, not per feed.  An event with no UID or a
    # UUIDv4 UID (the kind some feeds regenerate every poll) is
    # "unstable" — deduped by its start/end hash; an event with a
    # domain-anchored UID is "stable" — deduped by UID.  Per-event
    # classification means one domain-UID event in an otherwise-UUID
    # feed cannot flip every other event's canonical_uid scheme and
    # trigger a mass cancel/recreate.
    unstable = uid is None or bool(_UUID_V4_RE.match(uid))

    # A RECURRENCE-ID override of a stable-UID series is a modified
    # or cancelled single occurrence — route it to its own instance
    # ledger row so it does not collide with the parent series
    # (which carries the same UID).
    if event.get("recurrence_id") and not unstable and uid is not None:
        return await _ingest_ics_instance(
            db,
            user_id=user_id,
            subscription_id=subscription_id,
            event=event,
            parent_uid=uid,
            now=now,
        )

    if unstable:
        canonical = canonical_uid_webcal_unstable(
            subscription_id, event["start_at"] or "", event["end_at"] or "",
        )
    else:
        canonical = canonical_uid_webcal_stable(subscription_id, uid)

    fields = _ics_to_ledger_fields(event)
    when = now.isoformat()

    if event["status"] == "CANCELLED":
        existing = await (await db.execute(
            """SELECT id, status FROM ledger_events
                WHERE user_id = ? AND canonical_uid = ?""",
            (user_id, canonical),
        )).fetchone()
        if existing is None:
            return "skipped", None, canonical
        if existing["status"] == "cancelled":
            return "skipped", int(existing["id"]), canonical
        await _mark_cancelled(db, ledger_event_id=int(existing["id"]), now=now)
        return "cancelled", int(existing["id"]), canonical

    if unstable:
        # Distinct unstable events that share a start/end hash to the
        # same base canonical_uid.  Probe for a free slot so the later
        # one cannot silently overwrite the earlier (a lost event).  A
        # row already touched THIS poll (last_seen_at == when) belongs
        # to a different event processed earlier in the same poll — a
        # genuine collision; an older row is this same event from a
        # previous poll and is reused in place (so a rename, which
        # keeps the same hash, still does NOT duplicate).
        ordinal = 0
        while True:
            canonical = canonical_uid_webcal_unstable(
                subscription_id, event["start_at"] or "",
                event["end_at"] or "", ordinal,
            )
            existing = await (await db.execute(
                """SELECT * FROM ledger_events
                    WHERE user_id = ? AND canonical_uid = ?""",
                (user_id, canonical),
            )).fetchone()
            if existing is None or existing["last_seen_at"] != when:
                break
            ordinal += 1
    else:
        existing = await (await db.execute(
            """SELECT * FROM ledger_events
                WHERE user_id = ? AND canonical_uid = ?""",
            (user_id, canonical),
        )).fetchone()

    if existing is None:
        cursor = await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid,
                   source_type, source_calendar_id, source_event_id,
                   summary, description, location,
                   start_at, end_at, start_timezone, end_timezone,
                   is_all_day,
                   show_as, recurrence_rule_json, is_recurring,
                   status, version,
                   created_at, updated_at, last_seen_at)
               VALUES (?, ?,
                       'webcal', ?, ?,
                       ?, ?, ?,
                       ?, ?, ?, ?,
                       ?,
                       ?, ?, ?,
                       'active', 1,
                       ?, ?, ?)""",
            (
                user_id, canonical,
                subscription_id, uid,
                fields["summary"], fields["description"], fields["location"],
                fields["start_at"], fields["end_at"],
                fields["start_timezone"], fields["end_timezone"],
                fields["is_all_day"],
                fields["show_as"], fields["recurrence_rule_json"],
                fields["is_recurring"],
                when, when, when,
            ),
        )
        return "created", int(cursor.lastrowid), canonical

    old_hash = _ics_content_hash_from_row(existing)
    new_hash = _content_hash(fields)
    if old_hash == new_hash:
        await db.execute(
            "UPDATE ledger_events SET last_seen_at = ? WHERE id = ?",
            (when, int(existing["id"])),
        )
        return "skipped", int(existing["id"]), canonical

    await db.execute(
        """UPDATE ledger_events
              SET summary = ?, description = ?, location = ?,
                  start_at = ?, end_at = ?,
                  start_timezone = ?, end_timezone = ?, is_all_day = ?,
                  show_as = ?, recurrence_rule_json = ?, is_recurring = ?,
                  status = 'active',
                  version = version + 1,
                  updated_at = ?, last_seen_at = ?
            WHERE id = ?""",
        (
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"],
            fields["start_timezone"], fields["end_timezone"],
            fields["is_all_day"],
            fields["show_as"], fields["recurrence_rule_json"],
            fields["is_recurring"],
            when, when, int(existing["id"]),
        ),
    )
    return "updated", int(existing["id"]), canonical


async def _ingest_ics_instance(
    db: aiosqlite.Connection,
    *,
    user_id: int,
    subscription_id: int,
    event: dict,
    parent_uid: str,
    now: datetime,
) -> tuple[str, Optional[int], str]:
    """Upsert a modified or cancelled RECURRENCE-ID override as an
    instance ledger row (``parent_canonical_uid`` set), mirroring how
    client/main recurring instances are handled.  The planner drives
    it ABSENT when cancelled; the diff derives the per-instance
    Google ID from the parent series' projection."""
    parent_canonical = canonical_uid_webcal_stable(subscription_id, parent_uid)
    original_start = event["recurrence_id"]
    # An all-day RECURRENCE-ID (;VALUE=DATE) is formatted YYYY-MM-DD
    # with no time component; a timed one carries a 'T'.  The diff
    # needs this to build the correct Google instance-ID stamp.
    instance_is_all_day = bool(original_start) and "T" not in original_start
    instance_canonical = canonical_uid_for_instance(
        parent_canonical, original_start,
    )
    when = now.isoformat()
    existing = await (await db.execute(
        """SELECT * FROM ledger_events
            WHERE user_id = ? AND canonical_uid = ?""",
        (user_id, instance_canonical),
    )).fetchone()

    # Cancelled override — sticky cancelled instance row.
    if event["status"] == "CANCELLED":
        if existing is not None and existing["status"] == "cancelled":
            return "skipped", int(existing["id"]), instance_canonical
        if existing is None:
            cursor = await db.execute(
                """INSERT INTO ledger_events
                      (user_id, canonical_uid, parent_canonical_uid,
                       source_type, source_calendar_id, source_event_id,
                       recurrence_instance_original_start, is_all_day,
                       status, version, is_recurring,
                       created_at, updated_at, last_seen_at, cancelled_at)
                   VALUES (?, ?, ?, 'webcal', ?, ?, ?, ?,
                           'cancelled', 1, 0, ?, ?, ?, ?)""",
                (
                    user_id, instance_canonical, parent_canonical,
                    subscription_id, parent_uid, original_start,
                    instance_is_all_day,
                    when, when, when, when,
                ),
            )
            return "cancelled", int(cursor.lastrowid), instance_canonical
        await db.execute(
            """UPDATE ledger_events
                  SET status = 'cancelled', is_all_day = ?,
                      version = version + 1,
                      cancelled_at = ?, updated_at = ?, last_seen_at = ?
                WHERE id = ?""",
            (instance_is_all_day, when, when, when, int(existing["id"])),
        )
        return "cancelled", int(existing["id"]), instance_canonical

    # Modified (active) override — a single-occurrence content change.
    fields = _ics_to_ledger_fields(event)
    if existing is None:
        cursor = await db.execute(
            """INSERT INTO ledger_events
                  (user_id, canonical_uid, parent_canonical_uid,
                   source_type, source_calendar_id, source_event_id,
                   recurrence_instance_original_start,
                   summary, description, location,
                   start_at, end_at, start_timezone, end_timezone,
                   is_all_day, show_as,
                   recurrence_rule_json, is_recurring,
                   status, version,
                   created_at, updated_at, last_seen_at)
               VALUES (?, ?, ?, 'webcal', ?, ?, ?,
                       ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                       'active', 1, ?, ?, ?)""",
            (
                user_id, instance_canonical, parent_canonical,
                subscription_id, parent_uid, original_start,
                fields["summary"], fields["description"], fields["location"],
                fields["start_at"], fields["end_at"],
                fields["start_timezone"], fields["end_timezone"],
                fields["is_all_day"],
                fields["show_as"], fields["recurrence_rule_json"],
                fields["is_recurring"],
                when, when, when,
            ),
        )
        return "created", int(cursor.lastrowid), instance_canonical

    new_hash = _content_hash(fields)
    old_hash = _ics_content_hash_from_row(existing)
    if new_hash == old_hash:
        await db.execute(
            "UPDATE ledger_events SET last_seen_at = ? WHERE id = ?",
            (when, int(existing["id"])),
        )
        return "skipped", int(existing["id"]), instance_canonical
    await db.execute(
        """UPDATE ledger_events
              SET summary = ?, description = ?, location = ?,
                  start_at = ?, end_at = ?,
                  start_timezone = ?, end_timezone = ?, is_all_day = ?,
                  show_as = ?, recurrence_rule_json = ?, is_recurring = ?,
                  status = 'active',
                  version = version + 1,
                  updated_at = ?, last_seen_at = ?
            WHERE id = ?""",
        (
            fields["summary"], fields["description"], fields["location"],
            fields["start_at"], fields["end_at"],
            fields["start_timezone"], fields["end_timezone"],
            fields["is_all_day"],
            fields["show_as"], fields["recurrence_rule_json"],
            fields["is_recurring"],
            when, when, int(existing["id"]),
        ),
    )
    return "updated", int(existing["id"]), instance_canonical


def _ics_to_ledger_fields(event: dict) -> dict:
    """Subset of fields used for content-hash equality."""
    return {
        "summary": event.get("summary"),
        "description": event.get("description"),
        "location": event.get("location"),
        "start_at": event.get("start_at"),
        "end_at": event.get("end_at"),
        "start_timezone": event.get("start_timezone"),
        "end_timezone": event.get("end_timezone"),
        "is_all_day": event.get("is_all_day"),
        "show_as": event.get("show_as"),
        "recurrence_rule_json": event.get("recurrence_rule_json"),
        "is_recurring": event.get("is_recurring"),
    }


def _ics_content_hash_from_row(row) -> str:
    return _content_hash({
        "summary": row["summary"],
        "description": row["description"],
        "location": row["location"],
        "start_at": row["start_at"],
        "end_at": row["end_at"],
        "start_timezone": row["start_timezone"],
        "end_timezone": row["end_timezone"],
        "is_all_day": bool(row["is_all_day"]),
        "show_as": row["show_as"],
        "recurrence_rule_json": row["recurrence_rule_json"],
        "is_recurring": bool(row["is_recurring"]),
    })


# ---------------------------------------------------------------------------
# Sub state + helpers
# ---------------------------------------------------------------------------
async def _get_subscription(
    db: aiosqlite.Connection, *, subscription_id: int,
) -> Optional[aiosqlite.Row]:
    return await (await db.execute(
        "SELECT * FROM webcal_subscriptions WHERE id = ?",
        (subscription_id,),
    )).fetchone()


async def _record_fetch_success(
    db: aiosqlite.Connection,
    subscription_id: int,
    *,
    etag: Optional[str],
    now: datetime,
) -> None:
    # Production schema uses ``last_poll_at`` + ``last_success_at``;
    # test schema uses ``last_polled_at``.  Update each that exists.
    iso = now.isoformat()
    for column in ("last_poll_at", "last_polled_at"):
        try:
            await db.execute(
                f"UPDATE webcal_subscriptions SET {column} = ? WHERE id = ?",
                (iso, subscription_id),
            )
        except Exception:
            pass
    for column in ("last_success_at",):
        try:
            await db.execute(
                f"UPDATE webcal_subscriptions SET {column} = ? WHERE id = ?",
                (iso, subscription_id),
            )
        except Exception:
            pass
    await db.execute(
        """UPDATE webcal_subscriptions
              SET last_etag = ?,
                  consecutive_failures = 0, last_error = NULL
            WHERE id = ?""",
        (etag, subscription_id),
    )


async def _record_fetch_failure(
    db: aiosqlite.Connection,
    subscription_id: int,
    error: str,
) -> None:
    await db.execute(
        """UPDATE webcal_subscriptions
              SET consecutive_failures = consecutive_failures + 1,
                  last_error = ?
            WHERE id = ?""",
        (error[:1000], subscription_id),
    )
    await db.commit()


async def _mark_cancelled(
    db: aiosqlite.Connection, *, ledger_event_id: int, now: datetime,
) -> None:
    when = now.isoformat()
    await db.execute(
        """UPDATE ledger_events
              SET status = 'cancelled',
                  version = version + 1,
                  cancelled_at = ?, updated_at = ?, last_seen_at = ?
            WHERE id = ?""",
        (when, when, when, ledger_event_id),
    )
