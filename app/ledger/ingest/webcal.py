"""Webcal/ICS ingest.

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

The fetch step is parametrised: a ``fetch`` callable (see
``FetchHook`` below) accepts ``(url, if_none_match)`` and returns
a dict with ``status`` / ``etag`` / ``body`` keys.  In tests we
drive it from an in-memory dict; in production the hook wraps the
SSRF-guarded httpx fetch in ``app/utils/ics_fetch.py`` (wired up
in ``app/ledger/runtime.py``).
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

try:  # zoneinfo is stdlib on 3.9+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - defensive
    ZoneInfo = None  # type: ignore[assignment]

try:
    # icalendar ships Microsoft's Windows-timezone -> Olson/IANA table
    # (e.g. "W. Europe Standard Time" -> "Europe/Berlin").  Feeds from
    # Outlook/Exchange commonly use these as TZIDs.
    from icalendar.timezone.windows_to_olson import WINDOWS_TO_OLSON
except Exception:  # pragma: no cover - defensive
    WINDOWS_TO_OLSON = {}

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
        "errors": 0, "empty_skipped": 0, "failed": 0,
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
        try:
            outcome, ledger_id, canonical = await _ingest_ics_event(
                db,
                user_id=user_id,
                subscription_id=subscription_id,
                event=ev,
                now=now,
            )
        except Exception:
            # Isolate per-event failures.  A single poison VEVENT must
            # NOT abort the poll — that would skip _record_fetch_success,
            # freeze the etag, refail the identical body on every poll,
            # and silently stop every later VEVENT from ingesting.  Log
            # loudly, skip this one, keep going; stale-detection below is
            # suppressed for this poll so the failed event's existing
            # busy block is not misread as "gone from feed".
            logger.exception(
                "webcal ingest: skipping event uid=%s on sub=%s "
                "after error",
                ev.get("uid"), subscription_id,
            )
            counters["failed"] = counters.get("failed", 0) + 1
            continue
        counters[outcome] = counters.get(outcome, 0) + 1
        if ledger_id is not None:
            affected_ledger_ids.append(ledger_id)
            seen_canonical_uids.add(canonical)

    # Empty-feed guard.  A provider reset/outage can serve HTTP 200 with
    # a parseable but EVENTLESS VCALENDAR.  Letting that drive
    # stale-detection would cancel every busy block this feed produced —
    # the user would show FREE for real commitments and get double-booked
    # (the worst outcome for this tool).  We cannot tell "feed is
    # genuinely empty now" from "feed is mid-outage" no matter how long it
    # lasts, so an empty poll NEVER mass-cancels while the subscription
    # still holds active events.  Individual events dropping out of a
    # NON-empty feed are still cancelled below.  A consecutive-empty
    # counter is recorded and logged so a genuinely-dead feed is visible
    # to the operator (who can remove the subscription); a non-empty poll
    # resets it.
    keys = state.keys() if hasattr(state, "keys") else []
    prior_empty = (
        int(state["consecutive_empty_polls"])
        if "consecutive_empty_polls" in keys
        and state["consecutive_empty_polls"] is not None
        else 0
    )
    if counters["seen"] == 0:
        active_count = int((await (await db.execute(
            """SELECT COUNT(*) AS c FROM ledger_events
                WHERE user_id = ? AND source_type = 'webcal'
                  AND source_calendar_id = ? AND status = 'active'""",
            (user_id, subscription_id),
        )).fetchone())["c"])
        if active_count > 0:
            empties = prior_empty + 1
            await _set_consecutive_empty_polls(db, subscription_id, empties)
            logger.warning(
                "webcal sub=%s returned an empty-but-valid feed (%d "
                "consecutive) while holding %d active event(s); skipping "
                "stale-cancellation so a feed reset/outage cannot wipe real "
                "busy blocks. If the feed is genuinely empty, remove the "
                "subscription to clear them.",
                subscription_id, empties, active_count,
            )
            counters["empty_skipped"] = 1
            await _record_fetch_success(db, subscription_id, etag=etag, now=now)
            await db.commit()
            return counters
        # No active rows to protect — nothing to cancel; fall through.
    elif prior_empty:
        await _set_consecutive_empty_polls(db, subscription_id, 0)

    # Stale-detection: anything previously sourced from this
    # subscription but not in this poll's seen set, AND not seen
    # for >= 2 poll intervals, flips to cancelled.
    # Production schema stores poll_interval_minutes; some test
    # schemas use poll_interval_seconds.  Tolerate either.
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
    # A poll with per-event failures cannot drive stale-detection: a
    # failed event never reached the seen set (its canonical UID may
    # not even be computable — the failure can precede the unstable-UID
    # ordinal probe), so it would be misread as "gone from feed" and
    # its real busy block cancelled.  Not cancelling is the safe
    # direction (same reasoning as the empty-feed guard above); a
    # later clean poll performs any genuinely-needed cancellation.
    if counters["failed"]:
        logger.warning(
            "webcal sub=%s: %d event(s) failed this poll; skipping "
            "stale-cancellation until a clean poll.",
            subscription_id, counters["failed"],
        )
        rows = []
    else:
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
        dtstart, dtend, comp.get("DURATION"),
    )

    # The zone naive local times in this VEVENT (floating UNTIL,
    # floating RECURRENCE-ID) are interpreted in: the DTSTART's own
    # tzinfo when present (works for VTIMEZONE-derived custom TZIDs
    # too), else UTC — a floating DTSTART is itself treated as UTC by
    # ``_normalize_times``, so its companions must resolve the same
    # way or their instants diverge from the series grid.
    event_tz = None
    if dtstart is not None and isinstance(dtstart.dt, datetime):
        event_tz = dtstart.dt.tzinfo or UTC

    # RRULE + EXDATE + RDATE all belong in the `recurrence` array
    # Google materialises instances from.  Dropping EXDATE would
    # leave a ghost busy block on every excluded occurrence.
    recurrence_rule = _extract_recurrence(
        comp, event_tz=event_tz, is_all_day=is_all_day,
    )

    # A VEVENT carrying RECURRENCE-ID is a single-occurrence override
    # of the series with the same UID — a modified or cancelled
    # instance, not a standalone event.
    recurrence_id = _format_recurrence_id(
        comp.get("RECURRENCE-ID"), fallback_tz=event_tz,
    )

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


def _extract_recurrence(
    comp, *, event_tz=None, is_all_day: bool = False,
) -> Optional[list[str]]:
    """Collect RRULE / EXDATE / RDATE lines for the Google
    ``recurrence`` array.  Each property may appear more than once
    (common for EXDATE).

    Serialise via ``content_line`` so property PARAMETERS survive:
    ``item.to_ical()`` alone drops ``;VALUE=DATE`` / ``;TZID=...``,
    turning an all-day exclusion into an invalid line and a
    zoned one into a floating time that excludes the wrong instant —
    Google gets these lines verbatim as its ``recurrence`` array.

    RRULE lines on TIMED events are post-processed so UNTIL is RFC
    5545 compliant (see ``_rrule_until_to_utc``): Google rejects the
    whole series (HTTP 400, a permanent poison-pill) when UNTIL is a
    local/floating datetime under a zoned DTSTART."""
    lines: list[str] = []
    for key in ("RRULE", "EXDATE", "RDATE"):
        val = comp.get(key)
        if val is None:
            continue
        items = val if isinstance(val, list) else [val]
        for item in items:
            try:
                # Full RFC 5545 content line WITH parameters, e.g.
                # ``EXDATE;TZID=America/New_York:20260217T090000``.
                line = str(comp.content_line(key, item))
            except Exception:
                if hasattr(item, "to_ical"):
                    try:
                        line = f"{key}:" + item.to_ical().decode("ascii")
                    except Exception:
                        line = f"{key}:" + str(item)
                else:
                    line = f"{key}:" + str(item)
            if key == "RRULE" and not is_all_day:
                line = _rrule_until_to_utc(line, event_tz or UTC)
            lines.append(line)
    return lines or None


# UNTIL in an RRULE content line: date part, optional time part,
# optional trailing Z.
_RRULE_UNTIL_RE = re.compile(
    r"(UNTIL=)(\d{8})(?:T(\d{6})(Z?))?", re.IGNORECASE,
)


def _rrule_until_to_utc(line: str, tz) -> str:
    """Rewrite a non-compliant UNTIL on a TIMED event's RRULE to UTC.

    RFC 5545: when DTSTART is zoned, a DATE-TIME UNTIL MUST be UTC
    (trailing ``Z``).  Google enforces this and 400s the whole insert —
    the series then never mirrors.  Two repairs:

    * naive datetime UNTIL (``UNTIL=20261221T090000``) — interpret it
      in the event's resolved zone and rewrite as ``...T...Z``;
    * date-only UNTIL on a timed event — expand to the END of that
      local day (23:59:59 local, then UTC), so the final day's
      occurrence is not wrongly truncated (a bare date would otherwise
      be read as local/UTC midnight, cutting the last instance).

    A compliant ``...Z`` UNTIL and all-day events pass through
    unchanged (callers skip all-day)."""
    def repl(m: re.Match) -> str:
        if m.group(4):  # already UTC (trailing Z) — compliant
            return m.group(0)
        try:
            if m.group(3):
                local = datetime.strptime(
                    m.group(2) + m.group(3), "%Y%m%d%H%M%S",
                ).replace(tzinfo=tz)
            else:
                local = datetime.strptime(m.group(2), "%Y%m%d").replace(
                    hour=23, minute=59, second=59, tzinfo=tz,
                )
            return m.group(1) + local.astimezone(UTC).strftime(
                "%Y%m%dT%H%M%SZ",
            )
        except Exception:
            return m.group(0)
    return _RRULE_UNTIL_RE.sub(repl, line)


def _format_recurrence_id(rid, fallback_tz=None) -> Optional[str]:
    """Normalise a RECURRENCE-ID property to the same string shape
    used for ``recurrence_instance_original_start``.

    A NAIVE (floating) RECURRENCE-ID names the occurrence by the
    series' local wall-clock, so it is interpreted in ``fallback_tz``
    (the parent DTSTART's resolved tzinfo) and converted to UTC.
    Reading it as UTC would target an instant off by the zone offset —
    the override/cancellation then never matches the real occurrence
    and is silently lost.  True-UTC (``Z``) and TZID-carrying values
    are aware and convert as before."""
    if rid is None:
        return None
    dt = getattr(rid, "dt", None)
    if dt is None:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is not None:
            d = dt.astimezone(UTC)
        elif fallback_tz is not None:
            d = dt.replace(tzinfo=fallback_tz).astimezone(UTC)
        else:
            d = dt.replace(tzinfo=UTC)
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return dt.isoformat()  # date-only → YYYY-MM-DD


def _first_str(comp, key: str) -> Optional[str]:
    v = comp.get(key)
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def _normalize_times(
    dtstart, dtend, duration=None,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str], bool]:
    """Return ``(start_at, end_at, start_timezone, end_timezone,
    is_all_day)``.  All-day events keep their ``YYYY-MM-DD`` shape and
    carry no zone; timed events get an ISO-8601 UTC string plus the
    IANA zone their RRULE expands in (so a mirrored recurring webcal
    event stays correct across DST instead of drifting onto a fixed
    UTC grid).

    RFC 5545 allows DTSTART + DURATION instead of DTEND; honour it so
    e.g. a 2-hour meeting is not shrunk to the 30-minute default.  The
    default only applies when NEITHER DTEND nor DURATION is present."""
    if dtstart is None:
        return None, None, None, None, False
    sdt = dtstart.dt
    edt = dtend.dt if dtend is not None else None
    dur = getattr(duration, "dt", None) if duration is not None else None
    if isinstance(sdt, datetime):
        s = sdt.astimezone(UTC) if sdt.tzinfo else sdt.replace(tzinfo=UTC)
        if isinstance(edt, datetime):
            e = edt.astimezone(UTC) if edt.tzinfo else edt.replace(tzinfo=UTC)
        elif isinstance(dur, timedelta):
            e = s + dur
        else:
            e = s + timedelta(minutes=30)
        return (
            s.strftime("%Y-%m-%dT%H:%M:%SZ"),
            e.strftime("%Y-%m-%dT%H:%M:%SZ"),
            _iana_tz_name(sdt),
            _iana_tz_name(edt) if isinstance(edt, datetime) else _iana_tz_name(sdt),
            False,
        )
    # date-only → all-day
    if edt is not None:
        e = edt
    elif isinstance(dur, timedelta):
        # RFC 5545 restricts an all-day DURATION to whole days/weeks
        # (e.g. P2D); guard a degenerate sub-day value back to one day.
        e = sdt + (dur if dur >= timedelta(days=1) else timedelta(days=1))
    else:
        e = sdt + timedelta(days=1)
    return sdt.isoformat(), e.isoformat(), None, None, True


def _iana_tz_name(dt) -> Optional[str]:
    """The IANA zone key of a datetime's tzinfo, or ``None``.

    icalendar parses ``DTSTART;TZID=America/New_York:...`` into a
    ``zoneinfo.ZoneInfo`` whose ``.key`` is the IANA name.  A UTC /
    fixed-offset / floating time has no ``.key`` — return ``None`` so
    the renderer falls back to UTC.

    A CUSTOM/LOCALIZED TZID (e.g. ``Mitteleuropaeische Zeit`` from a
    German Outlook export) parses into a VTIMEZONE-derived tzinfo with
    no ``.key`` either; returning ``None`` for those silently anchors
    the series' RRULE on a fixed UTC grid that drifts an hour at every
    DST change — the user shows free during real meetings.  Resolve
    those to a real IANA zone instead (see ``_resolve_non_iana_tz``)."""
    tz = getattr(dt, "tzinfo", None)
    if tz is None:
        return None
    key = getattr(tz, "key", None)
    if key is not None:
        # "UTC" is not a drift-prone zone — treat it as "no zone" so the
        # renderer's UTC fallback applies and the column stays NULL.
        return None if key == "UTC" else key
    # datetime.timezone instances (timezone.utc / fixed offsets from
    # ``...Z`` or ``±HH:MM`` stamps) carry no wall-clock rules to
    # preserve — keep the historical UTC fallback for those.
    if isinstance(tz, timezone):
        return None
    return _resolve_non_iana_tz(tz, dt)


# Localized Windows/Outlook TZID display names seen in real feeds that
# WINDOWS_TO_OLSON (English names only) misses.  Deliberately short:
# the offset-probe fallback below covers the long tail; these just give
# a deterministic answer for the most common European Outlook locales.
# Keys are casefolded.
_LOCALIZED_TZID_ALIASES = {
    # German Outlook ("W. Europe Standard Time" localized), with and
    # without the umlaut transliteration.
    "mitteleuropäische zeit": "Europe/Berlin",
    "mitteleuropaeische zeit": "Europe/Berlin",
    "mitteleuropäische sommerzeit": "Europe/Berlin",
    "mitteleuropaeische sommerzeit": "Europe/Berlin",
    # French Outlook ("Romance Standard Time" localized).
    "heure d'europe centrale": "Europe/Paris",
    "heure de l'europe centrale": "Europe/Paris",
    # Spanish Outlook ("Romance Standard Time" localized).
    "hora de europa central": "Europe/Madrid",
    "hora estándar de europa central": "Europe/Madrid",
}

# Shortlist for the offset-probe fallback: common zones ordered so the
# first (January-offset, July-offset) match wins.  Zones that share
# both probe offsets (Berlin/Paris/Madrid, ...) are interchangeable for
# grid purposes — they produce identical instants for every occurrence
# — so picking the first is safe even though the city may be "wrong".
_OFFSET_PROBE_ZONES = (
    "Europe/London",
    "Europe/Berlin",
    "Europe/Helsinki",
    "Europe/Moscow",
    "America/New_York",
    "America/Chicago",
    "America/Denver",
    "America/Phoenix",
    "America/Los_Angeles",
    "America/Sao_Paulo",
    "Asia/Kolkata",
    "Asia/Shanghai",
    "Asia/Tokyo",
    "Australia/Sydney",
    "Pacific/Auckland",
)


def _resolve_non_iana_tz(tz, dt) -> Optional[str]:
    """Best-effort IANA name for a VTIMEZONE-derived (non-IANA) tzinfo.

    Resolution order:

    1. icalendar's Windows->Olson table on the raw TZID string
       (``W. Europe Standard Time`` -> ``Europe/Berlin``);
    2. a small alias table for localized Outlook TZID names;
    3. derive from the VTIMEZONE's actual behaviour: probe the
       tzinfo's utcoffset at two instants (mid-January and mid-July of
       the event's year — opposite sides of every DST transition) and
       match the (winter, summer) offset pair against a shortlist of
       common zones.  An offset-pair match pins down the zone's entire
       wall-clock grid for practical purposes, which is exactly what
       the RRULE expansion needs;
    4. give up with a WARNING naming the TZID (visible failure) and
       return ``None`` — the caller keeps the historical UTC fallback.
    """
    # The raw TZID string: dateutil's VTIMEZONE tzinfo (_tzicalvtz)
    # stores it as ``_tzid``; other implementations may use ``zone``.
    tzid = getattr(tz, "_tzid", None) or getattr(tz, "zone", None)
    if tzid:
        tzid = str(tzid)
        if tzid.casefold() in ("utc", "etc/utc", "gmt", "z"):
            return None  # same "no zone" treatment as key == "UTC"
        for candidate in (
            # A custom VTIMEZONE may still carry a genuine IANA TZID
            # (e.g. a pytz-style tzinfo exposing ``.zone``).
            tzid if _is_valid_zone(tzid) else None,
            WINDOWS_TO_OLSON.get(tzid),
            _LOCALIZED_TZID_ALIASES.get(tzid.strip().casefold()),
        ):
            if candidate and _is_valid_zone(candidate):
                return candidate
    year = getattr(dt, "year", None) or datetime.now(UTC).year
    if ZoneInfo is not None:
        jan = datetime(year, 1, 15, 12, 0)
        jul = datetime(year, 7, 15, 12, 0)
        try:
            offsets = (tz.utcoffset(jan), tz.utcoffset(jul))
        except Exception:
            offsets = (None, None)
        if None not in offsets:
            for name in _OFFSET_PROBE_ZONES:
                try:
                    z = ZoneInfo(name)
                except Exception:  # pragma: no cover - tzdata gap
                    continue
                if (z.utcoffset(jan), z.utcoffset(jul)) == offsets:
                    return name
    logger.warning(
        "webcal: could not resolve TZID %r to an IANA zone; recurring "
        "events in it will expand on a fixed UTC grid and may drift "
        "across DST changes.",
        tzid,
    )
    return None


def _is_valid_zone(name: str) -> bool:
    if ZoneInfo is None:  # pragma: no cover - defensive
        return False
    try:
        ZoneInfo(name)
        return True
    except Exception:
        return False


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
        if existing["status"] in ("cancelled", "released"):
            # 'released' = retired from sync (frozen on the calendars on
            # purpose); a feed cancellation must not delete its copy.
            return "skipped", None, canonical
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

    # A 'released' event was retired from sync by retention (frozen on the
    # calendars on purpose); never re-ingest or un-release it.
    if existing is not None and existing["status"] == "released":
        return "skipped", None, canonical

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


async def _set_consecutive_empty_polls(
    db: aiosqlite.Connection, subscription_id: int, value: int,
) -> None:
    """Persist the consecutive-empty-poll counter.

    Tolerant of a schema without the column (older test schemas): if the
    UPDATE fails the counter stays 0, which only makes the empty-feed
    guard MORE conservative (it still skips cancellation), so the safe
    behaviour is preserved either way.
    """
    try:
        await db.execute(
            "UPDATE webcal_subscriptions SET consecutive_empty_polls = ? "
            "WHERE id = ?",
            (int(value), subscription_id),
        )
    except Exception:
        pass


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
