"""Shared recurrence helpers: RRULE expansion, occurrence keying, and
coverage routing for "this and following" (``_R``) splits.

Lifted out of ``planner.py`` so the planner's parent-prune predicate, the
main-ingest orphan check, and any future owning-segment router all share one
implementation of:

* the occurrence-key convention (UTC-normalised timed instant / date-only
  all-day), so keys produced on the expansion side and the stored-instance
  side always match; and
* the RRULE expansion semantics, anchored in the series' IANA ``timeZone``
  so a DST-crossing series expands on the correct wall-clock grid (the fixed
  start offset alone mis-computes every occurrence after a DST transition).

Pure functions only — no DB or Google access.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from itertools import islice
from typing import Optional
import re

from dateutil.rrule import rrulestr

try:  # zoneinfo is stdlib on 3.9+, used elsewhere in ingest.
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except Exception:  # pragma: no cover - defensive
    ZoneInfo = None  # type: ignore[assignment]
    ZoneInfoNotFoundError = Exception  # type: ignore[assignment,misc]

UTC = timezone.utc

# Google's server-generated "this and following" split suffix is
# ``<originalParentId>_R<YYYYMMDD>T<HHMMSS>`` — a compact timestamp.  The trailing
# ``Z`` shown in some Google docs is OPTIONAL and is in fact absent on the real
# ids this deployment receives (observed in production), so we accept it but do
# not require it.  The ``R`` and ``T`` are uppercase, which are ILLEGAL in
# client-supplied event ids (Google's client alphabet is base32hex: lowercase
# a-v + 0-9), so this anchored pattern cannot false-match a legitimate base id
# even without the ``Z``.  Google chains splits (``<base>_R<ts1>_R<ts2>``), so
# strip iteratively to the ultimate base.
_R_SUFFIX_RE = re.compile(r"_R\d{8}T\d{6}Z?$")


def strip_r_suffix(event_id: str) -> str:
    """Strip every trailing ``_R<stamp>`` split-suffix, returning the
    ultimate base series id (the family key shared by base + every segment)."""
    if not event_id:
        return event_id
    while True:
        m = _R_SUFFIX_RE.search(event_id)
        if not m:
            return event_id
        event_id = event_id[: m.start()]


# ---------------------------------------------------------------------------
# Recurrence-line parsing
# ---------------------------------------------------------------------------
def parse_recurrence_lines(value: Optional[str]) -> Optional[list[str]]:
    """Decode a JSON-encoded list of iCal recurrence lines, or ``None``."""
    if not value:
        return None
    try:
        parsed = json.loads(value)
    except Exception:
        return None
    if not isinstance(parsed, list):
        return None
    lines = [str(item) for item in parsed if item]
    return lines or None


def looks_finite_recurrence(lines: list[str]) -> bool:
    """True if any RRULE carries COUNT/UNTIL, or an RDATE is present."""
    for line in lines:
        upper = line.upper()
        if upper.startswith("RRULE:") and ("COUNT=" in upper or "UNTIL=" in upper):
            return True
        if upper.startswith("RDATE"):
            return True
    return False


# ---------------------------------------------------------------------------
# Datetime parsing
# ---------------------------------------------------------------------------
def parse_instant(value: Optional[str], *, is_all_day: bool) -> Optional[datetime]:
    """Parse a STORED occurrence instant (an originalStartTime string).

    The stored value already encodes the instant (offset-aware or ``Z``, or a
    bare ``YYYY-MM-DD`` for all-day), so no IANA zone is needed.  All-day →
    naive date-midnight; timed → tz-aware (UTC assumed when naive).
    """
    if not value:
        return None
    try:
        if is_all_day:
            return datetime.fromisoformat(value[:10])
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def series_dtstart(
    start_at: Optional[str],
    start_timezone: Optional[str],
    *,
    is_all_day: bool,
) -> Optional[datetime]:
    """Anchor a recurring series' dtstart for RRULE expansion.

    Crucially DST-aware: a timed series is anchored in its IANA
    ``start_timezone`` (e.g. ``America/New_York``) so dateutil expands on the
    correct wall-clock grid and post-DST occurrences land at the right UTC
    instant.  Expanding on the series' fixed start offset alone shifts every
    occurrence after a DST transition by an hour — the cause of the
    cancel-all "ghost recurring mirror" bug.  All-day → naive date-midnight.

    Two ``start_at`` shapes are handled distinctly (mirroring
    ``ingest/client.py._resolve_original_start``): an OFFSET-AWARE instant
    (``...Z`` / ``±HH:MM``) is an absolute moment, re-expressed in the IANA
    zone; a NAIVE wall-time is local to ``start_timezone`` (some sources send a
    naive ``dateTime`` plus a separate IANA ``timeZone``) and must be anchored
    there — NOT read as UTC, which would shift the whole RRULE grid by the zone
    offset and make a covered occurrence look uncovered.
    """
    if is_all_day:
        if not start_at:
            return None
        try:
            return datetime.fromisoformat(start_at[:10])
        except (TypeError, ValueError):
            return None
    if not start_at:
        return None
    try:
        dt = datetime.fromisoformat(start_at.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    zone = None
    if start_timezone and start_timezone != "UTC" and ZoneInfo is not None:
        try:
            zone = ZoneInfo(start_timezone)
        except (ZoneInfoNotFoundError, ValueError, KeyError):
            zone = None
    if dt.tzinfo is None:
        # Naive wall-time: local to the IANA zone if one was supplied, else UTC.
        return dt.replace(tzinfo=zone) if zone is not None else dt.replace(tzinfo=UTC)
    # Offset-aware absolute instant: express in the IANA zone (if any) so the
    # RRULE expands on the correct wall-clock grid across DST.
    return dt.astimezone(zone) if zone is not None else dt.astimezone(UTC)


# ---------------------------------------------------------------------------
# Occurrence keys
# ---------------------------------------------------------------------------
def occurrence_key_from_datetime(dt: datetime, *, is_all_day: bool) -> str:
    """Canonical occurrence key: date-only (all-day) or UTC instant (timed)."""
    if is_all_day:
        return dt.date().isoformat()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).replace(microsecond=0).isoformat()


def occurrence_key(value: Optional[str], *, is_all_day: bool) -> Optional[str]:
    """Occurrence key for a STORED instant string, or ``None``."""
    dt = parse_instant(value, is_all_day=is_all_day)
    if dt is None:
        return None
    return occurrence_key_from_datetime(dt, is_all_day=is_all_day)


# ---------------------------------------------------------------------------
# Expansion + coverage
# ---------------------------------------------------------------------------
# A datetime-form ``UNTIL`` (``YYYYMMDDTHHMMSSZ``) on an all-day series, whose
# dtstart we build NAIVE, makes dateutil reject the rule ("UNTIL must be UTC
# when DTSTART is timezone-aware" / awareness mismatch).  Google may truncate
# an all-day series with either a DATE or a Z-stamped UNTIL; normalising a
# datetime UNTIL to its date part for a naive dtstart is awareness-consistent
# and does not change the all-day boundary (the date is what matters).
_UNTIL_DATETIME_RE = re.compile(r"(UNTIL=)(\d{8})T\d{6}Z?", re.IGNORECASE)


def _align_recurrence_for_dtstart(
    recurrence_lines: list[str], dtstart: datetime,
) -> list[str]:
    if dtstart.tzinfo is not None:
        return recurrence_lines
    return [
        _UNTIL_DATETIME_RE.sub(lambda m: m.group(1) + m.group(2), line)
        for line in recurrence_lines
    ]


def expand_occurrences(
    recurrence_lines: Optional[list[str]],
    dtstart: Optional[datetime],
    *,
    cap: int = 1000,
) -> Optional[list[datetime]]:
    """Expand a series' occurrences (up to ``cap``).

    Returns ``None`` when the recurrence is missing, unparseable, or expands
    past ``cap`` (infinite / very large) — callers should treat that
    conservatively (e.g. "leave the parent live").
    """
    if not recurrence_lines or dtstart is None:
        return None
    recurrence_lines = _align_recurrence_for_dtstart(recurrence_lines, dtstart)
    try:
        rule = rrulestr("\n".join(recurrence_lines), dtstart=dtstart, forceset=True)
        occurrences = list(islice(rule, cap + 1))
    except Exception:
        return None
    if len(occurrences) > cap:
        return None
    return occurrences


def occurrence_in_series(
    recurrence_lines: Optional[list[str]],
    dtstart: Optional[datetime],
    target: Optional[datetime],
    *,
    is_all_day: bool,
) -> Optional[bool]:
    """Tri-state coverage probe: does the live RRULE expand to ``target``?

    Returns ``True``/``False`` only when it can decide with confidence;
    ``None`` when it cannot (missing/unparseable recurrence, bad dtstart).
    Callers MUST treat ``None`` conservatively — never delete a mirror on an
    indeterminate answer.  Mirrors the fake's ``_is_dt_in_recurrence`` window
    semantics so routing/coverage decisions match Google's 404 behaviour, and
    works for infinite rules via a bounded ``between`` probe.
    """
    if not recurrence_lines or dtstart is None or target is None:
        return None
    recurrence_lines = _align_recurrence_for_dtstart(recurrence_lines, dtstart)
    try:
        rule = rrulestr("\n".join(recurrence_lines), dtstart=dtstart, forceset=True)
    except Exception:
        return None
    target_key = occurrence_key_from_datetime(target, is_all_day=is_all_day)
    if is_all_day:
        lo = target - timedelta(days=1)
        hi = target + timedelta(days=1)
    else:
        lo = target - timedelta(seconds=1)
        hi = target + timedelta(seconds=1)
    try:
        occurrences = rule.between(lo, hi, inc=True)
    except Exception:
        return None
    for occ in occurrences:
        occ_aware = occ
        if not is_all_day and occ.tzinfo is None:
            occ_aware = occ.replace(tzinfo=UTC)
        if occurrence_key_from_datetime(occ_aware, is_all_day=is_all_day) == target_key:
            return True
    return False
