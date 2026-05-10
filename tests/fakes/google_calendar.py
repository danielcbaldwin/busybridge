"""In-memory fake of the Google Calendar v3 API.

This fake is faithful to the documented quirks listed in
``REWRITE_PLAN.md`` §13 Stage 1:

* In-memory event store keyed by calendar.
* Client-supplied ``id`` on insert with 409 conflict semantics.
* ETag versioning with ``If-Match`` → 412 Precondition Failed on
  mismatch.
* Incremental sync tokens with realistic expiry (configurable;
  default 30 simulated days, mirroring Google's documented minimum).
* Recurring-event instance ID derivation
  (``parentId_YYYYMMDDTHHMMSSZ`` for timed events,
  ``parentId_YYYYMMDD`` for all-day).
* The ``_R`` "this and following" reschedule quirk.
* The full-sync-omits-cancelled-instances quirk.

The fake exposes a flat Python interface (``insert_event``,
``update_event``, ``list_events``, ...) rather than mimicking the
verbose ``service.events().list().execute()`` chain.  Consumers wrap
it however they like.

All errors are raised as :class:`GoogleApiError` with an HTTP status
code so production code that catches ``HttpError`` by status can be
adapted with a thin shim.
"""

from __future__ import annotations

import copy
import re
import struct
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from dateutil import rrule as _rrule
from dateutil.parser import isoparse

from tests.fakes.clock import SimulatedClock

UTC = timezone.utc

# Per Google docs: client-supplied event IDs must be 5–1024 chars,
# lowercase a–v plus 0–9 (base32hex).
_GOOGLE_ID_RE = re.compile(r"^[a-v0-9]{5,1024}$")

# Real Google sync tokens become invalid after roughly 30 days; the
# exact threshold is undocumented but ~30d is the safe assumption.
DEFAULT_SYNC_TOKEN_TTL = timedelta(days=30)


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class GoogleApiError(Exception):
    """Mimics the surface of ``googleapiclient.errors.HttpError``.

    ``status`` is the HTTP status code; ``reason`` is the short text;
    ``message`` is the long form.  Production code that branches on
    ``e.resp.status`` can branch on ``e.status`` instead.
    """

    def __init__(self, status: int, reason: str, message: str = ""):
        self.status = status
        self.reason = reason
        self.message = message
        super().__init__(f"HTTP {status} {reason}: {message}" if message else f"HTTP {status} {reason}")


def _bad_request(msg: str) -> GoogleApiError:
    return GoogleApiError(400, "Bad Request", msg)


def _not_found(msg: str = "Not Found") -> GoogleApiError:
    return GoogleApiError(404, "Not Found", msg)


def _conflict(msg: str) -> GoogleApiError:
    return GoogleApiError(409, "Conflict", msg)


def _gone(msg: str = "Gone") -> GoogleApiError:
    return GoogleApiError(410, "Gone", msg)


def _precondition_failed(msg: str = "etag mismatch") -> GoogleApiError:
    return GoogleApiError(412, "Precondition Failed", msg)


# ---------------------------------------------------------------------------
# Stored event
# ---------------------------------------------------------------------------
@dataclass
class _StoredEvent:
    """One event row in the in-memory store.

    Stored in fully-rendered ("API response") shape so reads can hand
    out copies without remarshalling.  Mutations go through helper
    methods that bump ``etag``, ``sequence``, ``updated``, and the
    parent calendar's change counter.
    """

    id: str
    etag: str
    status: str  # 'confirmed' or 'cancelled'
    summary: Optional[str]
    description: Optional[str]
    location: Optional[str]
    start: Optional[dict]
    end: Optional[dict]
    attendees: list[dict]
    organizer: Optional[dict]
    transparency: Optional[str]
    visibility: Optional[str]
    color_id: Optional[str]
    extended_properties: dict
    recurrence: Optional[list[str]]
    recurring_event_id: Optional[str]
    original_start_time: Optional[dict]
    created: str
    updated: str
    sequence: int
    # Internal: monotonic per-calendar; used by incremental sync.
    change_seq: int = 0

    def to_api_dict(self) -> dict:
        """Render as the dict shape Google's API returns."""
        out: dict[str, Any] = {
            "kind": "calendar#event",
            "etag": self.etag,
            "id": self.id,
            "status": self.status,
            "created": self.created,
            "updated": self.updated,
            "sequence": self.sequence,
        }
        if self.summary is not None:
            out["summary"] = self.summary
        if self.description is not None:
            out["description"] = self.description
        if self.location is not None:
            out["location"] = self.location
        if self.start is not None:
            out["start"] = copy.deepcopy(self.start)
        if self.end is not None:
            out["end"] = copy.deepcopy(self.end)
        if self.attendees:
            out["attendees"] = copy.deepcopy(self.attendees)
        if self.organizer is not None:
            out["organizer"] = copy.deepcopy(self.organizer)
        if self.transparency is not None:
            out["transparency"] = self.transparency
        if self.visibility is not None:
            out["visibility"] = self.visibility
        if self.color_id is not None:
            out["colorId"] = self.color_id
        if self.extended_properties:
            out["extendedProperties"] = copy.deepcopy(self.extended_properties)
        if self.recurrence is not None:
            out["recurrence"] = list(self.recurrence)
        if self.recurring_event_id is not None:
            out["recurringEventId"] = self.recurring_event_id
        if self.original_start_time is not None:
            out["originalStartTime"] = copy.deepcopy(self.original_start_time)
        return out


@dataclass
class _Calendar:
    """One calendar (collection of events) in the store."""

    id: str
    summary: str
    time_zone: str
    events: dict[str, _StoredEvent] = field(default_factory=dict)
    # Monotonic counter; incremented on every event mutation.  Used
    # as the cursor that backs sync tokens.
    change_counter: int = 0


# ---------------------------------------------------------------------------
# Sync token machinery
# ---------------------------------------------------------------------------
@dataclass
class _SyncTokenState:
    """Per-token bookkeeping for incremental sync."""

    calendar_id: str
    cursor: int
    issued_at: datetime


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def derive_instance_event_id(parent_event_id: str, original_start_time: dict) -> str:
    """Construct the Google instance event ID for one occurrence.

    Mirrors the helper in ``app/sync/google_calendar.py`` so tests
    can construct deterministic instance IDs without depending on
    production code.

    Args:
        parent_event_id: ``id`` of the parent recurring event.
        original_start_time: ``originalStartTime`` dict (either
            ``{"date": "YYYY-MM-DD"}`` or ``{"dateTime": "..."}``).

    Returns:
        ``"<parent>_YYYYMMDD"`` for all-day events,
        ``"<parent>_YYYYMMDDTHHMMSSZ"`` for timed events.
    """
    if "date" in original_start_time:
        return f"{parent_event_id}_{original_start_time['date'].replace('-', '')}"
    dt_str = original_start_time.get("dateTime")
    if not dt_str:
        raise ValueError(
            "originalStartTime has neither 'date' nor 'dateTime': "
            f"{original_start_time!r}"
        )
    if dt_str.endswith("Z"):
        dt = datetime.fromisoformat(dt_str[:-1]).replace(tzinfo=UTC)
    else:
        dt = isoparse(dt_str)
    dt_utc = dt.astimezone(UTC)
    return f"{parent_event_id}_{dt_utc.strftime('%Y%m%dT%H%M%S')}Z"


def _validate_client_id(event_id: str) -> None:
    """Raise 400 if a client-supplied event ID violates Google's rules."""
    if not isinstance(event_id, str):
        raise _bad_request("event id must be a string")
    if not _GOOGLE_ID_RE.match(event_id):
        raise _bad_request(
            "event id must be 5–1024 chars from base32hex alphabet "
            "(lowercase a-v plus 0-9)"
        )


def _new_etag() -> str:
    """Issue a fresh opaque ETag string."""
    return f'"{uuid.uuid4().hex}"'


def _new_event_id() -> str:
    """Issue a server-side event ID (when client did not supply one).

    Real Google generates IDs in the same alphabet as client-supplied
    ones; we use 26 hex-ish chars to keep test logs readable.
    """
    raw = uuid.uuid4().bytes
    # Map to base32hex lowercase.
    import base64
    return base64.b32hexencode(raw).decode().lower().rstrip("=")[:26]


def _to_iso_utc(dt: datetime) -> str:
    """Format ``dt`` as ``YYYY-MM-DDTHH:MM:SS.sssZ`` (Google's style)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt = dt.astimezone(UTC)
    # Google emits millisecond precision with a trailing Z.
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


# ---------------------------------------------------------------------------
# The fake itself
# ---------------------------------------------------------------------------
class FakeGoogleCalendar:
    """Drop-in fake for the parts of Google Calendar v3 the rewrite uses.

    Construct one instance per test; share across calendars and users
    by adding more calendars via :meth:`add_calendar`.

    All time-dependent behaviour (sync token expiry, ``updated``
    timestamps) is driven by the :class:`SimulatedClock` passed in.
    """

    def __init__(
        self,
        clock: Optional[SimulatedClock] = None,
        sync_token_ttl: timedelta = DEFAULT_SYNC_TOKEN_TTL,
    ):
        self._clock = clock or SimulatedClock()
        self._sync_token_ttl = sync_token_ttl
        self._calendars: dict[str, _Calendar] = {}
        self._sync_tokens: dict[str, _SyncTokenState] = {}

    # ------------------------------------------------------------------
    # Calendar lifecycle
    # ------------------------------------------------------------------
    def add_calendar(
        self,
        calendar_id: str,
        summary: str = "",
        time_zone: str = "UTC",
    ) -> dict:
        """Register a new calendar in the store.  Returns its metadata."""
        if calendar_id in self._calendars:
            raise _conflict(f"calendar {calendar_id} already exists")
        cal = _Calendar(id=calendar_id, summary=summary or calendar_id, time_zone=time_zone)
        self._calendars[calendar_id] = cal
        return self._calendar_to_api(cal)

    def list_calendars(self) -> dict:
        return {
            "kind": "calendar#calendarList",
            "items": [self._calendar_to_api(c) for c in self._calendars.values()],
        }

    def get_calendar(self, calendar_id: str) -> dict:
        cal = self._calendars.get(calendar_id)
        if cal is None:
            raise _not_found(f"calendar {calendar_id} not found")
        return self._calendar_to_api(cal)

    @staticmethod
    def _calendar_to_api(cal: _Calendar) -> dict:
        return {
            "kind": "calendar#calendarListEntry",
            "id": cal.id,
            "summary": cal.summary,
            "timeZone": cal.time_zone,
        }

    # ------------------------------------------------------------------
    # Event CRUD
    # ------------------------------------------------------------------
    def insert_event(self, calendar_id: str, body: dict) -> dict:
        """Create an event.

        If ``body['id']`` is supplied:
        * It must satisfy Google's alphabet/length rules → 400 otherwise.
        * If an event with that ID already exists on the calendar
          (including cancelled), raise 409 Conflict.

        On success, returns the freshly-stored event as a dict.
        """
        cal = self._require_calendar(calendar_id)

        # Resolve ID
        if "id" in body and body["id"] is not None:
            event_id = body["id"]
            _validate_client_id(event_id)
            if event_id in cal.events:
                raise _conflict(
                    f"event id {event_id} already exists on calendar {calendar_id}"
                )
        else:
            # Generate a fresh ID; retry on the (vanishingly unlikely)
            # collision.
            for _ in range(8):
                candidate = _new_event_id()
                if candidate not in cal.events:
                    event_id = candidate
                    break
            else:  # pragma: no cover
                raise GoogleApiError(500, "Internal", "could not allocate unique id")

        now_iso = _to_iso_utc(self._clock.now())
        cal.change_counter += 1
        ev = _StoredEvent(
            id=event_id,
            etag=_new_etag(),
            status=body.get("status", "confirmed"),
            summary=body.get("summary"),
            description=body.get("description"),
            location=body.get("location"),
            start=copy.deepcopy(body.get("start")),
            end=copy.deepcopy(body.get("end")),
            attendees=copy.deepcopy(body.get("attendees", []) or []),
            organizer=copy.deepcopy(body.get("organizer")),
            transparency=body.get("transparency"),
            visibility=body.get("visibility"),
            color_id=body.get("colorId"),
            extended_properties=copy.deepcopy(body.get("extendedProperties", {}) or {}),
            recurrence=list(body["recurrence"]) if body.get("recurrence") else None,
            recurring_event_id=body.get("recurringEventId"),
            original_start_time=copy.deepcopy(body.get("originalStartTime")),
            created=now_iso,
            updated=now_iso,
            sequence=int(body.get("sequence", 0) or 0),
            change_seq=cal.change_counter,
        )
        cal.events[event_id] = ev
        return ev.to_api_dict()

    def get_event(self, calendar_id: str, event_id: str) -> dict:
        cal = self._require_calendar(calendar_id)
        ev = cal.events.get(event_id)
        if ev is None:
            raise _not_found(f"event {event_id} not found on {calendar_id}")
        return ev.to_api_dict()

    def update_event(
        self,
        calendar_id: str,
        event_id: str,
        body: dict,
        if_match: Optional[str] = None,
    ) -> dict:
        """Full-replacement update.

        ``if_match`` is the value of the ``If-Match`` header.  If
        supplied, must equal the stored etag or 412 Precondition
        Failed is raised.  ``"*"`` matches anything.
        """
        cal = self._require_calendar(calendar_id)
        ev = cal.events.get(event_id)
        if ev is None or ev.status == "cancelled":
            raise _not_found(f"event {event_id} not found on {calendar_id}")
        self._check_if_match(ev, if_match)

        now_iso = _to_iso_utc(self._clock.now())
        cal.change_counter += 1
        ev.status = body.get("status", "confirmed")
        ev.summary = body.get("summary")
        ev.description = body.get("description")
        ev.location = body.get("location")
        ev.start = copy.deepcopy(body.get("start"))
        ev.end = copy.deepcopy(body.get("end"))
        ev.attendees = copy.deepcopy(body.get("attendees", []) or [])
        ev.organizer = copy.deepcopy(body.get("organizer"))
        ev.transparency = body.get("transparency")
        ev.visibility = body.get("visibility")
        ev.color_id = body.get("colorId")
        ev.extended_properties = copy.deepcopy(body.get("extendedProperties", {}) or {})
        ev.recurrence = list(body["recurrence"]) if body.get("recurrence") else None
        ev.updated = now_iso
        ev.sequence += 1
        ev.etag = _new_etag()
        ev.change_seq = cal.change_counter
        return ev.to_api_dict()

    def patch_event(
        self,
        calendar_id: str,
        event_id: str,
        body: dict,
        if_match: Optional[str] = None,
    ) -> dict:
        """Partial update.  Same If-Match semantics as ``update_event``."""
        cal = self._require_calendar(calendar_id)
        ev = cal.events.get(event_id)
        if ev is None or ev.status == "cancelled":
            raise _not_found(f"event {event_id} not found on {calendar_id}")
        self._check_if_match(ev, if_match)

        now_iso = _to_iso_utc(self._clock.now())
        cal.change_counter += 1
        # Apply only the provided fields.
        if "status" in body:
            ev.status = body["status"]
        if "summary" in body:
            ev.summary = body["summary"]
        if "description" in body:
            ev.description = body["description"]
        if "location" in body:
            ev.location = body["location"]
        if "start" in body:
            ev.start = copy.deepcopy(body["start"])
        if "end" in body:
            ev.end = copy.deepcopy(body["end"])
        if "attendees" in body:
            ev.attendees = copy.deepcopy(body["attendees"] or [])
        if "organizer" in body:
            ev.organizer = copy.deepcopy(body["organizer"])
        if "transparency" in body:
            ev.transparency = body["transparency"]
        if "visibility" in body:
            ev.visibility = body["visibility"]
        if "colorId" in body:
            ev.color_id = body["colorId"]
        if "extendedProperties" in body:
            # Patch semantics: deep-merge `private` and `shared` rather
            # than replace, mirroring Google's documented behaviour.
            existing = ev.extended_properties or {}
            incoming = body["extendedProperties"] or {}
            merged = copy.deepcopy(existing)
            for kind in ("private", "shared"):
                if kind in incoming:
                    merged.setdefault(kind, {}).update(incoming[kind] or {})
            ev.extended_properties = merged
        if "recurrence" in body:
            ev.recurrence = list(body["recurrence"]) if body["recurrence"] else None
        ev.updated = now_iso
        ev.sequence += 1
        ev.etag = _new_etag()
        ev.change_seq = cal.change_counter
        return ev.to_api_dict()

    def delete_event(
        self,
        calendar_id: str,
        event_id: str,
        if_match: Optional[str] = None,
    ) -> None:
        """Cancel an event.

        Like real Google: deletion does not remove the row, it sets
        ``status='cancelled'`` so subsequent incremental syncs see
        the cancellation.  Re-deleting an already-cancelled event
        succeeds silently (matches Google's idempotent delete).

        Raises 404 if the event never existed.
        """
        cal = self._require_calendar(calendar_id)
        ev = cal.events.get(event_id)
        if ev is None:
            raise _not_found(f"event {event_id} not found on {calendar_id}")
        if ev.status == "cancelled":
            return  # idempotent
        self._check_if_match(ev, if_match)

        now_iso = _to_iso_utc(self._clock.now())
        cal.change_counter += 1
        ev.status = "cancelled"
        ev.updated = now_iso
        ev.sequence += 1
        ev.etag = _new_etag()
        ev.change_seq = cal.change_counter

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _require_calendar(self, calendar_id: str) -> _Calendar:
        cal = self._calendars.get(calendar_id)
        if cal is None:
            raise _not_found(f"calendar {calendar_id} not found")
        return cal

    @staticmethod
    def _check_if_match(ev: _StoredEvent, if_match: Optional[str]) -> None:
        if if_match is None or if_match == "*":
            return
        if if_match != ev.etag:
            raise _precondition_failed(
                f"etag {if_match} does not match stored {ev.etag}"
            )
