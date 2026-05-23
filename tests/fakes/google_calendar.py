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
from zoneinfo import ZoneInfo

from dateutil.parser import isoparse
from dateutil.rrule import rrulestr

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
    guests_can_modify: Optional[bool]
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
        if self.guests_can_modify is not None:
            out["guestsCanModify"] = self.guests_can_modify
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


@dataclass
class _PageTokenState:
    """Per-token bookkeeping for in-progress pagination.

    Pagination snapshots are server-side: we record the full list of
    event IDs that the request would return, then drip them out one
    page at a time.  The eventual ``nextSyncToken`` reflects the
    snapshot's cursor, not the cursor at the moment the last page is
    served.  This matches Google's documented "consistent snapshot
    across pages" guarantee.
    """

    calendar_id: str
    remaining_event_ids: list[str]
    sync_cursor_at_snapshot: int
    page_size: int


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


def _validate_attendees(body: dict) -> None:
    """Mirror Google: every attendee on a write must carry an email.

    ``events.insert`` / ``events.update`` reject a bare
    ``{"self": True}`` (no email) with "400 Missing attendee email.
    [required]".  The fake enforced no such rule, which let the
    email-less main-copy self-attendee bug ship undetected.
    """
    for att in body.get("attendees") or []:
        if not att.get("email"):
            raise _bad_request("Missing attendee email. [required]")


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
        failure_injector: Optional["FailureInjector"] = None,
    ):
        self._clock = clock or SimulatedClock()
        self._sync_token_ttl = sync_token_ttl
        self._calendars: dict[str, _Calendar] = {}
        self._sync_tokens: dict[str, _SyncTokenState] = {}
        self._page_tokens: dict[str, _PageTokenState] = {}
        self._failures = failure_injector

    def _check_failures(self, operation: str, *, has_sync_token: bool = False) -> None:
        """Roll the failure injector for a pre-operation failure.

        No-op if no injector is configured.  Kept as a single
        chokepoint so each API method has one consistent injection
        point.
        """
        if self._failures is not None:
            self._failures.maybe_fail(operation, has_sync_token=has_sync_token)

    def _check_post_write(self, operation: str) -> None:
        """Roll the failure injector for a post-write crash."""
        if self._failures is not None:
            self._failures.maybe_crash_after_write(operation)

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
        self._check_failures("list_calendars")
        return {
            "kind": "calendar#calendarList",
            "items": [self._calendar_to_api(c) for c in self._calendars.values()],
        }

    def list_calendar_list(self) -> dict:
        """Alias for :meth:`list_calendars` matching the
        ``GoogleClient`` protocol.  Each calendar registered with
        :meth:`add_calendar` appears here; the first one added is
        marked ``primary=True`` so OAuth-callback main-calendar
        discovery works in tests."""
        out = self.list_calendars()
        for i, item in enumerate(out["items"]):
            item["primary"] = (i == 0)
        return out

    def get_calendar(self, calendar_id: str) -> dict:
        self._check_failures("get_calendar")
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
        self._check_failures("insert")
        cal = self._require_calendar(calendar_id)
        _validate_attendees(body)

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
            guests_can_modify=body.get("guestsCanModify"),
            created=now_iso,
            updated=now_iso,
            sequence=int(body.get("sequence", 0) or 0),
            change_seq=cal.change_counter,
        )
        cal.events[event_id] = ev
        self._check_post_write("insert")
        return ev.to_api_dict()

    def get_event(self, calendar_id: str, event_id: str) -> dict:
        """Fetch a single event.

        If ``event_id`` is a derived recurring-instance ID
        (``<parent>_<stamp>``) and no override row exists, synthesise
        the instance from the parent series.  This matches Google,
        which lets clients GET an instance by its derived ID even
        before any modification has been made.
        """
        self._check_failures("get")
        cal = self._require_calendar(calendar_id)
        ev = cal.events.get(event_id)
        if ev is not None:
            return ev.to_api_dict()
        synthesized = self._synthesize_instance(cal, event_id)
        if synthesized is not None:
            return synthesized
        raise _not_found(f"event {event_id} not found on {calendar_id}")

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

        If ``event_id`` is a derived recurring-instance ID for which
        no override has been created yet, materialise the override
        on the fly (matching real Google: ``events.update`` on an
        instance ID creates an exception entry transparently).
        """
        self._check_failures("update")
        cal = self._require_calendar(calendar_id)
        _validate_attendees(body)
        ev = cal.events.get(event_id)
        if ev is None:
            ev = self._materialize_instance_override(cal, event_id)
            if ev is None:
                raise _not_found(f"event {event_id} not found on {calendar_id}")
        if ev.status == "cancelled":
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
        # For instance overrides, preserve recurring_event_id and
        # original_start_time unless the caller explicitly supplies
        # them — real Google keeps these stable across updates.
        if "recurringEventId" in body:
            ev.recurring_event_id = body["recurringEventId"]
        if "originalStartTime" in body:
            ev.original_start_time = copy.deepcopy(body["originalStartTime"])
        ev.guests_can_modify = body.get("guestsCanModify")
        ev.updated = now_iso
        ev.sequence += 1
        ev.etag = _new_etag()
        ev.change_seq = cal.change_counter
        self._check_post_write("update")
        return ev.to_api_dict()

    def patch_event(
        self,
        calendar_id: str,
        event_id: str,
        body: dict,
        if_match: Optional[str] = None,
    ) -> dict:
        """Partial update.  Same If-Match semantics as ``update_event``.

        Like ``update_event``, this materialises an instance override
        on the fly when called with a derived recurring-instance ID.
        """
        self._check_failures("patch")
        cal = self._require_calendar(calendar_id)
        ev = cal.events.get(event_id)
        if ev is None:
            ev = self._materialize_instance_override(cal, event_id)
            if ev is None:
                raise _not_found(f"event {event_id} not found on {calendar_id}")
        if ev.status == "cancelled":
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
        if "guestsCanModify" in body:
            ev.guests_can_modify = body["guestsCanModify"]
        ev.updated = now_iso
        ev.sequence += 1
        ev.etag = _new_etag()
        ev.change_seq = cal.change_counter
        self._check_post_write("patch")
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

        If ``event_id`` is a derived recurring-instance ID for which
        no override has been created yet, materialise a cancelled
        override on the fly.  This matches Google: deleting an
        instance creates a cancelled exception entry on the series.

        Raises 404 if the event id is unknown and is not a derived
        instance ID of any series.
        """
        self._check_failures("delete")
        cal = self._require_calendar(calendar_id)
        ev = cal.events.get(event_id)
        if ev is None:
            ev = self._materialize_instance_override(cal, event_id)
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
        self._check_post_write("delete")

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

    # ------------------------------------------------------------------
    # events.list
    # ------------------------------------------------------------------
    def list_events(
        self,
        calendar_id: str,
        sync_token: Optional[str] = None,
        time_min: Optional[datetime | str] = None,
        time_max: Optional[datetime | str] = None,
        max_results: int = 250,
        single_events: bool = False,
        page_token: Optional[str] = None,
        show_deleted: bool = False,
    ) -> dict:
        """List events on a calendar.

        Three modes:

        * **Full sync** (no ``sync_token``): returns events in
          ``[time_min, time_max]``.  Cancelled rows are filtered out
          unless ``show_deleted=True``; in particular, **cancelled
          instance exceptions of recurring series are omitted from
          full sync** when ``show_deleted=False``.  This is the
          documented quirk that the rewrite plan calls out as the
          source of recurring-cancellation amnesia (§13 Stage 1).
        * **Incremental sync** (``sync_token`` set): returns every
          event whose change cursor is strictly greater than the
          token's recorded cursor, regardless of status — cancelled
          events are always included.
        * **Pagination continuation** (``page_token`` set): returns
          the next chunk of a previous request's snapshot.

        Returns a dict with keys:

        * ``items`` — list of event dicts (same shape as ``get_event``)
        * ``nextPageToken`` — present only if there are more pages
        * ``nextSyncToken`` — present only on the final page
        """
        # Failure injection: rolls before any work. The
        # sync_token_expiry mode only fires when a token is in play.
        self._check_failures("list", has_sync_token=sync_token is not None)
        cal = self._require_calendar(calendar_id)

        # --- Pagination continuation ------------------------------------
        if page_token is not None:
            return self._continue_pagination(page_token)

        # --- Mutual exclusion sanity check ------------------------------
        if sync_token is not None and (time_min is not None or time_max is not None):
            raise _bad_request("syncToken cannot be combined with timeMin/timeMax")

        # --- Build the snapshot ----------------------------------------
        if sync_token is not None:
            ev_ids, snapshot_cursor = self._snapshot_incremental(cal, sync_token)
        else:
            tmin = _coerce_datetime(time_min) if time_min is not None else None
            tmax = _coerce_datetime(time_max) if time_max is not None else None
            ev_ids, snapshot_cursor = self._snapshot_full(
                cal, tmin, tmax, single_events, show_deleted,
            )

        return self._serve_page(cal, ev_ids, snapshot_cursor, max_results, sync_token)

    def _snapshot_incremental(
        self, cal: _Calendar, sync_token: str,
    ) -> tuple[list[str], int]:
        """Materialise the event-ID list for an incremental sync."""
        state = self._sync_tokens.get(sync_token)
        if state is None or state.calendar_id != cal.id:
            raise _gone(f"unknown sync token {sync_token}")
        # Realistic expiry — older than TTL, the token is dead.
        if (self._clock.now() - state.issued_at) > self._sync_token_ttl:
            # Per Google: invalid token is reported as 410 Gone, and
            # the client must fall back to a full sync.
            raise _gone(
                f"sync token issued at {state.issued_at.isoformat()} "
                f"is older than the {self._sync_token_ttl} TTL"
            )
        snapshot_cursor = cal.change_counter
        ids = [
            eid for eid, ev in cal.events.items()
            if ev.change_seq > state.cursor
        ]
        # Deterministic order: by change_seq, then by id, so callers
        # see changes in roughly chronological order.
        ids.sort(key=lambda eid: (cal.events[eid].change_seq, eid))
        return ids, snapshot_cursor

    def _snapshot_full(
        self,
        cal: _Calendar,
        time_min: Optional[datetime],
        time_max: Optional[datetime],
        single_events: bool,
        show_deleted: bool,
    ) -> tuple[list[str], int]:
        """Materialise the event-ID list for a full sync.

        With ``single_events=True``, recurring parents are expanded
        into per-instance synthetic IDs; the snapshot returns those
        synthetic IDs (so the page-rendering path will synthesize
        the dicts via ``get_event``).  This matches Google's
        ``singleEvents=True`` semantics.
        """
        snapshot_cursor = cal.change_counter
        if single_events:
            return self._snapshot_full_single_events(
                cal, time_min, time_max, show_deleted,
            ), snapshot_cursor
        ids: list[str] = []
        for eid, ev in cal.events.items():
            if ev.status == "cancelled":
                if not show_deleted:
                    continue
                # **Quirk**: even with showDeleted=True, cancelled
                # instance exceptions of recurring series are *not*
                # returned by full sync.  Real Google has the same
                # behaviour and it is the source of the
                # recurring-cancellation amnesia documented in
                # REWRITE_PLAN.md §13 Stage 1.  To retrieve cancelled
                # instances reliably, callers must use
                # ``events.instances(showDeleted=True)`` or pull them
                # via incremental sync.
                if ev.recurring_event_id is not None:
                    continue
            if not _matches_window(ev, time_min, time_max):
                continue
            ids.append(eid)
        # Stable order: by start time then ID, so the snapshot is
        # reproducible across runs.
        ids.sort(key=lambda eid: (_event_sort_key(cal.events[eid]), eid))
        return ids, snapshot_cursor

    def _serve_page(
        self,
        cal: _Calendar,
        ev_ids: list[str],
        snapshot_cursor: int,
        max_results: int,
        original_sync_token: Optional[str],
    ) -> dict:
        """Return the first page of ``ev_ids`` and stash the rest.

        Each ID is rendered via :meth:`_render_event_id`, which
        handles both real stored events and synthesized recurring
        instances (whose IDs are derived from a parent's RRULE
        expansion and not present in ``cal.events``).
        """
        if max_results <= 0:
            raise _bad_request("maxResults must be positive")

        page = ev_ids[:max_results]
        rest = ev_ids[max_results:]

        items = [d for d in (self._render_event_id(cal, eid) for eid in page) if d is not None]
        out: dict = {
            "kind": "calendar#events",
            "items": items,
        }

        if rest:
            page_token = self._issue_page_token(
                cal.id, rest, snapshot_cursor, max_results,
            )
            out["nextPageToken"] = page_token
        else:
            out["nextSyncToken"] = self._issue_sync_token(cal.id, snapshot_cursor)

        # Consume the original sync token so a replay would 410
        # rather than silently double-process.  Real Google does NOT
        # invalidate the prior token on use, but the rewrite design
        # does not rely on that, and invalidating eagerly is a
        # cheap way to surface bugs.  Keep behaviour configurable;
        # for now, leave the prior token intact (closer to real Google).
        del original_sync_token

        return out

    def _continue_pagination(self, page_token: str) -> dict:
        """Serve the next page from a previously-stashed snapshot."""
        state = self._page_tokens.pop(page_token, None)
        if state is None:
            raise _gone(f"unknown page token {page_token}")
        cal = self._calendars.get(state.calendar_id)
        if cal is None:
            raise _gone(f"calendar {state.calendar_id} no longer exists")

        page_ids = state.remaining_event_ids[: state.page_size]
        rest = state.remaining_event_ids[state.page_size :]

        # Note: events that have been mutated since the snapshot
        # still surface here (we re-render their CURRENT state).
        # Real Google's behaviour here is undefined; we render
        # latest content, which matches the documented "consistent
        # snapshot of identities, latest content" interpretation.
        items: list[dict] = []
        for eid in page_ids:
            rendered = self._render_event_id(cal, eid)
            if rendered is not None:
                items.append(rendered)

        out: dict = {"kind": "calendar#events", "items": items}
        if rest:
            new_token = self._issue_page_token(
                cal.id, rest, state.sync_cursor_at_snapshot, state.page_size,
            )
            out["nextPageToken"] = new_token
        else:
            out["nextSyncToken"] = self._issue_sync_token(
                cal.id, state.sync_cursor_at_snapshot,
            )
        return out

    def _render_event_id(self, cal: _Calendar, event_id: str) -> Optional[dict]:
        """Render an event by ID, synthesizing recurring instances
        when the ID names one and no override row exists.
        """
        ev = cal.events.get(event_id)
        if ev is not None:
            return ev.to_api_dict()
        return self._synthesize_instance(cal, event_id)

    def _snapshot_full_single_events(
        self,
        cal: _Calendar,
        time_min: Optional[datetime],
        time_max: Optional[datetime],
        show_deleted: bool,
    ) -> list[str]:
        """Build a snapshot of expanded-instance IDs for full sync.

        Standalone events appear once.  Recurring parents are
        expanded; each occurrence yields either an override row's
        ID (if one exists) or a synthesized derived ID.

        The same full-sync cancelled-instance quirk applies: even
        with show_deleted=True, cancelled instance overrides are
        omitted.  Use ``events.instances`` or incremental sync to
        retrieve them reliably.
        """
        ids: list[str] = []
        for eid, ev in cal.events.items():
            if ev.recurring_event_id is not None:
                # Skip overrides; they are surfaced (or omitted) via
                # parent expansion below.
                continue
            if ev.recurrence:
                # Expand the parent.  _expand_instances takes care of
                # show_deleted for cancelled overrides at occurrence
                # dates.  We then translate dicts → IDs for the
                # snapshot.  But: per the quirk above, full-sync
                # *single-events* mode also drops cancelled overrides.
                instances = self._expand_instances(
                    cal, ev, time_min, time_max, show_deleted=False,
                )
                for inst in instances:
                    ids.append(inst["id"])
            else:
                if ev.status == "cancelled" and not show_deleted:
                    continue
                if not _matches_window(ev, time_min, time_max):
                    continue
                ids.append(eid)

        # Stable ordering
        def _sort_key(eid: str) -> tuple:
            real = cal.events.get(eid)
            if real is not None:
                return (_event_sort_key(real), eid)
            synth = self._synthesize_instance(cal, eid)
            if synth is None:
                return ((0.0,), eid)
            start = synth.get("start", {})
            if "dateTime" in start:
                ts = _coerce_datetime(start["dateTime"]).timestamp()
            elif "date" in start:
                ts = _coerce_datetime(start["date"]).timestamp()
            else:
                ts = 0.0
            return ((ts,), eid)

        ids.sort(key=_sort_key)
        return ids

    def _issue_sync_token(self, calendar_id: str, cursor: int) -> str:
        token = f"sync-{uuid.uuid4().hex}"
        self._sync_tokens[token] = _SyncTokenState(
            calendar_id=calendar_id,
            cursor=cursor,
            issued_at=self._clock.now(),
        )
        return token

    def _issue_page_token(
        self, calendar_id: str, remaining: list[str],
        cursor: int, page_size: int,
    ) -> str:
        token = f"page-{uuid.uuid4().hex}"
        self._page_tokens[token] = _PageTokenState(
            calendar_id=calendar_id,
            remaining_event_ids=remaining,
            sync_cursor_at_snapshot=cursor,
            page_size=page_size,
        )
        return token

    # ------------------------------------------------------------------
    # Recurring events
    # ------------------------------------------------------------------
    def list_instances(
        self,
        calendar_id: str,
        event_id: str,
        show_deleted: bool = False,
        max_results: int = 250,
        time_min: Optional[datetime | str] = None,
        time_max: Optional[datetime | str] = None,
    ) -> dict:
        """Return the instances of a recurring series.

        Mirrors Google's ``events.instances`` endpoint.  Each instance
        is either:

        * A modified-instance override (an event row with
          ``recurringEventId == event_id``), or
        * A cancelled-instance override (same, ``status='cancelled'``;
          included only if ``show_deleted=True``), or
        * A synthetic dict generated from the parent's RRULE for
          dates with no override.

        Default time window: ``[parent.start, parent.start + 2y]`` —
        enough to catch normal weekly/monthly series in tests.
        """
        self._check_failures("instances")
        cal = self._require_calendar(calendar_id)
        parent = cal.events.get(event_id)
        if parent is None:
            raise _not_found(f"event {event_id} not found on {calendar_id}")
        if not parent.recurrence:
            raise _bad_request(f"event {event_id} is not a recurring series")

        tmin = _coerce_datetime(time_min) if time_min is not None else None
        tmax = _coerce_datetime(time_max) if time_max is not None else None

        instances = self._expand_instances(
            cal, parent, tmin, tmax, show_deleted=show_deleted,
        )
        items = [inst for inst in instances][:max_results]
        return {"kind": "calendar#events", "items": items}

    def reschedule_series_this_and_following(
        self,
        calendar_id: str,
        parent_event_id: str,
        from_dt: datetime | str,
        new_body: dict,
    ) -> dict:
        """Simulate Google Calendar's "this and following" reschedule.

        When a user picks "this and following" in the Google UI to
        move part of a recurring series, Google internally:

        1. Truncates the original series's RRULE with an UNTIL just
           before ``from_dt``.
        2. Creates a NEW recurring event whose ID is
           ``<parent_event_id>_R<utc_compact_timestamp>`` (the source
           of the ``_R`` suffix the production code special-cases).
        3. Cancels any modified-instance overrides that fell on or
           after ``from_dt`` on the original series (their dates
           now belong to the new series).

        This is documented in REWRITE_PLAN.md §13 Stage 1 as one of
        the must-reproduce quirks.

        Returns the new (``_R``-suffixed) event's API dict.
        """
        cal = self._require_calendar(calendar_id)
        parent = cal.events.get(parent_event_id)
        if parent is None or not parent.recurrence:
            raise _bad_request(
                f"event {parent_event_id} is not a recurring series"
            )
        boundary = _coerce_datetime(from_dt)

        # 1. Truncate the original series.
        new_rrule = _add_until_to_rrule(parent.recurrence, boundary)
        cal.change_counter += 1
        parent.recurrence = new_rrule
        parent.updated = _to_iso_utc(self._clock.now())
        parent.sequence += 1
        parent.etag = _new_etag()
        parent.change_seq = cal.change_counter

        # 3. Cancel overrides (modified instance exceptions) on or
        #    after the boundary.  We scan for events whose
        #    recurring_event_id matches and whose original_start_time
        #    falls in the truncated range.
        for ev in list(cal.events.values()):
            if ev.recurring_event_id != parent_event_id:
                continue
            ost = ev.original_start_time
            if not ost:
                continue
            ost_dt = _original_start_time_to_dt(ost)
            if ost_dt is None or ost_dt < boundary:
                continue
            if ev.status == "cancelled":
                continue
            cal.change_counter += 1
            ev.status = "cancelled"
            ev.updated = parent.updated
            ev.sequence += 1
            ev.etag = _new_etag()
            ev.change_seq = cal.change_counter

        # 2. Create the new ``_R``-suffixed series.  We bypass
        #    insert_event's client-ID validation here because Google
        #    generates this ID itself (with uppercase letters that
        #    are illegal for client-supplied IDs but legal as
        #    server-generated identifiers).
        ts = boundary.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
        new_id = f"{parent_event_id}_R{ts}"
        if new_id in cal.events:
            raise _conflict(f"event id {new_id} already exists on calendar {calendar_id}")
        cal.change_counter += 1
        now_iso = _to_iso_utc(self._clock.now())
        ev = _StoredEvent(
            id=new_id,
            etag=_new_etag(),
            status="confirmed",
            summary=new_body.get("summary"),
            description=new_body.get("description"),
            location=new_body.get("location"),
            start=copy.deepcopy(new_body.get("start")),
            end=copy.deepcopy(new_body.get("end")),
            attendees=copy.deepcopy(new_body.get("attendees", []) or []),
            organizer=copy.deepcopy(new_body.get("organizer")),
            transparency=new_body.get("transparency"),
            visibility=new_body.get("visibility"),
            color_id=new_body.get("colorId"),
            extended_properties=copy.deepcopy(
                new_body.get("extendedProperties", {}) or {}
            ),
            recurrence=list(new_body["recurrence"]) if new_body.get("recurrence") else None,
            recurring_event_id=None,
            original_start_time=None,
            guests_can_modify=new_body.get("guestsCanModify"),
            created=now_iso,
            updated=now_iso,
            sequence=0,
            change_seq=cal.change_counter,
        )
        cal.events[new_id] = ev
        return ev.to_api_dict()

    # ------------------------------------------------------------------
    # Recurring helpers (internal)
    # ------------------------------------------------------------------
    def _materialize_instance_override(
        self, cal: _Calendar, event_id: str,
    ) -> Optional[_StoredEvent]:
        """If ``event_id`` looks like a derived instance ID, create
        an override row for it (status=confirmed, content copied
        from the synthesized instance).  Returns the new row, or
        ``None`` if no parent series matches.
        """
        parent_id, instance_dt = _parse_instance_id(event_id, cal)
        if parent_id is None:
            return None
        parent = cal.events[parent_id]
        if not _is_dt_in_recurrence(parent, instance_dt):
            return None

        synth = self._synthesize_instance(cal, event_id)
        if synth is None:
            return None
        # Reuse insert_event's bookkeeping but build the row by hand
        # so we don't go through validation again.
        cal.change_counter += 1
        now_iso = _to_iso_utc(self._clock.now())
        ev = _StoredEvent(
            id=event_id,
            etag=_new_etag(),
            status="confirmed",
            summary=synth.get("summary"),
            description=synth.get("description"),
            location=synth.get("location"),
            start=copy.deepcopy(synth.get("start")),
            end=copy.deepcopy(synth.get("end")),
            attendees=copy.deepcopy(synth.get("attendees", []) or []),
            organizer=copy.deepcopy(synth.get("organizer")),
            transparency=synth.get("transparency"),
            visibility=synth.get("visibility"),
            color_id=synth.get("colorId"),
            extended_properties=copy.deepcopy(synth.get("extendedProperties", {}) or {}),
            recurrence=None,
            recurring_event_id=parent_id,
            original_start_time=copy.deepcopy(synth.get("originalStartTime")),
            guests_can_modify=synth.get("guestsCanModify"),
            created=now_iso,
            updated=now_iso,
            sequence=0,
            change_seq=cal.change_counter,
        )
        cal.events[event_id] = ev
        return ev

    def _synthesize_instance(
        self, cal: _Calendar, event_id: str,
    ) -> Optional[dict]:
        """Return a synthetic instance dict for an unmodified
        occurrence of a recurring series, or ``None`` if the ID
        does not name an instance.
        """
        parent_id, instance_dt = _parse_instance_id(event_id, cal)
        if parent_id is None:
            return None
        parent = cal.events[parent_id]
        if not _is_dt_in_recurrence(parent, instance_dt):
            return None

        # Build a copy of the parent with adjusted start/end.
        parent_dict = parent.to_api_dict()
        instance = dict(parent_dict)
        instance.pop("recurrence", None)
        instance["id"] = event_id
        instance["recurringEventId"] = parent_id

        # Use the parent's wall-clock time-of-day, anchored to
        # instance_dt's calendar date.
        is_all_day = "date" in (parent.start or {})
        if is_all_day:
            ymd = instance_dt.astimezone(UTC).strftime("%Y-%m-%d")
            instance["start"] = {"date": ymd}
            instance["end"] = parent.end and {
                "date": _shift_date(ymd, _all_day_duration_days(parent))
            }
            instance["originalStartTime"] = {"date": ymd}
        else:
            instance["start"] = {
                "dateTime": _to_google_datetime(instance_dt, parent.start),
                "timeZone": (parent.start or {}).get("timeZone", "UTC"),
            }
            instance["end"] = {
                "dateTime": _to_google_datetime(
                    instance_dt + _timed_duration(parent), parent.end,
                ),
                "timeZone": (parent.end or {}).get("timeZone", "UTC"),
            }
            instance["originalStartTime"] = dict(instance["start"])
        return instance

    def _expand_instances(
        self,
        cal: _Calendar,
        parent: _StoredEvent,
        time_min: Optional[datetime],
        time_max: Optional[datetime],
        show_deleted: bool,
    ) -> list[dict]:
        """Yield the API-shape instance dicts for ``parent`` between
        ``time_min`` and ``time_max``.  Overrides take precedence
        over synthesized instances at matching dates.
        """
        parent_start_dt = _event_start_dt_for_expansion(parent)
        if parent_start_dt is None:
            return []

        # Default to a 2-year window starting at the parent.  Real
        # Google's default is similar (a few years out).
        if time_min is None:
            time_min = parent_start_dt
        if time_max is None:
            time_max = parent_start_dt + timedelta(days=730)

        # Build a date → override-event map for fast lookup.
        overrides_by_date: dict[str, _StoredEvent] = {}
        for ev in cal.events.values():
            if ev.recurring_event_id != parent.id:
                continue
            ost = ev.original_start_time
            if not ost:
                continue
            key = _ost_key(ost)
            overrides_by_date[key] = ev

        # Expand the RRULE.
        try:
            rule = rrulestr(
                "\n".join(parent.recurrence or []),
                dtstart=parent_start_dt,
                forceset=True,
            )
        except Exception as e:
            raise _bad_request(f"invalid recurrence: {e}")

        items: list[dict] = []
        for occurrence in rule.between(time_min, time_max, inc=True):
            occ_aware = occurrence if occurrence.tzinfo else occurrence.replace(tzinfo=UTC)
            key = _instance_key_for_dt(occ_aware, parent)
            override = overrides_by_date.pop(key, None)
            if override is not None:
                if override.status == "cancelled" and not show_deleted:
                    continue
                items.append(override.to_api_dict())
            else:
                synth_id = _instance_id_for_dt(parent, occ_aware)
                synth = self._synthesize_instance(cal, synth_id)
                if synth is not None:
                    items.append(synth)

        # Surface any overrides at dates that are NOT regular
        # occurrences (e.g. instance moved to a new day).  Real
        # Google still reports them via instances().
        for override in overrides_by_date.values():
            if override.status == "cancelled" and not show_deleted:
                continue
            items.append(override.to_api_dict())

        return items

    # ------------------------------------------------------------------
    # Test introspection (NOT part of the Google API surface)
    # ------------------------------------------------------------------
    def all_event_ids(self, calendar_id: str, *, include_cancelled: bool = True) -> list[str]:
        cal = self._require_calendar(calendar_id)
        return [
            eid for eid, ev in cal.events.items()
            if include_cancelled or ev.status != "cancelled"
        ]

    def event_count(self, calendar_id: str, *, include_cancelled: bool = False) -> int:
        return len(self.all_event_ids(calendar_id, include_cancelled=include_cancelled))

    def expire_sync_token(self, sync_token: str) -> None:
        """Force a sync token to be considered expired on next use.

        Useful for tests that want to exercise the 410 Gone path
        without advancing the clock by 30 days.
        """
        state = self._sync_tokens.get(sync_token)
        if state is None:
            raise KeyError(sync_token)
        state.issued_at = self._clock.now() - self._sync_token_ttl - timedelta(seconds=1)


# ---------------------------------------------------------------------------
# Internal helpers used by list_events
# ---------------------------------------------------------------------------
def _coerce_datetime(value: datetime | str) -> datetime:
    """Accept either a datetime or an ISO-8601 string."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        # Tolerate trailing Z, which fromisoformat refuses pre-3.11.
        s = value[:-1] + "+00:00" if value.endswith("Z") else value
        return isoparse(s)
    raise TypeError(f"unsupported time value: {value!r}")


def _event_start_dt(ev: _StoredEvent) -> Optional[datetime]:
    """Return the event's start as an aware UTC datetime, or None."""
    if not ev.start:
        return None
    if "dateTime" in ev.start:
        return _coerce_datetime(ev.start["dateTime"]).astimezone(UTC)
    if "date" in ev.start:
        d = date.fromisoformat(ev.start["date"])
        return datetime(d.year, d.month, d.day, tzinfo=UTC)
    return None


def _event_start_dt_for_expansion(ev: _StoredEvent) -> Optional[datetime]:
    """Parent start used as the RRULE ``dtstart``.

    Real Google expands a recurring series in the series' declared
    ``start.timeZone``: each occurrence keeps a fixed WALL-CLOCK time
    and therefore shifts its UTC instant across a DST transition.
    Anchoring ``dtstart`` in that zone (rather than UTC) reproduces
    that behaviour; without it the fake would expand on a fixed UTC
    grid and never drift, masking the very bug under test.
    """
    base = _event_start_dt(ev)
    if base is None:
        return None
    tz_name = (ev.start or {}).get("timeZone")
    if not tz_name or tz_name == "UTC":
        return base
    try:
        return base.astimezone(ZoneInfo(tz_name))
    except Exception:
        return base


def _event_end_dt(ev: _StoredEvent) -> Optional[datetime]:
    if not ev.end:
        return None
    if "dateTime" in ev.end:
        return _coerce_datetime(ev.end["dateTime"]).astimezone(UTC)
    if "date" in ev.end:
        d = date.fromisoformat(ev.end["date"])
        return datetime(d.year, d.month, d.day, tzinfo=UTC)
    return None


def _matches_window(
    ev: _StoredEvent,
    time_min: Optional[datetime],
    time_max: Optional[datetime],
) -> bool:
    """Decide whether an event falls inside ``[time_min, time_max]``.

    Recurring parents are always included; the planner / consumer
    is responsible for expanding RRULEs into instance windows.
    Standalone and modified-instance events are filtered on their
    start time.  An event with no start at all (a stub) is always
    included so misbehaving inputs don't silently disappear.
    """
    if ev.recurrence:
        return True
    start = _event_start_dt(ev)
    if start is None:
        return True
    if time_min is not None and start < time_min:
        # Event ends before the window — exclude only if the END is
        # also before the window, so an event that straddles the
        # boundary still appears.
        end = _event_end_dt(ev) or start
        if end < time_min:
            return False
    if time_max is not None and start >= time_max:
        return False
    return True


def _event_sort_key(ev: _StoredEvent) -> tuple:
    """Stable ordering key for snapshot determinism."""
    start = _event_start_dt(ev)
    return (start.timestamp() if start else 0.0,)


# ---------------------------------------------------------------------------
# Recurring-event helpers
# ---------------------------------------------------------------------------
_INSTANCE_SUFFIX_RE = re.compile(
    r"_(?P<stamp>\d{8}(T\d{6}Z)?)$"
)


def _parse_instance_id(
    event_id: str, cal: _Calendar,
) -> tuple[Optional[str], Optional[datetime]]:
    """If ``event_id`` matches ``<parentId>_<stamp>`` and the parent
    exists on ``cal`` and has a recurrence, return
    ``(parent_id, instance_dt)``.  Otherwise return ``(None, None)``.

    The stamp is either ``YYYYMMDD`` (all-day) or
    ``YYYYMMDDTHHMMSSZ`` (timed UTC).
    """
    m = _INSTANCE_SUFFIX_RE.search(event_id)
    if not m:
        return None, None
    stamp = m.group("stamp")
    parent_id = event_id[: m.start()]
    parent = cal.events.get(parent_id)
    if parent is None or not parent.recurrence:
        return None, None
    if "T" in stamp:
        # Timed: YYYYMMDDTHHMMSSZ
        dt = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    else:
        d = datetime.strptime(stamp, "%Y%m%d")
        dt = d.replace(tzinfo=UTC)
    return parent_id, dt


def _is_dt_in_recurrence(parent: _StoredEvent, dt: datetime) -> bool:
    """Return True if ``dt`` is one of the parent's RRULE occurrences.

    Uses dateutil to expand the RRULE and matches by exact second
    (timed) or exact date (all-day).
    """
    if not parent.recurrence:
        return False
    parent_start = _event_start_dt_for_expansion(parent)
    if parent_start is None:
        return False
    try:
        rule = rrulestr(
            "\n".join(parent.recurrence),
            dtstart=parent_start,
            forceset=True,
        )
    except Exception:
        return False
    is_all_day = "date" in (parent.start or {})
    if is_all_day:
        target_date = dt.date()
        for occ in rule.between(
            datetime.combine(target_date, datetime.min.time(), tzinfo=UTC) - timedelta(days=1),
            datetime.combine(target_date, datetime.min.time(), tzinfo=UTC) + timedelta(days=1),
            inc=True,
        ):
            occ_aware = occ if occ.tzinfo else occ.replace(tzinfo=UTC)
            if occ_aware.date() == target_date:
                return True
        return False
    else:
        # ±1 second window for floating-point safety.
        for occ in rule.between(
            dt - timedelta(seconds=1), dt + timedelta(seconds=1), inc=True,
        ):
            occ_aware = occ if occ.tzinfo else occ.replace(tzinfo=UTC)
            if abs((occ_aware - dt).total_seconds()) < 1:
                return True
        return False


def _instance_id_for_dt(parent: _StoredEvent, dt: datetime) -> str:
    """Return the derived instance ID for ``parent`` at occurrence ``dt``."""
    is_all_day = "date" in (parent.start or {})
    if is_all_day:
        return f"{parent.id}_{dt.astimezone(UTC).strftime('%Y%m%d')}"
    return f"{parent.id}_{dt.astimezone(UTC).strftime('%Y%m%dT%H%M%S')}Z"


def _instance_key_for_dt(dt: datetime, parent: _StoredEvent) -> str:
    """Stable key for matching an occurrence against an override map."""
    is_all_day = "date" in (parent.start or {})
    if is_all_day:
        return f"date:{dt.astimezone(UTC).strftime('%Y-%m-%d')}"
    return f"dt:{dt.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')}"


def _ost_key(ost: dict) -> str:
    """Stable key from an ``originalStartTime`` dict."""
    if "date" in ost:
        return f"date:{ost['date']}"
    dt = _coerce_datetime(ost["dateTime"]).astimezone(UTC)
    return f"dt:{dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"


def _original_start_time_to_dt(ost: dict) -> Optional[datetime]:
    if "date" in ost:
        d = date.fromisoformat(ost["date"])
        return datetime(d.year, d.month, d.day, tzinfo=UTC)
    if "dateTime" in ost:
        return _coerce_datetime(ost["dateTime"]).astimezone(UTC)
    return None


def _to_google_datetime(dt: datetime, like: Optional[dict]) -> str:
    """Format ``dt`` for a Google start/end ``dateTime`` field.

    Mirrors the timezone-handling style of the source: if the model
    field used ``Z``, emit ``Z``; otherwise emit ``+00:00``.
    """
    aware = dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    aware = aware.astimezone(UTC)
    sample = (like or {}).get("dateTime", "")
    if sample.endswith("Z"):
        return aware.strftime("%Y-%m-%dT%H:%M:%SZ")
    return aware.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _timed_duration(parent: _StoredEvent) -> timedelta:
    start = _event_start_dt(parent)
    end = _event_end_dt(parent)
    if start is None or end is None:
        return timedelta(0)
    return end - start


def _all_day_duration_days(parent: _StoredEvent) -> int:
    if not parent.start or not parent.end:
        return 1
    start = date.fromisoformat(parent.start["date"])
    end = date.fromisoformat(parent.end["date"])
    return max((end - start).days, 1)


def _shift_date(ymd: str, days: int) -> str:
    d = date.fromisoformat(ymd) + timedelta(days=days)
    return d.isoformat()


def _add_until_to_rrule(
    recurrence: list[str],
    boundary: datetime,
) -> list[str]:
    """Return ``recurrence`` with the RRULE truncated by ``UNTIL``.

    Used by the ``_R`` "this and following" reschedule simulator.
    The UNTIL value is set to one second before ``boundary`` so the
    instance at ``boundary`` is excluded from the original series.
    """
    until_dt = (boundary.astimezone(UTC) - timedelta(seconds=1))
    until_str = until_dt.strftime("%Y%m%dT%H%M%SZ")
    out: list[str] = []
    for line in recurrence:
        if line.startswith("RRULE:"):
            body = line[len("RRULE:") :]
            parts = [p for p in body.split(";") if not p.startswith(("UNTIL=", "COUNT="))]
            parts.append(f"UNTIL={until_str}")
            out.append("RRULE:" + ";".join(parts))
        else:
            out.append(line)
    return out
