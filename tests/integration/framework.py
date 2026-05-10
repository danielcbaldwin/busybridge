"""Integration test framework: scenarios over the fake Google.

A :class:`Scenario` is one composed test instance — clock + fake
Google + failure injector — with a high-level "given / when / then"
API designed for the integration tests called out in
``REWRITE_PLAN.md`` §14 Layer 3.

Usage sketch::

    def test_event_appears_on_target_calendars():
        s = Scenario()
        main = s.given_calendar("main")
        client_a = s.given_calendar("client_a")
        client_b = s.given_calendar("client_b")

        s.given_event("client_a", summary="Standup", start="2026-02-02T09:00:00Z")

        # When the (future) reconciler runs:
        # ...

        # Then:
        s.assert_event_exists("main", summary="Standup")
        s.assert_event_exists("client_b", summary_contains="Standup")

The Stage-2 ledger code does not exist yet; this framework is the
plug socket it will mount into.  When the new system lands, a
``Scenario.run_reconciler()`` (or similar) hook will drive it under
``advance()`` / ``run_until_idle()``.

Helpers favour readability over flexibility — tests should read
like specifications, not like code.  The underlying primitives
(``Scenario.google``, ``Scenario.clock``, ``Scenario.failures``)
are exposed for cases where the helpers do not suffice.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from tests.fakes.clock import SimulatedClock
from tests.fakes.failures import FailureInjector
from tests.fakes.google_calendar import (
    DEFAULT_SYNC_TOKEN_TTL,
    FakeGoogleCalendar,
    GoogleApiError,
)

UTC = timezone.utc


class ScenarioAssertionError(AssertionError):
    """Raised when a scenario assertion fails.

    Distinct subclass so test failures can be filtered from the
    framework's own bugs in the soak harness later.
    """


@dataclass
class Scenario:
    """One integration test instance.

    Construct, then chain ``given_*`` / ``when_*`` / ``assert_*``
    calls.  Each instance is fully self-contained: the clock starts
    at a stable epoch, the failure injector is seeded with the
    provided ``seed``, and the fake Google has no calendars or
    events.

    Attributes:
        clock: The simulated clock backing time-dependent behaviour.
        failures: The failure injector; mutate its rates to inject
            chaos.  Use ``failures.force_next(...)`` for one-shot
            failures.
        google: The fake Google Calendar; available for direct
            access if a helper does not exist for what you need.
    """

    seed: int = 0
    clock_start: Optional[datetime] = None
    sync_token_ttl: timedelta = DEFAULT_SYNC_TOKEN_TTL

    clock: SimulatedClock = field(init=False)
    failures: FailureInjector = field(init=False)
    google: FakeGoogleCalendar = field(init=False)

    _calendars_by_nick: dict[str, str] = field(init=False, default_factory=dict)
    _nicks_by_calendar: dict[str, str] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.clock = SimulatedClock(start=self.clock_start)
        self.failures = FailureInjector(seed=self.seed)
        self.google = FakeGoogleCalendar(
            clock=self.clock,
            failure_injector=self.failures,
            sync_token_ttl=self.sync_token_ttl,
        )

    # ------------------------------------------------------------------
    # GIVEN — setup
    # ------------------------------------------------------------------
    def given_calendar(
        self,
        nickname: str,
        *,
        summary: str = "",
        time_zone: str = "UTC",
        calendar_id: Optional[str] = None,
    ) -> str:
        """Register a calendar under a short test-readable nickname.

        Returns the resolved Google calendar ID.  Reuse the nickname
        in subsequent helpers (``given_event("nick", ...)``).
        """
        if nickname in self._calendars_by_nick:
            raise ValueError(f"calendar nickname already in use: {nickname!r}")
        cal_id = calendar_id or f"{nickname}@cal.test"
        self.google.add_calendar(cal_id, summary or nickname, time_zone)
        self._calendars_by_nick[nickname] = cal_id
        self._nicks_by_calendar[cal_id] = nickname
        return cal_id

    def given_event(
        self,
        on: str,
        *,
        summary: str = "Event",
        start: datetime | str = "2026-02-02T09:00:00Z",
        duration_minutes: int = 30,
        event_id: Optional[str] = None,
        attendees: Optional[list[dict]] = None,
        extended_properties: Optional[dict] = None,
        transparency: Optional[str] = None,
        location: Optional[str] = None,
        description: Optional[str] = None,
    ) -> dict:
        """Insert a single (non-recurring) event onto a calendar.

        Returns the inserted event dict (with ``id`` and ``etag``).
        """
        body = self._build_event_body(
            summary=summary,
            start=start,
            duration_minutes=duration_minutes,
            attendees=attendees,
            extended_properties=extended_properties,
            transparency=transparency,
            location=location,
            description=description,
        )
        if event_id is not None:
            body["id"] = event_id
        return self.google.insert_event(self.cal(on), body)

    def given_recurring_event(
        self,
        on: str,
        *,
        summary: str = "Recurring",
        start: datetime | str = "2026-02-02T09:00:00Z",
        duration_minutes: int = 30,
        rrule: str = "RRULE:FREQ=WEEKLY;COUNT=4",
        event_id: Optional[str] = None,
        **extra: Any,
    ) -> dict:
        """Insert a recurring series.  ``rrule`` is the full RRULE
        line including the ``RRULE:`` prefix."""
        body = self._build_event_body(
            summary=summary,
            start=start,
            duration_minutes=duration_minutes,
            **extra,
        )
        body["recurrence"] = [rrule]
        if event_id is not None:
            body["id"] = event_id
        return self.google.insert_event(self.cal(on), body)

    # ------------------------------------------------------------------
    # WHEN — actions
    # ------------------------------------------------------------------
    def advance(self, delta: timedelta | float) -> int:
        """Move the simulated clock forward.  Returns the number of
        scheduled callbacks fired during the advance."""
        return self.clock.run_for(delta)

    def cancel_event(self, on: str, event_id: str) -> None:
        self.google.delete_event(self.cal(on), event_id)

    def update_event(
        self,
        on: str,
        event_id: str,
        *,
        if_match: Optional[str] = None,
        **fields: Any,
    ) -> dict:
        """Replace an event's fields via update_event.

        Pass field overrides as keyword arguments
        (``summary="new"``, ``start="2026-02-02T10:00:00Z"`` etc.).
        Unspecified fields are taken from the current stored event.
        """
        current = self.google.get_event(self.cal(on), event_id)
        merged = self._merge_event_body(current, fields)
        return self.google.update_event(
            self.cal(on), event_id, merged, if_match=if_match,
        )

    def reschedule_recurring_this_and_following(
        self,
        on: str,
        parent_event_id: str,
        *,
        from_dt: datetime | str,
        new_summary: Optional[str] = None,
        new_start: Optional[datetime | str] = None,
        new_rrule: str = "RRULE:FREQ=WEEKLY;COUNT=8",
        duration_minutes: int = 30,
    ) -> dict:
        """Drive the ``_R``-suffixed quirk via the fake's helper."""
        parent = self.google.get_event(self.cal(on), parent_event_id)
        new_body = self._build_event_body(
            summary=new_summary or parent.get("summary", "Recurring"),
            start=new_start or from_dt,
            duration_minutes=duration_minutes,
        )
        new_body["recurrence"] = [new_rrule]
        return self.google.reschedule_series_this_and_following(
            self.cal(on), parent_event_id, from_dt, new_body,
        )

    # ------------------------------------------------------------------
    # THEN — assertions
    # ------------------------------------------------------------------
    def list_events(
        self,
        on: str,
        *,
        time_min: Optional[datetime | str] = None,
        time_max: Optional[datetime | str] = None,
        single_events: bool = False,
        show_deleted: bool = False,
    ) -> list[dict]:
        """Return all events matching the filters; auto-paginates."""
        items: list[dict] = []
        page_token = None
        while True:
            kwargs: dict = {
                "show_deleted": show_deleted,
                "single_events": single_events,
            }
            if page_token is None:
                if time_min is not None:
                    kwargs["time_min"] = time_min
                if time_max is not None:
                    kwargs["time_max"] = time_max
            else:
                kwargs["page_token"] = page_token
            out = self.google.list_events(self.cal(on), **kwargs)
            items.extend(out.get("items", []))
            page_token = out.get("nextPageToken")
            if not page_token:
                break
        return items

    def find_events(
        self,
        on: str,
        *,
        summary: Optional[str] = None,
        summary_contains: Optional[str] = None,
        start: Optional[datetime | str] = None,
        status: Optional[str] = None,
        recurring_event_id: Optional[str] = None,
    ) -> list[dict]:
        """Return events on ``on`` matching every supplied predicate."""
        events = self.list_events(on, show_deleted=status == "cancelled")
        results: list[dict] = []
        target_start = (
            _coerce_iso(start) if start is not None else None
        )
        for ev in events:
            if summary is not None and ev.get("summary") != summary:
                continue
            if summary_contains is not None and summary_contains not in (ev.get("summary") or ""):
                continue
            if status is not None and ev.get("status") != status:
                continue
            if recurring_event_id is not None and ev.get("recurringEventId") != recurring_event_id:
                continue
            if target_start is not None:
                ev_start = ev.get("start", {})
                if "dateTime" in ev_start:
                    if ev_start["dateTime"] != target_start:
                        continue
                elif "date" in ev_start:
                    if ev_start["date"] != target_start[:10]:
                        continue
                else:
                    continue
            results.append(ev)
        return results

    def assert_event_exists(
        self,
        on: str,
        *,
        summary: Optional[str] = None,
        summary_contains: Optional[str] = None,
        start: Optional[datetime | str] = None,
        status: str = "confirmed",
    ) -> dict:
        """Assert exactly one event matches the predicates.  Returns it."""
        results = self.find_events(
            on,
            summary=summary,
            summary_contains=summary_contains,
            start=start,
            status=status,
        )
        if len(results) != 1:
            raise ScenarioAssertionError(
                f"expected exactly one event on {on!r} matching "
                f"summary={summary!r} summary_contains={summary_contains!r} "
                f"start={start!r} status={status!r}, found {len(results)}: "
                f"{[(e.get('id'), e.get('summary')) for e in results]}"
            )
        return results[0]

    def assert_event_count(
        self,
        on: str,
        expected: int,
        *,
        include_cancelled: bool = False,
    ) -> None:
        actual = self.google.event_count(
            self.cal(on), include_cancelled=include_cancelled,
        )
        if actual != expected:
            raise ScenarioAssertionError(
                f"expected {expected} events on {on!r} "
                f"(include_cancelled={include_cancelled}), got {actual}"
            )

    def assert_no_events(self, on: str, *, include_cancelled: bool = False) -> None:
        self.assert_event_count(on, 0, include_cancelled=include_cancelled)

    def assert_no_event_with_summary(self, on: str, summary: str) -> None:
        results = self.find_events(on, summary=summary)
        if results:
            raise ScenarioAssertionError(
                f"expected no event on {on!r} with summary={summary!r}, "
                f"found {len(results)}"
            )

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------
    def cal(self, nickname: str) -> str:
        """Resolve a nickname to its calendar ID."""
        try:
            return self._calendars_by_nick[nickname]
        except KeyError:
            raise KeyError(
                f"unknown calendar nickname: {nickname!r}; "
                f"known: {sorted(self._calendars_by_nick)}"
            ) from None

    def nick(self, calendar_id: str) -> str:
        """Reverse-lookup: calendar ID to nickname."""
        try:
            return self._nicks_by_calendar[calendar_id]
        except KeyError:
            raise KeyError(f"unknown calendar id: {calendar_id!r}") from None

    @staticmethod
    def _build_event_body(
        *,
        summary: str,
        start: datetime | str,
        duration_minutes: int,
        attendees: Optional[list[dict]] = None,
        extended_properties: Optional[dict] = None,
        transparency: Optional[str] = None,
        location: Optional[str] = None,
        description: Optional[str] = None,
    ) -> dict:
        start_iso = _coerce_iso(start)
        # All-day if the input had no time component.
        if "T" not in start_iso:
            end_iso = (
                datetime.fromisoformat(start_iso) + timedelta(days=1)
            ).date().isoformat()
            body: dict = {
                "summary": summary,
                "start": {"date": start_iso},
                "end": {"date": end_iso},
            }
        else:
            start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
            end_dt = start_dt + timedelta(minutes=duration_minutes)
            body = {
                "summary": summary,
                "start": {"dateTime": start_iso, "timeZone": "UTC"},
                "end": {
                    "dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "timeZone": "UTC",
                },
            }
        if attendees is not None:
            body["attendees"] = attendees
        if extended_properties is not None:
            body["extendedProperties"] = extended_properties
        if transparency is not None:
            body["transparency"] = transparency
        if location is not None:
            body["location"] = location
        if description is not None:
            body["description"] = description
        return body

    @staticmethod
    def _merge_event_body(current: dict, overrides: dict) -> dict:
        """Build an update body by overlaying ``overrides`` on
        the API-shape ``current`` event.

        Recognises ``start`` as a string (which is auto-converted
        to a dateTime/date dict) and ``duration_minutes`` (recomputes
        end from start).
        """
        merged: dict = {
            k: v for k, v in current.items()
            if k in {
                "summary", "description", "location", "start", "end",
                "attendees", "transparency", "visibility", "colorId",
                "extendedProperties", "recurrence",
            }
        }
        for k, v in overrides.items():
            if k == "start" and isinstance(v, (str, datetime)):
                start_iso = _coerce_iso(v)
                if "T" not in start_iso:
                    merged["start"] = {"date": start_iso}
                else:
                    merged["start"] = {"dateTime": start_iso, "timeZone": "UTC"}
            elif k == "duration_minutes":
                start = merged.get("start", {})
                if "dateTime" in start:
                    start_dt = datetime.fromisoformat(
                        start["dateTime"].replace("Z", "+00:00")
                    )
                    end_dt = start_dt + timedelta(minutes=int(v))
                    merged["end"] = {
                        "dateTime": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "timeZone": "UTC",
                    }
            elif k == "end" and isinstance(v, (str, datetime)):
                end_iso = _coerce_iso(v)
                if "T" not in end_iso:
                    merged["end"] = {"date": end_iso}
                else:
                    merged["end"] = {"dateTime": end_iso, "timeZone": "UTC"}
            else:
                merged[k] = v
        return merged


def _coerce_iso(value: datetime | str) -> str:
    """Return the canonical ISO string for a datetime / ISO input.

    All-day inputs (date only) round-trip; timed inputs are
    normalised to UTC with a ``Z`` suffix.
    """
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        value = value.astimezone(UTC)
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, str):
        return value
    raise TypeError(f"unsupported time input: {value!r}")
