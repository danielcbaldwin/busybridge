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

import aiosqlite

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
    _users_by_nick: dict[str, "_LedgerUser"] = field(init=False, default_factory=dict)
    _db: Optional[aiosqlite.Connection] = field(init=False, default=None, repr=False)

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
        conference_data: Optional[dict] = None,
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
            conference_data=conference_data,
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
        timezone: str = "UTC",
        **extra: Any,
    ) -> dict:
        """Insert a recurring series.  ``rrule`` is the full RRULE
        line including the ``RRULE:`` prefix.

        ``timezone`` is the IANA zone the series' RRULE expands in;
        pass e.g. ``"America/New_York"`` (with a ``start`` carrying
        that zone's offset) to model a DST-sensitive series."""
        body = self._build_event_body(
            summary=summary,
            start=start,
            duration_minutes=duration_minutes,
            timezone=timezone,
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
        conference_data: Optional[dict] = None,
        timezone: str = "UTC",
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
            if timezone == "UTC":
                end_str = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            else:
                # Preserve the start's own UTC offset on the end so the
                # fake anchors the event correctly; ``timeZone`` names
                # the IANA zone the RRULE expands in.
                end_str = end_dt.isoformat()
            body = {
                "summary": summary,
                "start": {"dateTime": start_iso, "timeZone": timezone},
                "end": {"dateTime": end_str, "timeZone": timezone},
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
        if conference_data is not None:
            body["conferenceData"] = conference_data
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


# ---------------------------------------------------------------------------
# Ledger integration (REWRITE_PLAN.md Stage 2)
# ---------------------------------------------------------------------------
@dataclass
class _LedgerUser:
    """One user registered with the ledger system in a Scenario."""
    user_id: int
    email: str
    main_nick: str
    main_google_calendar_id: str
    client_calendar_ids: dict[str, int] = field(default_factory=dict)
    personal_calendar_ids: dict[str, int] = field(default_factory=dict)
    webcal_subscription_ids: dict[str, int] = field(default_factory=dict)
    """Maps client *nickname* → client_calendars.id (the integer FK)."""


# Patch helper methods onto Scenario so the file's main class
# definition stays compact while keeping the ledger glue here.
async def _scenario_setup_db(self: Scenario) -> aiosqlite.Connection:
    """Lazy-initialise an in-memory SQLite + load minimal app schema
    + ledger schema.  Idempotent."""
    if self._db is not None:
        return self._db
    # Late imports so importing the framework does not pull in
    # the entire app (and therefore is safe before app.config is
    # configured for tests).
    from app.ledger.schema import init_ledger_schema

    db = await aiosqlite.connect(":memory:", isolation_level=None)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA foreign_keys = ON")

    # Minimal app tables the ledger refers to via FK.  We don't need
    # the whole production schema — just users, client_calendars,
    # calendar_sync_state, main_calendar_sync_state,
    # webcal_subscriptions, personal_calendars.
    await db.executescript(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL UNIQUE,
            display_name TEXT,
            sync_paused BOOLEAN DEFAULT FALSE,
            main_calendar_id TEXT
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value_encrypted BLOB,
            value_plain TEXT,
            is_sensitive BOOLEAN DEFAULT FALSE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE webhook_channels (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            calendar_type TEXT NOT NULL,
            client_calendar_id INTEGER REFERENCES client_calendars(id),
            channel_id TEXT NOT NULL UNIQUE,
            resource_id TEXT NOT NULL,
            token TEXT NOT NULL DEFAULT '',
            expiration TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE client_calendars (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            google_calendar_id TEXT NOT NULL,
            display_name TEXT,
            color_id TEXT,
            calendar_type TEXT NOT NULL DEFAULT 'client',
            is_active BOOLEAN DEFAULT TRUE,
            disconnected_at TIMESTAMP
        );
        CREATE TABLE calendar_sync_state (
            id INTEGER PRIMARY KEY,
            client_calendar_id INTEGER NOT NULL
                REFERENCES client_calendars(id) ON DELETE CASCADE,
            sync_token TEXT,
            last_full_sync TIMESTAMP,
            last_incremental_sync TIMESTAMP,
            consecutive_failures INTEGER DEFAULT 0,
            last_error TEXT,
            UNIQUE(client_calendar_id)
        );
        CREATE TABLE main_calendar_sync_state (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            sync_token TEXT,
            last_full_sync TIMESTAMP,
            last_incremental_sync TIMESTAMP,
            consecutive_failures INTEGER DEFAULT 0,
            last_error TEXT,
            UNIQUE(user_id)
        );
        CREATE TABLE webcal_subscriptions (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            url TEXT NOT NULL,
            display_name TEXT,
            color_id TEXT,
            last_etag TEXT,
            last_polled_at TIMESTAMP,
            poll_interval_seconds INTEGER NOT NULL DEFAULT 3600,
            consecutive_failures INTEGER DEFAULT 0,
            last_error TEXT,
            is_active BOOLEAN DEFAULT TRUE,
            UNIQUE(user_id, url)
        );
        """
    )
    await init_ledger_schema(db)
    await db.commit()
    self._db = db
    return db


async def _scenario_given_user(
    self: Scenario,
    nickname: str,
    *,
    email: Optional[str] = None,
    main: str,
    clients: Iterable[str] = (),
    personals: Iterable[str] = (),
) -> _LedgerUser:
    """Register a user with the ledger system and bind them to
    previously-registered calendar nicknames.

    Args:
        nickname: short test handle for the user (``"alice"``).
        email: defaults to ``f"{nickname}@example.com"``.
        main: nickname of the user's main calendar.
        clients: nicknames of the user's client calendars.
        personals: nicknames of the user's personal calendars
            (read-only origin sources; not busy-block targets).
    """
    if nickname in self._users_by_nick:
        raise ValueError(f"user nickname already in use: {nickname!r}")
    db = await _scenario_setup_db(self)

    cursor = await db.execute(
        "INSERT INTO users (email) VALUES (?)",
        (email or f"{nickname}@example.com",),
    )
    user_id = int(cursor.lastrowid)

    main_id = self.cal(main)
    client_ids: dict[str, int] = {}
    for client_nick in clients:
        google_id = self.cal(client_nick)
        cur = await db.execute(
            """INSERT INTO client_calendars
                  (user_id, google_calendar_id, display_name,
                   calendar_type, is_active)
               VALUES (?, ?, ?, 'client', 1)""",
            (user_id, google_id, client_nick),
        )
        client_ids[client_nick] = int(cur.lastrowid)

    personal_ids: dict[str, int] = {}
    for personal_nick in personals:
        google_id = self.cal(personal_nick)
        cur = await db.execute(
            """INSERT INTO client_calendars
                  (user_id, google_calendar_id, display_name,
                   calendar_type, is_active)
               VALUES (?, ?, ?, 'personal', 1)""",
            (user_id, google_id, personal_nick),
        )
        personal_ids[personal_nick] = int(cur.lastrowid)
    await db.commit()

    user = _LedgerUser(
        user_id=user_id,
        email=email or f"{nickname}@example.com",
        main_nick=main,
        main_google_calendar_id=main_id,
        client_calendar_ids=client_ids,
        personal_calendar_ids=personal_ids,
    )
    self._users_by_nick[nickname] = user
    return user


async def _scenario_given_webcal(
    self: Scenario,
    user_nick: str,
    *,
    sub_nick: str,
    url: str,
) -> int:
    """Register a webcal subscription for a previously-created user."""
    user = self.user(user_nick)
    db = await _scenario_setup_db(self)
    cur = await db.execute(
        """INSERT INTO webcal_subscriptions
              (user_id, url, display_name, is_active)
           VALUES (?, ?, ?, 1)""",
        (user.user_id, url, sub_nick),
    )
    await db.commit()
    sub_id = int(cur.lastrowid)
    user.webcal_subscription_ids[sub_nick] = sub_id
    return sub_id


async def _scenario_run_reconciler(
    self: Scenario,
    user_nick: str,
    *,
    include_main: bool = True,
    drain: bool = True,
    run_discovery: bool = False,
    dry_run: bool = False,
    webcal_fetch=None,
) -> dict:
    """Drive one ingest → plan → diff → drain pass for a user.

    Returns the counters dict from the reconciler; tests can assert
    on it (``out['drain']['succeeded'] == 3`` etc.) or just rely
    on the observable Google state via ``find_events``.
    """
    from app.ledger.reconciler import reconcile_user

    user = self.user(user_nick)
    db = await _scenario_setup_db(self)
    # Build the list of calendars to ingest (active only) AND the
    # ID-mapping the diff/outbox use (all known calendars, including
    # disconnected — the outbox still needs to issue deletes
    # against them).
    active_clients: list[dict] = []
    all_clients: list[dict] = []
    for nick, cid in user.client_calendar_ids.items():
        row = await (await db.execute(
            "SELECT is_active FROM client_calendars WHERE id = ?", (cid,),
        )).fetchone()
        entry = {"id": cid, "google_calendar_id": self.cal(nick)}
        all_clients.append(entry)
        if row is not None and row["is_active"]:
            active_clients.append(entry)
    active_personals: list[dict] = []
    for nick, pid in user.personal_calendar_ids.items():
        row = await (await db.execute(
            "SELECT is_active FROM client_calendars WHERE id = ?", (pid,),
        )).fetchone()
        if row is not None and row["is_active"]:
            active_personals.append({"id": pid, "google_calendar_id": self.cal(nick)})
    webcal_subs: list[dict] = []
    for nick, sid in user.webcal_subscription_ids.items():
        row = await (await db.execute(
            "SELECT url, is_active FROM webcal_subscriptions WHERE id = ?", (sid,),
        )).fetchone()
        if row is not None and row["is_active"]:
            webcal_subs.append({"id": sid, "url": row["url"], "nick": nick})
    return await reconcile_user(
        db, self.google,
        user_id=user.user_id,
        user_email=user.email,
        main_google_calendar_id=user.main_google_calendar_id,
        client_calendars=active_clients,
        all_known_client_calendars=all_clients,
        personal_calendars=active_personals,
        webcal_subscriptions=webcal_subs,
        webcal_fetch=webcal_fetch,
        include_main=include_main,
        drain=drain,
        run_discovery=run_discovery,
        dry_run=dry_run,
        # Drive outbox timestamps off the simulated clock so backoff
        # is deterministic and advancing the clock fires retries.
        now=self.clock.now(),
    )


async def _scenario_run_reconciler_until_quiescent(
    self: Scenario,
    user_nick: str,
    *,
    max_passes: int = 5,
    advance_between_passes: timedelta | float = 1.0,
) -> list[dict]:
    """Run reconciliation until the outbox drains to zero.

    Useful when retries (post-write crash, rate limit, transient
    5xx) leave pending ops that the next pass should pick up.
    Advances the clock between passes so retry timers fire.
    """
    out: list[dict] = []
    for _ in range(max_passes):
        result = await _scenario_run_reconciler(self, user_nick)
        out.append(result)
        # Stop when no pending outbox ops remain for this user.
        db = await _scenario_setup_db(self)
        user = self.user(user_nick)
        row = await (await db.execute(
            """SELECT COUNT(*) AS n FROM outbox_operations
                WHERE user_id = ? AND status = 'pending'""",
            (user.user_id,),
        )).fetchone()
        if int(row["n"]) == 0:
            break
        # Advance past the longest backoff so the next pass picks up.
        self.advance(advance_between_passes)
    return out


def _scenario_user(self: Scenario, nickname: str) -> _LedgerUser:
    try:
        return self._users_by_nick[nickname]
    except KeyError:
        raise KeyError(
            f"unknown user nickname: {nickname!r}; "
            f"known: {sorted(self._users_by_nick)}"
        ) from None


async def _scenario_close(self: Scenario) -> None:
    """Tear down the in-memory DB.  Not strictly required —
    ``:memory:`` databases are reclaimed when the connection drops
    — but explicit cleanup keeps async-resource warnings quiet."""
    if self._db is not None:
        await self._db.close()
        self._db = None


# Bind the helpers onto the dataclass.  Doing it here rather than
# inline in the @dataclass body keeps the class definition compact
# and lets the ledger code be lazy-imported.
Scenario.setup_db = _scenario_setup_db  # type: ignore[attr-defined]
Scenario.given_user = _scenario_given_user  # type: ignore[attr-defined]
Scenario.given_webcal = _scenario_given_webcal  # type: ignore[attr-defined]
Scenario.run_reconciler = _scenario_run_reconciler  # type: ignore[attr-defined]
Scenario.run_reconciler_until_quiescent = (  # type: ignore[attr-defined]
    _scenario_run_reconciler_until_quiescent
)
Scenario.user = _scenario_user  # type: ignore[attr-defined]
Scenario.close = _scenario_close  # type: ignore[attr-defined]
