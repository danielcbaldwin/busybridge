"""Ground-truth oracle for soak tests.

Tracks the canonical "what should be on each calendar" derived
solely from the simulated user's actions.  After every reconcile
cycle, the invariant checker compares this oracle against the
actual state on the FakeGoogleCalendar and the ledger DB.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class OracleEvent:
    """One event the user has scheduled, before any projection."""
    source_kind: str  # 'client', 'personal', 'webcal', 'main_native'
    source_calendar: str
    source_event_id: str
    summary: str
    start_iso: str
    end_iso: str
    show_as: str = "busy"  # 'busy' or 'free'
    user_can_edit: bool = True


@dataclass
class Oracle:
    """The simulated user's view of what's on each calendar.

    Indexed by ``(source_kind, source_calendar, source_event_id)``.
    Active events here MUST correspond to ledger rows + projections
    after the next reconcile.
    """

    main_calendar: str = "main"
    client_calendars: tuple[str, ...] = ("client_a", "client_b")
    personal_calendars: tuple[str, ...] = ()
    webcal_subscriptions: tuple[str, ...] = ()

    events: dict[tuple[str, str, str], OracleEvent] = field(default_factory=dict)
    cancelled_user_intentional: set[tuple[str, str, str]] = field(default_factory=set)

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------
    def add(self, ev: OracleEvent) -> None:
        self.events[(ev.source_kind, ev.source_calendar, ev.source_event_id)] = ev

    def cancel(self, source_kind: str, source_calendar: str, source_event_id: str) -> None:
        key = (source_kind, source_calendar, source_event_id)
        self.events.pop(key, None)

    def user_intentionally_deletes(
        self, source_kind: str, source_calendar: str, source_event_id: str,
    ) -> None:
        """User deleted the synced copy on main; the event remains
        on the source but must NOT be re-projected.  Tracked so
        the invariant checker tolerates the divergence."""
        key = (source_kind, source_calendar, source_event_id)
        self.cancelled_user_intentional.add(key)
        self.events.pop(key, None)

    # ------------------------------------------------------------------
    # Derived views (the projection rules)
    # ------------------------------------------------------------------
    def expected_on(self, calendar: str) -> list[OracleEvent]:
        """Return the events that should be present on ``calendar``
        right now, derived purely from oracle state."""
        out: list[OracleEvent] = []
        for ev in self.events.values():
            if calendar == self.main_calendar:
                if ev.source_kind == "main_native":
                    out.append(ev)
                else:
                    # main-target projection of every non-main source.
                    out.append(ev)
            elif calendar in self.client_calendars:
                if ev.source_kind == "main_native":
                    # main native → busy block on every client unless free.
                    if ev.show_as != "free":
                        out.append(ev)
                elif ev.source_kind == "client":
                    if ev.source_calendar == calendar:
                        # Origin client: native event, no projection.
                        out.append(ev)
                    elif ev.show_as != "free":
                        out.append(ev)
                elif ev.source_kind == "personal":
                    out.append(ev)
                elif ev.source_kind == "webcal":
                    if ev.show_as != "free":
                        out.append(ev)
            elif calendar in self.personal_calendars:
                if ev.source_kind == "personal" and ev.source_calendar == calendar:
                    out.append(ev)
        return out

    def expected_main_copies(self) -> int:
        """How many full-detail copies should exist on main."""
        return sum(
            1 for ev in self.events.values()
            if ev.source_kind != "main_native"
        )

    def expected_busy_blocks_on(self, client_calendar: str) -> int:
        """How many busy blocks should be present on a client cal."""
        count = 0
        for ev in self.events.values():
            if ev.source_kind == "main_native":
                if ev.show_as != "free":
                    count += 1
            elif ev.source_kind == "client":
                if ev.source_calendar != client_calendar and ev.show_as != "free":
                    count += 1
            elif ev.source_kind == "personal":
                count += 1
            elif ev.source_kind == "webcal":
                if ev.show_as != "free":
                    count += 1
        return count
