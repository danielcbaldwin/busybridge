"""Soak harness — drives a simulated user and runs invariants.

A :class:`SoakHarness` wraps a :class:`tests.integration.framework.Scenario`
and a :class:`tests.soak.oracle.Oracle`.  Every call to one of the
``user_*`` methods updates BOTH the fake Google state AND the
oracle, so the invariant checker can compare them.

Usage::

    h = SoakHarness()
    await h.setup()
    h.user_creates_event_on(client_nick="client_a", summary="Standup", ...)
    await h.run_reconciler()
    violations = await h.check_invariants()
    assert not violations, violations
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from tests.integration.framework import Scenario
from tests.soak.invariants import InvariantChecker, InvariantViolation
from tests.soak.oracle import Oracle, OracleEvent


@dataclass
class SoakHarness:
    """One simulated user with main + N clients + ground-truth oracle."""

    seed: int = 0
    main_nick: str = "main"
    client_nicks: tuple[str, ...] = ("client_a", "client_b", "client_c")

    scenario: Scenario = field(init=False)
    oracle: Oracle = field(init=False)
    user_nick: str = "alice"
    _event_seq: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self.scenario = Scenario(seed=self.seed)
        self.oracle = Oracle(
            main_calendar=self.main_nick,
            client_calendars=tuple(self.client_nicks),
        )

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------
    async def setup(self) -> None:
        self.scenario.given_calendar(self.main_nick)
        for nick in self.client_nicks:
            self.scenario.given_calendar(nick)
        await self.scenario.given_user(
            self.user_nick, main=self.main_nick, clients=list(self.client_nicks),
        )

    # ------------------------------------------------------------------
    # User actions (update fake + oracle atomically)
    # ------------------------------------------------------------------
    def user_creates_event_on(
        self,
        *,
        client_nick: str,
        summary: str,
        start_iso: str,
        end_iso: Optional[str] = None,
        show_as: str = "busy",
    ) -> str:
        """Plant an event on a client calendar (the user's view)."""
        if end_iso is None:
            # +30 min by default.
            from datetime import datetime, timedelta
            dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
            end_iso = (dt + timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
        body_extra = {}
        if show_as == "free":
            body_extra["transparency"] = "transparent"
        result = self.scenario.given_event(
            client_nick,
            summary=summary,
            start=start_iso,
            **body_extra,
        )
        eid = result["id"]
        self.oracle.add(OracleEvent(
            source_kind="client",
            source_calendar=client_nick,
            source_event_id=eid,
            summary=summary,
            start_iso=start_iso,
            end_iso=end_iso,
            show_as=show_as,
        ))
        return eid

    def user_cancels_event(self, client_nick: str, event_id: str) -> None:
        self.scenario.cancel_event(client_nick, event_id)
        self.oracle.cancel("client", client_nick, event_id)

    # ------------------------------------------------------------------
    # Drive the reconciler
    # ------------------------------------------------------------------
    async def run_reconciler(self) -> dict:
        return await self.scenario.run_reconciler(self.user_nick)

    async def run_until_quiescent(self, max_passes: int = 5) -> None:
        await self.scenario.run_reconciler_until_quiescent(
            self.user_nick, max_passes=max_passes,
        )

    # ------------------------------------------------------------------
    # Invariants
    # ------------------------------------------------------------------
    async def check_invariants(self) -> list[str]:
        user = self.scenario.user(self.user_nick)
        db = await self.scenario.setup_db()
        checker = InvariantChecker(
            db=db,
            google=self.scenario.google,
            oracle=self.oracle,
            user_id=user.user_id,
            main_google_id=user.main_google_calendar_id,
            client_google_ids={
                nick: self.scenario.cal(nick)
                for nick in self.client_nicks
            },
            client_db_ids={
                nick: user.client_calendar_ids[nick]
                for nick in self.client_nicks
                if nick in user.client_calendar_ids
            },
        )
        return await checker.check_all()

    async def assert_invariants(self) -> None:
        violations = await self.check_invariants()
        if violations:
            raise InvariantViolation(
                f"{len(violations)} invariant(s) failed:\n  "
                + "\n  ".join(violations),
            )

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    async def close(self) -> None:
        await self.scenario.close()
