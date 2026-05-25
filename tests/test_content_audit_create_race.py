"""The content audit catches a create-race stale title.

A rapid create-then-rename can leave BusyBridge's ledger holding
Google's placeholder ("New Event") while the source already shows the
real title under the *same* change cursor — so incremental sync never
re-delivers it (modelled by ``fake.silently_edit_event``). The periodic
content audit (a full forward ``list`` + content compare) is the
backstop that catches and corrects it.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _main_has(s: Scenario, main_cal: str, summary: str) -> bool:
    return any(
        (ev.get("summary") == summary)
        for ev in s.list_events(main_cal, single_events=True)
    )


async def test_audit_fixes_create_race_stale_title():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])

    ev = s.given_event(
        "client_a", summary="New Event", start="2026-02-02T09:00:00Z",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)
    assert _main_has(s, "main", "New Event"), "main copy not created"

    # Create-race: the source title changes, but Google does NOT advance
    # the change cursor (same revision) — so incremental sync is blind.
    s.google.silently_edit_event(
        s.cal("client_a"), ev["id"], summary="Responsible Disclosure",
    )

    # A normal reconcile (incremental) cannot see it — main stays stale.
    await s.run_reconciler_until_quiescent("alice", max_passes=3)
    assert _main_has(s, "main", "New Event"), "incremental unexpectedly saw it"
    assert not _main_has(s, "main", "Responsible Disclosure")

    # The content audit re-lists + content-compares, so it catches it.
    await s.run_audit("alice")
    assert _main_has(s, "main", "Responsible Disclosure"), (
        "audit did not correct the stale title"
    )
    assert not _main_has(s, "main", "New Event")
    await s.close()


async def test_audit_does_not_revert_fresher_ledger_from_stale_read():
    """The ``updated >=`` guard: if a stale replica read carries an older
    ``updated`` than the ledger, the audit must NOT apply it (which would
    revert a legit edit incremental sync already pulled in)."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    await s.given_user("alice", main="main", clients=["client_a"])

    ev = s.given_event(
        "client_a", summary="Real Title", start="2026-02-02T09:00:00Z",
    )
    await s.run_reconciler_until_quiescent("alice", max_passes=4)
    assert _main_has(s, "main", "Real Title")

    # Simulate a stale replica: content shows an OLD value, and its
    # `updated` is older than what the ledger already recorded.
    s.google.silently_edit_event(
        s.cal("client_a"), ev["id"],
        summary="Stale Old Value", updated="2020-01-01T00:00:00.000Z",
    )
    await s.run_audit("alice")
    # Guard holds: the older read is ignored; the ledger value stands.
    assert _main_has(s, "main", "Real Title"), "audit reverted to a stale read"
    assert not _main_has(s, "main", "Stale Old Value")
    await s.close()
