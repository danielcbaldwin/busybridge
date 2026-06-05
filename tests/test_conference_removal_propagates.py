"""Removing the video call from a source event must clear it on the mirror.

Regression: the conference-link debounce only handled room SWAPS (a non-None
new conferenceId).  A REMOVAL produced a ``None`` candidate that was
indistinguishable from "no candidate", so it never confirmed and the main copy
kept serving a dead Meet link forever.  The debounce now uses an empty-string
sentinel for "pending removal" so a settled removal confirms on the second
read.
"""

from __future__ import annotations

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


def _conf(room, uri):
    return {
        "conferenceId": room,
        "entryPoints": [{"entryPointType": "video", "uri": uri}],
        "conferenceSolution": {"key": {"type": "hangoutsMeet"}, "name": "Google Meet"},
    }


def _conf_id(ev):
    cd = ev.get("conferenceData")
    return cd.get("conferenceId") if cd else None


async def test_conference_removal_propagates_to_main_copy():
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    ev0 = s.given_event(
        "client_a", summary="Call", start="2026-02-02T09:00:00Z",
        conference_data=_conf("ROOM-to-remove", "https://meet.example/dead-link"),
    )
    src_id = ev0["id"]
    await s.run_reconciler_until_quiescent("alice", max_passes=8)

    main0 = [e for e in s.list_events("main") if e.get("summary") == "Call"]
    assert main0 and _conf_id(main0[0]) == "ROOM-to-remove"

    # User removes the video call from the source event entirely.
    cal = s.google._calendars[s.cal("client_a")]
    cal.events[src_id].conference_data = None
    cal.change_counter += 1
    cal.events[src_id].change_seq = cal.change_counter

    # Conference changes propagate via the content audit (which re-reads every
    # windowed event), debounced over two consecutive reads.  Run several audit
    # + reconcile cycles so the settled removal confirms and re-renders.
    for _ in range(4):
        await s.run_audit("alice")
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

    main1 = [e for e in s.list_events("main") if e.get("summary") == "Call"]
    assert main1, "main copy vanished"
    assert _conf_id(main1[0]) is None, (
        f"main copy kept conference {_conf_id(main1[0])!r} after the source "
        f"removed it (dead link)"
    )
    await s.close()
