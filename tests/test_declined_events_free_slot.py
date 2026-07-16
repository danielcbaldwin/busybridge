"""Declined events free the slot (``declined_events_free_slot``, default on).

When the user declines a CLIENT-sourced (or main-native) meeting, it must
stop blocking their other calendars:

* the PEER busy-block projections go absent — the same mechanism as
  ``show_as == 'free'``;
* the MAIN full copy stays visible but renders with
  ``transparency: "transparent"`` so it no longer blocks;
* granularity follows Google's own layering: an instance-level decline
  frees only that occurrence, a series-level decline frees the whole
  series;
* PERSONAL sources always block (README promise) and the setting can be
  turned off to restore the status quo;
* re-accepting brings the busy block back;
* the origin RSVP writeback is unaffected — a decline still reaches the
  source, and a pending (undelivered) decline keeps the slot free.

Each test drives the full reconcile pipeline (ingest → plan → diff →
drain) against the fake Google, mirroring test_adversarial_recurrence's
scenario idioms, and asserts a zero-op steady state.
"""

from __future__ import annotations

import contextlib
from datetime import timezone

import pytest
from dateutil.parser import isoparse

from app.config import Settings
from app.ledger.identity import is_managed_google_event_id
from app.ledger.payload import EP_PROJ_ID
from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio

UTC = timezone.utc


# ---------------------------------------------------------------------------
# Helpers (test_adversarial_recurrence idioms)
# ---------------------------------------------------------------------------
@contextlib.asynccontextmanager
async def scenario():
    """Scenario with guaranteed teardown (an unclosed in-memory
    aiosqlite connection wedges pytest at shutdown)."""
    s = Scenario()
    try:
        yield s
    finally:
        with contextlib.suppress(Exception):
            await s.close()


def _norm_start(ev: dict) -> str:
    st = ev.get("start") or {}
    if "dateTime" in st:
        return isoparse(st["dateTime"]).astimezone(UTC).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    return st.get("date") or ""


def _is_managed(ev: dict) -> bool:
    if is_managed_google_event_id(ev.get("id") or ""):
        return True
    priv = (ev.get("extendedProperties") or {}).get("private") or {}
    return bool(priv.get(EP_PROJ_ID))


def _occurrences(
    s: Scenario,
    nick: str,
    *,
    managed_only: bool = False,
    summary_contains: str | None = None,
) -> dict[str, list[dict]]:
    """Confirmed occurrence start -> [events] on a calendar, expanded."""
    out: dict[str, list[dict]] = {}
    for ev in s.list_events(nick, single_events=True):
        if ev.get("status") == "cancelled":
            continue
        if managed_only and not _is_managed(ev):
            continue
        if summary_contains is not None and summary_contains not in (
            ev.get("summary") or ""
        ):
            continue
        out.setdefault(_norm_start(ev), []).append(ev)
    return out


async def _assert_zero_op_steady_state(s: Scenario, label: str) -> None:
    """THE churn detector: a reconcile with no source change must
    enqueue 0 ops.  One echo pass is absorbed first (the documented,
    bounded FINDING-1 echo after instance-level changes)."""
    echo = await s.run_reconciler("alice")
    out = await s.run_reconciler("alice")
    assert out["enqueued"] == 0, (
        f"{label}: op churn — reconcile with NO source change enqueued "
        f"{out['enqueued']} op(s) (echo pass had absorbed "
        f"{echo['enqueued']})"
    )


def _self_attendees(status: str) -> list[dict]:
    return [
        {"email": "alice@example.com", "self": True,
         "responseStatus": status},
        {"email": "bob@example.com", "organizer": True,
         "responseStatus": "accepted"},
    ]


def _recurring_meeting(s: Scenario, event_id: str, *, rsvp: str = "accepted"):
    """A recurring client meeting alice attends (organized by bob, so
    the copy is locked — RSVP is still writable)."""
    return s.google.insert_event(s.cal("client_a"), {
        "id": event_id,
        "summary": "Weekly client sync",
        "start": {"dateTime": "2026-02-02T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-02T09:30:00Z", "timeZone": "UTC"},
        "recurrence": ["RRULE:FREQ=WEEKLY;COUNT=4"],
        "organizer": {"email": "bob@example.com"},
        "attendees": _self_attendees(rsvp),
    })


async def _two_client_alice(s: Scenario):
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    return await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )


ALL_STARTS = {
    "2026-02-02T09:00:00Z", "2026-02-09T09:00:00Z",
    "2026-02-16T09:00:00Z", "2026-02-23T09:00:00Z",
}


# ---------------------------------------------------------------------------
# Occurrence-level decline
# ---------------------------------------------------------------------------
async def test_occurrence_decline_frees_that_slot_only():
    """Declining ONE occurrence (on the source) removes exactly that
    occurrence's busy block from the peer; the other occurrences keep
    blocking, and the main copy of the declined occurrence stays
    visible but transparent."""
    async with scenario() as s:
        await _two_client_alice(s)
        series = _recurring_meeting(s, "declfree00001")
        await s.run_reconciler_until_quiescent("alice", max_passes=5)
        assert set(_occurrences(s, "client_b", managed_only=True)) == ALL_STARTS

        # Decline the 2026-02-16 occurrence on the SOURCE.
        s.update_event(
            "client_a", f"{series['id']}_20260216T090000Z",
            attendees=_self_attendees("declined"),
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        peer = _occurrences(s, "client_b", managed_only=True)
        assert "2026-02-16T09:00:00Z" not in peer, (
            f"declined occurrence still blocks the peer: {sorted(peer)}"
        )
        assert set(peer) == ALL_STARTS - {"2026-02-16T09:00:00Z"}, (
            f"an undeclined occurrence lost its busy block: {sorted(peer)}"
        )

        # The main copy of the declined occurrence is still VISIBLE —
        # the user sees the meeting they declined — but transparent.
        main = _occurrences(s, "main", summary_contains="Weekly client sync")
        assert "2026-02-16T09:00:00Z" in main, (
            "the declined occurrence's main copy must remain visible"
        )
        declined_copy = main["2026-02-16T09:00:00Z"][0]
        assert declined_copy.get("transparency") == "transparent", (
            f"declined main copy must be transparent, got "
            f"{declined_copy.get('transparency')!r}"
        )
        # Undeclined occurrences stay opaque (no transparency set).
        other_copy = main["2026-02-09T09:00:00Z"][0]
        assert other_copy.get("transparency") != "transparent", (
            "an undeclined occurrence's main copy went transparent"
        )
        # The source event itself is untouched (still there, declined).
        src = _occurrences(s, "client_a", summary_contains="Weekly client sync")
        assert "2026-02-16T09:00:00Z" in src

        await _assert_zero_op_steady_state(s, "occurrence decline")


async def test_reaccept_restores_the_busy_block():
    """Re-accepting a previously declined occurrence brings its busy
    block back on the peer and makes the main copy opaque again."""
    async with scenario() as s:
        await _two_client_alice(s)
        series = _recurring_meeting(s, "declfree00002")
        await s.run_reconciler_until_quiescent("alice", max_passes=5)
        # Settle the baseline (consume the main-copy write echo) so the
        # next source change cannot collide with it — same idiom as
        # test_rsvp_only_does_not_clobber_source_on_source_side_change.
        await s.run_reconciler_until_quiescent("alice", max_passes=2)

        inst_id = f"{series['id']}_20260216T090000Z"
        s.update_event("client_a", inst_id,
                       attendees=_self_attendees("declined"))
        await s.run_reconciler_until_quiescent("alice", max_passes=6)
        await s.run_reconciler_until_quiescent("alice", max_passes=2)
        assert "2026-02-16T09:00:00Z" not in _occurrences(
            s, "client_b", managed_only=True,
        ), "precondition: decline freed the slot"

        # Change of plans — alice re-accepts.
        s.update_event("client_a", inst_id,
                       attendees=_self_attendees("accepted"))
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        peer = _occurrences(s, "client_b", managed_only=True)
        assert "2026-02-16T09:00:00Z" in peer, (
            f"re-accept did not restore the busy block: {sorted(peer)}"
        )
        main = _occurrences(s, "main", summary_contains="Weekly client sync")
        copy = main["2026-02-16T09:00:00Z"][0]
        assert copy.get("transparency") != "transparent", (
            "re-accepted main copy must be opaque again"
        )
        await _assert_zero_op_steady_state(s, "re-accept")


async def test_decline_on_main_writes_back_and_frees_slot():
    """Interaction check: declining an occurrence ON THE MAIN COPY still
    fires the origin RSVP writeback (the decline reaches the source),
    AND the freed busy block does not flap back while converging."""
    async with scenario() as s:
        await _two_client_alice(s)
        _recurring_meeting(s, "declfree00003")
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Find the managed main copy's 2026-02-16 instance and decline it.
        main_inst = None
        for ev in s.list_events("main", single_events=True):
            if _norm_start(ev) == "2026-02-16T09:00:00Z" and _is_managed(ev):
                main_inst = ev
                break
        assert main_inst is not None, "managed main copy instance not found"
        atts = [dict(a) for a in main_inst.get("attendees") or []]
        assert any(a.get("self") for a in atts)
        for a in atts:
            if a.get("self"):
                a["responseStatus"] = "declined"
        s.update_event("main", main_inst["id"], attendees=atts)

        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        # The decline reached the ORIGIN occurrence (writeback intact).
        origin = s.google.get_event(
            s.cal("client_a"), "declfree00003_20260216T090000Z",
        )
        alice = next(
            (a for a in origin.get("attendees") or []
             if a.get("email") == "alice@example.com"), None,
        )
        assert alice is not None and alice["responseStatus"] == "declined", (
            f"decline did not reach the source: {origin.get('attendees')}"
        )
        # ...and the slot is free on the peer.
        peer = _occurrences(s, "client_b", managed_only=True)
        assert "2026-02-16T09:00:00Z" not in peer
        assert "2026-02-09T09:00:00Z" in peer

        # No flap: further reconciles must not bring the block back.
        await _assert_zero_op_steady_state(s, "decline on main")
        assert "2026-02-16T09:00:00Z" not in _occurrences(
            s, "client_b", managed_only=True,
        ), "the freed slot flapped back busy"


async def test_pending_decline_keeps_slot_free_during_source_reingest():
    """The freed slot cannot flap back busy MID-WRITEBACK: while
    ``origin_writeback_pending=1`` client ingest preserves the local
    declined status against a stale source re-read, so the planner
    keeps the peer projection absent until the patch lands."""
    async with scenario() as s:
        user = await _two_client_alice(s)
        s.google.insert_event(s.cal("client_a"), {
            "id": "declpend00001",
            "summary": "One-off review",
            "start": {"dateTime": "2026-02-05T10:00:00Z", "timeZone": "UTC"},
            "end": {"dateTime": "2026-02-05T10:30:00Z", "timeZone": "UTC"},
            "organizer": {"email": "bob@example.com"},
            "attendees": _self_attendees("accepted"),
        })
        await s.run_reconciler_until_quiescent("alice", max_passes=5)
        # Settle the baseline (consume the main-copy write echo) before
        # wedging the pending state — see test_reaccept_restores_the_busy_block.
        await s.run_reconciler_until_quiescent("alice", max_passes=2)
        assert _occurrences(s, "client_b", managed_only=True), (
            "precondition: the accepted meeting blocks the peer"
        )

        # Re-arm the exact wedged production state: the decline is
        # stored locally with the writeback still pending (undelivered).
        db = await s.setup_db()
        row = await (await db.execute(
            "SELECT id FROM ledger_events "
            "WHERE user_id = ? AND source_event_id = 'declpend00001'",
            (user.user_id,),
        )).fetchone()
        await db.execute(
            "UPDATE ledger_events SET user_rsvp_status = 'declined', "
            "origin_writeback_pending = 1 WHERE id = ?", (row["id"],),
        )
        await db.commit()
        # The organizer moves the meeting on the SOURCE with alice's
        # stale (accepted) response — the re-ingest that used to clobber
        # pending RSVPs.
        s.update_event(
            "client_a", "declpend00001",
            start="2026-02-05T11:00:00Z", end="2026-02-05T11:30:00Z",
            attendees=_self_attendees("accepted"),
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        # The decline survived, was delivered, and the peer stays free.
        led = await (await db.execute(
            "SELECT user_rsvp_status FROM ledger_events WHERE id = ?",
            (row["id"],),
        )).fetchone()
        assert led["user_rsvp_status"] == "declined", (
            "source re-ingest clobbered the pending decline"
        )
        assert _occurrences(s, "client_b", managed_only=True) == {}, (
            "the declined meeting's busy block flapped back mid-writeback"
        )
        origin = s.google.get_event(s.cal("client_a"), "declpend00001")
        alice = next(
            (a for a in origin.get("attendees") or []
             if a.get("email") == "alice@example.com"), None,
        )
        assert alice is not None and alice["responseStatus"] == "declined"


# ---------------------------------------------------------------------------
# Series-level decline
# ---------------------------------------------------------------------------
async def test_series_decline_frees_the_whole_series():
    async with scenario() as s:
        await _two_client_alice(s)
        series = _recurring_meeting(s, "declfree00004")
        await s.run_reconciler_until_quiescent("alice", max_passes=5)
        # Settle the baseline (consume the main-copy write echo) before
        # the source-side change — see test_reaccept_restores_the_busy_block.
        await s.run_reconciler_until_quiescent("alice", max_passes=2)
        assert set(_occurrences(s, "client_b", managed_only=True)) == ALL_STARTS

        # Decline the whole series on the source.
        s.update_event("client_a", series["id"],
                       attendees=_self_attendees("declined"))
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        assert _occurrences(s, "client_b", managed_only=True) == {}, (
            "a series-level decline must free every occurrence's slot"
        )
        # The main copy of the series is still visible — transparent.
        main = _occurrences(s, "main", summary_contains="Weekly client sync")
        assert set(main) == ALL_STARTS, (
            "the declined series' main copy must remain visible"
        )
        for start, evs in main.items():
            assert evs[0].get("transparency") == "transparent", (
                f"declined series' main copy at {start} is not transparent"
            )
        # The source series is untouched.
        assert set(
            _occurrences(s, "client_a", summary_contains="Weekly client sync")
        ) == ALL_STARTS

        await _assert_zero_op_steady_state(s, "series decline")


# ---------------------------------------------------------------------------
# Personal-source declined RSVPs also free the slot (Ghost-Main fork
# behaviour — upstream busybridge kept personal always-blocks).
# ---------------------------------------------------------------------------
async def test_personal_event_with_declined_rsvp_frees_slot():
    async with scenario() as s:
        s.given_calendar("main")
        s.given_calendar("client_a")
        s.given_calendar("personal_a")
        await s.given_user(
            "alice", main="main", clients=["client_a"],
            personals=["personal_a"],
        )
        s.google.insert_event(s.cal("personal_a"), {
            "id": "declpers00001",
            "summary": "Family dinner",
            "start": {"dateTime": "2026-02-05T18:00:00Z", "timeZone": "UTC"},
            "end": {"dateTime": "2026-02-05T19:00:00Z", "timeZone": "UTC"},
            "organizer": {"email": "partner@example.com"},
            "attendees": [
                {"email": "alice@example.com", "self": True,
                 "responseStatus": "declined"},
                {"email": "partner@example.com", "organizer": True,
                 "responseStatus": "accepted"},
            ],
        })
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        # Declined personal-source event: no busy block anywhere.
        s.assert_no_event_with_summary("main", "Busy (personal)")
        s.assert_no_event_with_summary("client_a", "Busy (personal)")


# ---------------------------------------------------------------------------
# Setting off → status quo
# ---------------------------------------------------------------------------
async def test_setting_off_preserves_status_quo(monkeypatch):
    """With ``declined_events_free_slot`` off, a declined occurrence
    keeps its busy block and its main copy stays opaque — byte-for-byte
    the pre-decision behaviour.

    Both the planner and the renderer read the setting through their
    own module-level ``get_settings`` binding (which is lru_cached in
    app.config), so both bindings are monkeypatched with a fresh
    Settings object rather than mutating the cached instance.
    """
    cfg = Settings(declined_events_free_slot=False)
    monkeypatch.setattr("app.ledger.planner.get_settings", lambda: cfg)
    monkeypatch.setattr("app.ledger.payload.get_settings", lambda: cfg)

    async with scenario() as s:
        await _two_client_alice(s)
        series = _recurring_meeting(s, "declfree00005")
        await s.run_reconciler_until_quiescent("alice", max_passes=5)

        s.update_event(
            "client_a", f"{series['id']}_20260216T090000Z",
            attendees=_self_attendees("declined"),
        )
        await s.run_reconciler_until_quiescent("alice", max_passes=6)

        peer = _occurrences(s, "client_b", managed_only=True)
        assert set(peer) == ALL_STARTS, (
            f"setting off: the declined occurrence must keep blocking, "
            f"got {sorted(peer)}"
        )
        main = _occurrences(s, "main", summary_contains="Weekly client sync")
        copy = main["2026-02-16T09:00:00Z"][0]
        assert copy.get("transparency") != "transparent", (
            "setting off: the declined main copy must stay opaque"
        )
        await _assert_zero_op_steady_state(s, "setting off")
