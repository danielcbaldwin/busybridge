"""WebCal placement contract — see webcal.md for the spec.

Tests are grouped to match the 34 spec test cases. Each docstring
quotes the spec point being verified, with the test number from
webcal.md §Tests in brackets so reviewers can cross-check.
"""

from __future__ import annotations

import json
import textwrap
from typing import Optional

import pytest

from tests.integration.framework import Scenario

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ics(*vevents: str) -> str:
    body = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:test\r\n"
    for v in vevents:
        lines = v.strip().split("\n")
        body += "BEGIN:VEVENT\r\n"
        for ln in lines:
            body += ln + "\r\n"
        body += "END:VEVENT\r\n"
    body += "END:VCALENDAR\r\n"
    return body


def _fetcher(body: str, etag: str = '"v1"'):
    async def fetch(url, if_none_match):
        return {"status": 200, "etag": etag, "body": body}
    return fetch


async def _setup_with_placement(
    sub_nick: str = "feed",
    placement_kind: str = "client",
    placement_target_nick: Optional[str] = "client_a",
) -> tuple[Scenario, int, dict]:
    """Build a scenario: main + two clients + a webcal sub placed on
    one of them.  Returns (scenario, subscription_id, ids_dict)."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    s.given_calendar("client_b")
    user = await s.given_user(
        "alice", main="main", clients=["client_a", "client_b"],
    )
    sub_id = await s.given_webcal(
        "alice", sub_nick=sub_nick, url=f"https://e.test/{sub_nick}.ics",
    )
    db = await s.setup_db()
    # Give clients real colors so we can verify color routing.
    await db.execute(
        "UPDATE client_calendars SET color_id = '7' WHERE id = ?",
        (user.client_calendar_ids["client_a"],),
    )
    await db.execute(
        "UPDATE client_calendars SET color_id = '3' WHERE id = ?",
        (user.client_calendar_ids["client_b"],),
    )
    # Stamp a feed display_prefix so the Source: footer has content.
    await db.execute(
        "UPDATE webcal_subscriptions SET display_prefix = ? WHERE id = ?",
        ("ISO Events", sub_id),
    )
    # Configure placement directly (bypassing the API to keep tests
    # focused on the planner / payload contract).
    if placement_kind == "client":
        target_id = user.client_calendar_ids[placement_target_nick]
        target_name = placement_target_nick  # the framework uses nick as display_name
        await db.execute(
            """UPDATE webcal_subscriptions
                  SET placement_kind = 'client',
                      placement_client_calendar_id = ?,
                      placement_client_display_name_cache = ?
                WHERE id = ?""",
            (target_id, target_name, sub_id),
        )
    await db.commit()
    return s, sub_id, {
        "user_id": user.user_id,
        "client_a_id": user.client_calendar_ids["client_a"],
        "client_b_id": user.client_calendar_ids["client_b"],
    }


async def _projection_states(s: Scenario, user_id: int) -> dict[tuple[str, Optional[int]], str]:
    db = await s.setup_db()
    rows = await (await db.execute(
        """SELECT p.target_kind, p.target_calendar_id, p.desired_state
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ? AND e.source_type = 'webcal'""",
        (user_id,),
    )).fetchall()
    return {
        (r["target_kind"], r["target_calendar_id"]): r["desired_state"]
        for r in rows
    }


# ---------------------------------------------------------------------------
# §Data Model — migration
# ---------------------------------------------------------------------------


async def test_01_migration_defaults_existing_subs_to_main_placement():
    """[1] Existing WebCal subscriptions default to Main placement after
    migration.  Verified via the framework's CREATE TABLE (which now
    mirrors the production placement defaults)."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    sub_id = await s.given_webcal(
        "alice", sub_nick="feed", url="https://e.test/m.ics",
    )
    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT placement_kind, placement_client_calendar_id,
                  placement_client_display_name_cache
             FROM webcal_subscriptions WHERE id = ?""",
        (sub_id,),
    )).fetchone()
    assert row["placement_kind"] == "main"
    assert row["placement_client_calendar_id"] is None
    assert row["placement_client_display_name_cache"] is None
    await s.close()


# ---------------------------------------------------------------------------
# §Planner Rules — projection states
# ---------------------------------------------------------------------------


async def test_02_main_placed_full_on_main_busy_on_all_clients():
    """[2] Main-placed WebCal keeps current behavior."""
    s, sub_id, ids = await _setup_with_placement(placement_kind="main")
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    states = await _projection_states(s, ids["user_id"])
    assert states[("main", None)] == "present_full"
    assert states[("client", ids["client_a_id"])] == "present_busy"
    assert states[("client", ids["client_b_id"])] == "present_busy"
    await s.close()


async def test_03_client_placed_full_on_main_and_selected():
    """[3] Client-placed WebCal creates full details on main and the
    selected client."""
    s, sub_id, ids = await _setup_with_placement(
        placement_target_nick="client_a",
    )
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:MLPerf review\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    states = await _projection_states(s, ids["user_id"])
    assert states[("main", None)] == "present_full"
    assert states[("client", ids["client_a_id"])] == "present_full"
    # [4] Other clients still receive Busy blocks.
    assert states[("client", ids["client_b_id"])] == "present_busy"
    await s.close()


async def test_05_free_event_client_placed_no_busy_elsewhere():
    """[5] Free/transparent client-placed WebCal: full on main and
    selected client, no Busy blocks elsewhere."""
    s, sub_id, ids = await _setup_with_placement(
        placement_target_nick="client_a",
    )
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:Optional\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
        "TRANSP:TRANSPARENT\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    states = await _projection_states(s, ids["user_id"])
    assert states[("main", None)] == "present_full"
    assert states[("client", ids["client_a_id"])] == "present_full"
    # Other clients: absent (no busy block for free events).
    assert states[("client", ids["client_b_id"])] == "absent"
    await s.close()


# ---------------------------------------------------------------------------
# §Payload Rules — managed markers and visibility
# ---------------------------------------------------------------------------


async def test_06_07_20_selected_client_copy_managed_not_private_color_label():
    """[6][7][20] Selected-client full copy: carries [BusyBridge] tag,
    is not forced private, uses placement client's color, footer shows
    Source: + Placement:."""
    s, sub_id, ids = await _setup_with_placement(
        placement_target_nick="client_a",
    )
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:MLPerf review\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))

    # Find the full copy on client_a (the placement target).
    db = await s.setup_db()
    row = await (await db.execute(
        """SELECT p.desired_state
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.source_type = 'webcal'
              AND p.target_kind = 'client'
              AND p.target_calendar_id = ?""",
        (ids["client_a_id"],),
    )).fetchone()
    assert row["desired_state"] == "present_full"

    # Render the same projection to inspect its payload.
    from app.ledger.payload import PRESENT_FULL, render_payload, managed_tag
    from app.ledger.planner import _get_ledger_row, _row_to_dict
    ledger = await (await db.execute(
        "SELECT id FROM ledger_events WHERE source_type='webcal'"
    )).fetchone()
    lrow = await _get_ledger_row(db, int(ledger["id"]))
    body_payload = render_payload(
        desired_state=PRESENT_FULL,
        ledger_row=_row_to_dict(lrow),
        target_kind="client",
        projection_id=1,
        ledger_version=int(lrow["version"]),
    )
    # Managed description tag present (full copies, all targets).
    tag = managed_tag()
    assert tag and tag in (body_payload.get("description") or "")
    # NOT forced private (only busy blocks are).
    assert body_payload.get("visibility") != "private"
    # Colored by placement client (client_a → '7').
    assert body_payload.get("colorId") == "7"
    # Footer: Source: feed name + Placement: target name.
    desc = body_payload["description"]
    assert "Source: ISO Events" in desc
    assert "Placement: client_a" in desc

    # And the rendered main copy: same colorId (placement client),
    # but no Placement line is omitted there per spec — actually the
    # spec keeps it on main too (a placed feed *is* placed).
    main_payload = render_payload(
        desired_state=PRESENT_FULL,
        ledger_row=_row_to_dict(lrow),
        target_kind="main",
        projection_id=2,
        ledger_version=int(lrow["version"]),
    )
    assert main_payload.get("colorId") == "7"
    assert "Source: ISO Events" in main_payload["description"]
    assert "Placement: client_a" in main_payload["description"]
    await s.close()


async def test_30_main_placed_footer_omits_placement_line_and_color():
    """[30] Main-placed subscriptions omit the Placement footer line
    entirely and the main copy is uncolored."""
    s, sub_id, ids = await _setup_with_placement(placement_kind="main")
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))

    from app.ledger.payload import PRESENT_FULL, render_payload
    from app.ledger.planner import _get_ledger_row, _row_to_dict
    db = await s.setup_db()
    ledger = await (await db.execute(
        "SELECT id FROM ledger_events WHERE source_type='webcal'"
    )).fetchone()
    lrow = await _get_ledger_row(db, int(ledger["id"]))
    payload = render_payload(
        desired_state=PRESENT_FULL,
        ledger_row=_row_to_dict(lrow),
        target_kind="main",
        projection_id=1,
        ledger_version=int(lrow["version"]),
    )
    # No colorId on a main-placed feed.
    assert "colorId" not in payload
    desc = payload["description"]
    assert "Source: ISO Events" in desc
    assert "Placement:" not in desc, (
        "main-placed feeds must not render a Placement line"
    )
    await s.close()


# ---------------------------------------------------------------------------
# §Identity Rules — canonical_uid stability
# ---------------------------------------------------------------------------


async def test_13_canonical_uid_unchanged_after_placement_change():
    """[13] canonical_uid is unchanged after a placement change."""
    s, sub_id, ids = await _setup_with_placement(placement_kind="main")
    body = _ics(
        "UID:stable@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    db = await s.setup_db()
    before = await (await db.execute(
        "SELECT canonical_uid FROM ledger_events WHERE source_type='webcal'"
    )).fetchone()
    # Flip placement to client_a.
    await db.execute(
        """UPDATE webcal_subscriptions
              SET placement_kind = 'client',
                  placement_client_calendar_id = ?
            WHERE id = ?""",
        (ids["client_a_id"], sub_id),
    )
    await db.commit()
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    after = await (await db.execute(
        "SELECT canonical_uid FROM ledger_events WHERE source_type='webcal'"
    )).fetchone()
    assert after["canonical_uid"] == before["canonical_uid"]
    # And the canonical_uid still uses the webcal stable form.
    assert after["canonical_uid"].startswith("webcal:")
    await s.close()


async def test_14_unstable_uid_no_duplicate_after_placement():
    """[14] Unstable-UID feed does not duplicate events after placement
    is added (the unstable hash deliberately excludes the subscription
    metadata, so placement changes don't change the canonical_uid)."""
    s, sub_id, ids = await _setup_with_placement(placement_kind="main")
    # Use a UUIDv4-style UID — the ingest classifier treats this as
    # unstable and falls back to the start/end hash.
    body = _ics(
        "UID:8f14e45f-ceea-467a-9575-d9111111aaaa\n"
        "SUMMARY:Lunch\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    db = await s.setup_db()
    # Add placement.
    await db.execute(
        """UPDATE webcal_subscriptions
              SET placement_kind = 'client',
                  placement_client_calendar_id = ?
            WHERE id = ?""",
        (ids["client_a_id"], sub_id),
    )
    await db.commit()
    # Re-ingest with the same feed body (different etag).
    await s.run_reconciler(
        "alice", webcal_fetch=_fetcher(body, etag='"v2"'),
    )
    count = await (await db.execute(
        """SELECT COUNT(*) AS n FROM ledger_events
            WHERE source_type='webcal' AND status='active'"""
    )).fetchone()
    assert count["n"] == 1, "unstable-UID event must not duplicate"
    await s.close()


# ---------------------------------------------------------------------------
# §Identity Rules — id-space collision
# ---------------------------------------------------------------------------


async def test_16_subscription_id_collision_with_client_id():
    """[16] webcal_subscriptions.id == client_calendars.id for the same
    user: projections are not misrouted, _resolve_targets does not
    fire origin-exclusion for source_type='webcal' without an active
    placement_client_calendar_id."""
    s = Scenario()
    s.given_calendar("main")
    s.given_calendar("client_a")
    user = await s.given_user("alice", main="main", clients=["client_a"])
    db = await s.setup_db()
    client_a_id = user.client_calendar_ids["client_a"]
    # Force the subscription's id to equal client_a's id.  The
    # framework's reconcile loop iterates user.webcal_subscription_ids,
    # so we must register the id there too — a bare INSERT is invisible
    # to the harness.
    await db.execute(
        """INSERT INTO webcal_subscriptions
              (id, user_id, url, display_prefix, is_active)
           VALUES (?, ?, ?, '', 1)""",
        (client_a_id, user.user_id, "https://e.test/collision.ics"),
    )
    await db.commit()
    user.webcal_subscription_ids["collision_feed"] = client_a_id
    body = _ics(
        "UID:c1@x\n"
        "SUMMARY:Collision\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    # client_a (id=N) must still get a busy block from the webcal
    # subscription (id=N), NOT be treated as the origin.
    row = await (await db.execute(
        """SELECT p.desired_state
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.source_type = 'webcal'
              AND p.target_kind = 'client'
              AND p.target_calendar_id = ?""",
        (client_a_id,),
    )).fetchone()
    assert row is not None, "collision excluded the client from targets"
    assert row["desired_state"] == "present_busy", (
        f"expected present_busy under id collision, got {row['desired_state']!r}"
    )
    await s.close()


# ---------------------------------------------------------------------------
# §Pause vs Delete
# ---------------------------------------------------------------------------


async def test_28_pause_does_not_cancel_projections():
    """[28] Pausing a subscription (is_active=false) does not cancel
    projections."""
    s, sub_id, ids = await _setup_with_placement(placement_kind="main")
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    db = await s.setup_db()
    # Pause the subscription.
    await db.execute(
        "UPDATE webcal_subscriptions SET is_active = 0 WHERE id = ?",
        (sub_id,),
    )
    await db.commit()
    # Run reconciler again — ingest should be skipped, projections
    # should remain active.
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    states = await _projection_states(s, ids["user_id"])
    assert states[("main", None)] == "present_full"
    # Ledger row still active.
    row = await (await db.execute(
        "SELECT status FROM ledger_events WHERE source_type='webcal'"
    )).fetchone()
    assert row["status"] == "active", "pause must not cancel ledger rows"
    await s.close()


async def test_29_deletion_cancels_placed_selected_client_copy():
    """[29] Subscription deletion cancels every projection including
    the placed selected-client full copy."""
    s, sub_id, ids = await _setup_with_placement(
        placement_target_nick="client_a",
    )
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    db = await s.setup_db()
    # Simulate the API's delete flow: cancel rows, mark sub inactive,
    # enqueue replan.
    await db.execute(
        """UPDATE ledger_events
              SET status='cancelled', version=version+1
            WHERE source_type='webcal' AND source_calendar_id=?""",
        (sub_id,),
    )
    affected = await (await db.execute(
        "SELECT id FROM ledger_events WHERE source_type='webcal'"
    )).fetchall()
    from app.ledger.admin_ops import _append_affected
    await _append_affected(
        db, user_id=ids["user_id"],
        ledger_ids=[int(r["id"]) for r in affected],
    )
    await db.commit()
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    states = await _projection_states(s, ids["user_id"])
    # Everywhere → absent.
    assert states[("main", None)] == "absent"
    assert states[("client", ids["client_a_id"])] == "absent"
    assert states[("client", ids["client_b_id"])] == "absent"
    await s.close()


# ---------------------------------------------------------------------------
# §Placement Target Lifecycle — stale placement falls back to main
# ---------------------------------------------------------------------------


async def test_23_stale_placement_falls_back_to_main_render():
    """[23] When the placement target is inactive: planner falls back
    to main-only full copy with busy blocks on every client; footer
    drops the Placement line; main copy becomes uncolored."""
    s, sub_id, ids = await _setup_with_placement(
        placement_target_nick="client_a",
    )
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    db = await s.setup_db()
    # Deactivate the placement target (simulating a disconnect).
    await db.execute(
        "UPDATE client_calendars SET is_active = 0 WHERE id = ?",
        (ids["client_a_id"],),
    )
    # Enqueue a replan for the affected webcal rows.
    affected = await (await db.execute(
        "SELECT id FROM ledger_events WHERE source_type='webcal'"
    )).fetchall()
    from app.ledger.admin_ops import _append_affected
    await _append_affected(
        db, user_id=ids["user_id"],
        ledger_ids=[int(r["id"]) for r in affected],
    )
    await db.commit()
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))

    # Inspect the rendered main payload — should be uncolored, no
    # Placement footer line.
    from app.ledger.payload import PRESENT_FULL, render_payload
    from app.ledger.planner import _get_ledger_row, _row_to_dict
    ledger = await (await db.execute(
        "SELECT id FROM ledger_events WHERE source_type='webcal'"
    )).fetchone()
    lrow = await _get_ledger_row(db, int(ledger["id"]))
    main_payload = render_payload(
        desired_state=PRESENT_FULL,
        ledger_row=_row_to_dict(lrow),
        target_kind="main",
        projection_id=1,
        ledger_version=int(lrow["version"]),
    )
    assert "colorId" not in main_payload, (
        "stale placement must not color the main copy"
    )
    assert "Placement:" not in (main_payload.get("description") or ""), (
        "stale placement must not render a Placement footer"
    )
    # And placement_kind in the DB is unchanged (planner does not
    # auto-flip to main).
    row = await (await db.execute(
        "SELECT placement_kind FROM webcal_subscriptions WHERE id = ?",
        (sub_id,),
    )).fetchone()
    assert row["placement_kind"] == "client"
    # Projection states: main full + ALL clients busy (the spec's
    # stale-placement fallback — including the formerly-selected
    # client, which is now inactive so it gets no projection at
    # all, and the still-active peer client which goes to busy).
    states = await _projection_states(s, ids["user_id"])
    assert states[("main", None)] == "present_full"
    # client_a is now inactive — its projection should be absent
    # (no full copy, no busy block on a disconnected calendar).
    assert states.get(("client", ids["client_a_id"])) == "absent"
    # client_b is still active — gets a busy block.
    assert states[("client", ids["client_b_id"])] == "present_busy"
    await s.close()


async def test_25_hard_delete_placement_target_set_null():
    """[25] Hard delete of placement client (FK ON DELETE SET NULL):
    placement_client_calendar_id becomes NULL, placement_kind stays
    'client', and downstream reconcile produces the stale-placement
    fallback (main full, remaining clients busy)."""
    s, sub_id, ids = await _setup_with_placement(
        placement_target_nick="client_a",
    )
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    db = await s.setup_db()
    # Hard delete the target row.  (In production this only happens
    # via factory-reset.)  Capture the affected ledger ids first so
    # we can manually enqueue them — the spec's "no alert needed for
    # factory-reset" path skips the disconnect trigger, but the
    # planner's JOIN still must behave correctly on next reconcile.
    affected = await (await db.execute(
        "SELECT id FROM ledger_events WHERE source_type='webcal'"
    )).fetchall()
    await db.execute(
        "DELETE FROM client_calendars WHERE id = ?",
        (ids["client_a_id"],),
    )
    await db.commit()
    row = await (await db.execute(
        """SELECT placement_kind, placement_client_calendar_id
             FROM webcal_subscriptions WHERE id = ?""",
        (sub_id,),
    )).fetchone()
    assert row["placement_kind"] == "client"
    assert row["placement_client_calendar_id"] is None, (
        "FK ON DELETE SET NULL must clear the target id on hard delete"
    )
    # Run a reconcile and verify the planner's stale-placement
    # fallback fires: main keeps the full copy, client_b stays busy.
    from app.ledger.admin_ops import _append_affected
    await _append_affected(
        db, user_id=ids["user_id"],
        ledger_ids=[int(r["id"]) for r in affected],
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    states = await _projection_states(s, ids["user_id"])
    assert states[("main", None)] == "present_full"
    assert states[("client", ids["client_b_id"])] == "present_busy"
    # client_a row is gone, so it should no longer appear as an
    # active target.
    assert ("client", ids["client_a_id"]) not in {
        (k, c) for (k, c), state in states.items() if state != "absent"
    }, "deleted client must not have an active projection"
    await s.close()


# ---------------------------------------------------------------------------
# §Triggers — recolor of the placement client re-renders placed feeds
# ---------------------------------------------------------------------------


async def test_26_recoloring_placement_client_replans_webcal():
    """[26] Recoloring the placement client calendar re-renders all
    placed WebCal projections — both that affected_ledger_events
    is populated AND that the next reconcile actually renders the
    main + selected-client copies with the new color."""
    s, sub_id, ids = await _setup_with_placement(
        placement_target_nick="client_a",
    )
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    db = await s.setup_db()
    # Recolor client_a via the admin function.
    from app.ledger.admin_ops import recolor_client_calendar
    await recolor_client_calendar(
        db, client_calendar_id=ids["client_a_id"], new_color_id="11",
    )
    # affected_ledger_events should now contain rows for the webcal
    # subscription placed on client_a.
    rows = await (await db.execute(
        """SELECT ledger_event_id FROM affected_ledger_events"""
    )).fetchall()
    affected_ids = {int(r["ledger_event_id"]) for r in rows}
    webcal_ids = {
        int(r["id"]) for r in await (await db.execute(
            "SELECT id FROM ledger_events WHERE source_type='webcal'"
        )).fetchall()
    }
    assert webcal_ids.issubset(affected_ids), (
        f"recolor must enqueue placed-webcal rows for replan; "
        f"affected={affected_ids}, webcal={webcal_ids}"
    )
    # Run the reconcile so the planner actually re-renders.  Verify
    # the rendered payloads now carry colorId='11' (the new placement
    # client color).  This catches a "recolor enqueues but planner
    # JOIN doesn't pick up the new color" regression.
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    from app.ledger.payload import PRESENT_FULL, render_payload
    from app.ledger.planner import _get_ledger_row, _row_to_dict
    ledger_id = next(iter(webcal_ids))
    lrow = await _get_ledger_row(db, ledger_id)
    main_payload = render_payload(
        desired_state=PRESENT_FULL, ledger_row=_row_to_dict(lrow),
        target_kind="main", projection_id=1,
        ledger_version=int(lrow["version"]),
    )
    assert main_payload.get("colorId") == "11", (
        f"recolor did not propagate to the main copy; got colorId="
        f"{main_payload.get('colorId')!r}"
    )
    client_payload = render_payload(
        desired_state=PRESENT_FULL, ledger_row=_row_to_dict(lrow),
        target_kind="client", projection_id=2,
        ledger_version=int(lrow["version"]),
    )
    assert client_payload.get("colorId") == "11", (
        f"recolor did not propagate to the placement-client copy; got "
        f"colorId={client_payload.get('colorId')!r}"
    )
    await s.close()


# ---------------------------------------------------------------------------
# §Placement Changes — no duplicate ledger rows on transition
# ---------------------------------------------------------------------------


async def test_11_main_to_client_no_duplicate_ledger_rows():
    """[11] Changing Main → Client X updates projections in place
    without creating duplicate ledger rows."""
    s, sub_id, ids = await _setup_with_placement(placement_kind="main")
    body = _ics(
        "UID:stable@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    db = await s.setup_db()
    before = await (await db.execute(
        "SELECT id, canonical_uid FROM ledger_events WHERE source_type='webcal'"
    )).fetchall()
    assert len(before) == 1

    # Transition to client_a.
    await db.execute(
        """UPDATE webcal_subscriptions
              SET placement_kind='client',
                  placement_client_calendar_id=?
            WHERE id=?""",
        (ids["client_a_id"], sub_id),
    )
    from app.ledger.admin_ops import _append_affected
    await _append_affected(
        db, user_id=ids["user_id"],
        ledger_ids=[int(r["id"]) for r in before],
    )
    await db.commit()
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))

    after = await (await db.execute(
        "SELECT id, canonical_uid FROM ledger_events WHERE source_type='webcal'"
    )).fetchall()
    assert len(after) == 1, "transition created a duplicate ledger row"
    assert after[0]["id"] == before[0]["id"]
    states = await _projection_states(s, ids["user_id"])
    # Now client_a holds the full copy.
    assert states[("client", ids["client_a_id"])] == "present_full"
    assert states[("client", ids["client_b_id"])] == "present_busy"
    await s.close()


async def test_display_prefix_change_propagates_to_source_footer():
    """Renaming display_prefix must re-render every existing copy's
    Source: footer.  Regression for the bug where display_prefix
    wasn't in the ICS content_hash, so re-fetching the feed left old
    rows untouched and the new prefix never reached the footer."""
    s, sub_id, ids = await _setup_with_placement(placement_kind="main")
    body = _ics(
        "UID:e1@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))

    # Verify the initial footer says "ISO Events" (the prefix we set).
    from app.ledger.payload import PRESENT_FULL, render_payload
    from app.ledger.planner import _get_ledger_row, _row_to_dict
    db = await s.setup_db()
    ledger = await (await db.execute(
        "SELECT id FROM ledger_events WHERE source_type='webcal'"
    )).fetchone()
    lrow = await _get_ledger_row(db, int(ledger["id"]))
    payload = render_payload(
        desired_state=PRESENT_FULL, ledger_row=_row_to_dict(lrow),
        target_kind="main", projection_id=1,
        ledger_version=int(lrow["version"]),
    )
    assert "Source: ISO Events" in (payload.get("description") or "")

    # Rename the display_prefix and enqueue affected rows (the same
    # pattern the PATCH handler uses).
    await db.execute(
        "UPDATE webcal_subscriptions SET display_prefix = ? WHERE id = ?",
        ("Renamed Feed", sub_id),
    )
    from app.ledger.triggers import record_affected_events
    await record_affected_events(
        db, user_id=ids["user_id"], ledger_event_ids=[int(ledger["id"])],
    )
    await db.commit()
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))

    # Re-render and verify the footer reflects the new prefix.
    lrow = await _get_ledger_row(db, int(ledger["id"]))
    payload = render_payload(
        desired_state=PRESENT_FULL, ledger_row=_row_to_dict(lrow),
        target_kind="main", projection_id=1,
        ledger_version=int(lrow["version"]),
    )
    desc = payload.get("description") or ""
    assert "Source: Renamed Feed" in desc, (
        f"display_prefix change did not propagate to footer; desc={desc!r}"
    )
    assert "Source: ISO Events" not in desc, (
        "stale Source: line was not replaced"
    )
    await s.close()


async def test_12_client_to_client_no_duplicate_ledger_rows():
    """[12] Changing Client A → Client B updates projections without
    duplicate ledger rows; B gets full, A reverts to busy."""
    s, sub_id, ids = await _setup_with_placement(
        placement_target_nick="client_a",
    )
    body = _ics(
        "UID:stable@x\n"
        "SUMMARY:Trip\n"
        "DTSTART:20260301T120000Z\n"
        "DTEND:20260301T130000Z\n"
    )
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    db = await s.setup_db()
    before = await (await db.execute(
        "SELECT id FROM ledger_events WHERE source_type='webcal'"
    )).fetchall()
    # Pivot to client_b.
    await db.execute(
        """UPDATE webcal_subscriptions
              SET placement_client_calendar_id=?
            WHERE id=?""",
        (ids["client_b_id"], sub_id),
    )
    from app.ledger.admin_ops import _append_affected
    await _append_affected(
        db, user_id=ids["user_id"],
        ledger_ids=[int(r["id"]) for r in before],
    )
    await db.commit()
    await s.run_reconciler("alice", webcal_fetch=_fetcher(body))
    after = await (await db.execute(
        "SELECT COUNT(*) AS n FROM ledger_events WHERE source_type='webcal'"
    )).fetchone()
    assert after["n"] == 1
    states = await _projection_states(s, ids["user_id"])
    assert states[("client", ids["client_b_id"])] == "present_full"
    assert states[("client", ids["client_a_id"])] == "present_busy"
    await s.close()
