"""Ghost Main: the ``main_virtual`` flag suppresses every write to a
real Google main calendar.

BusyBridge's stock architecture treats one Google calendar as the
"main" — source of truth and aggregation view.  For a peer-to-peer
topology (multiple client calendars with no privileged aggregator, e.g.
Fantastical takes the aggregation role) the main calendar is a wasted
Google account.  ``settings.main_virtual`` keeps the internal ledger
intact but skips every projection, diff, and webhook targeted at main.

The chokepoint is the planner: once ``_resolve_targets`` stops emitting
``TARGET_MAIN`` triples, the entire downstream pipeline naturally does
nothing main-related.  ``diff._decide`` carries a belt-and-suspenders
early return for any main projection that leaks through (e.g. legacy
rows from before the flag was flipped on).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from app.config import Settings
from app.ledger.diff import _decide
from app.ledger.planner import TARGET_MAIN, TARGET_CLIENT, _resolve_targets


def _client_ledger():
    """Minimal ledger row for a client-sourced event.

    Only the fields ``_resolve_targets`` reads are populated; the rest
    of the pipeline is not exercised in these unit tests.
    """
    return {
        "source_type": "client",
        "source_calendar_id": 10,
        "placement_client_resolved_id": None,
    }


def _active_client(cal_id: int):
    return {"id": cal_id, "google_calendar_id": f"cal-{cal_id}@example.com"}


def _desired_full():
    """A client-source ledger row's normal desired projection map."""
    return {"main": "present_full", "peer_clients": "present_busy", "origin_client": "absent"}


# ---------------------------------------------------------------------------
# Planner: main projection is emitted / suppressed by the flag
# ---------------------------------------------------------------------------
def test_resolve_targets_emits_main_by_default(monkeypatch):
    """Regression: with ``main_virtual=False`` (the default), the
    planner still emits the main target so stock BusyBridge behaviour
    is unchanged."""
    cfg = Settings(main_virtual=False)
    monkeypatch.setattr("app.ledger.planner.get_settings", lambda: cfg)

    triples = _resolve_targets(
        _client_ledger(),
        _desired_full(),
        [_active_client(10), _active_client(20)],
    )

    kinds = [k for k, _, _ in triples]
    assert TARGET_MAIN in kinds
    # One main + one entry per active client.
    assert kinds.count(TARGET_MAIN) == 1
    assert kinds.count(TARGET_CLIENT) == 2


def test_resolve_targets_suppresses_main_when_virtual(monkeypatch):
    """With ``main_virtual=True``, the planner emits zero main-target
    triples — the ledger is the only place main-related state lives."""
    cfg = Settings(main_virtual=True)
    monkeypatch.setattr("app.ledger.planner.get_settings", lambda: cfg)

    triples = _resolve_targets(
        _client_ledger(),
        _desired_full(),
        [_active_client(10), _active_client(20)],
    )

    kinds = [k for k, _, _ in triples]
    assert TARGET_MAIN not in kinds
    # Client-target triples are unaffected: peer sync still fans out.
    assert kinds.count(TARGET_CLIENT) == 2


def test_resolve_targets_virtual_preserves_origin_exclusion(monkeypatch):
    """The origin client (the one that sourced the event) still gets
    the origin_client state — client-side routing is orthogonal to the
    main-virtual flag."""
    cfg = Settings(main_virtual=True)
    monkeypatch.setattr("app.ledger.planner.get_settings", lambda: cfg)

    ledger = _client_ledger()  # source_calendar_id = 10
    triples = _resolve_targets(
        ledger,
        _desired_full(),
        [_active_client(10), _active_client(20)],
    )

    # The origin client (id 10) gets origin_client=absent; the peer
    # client (id 20) gets peer_clients=present_busy.
    by_cal = {cal_id: state for kind, cal_id, state in triples if kind == TARGET_CLIENT}
    assert by_cal[10] == "absent"
    assert by_cal[20] == "present_busy"


# ---------------------------------------------------------------------------
# Diff: belt-and-suspenders early return for any legacy main projection
# ---------------------------------------------------------------------------
def _main_projection(state: str = "present_full"):
    """A projection row targeted at main. Only the fields ``_decide``
    reads on the main-target early-return path need to be populated."""
    proj = MagicMock()
    # __getitem__ / dict-style access is used in _decide.
    def _get(key):
        return {
            "desired_state": state,
            "current_state": "absent",
            "target_kind": "main",
            "target_calendar_id": None,
            "google_event_id": None,
            "id": 1,
            "source_type": "client",
            "source_calendar_id": 10,
        }[key]
    proj.__getitem__.side_effect = _get
    return proj


def test_diff_decide_skips_main_when_virtual(monkeypatch):
    """A legacy main projection (e.g. left over from before the flag
    was flipped on) must never turn into a Google write."""
    cfg = Settings(main_virtual=True)
    monkeypatch.setattr("app.ledger.diff.get_settings", lambda: cfg)

    op, payload, target_cal = _decide(
        proj=_main_projection(state="present_full"),
        main_calendar_id="should-never-be-used",
        google_calendar_id_for={},
    )

    assert op is None
    assert payload is None
    assert target_cal == ""


# The non-virtual code path in _decide is heavily covered by existing
# tests (test_diff_decide.py and friends); adding a bespoke regression
# here would require fabricating a fully-populated projection row for
# payload rendering, which duplicates coverage without adding value.
# The four tests above are enough to prove the main_virtual flag both
# fires (planner+diff) and does not disturb non-main routing.
