"""Busy-block titles are configurable via BUSY_BLOCK_TITLE /
PERSONAL_BUSY_BLOCK_TITLE, and default to the historical rendered values
so existing blocks stay byte-stable (no churn) when nothing is set.
"""

from __future__ import annotations

from types import SimpleNamespace

from app.ledger.payload import (
    BUSY_SUMMARY,
    PERSONAL_BUSY_SUMMARY,
    PRESENT_BUSY,
    PRESENT_PERSONAL_BUSY,
    render_payload,
)

_ROW = {
    "start_at": "2026-02-02T09:00:00Z",
    "end_at": "2026-02-02T09:30:00Z",
    "is_all_day": 0,
    "start_timezone": "UTC",
    "end_timezone": "UTC",
    "recurrence_rule_json": None,
}


def _summary(desired: str) -> str:
    body = render_payload(
        desired_state=desired,
        ledger_row=dict(_ROW),
        projection_id=1,
        ledger_version=1,
        target_kind="client",
    )
    return body["summary"]


def test_default_titles_match_historical_literals():
    # Real settings (defaults) must reproduce the old hardcoded strings,
    # so deploying the wired-up settings changes no rendered block.
    assert _summary(PRESENT_BUSY) == BUSY_SUMMARY == "Busy"
    assert _summary(PRESENT_PERSONAL_BUSY) == PERSONAL_BUSY_SUMMARY == "Busy (personal)"


def test_titles_are_configurable(monkeypatch):
    monkeypatch.setattr(
        "app.ledger.payload.get_settings",
        lambda: SimpleNamespace(
            managed_event_prefix="[BusyBridge]",
            busy_block_title="Unavailable",
            personal_busy_block_title="Out of office",
        ),
    )
    assert _summary(PRESENT_BUSY) == "Unavailable"
    assert _summary(PRESENT_PERSONAL_BUSY) == "Out of office"
