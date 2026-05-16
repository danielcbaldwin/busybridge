"""Render ledger rows into Google-event payloads, plus content
hashing for drift detection.

The planner decides *which* projection to render (target kind +
desired state); the renderer lives here.  Output shape matches
what the fake Google (and real Google) accept on
``events.insert`` / ``events.update``.

Five render modes:

* ``present_full``         — full-detail copy of a client-source
  event on main, with edit-rights metadata + colorId.
* ``present_busy``         — opaque "Busy" placeholder on a
  client calendar.
* ``present_personal_busy``— opaque "Busy (personal)" placeholder
  on main or any client calendar.
* ``present_full_rsvp_only``— update only the RSVP back to the
  source calendar (used by the edit-on-main → propagate path).
* ``absent``               — no payload; outbox emits a delete.

The lock emoji "🔒 " is prepended to the summary when the user
cannot edit the source event, per REWRITE_PLAN.md §9.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Optional

# State constants — keep in sync with planner.py.
PRESENT_FULL = "present_full"
PRESENT_BUSY = "present_busy"
PRESENT_PERSONAL_BUSY = "present_personal_busy"
PRESENT_FULL_RSVP_ONLY = "present_full_rsvp_only"
ABSENT = "absent"

LOCK_PREFIX = "🔒 "
BUSY_SUMMARY = "Busy"
PERSONAL_BUSY_SUMMARY = "Busy (personal)"

# Extended-property keys we stamp onto every payload as
# defence-in-depth.  Primary identification is via
# ledger_projections.google_event_id, but the props let an
# orphan scan recognise our writes even when DB state is missing.
EP_PROJ_ID = "bb_proj_id"
EP_LEDGER_VERSION = "bb_ledger_version"
EP_TARGET_KIND = "bb_target_kind"


# ---------------------------------------------------------------------------
# Public renderer
# ---------------------------------------------------------------------------
def render_payload(
    *,
    desired_state: str,
    ledger_row: dict,
    projection_id: Optional[int] = None,
    ledger_version: Optional[int] = None,
    target_kind: Optional[str] = None,
) -> Optional[dict]:
    """Render the body to send to Google for one projection.

    Returns ``None`` when ``desired_state == ABSENT`` (the outbox
    will issue a delete) or when no payload is appropriate.

    ``ledger_row`` is the dict-shaped row from ``ledger_events``;
    keys are the column names.

    The optional metadata params (``projection_id``,
    ``ledger_version``, ``target_kind``) are stamped into
    ``extendedProperties.private`` for defence-in-depth.  They are
    not load-bearing — identity is via the deterministic Google ID
    on inserts and via the etag on updates — but they make
    bug-investigation grep-friendly.
    """
    if desired_state == ABSENT:
        return None

    if desired_state == PRESENT_FULL:
        body = _render_full_copy(ledger_row, target_kind)
    elif desired_state == PRESENT_BUSY:
        body = _render_busy_block(ledger_row)
    elif desired_state == PRESENT_PERSONAL_BUSY:
        body = _render_personal_busy(ledger_row)
    elif desired_state == PRESENT_FULL_RSVP_ONLY:
        body = _render_rsvp_only(ledger_row)
    else:
        raise ValueError(f"unknown desired_state: {desired_state!r}")

    _stamp_extended_properties(
        body,
        projection_id=projection_id,
        ledger_version=ledger_version,
        target_kind=target_kind,
    )
    return body


def hash_payload(payload: Optional[dict]) -> str:
    """Stable SHA-256 hash of a payload, used for drift detection.

    ``None`` (the absent state) hashes to a sentinel string so
    "we want it gone" round-trips through equality checks.
    """
    if payload is None:
        return "absent"
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Internal renderers
# ---------------------------------------------------------------------------
def _render_full_copy(row: dict, target_kind: Optional[str] = None) -> dict:
    """Full-detail copy of a client / webcal event onto main."""
    body: dict[str, Any] = {}
    summary = row.get("summary") or "(no title)"
    if not row.get("user_can_edit"):
        summary = LOCK_PREFIX + summary
    body["summary"] = summary
    if row.get("description"):
        body["description"] = row["description"]
    if row.get("location"):
        body["location"] = row["location"]
    body["start"] = _start_dict(row)
    body["end"] = _end_dict(row)
    if row.get("color_id"):
        body["colorId"] = row["color_id"]
    if row.get("show_as") == "free":
        body["transparency"] = "transparent"
    if row.get("recurrence_rule_json"):
        body["recurrence"] = json.loads(row["recurrence_rule_json"])
    # On the main copy, carry the user as an attendee with their
    # stored RSVP (REWRITE_PLAN.md §9) so they can see and change
    # their response there.  An RSVP set on the main copy is
    # detected at main-ingest and written back to the source event.
    if target_kind == "main" and row.get("user_rsvp_status"):
        body["attendees"] = [
            {"self": True, "responseStatus": row["user_rsvp_status"]},
        ]
    return body


def _render_busy_block(row: dict) -> dict:
    """Opaque "Busy" placeholder on a peer client calendar."""
    body: dict[str, Any] = {
        "summary": BUSY_SUMMARY,
        "start": _start_dict(row),
        "end": _end_dict(row),
        "transparency": "opaque",
        "visibility": "private",
    }
    if row.get("recurrence_rule_json"):
        body["recurrence"] = json.loads(row["recurrence_rule_json"])
    return body


def _render_personal_busy(row: dict) -> dict:
    """Opaque "Busy (personal)" placeholder.  No detail leaks across
    the personal/work boundary."""
    body: dict[str, Any] = {
        "summary": PERSONAL_BUSY_SUMMARY,
        "start": _start_dict(row),
        "end": _end_dict(row),
        "transparency": "opaque",
        "visibility": "private",
    }
    if row.get("recurrence_rule_json"):
        body["recurrence"] = json.loads(row["recurrence_rule_json"])
    return body


def _render_rsvp_only(row: dict) -> dict:
    """Body for an ``events.patch`` that writes the user's RSVP back
    to the calendar that sourced the event.

    The COMPLETE attendee list is included: ``events.patch`` replaces
    the ``attendees`` array wholesale, so sending only the user's
    entry would drop every other guest.  Only the user's own entry
    (``self=True``) has its ``responseStatus`` changed; all other
    fields of the source event are untouched (patch is field-scoped).
    """
    rsvp = row.get("user_rsvp_status") or "needsAction"
    raw = row.get("attendees_json")
    try:
        attendees = json.loads(raw) if raw else []
    except (TypeError, ValueError):
        attendees = []
    out: list[dict] = []
    found_self = False
    for att in attendees:
        att = dict(att)
        if att.get("self"):
            att["responseStatus"] = rsvp
            found_self = True
        out.append(att)
    if not found_self:
        out.append({"self": True, "responseStatus": rsvp})
    return {"attendees": out}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _start_dict(row: dict) -> dict:
    if row.get("is_all_day"):
        return {"date": row["start_at"]}
    return {"dateTime": row["start_at"], "timeZone": "UTC"}


def _end_dict(row: dict) -> dict:
    if row.get("is_all_day"):
        return {"date": row["end_at"]}
    return {"dateTime": row["end_at"], "timeZone": "UTC"}


def _stamp_extended_properties(
    body: dict,
    *,
    projection_id: Optional[int],
    ledger_version: Optional[int],
    target_kind: Optional[str],
) -> None:
    if projection_id is None and ledger_version is None and target_kind is None:
        return
    ep = body.setdefault("extendedProperties", {})
    priv = ep.setdefault("private", {})
    if projection_id is not None:
        priv[EP_PROJ_ID] = str(projection_id)
    if ledger_version is not None:
        priv[EP_LEDGER_VERSION] = str(ledger_version)
    if target_kind is not None:
        priv[EP_TARGET_KIND] = target_kind
