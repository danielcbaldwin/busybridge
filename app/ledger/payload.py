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
* ``present_full_rsvp_only``— events.patch back to the calendar
  that sourced the event, carrying the user's edits (RSVP, time,
  and — for client sources — detail).  Used by the edit-on-main
  → propagate path; never creates or deletes the source.
* ``absent``               — no payload; outbox emits a delete.

The lock emoji "🔒 " is prepended to the summary when the user
cannot edit the source event, per REWRITE_PLAN.md §9.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Optional

from app.config import get_settings

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
    main_calendar_email: Optional[str] = None,
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

    if desired_state == PRESENT_FULL_RSVP_ONLY:
        # events.patch onto the user's real source event — a calendar
        # entry we do NOT own.  Returned unstamped: branding the
        # source with our extendedProperties would make the orphan
        # scan mis-claim it as one of our managed writes and relink
        # the writeback projection to it.
        return _render_origin_writeback(ledger_row)

    if desired_state == PRESENT_FULL:
        body = _render_full_copy(ledger_row, target_kind, main_calendar_email)
    elif desired_state == PRESENT_BUSY:
        body = _render_busy_block(ledger_row)
    elif desired_state == PRESENT_PERSONAL_BUSY:
        body = _render_personal_busy(ledger_row)
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
def _render_full_copy(
    row: dict,
    target_kind: Optional[str] = None,
    main_calendar_email: Optional[str] = None,
) -> dict:
    """Full-detail copy of a client / webcal event onto main."""
    body: dict[str, Any] = {}
    summary = row.get("summary") or "(no title)"
    if not row.get("user_can_edit"):
        summary = LOCK_PREFIX + summary
    body["summary"] = summary
    desc = _tag_description(row.get("description"))
    if desc:
        body["description"] = desc
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
    #
    # The attendee MUST carry an explicit email.  ``events.insert``
    # rejects a bare ``{"self": True}`` with "400 Missing attendee
    # email. [required]" — ``self`` only resolves on a patch of an
    # event Google already ties to the caller, not on a fresh create.
    # The main copy's owner is the main calendar itself, so its
    # address is the attendee email.
    if target_kind == "main" and row.get("user_rsvp_status"):
        attendee: dict[str, Any] = {
            "self": True,
            "responseStatus": row["user_rsvp_status"],
        }
        if main_calendar_email:
            attendee["email"] = main_calendar_email
        body["attendees"] = [attendee]
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
    desc = _tag_description(None)
    if desc:
        body["description"] = desc
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
    desc = _tag_description(None)
    if desc:
        body["description"] = desc
    if row.get("recurrence_rule_json"):
        body["recurrence"] = json.loads(row["recurrence_rule_json"])
    return body


def _render_origin_writeback(row: dict) -> dict:
    """Body for an ``events.patch`` that writes the user's edits back
    to the calendar that sourced the event.

    ``events.patch`` is field-scoped — only the keys present here are
    changed on the source event; every other field is left intact.

    Always carries the canonical ``start``/``end``.  It carries the
    ``attendees`` array only when the user has an RSVP to write (a
    solo source event has no attendees, and sending the array would
    spuriously add the user as one).  For a client-sourced event it
    also carries the editable detail fields, sent as the ledger's
    own values — a JSON ``null`` (an absent ledger field) clears the
    field on the source, which both no-ops a field that was never
    set and propagates a genuine clear.  A personal source omits
    detail: its main copy is an opaque "Busy (personal)"
    placeholder, so an edit there is a placeholder edit, never a
    real-event edit.
    """
    body: dict[str, Any] = {
        "start": _start_dict(row),
        "end": _end_dict(row),
    }
    if row.get("user_rsvp_status"):
        body["attendees"] = _rsvp_attendees(row)
    if row.get("source_type") == "client":
        body["summary"] = row.get("summary")
        body["description"] = row.get("description")
        body["location"] = row.get("location")
    return body


def _rsvp_attendees(row: dict) -> list[dict]:
    """The source event's attendee list with the user's own
    ``responseStatus`` set to the stored RSVP.

    The COMPLETE list is returned: ``events.patch`` replaces the
    ``attendees`` array wholesale, so sending only the user's entry
    would drop every other guest.  Only the user's own entry
    (``self=True``) is changed.
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
    return out


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def managed_tag() -> str:
    """Visible marker appended to the DESCRIPTION of every event
    BusyBridge writes, so a user can find — and worst-case bulk
    delete — all of them by searching their calendar for the tag.

    It lives in the description, not the title, so events still read
    normally at a glance while remaining searchable.  Configurable via
    ``MANAGED_EVENT_PREFIX``; an empty value disables tagging.
    """
    return (get_settings().managed_event_prefix or "").strip()


def _tag_description(description: Optional[str]) -> Optional[str]:
    """Append the managed tag on its own trailing line.  Returns the
    description unchanged when tagging is disabled."""
    tag = managed_tag()
    if not tag:
        return description
    base = (description or "").rstrip()
    return f"{base}\n\n{tag}" if base else tag


def strip_managed_tag(description: Optional[str]) -> Optional[str]:
    """Inverse of :func:`_tag_description`: remove a trailing managed
    tag.  Applied when reading one of our OWN copies back (edit-on-main
    detection, managed-instance re-ingest) so the tag never leaks into
    the ledger or onto the user's real source event.  ``None`` when
    nothing but the tag remains."""
    tag = managed_tag()
    if not tag or not description:
        return description
    stripped = description.rstrip()
    if stripped == tag:
        return None
    if stripped.endswith(tag):
        return stripped[: -len(tag)].rstrip() or None
    return description


def _start_dict(row: dict) -> dict:
    if row.get("is_all_day"):
        return {"date": row["start_at"]}
    # Carry the source's IANA timezone so a recurring mirror expands
    # its RRULE on the source's wall-clock grid (correct across DST)
    # instead of a fixed UTC grid.  Falls back to UTC only when the
    # source supplied no zone — see REWRITE_PLAN.md timezone notes.
    return {
        "dateTime": row["start_at"],
        "timeZone": _row_get(row, "start_timezone") or "UTC",
    }


def _end_dict(row: dict) -> dict:
    if row.get("is_all_day"):
        return {"date": row["end_at"]}
    return {
        "dateTime": row["end_at"],
        "timeZone": _row_get(row, "end_timezone") or "UTC",
    }


def _row_get(row: dict, key: str) -> Any:
    """``row.get`` that also tolerates an ``aiosqlite.Row`` (no
    ``.get``) and a row missing the column entirely."""
    try:
        return row[key]
    except (KeyError, IndexError):
        return None


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
