"""Live verification: ledger projections vs. actual Google state.

This is the Stage-4 confidence tool (REWRITE_PLAN.md §13).  It
answers "is the new system's model of the world consistent with
what's actually on Google right now?" without writing anything.

Two checks:

* :func:`verify_user` — for every projection the ledger believes
  is ``present``, GET the event from Google and confirm it
  exists and is not cancelled.  For every projection the ledger
  believes is ``absent``, confirm Google agrees (or never had
  it).  Divergences are returned, not fixed.
* :func:`preview_user` — run an ingest + plan + diff pass with
  the outbox left UNDRAINED, then return the pending outbox
  operations: the exact list of writes the system *would* make.
  Pure read; nothing is sent to Google.

Both are safe to run against production / real Google at any
time — they never mutate Google state.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

import aiosqlite

from app.ledger.async_google import as_async_google
from app.ledger.google_client import GoogleClient

logger = logging.getLogger(__name__)


async def verify_user(
    db: aiosqlite.Connection,
    google: GoogleClient,
    *,
    user_id: int,
    main_google_calendar_id: str,
    google_calendar_id_for: dict[int, str],
) -> dict:
    """Compare the ledger's projection state against live Google.

    Returns a dict::

        {
          "checked": <int>,        # projections inspected
          "ok": <int>,             # projections that matched
          "divergences": [str],    # human-readable mismatch list
        }
    """
    google = as_async_google(google)
    rows = await (await db.execute(
        """SELECT p.id, p.target_kind, p.target_calendar_id,
                  p.current_state, p.google_event_id,
                  p.desired_state, p.permanently_failed,
                  e.summary, e.canonical_uid
             FROM ledger_projections p
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE e.user_id = ?""",
        (user_id,),
    )).fetchall()

    checked = 0
    ok = 0
    divergences: list[str] = []

    for r in rows:
        target_cal = _resolve(
            r["target_kind"], r["target_calendar_id"],
            main_google_calendar_id, google_calendar_id_for,
        )
        if target_cal is None:
            divergences.append(
                f"projection {r['id']}: cannot resolve target calendar "
                f"(kind={r['target_kind']}, cal_id={r['target_calendar_id']})"
            )
            continue

        current = r["current_state"]
        gid = r["google_event_id"]

        if current == "present" and gid:
            checked += 1
            try:
                ev = await google.get_event(target_cal, gid)
            except Exception as e:
                if getattr(e, "status", None) in (404, 410):
                    divergences.append(
                        f"projection {r['id']} ({r['summary']!r}): ledger "
                        f"says present, but {gid} is MISSING from {target_cal}"
                    )
                else:
                    divergences.append(
                        f"projection {r['id']}: GET {gid} on {target_cal} "
                        f"errored: {e}"
                    )
                continue
            if ev.get("status") == "cancelled":
                divergences.append(
                    f"projection {r['id']} ({r['summary']!r}): ledger says "
                    f"present, but {gid} on {target_cal} is CANCELLED"
                )
            else:
                ok += 1
        elif current == "absent" and gid:
            checked += 1
            # Ledger believes it deleted this; confirm Google agrees.
            try:
                ev = await google.get_event(target_cal, gid)
                if ev.get("status") != "cancelled":
                    divergences.append(
                        f"projection {r['id']} ({r['summary']!r}): ledger "
                        f"says absent, but {gid} on {target_cal} still EXISTS"
                    )
                else:
                    ok += 1
            except Exception as e:
                if getattr(e, "status", None) in (404, 410):
                    ok += 1  # gone on both sides — consistent
                else:
                    divergences.append(
                        f"projection {r['id']}: GET {gid} errored: {e}"
                    )

    return {
        "checked": checked,
        "ok": ok,
        "divergences": divergences,
        "consistent": not divergences,
    }


async def preview_pending_outbox(
    db: aiosqlite.Connection, *, user_id: int,
) -> list[dict]:
    """Return the pending outbox operations for a user as a
    human-readable preview.  Run this after a dry-run reconcile to
    see exactly what the system would write to Google."""
    rows = await (await db.execute(
        """SELECT o.id, o.operation, o.target_google_calendar_id,
                  o.idempotency_key, o.payload_json, o.status,
                  e.summary, e.canonical_uid, p.target_kind
             FROM outbox_operations o
             JOIN ledger_projections p ON p.id = o.projection_id
             JOIN ledger_events e ON e.id = p.ledger_event_id
            WHERE o.user_id = ? AND o.status = 'pending'
            ORDER BY o.id""",
        (user_id,),
    )).fetchall()
    out: list[dict] = []
    for r in rows:
        payload = json.loads(r["payload_json"]) if r["payload_json"] else None
        out.append({
            "outbox_id": int(r["id"]),
            "operation": r["operation"],
            "target_calendar": r["target_google_calendar_id"],
            "target_kind": r["target_kind"],
            "event_summary": r["summary"],
            "canonical_uid": r["canonical_uid"],
            "would_send_summary": (payload or {}).get("summary"),
        })
    return out


def _resolve(
    target_kind: str,
    target_calendar_id: Optional[int],
    main_google_calendar_id: str,
    google_calendar_id_for: dict[int, str],
) -> Optional[str]:
    if target_kind == "main":
        return main_google_calendar_id
    if target_calendar_id is None:
        return None
    return google_calendar_id_for.get(int(target_calendar_id))
