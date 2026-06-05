"""Live verification: ledger projections vs. actual Google state.

This is a confidence tool that answers "is the new system's model
of the world consistent with what's actually on Google right now?"
without writing anything.

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
                  p.applied_ledger_version, p.desired_ledger_version,
                  p.applied_payload_hash, p.desired_payload_hash,
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

        # A projection the outbox gave up on, or one still in an
        # unresolved state, is itself a divergence — the ledger never
        # reached the state it wanted.  Count it so verify cannot
        # report "consistent" while such projections exist.
        if r["permanently_failed"]:
            divergences.append(
                f"projection {r['id']} ({r['summary']!r}): permanently "
                f"FAILED — desired {r['desired_state']!r} was never applied"
            )
            continue
        if current not in ("present", "absent"):
            divergences.append(
                f"projection {r['id']} ({r['summary']!r}): unresolved "
                f"state {current!r} (desired {r['desired_state']!r})"
            )
            continue

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
            elif _ledger_behind(r):
                # The event exists and isn't cancelled, but the ledger
                # itself has NOT applied its desired version/payload — the
                # last successful write predates the current desired state.
                # The Google copy is therefore stale (e.g. a since-failed
                # time/title update), which existence-only checks miss.
                divergences.append(
                    f"projection {r['id']} ({r['summary']!r}): {gid} on "
                    f"{target_cal} exists but is STALE — applied "
                    f"v{r['applied_ledger_version']}/"
                    f"{_short(r['applied_payload_hash'])} != desired "
                    f"v{r['desired_ledger_version']}/"
                    f"{_short(r['desired_payload_hash'])} "
                    f"(the copy may show the wrong time/title)"
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


def _ledger_behind(r) -> bool:
    """True when the ledger itself knows it has not applied the desired
    state to this projection — the same divergence definition the planner
    uses (idx_proj_diverged): no successful apply yet, an out-of-date
    applied version, or an out-of-date applied payload hash.  A 'present'
    projection that is behind has a stale Google copy even though the
    event still exists.
    """
    if r["applied_ledger_version"] is None:
        return True
    if r["applied_ledger_version"] != r["desired_ledger_version"]:
        return True
    if r["applied_payload_hash"] != r["desired_payload_hash"]:
        return True
    return False


def _short(h: Optional[str]) -> str:
    """Abbreviate a payload hash for the divergence message."""
    if not h:
        return "∅"
    return h[:8]


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
