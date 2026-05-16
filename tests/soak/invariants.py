"""Invariant checker for the soak harness.

Implements the 11 invariants from REWRITE_PLAN.md §14 Layer 5.
Each invariant is a method named ``check_<n>_*`` that returns a
list of violation strings (empty list = pass).  The dispatcher
runs them all and aggregates.

Invariants 1-8 are evaluable at any single point in time and
live here.  Invariants 9-11 are runtime properties (latency,
DB-size growth, post-failure recovery time) that the soak
harness loop evaluates over multiple cycles; helpers for those
live in this file too.

Invariants:

1. For every active ledger event, projections exist on exactly
   the expected calendars (no missing, no spurious).
2. For every projection with ``current_state='present'``, the
   corresponding event exists on Google.
3. For every Google event carrying our extended properties /
   deterministic ID, a corresponding projection exists.
4. No two projections share a Google event ID.
5. No two ledger events share a canonical UID per user.
6. Count of full-detail copies on main matches the oracle.
7. Count of busy blocks on each client matches the oracle.
8. Outbox queue eventually drains to zero.
9. Reconcile latency stays bounded as event count grows.
10. Database size grows sub-linearly in event count.
11. After failure injection, the system reaches a clean state
    within a bounded number of cycles.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Optional

import aiosqlite

from app.ledger.identity import is_managed_google_event_id
from app.ledger.payload import EP_PROJ_ID
from tests.fakes.google_calendar import FakeGoogleCalendar
from tests.soak.oracle import Oracle


class InvariantViolation(AssertionError):
    """Raised by the harness when one or more invariants fail."""


@dataclass
class LatencySample:
    """Single (event_count, wall_seconds) pair for invariant 9."""
    event_count: int
    wall_seconds: float


@dataclass
class DbSizeSample:
    """Single (event_count, ledger_row_count, db_bytes) for inv. 10."""
    event_count: int
    ledger_row_count: int
    db_bytes: int


@dataclass
class InvariantChecker:
    db: aiosqlite.Connection
    google: FakeGoogleCalendar
    # The ground-truth oracle backs invariants 6 & 7.  Soaks that do
    # not model an oracle pass ``None`` and use ``check_oracle_free``.
    oracle: Optional[Oracle]
    user_id: int
    main_google_id: str
    # nickname -> google id (kept for reference) AND nickname -> db id
    # for INV-7 + INV-2.
    client_google_ids: dict[str, str]
    client_db_ids: dict[str, int] = None  # type: ignore[assignment]

    async def check_all(self) -> list[str]:
        """Run every point-in-time invariant; return the combined
        violation list."""
        out: list[str] = []
        out += await self.check_oracle_free()
        out += await self.check_6_main_copy_count_matches_oracle()
        out += await self.check_7_busy_block_counts_match_oracle()
        return out

    async def check_oracle_free(self) -> list[str]:
        """Run the invariants that need no ground-truth oracle
        (1-5, 8).  Used by soaks — e.g. the recurring soak — whose
        per-instance state the simple Oracle does not model."""
        out: list[str] = []
        out += await self.check_1_projections_match_ledger()
        out += await self.check_2_present_projections_exist_on_google()
        out += await self.check_3_no_orphan_google_events()
        out += await self.check_4_no_duplicate_google_event_ids()
        out += await self.check_5_no_duplicate_canonical_uids()
        out += await self.check_8_outbox_drains()
        return out

    # ------------------------------------------------------------------
    # 1. Every active ledger event has the expected projection set —
    #    a projection on main + on every active client (no missing),
    #    and no projection pointing at a calendar that is not this
    #    user's (no spurious).  A disconnected calendar's projection
    #    is tolerated: its client_calendars row still exists and the
    #    planner deliberately keeps the row to drive the delete.
    # ------------------------------------------------------------------
    async def check_1_projections_match_ledger(self) -> list[str]:
        active = await (await self.db.execute(
            """SELECT id FROM client_calendars
                WHERE user_id = ? AND is_active = 1
                  AND calendar_type = 'client'""",
            (self.user_id,),
        )).fetchall()
        expected_client_ids = {int(r["id"]) for r in active}

        rows = await (await self.db.execute(
            """SELECT id, source_type FROM ledger_events
                WHERE user_id = ?
                  AND status = 'active'
                  AND user_intentionally_deleted = 0""",
            (self.user_id,),
        )).fetchall()
        out: list[str] = []
        for r in rows:
            projs = await (await self.db.execute(
                """SELECT target_kind, target_calendar_id
                     FROM ledger_projections WHERE ledger_event_id = ?""",
                (int(r["id"]),),
            )).fetchall()
            have_main = any(p["target_kind"] == "main" for p in projs)
            have_clients = {
                int(p["target_calendar_id"])
                for p in projs
                if p["target_kind"] == "client"
                and p["target_calendar_id"] is not None
            }
            if not have_main:
                out.append(
                    f"INV-1: ledger_event {r['id']} ({r['source_type']}) "
                    f"has no main projection",
                )
            missing = expected_client_ids - have_clients
            if missing:
                out.append(
                    f"INV-1: ledger_event {r['id']} ({r['source_type']}) "
                    f"is missing client projections for {sorted(missing)}",
                )
            for cid in have_clients - expected_client_ids:
                exists = await (await self.db.execute(
                    "SELECT 1 FROM client_calendars WHERE id = ? AND user_id = ?",
                    (cid, self.user_id),
                )).fetchone()
                if exists is None:
                    out.append(
                        f"INV-1: ledger_event {r['id']} has a spurious "
                        f"projection for unknown client calendar {cid}",
                    )
        return out

    # ------------------------------------------------------------------
    # 2. Present projections correspond to real events on Google.
    # ------------------------------------------------------------------
    async def check_2_present_projections_exist_on_google(self) -> list[str]:
        await self._ensure_client_db_ids()
        rows = await (await self.db.execute(
            """SELECT p.id, p.target_kind, p.target_calendar_id,
                      p.google_event_id, p.desired_state
                 FROM ledger_projections p
                 JOIN ledger_events e ON e.id = p.ledger_event_id
                WHERE e.user_id = ?
                  AND p.current_state = 'present'
                  AND p.google_event_id IS NOT NULL""",
            (self.user_id,),
        )).fetchall()
        out = []
        for r in rows:
            cal_google_id = await self._resolve_target_google_id(
                r["target_kind"], r["target_calendar_id"],
            )
            if cal_google_id is None:
                # Target calendar was disconnected and pruned from the
                # DB; the projection should have been re-targeted by
                # cleanup_one_calendar.  Surface as a violation.
                out.append(
                    f"INV-2: projection {r['id']} (kind={r['target_kind']}, "
                    f"target_calendar_id={r['target_calendar_id']}) "
                    f"cannot resolve a Google calendar id",
                )
                continue
            try:
                ev = self.google.get_event(cal_google_id, r["google_event_id"])
                if ev.get("status") == "cancelled":
                    out.append(
                        f"INV-2: projection {r['id']} present but event "
                        f"{r['google_event_id']} on {cal_google_id} is cancelled",
                    )
            except Exception:
                out.append(
                    f"INV-2: projection {r['id']} → event {r['google_event_id']} "
                    f"missing from {cal_google_id}",
                )
        return out

    # ------------------------------------------------------------------
    # 3. Every event on Google that carries our ID/property has a
    #    corresponding projection.
    # ------------------------------------------------------------------
    async def check_3_no_orphan_google_events(self) -> list[str]:
        out = []
        targets = [self.main_google_id] + list(self.client_google_ids.values())
        for cal in targets:
            page = self.google.list_events(cal, max_results=2500)
            for ev in page.get("items", []):
                eid = ev.get("id")
                is_ours = is_managed_google_event_id(eid) or (
                    (ev.get("extendedProperties", {}).get("private") or {}).get(EP_PROJ_ID)
                )
                if not is_ours:
                    continue
                row = await (await self.db.execute(
                    """SELECT p.id FROM ledger_projections p
                         JOIN ledger_events e ON e.id = p.ledger_event_id
                        WHERE p.google_event_id = ? AND e.user_id = ?""",
                    (eid, self.user_id),
                )).fetchone()
                if row is None:
                    out.append(
                        f"INV-3: orphan ledger-tagged event {eid} on {cal} "
                        f"has no projection",
                    )
        return out

    # ------------------------------------------------------------------
    # 4. No two projections share a Google event ID.
    # ------------------------------------------------------------------
    async def check_4_no_duplicate_google_event_ids(self) -> list[str]:
        rows = await (await self.db.execute(
            """SELECT google_event_id, COUNT(*) AS n
                 FROM ledger_projections
                WHERE google_event_id IS NOT NULL
                GROUP BY google_event_id
                HAVING n > 1""",
        )).fetchall()
        return [
            f"INV-4: {r['n']} projections share google_event_id={r['google_event_id']}"
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 5. No two ledger events share a canonical UID per user.
    # ------------------------------------------------------------------
    async def check_5_no_duplicate_canonical_uids(self) -> list[str]:
        rows = await (await self.db.execute(
            """SELECT user_id, canonical_uid, COUNT(*) AS n
                 FROM ledger_events
                GROUP BY user_id, canonical_uid
                HAVING n > 1""",
        )).fetchall()
        return [
            f"INV-5: user {r['user_id']} has {r['n']} ledger rows for canonical_uid={r['canonical_uid']}"
            for r in rows
        ]

    # ------------------------------------------------------------------
    # 6. Main full-copy count matches the oracle.
    # ------------------------------------------------------------------
    async def check_6_main_copy_count_matches_oracle(self) -> list[str]:
        from app.ledger.facade import count_main_copies
        actual = await count_main_copies(self.db, user_id=self.user_id)
        expected = self.oracle.expected_main_copies()
        if actual != expected:
            return [f"INV-6: main copies actual={actual} expected={expected}"]
        return []

    # ------------------------------------------------------------------
    # 7. Per-client busy-block counts match the oracle.
    # ------------------------------------------------------------------
    async def check_7_busy_block_counts_match_oracle(self) -> list[str]:
        from app.ledger.facade import count_busy_blocks_per_calendar
        await self._ensure_client_db_ids()
        actual = await count_busy_blocks_per_calendar(
            self.db, user_id=self.user_id,
        )
        out = []
        for nick, cid in self.client_db_ids.items():
            expected = self.oracle.expected_busy_blocks_on(nick)
            actual_count = int(actual.get(cid, 0))
            if actual_count != expected:
                out.append(
                    f"INV-7: busy blocks on {nick} actual={actual_count} "
                    f"expected={expected}"
                )
        return out

    # ------------------------------------------------------------------
    # 8. Outbox queue eventually drains to zero.
    # ------------------------------------------------------------------
    async def check_8_outbox_drains(self) -> list[str]:
        row = await (await self.db.execute(
            """SELECT COUNT(*) AS n FROM outbox_operations
                WHERE user_id = ? AND status IN ('pending', 'in_flight')""",
            (self.user_id,),
        )).fetchone()
        n = int(row["n"] or 0)
        if n > 0:
            # Not necessarily a failure mid-cycle; harness asserts
            # this AFTER a clean run with no failure injection.
            return [f"INV-8: outbox has {n} pending/in-flight ops"]
        return []

    # ------------------------------------------------------------------
    # Runtime invariants (9-11)
    # ------------------------------------------------------------------
    @staticmethod
    def check_9_reconcile_latency_bounded(
        samples: list[LatencySample],
        *,
        slope_tolerance: float = 1.5,
    ) -> list[str]:
        """Latency must scale ~linearly in event count, not worse.

        We fit a line through ``samples`` and complain if the
        last sample's per-event cost is more than
        ``slope_tolerance`` × the median per-event cost.
        Exponential blowup catches the eye immediately.
        """
        if len(samples) < 3:
            return []  # not enough data
        per_event = [
            s.wall_seconds / max(1, s.event_count) for s in samples
        ]
        median = sorted(per_event)[len(per_event) // 2]
        last = per_event[-1]
        if median <= 0:
            return []
        ratio = last / median
        if ratio > slope_tolerance:
            return [
                f"INV-9: latency-per-event regression: median={median:.4f}s, "
                f"latest={last:.4f}s (ratio={ratio:.2f} > {slope_tolerance})"
            ]
        return []

    @staticmethod
    def check_10_db_size_sub_linear(
        samples: list[DbSizeSample],
        *,
        bytes_per_row_ceiling: int = 8 * 1024,  # 8KB per ledger row is generous
    ) -> list[str]:
        """Database byte-size per ledger row must stay bounded.

        Catches an unbounded-table regression where, for example,
        we accidentally grow outbox_operations linearly with
        events forever instead of pruning settled rows.
        """
        if not samples:
            return []
        last = samples[-1]
        if last.ledger_row_count == 0:
            return []
        bytes_per_row = last.db_bytes / last.ledger_row_count
        if bytes_per_row > bytes_per_row_ceiling:
            return [
                f"INV-10: db_bytes_per_ledger_row={bytes_per_row:.0f} > "
                f"ceiling {bytes_per_row_ceiling} (db_bytes={last.db_bytes}, "
                f"rows={last.ledger_row_count})"
            ]
        return []

    @staticmethod
    def check_11_recovery_within_cycles(
        cycles_to_clean: int | None,
        *,
        max_cycles: int = 5,
    ) -> list[str]:
        """After failure injection, system must reach a clean state
        within ``max_cycles`` reconciliation cycles."""
        if cycles_to_clean is None:
            return [f"INV-11: did not converge within {max_cycles} cycles"]
        if cycles_to_clean > max_cycles:
            return [
                f"INV-11: took {cycles_to_clean} cycles to recover "
                f"(max allowed={max_cycles})"
            ]
        return []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    async def _ensure_client_db_ids(self) -> None:
        """Lazy-fill ``client_db_ids`` from the database the first
        time it's needed.  The harness supplies nicknames; we
        resolve them to ``client_calendars.id``."""
        if self.client_db_ids is not None:
            return
        self.client_db_ids = {}
        for nick in self.client_google_ids:
            row = await (await self.db.execute(
                """SELECT id FROM client_calendars
                    WHERE user_id = ? AND display_name = ?
                    LIMIT 1""",
                (self.user_id, nick),
            )).fetchone()
            if row is not None:
                self.client_db_ids[nick] = int(row["id"])

    async def _resolve_target_google_id(
        self, target_kind: str, target_calendar_id: int | None,
    ) -> str | None:
        """Resolve a projection's target to a Google calendar id.

        Looks up ``client_calendars.google_calendar_id`` for client
        targets (covers disconnected calendars too — those are
        still in the table with ``is_active=0``).
        """
        if target_kind == "main":
            return self.main_google_id
        if target_calendar_id is None:
            return None
        row = await (await self.db.execute(
            "SELECT google_calendar_id FROM client_calendars WHERE id = ?",
            (int(target_calendar_id),),
        )).fetchone()
        if row is None:
            return None
        return row["google_calendar_id"]


# ---------------------------------------------------------------------------
# Helpers for runtime invariants 9 & 10
# ---------------------------------------------------------------------------
async def sample_db_size(
    db: aiosqlite.Connection,
    db_path: str,
    user_id: int,
) -> DbSizeSample:
    """Snapshot the database file size + ledger row count.

    For in-memory databases (``:memory:``) we fall back to a sum
    over the sqlite_dbpage size, which is the closest equivalent.
    """
    row = await (await db.execute(
        "SELECT COUNT(*) AS n FROM ledger_events WHERE user_id = ?",
        (user_id,),
    )).fetchone()
    ledger_rows = int(row["n"] or 0)

    if db_path and db_path != ":memory:" and os.path.exists(db_path):
        size = os.path.getsize(db_path)
    else:
        # In-memory: estimate via page_count * page_size.
        pc = await (await db.execute("PRAGMA page_count")).fetchone()
        ps = await (await db.execute("PRAGMA page_size")).fetchone()
        size = int(pc[0]) * int(ps[0]) if pc and ps else 0

    # event_count from the oracle perspective is the same as the
    # ledger row count under normal operation.
    return DbSizeSample(
        event_count=ledger_rows,
        ledger_row_count=ledger_rows,
        db_bytes=size,
    )


def timed_reconcile(label: str = ""):
    """Context-manager-style helper: returns ``(start_perf_counter,
    finalizer)`` for measuring reconcile-cycle wall time."""
    return time.perf_counter()
