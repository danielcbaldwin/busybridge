"""Regression tests for verified backup/restore review findings.

Covers:
  1. restore_db=True + restore_calendars=False still resets projections
     and clears sync tokens (a DB-only restore must not leave Google
     permanently drifted).
  2. The restore audit row derives its status from the outcome and
     records the errors list in details.
  3. Retention keeps the most-recent backup per calendar DAY (same-day
     manual backups don't evict a week of scheduled history).
  4. A backup with unreadable metadata is never deleted by retention.
  5. Scoped restore validates user_ids against the backup DB's users
     table (half-onboarded users are restorable), and a pre-ledger
     backup fails early with a clear error on both preview and real path.
  6. Restore paths stream database.db out of the zip via one shared
     helper instead of slurping it into memory.
"""

from __future__ import annotations

import io
import json
import os
import zipfile
from typing import Optional

import pytest

from app.database import get_database


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_backup_zip(metadata: dict, db_bytes: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("metadata.json", json.dumps(metadata))
        zf.writestr("database.db", db_bytes)
    return buf.getvalue()


async def _build_backup_db(path, users: list[tuple[int, Optional[str]]]) -> None:
    """Create a full-schema backup DB with the given (id, main_calendar_id)
    users at ``path``."""
    import aiosqlite
    from app.database import init_schema

    conn = await aiosqlite.connect(str(path))
    conn.row_factory = aiosqlite.Row
    await init_schema(conn)
    for uid, main_cal in users:
        await conn.execute(
            """INSERT INTO users
                  (id, email, google_user_id, display_name, main_calendar_id)
               VALUES (?, ?, ?, ?, ?)""",
            (uid, f"u{uid}@example.com", f"g-{uid}", f"u{uid}", main_cal),
        )
    await conn.commit()
    await conn.close()


async def _add_converged_ledger_state(path, user_id: int) -> None:
    """Add a converged (applied == desired) ledger event/projection, a done
    and a pending outbox op, and a main-calendar sync token for user_id."""
    import aiosqlite

    conn = await aiosqlite.connect(str(path))
    await conn.execute(
        """INSERT INTO ledger_events
              (id, user_id, canonical_uid, source_type, status, version,
               summary, created_at, updated_at)
           VALUES (10, ?, 'client:1:evt-abc', 'client', 'active', 1,
                   'Converged meeting', '2026-01-01T00:00:00Z',
                   '2026-01-01T00:00:00Z')""",
        (user_id,),
    )
    await conn.execute(
        """INSERT INTO ledger_projections
              (id, ledger_event_id, target_kind, target_calendar_id,
               desired_state, desired_payload_hash, desired_ledger_version,
               current_state, google_event_id, applied_ledger_version,
               applied_payload_hash)
           VALUES (20, 10, 'main', NULL, 'present_full', 'hash1', 1,
                   'present', 'bb000abc', 1, 'hash1')""",
    )
    await conn.execute(
        """INSERT INTO outbox_operations
              (id, user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, target_google_calendar_id, status)
           VALUES (30, ?, 20, 'create', 'proj:20:v1:create', 1,
                   'main-cal', 'done')""",
        (user_id,),
    )
    await conn.execute(
        """INSERT INTO outbox_operations
              (id, user_id, projection_id, operation, idempotency_key,
               ledger_version_at_enqueue, target_google_calendar_id, status)
           VALUES (31, ?, 20, 'update', 'proj:20:v1:update', 1,
                   'main-cal', 'pending')""",
        (user_id,),
    )
    await conn.execute(
        "INSERT INTO main_calendar_sync_state (user_id, sync_token) "
        "VALUES (?, 'stale-token')",
        (user_id,),
    )
    await conn.commit()
    await conn.close()


def _write_backup_file(tmp_path, metadata: dict, db_path) -> str:
    bid = metadata["backup_id"]
    (tmp_path / f"{bid}.zip").write_bytes(
        _make_backup_zip(metadata, db_bytes=db_path.read_bytes())
    )
    return bid


async def _fetch_audit_rows() -> list:
    db = await get_database()
    return await (await db.execute(
        "SELECT status, details FROM sync_log WHERE action = 'backup_restore'"
    )).fetchall()


# ---------------------------------------------------------------------------
# Fix 1: DB-only restore must still queue the Google re-convergence
# ---------------------------------------------------------------------------


class TestDbOnlyRestoreQueuesReconvergence:
    @pytest.mark.asyncio
    async def test_restore_db_without_calendars_resets_projections_and_tokens(
        self, test_db, tmp_path, monkeypatch
    ):
        """restore_db=True, restore_calendars=False used to skip the
        projection reset + sync-token clearing entirely, so the restored
        applied==desired ledger made every future reconcile a no-op and
        Google stayed permanently drifted.  The reset is now tied to
        restore_db; only the immediate Google pass is optional."""
        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))

        backup_db = tmp_path / "src.db"
        # Two users so a user_ids=[1] restore takes the per-user path.
        await _build_backup_db(backup_db, [(1, "main-cal"), (2, "main-cal")])
        await _add_converged_ledger_state(backup_db, 1)

        metadata = {
            "backup_id": "backup-20260101-000000-daily",
            "backup_type": "daily",
            "created_at": "2026-01-01T00:00:00",
            "user_ids_snapshotted": [1, 2],
        }
        bid = _write_backup_file(tmp_path, metadata, backup_db)

        from app.sync.backup import restore_from_backup
        result = await restore_from_backup(
            bid, user_ids=[1], restore_db=True,
            restore_calendars=False, dry_run=False,
        )
        assert result["db_restored"] is True
        assert result["users_restored"] == [1]
        assert result["errors"] == []

        live = await get_database()

        # Projection restored but RESET: applied_* nulled, state unknown
        # — the next scheduled reconcile re-asserts it against Google.
        proj = await (await live.execute(
            """SELECT current_state, applied_ledger_version,
                      applied_payload_hash, google_etag
                 FROM ledger_projections WHERE id = 20"""
        )).fetchone()
        assert proj["current_state"] == "unknown"
        assert proj["applied_ledger_version"] is None
        assert proj["applied_payload_hash"] is None

        # Stale queued work dropped; completed history kept.
        ops = await (await live.execute(
            "SELECT id, status FROM outbox_operations WHERE user_id = 1"
        )).fetchall()
        assert {(r["id"], r["status"]) for r in ops} == {(30, "done")}

        # Sync token cleared so the next sync does a clean full re-fetch.
        tok = await (await live.execute(
            "SELECT sync_token FROM main_calendar_sync_state WHERE user_id = 1"
        )).fetchone()
        assert tok["sync_token"] is None


# ---------------------------------------------------------------------------
# Fix 2: audit row status reflects the outcome and carries the errors
# ---------------------------------------------------------------------------


class TestRestoreAuditRow:
    async def _make_simple_backup(self, tmp_path) -> str:
        backup_db = tmp_path / "src.db"
        await _build_backup_db(backup_db, [(1, "main-cal"), (2, "main-cal")])
        metadata = {
            "backup_id": "backup-20260101-000000-daily",
            "backup_type": "daily",
            "created_at": "2026-01-01T00:00:00",
            "user_ids_snapshotted": [1, 2],
        }
        return _write_backup_file(tmp_path, metadata, backup_db)

    @pytest.mark.asyncio
    async def test_clean_restore_logs_success_with_empty_errors(
        self, test_db, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        bid = await self._make_simple_backup(tmp_path)

        from app.sync.backup import restore_from_backup
        await restore_from_backup(
            bid, user_ids=[1], restore_db=True,
            restore_calendars=False, dry_run=False,
        )

        rows = await _fetch_audit_rows()
        assert len(rows) == 1
        assert rows[0]["status"] == "success"
        details = json.loads(rows[0]["details"])
        assert details["errors"] == []
        assert details["backup_id"] == bid

    @pytest.mark.asyncio
    async def test_failed_reconverge_logs_failure_with_errors_in_details(
        self, test_db, tmp_path, monkeypatch
    ):
        """The audit row used to hardcode status='success' even when
        summary['errors'] was non-empty, and the errors were lost.  The
        admin dashboards count sync_log rows with status='failure', so a
        restore that hit errors must be recorded as one."""
        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        bid = await self._make_simple_backup(tmp_path)

        async def exploding_reconverge(user_id, since_iso):
            raise RuntimeError("Google said no")

        monkeypatch.setattr(
            "app.sync.backup._reconverge_user", exploding_reconverge
        )

        from app.sync.backup import restore_from_backup
        result = await restore_from_backup(
            bid, user_ids=[1], restore_db=True,
            restore_calendars=True, dry_run=False,
        )
        assert result["errors"]  # the failure is in the summary...

        rows = await _fetch_audit_rows()
        assert len(rows) == 1
        assert rows[0]["status"] == "failure"  # ...and in the audit status
        details = json.loads(rows[0]["details"])
        assert len(details["errors"]) == 1
        assert "re-converge failed for user 1" in details["errors"][0]


# ---------------------------------------------------------------------------
# Fixes 3 + 4: retention is per calendar day and never eats unreadable backups
# ---------------------------------------------------------------------------


class TestRetentionPerDayAndUnreadable:
    def _write_backup(self, tmp_path, created_at: str, btype: str) -> str:
        ts = created_at.replace("-", "").replace("T", "-").replace(":", "")[:15]
        bid = f"backup-{ts}-{btype}"
        meta = {"backup_id": bid, "backup_type": btype, "created_at": created_at}
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("metadata.json", json.dumps(meta))
            zf.writestr("database.db", b"SQLite format 3")
        (tmp_path / f"{bid}.zip").write_bytes(buf.getvalue())
        return bid

    def test_same_day_manual_backups_do_not_evict_scheduled_dailies(
        self, tmp_path, monkeypatch
    ):
        """Seven distinct daily days plus three same-day manual extras:
        the docstring promises "most-recent backup from each of the last
        7 days", so the extras collapse to their day's newest instead of
        evicting a week of scheduled history."""
        from app.sync.backup import apply_retention_policy

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        daily_ids = [
            self._write_backup(tmp_path, f"2026-07-{d:02d}T12:00:00", "daily")
            for d in range(2, 9)  # 7 distinct regular days
        ]
        # Three manual backups later the same afternoon on the newest day.
        manual_ids = [
            self._write_backup(tmp_path, f"2026-07-08T{h}:00:00", "daily")
            for h in ("13", "14", "15")
        ]

        result = apply_retention_policy()

        # Kept: newest backup of each of the 7 distinct days — i.e. the
        # 15:00 manual on 07-08 plus the six older scheduled dailies.
        expected_kept = set(daily_ids[:-1]) | {manual_ids[-1]}
        assert set(result["kept"]) == expected_kept
        # Deleted: only the older same-day duplicates.
        assert set(result["deleted"]) == {daily_ids[-1]} | set(manual_ids[:-1])
        for bid in expected_kept:
            assert (tmp_path / f"{bid}.zip").exists()

    def test_weekly_bucket_dedupes_same_sunday(self, tmp_path, monkeypatch):
        """Two backups on one Sunday count as ONE retained Sunday —
        consistent with the per-day rule in the daily bucket."""
        from app.sync.backup import apply_retention_policy

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        # 2024-03-10 / 17 / 24 are Sundays; limit is 2 distinct Sundays.
        s1 = self._write_backup(tmp_path, "2024-03-10T12:00:00", "weekly")
        s2 = self._write_backup(tmp_path, "2024-03-17T12:00:00", "weekly")
        s3a = self._write_backup(tmp_path, "2024-03-24T08:00:00", "weekly")
        s3b = self._write_backup(tmp_path, "2024-03-24T12:00:00", "weekly")

        result = apply_retention_policy()
        assert set(result["kept"]) == {s3b, s2}
        assert set(result["deleted"]) == {s3a, s1}

    def test_backup_with_unreadable_metadata_is_never_deleted(
        self, tmp_path, monkeypatch
    ):
        """The old fallback meta {'backup_id': ...} classified an
        unreadable backup as the oldest daily, so retention silently
        destroyed it.  A transiently unreadable file must survive."""
        from app.sync.backup import apply_retention_policy

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))
        # Fill the daily bucket to its limit of 7 distinct days...
        for d in range(2, 9):
            self._write_backup(tmp_path, f"2026-07-{d:02d}T12:00:00", "daily")
        # ...and add a backup whose metadata cannot be read.
        (tmp_path / "backup-mystery.zip").write_bytes(b"not a zip at all")

        result = apply_retention_policy()

        assert "backup-mystery" not in result["deleted"]
        assert "backup-mystery" in result["kept"]
        assert (tmp_path / "backup-mystery.zip").exists()
        # The readable dailies were all within limits — nothing deleted.
        assert result["deleted"] == []


# ---------------------------------------------------------------------------
# Fix 5: scoped restore validates against the backup DB's users table
# ---------------------------------------------------------------------------


class TestScopedRestoreValidation:
    @pytest.mark.asyncio
    async def test_half_onboarded_user_is_restorable(
        self, test_db, tmp_path, monkeypatch
    ):
        """User 3 has no main calendar, so it is absent from metadata's
        user_ids_snapshotted — but its rows ARE in database.db, and a
        scoped restore of it must succeed instead of being rejected."""
        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))

        backup_db = tmp_path / "src.db"
        await _build_backup_db(
            backup_db, [(1, "main-cal"), (3, None)]  # 3 = half-onboarded
        )
        metadata = {
            "backup_id": "backup-20260101-000000-daily",
            "backup_type": "daily",
            "created_at": "2026-01-01T00:00:00",
            "user_ids_snapshotted": [1],  # snapshot list omits user 3
        }
        bid = _write_backup_file(tmp_path, metadata, backup_db)

        from app.sync.backup import restore_from_backup
        result = await restore_from_backup(
            bid, user_ids=[3], restore_db=True,
            restore_calendars=False, dry_run=False,
        )
        assert result["db_restored"] is True
        assert result["users_restored"] == [3]

        live = await get_database()
        row = await (await live.execute(
            "SELECT email, main_calendar_id FROM users WHERE id = 3"
        )).fetchone()
        assert row["email"] == "u3@example.com"
        assert row["main_calendar_id"] is None

    @pytest.mark.asyncio
    async def test_user_absent_from_backup_db_is_rejected(
        self, test_db, tmp_path, monkeypatch
    ):
        """A typo'd / stale id must still be rejected up front — the
        per-user restore deletes the live rows before loading the backup
        rows, so restoring a missing user would wipe them."""
        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))

        backup_db = tmp_path / "src.db"
        await _build_backup_db(backup_db, [(1, "main-cal")])
        metadata = {
            "backup_id": "backup-20260101-000000-daily",
            "backup_type": "daily",
            "created_at": "2026-01-01T00:00:00",
            "user_ids_snapshotted": [1],
        }
        bid = _write_backup_file(tmp_path, metadata, backup_db)

        from app.sync.backup import restore_from_backup
        with pytest.raises(ValueError, match=r"users \[99\] are not in backup"):
            await restore_from_backup(
                bid, user_ids=[99], restore_db=True,
                restore_calendars=False, dry_run=False,
            )

    @pytest.mark.asyncio
    async def test_pre_ledger_backup_fails_early_and_clearly_on_both_paths(
        self, test_db, tmp_path, monkeypatch
    ):
        """A backup DB without the ledger tables used to pass the dry-run
        preview (counts silently 0) and then die mid-restore with a bare
        OperationalError.  Both paths now reject it up front with the
        same clear error, before any live rows are deleted."""
        import sqlite3 as _sqlite3

        monkeypatch.setenv("BACKUP_PATH", str(tmp_path))

        # Pre-ledger database: a users table and nothing else.
        backup_db = tmp_path / "preledger.db"
        conn = _sqlite3.connect(str(backup_db))
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)"
        )
        conn.execute("INSERT INTO users (id, email) VALUES (1, 'a@b.c')")
        conn.commit()
        conn.close()

        metadata = {
            "backup_id": "backup-20200101-000000-daily",
            "backup_type": "daily",
            "created_at": "2020-01-01T00:00:00",
            "user_ids_snapshotted": [1],
        }
        bid = _write_backup_file(tmp_path, metadata, backup_db)

        from app.sync.backup import restore_from_backup

        # Scoped real restore: rejected during user-id validation.
        with pytest.raises(ValueError, match="predates the ledger schema"):
            await restore_from_backup(
                bid, user_ids=[1], restore_db=True,
                restore_calendars=False, dry_run=False,
            )

        # Dry-run preview: the same error, not an all-zeros plan.
        with pytest.raises(ValueError, match="predates the ledger schema"):
            await restore_from_backup(
                bid, restore_db=True, restore_calendars=False, dry_run=True,
            )

        # Unscoped real restore (per-user path via the metadata list):
        # rejected before any live rows are deleted.
        live = await get_database()
        await live.execute(
            """INSERT INTO users (id, email, google_user_id, display_name)
               VALUES (1, 'live@example.com', 'g-live', 'live')"""
        )
        await live.commit()
        with pytest.raises(ValueError, match="predates the ledger schema"):
            await restore_from_backup(
                bid, restore_db=True, restore_calendars=False, dry_run=False,
            )
        row = await (await live.execute(
            "SELECT email FROM users WHERE id = 1"
        )).fetchone()
        assert row["email"] == "live@example.com"  # live user untouched


# ---------------------------------------------------------------------------
# Fix 6: the shared streaming extract helper
# ---------------------------------------------------------------------------


class TestExtractedBackupDbHelper:
    def test_yields_db_content_and_cleans_up(self, tmp_path):
        from app.sync.backup import _extracted_backup_db

        payload = b"SQLite format 3\x00" + os.urandom(4096)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("database.db", payload)

        with zipfile.ZipFile(io.BytesIO(buf.getvalue()), "r") as zf:
            with _extracted_backup_db(zf) as tmp_db_path:
                with open(tmp_db_path, "rb") as f:
                    assert f.read() == payload
            # Temp file is removed once the context exits.
            assert not os.path.exists(tmp_db_path)

    def test_cleans_up_even_when_body_raises(self, tmp_path):
        from app.sync.backup import _extracted_backup_db

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("database.db", b"data")

        with zipfile.ZipFile(io.BytesIO(buf.getvalue()), "r") as zf:
            with pytest.raises(RuntimeError):
                with _extracted_backup_db(zf) as tmp_db_path:
                    raise RuntimeError("boom")
            assert not os.path.exists(tmp_db_path)
