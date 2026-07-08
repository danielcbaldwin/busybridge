"""Calendar backup and rollback logic.

A backup is a ZIP file containing:
  - metadata.json         : version, timestamps, user list, event counts
  - database.db           : binary copy of the live SQLite database
  - snapshots/<uid>.json  : per-user Google Calendar event snapshots

Retention policy (applied automatically after every scheduled backup):
  - 7 daily   (most-recent backup from each of the last 7 days)
  - 2 weekly  (most-recent Sunday backup, last 2)
  - 6 monthly (most-recent 1st-of-month backup, last 6)

Restore is ledger-native: the ``database.db`` in the backup carries
the full canonical ledger (``ledger_events`` / ``ledger_projections``
/ ``outbox_operations``), which IS the post-rewrite source of truth.
A restore replaces the DB rows, then resets the projections so the
reconciler's idempotent outbox re-converges Google.  There is no
event-diffing or Google-side ID remapping — the outbox's
deterministic event IDs make re-creation a 409-as-success.
"""

import io
import json
import logging
import os
import secrets
import shutil
import sqlite3
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional

from app.config import get_settings
from app.database import get_database

logger = logging.getLogger(__name__)

BACKUP_VERSION = "1"
UTC = timezone.utc


# ---------------------------------------------------------------------------
# Directory helpers
# ---------------------------------------------------------------------------

def get_backup_dir() -> str:
    """Return the backup directory, creating it if necessary."""
    # Read live from the environment (not Settings): tests monkeypatch
    # BACKUP_PATH per-case, and the cached Settings object would pin the
    # value from process start.  This is the ONE place the variable is
    # read — everything else (including the ICS export dir) derives
    # from this helper.
    path = os.environ.get("BACKUP_PATH", "/data/backups")
    os.makedirs(path, exist_ok=True)
    return path


def _backup_filepath(backup_id: str) -> str:
    return os.path.join(get_backup_dir(), f"{backup_id}.zip")


# ---------------------------------------------------------------------------
# Retention / classification helpers
# ---------------------------------------------------------------------------

def _classify_backup(dt: datetime) -> str:
    """Return 'monthly', 'weekly', or 'daily' for a backup datetime."""
    if dt.day == 1:
        return "monthly"
    if dt.weekday() == 6:  # Sunday
        return "weekly"
    return "daily"


def apply_retention_policy() -> dict:
    """
    Enforce 7 daily / 2 weekly / 6 monthly retention.

    The unit of retention is the calendar DAY, not the file: within each
    bucket the most-recent backup from each distinct day (derived from
    ``created_at``) is kept, up to the bucket's day limit — exactly what
    the module docstring promises ("most-recent backup from each of the
    last 7 days").  Same-day extras (e.g. several manual backups in one
    afternoon) collapse to that day's newest instead of each consuming a
    retention slot and evicting a week of scheduled history.  The same
    per-day rule keeps the weekly/monthly buckets consistent: two backups
    taken on one Sunday count as ONE retained Sunday, not two.

    A backup whose metadata cannot be read is never deleted: without
    ``created_at`` it cannot be classified or aged, and a transiently
    unreadable file must not be destroyed.  It is kept and logged.

    Returns {'kept': [...], 'deleted': [...]} lists of backup IDs.
    """
    limits = {"daily": 7, "weekly": 2, "monthly": 6}
    backups = list_backups()  # newest-first

    to_delete: list[str] = []
    kept: list[str] = []
    # Distinct days that already hold a kept backup, per bucket
    # (newest-first, mirroring the list_backups sort).
    days_kept: dict[str, list[str]] = {"daily": [], "weekly": [], "monthly": []}

    for b in backups:
        backup_id = b["backup_id"]
        if b.get("metadata_unreadable") or not b.get("created_at"):
            logger.warning(
                f"Retention: keeping backup {backup_id} — its metadata is "
                f"unreadable or lacks created_at, refusing to delete a "
                f"backup that cannot be classified"
            )
            kept.append(backup_id)
            continue
        btype = b.get("backup_type", "daily")
        if btype not in limits:
            continue  # unknown type: leave it alone
        day = str(b["created_at"])[:10]  # ISO timestamp → YYYY-MM-DD
        days = days_kept[btype]
        if day in days:
            # An older backup from a day whose newest is already kept.
            to_delete.append(backup_id)
        elif len(days) < limits[btype]:
            days.append(day)
            kept.append(backup_id)
        else:
            to_delete.append(backup_id)  # beyond the bucket's day window

    for backup_id in to_delete:
        path = _backup_filepath(backup_id)
        try:
            os.remove(path)
            logger.info(f"Retention: deleted backup {backup_id}")
        except OSError as e:
            logger.warning(f"Could not delete backup {backup_id}: {e}")

    return {"kept": kept, "deleted": to_delete}


# ---------------------------------------------------------------------------
# Event snapshot helpers
# ---------------------------------------------------------------------------

_SNAPSHOT_FIELDS = [
    "id", "summary", "description", "location", "status",
    "start", "end", "recurrence", "recurringEventId", "originalStartTime",
    "extendedProperties", "colorId", "transparency", "visibility",
    "attendees", "organizer", "guestsCanModify", "guestsCanInviteOthers",
    "guestsCanSeeOtherGuests", "reminders",
]


def _event_snapshot_fields(event: dict) -> dict:
    """Keep only the fields needed to store and later restore an event."""
    return {k: event[k] for k in _SNAPSHOT_FIELDS if k in event}


async def _snapshot_user(user: dict) -> dict:
    """Fetch all BusyBridge-managed events for one user across all calendars.

    The snapshot is informational — recorded in the backup ZIP for
    operator inspection / forensics.  Restore does NOT replay it: the
    authoritative state is the ledger inside ``database.db``.
    """
    from app.auth.google import get_valid_access_token
    from app.sync.google_calendar import AsyncGoogleCalendarClient

    user_id = user["id"]
    settings = get_settings()

    result: dict = {
        "user_id": user_id,
        "user_email": user["email"],
        "main_calendar_id": user["main_calendar_id"],
        "main_calendar_events": [],
        "client_calendars": [],
        "errors": [],
    }

    # Main calendar
    try:
        token = await get_valid_access_token(user_id, user["email"])
        main_client = AsyncGoogleCalendarClient(token, settings=settings)
        resp = await main_client.list_events(user["main_calendar_id"], single_events=False)
        result["main_calendar_events"] = [
            _event_snapshot_fields(e)
            for e in resp.get("events", [])
            if main_client.is_our_event(e) and e.get("status") != "cancelled"
        ]
    except Exception as e:
        msg = f"main calendar snapshot failed: {e}"
        logger.error(f"User {user_id}: {msg}")
        result["errors"].append(msg)

    # Client calendars
    db = await get_database()
    cursor = await db.execute(
        """SELECT cc.id, cc.google_calendar_id, cc.display_name,
                  ot.google_account_email
           FROM client_calendars cc
           JOIN oauth_tokens ot ON cc.oauth_token_id = ot.id
           WHERE cc.user_id = ? AND cc.is_active = TRUE""",
        (user_id,),
    )
    client_cals = await cursor.fetchall()

    for cal in client_cals:
        cal_entry: dict = {
            "client_calendar_id": cal["id"],
            "calendar_id": cal["google_calendar_id"],
            "display_name": cal["display_name"],
            "events": [],
            "errors": [],
        }
        try:
            token = await get_valid_access_token(user_id, cal["google_account_email"])
            client = AsyncGoogleCalendarClient(token, settings=settings)
            resp = await client.list_events(cal["google_calendar_id"], single_events=False)
            cal_entry["events"] = [
                _event_snapshot_fields(e)
                for e in resp.get("events", [])
                if client.is_our_event(e) and e.get("status") != "cancelled"
            ]
        except Exception as e:
            msg = f"client calendar {cal['id']} snapshot failed: {e}"
            logger.error(f"User {user_id}: {msg}")
            cal_entry["errors"].append(msg)

        result["client_calendars"].append(cal_entry)

    return result


# ---------------------------------------------------------------------------
# Backup creation
# ---------------------------------------------------------------------------

async def create_backup(user_ids: Optional[list[int]] = None) -> dict:
    """Create a full backup ZIP.

    Args:
        user_ids: If provided, only snapshot these users' calendars. The
                  database dump is always the complete database.

    Returns metadata dict (backup_id, created_at, counts, etc.).
    """
    settings = get_settings()
    now = datetime.now()  # respects TZ env var
    backup_type = _classify_backup(now)
    # A random suffix keeps two backups started in the same second
    # from colliding on the same id (and overwriting each other's ZIP).
    backup_id = (
        f"backup-{now.strftime('%Y%m%d-%H%M%S')}-{backup_type}"
        f"-{secrets.token_hex(3)}"
    )
    zip_path = _backup_filepath(backup_id)

    db = await get_database()

    # Which users to snapshot
    if user_ids:
        placeholders = ",".join("?" * len(user_ids))
        cursor = await db.execute(
            f"SELECT * FROM users WHERE id IN ({placeholders}) AND main_calendar_id IS NOT NULL",
            user_ids,
        )
    else:
        cursor = await db.execute(
            "SELECT * FROM users WHERE main_calendar_id IS NOT NULL"
        )
    users = await cursor.fetchall()

    snapshots: dict[str, dict] = {}
    total_events = 0
    snapshot_errors: list[str] = []

    for user in users:
        snap = await _snapshot_user(dict(user))
        snapshots[str(user["id"])] = snap
        total_events += len(snap["main_calendar_events"])
        total_events += sum(len(c["events"]) for c in snap["client_calendars"])
        snapshot_errors.extend(snap.get("errors", []))

    metadata = {
        "version": BACKUP_VERSION,
        "backup_id": backup_id,
        "backup_type": backup_type,
        "created_at": now.isoformat(),
        "user_ids_snapshotted": [u["id"] for u in users],
        # True only for a whole-instance backup.  A backup scoped to
        # specific user_ids still carries the complete database.db, so
        # restore must NOT file-swap it (that would roll back users
        # who were never part of the requested scope) — it does a
        # per-user row restore instead.
        "full_db_backup": user_ids is None,
        "total_events_snapshotted": total_events,
        "snapshot_errors": snapshot_errors,
    }

    backup_dir = get_backup_dir()

    # Free-space guard: refuse rather than produce a silently-truncated
    # (corrupt) backup if the disk can't hold the DB copy + the zip.  Only
    # meaningful for a file-backed DB (skip for the in-memory test DB).
    if os.path.exists(settings.database_path):
        db_size = os.path.getsize(settings.database_path)
        st = os.statvfs(backup_dir)
        free = st.f_bavail * st.f_frsize
        needed = int(db_size * 1.5)  # DB copy + (compressed) zip headroom
        if free < needed:
            raise RuntimeError(
                f"refusing to back up: only {free} bytes free in {backup_dir}, "
                f"but ~{needed} are needed to copy a {db_size}-byte database"
            )

    # Make a consistent DB copy (sqlite3 backup API, WAL-safe) in the
    # backup dir, then VERIFY it before we ship it — a backup that only
    # looks complete is worse than none (you find out at restore time).
    # The copy is then STREAMED into the zip via ZipFile.write (reads in
    # blocks) instead of being read whole into memory: the live DB can be
    # hundreds of MB on the Pi, and f.read() spiked RAM by that much.
    fd, tmp_path = tempfile.mkstemp(suffix=".db", dir=backup_dir)
    os.close(fd)
    try:
        src_conn = sqlite3.connect(settings.database_path)
        dst_conn = sqlite3.connect(tmp_path)
        try:
            src_conn.backup(dst_conn)
        finally:
            src_conn.close()
            dst_conn.close()

        check = sqlite3.connect(tmp_path)
        try:
            result = check.execute("PRAGMA integrity_check").fetchone()
            if not result or result[0] != "ok":
                raise RuntimeError(
                    f"backup DB copy failed integrity_check: {result!r}"
                )
        finally:
            check.close()

        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("metadata.json", json.dumps(metadata, indent=2))
                zf.write(tmp_path, arcname="database.db")
                for uid, snap in snapshots.items():
                    zf.writestr(f"snapshots/{uid}.json", json.dumps(snap, indent=2))
        except BaseException:
            # Never leave a half-written zip that masquerades as a backup.
            try:
                os.unlink(zip_path)
            except FileNotFoundError:
                pass
            raise
    finally:
        os.unlink(tmp_path)

    file_size = os.path.getsize(zip_path)
    metadata["file_size_bytes"] = file_size

    logger.info(
        f"Backup created: {backup_id} "
        f"({file_size} bytes, {total_events} events, {len(users)} users)"
    )
    return metadata


# ---------------------------------------------------------------------------
# Listing and deleting
# ---------------------------------------------------------------------------

def list_backups() -> list[dict]:
    """Return all backups sorted newest-first."""
    backup_dir = get_backup_dir()
    results: list[dict] = []

    for fname in os.listdir(backup_dir):
        if not (fname.startswith("backup-") and fname.endswith(".zip")):
            continue
        fpath = os.path.join(backup_dir, fname)
        backup_id = fname[: -len(".zip")]
        try:
            with zipfile.ZipFile(fpath, "r") as zf:
                with zf.open("metadata.json") as f:
                    meta = json.load(f)
        except Exception:
            # Corrupt zip, missing metadata.json, or a transient read
            # failure.  Surface the backup with an explicit marker rather
            # than a bare fallback dict: apply_retention_policy uses the
            # marker to refuse to delete a backup it cannot classify.
            meta = {"backup_id": backup_id, "metadata_unreadable": True}

        meta["file_size_bytes"] = os.path.getsize(fpath)
        results.append(meta)

    results.sort(key=lambda m: m.get("created_at", ""), reverse=True)
    return results


def delete_backup(backup_id: str) -> bool:
    """Delete a backup by ID. Returns True if deleted, False if not found."""
    path = _backup_filepath(backup_id)
    if not os.path.exists(path):
        return False
    os.remove(path)
    logger.info(f"Deleted backup {backup_id}")
    return True


# ---------------------------------------------------------------------------
# Startup restore (catastrophic recovery path)
# ---------------------------------------------------------------------------

async def apply_startup_restore(zip_path: str) -> dict:
    """Restore the database from a backup ZIP before the scheduler starts.

    This is the catastrophic recovery entry point.  It must be called
    BEFORE get_database() opens the aiosqlite connection so that aiosqlite
    sees the restored file when it first opens it.

    The whole database (including the canonical ledger) is replaced.
    Every restored projection is then reset so the first scheduled
    reconcile re-converges Google idempotently — without this a
    catastrophic restore would leave Google permanently drifted from
    the restored ledger (the legacy "consistency check" no longer
    reconciles anything).

    Returns the backup metadata dict.
    Raises on any validation or I/O error (caller should abort startup).
    """
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Restore file not found: {zip_path}")
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Not a valid ZIP file: {zip_path}")

    with zipfile.ZipFile(zip_path, "r") as zf:
        names = zf.namelist()
        if "metadata.json" not in names:
            raise ValueError("Not a valid BusyBridge backup: missing metadata.json")
        if "database.db" not in names:
            raise ValueError("Not a valid BusyBridge backup: missing database.db")

        with zf.open("metadata.json") as f:
            metadata = json.load(f)

        # Replace the DB file at the filesystem level using sqlite3 — no
        # aiosqlite connection is open yet so this is safe.
        await _restore_full_db(zf)

    # Queue a full re-convergence: reset every projection + clear sync
    # tokens directly on the restored file (still no aiosqlite conn).
    _reset_for_reconverge_in_file(get_settings().database_path, user_ids=None)

    logger.info(
        f"Startup restore: database replaced with backup "
        f"'{metadata.get('backup_id', 'unknown')}' "
        f"(originally created {metadata.get('created_at', 'unknown')}); "
        f"projections reset for re-convergence on the next reconcile"
    )
    return metadata


# ---------------------------------------------------------------------------
# Per-user DB-row restore: table mappings
# ---------------------------------------------------------------------------
# The post-rewrite source of truth is the canonical ledger
# (ledger_events / ledger_projections / outbox_operations) plus the
# per-user reconcile queue (reconcile_requests).  These MUST be
# restored.  The legacy event_mappings / busy_blocks tables were
# dropped at the Stage-5 cutover and no longer exist.

# FK-safe delete order for per-user rows (children before parents).
_USER_DELETE_ORDER: list[tuple[str, str]] = [
    ("outbox_operations",        "user_id = ?"),
    ("ledger_projections",
     "ledger_event_id IN (SELECT id FROM ledger_events WHERE user_id = ?)"),
    ("ledger_events",            "user_id = ?"),
    ("reconcile_requests",       "user_id = ?"),
    ("webhook_channels",         "user_id = ?"),
    ("calendar_sync_state",
     "client_calendar_id IN (SELECT id FROM client_calendars WHERE user_id = ?)"),
    ("main_calendar_sync_state", "user_id = ?"),
    ("sync_log",                 "user_id = ?"),
    ("webcal_subscriptions",     "user_id = ?"),
    ("integrity_status",         "user_id = ?"),
    ("client_calendars",         "user_id = ?"),
    ("oauth_tokens",             "user_id = ?"),
    ("users",                    "id = ?"),
]

# FK-safe insert order (reverse of delete: parents before children).
_USER_INSERT_ORDER = [
    "users", "oauth_tokens", "client_calendars",
    "calendar_sync_state", "main_calendar_sync_state", "sync_log",
    "webcal_subscriptions", "integrity_status",
    "webhook_channels", "reconcile_requests",
    "ledger_events", "ledger_projections", "outbox_operations",
]

# How to filter backup rows by user_id for each table.
# None means use the subquery in _USER_SUBQUERY instead.
_USER_COLUMN: dict[str, Optional[str]] = {
    "users":                    "id",
    "oauth_tokens":             "user_id",
    "client_calendars":         "user_id",
    "calendar_sync_state":      None,
    "main_calendar_sync_state": "user_id",
    "sync_log":                 "user_id",
    "webcal_subscriptions":     "user_id",
    "integrity_status":         "user_id",
    "webhook_channels":         "user_id",
    "reconcile_requests":       "user_id",
    "ledger_events":            "user_id",
    "ledger_projections":       None,
    "outbox_operations":        "user_id",
}

_USER_SUBQUERY: dict[str, str] = {
    "calendar_sync_state":
        "client_calendar_id IN (SELECT id FROM client_calendars WHERE user_id = ?)",
    "ledger_projections":
        "ledger_event_id IN (SELECT id FROM ledger_events WHERE user_id = ?)",
}


def _fetch_backup_user_rows(bk_conn: sqlite3.Connection, table: str, user_id: int) -> list:
    """Fetch rows belonging to user_id from the backup (read-only) sqlite3 connection."""
    bk_conn.row_factory = sqlite3.Row
    cur = bk_conn.cursor()
    subquery = _USER_SUBQUERY.get(table)
    if subquery:
        cur.execute(f"SELECT * FROM {table} WHERE {subquery}", (user_id,))
    else:
        col = _USER_COLUMN.get(table)
        if not col:
            return []
        cur.execute(f"SELECT * FROM {table} WHERE {col} = ?", (user_id,))
    return cur.fetchall()


async def _restore_single_user(live_db, bk_conn: sqlite3.Connection, user_id: int) -> None:
    """Delete a user's live rows and replace them with rows from the backup DB."""
    # Delete in FK-safe order
    for table, where in _USER_DELETE_ORDER:
        await live_db.execute(f"DELETE FROM {table} WHERE {where}", (user_id,))

    # Insert in FK-safe order
    for table in _USER_INSERT_ORDER:
        rows = _fetch_backup_user_rows(bk_conn, table, user_id)
        if not rows:
            continue
        col_names = list(rows[0].keys())
        placeholders = ", ".join("?" * len(col_names))
        cols_str = ", ".join(col_names)
        sql = f"INSERT OR REPLACE INTO {table} ({cols_str}) VALUES ({placeholders})"
        for row in rows:
            await live_db.execute(sql, list(row))


@contextmanager
def _extracted_backup_db(backup_zip: zipfile.ZipFile):
    """Extract the backup's ``database.db`` to a temp file; yield its path.

    The zip entry is STREAMED out with shutil.copyfileobj for the same
    reason the create side streams (see create_backup): database.db can
    be hundreds of MB on the Pi, and ``src.read()`` spiked RAM by that
    much.  The temp file is always removed on exit.
    """
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with backup_zip.open("database.db") as src:
            with open(tmp_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
        yield tmp_path
    finally:
        os.unlink(tmp_path)


# Tables a ledger-native restore cannot do without — per the mapping
# comment above, the canonical ledger "MUST be restored".  The per-user
# restore path deletes a user's live rows before copying the backup
# rows in, so discovering a missing table mid-copy (a bare "no such
# table" OperationalError out of _fetch_backup_user_rows) would come
# far too late — and after a dry-run preview that implied the restore
# was fine.  A pre-ledger or unreadable backup database is therefore
# rejected up front, with the SAME clear error on both the preview and
# the real path.  (The whole-file swap paths deliberately stay
# tolerant: init_schema migrates a swapped-in pre-ledger file when the
# connection reopens, and apply_startup_restore's file-level reset
# skips missing tables by design.)
_REQUIRED_RESTORE_TABLES = (
    "users", "ledger_events", "ledger_projections", "outbox_operations",
)


def _require_restorable_backup_db(conn: sqlite3.Connection) -> None:
    """Raise ValueError unless ``conn`` is a readable, ledger-era backup DB."""
    try:
        tables = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    except sqlite3.DatabaseError as e:
        raise ValueError(
            f"Not a restorable backup: database.db is not a readable "
            f"SQLite database ({e})"
        )
    missing = [t for t in _REQUIRED_RESTORE_TABLES if t not in tables]
    if missing:
        raise ValueError(
            f"Not a restorable backup: database.db predates the ledger "
            f"schema (missing tables: {', '.join(missing)})"
        )


def _backup_db_user_ids(backup_zip: zipfile.ZipFile) -> set[int]:
    """Return the set of user ids present in the backup's ``users`` table.

    This — not metadata's ``user_ids_snapshotted`` — is what a scoped
    restore must validate against: the snapshot list only names users
    that had a main calendar at backup time, but database.db carries
    EVERY user's rows, so a half-onboarded user is fully restorable.
    Also validates the backup DB up front (see
    :func:`_require_restorable_backup_db`).
    """
    with _extracted_backup_db(backup_zip) as tmp_path:
        conn = sqlite3.connect(f"file:{tmp_path}?mode=ro", uri=True)
        try:
            _require_restorable_backup_db(conn)
            return {
                int(r[0])
                for r in conn.execute("SELECT id FROM users").fetchall()
            }
        finally:
            conn.close()


async def _restore_db_for_users(backup_zip: zipfile.ZipFile, user_ids: list[int]) -> None:
    """Restore database rows for specific users from the backup ZIP."""
    with _extracted_backup_db(backup_zip) as tmp_path:
        bk_conn = sqlite3.connect(f"file:{tmp_path}?mode=ro", uri=True)
        try:
            # Fail BEFORE any live rows are deleted: a backup without the
            # ledger tables has nothing to put back after the delete.
            _require_restorable_backup_db(bk_conn)
            live_db = await get_database()
            # The app connection runs in autocommit mode (isolation_level
            # =None), so without an explicit transaction every DELETE and
            # INSERT in _restore_single_user commits on its own — a crash
            # mid-restore would leave a user partially wiped.  BEGIN
            # IMMEDIATE makes the whole scoped restore atomic: every user
            # lands, or none of them do.
            await live_db.execute("BEGIN IMMEDIATE")
            try:
                for uid in user_ids:
                    await _restore_single_user(live_db, bk_conn, uid)
            except Exception:
                await live_db.rollback()
                raise
            await live_db.commit()
        finally:
            bk_conn.close()


async def _restore_full_db(backup_zip: zipfile.ZipFile) -> None:
    """Replace the entire live database with the backup's database.db.

    Goes through :func:`app.database.replace_database_file`, which
    closes the app's shared connection, swaps the file, and lets the
    next ``get_database()`` reopen on the new file — so the swap never
    races a live connection.  At startup (no connection open yet) it
    simply copies the file.  Runtime callers MUST hold maintenance
    mode so the scheduler cannot reopen a connection mid-swap.
    """
    from app.database import replace_database_file

    dest_db_path = get_settings().database_path
    with _extracted_backup_db(backup_zip) as tmp_path:
        await replace_database_file(tmp_path, dest_db_path)


async def _clear_sync_tokens(user_ids: Optional[list[int]] = None) -> None:
    """Null out sync tokens so the next sync does a clean full re-fetch."""
    db = await get_database()
    if user_ids:
        placeholders = ",".join("?" * len(user_ids))
        await db.execute(
            f"""UPDATE calendar_sync_state SET sync_token = NULL
                WHERE client_calendar_id IN (
                    SELECT id FROM client_calendars WHERE user_id IN ({placeholders})
                )""",
            user_ids,
        )
        await db.execute(
            f"UPDATE main_calendar_sync_state SET sync_token = NULL WHERE user_id IN ({placeholders})",
            user_ids,
        )
    else:
        await db.execute("UPDATE calendar_sync_state SET sync_token = NULL")
        await db.execute("UPDATE main_calendar_sync_state SET sync_token = NULL")
    await db.commit()


# ---------------------------------------------------------------------------
# Ledger-native re-convergence
# ---------------------------------------------------------------------------
# SQL that resets a projection so the diff re-enqueues it.  After a DB
# restore the projections' applied_* columns already equal desired_*
# (the backup captured a converged state), so a plain reconcile would
# be a no-op even though live Google may have drifted.  Nulling the
# applied_* / current_state columns forces the diff to re-assert every
# projection; the outbox's deterministic event IDs make the re-write
# idempotent (insert of an existing event → 409-as-success, delete of
# a missing event → 404/410-as-success).
_RESET_PROJECTION_COLUMNS = """
    SET applied_ledger_version = NULL,
        applied_payload_hash = NULL,
        current_state = 'unknown',
        google_etag = NULL,
        permanently_failed = 0,
        attempts = 0,
        next_attempt_at = NULL,
        last_error = NULL
"""


async def _reset_projections_for_reconverge(live_db, user_id: int) -> int:
    """Reset every projection for ``user_id`` so the next reconcile
    re-asserts it against Google.  Returns the projection count."""
    # Drop stale queued work; the re-diff rebuilds it from scratch.
    await live_db.execute(
        """DELETE FROM outbox_operations
            WHERE user_id = ? AND status IN ('pending', 'in_flight')""",
        (user_id,),
    )
    await live_db.execute(
        f"""UPDATE ledger_projections {_RESET_PROJECTION_COLUMNS}
            WHERE ledger_event_id IN (
                SELECT id FROM ledger_events WHERE user_id = ?
            )""",
        (user_id,),
    )
    row = await (await live_db.execute(
        """SELECT COUNT(*) AS n FROM ledger_projections
            WHERE ledger_event_id IN (
                SELECT id FROM ledger_events WHERE user_id = ?
            )""",
        (user_id,),
    )).fetchone()
    await live_db.commit()
    return int(row["n"]) if row else 0


def _reset_for_reconverge_in_file(db_path: str, user_ids: Optional[list[int]]) -> None:
    """Reset projections + clear sync tokens directly on a DB file.

    Used by :func:`apply_startup_restore`, which runs before aiosqlite
    opens the connection.  ``user_ids=None`` resets every user.

    Tolerant of a pre-ledger or minimal backup: any table that is not
    present is simply skipped.
    """
    conn = sqlite3.connect(db_path)
    try:
        try:
            tables = {
                r[0] for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        except sqlite3.DatabaseError:
            return  # not a usable database — nothing to reset

        marks = ",".join("?" * len(user_ids)) if user_ids else ""

        def run(table: str, sql: str, needs: tuple[str, ...] = ()) -> None:
            if table not in tables:
                return
            if any(t not in tables for t in needs):
                return
            conn.execute(sql, user_ids if user_ids else ())

        if user_ids:
            run("ledger_projections",
                f"""UPDATE ledger_projections {_RESET_PROJECTION_COLUMNS}
                    WHERE ledger_event_id IN (
                        SELECT id FROM ledger_events WHERE user_id IN ({marks}))""",
                needs=("ledger_events",))
            run("outbox_operations",
                f"""DELETE FROM outbox_operations
                     WHERE status IN ('pending', 'in_flight')
                       AND user_id IN ({marks})""")
            run("calendar_sync_state",
                f"""UPDATE calendar_sync_state SET sync_token = NULL
                     WHERE client_calendar_id IN (
                         SELECT id FROM client_calendars WHERE user_id IN ({marks}))""",
                needs=("client_calendars",))
            run("main_calendar_sync_state",
                f"UPDATE main_calendar_sync_state SET sync_token = NULL "
                f"WHERE user_id IN ({marks})")
        else:
            run("ledger_projections",
                f"UPDATE ledger_projections {_RESET_PROJECTION_COLUMNS}")
            run("outbox_operations",
                "DELETE FROM outbox_operations "
                "WHERE status IN ('pending', 'in_flight')")
            run("calendar_sync_state",
                "UPDATE calendar_sync_state SET sync_token = NULL")
            run("main_calendar_sync_state",
                "UPDATE main_calendar_sync_state SET sync_token = NULL")
        conn.commit()
    finally:
        conn.close()


async def _reconverge_user(user_id: int, since_iso: str) -> dict:
    """Run one reconcile pass so the outbox re-writes Google, then
    report how many create/update/delete ops it completed."""
    from app.ledger.runtime import reconcile_user_by_id

    # allow_in_maintenance: the restore holds maintenance mode to keep
    # the scheduler out, but its own re-converge pass must still run.
    await reconcile_user_by_id(
        user_id, run_discovery=True, allow_in_maintenance=True,
    )

    db = await get_database()
    rows = await (await db.execute(
        """SELECT operation, COUNT(*) AS n
             FROM outbox_operations
            WHERE user_id = ? AND status = 'done' AND created_at >= ?
            GROUP BY operation""",
        (user_id, since_iso),
    )).fetchall()
    counts = {r["operation"]: int(r["n"]) for r in rows}
    return {
        "create": counts.get("create", 0),
        "update": counts.get("update", 0),
        "delete": counts.get("delete", 0),
    }


def _preview_restore(zip_path: str, user_ids: list[int]) -> list[dict]:
    """Open the backup DB read-only and report per-user row counts.

    This is the dry-run preview: it states what the restore WOULD
    replace, without touching the live DB or Google.  It applies the
    same up-front backup-DB validation as the real restore path
    (:func:`_require_restorable_backup_db`) — a preview must not imply
    that an unrestorable (pre-ledger / corrupt) backup would restore
    fine.
    """
    actions: list[dict] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        with _extracted_backup_db(zf) as tmp_path:
            conn = sqlite3.connect(f"file:{tmp_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            try:
                _require_restorable_backup_db(conn)

                def _count(sql: str, params: tuple) -> int:
                    try:
                        row = conn.execute(sql, params).fetchone()
                        return int(row["n"]) if row else 0
                    except sqlite3.DatabaseError:
                        # Defensive: tables beyond the required set may
                        # be absent in an old-but-restorable backup.
                        return 0

                for uid in user_ids:
                    actions.append({
                        "action": "restore_user",
                        "user_id": uid,
                        "ledger_events": _count(
                            "SELECT COUNT(*) AS n FROM ledger_events WHERE user_id = ?",
                            (uid,),
                        ),
                        "ledger_projections": _count(
                            """SELECT COUNT(*) AS n FROM ledger_projections
                                WHERE ledger_event_id IN (
                                    SELECT id FROM ledger_events WHERE user_id = ?)""",
                            (uid,),
                        ),
                        "client_calendars": _count(
                            "SELECT COUNT(*) AS n FROM client_calendars WHERE user_id = ?",
                            (uid,),
                        ),
                        "note": (
                            "DB rows replaced from backup; calendars then "
                            "re-converged idempotently via the ledger outbox"
                        ),
                    })
            finally:
                conn.close()
    return actions


# ---------------------------------------------------------------------------
# Main restore entry point
# ---------------------------------------------------------------------------

async def restore_from_backup(
    backup_id: str,
    user_ids: Optional[list[int]] = None,
    restore_db: bool = True,
    restore_calendars: bool = True,
    dry_run: bool = False,
) -> dict:
    """Restore from a backup (ledger-native).

    The backup's ``database.db`` carries the full canonical ledger, so
    a restore:

    1. Replaces the user's DB rows (including the ledger) from the backup.
    2. Clears sync tokens and resets every restored projection so the
       reconciler re-converges Google.  The outbox uses deterministic
       event IDs, so re-creation hits 409-as-success when the event
       still exists and recreates it otherwise — no event diffing or
       Google-side ID remapping is needed.

    Args:
        backup_id:          ID of the backup to restore from.
        user_ids:           Users to restore. None = all users in the backup.
        restore_db:         Whether to replace the DB rows.  This also
                            resets the restored projections and clears
                            sync tokens (step 2 is inseparable from a
                            ledger rewrite).
        restore_calendars:  Whether to run the immediate Google
                            re-converge pass after the DB restore.  If
                            False, the reset state simply waits for the
                            next scheduled reconcile.
        dry_run:            Preview what would be restored; touch nothing.

    Returns a summary dict.
    """
    zip_path = _backup_filepath(backup_id)
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Backup not found: {backup_id}")

    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open("metadata.json") as f:
            metadata = json.load(f)

    backup_user_ids: list[int] = metadata.get("user_ids_snapshotted", [])
    if user_ids:
        # A scoped restore must name only users actually present in
        # this backup.  _restore_single_user deletes the live rows
        # before loading the backup rows, so a typo'd / stale id would
        # otherwise wipe that live user with nothing to restore.
        # Validate against the backup DATABASE's users table, not
        # metadata's user_ids_snapshotted: the snapshot list only holds
        # users that had a main calendar at backup time, but
        # database.db carries every user's rows — a half-onboarded
        # user is fully restorable and must not be rejected.
        with zipfile.ZipFile(zip_path, "r") as zf:
            present_user_ids = _backup_db_user_ids(zf)
        unknown = sorted(set(user_ids) - present_user_ids)
        if unknown:
            raise ValueError(
                f"users {unknown} are not in backup {backup_id} "
                f"(present in backup database: {sorted(present_user_ids)})"
            )
        target_user_ids: list[int] = list(user_ids)
    else:
        target_user_ids = list(backup_user_ids)

    summary: dict = {
        "backup_id": backup_id,
        "dry_run": dry_run,
        "users_restored": [],
        "db_restored": False,
        "events_deleted": 0,
        "events_created": 0,
        "events_updated": 0,
        "errors": [],
        "planned_actions": [] if dry_run else None,
    }

    # Dry run: report what would happen, touch nothing.
    if dry_run:
        summary["planned_actions"] = _preview_restore(zip_path, target_user_ids)
        summary["users_restored"] = list(target_user_ids)
        return summary

    # Hard-freeze the sync engine for the duration of a real restore.
    # Maintenance mode (in-process) — not settings.sync_paused — is
    # used deliberately: a full restore swaps the database file out,
    # so the flag that guards the swap must live outside the DB, and
    # the restore's own re-converge pass can still run by bypassing it.
    from app.maintenance import (
        enter_maintenance,
        exit_maintenance,
        wait_for_reconcile_quiescence,
    )

    enter_maintenance()
    logger.info("Restore: maintenance mode engaged")

    restore_started = datetime.now(UTC).isoformat()
    try:
        # Drain any reconcile pass that was already running before we
        # touch the database file — maintenance mode stops new passes,
        # but not one already in flight.
        await wait_for_reconcile_quiescence()
        # Step 1: restore DB rows.
        if restore_db:
            # A whole-database file swap is only safe for a backup
            # taken of the whole instance AND a restore that targets
            # every snapshotted user.  A scoped backup (or a scoped
            # restore) goes row-by-row so users outside the scope are
            # never rolled back.  Legacy backups lack the flag — treat
            # them as scoped (the safe default).
            restore_all_users = (
                bool(metadata.get("full_db_backup"))
                and set(target_user_ids) == set(backup_user_ids)
            )
            with zipfile.ZipFile(zip_path, "r") as zf:
                if restore_all_users:
                    await _restore_full_db(zf)
                    logger.info("Restore: full database restored")
                else:
                    await _restore_db_for_users(zf, target_user_ids)
                    logger.info(
                        "Restore: per-user DB rows restored for %s",
                        target_user_ids,
                    )
            summary["db_restored"] = True

            # Step 2: queue re-convergence.  Tied to restore_db — NOT
            # restore_calendars — because it is what any restore that
            # rewrites ledger state needs: the backup captured a
            # converged ledger (applied_* == desired_*), so once the
            # rows are rewritten every future reconcile would no-op
            # against a live Google that may have drifted.  Skipping
            # the immediate Google pass (restore_calendars=False) must
            # not skip this reset, or a DB-only restore leaves Google
            # permanently drifted (see the module docstring and
            # apply_startup_restore, which resets for the same reason).
            await _clear_sync_tokens(target_user_ids if user_ids else None)
            db = await get_database()
            for uid in target_user_ids:
                try:
                    n = await _reset_projections_for_reconverge(db, uid)
                    logger.info(
                        "Restore: reset %d projections for user %s "
                        "(re-converges on the next reconcile)", n, uid,
                    )
                except Exception as e:
                    # A user whose projections stayed applied==desired
                    # WILL drift — record it so the audit row and the
                    # summary make the failure visible.
                    msg = f"projection reset failed for user {uid}: {e}"
                    logger.error(msg)
                    summary["errors"].append(msg)

        # Step 3: immediate Google re-converge pass (optional).  With
        # restore_db=False nothing was rewritten and nothing was reset,
        # so this is just an ordinary on-demand reconcile.
        if restore_calendars:
            for uid in target_user_ids:
                try:
                    counters = await _reconverge_user(uid, restore_started)
                    summary["events_created"] += counters["create"]
                    summary["events_updated"] += counters["update"]
                    summary["events_deleted"] += counters["delete"]
                    summary["users_restored"].append(uid)
                    logger.info("Restore: re-converged user %s", uid)
                except Exception as e:
                    msg = f"calendar re-converge failed for user {uid}: {e}"
                    logger.error(msg)
                    summary["errors"].append(msg)
        else:
            summary["users_restored"] = list(target_user_ids)

        # Step 4: audit log.  The status must reflect the outcome: the
        # admin dashboards (app/api/admin.py, app/ui/routes.py) count
        # sync_log rows WHERE status = 'failure' as "sync errors", and
        # no consumer recognizes 'partial' — so any restore that hit
        # errors is recorded as 'failure' rather than hidden behind
        # 'success' (or an invisible 'partial').  The errors themselves
        # go into details; per-user granularity lives in the summary.
        db = await get_database()
        audit_status = "failure" if summary["errors"] else "success"
        await db.execute(
            """INSERT INTO sync_log (action, status, details)
               VALUES ('backup_restore', ?, ?)""",
            (audit_status, json.dumps({
                "backup_id": backup_id,
                "users": target_user_ids,
                "db_restored": summary["db_restored"],
                "events_created": summary["events_created"],
                "events_updated": summary["events_updated"],
                "events_deleted": summary["events_deleted"],
                "errors": summary["errors"],
            })),
        )
        await db.commit()

    finally:
        exit_maintenance()
        logger.info("Restore: maintenance mode released")

    return summary
