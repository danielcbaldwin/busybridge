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
import sqlite3
import tempfile
import zipfile
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

    Returns {'kept': [...], 'deleted': [...]} lists of backup IDs.
    """
    limits = {"daily": 7, "weekly": 2, "monthly": 6}
    backups = list_backups()  # newest-first

    by_type: dict[str, list] = {"daily": [], "weekly": [], "monthly": []}
    for b in backups:
        btype = b.get("backup_type", "daily")
        if btype in by_type:
            by_type[btype].append(b)

    to_delete: list[str] = []
    kept: list[str] = []

    for btype, limit in limits.items():
        for i, entry in enumerate(by_type[btype]):
            if i < limit:
                kept.append(entry["backup_id"])
            else:
                to_delete.append(entry["backup_id"])

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
    backup_id = f"backup-{now.strftime('%Y%m%d-%H%M%S')}-{backup_type}"
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
        "total_events_snapshotted": total_events,
        "snapshot_errors": snapshot_errors,
    }

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("metadata.json", json.dumps(metadata, indent=2))

        # Consistent DB copy via sqlite3 backup API (WAL-safe)
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            src_conn = sqlite3.connect(settings.database_path)
            dst_conn = sqlite3.connect(tmp_path)
            src_conn.backup(dst_conn)
            src_conn.close()
            dst_conn.close()
            with open(tmp_path, "rb") as f:
                zf.writestr("database.db", f.read())
        finally:
            os.unlink(tmp_path)

        for uid, snap in snapshots.items():
            zf.writestr(f"snapshots/{uid}.json", json.dumps(snap, indent=2))

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
            meta = {"backup_id": backup_id}

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
    ("webhook_channels",
     "client_calendar_id IN (SELECT id FROM client_calendars WHERE user_id = ?)"),
    ("calendar_sync_state",
     "client_calendar_id IN (SELECT id FROM client_calendars WHERE user_id = ?)"),
    ("main_calendar_sync_state", "user_id = ?"),
    ("sync_log",                 "user_id = ?"),
    ("client_calendars",         "user_id = ?"),
    ("oauth_tokens",             "user_id = ?"),
    ("users",                    "id = ?"),
]

# FK-safe insert order (reverse of delete: parents before children).
_USER_INSERT_ORDER = [
    "users", "oauth_tokens", "client_calendars",
    "calendar_sync_state", "main_calendar_sync_state", "sync_log",
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
    "webhook_channels":         None,
    "reconcile_requests":       "user_id",
    "ledger_events":            "user_id",
    "ledger_projections":       None,
    "outbox_operations":        "user_id",
}

_USER_SUBQUERY: dict[str, str] = {
    "calendar_sync_state":
        "client_calendar_id IN (SELECT id FROM client_calendars WHERE user_id = ?)",
    "webhook_channels":
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


async def _restore_db_for_users(backup_zip: zipfile.ZipFile, user_ids: list[int]) -> None:
    """Restore database rows for specific users from the backup ZIP."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with backup_zip.open("database.db") as src:
            with open(tmp_path, "wb") as dst:
                dst.write(src.read())

        bk_conn = sqlite3.connect(f"file:{tmp_path}?mode=ro", uri=True)
        live_db = await get_database()

        for uid in user_ids:
            await _restore_single_user(live_db, bk_conn, uid)

        bk_conn.close()
        await live_db.commit()
    finally:
        os.unlink(tmp_path)


async def _restore_full_db(backup_zip: zipfile.ZipFile) -> None:
    """Replace the entire live database with the backup DB (sqlite3 backup API)."""
    settings = get_settings()
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with backup_zip.open("database.db") as src:
            with open(tmp_path, "wb") as dst:
                dst.write(src.read())

        src_conn = sqlite3.connect(tmp_path)
        dst_conn = sqlite3.connect(settings.database_path)
        src_conn.backup(dst_conn)
        src_conn.close()
        dst_conn.close()
    finally:
        os.unlink(tmp_path)


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

    await reconcile_user_by_id(user_id, run_discovery=True)

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
    replace, without touching the live DB or Google.
    """
    actions: list[dict] = []
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            with zf.open("database.db") as src, open(tmp_path, "wb") as dst:
                dst.write(src.read())
        conn = sqlite3.connect(f"file:{tmp_path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row

        def _count(sql: str, params: tuple) -> int:
            try:
                row = conn.execute(sql, params).fetchone()
                return int(row["n"]) if row else 0
            except sqlite3.DatabaseError:
                return 0  # pre-ledger backup, or not a usable database

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
        conn.close()
    finally:
        os.unlink(tmp_path)
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
        restore_db:         Whether to replace the DB rows.
        restore_calendars:  Whether to re-converge Google after the DB restore.
        dry_run:            Preview what would be restored; touch nothing.

    Returns a summary dict.
    """
    from app.database import get_setting, set_setting

    zip_path = _backup_filepath(backup_id)
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Backup not found: {backup_id}")

    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open("metadata.json") as f:
            metadata = json.load(f)

    backup_user_ids: list[int] = metadata.get("user_ids_snapshotted", [])
    target_user_ids: list[int] = user_ids if user_ids else backup_user_ids

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

    # Pause the scheduler for the duration of a real restore.
    paused_setting = await get_setting("sync_paused")
    originally_paused = bool(
        paused_setting and paused_setting.get("value_plain") == "true"
    )
    if not originally_paused:
        await set_setting("sync_paused", "true")
        logger.info("Restore: sync paused")

    restore_started = datetime.now(UTC).isoformat()
    try:
        # Step 1: restore DB rows.
        if restore_db:
            restore_all_users = set(target_user_ids) == set(backup_user_ids)
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

        # Step 2: re-converge Google from the restored ledger.
        if restore_calendars:
            await _clear_sync_tokens(target_user_ids if user_ids else None)
            db = await get_database()
            for uid in target_user_ids:
                try:
                    n = await _reset_projections_for_reconverge(db, uid)
                    counters = await _reconverge_user(uid, restore_started)
                    summary["events_created"] += counters["create"]
                    summary["events_updated"] += counters["update"]
                    summary["events_deleted"] += counters["delete"]
                    summary["users_restored"].append(uid)
                    logger.info(
                        "Restore: re-converged user %s (%d projections reset)",
                        uid, n,
                    )
                except Exception as e:
                    msg = f"calendar re-converge failed for user {uid}: {e}"
                    logger.error(msg)
                    summary["errors"].append(msg)
        else:
            summary["users_restored"] = list(target_user_ids)

        # Step 3: audit log.
        db = await get_database()
        await db.execute(
            """INSERT INTO sync_log (action, status, details)
               VALUES ('backup_restore', 'success', ?)""",
            (json.dumps({
                "backup_id": backup_id,
                "users": target_user_ids,
                "db_restored": summary["db_restored"],
                "events_created": summary["events_created"],
                "events_updated": summary["events_updated"],
                "events_deleted": summary["events_deleted"],
            }),),
        )
        await db.commit()

    finally:
        if not originally_paused:
            await set_setting("sync_paused", "false")
            logger.info("Restore: sync resumed")

    return summary
