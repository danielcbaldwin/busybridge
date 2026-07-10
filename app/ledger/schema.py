"""Schema for the canonical-ledger architecture.

These tables are additive — they sit alongside the legacy
``event_mappings`` / ``busy_blocks`` until the cutover.

Use :func:`init_ledger_schema` against any aiosqlite-compatible
connection.  Safe to call repeatedly (all CREATEs are
``IF NOT EXISTS``).
"""

from __future__ import annotations

import sqlite3

import aiosqlite

LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS ledger_events (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    canonical_uid TEXT NOT NULL,
    parent_canonical_uid TEXT,

    source_type TEXT NOT NULL,
    source_calendar_id INTEGER,
    source_event_id TEXT,
    source_etag TEXT,
    source_updated_at TIMESTAMP,
    -- Google's stable cross-calendar event identity (events.iCalUID).
    -- The SAME meeting shares one iCalUID across every calendar the user
    -- is on, so this lets the planner recognise when a 'main_native'
    -- reflection of a meeting is the same event already ingested from a
    -- client/personal source and suppress the duplicate busy block.
    -- Stored as identity only; deliberately NOT part of the content hash.
    ical_uid TEXT,

    summary TEXT,
    description TEXT,
    location TEXT,
    start_at TEXT,
    end_at TEXT,
    start_timezone TEXT,
    end_timezone TEXT,
    is_all_day BOOLEAN DEFAULT FALSE,
    show_as TEXT,
    visibility TEXT,
    color_id TEXT,
    organizer_email TEXT,
    user_can_edit BOOLEAN DEFAULT TRUE,
    user_rsvp_status TEXT,
    attendees_json TEXT,
    conference_data_json TEXT,
    -- Debounce state for conference-link change detection: the candidate
    -- new conferenceId awaiting a second confirming read.  Kept OUT of the
    -- content hash so it never bumps the version.  See client.py
    -- _resolve_conference.
    pending_conference_id TEXT,
    source_html_link TEXT,
    attachments_json TEXT,
    recurrence_rule_json TEXT,
    recurrence_instance_original_start TEXT,

    status TEXT NOT NULL DEFAULT 'active',
    user_intentionally_deleted BOOLEAN DEFAULT FALSE,
    is_recurring BOOLEAN DEFAULT FALSE,

    -- Set when a main-side edit produced a change the source event
    -- has not yet received, so the origin-writeback patch must fire
    -- even on the projection's first appearance (when applied_payload_hash
    -- is still NULL).  Cleared by the outbox once the patch lands.
    origin_writeback_pending BOOLEAN DEFAULT FALSE,

    -- Set when the user cancelled one occurrence of a managed
    -- recurring copy on the main calendar: that single source
    -- occurrence must be destructively deleted on the real source
    -- calendar.  Cleared by the outbox once the delete lands.
    source_delete_pending BOOLEAN DEFAULT FALSE,

    version INTEGER NOT NULL DEFAULT 1,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    cancelled_at TIMESTAMP,
    last_seen_at TIMESTAMP,

    UNIQUE(user_id, canonical_uid)
);

CREATE INDEX IF NOT EXISTS idx_ledger_user_status
    ON ledger_events(user_id, status);
CREATE INDEX IF NOT EXISTS idx_ledger_source
    ON ledger_events(source_type, source_calendar_id, source_event_id);
CREATE INDEX IF NOT EXISTS idx_ledger_recurrence_parent
    ON ledger_events(parent_canonical_uid)
    WHERE parent_canonical_uid IS NOT NULL;


CREATE TABLE IF NOT EXISTS ledger_projections (
    id INTEGER PRIMARY KEY,
    ledger_event_id INTEGER NOT NULL REFERENCES ledger_events(id) ON DELETE CASCADE,

    target_kind TEXT NOT NULL,
    target_calendar_id INTEGER,

    desired_state TEXT NOT NULL,
    desired_payload_hash TEXT,
    desired_ledger_version INTEGER NOT NULL,

    current_state TEXT NOT NULL DEFAULT 'unknown',
    google_event_id TEXT,
    google_etag TEXT,
    applied_payload_hash TEXT,
    applied_ledger_version INTEGER,

    last_attempt_at TIMESTAMP,
    next_attempt_at TIMESTAMP,
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    permanently_failed BOOLEAN DEFAULT FALSE,

    -- Bumped when this projection's deterministic Google id is burned
    -- by a cancelled tombstone; derive_google_event_id folds it in to
    -- produce a fresh, still-deterministic id.
    google_id_generation INTEGER NOT NULL DEFAULT 0,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,

    UNIQUE(ledger_event_id, target_kind, target_calendar_id)
);

CREATE INDEX IF NOT EXISTS idx_proj_diverged
    ON ledger_projections(next_attempt_at)
    WHERE applied_ledger_version IS NULL
       OR applied_ledger_version != desired_ledger_version
       OR current_state = 'errored';
CREATE INDEX IF NOT EXISTS idx_proj_google_event
    ON ledger_projections(google_event_id)
    WHERE google_event_id IS NOT NULL;


CREATE TABLE IF NOT EXISTS outbox_operations (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    projection_id INTEGER NOT NULL REFERENCES ledger_projections(id) ON DELETE CASCADE,

    operation TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    ledger_version_at_enqueue INTEGER NOT NULL,
    -- The projection's desired_payload_hash at the moment this op was
    -- enqueued.  On success the projection records THIS as its
    -- applied_payload_hash — not its (possibly since-changed) current
    -- desired hash — so a desired-state change while the op was in
    -- flight is not silently masked.
    desired_payload_hash TEXT,
    target_google_calendar_id TEXT NOT NULL,
    payload_json TEXT,

    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER DEFAULT 0,
    next_attempt_at TIMESTAMP,
    last_error TEXT,
    last_http_status INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,

    UNIQUE(idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_outbox_due
    ON outbox_operations(user_id, next_attempt_at, status)
    WHERE status IN ('pending', 'in_flight');

-- Drain hot path: _claim_next / _reclaim_stale_operations filter by
-- (user_id, status) equality then a next_attempt_at range, and run every
-- 30s for every due user.  idx_outbox_due puts the range column
-- (next_attempt_at) before status, and its IN-list partial predicate is
-- not matched by the `status = 'pending'` + `next_attempt_at IS NULL OR
-- <= ?` claim query, so the claim falls back to a full table SCAN (175K
-- rows when the table is backlogged).  Equality columns first fixes it.
CREATE INDEX IF NOT EXISTS idx_outbox_claim
    ON outbox_operations(user_id, status, next_attempt_at);

-- Nightly retention prune (jobs/cleanup.py) deletes settled rows by
-- (status, completed_at); without this it full-scans the whole table
-- under the single write lock.
CREATE INDEX IF NOT EXISTS idx_outbox_settled
    ON outbox_operations(status, completed_at);


CREATE TABLE IF NOT EXISTS reconcile_requests (
    user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    sources_json TEXT,
    enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scheduled_for TIMESTAMP,
    in_flight BOOLEAN DEFAULT FALSE,
    last_run_at TIMESTAMP
);


-- Ledger events awaiting a replan.  APPEND-ONLY: every enqueue is a
-- fresh row with its own autoincrement id, so there is no
-- read-merge-write to race (the old sources_json blob) and no
-- (user, event) primary key to make a re-enqueue an INSERT-OR-IGNORE
-- no-op.  The reconciler reads the rows, plans the distinct events,
-- and deletes ONLY the row ids it read — a row enqueued mid-pass has
-- a higher id, is not in that set, and survives to the next pass.
CREATE TABLE IF NOT EXISTS affected_ledger_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    ledger_event_id INTEGER NOT NULL REFERENCES ledger_events(id) ON DELETE CASCADE,
    enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_affected_user
    ON affected_ledger_events(user_id);


-- Orphan guard.  Hard-deleting a ledger_event
-- whose projection is still 'present' on Google cascades the
-- projection away without deleting the Google event, leaving an
-- orphan.  The correct path is status='cancelled' — the planner
-- drives every projection to 'absent' and the outbox deletes the
-- Google copies; only then may the row be hard-deleted.  This
-- trigger makes the unsafe delete structurally impossible.
--
-- It does NOT fire for FK-cascade deletes (recursive_triggers is
-- off by default), so DELETE FROM users still cleanly removes an
-- entire account.
CREATE TRIGGER IF NOT EXISTS trg_ledger_events_block_orphaning_delete
BEFORE DELETE ON ledger_events
FOR EACH ROW
WHEN EXISTS (
    SELECT 1 FROM ledger_projections
     WHERE ledger_event_id = OLD.id AND current_state = 'present'
)
BEGIN
    SELECT RAISE(ABORT, 'refusing to delete a ledger_event with a live projection: set status=cancelled and let the outbox drain the deletes first');
END;
"""


async def init_ledger_schema(db: aiosqlite.Connection) -> None:
    """Create the ledger tables if they don't already exist.

    Safe to call multiple times.  Does not touch any legacy table
    or any non-ledger setting.
    """
    await db.executescript(LEDGER_SCHEMA)
    await db.commit()

    # Migrations for columns added after a table's initial release.
    # CREATE TABLE IF NOT EXISTS above does not alter an existing
    # table, so each added column needs an ALTER; a "duplicate column"
    # error just means the migration already ran.
    for stmt in (
        "ALTER TABLE outbox_operations ADD COLUMN desired_payload_hash TEXT",
        "ALTER TABLE ledger_projections "
        "ADD COLUMN google_id_generation INTEGER NOT NULL DEFAULT 0",
        # Episode base for the id-generation ceiling: the ceiling
        # compares (generation - floor), and a successful create sets
        # floor = generation.  Routine absent->present toggles each burn
        # one generation by design (delete leaves a tombstone at the old
        # id), so a LIFETIME cap falsely bricked long-lived recurring
        # events; the pathology the ceiling exists for is 50 burned ids
        # within a single convergence episode.
        "ALTER TABLE ledger_projections "
        "ADD COLUMN google_id_generation_floor INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE ledger_events ADD COLUMN start_timezone TEXT",
        "ALTER TABLE ledger_events ADD COLUMN end_timezone TEXT",
        "ALTER TABLE ledger_events "
        "ADD COLUMN origin_writeback_pending BOOLEAN DEFAULT FALSE",
        "ALTER TABLE ledger_events "
        "ADD COLUMN source_delete_pending BOOLEAN DEFAULT FALSE",
        # Link back to the source event, shown in the main copy's
        # description footer ("Original event: …").
        "ALTER TABLE ledger_events ADD COLUMN source_html_link TEXT",
        # Google's cross-calendar event identity for same-meeting dedup.
        "ALTER TABLE ledger_events ADD COLUMN ical_uid TEXT",
        # Debounce candidate for conference-link change detection (a new
        # conferenceId awaiting a second confirming read).  See
        # client.py _resolve_conference.
        "ALTER TABLE ledger_events ADD COLUMN pending_conference_id TEXT",
    ):
        try:
            await db.execute(stmt)
            await db.commit()
        except sqlite3.OperationalError as e:
            # A "duplicate column" error just means this migration
            # already ran.  Anything else is a real failure and must
            # not be swallowed.
            if "duplicate column" not in str(e).lower():
                raise

    # Index for the cross-source dedup sibling lookup.  Created AFTER the
    # ALTER above so it never references ical_uid before that column
    # exists on an upgraded database.
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_ledger_ical_uid "
        "ON ledger_events(user_id, ical_uid) WHERE ical_uid IS NOT NULL"
    )
    await db.commit()

    # One-time backfill for the generation-ceiling episode floor.  The
    # ALTER above defaults google_id_generation_floor to 0, but a
    # database that predates the floor carries generations accumulated
    # under the old lifetime-cap semantics (observed at 3000+ in
    # production from routine absent->present toggles).  Left at
    # floor=0, every such HEALTHY projection would insta-fail the
    # per-episode ceiling (generation - floor >= cap) on its next
    # present toggle — a false permanent failure plus an operator alert
    # each time.  Start those rows on a fresh episode.  Rows already
    # marked permanently_failed are deliberately left for the admin
    # retry action, which resets their floor itself.  Idempotent: after
    # the backfill (and under the new code, always) an unfailed row's
    # gap stays below the cap, so the WHERE never matches again.
    # The literal 50 mirrors outbox._MAX_TOTAL_ID_GENERATIONS (not
    # imported here: schema must stay import-light and the value is
    # frozen into upgraded databases at migration time anyway).
    await db.execute(
        """UPDATE ledger_projections
              SET google_id_generation_floor = google_id_generation
            WHERE google_id_generation - google_id_generation_floor >= 50
              AND permanently_failed = 0"""
    )
    await db.commit()

    # One-time repair: origin writebacks only ever target CLIENT
    # sources (personal calendars are read-only; the planner never
    # creates a writeback projection for them), but an earlier main-
    # ingest path armed the flag regardless of source type.  A flag on
    # a non-client row can never be cleared by a delivered patch, so it
    # sits armed forever — observed in production as personal-source
    # rows pending since May.  Idempotent; a no-op on healthy data.
    await db.execute(
        """UPDATE ledger_events
              SET origin_writeback_pending = 0
            WHERE origin_writeback_pending = 1
              AND source_type != 'client'"""
    )
    await db.commit()

    # affected_ledger_events was first shipped with a
    # (user_id, ledger_event_id) primary key, which made a re-enqueue
    # of the same event an INSERT-OR-IGNORE no-op — a lost update.
    # Rebuild it with the append-only autoincrement-id shape, COPYING
    # any rows already queued: a row here is an event that ingest has
    # not yet planned, and dropping it would strand that change.
    cols = await (await db.execute(
        "PRAGMA table_info(affected_ledger_events)"
    )).fetchall()
    if cols and not any(c[1] == "id" for c in cols):
        # The rebuild is RENAME → CREATE → INSERT → DROP → CREATE INDEX.
        # On the autocommit connection each statement commits on its
        # own, so a crash mid-rebuild could leave the table missing
        # entirely or duplicated.  SQLite supports transactional DDL —
        # wrap the whole rebuild so it either fully applies or not at
        # all, leaving the original table untouched on any failure.
        await db.execute("BEGIN IMMEDIATE")
        try:
            await db.execute(
                "ALTER TABLE affected_ledger_events "
                "RENAME TO affected_ledger_events_old"
            )
            await db.execute(
                """
                CREATE TABLE affected_ledger_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    ledger_event_id INTEGER NOT NULL
                        REFERENCES ledger_events(id) ON DELETE CASCADE,
                    enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await db.execute(
                """INSERT INTO affected_ledger_events
                      (user_id, ledger_event_id, enqueued_at)
                   SELECT user_id, ledger_event_id, enqueued_at
                     FROM affected_ledger_events_old"""
            )
            # DROP last: the RENAME carried idx_affected_user onto the
            # _old table, so creating the index before the drop would
            # collide on the name (CREATE INDEX IF NOT EXISTS no-ops) and
            # the drop would then take the only copy.  Drop first, then
            # create the index fresh on the new table.
            await db.execute("DROP TABLE affected_ledger_events_old")
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_affected_user "
                "ON affected_ledger_events(user_id)"
            )
            await db.execute("COMMIT")
        except BaseException:
            await db.execute("ROLLBACK")
            raise
