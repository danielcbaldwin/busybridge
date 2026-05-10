"""Schema for the canonical-ledger architecture.

These tables are additive — they sit alongside the legacy
``event_mappings`` / ``busy_blocks`` until the Stage 5 cutover.
The DDL below is the full definition from REWRITE_PLAN.md §4.

Use :func:`init_ledger_schema` against any aiosqlite-compatible
connection.  Safe to call repeatedly (all CREATEs are
``IF NOT EXISTS``).
"""

from __future__ import annotations

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

    summary TEXT,
    description TEXT,
    location TEXT,
    start_at TEXT,
    end_at TEXT,
    is_all_day BOOLEAN DEFAULT FALSE,
    show_as TEXT,
    visibility TEXT,
    color_id TEXT,
    organizer_email TEXT,
    user_can_edit BOOLEAN DEFAULT TRUE,
    user_rsvp_status TEXT,
    attendees_json TEXT,
    conference_data_json TEXT,
    attachments_json TEXT,
    recurrence_rule_json TEXT,
    recurrence_instance_original_start TEXT,

    status TEXT NOT NULL DEFAULT 'active',
    user_intentionally_deleted BOOLEAN DEFAULT FALSE,
    is_recurring BOOLEAN DEFAULT FALSE,

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


CREATE TABLE IF NOT EXISTS reconcile_requests (
    user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    sources_json TEXT,
    enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scheduled_for TIMESTAMP,
    in_flight BOOLEAN DEFAULT FALSE,
    last_run_at TIMESTAMP
);
"""


async def init_ledger_schema(db: aiosqlite.Connection) -> None:
    """Create the ledger tables if they don't already exist.

    Safe to call multiple times.  Does not touch any legacy table
    or any non-ledger setting.
    """
    await db.executescript(LEDGER_SCHEMA)
    await db.commit()
