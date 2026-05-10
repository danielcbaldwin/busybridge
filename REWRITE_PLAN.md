# BusyBridge Rewrite Plan: Canonical Ledger Architecture

> **Status:** Draft for review — no architectural code changes yet.
> **Branch:** `claude/improve-reliability-hKbKZ`
> **Companion document:** [`FEATURE_INVENTORY.md`](./FEATURE_INVENTORY.md) — ~330 item acceptance checklist.
> **Hard requirement:** zero feature loss. Every checkbox in the inventory must work identically (or better) post-rewrite.

---

## Executive Summary

Calendar synchronization is, at heart, a problem about *who has the authority to say what is true.*

**Today**, BusyBridge functions like a translator at a meeting where everyone is talking at once. Each calendar — your main one, your client calendars, your personal Gmail, any conference subscriptions — has its own ongoing record of "what's happening to you." BusyBridge listens to all of these conversations simultaneously and tries to keep them informed about each other. There is no umpire. There is no single authority that can say, definitively, "this is what is on your schedule."

The result is a confederation of equal calendars with no federal record-keeper. When two calendars disagree, there is no higher authority to resolve it. Duplicate events happen because two listening processes can hear the same change and both act. Missing events happen because a process can complete its "read what changed" step before successfully writing all the consequences. Reliability mechanisms patched on top (per-calendar locks, hourly consistency checks, orphan scans, verification re-fetches) cannot fix this; the architecture is the problem.

**The rewrite introduces a registrar.** BusyBridge becomes the central, authoritative record of your schedule. Inside its database lives one canonical entry per event (the *ledger*), regardless of which calendar originated it. Each Google calendar — including your main one — becomes a *view* of that record, not a participant in deciding what's true. The relationship is one-way: changes flow into the record first, then the record is rendered out to the views.

Three structural properties make this fix reliability across the board, not just patch over symptoms:

1. **Single point of truth.** "What should be on your calendar?" becomes a single database query with a single answer. Any divergence from that answer is wrong by definition. Detection and correction become mechanical rather than heuristic.
2. **Deterministic operation identities.** Every instruction sent to Google carries a unique fingerprint we choose ourselves. If the same instruction goes to Google twice (network retry, process restart, anything), Google recognises the fingerprint and refuses the second attempt. Duplicates from retries become structurally impossible.
3. **One author at a time per user.** Today's three concurrent sync paths (webhook, periodic, verification re-fetch) feed into a single queue per user, drained by one worker in order. Two reconciliations cannot be in flight at the same time for the same person. The race conditions that produce most current duplicates become impossible.

These compose: each closes a different class of bug; together they make today's main failure modes structurally impossible rather than merely mitigated.

**The migration is clean-cut**, not phased. Build the complete new system in isolation on this branch; validate exhaustively via soak tests against a faithful fake Google; observe for a week against real test-Google accounts; then cut over production via the existing "Cleanup & Pause" feature — which already produces a known-clean state every time it runs. This trades the phased-migration approach's complexity (dual-write, shadow ledger, gated cutover) for a much simpler implementation path. The transition is briefly visible (~5–15 minutes during which calendars look empty before the new system populates them from scratch), but the failure mode is "run cleanup-and-resync again," not data loss. About 30–50% less total work than a phased migration, and a cleaner end-state codebase with no legacy compatibility carryover.

**Honest caveats:**
- Some genuinely hard problems remain — Google's eventual-consistency lag, recurring-event semantics, ill-behaved external feeds. The new architecture isolates these to the ingest layer; it does not make them disappear.
- The clean-cut approach trades validation-against-production for simplicity. We rely on the soak harness plus a real-Google validation week to give us confidence; if the soak misses something subtle, it surfaces in production rather than in shadow mode. The compensating advantage is much less code and a faster timeline.
- A meaningful chunk of the work (the soak-test harness) is investment in test infrastructure, not new product features. This is deliberate: today's test suite has 98% coverage and 171 passing tests but cannot see the bugs that hurt users. We will not ship the rewrite until tests can.

The rest of this document is the engineering plan. §1–§3 describe goals and architecture. §4–§12 describe the new system in detail. §13 lays out the five clean-cut migration stages. §14 describes the test strategy including the soak-test harness. §15–§17 map every feature in the inventory to its new home and call out risks.

---

## 1. Goals & Non-Goals

### Goals
- Eliminate duplicate events caused by concurrent webhook/periodic/verification syncs.
- Eliminate missed events caused by sync-token advancement past partial failures.
- Eliminate orphans caused by Google-write-succeeded / DB-commit-failed crashes.
- Make every Google API operation **idempotent** and **etag-gated**.
- Replace string-prefix and extended-property heuristics for "is this our event?" with a precise lookup.
- Reduce reconciler logic from ~10 entangled files to a single planner + outbox drainer.
- Make adding a new event source (Apple, Outlook, Notion, Linear) a 1-day task instead of a 1-week task.
- **Remove service-account (sa_tier) mode.** Research showed it does not reliably deliver immovable events on the user's own calendar — calendar ownership trumps event-level `guestsCanModify=false`. The 🔒 emoji + revert-on-drift mechanism (already used for sa_tier=0, personal, webcal) becomes the uniform approach for non-editable events. SA code is simply not carried over into the new system.

### Non-Goals (this rewrite)
- No change to the user-visible UI or URL routes.
- No change to OAuth flow, OOBE, or admin features.
- No change to backup/restore format.
- No support for multi-org or shared calendars.
- No change from SQLite to a server database (still single-process).
- No change to the deployment story (single Docker container).

---

## 2. Why the Current Architecture Fails

The current system is a **state-replication** model: each Google calendar has an independent timeline (sync token), and `event_mappings` is a join table recording "I think main_event=X corresponds to client_event=Y." Every reliability mechanism in the codebase (per-calendar locks, verification re-fetch, orphan scanner, consistency checker, retry-missing-busy-blocks, instance re-keying) is a patch on top of this model. The patches are good; the model is the problem:

- **No atomic boundary** between Google writes and DB writes. A crash between `events.insert()` (`rules.py:326`) and the subsequent DB INSERT (`rules.py:343`) leaves Google with a copy and the DB without one. The next sync re-creates it.
- **Three concurrent sync triggers** (webhook, periodic, verification) can each see the same eventually-consistent Google snapshot. Per-calendar locks serialise *processing*, not *observation* — two consecutive lock holders can both observe and act on stale data.
- **Google's `events.insert()` is not idempotent.** A retry creates a second event.
- **Sync tokens advance too eagerly** — token moves on after the per-event loop, even if individual busy-block writes failed.
- **Identity has four loosely-coupled layers** (`bb_origin_id`, `bb_mapping_id`, `event_mappings` row, `busy_blocks` row, plus a string-prefix heuristic). They drift.

---

## 3. The New Architecture

Three concepts replace the current model:

```
┌─────────────────────────────────────────────────────────────────┐
│  LEDGER (canonical source of truth)                             │
│  • One row per logical event the user has on their schedule     │
│  • Indexed by stable canonical_uid                              │
│  • Stores source attribution + content + lifecycle state        │
└─────────────────────────────────────────────────────────────────┘
                              ↓ planner
┌─────────────────────────────────────────────────────────────────┐
│  PROJECTIONS (desired state per target calendar)                │
│  • One row per (ledger_event, target_calendar)                  │
│  • desired_state: present_full / present_busy / absent          │
│  • current_state: what's actually on Google                     │
│  • applied_payload_hash: drift detection                        │
└─────────────────────────────────────────────────────────────────┘
                              ↓ diff
┌─────────────────────────────────────────────────────────────────┐
│  OUTBOX (queued writes to Google)                               │
│  • One row per pending operation                                │
│  • idempotency_key = deterministic Google event ID              │
│  • Drained by per-user worker; etag-gated                       │
└─────────────────────────────────────────────────────────────────┘
```

The reconciler is a five-step loop with no ambiguity: **ingest → upsert ledger → plan projections → diff → enqueue outbox.** A separate worker drains the outbox.

### Key invariants (enforced by schema and transactions)

1. **Ledger rows are append-mostly with monotonic `version`.** Every material change bumps the version.
2. **Projections are unique on `(ledger_event_id, target_kind, target_calendar_id)`.** No room for duplicate intents.
3. **Outbox rows are unique on `idempotency_key`.** A retry of the same logical operation produces the same key, hits the unique index, and is deduplicated.
4. **Sync tokens advance only inside the same DB transaction that wrote the ledger upserts and outbox rows.** No partial-failure window.
5. **Google writes use `id=idempotency_key` (client-supplied event ID) for inserts and `If-Match: <etag>` for updates.** A retry of an in-flight operation either succeeds or returns 409/412, both recoverable.

### Identity model

A single canonical identifier per ledger row. **No more four-layer cross-checking.**

| Source | `canonical_uid` format |
|---|---|
| Main calendar (native event, no source elsewhere) | `main_native:{user_id}:{google_event_id}` |
| Client calendar | `client:{client_calendar_id}:{google_event_id}` |
| Personal calendar | `personal:{personal_calendar_id}:{google_event_id}` |
| Webcal with stable UID | `webcal:{subscription_id}:{ics_uid}` |
| Webcal with unstable UID | `webcal:{subscription_id}:hash:{sha256_of_(start, end, normalized_summary)}` |

Recurring instances inherit the parent's canonical_uid prefix; a per-instance suffix `:inst:{originalStartTime}` is appended for modified-instance ledger rows.

"Is this Google event one of ours?" is now a single lookup: does `ledger_projections.google_event_id = X` exist for this user? Yes → ours, skip. No → new event from a source. The `extendedProperties` tags become defence-in-depth, not the primary mechanism.

---

## 4. Schema

### New tables (created on the new branch)

```sql
-- Canonical ledger: one row per logical event in the user's life.
CREATE TABLE ledger_events (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    -- Identity (canonical, stable across syncs)
    canonical_uid TEXT NOT NULL,
    parent_canonical_uid TEXT,                    -- For modified instances; NULL otherwise.

    -- Source attribution
    source_type TEXT NOT NULL,                    -- 'main_native', 'client', 'personal', 'webcal'
    source_calendar_id INTEGER,                   -- FK to client_calendars or webcal_subscriptions; NULL for main_native
    source_event_id TEXT,                         -- Google/ICS event ID at source
    source_etag TEXT,                             -- For optimistic concurrency on ingest
    source_updated_at TIMESTAMP,                  -- last-modified at source

    -- Event content (canonical "what should appear on main")
    summary TEXT,
    description TEXT,
    location TEXT,
    start_at TEXT,                                -- ISO8601; either dateTime or date (all-day)
    end_at TEXT,
    is_all_day BOOLEAN DEFAULT FALSE,
    show_as TEXT,                                 -- 'busy' or 'free' (transparency)
    visibility TEXT,
    color_id TEXT,                                -- Google color, if assigned via source calendar
    organizer_email TEXT,
    user_can_edit BOOLEAN DEFAULT TRUE,
    user_rsvp_status TEXT,                        -- User's response if attendee
    attendees_json TEXT,                          -- Full attendee list as JSON
    conference_data_json TEXT,                    -- Meet/Zoom data
    attachments_json TEXT,                        -- File attachments
    recurrence_rule_json TEXT,                    -- RRULE/EXDATE/etc as JSON array
    recurrence_instance_original_start TEXT,     -- For modified-instance rows

    -- Lifecycle
    status TEXT NOT NULL DEFAULT 'active',        -- 'active', 'cancelled'
    user_intentionally_deleted BOOLEAN DEFAULT FALSE,  -- replaces today's soft-delete-prevents-resurrect
    is_recurring BOOLEAN DEFAULT FALSE,

    -- Versioning
    version INTEGER NOT NULL DEFAULT 1,           -- Bumps on each material change

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,
    cancelled_at TIMESTAMP,                       -- Set when status moves to 'cancelled'
    last_seen_at TIMESTAMP,                       -- Last time source ingest saw this event (used for webcal staleness)

    UNIQUE(user_id, canonical_uid)
);

CREATE INDEX idx_ledger_user_status ON ledger_events(user_id, status);
CREATE INDEX idx_ledger_source ON ledger_events(source_type, source_calendar_id, source_event_id);
CREATE INDEX idx_ledger_recurrence_parent ON ledger_events(parent_canonical_uid) WHERE parent_canonical_uid IS NOT NULL;


-- Projections: desired & current state of this event on each target calendar.
CREATE TABLE ledger_projections (
    id INTEGER PRIMARY KEY,
    ledger_event_id INTEGER NOT NULL REFERENCES ledger_events(id) ON DELETE CASCADE,

    -- Target identification
    target_kind TEXT NOT NULL,                    -- 'main' or 'client'
    target_calendar_id INTEGER,                   -- FK to client_calendars when target_kind='client'; NULL when 'main'

    -- Desired state (derived from ledger by the planner)
    desired_state TEXT NOT NULL,                  -- 'present_full', 'present_busy', 'present_personal_busy', 'absent'
    desired_payload_hash TEXT,                    -- sha256 of the rendered payload; drift detection
    desired_ledger_version INTEGER NOT NULL,      -- Ledger version that produced desired_state

    -- Current state (what's actually on Google)
    current_state TEXT NOT NULL DEFAULT 'unknown',  -- 'unknown', 'present', 'absent', 'errored'
    google_event_id TEXT,                         -- Set once written
    google_etag TEXT,                             -- Returned by Google; used for If-Match
    applied_payload_hash TEXT,                    -- The hash of the payload last successfully written
    applied_ledger_version INTEGER,               -- Which ledger version is currently reflected

    -- Reconciliation tracking
    last_attempt_at TIMESTAMP,
    next_attempt_at TIMESTAMP,
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    permanently_failed BOOLEAN DEFAULT FALSE,     -- Poison-pill flag; alerts and stops retrying

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP,

    UNIQUE(ledger_event_id, target_kind, target_calendar_id)
);

CREATE INDEX idx_proj_diverged ON ledger_projections(next_attempt_at)
    WHERE applied_ledger_version IS NULL OR applied_ledger_version != desired_ledger_version OR current_state = 'errored';
CREATE INDEX idx_proj_google_event ON ledger_projections(google_event_id) WHERE google_event_id IS NOT NULL;


-- Outbox: pending writes to Google. Drained by per-user worker.
CREATE TABLE outbox_operations (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    projection_id INTEGER NOT NULL REFERENCES ledger_projections(id) ON DELETE CASCADE,

    operation TEXT NOT NULL,                      -- 'create', 'update', 'delete'
    idempotency_key TEXT NOT NULL,                -- Deterministic; also used as Google's `id` for creates
    ledger_version_at_enqueue INTEGER NOT NULL,
    target_google_calendar_id TEXT NOT NULL,      -- Resolved from target_kind/target_calendar_id at enqueue
    payload_json TEXT,                            -- The exact body to send to Google

    status TEXT NOT NULL DEFAULT 'pending',       -- 'pending', 'in_flight', 'done', 'permanent_failure', 'superseded'
    attempts INTEGER DEFAULT 0,
    next_attempt_at TIMESTAMP,
    last_error TEXT,
    last_http_status INTEGER,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,

    UNIQUE(idempotency_key)
);

CREATE INDEX idx_outbox_due ON outbox_operations(user_id, next_attempt_at, status)
    WHERE status IN ('pending', 'in_flight');


-- Per-user reconcile queue: webhooks/timer enqueue, reconciler drains.
-- Single row per user — multiple enqueues coalesce into the existing row.
CREATE TABLE reconcile_requests (
    user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    sources_json TEXT,                            -- JSON array of source hints, e.g. ["client:7","main"]
    enqueued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scheduled_for TIMESTAMP,                      -- earliest time to start (for debounce)
    in_flight BOOLEAN DEFAULT FALSE,
    last_run_at TIMESTAMP
);
```

### Tables to retire (at production cutover, Stage 5)
- `event_mappings` — superseded by `ledger_events` + `ledger_projections`
- `busy_blocks` — superseded by `ledger_projections` (target_kind='client', desired_state='present_busy')

### Tables unchanged
All other tables (`users`, `oauth_tokens`, `client_calendars`, `webcal_subscriptions`, `calendar_sync_state`, `main_calendar_sync_state`, `webhook_channels`, `alert_queue`, `job_locks`, `oauth_states`, `integrity_status`, `sync_log`, `organization`, `settings`) remain untouched. Sync tokens still live in `calendar_sync_state` / `main_calendar_sync_state`.

---

## 5. Ingest Paths (one per source kind)

Ingest is the "pull from Google → write to ledger" half. It runs inside the per-user reconciler, **always inside a single DB transaction** that includes the sync token update.

### 5.1 Client calendar ingest (OAuth, Google Calendar API)

```
INPUT:  client_calendar_id, current sync_token
1. Fetch incremental events list with sync_token (or full if expired)
2. For each event in the response:
   a. If event.id matches a ledger_projection.google_event_id for this user
      → this is a busy block we wrote, skip.
   b. canonical_uid = f"client:{client_calendar_id}:{event.id}"
   c. Detect _R rescheduled-parent case; if applies, look up ledger row
      by stripped base ID and update canonical_uid to new event.id
      (re-keying logic that today lives in rules.py:82-118).
   d. Compute content fields (summary, start, end, attendees, edit rights,
      RSVP status, recurrence_rule, ...).
   e. UPSERT ledger row by (user_id, canonical_uid). Bump version
      ONLY if a material field changed (use a content hash to detect).
   f. last_seen_at = now()
3. After loop completes, advance sync_token in calendar_sync_state.
4. COMMIT transaction. (If any step raises, sync_token does not advance
   and we re-process on the next run.)
5. After commit: enqueue planner for affected ledger rows.
```

### 5.2 Main calendar ingest (OAuth)

Same shape as client calendar, with these differences:
- Loop prevention: skip events whose `id` matches any `ledger_projection.google_event_id` for this user (these are events we wrote there).
- Native main events (those NOT created by us as projections of a client/personal/webcal source) get `canonical_uid = main_native:{user_id}:{event.id}`, `source_type='main_native'`.
- For events that ARE projections (we recognise them by `google_event_id` lookup): if Google has cancelled them, this means the user deleted from main — set `user_intentionally_deleted=TRUE` on the parent ledger row, bump version.
- For events that ARE projections and we detect a time/RSVP edit on main: depending on source and edit rights, this either propagates back to the source (RSVP) or triggers a revert (move on non-editable). Encoded in the planner — ingest just records the divergence by bumping `desired_ledger_version` ahead of `applied_ledger_version`.

### 5.3 Personal calendar ingest (OAuth, read-only semantics)

Same shape as client calendar but `source_type='personal'`. The planner produces `present_personal_busy` projections on main + all clients (no `present_full` anywhere — privacy).

### 5.4 Webcal/ICS ingest

```
INPUT:  webcal_subscription_id
1. HTTP GET feed URL with If-None-Match: last_etag
   - 304 Not Modified → return early, no work
   - 200 → parse body, store new etag for next poll
2. Parse with recurring_ical_events, expand within window
   (now-30d to now+365d).
3. Detect unstable UIDs (bare UUID v4 that changes per request)
   — same heuristic as ics_parser.py today.
4. For each parsed event:
   a. canonical_uid =
        webcal:{sub_id}:{ics_uid}                     if stable
        webcal:{sub_id}:hash:{sha256(start|end|sum)}  if unstable
   b. UPSERT ledger row. Bump version only if material change.
      IMPORTANT: do NOT re-hash if summary changes — for the unstable
      case, the canonical_uid remains stable as long as (start, end)
      do; the summary update is just a content change. This fixes
      the "Eventbrite renames event → BB creates duplicate" bug.
   c. last_seen_at = now()
5. Reconcile staleness: any ledger row with source_type='webcal',
   subscription_id matches, last_seen_at < (now - 2 * poll_interval),
   status='active' → set status='cancelled'. (Upstream removed it.)
6. Out-of-window events (their start_at is far past or far future)
   stay in ledger but are NOT pruned. The planner will mark their
   projections 'absent' if the window logic dictates, but the
   ledger keeps them so they reactivate naturally if re-included.
7. COMMIT.
```

### 5.5 Discovery ingest (orphan scan)

Runs every 6 hours (preserving today's cadence). Different shape: lists Google's events on every connected calendar and matches against `ledger_projections.google_event_id` and ledger source identities. Anything that has BB extendedProperties but isn't tracked → either ingest as a forgotten projection (re-link) or schedule deletion. Today's prefix-sweep behaviour is preserved by also matching on `MANAGED_EVENT_PREFIX` for legacy events created before extendedProperties were stamped.

---

## 6. Reconciler (Planner + Diff + Outbox Drain)

### 6.1 Planner

Runs after ingest produces ledger changes. For each affected `ledger_event`:

```
DESIRED projections per source_type:

source_type = 'main_native':
  • main: present_full (visible to user; informational)
    — but no projection row needed; the event LIVES on main natively.
  • each client_calendar (active): present_busy

source_type = 'client':
  • main: present_full (with edit-rights metadata + colorId)
  • each OTHER client (active): present_busy
  • origin client: NO projection (the event is native there)

source_type = 'personal':
  • main: present_personal_busy
  • each client (active): present_personal_busy
  • origin personal calendar: NO projection (read-only)

source_type = 'webcal':
  • main: present_full (with [prefix] in title and "Managed by..." footer)
  • each client (active): present_busy

UNIVERSAL OVERRIDES:
  • If ledger.user_intentionally_deleted = TRUE
    → all desired_state = 'absent' (including back to source if applicable)
  • If ledger.status = 'cancelled'
    → all desired_state = 'absent'
  • If ledger.show_as = 'free' AND target=client
    → desired_state = 'absent' (free events don't block)
  • If parent series cancelled and this is an instance row
    → 'absent' for this instance only
```

For each `(ledger_event, target)` pair, the planner:
1. Computes `desired_state` and `desired_payload` from the ledger row.
2. `desired_payload_hash = sha256(desired_payload)`.
3. UPSERTs `ledger_projections` row with new `desired_state`, `desired_payload_hash`, `desired_ledger_version`.

### 6.2 Diff & Enqueue

For each `ledger_projection` where:
- `applied_ledger_version IS NULL` (never written), OR
- `applied_ledger_version != desired_ledger_version` (drift detected at ingest), OR
- `applied_payload_hash != desired_payload_hash` (content drift):

Insert a row into `outbox_operations`:
- `operation` = 'create' | 'update' | 'delete' based on current_state vs desired_state
- `idempotency_key` = `f"proj:{projection_id}:v{desired_ledger_version}:{operation}"` (for `create`, this string also becomes the Google event ID — see §7)
- Mark any existing pending outbox rows for this projection as `superseded` (a newer version is taking over).

### 6.3 Outbox Drain (per-user worker)

```
loop:
    op = SELECT ... FROM outbox_operations
         WHERE user_id = ? AND status='pending' AND next_attempt_at <= now()
         ORDER BY id LIMIT 1
    if not op: sleep(1s); continue

    UPDATE op SET status='in_flight', started_at=now(), attempts=attempts+1

    try:
        if op.operation == 'create':
            payload = op.payload_json
            payload['id'] = op.idempotency_key  # CLIENT-SUPPLIED ID
            payload['extendedProperties.private.bb_proj_id'] = projection_id
            result = google.events.insert(target_calendar, payload)
            # On 409 (id exists): GET event, verify it matches our hash, treat as success.
            UPDATE projection SET current_state='present',
                google_event_id=result.id, google_etag=result.etag,
                applied_payload_hash=desired_payload_hash,
                applied_ledger_version=ledger_version_at_enqueue
            UPDATE op SET status='done', completed_at=now()

        elif op.operation == 'update':
            payload = op.payload_json
            try:
                result = google.events.update(target_cal, projection.google_event_id,
                                              payload, if_match=projection.google_etag)
            except 412 PreconditionFailed:
                # Etag mismatch: someone (or us) changed the event since we read it.
                # Mark this op superseded and re-plan from fresh state.
                UPDATE op SET status='superseded', last_error='etag_mismatch'
                schedule_planner_replan(projection_id)
                continue
            UPDATE projection SET applied_payload_hash=..., applied_ledger_version=...,
                google_etag=result.etag
            UPDATE op SET status='done', completed_at=now()

        elif op.operation == 'delete':
            try:
                google.events.delete(target_cal, projection.google_event_id)
            except 404, 410:
                pass  # already gone is success
            UPDATE projection SET current_state='absent',
                applied_ledger_version=desired_ledger_version
            UPDATE op SET status='done', completed_at=now()

    except RateLimitError:
        backoff = exponential(op.attempts)  # 4s, 8s, 16s, 32s, 60s — same as today
        UPDATE op SET status='pending', next_attempt_at=now()+backoff,
                       last_error=e
    except (5xx, NetworkError):
        backoff = exponential(op.attempts)  # 1s, 2s, 4s, 8s, 16s — same as today
        UPDATE op SET status='pending', next_attempt_at=now()+backoff
    except 4xx (non-retriable):
        if op.attempts >= POISON_PILL_THRESHOLD (default 5):
            UPDATE op SET status='permanent_failure'
            UPDATE projection SET permanently_failed=TRUE, current_state='errored'
            queue_alert("event_sync_poison_pill", projection_id)
        else:
            UPDATE op SET status='pending', next_attempt_at=now()+5min
    except (other):
        UPDATE op SET status='pending', next_attempt_at=now()+30s
```

The drain runs as a single coroutine per active user, started on demand, idle when no pending ops. Backoff and rate-limit semantics match today's `google_calendar.py` exactly.

---

## 7. Idempotency Scheme

### Deterministic Google event IDs
Google accepts a client-supplied `id` on `events.insert()`. Allowed alphabet: lowercase `a-v` and `0-9` (base32hex), 5–1024 chars.

```python
def projection_to_google_id(projection_id: int) -> str:
    # Encode int as base32hex lowercase, prefix with 'bb' for sentinel.
    # 64-bit int fits in 13 base32 chars; total 15 chars.
    encoded = base64.b32hexencode(struct.pack(">Q", projection_id)).decode().lower().rstrip("=")
    return f"bb{encoded}"
```

A retry of `events.insert(id=X)` with the same `X`:
- Returns the existing event (HTTP 200 with the stored event), OR
- Returns 409 Conflict (depending on Google's mood — both are observed).

Either way is recoverable: GET the event, verify hash, mark done.

### Outbox idempotency_key
Same string serves as both the unique constraint on outbox_operations and the Google event ID for create operations. For update/delete, the key is informational only (Google operations are addressed by event ID, not the key).

### Etag-gated updates
Every `events.update()` sends `If-Match: <stored_etag>`. On 412, the operation is marked superseded and the planner re-plans from a fresh GET. **This eliminates the "verification re-fetch overwrites fresher data" bug today** (where a 40s-delayed re-sync clobbers an in-flight RSVP).

---

## 8. Recurring Events

The hardest area. Today's code has multiple paths (re-key parent, cancel instance, propagate cancelled instances, derive instance event ID). The new model:

### Series ledger row
- One ledger_events row per recurring series (parent), `is_recurring=true`, `recurrence_rule_json` populated.
- Projections: one per target calendar; the projection's payload includes the RRULE, and Google generates the per-instance events automatically.

### Modified-instance ledger row
- A second ledger_events row, `parent_canonical_uid` set to series's canonical_uid, `recurrence_instance_original_start` set.
- Projections: one per target calendar with `desired_state='present_full'` (or `present_busy`), `applied to` Google as a single-instance modification. The Google event ID for the instance is derived from `parent_google_event_id + originalStartTime` — see `derive_instance_event_id` in current code, preserved as a helper.

### Cancelled-instance ledger row
- Same shape as modified-instance, but `status='cancelled'`. Planner produces `desired_state='absent'` for all projections, which the outbox translates to a delete on the derived instance ID.

### Rescheduled "this-and-following" series (`_R` suffix)
- Ingest detects new event with `_R` suffix and existing ledger row at the base ID (today's logic at `rules.py:82-118`).
- Updates the ledger row's `source_event_id` to the new ID.
- **No projection re-keying needed** — projections are keyed by ledger_event_id, not by source ID.
- Old instance ledger rows get their parent_canonical_uid updated, OR get cancelled if they're now duplicates of the new series's auto-generated instances.

### Instance ID derivation
Preserved as `derive_instance_event_id(parent_google_event_id, original_start_time)`. Used during outbox drain when the operation targets a single instance of a recurring projection.

---

## 9. Special Modes

SA mode is not implemented in the new system. The remaining modes are:

### Projection rendering
```
when target_kind='main' and source_type='client':
  payload writer = user_token (always)
  if user_can_edit:
    summary = source_summary
    attendees = [{email: user_email, responseStatus: stored_rsvp}]
  else:
    summary = "🔒 " + source_summary
    attendees = [{email: user_email, responseStatus: stored_rsvp}]
    # Revert-on-drift handles physical immovability post-hoc.
```

A single rendering path on main. The 🔒 emoji is informational; the **revert-on-drift mechanism (below) is what actually keeps non-editable events in place.**

### Edit-on-main → propagate to client (RSVP, time if editable)
Today's logic in `rules.py:1050-1099`. Under the new model, this is detected at main-calendar **ingest**: the event we observe on Google for a client-origin projection differs from `applied_payload_hash`, AND the difference is in fields the user is allowed to change (RSVP, time if `user_can_edit`). The reconciler:
1. Updates the ledger row's `user_rsvp_status` (or `start_at`/`end_at`).
2. Bumps version.
3. Planner now wants the SOURCE projection (back on the client calendar) to reflect this change.

So: source-client gets a "phantom projection" row of `target_kind='client', target_calendar_id=origin, desired_state='present_full_rsvp_only'`. The outbox writes the RSVP back to Google. Same observable behaviour as today, with stronger atomicity.

### Edit-on-main when user can NOT edit (revert-on-drift)
Detection path: ingest sees the Google event for a non-editable projection has start/end different from `ledger.start_at`/`end_at`. The planner's `desired_payload_hash` no longer matches what's on Google. The outbox enqueues an `update` to restore the ledger's authoritative state. **Same observable behaviour as today's `_revert_if_moved` for personal/webcal events, now uniformly applied to client-event copies on main as well.**

This **fixes a silent bug in the current code:** today, non-editable client copies on main have *no* revert mechanism (the original author assumed SA mode handled it; SA mode actually doesn't). The new system closes the gap by applying the revert mechanism uniformly to all non-editable events on all targets.

### user_intentionally_deleted
When ingest detects the user deleted a synced event from main:
1. Set `ledger.user_intentionally_deleted=TRUE`, bump version.
2. Planner sets desired_state='absent' for ALL projections, including back to source (matching today's "delete on main → decline on client").
3. The flag persists across re-ingests of the source — even if the client calendar still shows the event, the ledger refuses to reactivate it.
4. If the user wants it back, they explicitly create it on main (new ledger row).

This replaces today's `event_mappings.deleted_at` soft-delete pattern.

### Color coding
`client_calendars.color_id` is read by the planner. When source=client and target=main, payload's `colorId` is set. Color change on a client calendar → bump version on every ledger_event sourced from that calendar → projections re-render.

---

## 10. Cleanup, Disconnect, and Pause

All implementable as ledger operations, no new code paths:

| User action | Ledger operation |
|---|---|
| Cleanup & re-sync (one calendar) | All projections targeting that calendar → `desired_state='absent'`. Ledger rows sourced from that calendar → `status='cancelled'`. Sync token cleared. Outbox drains; full re-fetch repopulates. |
| Cleanup & pause (global) | All projections for user → `desired_state='absent'`. `users.sync_paused=TRUE`. Outbox drains. |
| Disconnect calendar | Same as cleanup-one-calendar + delete `client_calendars` row + revoke OAuth token. |
| Full re-sync | Clear sync tokens. Next reconcile re-fetches everything; ledger upserts dedupe; no actual writes happen unless content changed. |
| Pause (global) | `users.sync_paused=TRUE` (or `settings.sync_paused`). Reconciler skips this user. |

The two-pass cleanup (DB-driven + prefix sweep) is replaced by the orphan scan, which is now precise: it finds events with BB extended properties whose `bb_proj_id` doesn't appear in our projections table. Prefix matching is kept as a fallback for legacy events.

---

## 11. Triggers (Webhook, Periodic, Manual)

All triggers funnel into the `reconcile_requests` table. Instead of N concurrent sync paths, one per-user reconciler drains:

```
WEBHOOK arrives:
    UPSERT reconcile_requests SET
        sources_json = (existing list) UNION {hint},
        scheduled_for = max(scheduled_for, now()+5s)  -- debounce
    WHERE user_id = X
    notify_reconciler(user_id=X)

PERIODIC TIMER (every 5 min):
    for each active user:
        UPSERT reconcile_requests with sources={"all"}, scheduled_for=now()
        notify_reconciler(user_id)

MANUAL SYNC (one calendar):
    UPSERT reconcile_requests with sources={f"client:{cal_id}"},
        scheduled_for=now()+25s  -- preserves today's settling delay
    notify_reconciler(user_id)
    return {progress_url: ...}  -- progress polled from reconcile_requests state
```

The reconciler:
```
RECONCILER (one coroutine per user):
    while True:
        req = SELECT * FROM reconcile_requests
              WHERE user_id=? AND in_flight=FALSE AND scheduled_for<=now()
        if not req: wait_for_notify_or_periodic_check()
        UPDATE req SET in_flight=TRUE, sources_json=NULL, last_run_at=now()
        try:
            for source in req.sources:
                ingest(source)
            plan_affected_ledger_events()
            diff_and_enqueue()
            # outbox drain runs as separate worker; not blocking
        finally:
            UPDATE req SET in_flight=FALSE
```

The 40-second verification re-fetch is **no longer needed** because:
- Etag-gated updates (412 → re-plan) catch stale reads at write time.
- Idempotent inserts (409 → reconcile) catch duplicates at write time.
- The reconciler is the single writer per user, so there's no concurrent observer to corrupt state.

If real-world Google consistency lag turns out to bite us anyway, we can re-add a "delayed second reconcile pass" by simply enqueuing a second `reconcile_request` 40s after the first. No new code.

---

## 12. Failure Handling

All today's failure handling is preserved or strengthened:

| Behaviour today | Behaviour after |
|---|---|
| Per-calendar `consecutive_failures` counter | Same. Incremented when ingest raises; reset on success. |
| Circuit breaker pauses all users at 3+ failures across all calendars | Same. Logic moves from `sync_job.py:97-148` to a periodic check on `calendar_sync_state`. |
| 5+ consecutive failures → email alert | Same. |
| Token revocation (invalid_grant) → alert + disable calendar | Same; detection happens in ingest. |
| Sync token preservation on failure | **Stronger**: token only advances inside the same DB transaction as ledger upserts, so partial failures cannot lose events. |
| Missed busy block retry (`_retry_missing_busy_blocks`) | **Replaced**: any projection where `applied_ledger_version != desired_ledger_version` is automatically retried by the outbox. No special "retry missing" code needed. |
| Poison-pill events freezing a calendar | **Fixed**: per-projection `permanently_failed` flag advances past bad events and alerts. |

---

## 13. Migration Plan (Clean-Cut)

The migration is a single coordinated cutover, not a phased one. Build the complete new system in isolation on this branch, validate exhaustively, then swap over production via the existing "Cleanup & Pause" feature.

**Why clean-cut fits this project:**

- One user (you), so the coordination cost of "calendars look briefly empty during cutover" is small.
- The "Cleanup & Pause" feature already exists and is well-tested: it deletes every BusyBridge-managed event from every calendar, clears sync tokens, and pauses sync. After it runs, the system is in a known-clean state by construction.
- The new system is idempotent and self-recovering. Once it starts on a clean slate it converges to correct state without intervention.
- The soak harness gives strong correctness confidence before any real-Google contact.
- Skipping phased-migration scaffolding (dual-write, shadow ledger, flag-gated cutover) saves ~30–50% of total work, and the end-state codebase has no legacy compatibility carryover.

The trade-off is concentrated risk at the cutover moment. We mitigate that with a faithful fake Google + extensive soak tests + a 7-day real-Google validation period before touching production.

**Stages and order:**

### Stage 1 — Test infrastructure (5–8 days)

Build before any rewrite work begins. Without this we'd be developing the new system blind.

- **Fake Google Calendar API** with faithful semantics: in-memory event store; client-supplied `id` parameter with conflict semantics; ETag support with `If-Match` (412 on mismatch); incremental sync tokens with realistic expiry; recurring-event instance derivation; the `_R` reschedule quirk; **specifically reproduce the bug that full-sync responses omit cancelled instances of recurring series** (this is the source of one of the recurring-cancellation problems and must be modelled).
- **Simulated clock** (1 sim-day per real second; settable).
- **Failure injection knobs**: configurable rate of network errors, rate limits, 5xx errors, sync-token expiry, process crashes between Google call and DB commit.
- **Basic integration test infrastructure**: a small framework that lets us write tests of the shape "given calendar state X, run sequence Y, assert state Z" against the fake Google.

**Deliverable:** a test harness sufficient to validate ledger code as we build it. Lives under `tests/fakes/` and `tests/integration/`. The fake Google is also what the soak harness (Stage 3) sits on top of.

### Stage 2 — Build the complete new system (20–30 days)

Implement everything described in §3–§12 of this document. The old code is removed as new modules go in; no dual-write, no compatibility shims, no flag-gated cutover.

**Scope (all naturally subsumed by the new architecture, no separate Phase-0-style patches needed):**

- New tables: `ledger_events`, `ledger_projections`, `outbox_operations`, `reconcile_requests`.
- The five ingest paths (client OAuth, main OAuth, personal OAuth, webcal/ICS, discovery/orphan-scan).
- The reconciler (planner + diff + outbox drain).
- Idempotency scheme (deterministic Google IDs, etag-gated updates).
- Recurring-event handling (parent and instance ledger rows; cancellations as first-class sticky rows that survive sync-token expiry — closes today's recurring-cancellation pain).
- Revert-on-drift for all non-editable events, uniform across sources (closes today's silent gap on non-editable client copies on main).
- Cleanup, disconnect, and pause as ledger operations.
- Per-user reconciler queue replacing per-calendar locks (closes today's concurrent-sync duplicate class).
- Outbox drain worker.
- All UI / API surfaces ported to read from the ledger.
- Service-account mode is not implemented; `sa_tier` column is dropped. The 🔒 emoji + revert mechanism handles all non-editable events uniformly.

**Old code is deleted as we go.** `app/sync/engine.py`, `app/sync/rules.py`, `app/sync/consistency.py` all shrink dramatically; what remains is mostly pure payload-rendering helpers. `app/auth/service_account.py` is deleted. OOBE wizard goes from 7 steps to 6.

**Tests during development:** every new component has unit tests; the fake Google enables full pipeline integration tests; a continuously-running mini-soak (90 simulated days) runs in CI to catch regressions as the system grows.

### Stage 3 — Soak harness (5–7 days; can overlap end of Stage 2)

Build the heavyweight long-running validation layer on top of the Stage 1 fake Google.

- **Simulated user persona** with realistic event patterns (mix of single-instance, recurring weekly, recurring-with-cancellations, rescheduled series, manual edits, RSVP changes, calendar disconnect/reconnect).
- **Ground-truth oracle**: the harness maintains its own model of what events should exist where, updated as the simulated user takes actions.
- **Invariant checker** (the 11 assertions in §14).
- **Targeted recurring-cancellation soak** (365 sim days; today's system would fail this in a week).
- **Adversarial scenarios**: crash mid-write recovery, rapid back-and-forth edits, sync-token expiry races, calendar permission revocation.
- **Reproducibility**: random seeds, automatic shrinking of failed traces, captured-failure regression tests.

**Release gate to advance to Stage 4:** soak harness runs 90 simulated days on three different random seeds with full failure injection enabled, all invariants stay green throughout.

### Stage 4 — Real-Google validation (7 calendar days)

Deploy the new code to a development environment pointing at your test Google Workspace accounts. Run for one real week.

- Create realistic event patterns by hand: recurring meetings with cancellations, rescheduled series, webcal subscriptions, manual edits on main, bursts of new events.
- Observe convergence after each operation.
- Specifically exercise the recurring-cancellation scenarios that hurt today.
- Watch for any drift or unexpected behaviour.

**Why this stage exists:** soak tests give confidence that the architecture is correct under simulated load. They cannot give confidence that the fake Google is fully faithful to real Google. Real Google has quirks (delivery timing, webhook semantics, eventual consistency exact behaviour) that we won't anticipate. One week of real-world use against test accounts surfaces any gap before it hits production.

**Release gate to advance to Stage 5:** 7 days with zero unresolved divergences between observed state and expected state.

### Stage 5 — Production cutover (1 day)

Choose a quiet time (weekend morning is ideal).

1. **Take a careful backup** of the production database and encryption key, copied off-server (procedure described in deployment guide).
2. **On production, run "Cleanup & Pause"** from the admin UI. Every BusyBridge-managed event is deleted from every calendar; sync tokens are cleared; sync is paused. The system is now in a known-clean state.
3. **Stop the container.**
4. **Git fetch and checkout the new branch.** Schema migration runs automatically on next startup (additive only — new tables created, existing tables untouched).
5. **Start the container** with the new code. Watch logs for clean startup.
6. **Resume sync** from the admin UI. The new system populates from scratch — every calendar's events are re-ingested, every projection is computed, every busy block is created idempotently.
7. **Observe for the first few hours.** Run a manual sync, check the activity feed, verify no errors accumulate.

**Total downtime visible to free/busy consumers:** ~5–15 minutes between cleanup and the new system catching up. Schedule for a low-traffic window.

**Rollback procedure** (in case anything goes catastrophically wrong post-cutover):

1. Stop container.
2. `git checkout` the previous main branch.
3. Restore production database from the pre-cutover backup.
4. Restart container.
5. Run "Cleanup & Pause" again, then resume sync. Old code re-populates from scratch.

The "Cleanup & Resync" effect is recoverable. The data is recoverable from backup. Worst case: spend a Saturday morning recovering. No permanent damage possible.

### What is NOT in scope for the rewrite

For completeness, what's deliberately deferred to follow-up work:

- OAuth scope reduction (from full `calendar` to `calendar.events` where possible).
- Multi-organization support.
- Migration from SQLite to a server database.
- Horizontal scaling beyond single-Docker-container.
- Apple/Outlook/Notion calendar sources (the architecture supports them; we just don't build them now).


---

## 14. Test Strategy

The current test suite has 171 passing tests at 98% line coverage but ANALYSIS.md correctly notes the suite can't see concurrency, can't see what was actually written to Google, and never runs the full pipeline end-to-end. The rewrite ships with new test infrastructure first.

A system like this cannot be tested with unit tests alone. The bugs that hurt users aren't local — they're emergent properties of three concurrent processes interacting with an eventually-consistent external service. The strategy is **five layers**, each catching a class of failure the others cannot:

### Layer 1 — Unit tests
For pure functions only: canonical UID generation, payload rendering, ICS parsing, hash computation. Fast and exhaustive. Catches logic mistakes inside individual transformations.

### Layer 2 — Property tests (Hypothesis)
For invariants that should hold over large random inputs. Examples:
- Ingesting the same event twice produces the same ledger state, regardless of order.
- Rendering a payload from a ledger row and then ingesting that payload back produces the same ledger row (round-trip property).
- For any sequence of operations, replaying the operations against an empty system produces the same final state (idempotency property).
- For any RRULE + EXDATE + modified-instance set, the expanded instance set matches the expected count after each operation.

Catches logic mistakes that unit tests miss because the unit tester only thought of three cases.

### Layer 3 — Integration tests
Full pipeline scenarios run against the fake Google. Each test is a story with a starting state, a sequence of events, and an assertion about the final observable state. Examples:
- "User creates event on client A; assert that within N reconciliation cycles, a full-detail copy exists on main and busy blocks exist on B and C."
- "User cancels one instance of a recurring meeting; assert that the cancelled instance is absent from every busy block within N cycles."
- "Webhook arrives → reconcile_request enqueued → ingest runs → planner runs → outbox drains → fake Google reflects expected state → next ingest sees no drift."

Asserts on the actual payload sent to Google, not just the return value. Catches wiring mistakes between components.

### Layer 4 — Concurrency / chaos tests
Deliberately adversarial timing. Examples:
- Two webhooks arrive simultaneously for the same calendar.
- The periodic timer fires during a webhook sync.
- The network drops a response mid-write.
- The process crashes between a Google call and a database commit (kill -9 the simulated worker).
- Two simulated users perform overlapping operations.

These catch the race conditions that produce most current duplicates. Tests assert that any interleaving produces the same final state.

### Layer 5 — Soak tests (the hardest class, the most valuable)

This layer is what catches the bugs the user currently lives with: slow drift, accumulating ghosts, sync-token-expiry effects, memory leaks, compounding failures, and the recurring-cancellation regressions that need *time* to manifest. The harness is described in detail in Stage 3 (§13); this section summarises the contract.

**What soak tests check that lower layers cannot:**

- **Slow accumulation.** A 0.1% per-event duplicate rate is invisible in a 100-event integration test. It produces ten duplicates a year for a real user. A 90-sim-day soak with several thousand events makes it obvious.
- **Long-cycle bugs.** Anything triggered by sync-token expiry (monthly), retention cleanup (daily), webhook channel renewal (every 6 hours), or month-boundary recurrence. Tests that don't simulate months cannot see these. The recurring-cancellation amnesia is exactly this kind of bug.
- **Compounding failures.** Bugs where a small initial error produces state that makes the next error more likely. These have an exponential signature in the divergence time-series; a soak test sees the curve curl upward over simulated days.
- **Resource leaks.** Ledger or outbox tables that grow without bound; reconciler memory that doesn't release.

**Invariants checked after every soak reconciliation cycle:**

1. For every active ledger event, projections exist on exactly the expected calendars (no missing, no spurious).
2. For every projection with `current_state='present'`, the corresponding event exists on Google.
3. For every Google event carrying our extended properties, a corresponding projection exists (no orphans on Google's side).
4. No two projections share a Google event ID, ever.
5. No two ledger events share a canonical UID.
6. Count of full-detail copies on main matches the oracle (ground-truth model).
7. Count of busy blocks on each client matches the oracle, excluding self-origin events.
8. Outbox queue eventually drains to zero between event bursts.
9. Reconcile latency stays bounded as event count grows (no exponential blowup).
10. Database size grows sub-linearly in event count (no unbounded growth).
11. After any failure injection, the system reaches a clean state within a bounded number of cycles.

**Targeted recurring-cancellation soak** (specific to the user's reported pain):

Generate 100 weekly recurring meetings. Schedule cancellations at varied positions: some on the next instance, some mid-series, some after a `_R` reschedule has moved the parent. Force sync-token expiry partway through. Run 365 simulated days. Assert after every cycle that no ghost instances exist on any client calendar. Today's system fails this within a simulated week; the goal post-rewrite is zero failures across all seeds.

**Release gates:**
- No stage advances without a green 90-simulated-day run.
- Stage 4 (real-Google validation) requires a green 365-simulated-day run with all failure injection enabled.
- A failed soak seed is automatically saved as a regression test and added to CI permanently.

### Existing tests
The current test suite (`tests/`, `e2e/`, `sidecar/tests/`) depends heavily on internals (`event_mappings`, `busy_blocks`, the old engine functions). Since the new system replaces these wholesale, the test suite is also replaced — old tests are deleted alongside the old code. The new test suite (built in Stage 1) covers the new system's behaviour, with the soak harness providing the long-running validation that the old suite couldn't.

---

## 15. Feature Preservation Matrix

For each category in [`FEATURE_INVENTORY.md`](./FEATURE_INVENTORY.md), where the behaviour lives in the new system. **Every checklist item is preserved.**

| Inventory Category | Where in new system | Notes |
|---|---|---|
| 1. OAuth & Accounts | Unchanged for OAuth/sessions/OOBE. **SA mode not carried over** (was unreliable; see §9). OOBE goes from 7 to 6 steps. | Ledger doesn't affect login flows or factory reset. |
| 2. Calendar Connections | Unchanged at the API layer; storage tables (`client_calendars`, `webcal_subscriptions`) unchanged. | Disconnect implemented as ledger op (§10). |
| 3. Sync Behaviours | New: §5 ingest + §6 planner + §6 outbox drain. Replaces `app/sync/rules.py`. | All specific rules (busy block creation, RSVP, edit rights, recurring, etc.) preserved as planner logic + payload renderers. **SA-mode immovability replaced by uniform 🔒+revert.** See §8, §9, §10. |
| 4. Triggers | New: §11. `reconcile_requests` table. | Debounce/settling delays preserved. Verification re-fetch retired (§11) — its purpose is met by etag-gated updates. |
| 5. Background Jobs | Schedule unchanged. Periodic sync internals replaced. | `app/jobs/scheduler.py` unchanged. Job bodies migrate to enqueuing reconcile_requests. |
| 6. Failure Handling | §12. Strengthened. | Sync-token-preservation is stronger; poison-pill is new. |
| 7. Admin Features | Unchanged except **SA admin endpoints removed** (`/api/admin/service-account` etc.). | `app/api/admin.py` shrinks slightly. Cleanup operations are ledger ops (§10) but the API surface is otherwise identical. |
| 8. UI Surfaces | Unchanged. Counts read from the new ledger after cutover. | Dashboard, settings, exports — no user-visible change other than OOBE losing Step 5. |
| 9. API Surface | Unchanged except SA endpoints (removed). | All other endpoints preserved. |
| 10. Data Lifecycle & Retention | Unchanged. Retention rules apply to `ledger_events` instead of `event_mappings` (same fields). | `app/jobs/cleanup.py` rewritten in Stage 2. |
| 11. Security Controls | Unchanged. | Webhook auth, rate limits, SSRF, encryption — all untouched. |
| 12. Email Alerts | Unchanged. New alert type added: `event_sync_poison_pill`. | Otherwise identical. |
| 13. Backup & Export | Unchanged. New tables included in backup. ICS export reads from ledger. | Same retention. |
| 14. Edge Cases | All preserved. See §8 (recurring), §9 (modes), §10 (cleanup). **SA-fallback edge cases gone.** Webcal-rename-creates-duplicate is fixed (§5.4). Concurrent-sync-creates-duplicate is fixed structurally. Recurring-cancellation amnesia fixed by sticky cancellation ledger rows. |

---

## 16. Features At Risk (Honest List)

Behaviours that need explicit verification, with mitigation plan:

1. **40-second verification re-fetch** — retired in favour of etag-gated updates. **Risk:** if Google's eventual-consistency lag exceeds what etag-gating handles, we'd see no observable issue (writes are idempotent), but might delay convergence. **Mitigation:** keep a low-frequency re-reconcile after webhooks (e.g., one extra ingest 60s later). Trivial to add.

2. **Lock emoji + revert (uniform in new system)** — mechanism changes from "compare DB to Google" to "compare ledger.applied_payload_hash to Google etag." **Risk:** edge case where user moves event multiple times in rapid succession could trigger ping-pong. **Mitigation:** the etag check + idempotent operation makes this provably converge. Add a specific concurrency test. **NEW behaviour:** uniform across non-editable client copies on main, personal, webcal, and busy blocks on clients, fixing today's silent gap on non-editable client copies on main.

3. **String-prefix `_event_has_managed_prefix` heuristic** — replaced by precise lookup in projections. **Risk:** legacy events created on a previous system version might lack `bb_proj_id`. **Mitigation:** orphan scan keeps prefix-matching as a secondary criterion for the first 30 days post-cutover; we re-link rather than delete.

4. **Two-pass cleanup (DB-driven + prefix sweep)** — replaced by orphan scan + projection-state cleanup. **Risk:** an event created by BB but somehow missing from projections (very rare) wouldn't be cleaned. **Mitigation:** orphan scan covers this — any Google event with `bb_*` extended properties whose `bb_proj_id` doesn't exist gets queued for deletion.

5. **Soft-delete of `event_mappings` (`deleted_at`)** — replaced by `ledger.user_intentionally_deleted`. **Risk:** semantic must match exactly: "user removed event from main → don't re-create it from source." **Mitigation:** the flag is set on the ledger row, not deleted with it; ingest checks it on every upsert.

6. **`bb_mapping_id` patch on insert** (currently best-effort, silently fails — `rules.py:359-363`). **Risk:** retired entirely. **Improvement:** the deterministic Google event ID (§7) replaces this; no patch step needed.

7. **Color recolor when `client_calendars.color_id` changes** — must trigger re-render of all projections sourced from that calendar. **Mitigation:** explicit handler bumps version on all matching ledger rows.

8. **`is_our_event()` based on extended properties + summary prefix + sync tag** — replaced by `ledger_projections.google_event_id` lookup. **Risk:** edge case where extended properties were stripped (today's Bug 1, fixed but watchful). **Mitigation:** lookup is the primary check; extended properties become defence-in-depth only.

### Features that get **better** (not at risk — improvements)
- Concurrent webhook + periodic creating duplicates → eliminated structurally.
- Crash mid-write leaving Google/DB divergent → eliminated structurally.
- Sync token advancing past missed events → eliminated structurally.
- Webcal rename creating duplicates → fixed (canonical UID stays stable across summary changes).
- Webcal partial-failure permanent gaps → fixed (per-event outbox).
- Poison-pill event freezing a calendar → fixed (per-projection failure flag).
- Verification re-fetch overwriting fresher data with stale data → eliminated (etag-gated updates).

---

## 17. What Changes for Users

**Almost zero observable changes for normal operation.** Same UI, same URLs, same API, same email alerts, same backups.

### One brief observable transition at cutover (~5–15 minutes)

When the cutover happens (Stage 5):
- You run "Cleanup & Pause" from the admin UI. All BusyBridge-managed events disappear from all calendars (busy blocks on clients, synced copies on main).
- The old container stops; the new container starts on the new code.
- You resume sync. The new system populates from scratch.
- During this window (typically 5–15 minutes depending on calendar count and event count), anyone with free/busy access to your calendar sees you as fully free. As the new system catches up, events re-populate.

Schedule for a quiet time (weekend morning is ideal). The downtime window is recoverable from backup; no permanent data loss possible.

### Post-cutover differences

**Removed features** (gone forever, not coming back):
- Service account mode (was unreliable; see §9). Existing SA-organized events are cleaned up at cutover.
- OOBE Step 5 (Service Account upload). Wizard goes from 7 steps to 6.
- `/api/admin/service-account` endpoints and the SA admin page.

**Added behaviours** (improvements):
- **Non-editable events on your main calendar revert if moved.** Today's code has a silent gap here; the new system closes it.
- **Cancelled instances of recurring meetings reliably stay deleted.** This is the area you flagged as a constant problem. The new architecture (sticky cancellation ledger rows + outbox retry of failed instance-deletes + reliable re-cancellation after recurring rewrites) makes this structurally correct.
- **Fewer duplicate events** (idempotent inserts + single per-user reconciler).
- **Fewer missing busy blocks** (sync token only advances after every projection is queued in the outbox).
- **Faster post-edit convergence** (etag-gated updates avoid today's 40-second verification window; eliminates the "verification overwrites fresher data" bug).
- **A single bad event no longer freezes a calendar.** Today, a poison-pill event blocks sync token advancement indefinitely. New system flags per-projection failure and advances past it with an admin alert.

**One new admin-visible alert type:** `event_sync_poison_pill` — fires when a specific event has failed sync 5 times. Tells the admin the event needs manual review.

---

## 18. Open Questions for Review

Before I start coding, please confirm or steer:

1. **Length of real-Google validation period (Stage 4):** I propose 7 calendar days against test accounts before production cutover. Acceptable, or longer? (Already answered earlier: 14 days is the conservative choice.)
2. **Outbox concurrency model:** I'm proposing one drain coroutine per active user. With ~50 users this is fine; if you expect 1000+ users we'd want a worker pool. What's the upper bound?
3. **Webcal canonical UID for unstable feeds:** today's hash uses `(summary, start, end)`. I'm proposing `(start, end, normalized_summary)` *only when summary stays the same*, to avoid the rename-creates-duplicate bug. Is it acceptable that an event's title changing AND its time changing simultaneously creates a new ledger row? (Same behaviour as today, but worth flagging.)
4. **Schema-level cascade deletes:** I have `ON DELETE CASCADE` on `ledger_events → ledger_projections` and `ledger_projections → outbox_operations`. This means a force-delete of a ledger row drops outbox without writing the deletion to Google. We should never force-delete in the new model — `status='cancelled'` is the correct path. Want to add a DB trigger to refuse `DELETE FROM ledger_events`? I lean yes.
5. **`derive_instance_event_id` for cancelled instances of recurring projections** — preserve today's helper as-is, or replace with a more robust scheme? Today's works; lean toward preserve.
6. **OAuth scope changes** — none needed for the rewrite, but ANALYSIS.md noted we use full `calendar` scope. Out of scope here, but worth a follow-up?

---

## 19. Estimated Effort

| Stage | Effort | Calendar time | Risk | What it produces |
|---|---|---|---|---|
| 1. Test infrastructure (fake Google + clock) | 5–8 days | 1–2 weeks | Low | Foundation for integration & soak tests |
| 2. Complete new system | 20–30 days | 4–6 weeks | Medium | The rewrite itself, validated via integration tests as it grows |
| 3. Soak harness (overlaps end of Stage 2) | 5–7 days | (included) | Low | 365-sim-day adversarial validation |
| 4. Real-Google validation | passive observation | 1–2 weeks | Medium | Evidence the architecture works against real Google quirks |
| 5. Production cutover | 1 day | 1 day | High (concentrated) | Live on new system |
| **Total** | **31–46 days work** | **7–10 calendar weeks** | | |

**Significantly less work than the phased migration originally proposed** (which was 36–55 days work and 9–14 calendar weeks). The savings come from skipping dual-write, shadow ledger, and gated cutover scaffolding.

**The soak harness in Stage 3 is the most underappreciated investment.** Without it we cannot prove the rewrite delivers the reliability it claims. With it, we can validate against months of simulated adversarial load before any real-Google contact. It also persists as a permanent regression safety net.

**Stage 4 is what makes clean-cut safe.** The soak harness validates that the architecture is correct under simulated load; the real-Google week validates that our fake Google is faithful enough to catch real-world quirks. Both are needed; neither substitutes for the other.

---

## 20. Sign-off

This plan is ready for review. Specifically asking for:
- Confirmation that no inventory item is missed (or call out which).
- Direction on the open questions in §18.
- Approval to begin Stage 1 (test infrastructure: fake Google + clock + integration test framework).

No code changes have been made.
