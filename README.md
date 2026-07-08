# BusyBridge

A self-hosted calendar synchronization service for consulting organizations.
Connect multiple **client calendars** to a single **main calendar** so your
availability stays in sync everywhere — without leaking event details across
clients.

Your main calendar is the single source of truth. Client-calendar events are
mirrored to your main calendar in full detail; everything on your main calendar
is reflected back to each client as an opaque **"Busy"** block. Other clients
never see what a given client meeting actually is.

---

## Features

- **Bidirectional sync** — client events appear on your main calendar with full
  detail; main-calendar events appear as opaque "Busy" blocks on every client
  calendar.
- **Personal calendar sync** — connect personal Gmail/Workspace calendars as
  **read-only** sources that cast privacy-preserving "Busy (personal)" blocks on
  your main and all client calendars. No details are shared. All-day personal
  events are not mirrored (they would only block out the whole day with no
  information); toggle with `SYNC_PERSONAL_ALL_DAY_EVENTS`.
- **Webcal/ICS subscriptions** — subscribe to external ICS feeds (conferences,
  travel) that mirror to your main calendar (and optionally one chosen client),
  with busy blocks elsewhere. Unstable-UID feeds are handled by content hashing.
- **Recurring events** — RRULEs are copied verbatim (with the source timezone,
  so DST stays correct). Single-instance edits and cancellations sync
  individually; cancellations are **sticky** and survive sync-token expiry.
- **Free/Busy aware** — events marked Free don't create busy blocks (personal
  sources always block).
- **RSVP propagation** — accepting/declining a client meeting on your main
  calendar is written back to the originating client event.
- **Conference links** — Google Meet/Zoom links are carried onto the main copy
  (no new conference is minted), with churn-safe re-sync when a room changes.
- **Color coding** — main-calendar copies are colored by their source client
  calendar.
- **Drift revert** — non-editable managed events that get moved or deleted on
  any calendar are re-asserted to their canonical state on the next sync.
- **Idempotent writes** — every Google write uses a deterministic event ID plus
  an etag precondition, so retries never duplicate.
- **Google API rate limiting** — a process-wide token-bucket limiter (5 req/s by
  default) with exponential backoff prevents quota exhaustion.
- **Real-time sync** — Google push notifications trigger a debounced reconcile
  (~5 s); a 30-second drain loop pushes pending writes promptly.
- **Self-healing** — a 6-hourly orphan scan reclaims events that escaped
  tracking, a 10-minute content audit catches source edits that incremental sync
  missed, and a per-user circuit breaker auto-pauses a user whose every calendar
  is failing.
- **ICS export** — full-calendar ICS export, plus a "clean" variant that strips
  BusyBridge-managed events (for migration or external backup).
- **Email alerts** — notifications for token revocation, unmirrorable events,
  failing calendars, webhook-registration failures, and circuit-breaker trips.
- **History preservation** — past one-off events are *released* (frozen on the
  calendars and retired from sync) at the retention window rather than deleted, so
  old history stays visible; toggle with `RELEASE_EXPIRED_EVENTS`.
- **Automated backups** — daily database + ICS backups with 7-daily / 2-weekly /
  6-monthly retention, and a drop-in restore flow.
- **Admin dashboard** — user management, system health, sync activity, log
  viewer, factory reset, and a permanent-failure surface.

---

## How It Works

Your **main calendar is the source of truth.** Client, personal, and webcal
calendars are managed automatically.

### Creating appointments

**On your main calendar** — for personal blocks, internal meetings, or anything
you own. BusyBridge casts a "Busy" block onto every connected client calendar:

```
Event on Main Calendar
        │
        └─► "Busy" blocks on Client A, B, C
```

**On a client calendar** — when the invite should come from that client's
domain. BusyBridge copies it to your main calendar (full detail) and casts
"Busy" blocks on the *other* clients:

```
Event on Client A
        │
        ├─► Full-detail copy on Main Calendar
        └─► "Busy" blocks on Client B, C  (not A — it owns the real event)
```

### Personal calendars (read-only)

Personal calendars never receive writes. Their events cast privacy-preserving
blocks titled **"Busy (personal)"** on your main calendar and all client
calendars — no titles, no details. **All-day** personal events are skipped
entirely (an all-day "Personal" block only marks the whole day busy without
conveying anything); set `SYNC_PERSONAL_ALL_DAY_EVENTS=true` to mirror them.

### Webcal/ICS subscriptions

Subscribe to external feeds (conference schedules, travel itineraries). By
default a feed mirrors to your main calendar with busy blocks on clients. A feed
can also be **placed on one chosen client calendar**, in which case it appears in
full detail on both your main calendar and that client, with busy blocks
everywhere else. Feed copies are prefixed with the subscription's display label
(e.g. `[ISO] Standards Call`).

### When clients schedule you

Client-created events sync to your main calendar with full detail automatically.
Other clients only ever see "Busy" — no cross-client information is shared.

| Action | Where |
|--------|-------|
| A personal/internal appointment | Your **main calendar** |
| A meeting inside a client's domain | That **client's calendar** |
| Block personal time everywhere | Connect a **personal calendar** |
| Subscribe to a schedule | Add a **webcal subscription** |
| Accept/decline a client meeting | Your **main calendar** (RSVP propagates back) |
| See your full schedule | Your **main calendar** |

### Event markers

- **Lock icon** (🔒) is prepended to the title of any managed copy you cannot
  edit at its source (and to every webcal copy, since feeds are read-only).
- The configurable tag `[BusyBridge]` (`MANAGED_EVENT_PREFIX`) is appended on its
  own line at the bottom of the **description** — events read normally but stay
  searchable. Set it empty to disable tagging.
- Full copies also carry a footer with the source label, the placement label
  (for client-placed webcal feeds), the original-event link, and a guest list.
- **Edit protection** — if you move or edit a non-editable managed event, it is
  reverted to its canonical state within one sync cycle.

---

## Architecture

A single Docker container runs:

- **FastAPI** — HTTP, OAuth, webhooks, and the web UI.
- **APScheduler** — background jobs (the sync drain, audits, maintenance).
- **SQLite** (`aiosqlite`, WAL mode) — all configuration and sync state.
- A **rate-limited Google Calendar client** — token-bucket, 5 req/s.

### The ledger pipeline

BusyBridge (v2) is a **canonical ledger + projection + idempotent outbox**. All
live sync logic is in `app/ledger/`. Each user is reconciled by a single writer
in one pass:

```
                 ┌──────────── reconcile_user (one writer per user) ───────────┐
sources ──► ingest ──► ledger_events ──► planner ──► ledger_projections ──► diff ──► outbox ──► Google
(client/main/                (canonical    (desired      (desired vs        (one op   (drain,
 personal/webcal)             truth)        state)        applied)           each)     idempotent)
```

1. **Ingest** (`ingest/{client,main,personal,webcal}.py`, `discovery.py`) reads
   each source and upserts canonical rows into `ledger_events`.
2. **Planner** (`planner.py`) is a pure function: for each changed ledger row it
   computes the desired set of projections (what each target calendar *should*
   show).
3. **Diff** (`diff.py`) turns each projection whose desired state differs from
   its applied state into exactly one **outbox** operation.
4. **Outbox drain** (`outbox.py`) executes operations against Google oldest-first
   with idempotent retry.

Key tables (`schema.py`):

| Table | Role |
|-------|------|
| `ledger_events` | Canonical store — one row per logical event per user. |
| `ledger_projections` | Desired-vs-applied state per (event, target calendar). |
| `outbox_operations` | The write queue (create / update / delete / patch / delete_source). |
| `reconcile_requests` | One debounced trigger row per user. |
| `affected_ledger_events` | Append-only replan queue (race-safe). |

A `BEFORE DELETE` trigger on `ledger_events` refuses to hard-delete a row that
still has a live projection, forcing the safe *cancel → drain deletes → delete*
path.

### Single writer, idempotent writes

- **One writer per user.** `reconcile_user` runs under a per-user lock; across
  processes, atomic compare-and-claim updates on `reconcile_requests` and
  `outbox_operations` serialize work.
- **Deterministic IDs.** Inserts send a client-supplied Google event ID derived
  from the projection (`bb` + base32hex, ~15 chars). A retry collides on the
  per-calendar uniqueness constraint and is treated as success after a confirming
  GET — so retries never duplicate.
- **Etag preconditions.** Updates send `If-Match`; a `412` marks the operation
  superseded and replans from a fresh GET. Deletes are unconditional.
- **Rate limiting & backoff.** Outbound calls pass through a shared token bucket
  (`GOOGLE_API_RATE_LIMIT_PER_SECOND`, default 5/s). Retries back off
  exponentially to 60 s; quota responses (`429`, `403` quota) are retried
  forever, never poison-pilled.

### Loop prevention

BusyBridge recognizes its own writes structurally — by the deterministic `bb…`
event ID and an exact `ledger_projections.google_event_id` lookup — and skips
them on ingest. Rendered events also carry `extendedProperties.private.bb_proj_id`
and `bb_target_kind` as defence-in-depth. (The ledger version is deliberately
*not* stamped onto the body, since that would change the payload hash on every
bump and cause a write loop.)

### Triggers & cadence

Reconciles are triggered three ways, all debounced through `reconcile_requests`:

- **Webhook** — Google push notifications schedule a reconcile after a ~5 s
  debounce.
- **Periodic** — a job enqueues every active user every 5 minutes; a separate
  drain job runs **every 30 seconds**.
- **Manual** — dashboard actions enqueue immediately (with a settling delay).

### Key directories

```
app/
  ledger/        v2 sync engine: reconciler, ingest/, planner, diff, outbox,
                 payload, identity, recurrence, async_google, runtime, schema
  api/           REST API endpoints (mounted under /api)
  auth/          Google OAuth, sessions (JWT)
  ui/            web UI routes + Jinja2 templates, setup wizard
  jobs/          APScheduler job definitions
  alerts/        email alerting
  sync/          legacy v1 helpers still in use: ics_export, backup,
                 google_calendar (API adapter). The v1 engine was retired.
```

---

## Quick Start

### Prerequisites

1. Docker and Docker Compose.
2. A Google Cloud project with the Calendar API enabled and OAuth 2.0
   credentials.
3. A domain with HTTPS (for webhooks and OAuth callbacks).

### Install

```bash
git clone <repository-url>
cd busybridge
mkdir -p data secrets
docker compose up -d
```

The shipped `docker-compose.yml` publishes the app on host port **8033**
(container port 3000) and sets `TZ=America/New_York`. Adjust `PUBLIC_URL` and the
published port to taste.

Then open the setup wizard at your `PUBLIC_URL`. It walks through six pages:

1. **Welcome** — overview and prerequisites.
2. **Google Cloud credentials** — paste your OAuth client ID/secret.
3. **Admin authentication** — sign in with Google; this captures your home-org
   domain.
4. **Email alerts** — optional SMTP configuration (skippable).
5. **Encryption key** — a master key is generated; **save it** — it decrypts all
   stored OAuth tokens.
6. **Complete** — link to the dashboard.

> **Secure the first run.** Until the wizard finishes, the instance is
> unconfigured and `/setup` is unauthenticated. Run setup over `localhost`, a
> VPN, or behind a firewall, complete it in a single browser session, and only
> expose the service publicly once setup is done. The wizard binds to one browser
> via a cookie; if the container restarts mid-setup, just rerun it.

Google OAuth credentials and SMTP settings are stored encrypted in the database
after setup, not in environment variables.

### Google Cloud setup

1. In the [Google Cloud Console](https://console.cloud.google.com/), create a
   project and enable the **Google Calendar API**.
2. Configure the OAuth consent screen with scopes `calendar`,
   `calendar.readonly`, `email`, `profile`, `openid`.
3. Create **Web application** OAuth 2.0 credentials with these redirect URIs
   (replace the host with your `PUBLIC_URL`):
   - `https://your-domain/auth/callback`
   - `https://your-domain/auth/connect-client/callback`
   - `https://your-domain/auth/connect-personal/callback`
   - `https://your-domain/setup/step/3/callback`

> Keep the OAuth app in **In production** (not Testing) status — Testing-mode
> refresh tokens expire after ~7 days and force reconnects.

---

## Configuration

### Environment variables

All settings have safe defaults; override via `.env` or the environment. Google
credentials and SMTP live in the database, not here.

**Core**

| Variable | Description | Default |
|----------|-------------|---------|
| `PUBLIC_URL` | Public base URL (drives cookie security, CORS, HSTS, OAuth/webhook URLs) | `http://localhost:3000` |
| `DATABASE_PATH` | SQLite database path | `/data/calendar-sync.db` |
| `ENCRYPTION_KEY_FILE` | AES key file (decrypts stored tokens) | `/secrets/encryption.key` |
| `SESSION_SECRET_KEY` | JWT signing secret (auto-generated + persisted if unset) | _(generated)_ |
| `SESSION_EXPIRE_DAYS` | Session/JWT lifetime | `7` |
| `LOG_LEVEL` | Logging level | `info` |
| `LOG_DIR` | Rotating log directory | `/data/logs` |
| `BACKUP_PATH` | Backup directory | `/data/backups` |
| `ENABLE_WEBHOOKS` | Google push notifications | `true` |
| `ENABLE_LEDGER_JOBS` | Use the v2 ledger jobs (off = legacy rollback path) | `true` |
| `LEDGER_DRY_RUN` | Plan + fill outbox but never write to Google | `false` |
| `TRUST_PROXY_HEADERS` | Trust `X-Forwarded-For`/`X-Real-IP` for rate-limit IP | `false` |

**Sync cadence**

| Variable | Description | Default |
|----------|-------------|---------|
| `SYNC_INTERVAL_MINUTES` | Periodic enqueue + health-check interval | `5` |
| `CONTENT_AUDIT_MINUTES` | Source content re-audit interval | `10` |
| `TOKEN_REFRESH_MINUTES` | OAuth token refresh interval | `30` |
| `WEBHOOK_RENEWAL_HOURS` | Push-channel renewal interval | `6` |
| `ALERT_PROCESS_MINUTES` | Email-alert queue tick | `1` |

**Rate limits**

| Variable | Description | Default |
|----------|-------------|---------|
| `RATE_LIMIT_PER_MINUTE` | Per-IP limit on general endpoints | `60` |
| `WEBHOOK_RATE_LIMIT_PER_MINUTE` | Per-channel limit on the webhook endpoint | `30` |
| `AUTH_RATE_LIMIT_PER_MINUTE` | Limit on auth endpoints | `10` |
| `GOOGLE_API_RATE_LIMIT_PER_SECOND` | Outbound Google call cap (≤0 disables) | `5.0` |

**Retention**

| Variable | Description | Default |
|----------|-------------|---------|
| `EVENT_RETENTION_DAYS` | Age (since end) at which a single-occurrence event is released or pruned | `30` |
| `RELEASE_EXPIRED_EVENTS` | At the window, *release* expired one-off events (freeze their copies on the calendars, retire them from sync) instead of deleting them. `false` restores the legacy delete. Genuine cancellations are deleted either way. | `true` |
| `RECURRING_SOFT_DELETE_DAYS` | Hard-delete cancelled series after | `30` |
| `AUDIT_LOG_RETENTION_DAYS` | Keep `sync_log` rows | `90` |
| `DISCONNECTED_CALENDAR_RETENTION_DAYS` | Purge disconnected calendars after | `30` |

**Behavior**

| Variable | Description | Default |
|----------|-------------|---------|
| `SYNC_PERSONAL_ALL_DAY_EVENTS` | Mirror all-day personal-calendar events as busy blocks. Off = suppress them everywhere (they only block the whole day with no info). | `false` |
| `DELETE_PROPAGATION_MODE` | Propagate a delete of a managed non-recurring client copy back to the source: `off` / `shadow` (log only) / `on`. See `DELETE_PROPAGATION_PLAN.md`. | `off` |

**Markers, titles & test mode**

| Variable | Description | Default |
|----------|-------------|---------|
| `MANAGED_EVENT_PREFIX` | Tag appended to managed event descriptions (empty disables) | `[BusyBridge]` |
| `BUSY_BLOCK_TITLE` | Title of busy blocks from main/client sources | `Busy` |
| `PERSONAL_BUSY_BLOCK_TITLE` | Title of busy blocks from personal sources | `Busy (personal)` |
| `TEST_MODE` | Gmail-safe testing mode (see below) | `false` |
| `TEST_MODE_ALLOWED_HOME_EMAILS` | Home-login email allowlist | _(none)_ |
| `TEST_MODE_ALLOWED_CLIENT_EMAILS` | Client-connect email allowlist | _(none)_ |
| `BB_FAKE_GOOGLE` | Boot with the in-memory fake Google Calendar wired in and mount the `/_fake/*` debug endpoints. **Testing only — never enable in production.** | `false` |

> `TZ` is an OS/container variable (not a setting); `docker-compose.yml` sets it
> to `America/New_York`, so the daily cron jobs run in US Eastern by default.

### Scheduled jobs

In the default ledger mode (`ENABLE_LEDGER_JOBS=true`):

| Job | Schedule | Purpose |
|-----|----------|---------|
| Ledger drain | every 30 s | Push pending outbox writes to Google |
| Ledger periodic enqueue | every 5 min | Queue a reconcile for each active user |
| Sync health checks | every 5 min | Circuit breaker + failing-calendar alerts |
| Content audit | every 10 min | Re-verify source content vs the ledger (catches missed edits) |
| Token refresh | every 30 min | Refresh OAuth tokens expiring within an hour |
| Alert processing | every 1 min | Send queued email alerts |
| Orphan scan | every 6 h | Reclaim Google events that escaped tracking |
| Webhook renewal | every 6 h | Renew channels expiring within 24 h |
| Webhook registration | on startup | Register push channels for all users |
| Retention cleanup | daily 3:00 AM | Release/prune expired records (fault-isolated per bucket) |
| Stale alert cleanup | daily 4:00 AM | Remove old sent/failed alerts |
| Database VACUUM | weekly Sun 4:30 AM | Reclaim space after deletes |
| Daily backup | daily 11:00 PM | DB + ICS backup, then enforce retention |

(With `ENABLE_LEDGER_JOBS=false`, a single legacy `periodic_sync` job replaces
the drain/enqueue pair — a rollback switch only.)

---

## Dashboard

The dashboard (`/app`) shows:

- **Status grid** — connected calendars, total tracked events, health, issues.
- **Integrity panel** — live consistency status.
- **Client calendars** — each with a status icon, color picker, last-sync time,
  event/busy-block counts, a per-calendar sync button with live progress, and
  actions (cleanup & re-sync, disconnect).
- **Personal calendars** — status, counts, sync/disconnect.
- **Webcal subscriptions** — add feeds (URL + display prefix + placement), with
  status and sync/delete.
- **Sync activity feed** — recent sync events.

### Sync control (`/app/settings/sync`)

- **Full re-sync** — clear sync tokens and reprocess everything.
- **Cleanup & re-sync** — delete all managed events, then recreate.
- **Cleanup & pause** — remove managed events and stop (for troubleshooting).
- **Connection health check** — test all OAuth tokens.
- Live progress with step labels and counts.

Per-user pause (`POST /api/sync/my/pause` / `/my/resume`) is distinct from the
admin-wide global pause. Per-calendar cleanup is available from the dashboard.

### Calendar exports (`/app/settings/exports`)

- **Full export** — a ZIP with one `.ics` per calendar (attendees, Meet links,
  recurrence, etc.).
- **Clean export** — the same with BusyBridge-managed events filtered out (for
  migrating away).
- Daily automatic exports follow the 7/2/6 retention policy. Download/create/
  delete go through admin-only `/api/admin/backup/ics` endpoints.

### Admin (`/admin`)

User management, system health (active calendars, recent errors, alert queue),
log viewer, SMTP/alert settings, factory reset, and the permanent-failure
surface. Admins grant admin to others from `/admin/users` (the last admin can't
be demoted).

---

## Sync Behavior (details)

- **Full copy vs busy block.** A client/webcal source becomes a full-detail copy
  on your main calendar and opaque "Busy" blocks on the other clients; the origin
  client keeps its real event. Main-only events cast busy blocks on all clients.
- **Free/Busy.** Events marked Free (`transparency=transparent`) cast no busy
  block. Personal sources always block.
- **RSVP propagation** is one-directional — main → originating client — and is
  gated on an intent flag so a source-side RSVP is never clobbered. Personal and
  webcal sources never receive write-back.
- **Recurring events.** RRULEs are copied verbatim with the source IANA timezone
  (DST-correct). Modified/cancelled instances become their own sticky rows;
  cancellations are recovered during full sync via `events.instances(showDeleted)`.
  A "change all events from here forward" split is treated **additively** (each
  segment is its own series), never by re-keying the base.
- **Conference links.** Meet/Zoom data is carried onto the main copy (existing
  entry points preserved, no new conference minted) and excluded from the change
  hash; room changes are adopted only after the new value is seen twice
  (debounced), and removals settle the same way.
- **Same-meeting dedup.** When a meeting lands on your main calendar through more
  than one path, the redundant copy is suppressed by Google's `iCalUID` (matched
  by identity, never by time — distinct same-time meetings each keep a block).
- **Drift revert** applies to both your main calendar and client calendars: a
  moved/edited/deleted managed copy is re-asserted to its canonical state. A
  deleted webcal/personal block on your main calendar is re-created, never
  propagated back.
- **Color coding.** Full copies take the source client calendar's color, read at
  render time; busy blocks are uncolored.
- **Retention / history.** A one-off event past `EVENT_RETENTION_DAYS` (since its
  end) is, by default, *released*: its copies are frozen on the calendars and the
  event is retired from sync (planner, diff, every ingest path, and the orphan
  scan all leave a `released` row alone — its projections are kept, so the copy
  reads as live). Set `RELEASE_EXPIRED_EVENTS=false` to delete the copies instead.
  Recurring series are unaffected (one mirrored event whose occurrences persist
  while the series is active); genuine user cancellations are always deleted.

---

## Backup & Recovery

Daily backups run at 11 PM with a retention policy of **7 daily / 2 weekly /
6 monthly**, for both the database and the ICS exports, under `BACKUP_PATH`
(default `/data/backups`).

**What to back up**

- `/data/calendar-sync.db` — all application data.
- `/secrets/encryption.key` — required to decrypt OAuth tokens.

**Manual backup** (admin-authenticated):

```bash
curl -X POST https://your-domain/api/admin/backup \
  -H "Cookie: session=<admin-session-jwt>"
```

Or use the web UI (ICS exports at `/app/settings/exports`).

**Recovery**

1. Stop the container.
2. Place the backup as `<data-dir>/restore-pending.zip` (default
   `/data/restore-pending.zip`).
3. Start the container — it detects the file, restores the ledger database, and
   resets projections so the idempotent outbox re-converges Google. The file is
   renamed to `restore-pending-done-<timestamp>.zip` so it isn't re-applied.
4. Verify from the admin dashboard.

---

## Security

- **Encryption at rest** — OAuth tokens and secrets are encrypted with
  AES-256-GCM (random per-message nonce); the 32-byte key lives in
  `ENCRYPTION_KEY_FILE`, separate from the database. Startup fails closed if the
  key is missing or can't decrypt stored credentials.
- **Sessions** — JWTs (HS256) in an httpOnly, SameSite=Lax cookie (Secure when
  `PUBLIC_URL` is HTTPS), default 7-day expiry. A per-user token-version field
  lets admins force re-authentication and invalidates old cookies.
- **Home-org restriction** — home login is restricted to your Google Workspace
  domain at the OAuth callback (in `TEST_MODE`, an explicit email allowlist
  replaces the domain check).
- **Rate limiting** — 60/min on general endpoints, 30/min per channel on
  webhooks, 10/min on auth.
- **CSRF + headers** — same-origin enforcement on state-changing requests; CSP,
  `X-Frame-Options: DENY`, `X-Content-Type-Options`, `Referrer-Policy`, and HSTS
  (over HTTPS). CORS is restricted to `PUBLIC_URL`.
- **Non-root** — runs as `appuser` (uid 1000). Designed to sit behind a
  TLS-terminating reverse proxy.

---

## Development

### Local setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt

export DATABASE_PATH=./data/calendar-sync.db
export ENCRYPTION_KEY_FILE=./secrets/encryption.key
export PUBLIC_URL=http://localhost:3000

python -m uvicorn app.main:app --host 0.0.0.0 --port 3000 --reload
```

### Tests

```bash
pytest                      # ~748 tests (the 5 slow soak tests are excluded)
pytest -m slow              # run only the soak tests
pytest -m ""                # run everything (~753)
pytest --cov=app --cov-report=html
```

Tests run entirely against an in-memory fake Google Calendar
(`tests/fakes/`) that faithfully reproduces real API quirks — see
`tests/fakes/QUIRKS.md`. Layers: unit, Hypothesis property tests
(`tests/integration/test_property.py`), integration (`tests/integration/`),
chaos/concurrency, and soak (`tests/soak/`).

### End-to-end tests

`e2e/` runs Playwright + the real Calendar API against a live instance:

```bash
./e2e/run.sh            # all
./e2e/run.sh api        # API-only
./e2e/run.sh browser    # browser flows
```

See `e2e/README.md` for OAuth/session bootstrap.

### Upgrading

```bash
git pull
docker compose up -d --build calendar-sync
```

Schema migrations run automatically on startup (idempotent inline `ALTER TABLE`
statements); no manual steps.

---

## Test Mode (Gmail-only testing)

`TEST_MODE=true` lets you run a production-like instance against throwaway Gmail
accounts without paying for Workspace seats. When enabled:

1. Home login is restricted to an exact email allowlist
   (`TEST_MODE_ALLOWED_HOME_EMAILS`) instead of a domain.
2. Client connections are restricted to `TEST_MODE_ALLOWED_CLIENT_EMAILS`.
3. If either allowlist is empty, that auth path fails closed.

Example `.env`:

```env
PUBLIC_URL=https://busybridge-test.example.com
TEST_MODE=true
TEST_MODE_ALLOWED_HOME_EMAILS=bb-home-admin@gmail.com
TEST_MODE_ALLOWED_CLIENT_EMAILS=bb-client-1@gmail.com,bb-client-2@gmail.com
MANAGED_EVENT_PREFIX=[BusyBridge]
# Optional: poll-only, no push notifications
ENABLE_WEBHOOKS=false
```

Setup: create dedicated Gmail accounts (never reuse real ones); a dedicated
Google Cloud project with the Calendar API and the four redirect URIs above; move
the OAuth app to **In production** to avoid ~7-day refresh-token expiry. Then run
the setup wizard, sign in as a home-allowlisted account, connect each client
Gmail, and select a writable calendar for each.

Common `TEST_MODE` errors: `test_mode_no_home_allowlist` /
`test_mode_no_client_allowlist` (allowlist empty), `email_not_allowed` /
`client_email_not_allowed` (account not allowlisted), `no_refresh_token`
(reconnect with full consent).

---

## Troubleshooting

### Logs

```bash
# Today's errors/warnings (excluding known noise)
docker exec calendar-sync sh -c 'cat /data/logs/busybridge.log | grep " - ERROR - \| - WARNING - " | grep -v "file_cache\|discovery_cache\|Unknown webhook channel"'

# Available log files (14-day retention, daily rotation)
docker exec calendar-sync ls /data/logs/
```

### Common issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| "credentials do not contain" | OAuth lost its refresh token | Disconnect + reconnect the account |
| `403 rateLimitExceeded` | API burst | Handled automatically by the limiter; check for a calendar with excessive events |
| "Service accounts cannot invite attendees" | No Domain-Wide Delegation | Expected; the retry-without-attendees path handles it |
| Backup "permission denied" | `/data/backups` owned by root | `docker exec -u root calendar-sync chown appuser:appuser /data/backups` |
| Sync not running | Global pause is on | Admin dashboard, or `POST /api/sync/resume` (admin) |
| "Unknown webhook channel" warnings | Stale channels after restart | Harmless; self-resolves as channels expire |

---

## API

Interactive API docs when running:

- Swagger UI: `https://your-domain/docs`
- ReDoc: `https://your-domain/redoc`

Notable routes: `/auth/*` (OAuth + sessions), `/api/*` (REST — calendars, sync,
webcal subscriptions, admin, the ledger admin surface under `/api/admin/ledger`),
`POST /api/webhooks/google-calendar` (Google push receiver), and `/health`
(readiness probe).

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.12 |
| Web framework | FastAPI (Uvicorn) |
| Database | SQLite (`aiosqlite`, WAL) |
| Google API | `google-api-python-client` |
| Scheduling | APScheduler |
| Encryption | AES-256-GCM (`cryptography`) |
| Sessions | `python-jose` (JWT) |
| Email | `aiosmtplib` |
| ICS | `icalendar` |
| Rate limiting | SlowAPI (HTTP) + a custom token bucket (Google) |
| Frontend | Jinja2 + htmx + Alpine.js + Tailwind (vendored, no CDN) |

Exact pins are in `requirements.txt` / `requirements.lock`.

---

## License

MIT License — see [`LICENSE`](./LICENSE).
