# BusyBridge Feature Inventory & Acceptance Checklist

> **Purpose.** This is the acceptance checklist for the canonical-ledger rewrite (see `REWRITE_PLAN.md`). Zero feature loss means every checkbox below works identically (or better) in the new implementation. Items reference current source locations as of branch `claude/improve-reliability-hKbKZ`.

---

## Features REMOVED in Phase 0 (intentional, not preserved)

The following capabilities are **removed** as part of REWRITE_PLAN.md Phase 0 and are **not** acceptance criteria for the rewrite. Research established that the service account mode does not reliably deliver immovable events on the user's own calendar (calendar ownership trumps event-level `guestsCanModify=false`). The 🔒 emoji + revert-on-drift mechanism (already used for sa_tier=0, personal, webcal) becomes the uniform approach.

- **Service account mode entirely.** `users.sa_tier` column reads removed; `app/auth/service_account.py` deleted; SA branches in `rules.py` / `engine.py` / `consistency.py` deleted.
- **OOBE Step 5 (Service Account upload).** Wizard goes from 7 steps to 6.
- **`SERVICE_ACCOUNT_KEY_FILE` env var and `service_account_key_file` config setting.**
- **Admin endpoints:** `GET /api/admin/service-account`, `POST /api/admin/service-account/test/{userId}`, deactivate-SA endpoint.
- **Admin UI:** SA status / SA test buttons on user-management page.
- **SA fallback on 403:** the "SA access lost → reset sa_tier=0" logic in `consistency.py:99-107` becomes a no-op (no more SA path to fall back from).
- **The "natively immovable in Google UI" claim** for non-editable events. Replaced by the existing 🔒 emoji + revert mechanism (post-hoc, not real-time).

**Net change for users:** OOBE has one fewer step. Existing sa_tier=2 events stay where they are; new/updated non-editable events show 🔒 in title and snap back if moved. The `_revert_if_moved` mechanism, which today only covers personal/webcal blocks on main and busy blocks on clients, is **extended to non-editable client-event copies on main** — fixing a silent gap in current code.

---

## 1. OAuth & Accounts

### Login Flows
- [ ] Home org login initiates Google OAuth with HOME_SCOPES (calendar, email, profile, openid) — `app/auth/routes.py:125-133`
- [ ] OAuth callback extracts email domain and restricts to home organization domain — `app/auth/routes.py:190-199`, SPEC.md:194-199
- [ ] Test mode supports email allowlist instead of domain restriction — `app/auth/routes.py:174-188`, `app/config.py:37-39`
- [ ] Logout clears session cookie (SESSION_COOKIE_NAME) — `app/auth/session.py`
- [ ] Login error handling: invalid_grant, permission_denied, network timeout — `app/auth/routes.py:273-278`
- [ ] Domain mismatch error displays required domain to user — `app/auth/routes.py:196`
- [ ] Redirect URI sanitization prevents open-redirect attacks — `app/auth/routes.py:116-117`
- [ ] OAuth state stored in DB with 10-minute TTL; one-time use — `app/auth/routes.py:45-80`
- [ ] Expired OAuth states cleaned up — `app/auth/routes.py:83-92`

### Token Management
- [ ] Refresh tokens preserved across re-login to prevent breaking background sync — `app/auth/routes.py:208-215`
- [ ] Tokens encrypted at rest using AES-256-GCM — `app/encryption.py`
- [ ] Token refresh job proactively refreshes tokens expiring within 1 hour — `app/jobs/sync_job.py:192-231`
- [ ] Revoked tokens (invalid_grant) trigger token_revoked alert and disable calendar — `app/jobs/sync_job.py:224-230`
- [ ] Token expiry timestamp stored in oauth_tokens table — SPEC.md:370
- [ ] Encryption key loaded from file (default `/secrets/encryption.key`) on startup — `app/config.py:103-124`
- [ ] Encryption key generated during OOBE; never in DB or env vars — SPEC.md:102-110

### Multi-Account Support
- [ ] Each user has one home org account (`account_type='home'`) — SPEC.md:364
- [ ] Each user can connect multiple client org accounts (`account_type='client'`) — SPEC.md:149-150
- [ ] Personal calendars connected via separate OAuth (`account_type='personal'`) — README.md:101-111
- [ ] OAuth tokens table has UNIQUE constraint on `(user_id, google_account_email)` — SPEC.md:374
- [ ] User can switch main calendar via API without re-authenticating — `app/api/users.py:90-124`
- [ ] Client calendars can be connected/disconnected independently — `app/api/calendars.py:102-256`

### Service Account Mode
**REMOVED in Phase 0** — see "Features REMOVED in Phase 0" section at top of this file.

### Session Management
- [ ] Session token is JWT signed with secret derived from encryption key — `app/config.py:127-144`
- [ ] Session expires after 7 days of inactivity — `app/config.py:31`
- [ ] Session cookie is httponly, secure, samesite=lax — `app/auth/routes.py:262-268`
- [ ] Multiple users can be logged in simultaneously (one session per browser)

### OOBE Wizard
- [ ] Step 1 (Welcome): overview + prerequisites — SPEC.md:46-53
- [ ] Step 2 (Google Cloud Credentials): Client ID/Secret + Test Connection — SPEC.md:56-70
- [ ] Step 3 (Admin Authentication): extracts home org domain from email — SPEC.md:71-79
- [ ] Step 4 (Email Alerts): SMTP host, port, username, password, from, alert emails — SPEC.md:81-92
- [ ] Step 5 (Encryption Key): auto-generates 32-byte key, requires confirmation checkbox — SPEC.md:102-110 *(was Step 6)*
- [ ] Step 6 (Complete): success message, next steps, link to dashboard — SPEC.md:112-119 *(was Step 7)*
- [ ] OOBE only runs when organization table is empty — `app/database.py`, `app/ui/setup.py`
- [ ] OOBE stores: OAuth credentials, home org domain, admin user, SMTP config, SA key — SPEC.md:121-130

### Factory Reset
- [ ] Admin can trigger factory reset from settings (admin only)
- [ ] Requires typing "RESET" for confirmation — `app/api/admin.py:83-86`, SPEC.md:783-796
- [ ] Deletes all users, calendars, tokens, sync state, events, alert queue — `app/api/admin.py`
- [ ] Returns to OOBE wizard on next access — `app/ui/setup.py`

---

## 2. Calendar Connections

### Adding Client Calendars
- [ ] User initiates "Connect Client Calendar" → Google OAuth — `app/auth/routes.py:281-300+`
- [ ] OAuth flow uses CLIENT_SCOPES (calendar, email, profile, openid) — `app/auth/google.py`
- [ ] After auth, user selects which calendar to sync from list
- [ ] Calendar marked active and inserted in `client_calendars` — `app/api/calendars.py:102-210`
- [ ] Sync state entry created with empty sync_token — `app/api/calendars.py:180-184`
- [ ] Auto-assigns next unused color (1–11) — `app/api/calendars.py:158-166`
- [ ] Initial sync triggered in background — `app/api/calendars.py:195-201`

### Disconnecting Calendars
- [ ] User clicks "Disconnect" on dashboard
- [ ] Triggers cleanup of managed events on all calendars — `app/sync/engine.py`
- [ ] Calendar marked `disconnected_at` (soft delete) — `app/api/calendars.py:240-245`
- [ ] All busy blocks created for this calendar deleted from other calendars
- [ ] All events synced from this calendar deleted from main
- [ ] Retention cleans disconnected_calendar records after 30 days — SPEC.md:322

### Personal Calendars (Read-Only)
- [ ] User authenticates Gmail account separately (PERSONAL_SCOPES)
- [ ] Marked as `calendar_type='personal'` in DB
- [ ] Events do NOT sync as detailed copies — README.md:101-111
- [ ] Create "Busy (Personal)" blocks on main + all client calendars
- [ ] Personal events completely hidden (no description shared)
- [ ] Can be disconnected like client calendars (30-day retention) — SPEC.md:322

### Webcal/ICS Subscriptions
- [ ] Subscribe to external ICS feeds via URL — README.md:113-115
- [ ] Optional `display_prefix` (e.g. "[ISO]", "[Travel]") — SPEC.md:528
- [ ] Configurable poll interval (default 5 min) — SPEC.md:530
- [ ] ETag caching: 304 Not Modified skips processing — SPEC.md:618
- [ ] Unstable UIDs detected and replaced with SHA256 content hash — SPEC.md:617-619
- [ ] Events created on main with "[prefix] title" format — SPEC.md:615
- [ ] "Managed by [BusyBridge]" footer in description — SPEC.md:615
- [ ] Busy blocks created on all client calendars — `app/sync/webcal_sync.py`
- [ ] Can be deleted like client calendars — `app/api/webcal.py`

### Main Calendar Selection
- [ ] On first login, primary calendar auto-detected and set — `app/auth/routes.py:228-245`
- [ ] User can change main calendar via Settings — `app/api/users.py:90-124`
- [ ] Main calendar ID verified accessible before saving — `app/api/users.py:106-108`
- [ ] Tracked separately in `main_calendar_sync_state` — SPEC.md:494-500

### Calendar Colors
- [ ] Each client calendar can be assigned one of Google's 11 colors — SPEC.md:675
- [ ] New calendars auto-assigned first unused color — `app/api/calendars.py:158-166`
- [ ] Color changeable via dashboard color-dot picker
- [ ] Color change recolors all existing events from that calendar
- [ ] Color-coded events on main make different clients visually distinct — SPEC.md:676

### Calendar Display Names
- [ ] Optional, defaults to Google calendar name — SPEC.md:382-383
- [ ] Customizable by user — `app/api/calendars.py:39`
- [ ] Stored in `client_calendars.display_name` — SPEC.md:382

---

## 3. Sync Behaviours (Sync Rules)

### Client → Main Sync
- [ ] New event on client → full-detail copy on main — `app/sync/rules.py:25-365`
- [ ] Copy includes: title, description, location, attendees list, time, recurrence — SPEC.md:166
- [ ] Event modified on client → updates main copy — `app/sync/rules.py:213-262`
- [ ] Event deleted on client → deletes main copy AND all busy blocks on other clients — `app/sync/rules.py:805-983`
- [ ] User edits synced event on main → syncs back to client IF user has edit rights — `app/sync/rules.py:1050-1099`
- [ ] User deletes synced event on main → declines event on client if attendee, else removes — `app/sync/rules.py:805-983`

### Main → Client Busy Block Sync
- [ ] New main event → "Busy" block on ALL connected client calendars — `app/sync/rules.py:503-749`
- [ ] Block title is "Busy" — `app/config.py:65`
- [ ] Block description is empty — SPEC.md:184
- [ ] Block visibility is Private
- [ ] Main event modified → updates times on all busy blocks — `app/sync/rules.py:676-718`
- [ ] Main event deleted → deletes busy blocks from all clients
- [ ] All-day "Free" events do NOT create busy blocks — SPEC.md:269
- [ ] All-day "Busy" events create all-day busy blocks

### Personal Calendar → Main + Clients
- [ ] New personal event → "Busy (Personal)" on main + all clients — `app/sync/rules.py:sync_personal_event_to_all`
- [ ] No event details shared — README.md:101-111
- [ ] Block title from `app/config.py:66` ("Personal" / "Busy (Personal)")
- [ ] Personal events never sync as detailed copies (read-only sources)

### Webcal/ICS → Main + Clients
- [ ] External ICS feeds fetched during periodic sync and main sync — `app/jobs/sync_job.py:57-71`
- [ ] Events created on main with "[prefix] title" — SPEC.md:615
- [ ] "Managed by [BusyBridge]" footer added — SPEC.md:615
- [ ] Busy blocks created on all client calendars — SPEC.md:612-619
- [ ] Events matched across polls using stable UIDs (or SHA256 hash) — SPEC.md:617-619

### Busy Block Creation Rules
- [ ] Only created if event does not have transparent "Show As" — SPEC.md:179
- [ ] Honored for all-day and timed events
- [ ] Not created for events originating from same client (avoid self-block) — `app/sync/rules.py:659-661`
- [ ] Not created if event is cancelled — `app/sync/rules.py:52-54`

### Free/Busy Honoring
- [ ] Events marked "Show as: Free" do not create busy blocks — SPEC.md:179
- [ ] Applies to all-day and timed events — SPEC.md:179, 269
- [ ] Check made in `should_create_busy_block()` — `app/sync/google_calendar.py`

### All-Day Event Handling
- [ ] All-day detected via `date` field (not `dateTime`) — `app/sync/rules.py:202-208`
- [ ] All-day "Free" syncs to main but no busy blocks — SPEC.md:269
- [ ] All-day "Busy" creates all-day busy blocks
- [ ] All-day status and Show-as preserved during sync

### Recurring Events
- [ ] Sync as recurring events with RRULE preserved — SPEC.md:205-220
- [ ] Parent tracked; instances NOT expanded into individual rows — `app/sync/rules.py:211, 580`
- [ ] Single-instance modifications (exceptions) tracked separately — `app/sync/rules.py:124-158`
- [ ] Single-instance cancellations propagated as EXDATE to all copies — `app/sync/rules.py:752-802`
- [ ] Rescheduled series (_R suffix in event ID) detected and re-keyed — `app/sync/rules.py:82-118`
- [ ] Old-parent instance mappings cleaned up after re-key — `app/sync/rules.py:111-118, 368-458`
- [ ] Parent/instance relationships in `origin_recurring_event_id` — SPEC.md:410
- [ ] Cancelled instances detected via `list_cancelled_instances()` and cancelled on busy blocks — `app/sync/rules.py:707-744`

### RSVP Propagation
- [ ] User accepts/declines on main (responseStatus on attendee) — `app/sync/rules.py:985-1047`
- [ ] RSVP propagated back to client event (patch attendee responseStatus) — `app/sync/rules.py:1023-1028`
- [ ] RSVP stored in `event_mappings.rsvp_status` — SPEC.md:544
- [ ] Only triggered if user is attendee on original event — `app/sync/rules.py:1015-1016`

### Edit-Rights Detection
- [ ] User is organizer → can edit
- [ ] User has guestsCanModify → can edit
- [ ] User has explicit writer ACL → can edit
- [ ] Non-editable marked `user_can_edit=FALSE` in mapping — `app/sync/rules.py:161, 420`
- [ ] Editable client-origin events: edits on main sync back to client — `app/sync/rules.py:1050-1099`

### Lock Emoji (🔒) for Non-Editable Events
- [ ] Lock emoji prepended to title — `app/sync/google_calendar.py:copy_event_for_main`
- [ ] Revert mechanism restores original time when user moves a non-editable event on main — `_revert_if_moved` (extended to client copies in Phase 0)
- [ ] Same mechanism applies to: non-editable client copies on main, personal busy blocks on main, webcal events on main, busy blocks on client calendars

### "Managed by [BusyBridge]" Footer
- [ ] Appended to descriptions of all synced copies
- [ ] Appended to webcal/ICS events — SPEC.md:615
- [ ] Used to identify cleanable events on disconnect — `app/sync/engine.py:_event_has_managed_prefix`

### Event Origin Tracking
- [ ] Each mapping stores `origin_type` ('main', 'client', 'personal', 'webcal') — SPEC.md:407
- [ ] `origin_calendar_id` (NULL if origin_type='main') — SPEC.md:408
- [ ] `origin_event_id` (event ID at source) — SPEC.md:409
- [ ] Extended properties: `bb_origin_id`, `bb_mapping_id`, `bb_type` for cross-check
- [ ] Loop prevention: events tagged `calendarSyncEngine=true` skipped — SPEC.md:596, `app/sync/rules.py:48-50`

### Colored Event Copies on Main
- [ ] Synced client events inherit client calendar's color — `app/sync/rules.py:191-192`
- [ ] Color applied via colorId field
- [ ] Origin obvious at a glance — SPEC.md:676

### Attendee/RSVP Display
- [ ] Attendee list from original client event included in main copy description
- [ ] RSVP status tracked and displayed — `app/sync/rules.py:179, 245-247, 336-338`
- [ ] Response status preserved across re-syncs

### Conflict Handling
- [ ] Overlapping events from different clients both sync to main — SPEC.md:281-287
- [ ] UI shows them overlapping (real conflict surfaced)
- [ ] No automatic conflict resolution

---

## 4. Triggers

### Webhook Receipt + Verification Re-fetch
- [ ] Google sends POST to `/api/webhooks/google-calendar` — `app/api/webhooks.py:18-141`
- [ ] Headers validated: X-Goog-Channel-ID, -Token, -Resource-ID, -Resource-State
- [ ] Token verified with HMAC comparison — `app/api/webhooks.py:67-71`
- [ ] Resource ID must match registered channel — `app/api/webhooks.py:76-85`
- [ ] Expired channels detected and removed — `app/api/webhooks.py:88-99`
- [ ] Sync triggered with 5-second debounce — `app/api/webhooks.py:111`
- [ ] Verification re-sync scheduled 40s later for cross-session consistency — `app/sync/engine.py:148`
- [ ] Verification only for webhook-triggered syncs, not periodic — `app/api/webhooks.py:115, 127`
- [ ] Multiple rapid webhooks reset the verification timer (accumulate event IDs) — `app/sync/engine.py:176-179`

### Periodic 5-Minute Sync
- [ ] Scheduled every 5 minutes — `app/jobs/scheduler.py:24-30`
- [ ] Syncs all active client/personal/main calendars + webcal subs — `app/jobs/sync_job.py:14-95`
- [ ] Skips paused users — `app/jobs/sync_job.py:34`
- [ ] Uses sync token (incremental); falls back to full list if expired — `app/sync/google_calendar.py:147-201`
- [ ] No verification re-sync — `app/jobs/sync_job.py:47`

### Manual Per-Calendar Sync
- [ ] User clicks "Sync Now" on dashboard
- [ ] 25-second settling delay — `app/api/calendars.py:259-295`
- [ ] Tracks progress (settling → syncing → complete) — `app/sync/engine.py:264-276`
- [ ] Schedules verification if events processed — `app/sync/engine.py:278-285`

### Manual Full Re-Sync
- [ ] User clicks "Full Re-sync" in Settings
- [ ] Clears all sync tokens for user's calendars — `app/api/sync.py:211-228`
- [ ] Next sync does full fetch (no incremental token)
- [ ] Logs to sync_log — `app/api/sync.py:238-244`

### Per-Calendar Cleanup & Re-Sync
- [ ] Deletes only this calendar's managed events
- [ ] Clears sync token for this calendar only
- [ ] Triggers immediate re-sync

### Global Cleanup & Pause
- [ ] Two-pass: DB-driven deletion + prefix sweep for orphans — SPEC.md:701
- [ ] Clears all sync tokens
- [ ] Pauses sync for user
- [ ] Live progress tracking with step labels — `app/sync/engine.py:_update_cleanup_progress`

### Global Cleanup Only (No Pause)
- [ ] Same cleanup, sync resumes on next scheduled run

### Debounce Behaviour
- [ ] Webhook syncs: 5s debounce — `app/api/webhooks.py:111`
- [ ] Manual per-calendar syncs: 25s debounce — `app/api/calendars.py:283`
- [ ] Periodic syncs: 0 debounce
- [ ] Multiple webhooks within debounce window reset timer — `app/sync/engine.py:176-179`
- [ ] Debounce waits before fetching; verification waits after sync — `app/sync/engine.py:250-261, 186-203`

---

## 5. Background Jobs

### Periodic Sync (every 5 minutes)
- [ ] Defined in `app/jobs/scheduler.py:24-30`
- [ ] Executes `run_periodic_sync()` — `app/jobs/sync_job.py:14-95`
- [ ] Syncs all active calendars (client, personal, main, webcal)
- [ ] Skips paused users
- [ ] Job lock prevents concurrent runs — `app/jobs/sync_job.py:233-268`
- [ ] Errors logged per calendar but continue for others

### Webhook Renewal (every 6 hours)
- [ ] Renews channels expiring within 24 hours — `app/jobs/webhook_renewal.py`
- [ ] Google max lifetime is 7 days; BB registers for 6 days — SPEC.md:576, 167
- [ ] Initial registration runs at startup — `app/jobs/scheduler.py:42-47`
- [ ] Disabled if `ENABLE_WEBHOOKS=false` — `app/jobs/scheduler.py:48-49`

### Consistency Check (every hour)
- [ ] Verifies event_mappings still exist on origin and main — `app/sync/consistency.py:62-239`
- [ ] If origin deleted but copies remain: deletes copies
- [ ] If copies deleted but origin remains: recreates copies
- [ ] Logs discrepancies to sync_log
- [ ] Per-user results in `integrity_status`

### Orphan Scan (every 6 hours)
- [ ] Finds events on Google not tracked in DB — SPEC.md:705
- [ ] Cleans up orphaned events
- [ ] Detects/deduplicates DB rows

### Token Refresh (every 30 minutes)
- [ ] Finds tokens expiring within 1 hour — `app/jobs/sync_job.py:192-231`
- [ ] Proactively refreshes via `get_valid_access_token()`
- [ ] Detects revoked tokens (invalid_grant) and queues alert

### Alert Queue Processing (every minute)
- [ ] Sends queued email alerts (alert_queue table) — `app/jobs/alerts.py`
- [ ] Retries failed sends up to 3 times with exponential backoff
- [ ] De-duplicates: same alert type + calendar within 1 hour — `app/alerts/email.py:115-128`
- [ ] Marks `sent_at` after successful send

### Retention Cleanup (daily at 3 AM)
- [ ] Deletes event_mappings per retention policy — SPEC.md:315-322
- [ ] Deletes sync_log entries older than 90 days — SPEC.md:321
- [ ] Deletes disconnected_calendar records older than 30 days — SPEC.md:322
- [ ] Deletes old backups per retention policy — SPEC.md:665, README.md:272

### Stale Alert Cleanup (daily at 4 AM)
- [ ] Deletes alert_queue entries older than 7 days — SPEC.md:1064

### Daily Backup (daily at 11 PM)
- [ ] Creates backup ZIP (DB + optional ICS exports) — `app/sync/backup.py`
- [ ] Retention: 7 daily, 2 weekly, 6 monthly — SPEC.md:665
- [ ] Triggers ICS exports (full + clean) — SPEC.md:640-668
- [ ] Stored in `/data/backups/` — SPEC.md:272

### Job Locking
- [ ] Each job acquires lock before running — `app/jobs/sync_job.py:233-261`
- [ ] Lock with timeout (30 min default)
- [ ] Stale locks cleaned up automatically — `app/jobs/sync_job.py:244-248`
- [ ] Prevents concurrent runs of same job

---

## 6. Failure Handling

### Circuit Breaker (Auto-Pause on Systemic Failure)
- [ ] Triggers when ALL active calendars have 3+ consecutive failures — `app/jobs/sync_job.py:11, 120-124`
- [ ] Auto-pauses sync globally and queues alert — `app/jobs/sync_job.py:135-147`
- [ ] Prevents wasting API quota and flooding logs — SPEC.md:559

### Per-Calendar Consecutive-Failure Tracking
- [ ] `calendar_sync_state.consecutive_failures` increments on error
- [ ] Resets to 0 on successful sync
- [ ] Warning at 1+; error at 5+ — SPEC.md:74
- [ ] Circuit breaker at 3+ — `app/jobs/sync_job.py:11`

### Alert Thresholds
- [ ] "Sync failures" at 5+ consecutive — SPEC.md:719
- [ ] "Token revoked" on invalid_grant — `app/jobs/sync_job.py:224-230`
- [ ] "Calendar inaccessible" on 404/403
- [ ] "Webhook registration failed" if registration fails
- [ ] "System error" on unhandled exceptions — `app/main.py:224-240`

### Sync Token Preservation on Failure
- [ ] Sync token NOT updated if sync fails
- [ ] Allows retry with same token on next run — SPEC.md:603, 1099
- [ ] Token cleared on full re-sync or cleanup

### Missed Busy Block Retry
- [ ] If creation fails, exception logged but other blocks continue — `app/sync/rules.py:746-747`
- [ ] Retried on next sync — `app/sync/rules.py:676-718`
- [ ] If update fails, replaced with new block — `app/sync/rules.py:687-716`
- [ ] `_retry_missing_busy_blocks` scans for gaps and refills — `app/sync/engine.py:729`

### Partial-Cleanup Tracking
- [ ] Tracks successfully-deleted events
- [ ] Only removes DB rows for events confirmed deleted on Google
- [ ] Leaves DB rows for failed deletions for retry
- [ ] Two-pass cleanup: DB-driven + prefix sweep — SPEC.md:701

### Transient Error Retry Strategy
- [ ] Network timeouts: exponential backoff — `app/sync/google_calendar.py:111-145`
- [ ] 5xx errors: exponential backoff — `app/sync/google_calendar.py:127-137`
- [ ] Rate limits (403/429): global backoff — `app/sync/google_calendar.py:132-135`
- [ ] Max 5 retries; 1/2/4/8/16s schedule
- [ ] Rate limit backoff: 4/8/16/32/60s

### Permanent Error Handling
- [ ] Permission denied (non-rate-limit 403): fail immediately
- [ ] Token revoked (invalid_grant): alert user, mark token invalid
- [ ] Calendar deleted (404): alert user, mark calendar inaccessible
- [ ] Insufficient permissions (403): alert user to re-auth

---

## 7. Admin Features

### User Management
- [ ] List all users with search/filter — `app/api/admin.py:167-202`
- [ ] View user details (calendars, last login, sync history) — `app/api/admin.py:205-255`
- [ ] Trigger sync for any user — `app/api/admin.py:258-280`
- [ ] Force user re-authentication — `app/api/admin.py:283-350+`
- [ ] Promote user to admin
- [ ] Delete user and all data
- [ ] Disconnect calendar on behalf of user

### Sync Pause (Global)
- [ ] Sets `sync_paused='true'` in settings — `app/api/sync.py:252`
- [ ] All periodic syncs check this flag and skip — `app/jobs/sync_job.py:17-20`
- [ ] Resumable by admin — `app/api/sync.py:265-278`

### Sync Pause (Per-User)
- [ ] User pauses own sync — `app/api/sync.py:281-294`
- [ ] Sets `sync_paused=TRUE` in users table
- [ ] Periodic syncs skip this user — `app/jobs/sync_job.py:34`
- [ ] User can resume own sync

### System Health View
- [ ] Total/active users (24h) — `app/api/admin.py:88-164`
- [ ] Total/active calendars, events synced, busy blocks
- [ ] Sync errors in last 24h
- [ ] Active/expiring webhooks
- [ ] Database size
- [ ] Sync pause status

### Integrity Status
- [ ] Stored in `integrity_status` (`last_check_at`, `issues_found`, `issues_auto_fixed`, `unresolved_issues`) — SPEC.md:514-521
- [ ] Dashboard displays integrity status — `app/ui/routes.py:103-116`
- [ ] Unresolved issues listed in `details_json`

### Log Viewer
- [ ] Filterable sync log at `/admin/logs`
- [ ] Filter by user, calendar, action, status, date range — SPEC.md:789
- [ ] Shows: timestamp, calendar, action, status, details — `app/api/sync.py:183-196`
- [ ] Paginated (50 per page)

### Factory Reset
- [ ] Requires "RESET" confirmation
- [ ] Deletes all users, calendars, tokens, events, backups, logs
- [ ] Returns to OOBE wizard
- [ ] Confirmed with popup warning — SPEC.md:783-796

### Service Account Management
**REMOVED in Phase 0** — see "Features REMOVED in Phase 0" section at top of this file.

### Sync Activity Feed
- [ ] Real-time feed of recent sync events — `app/sync/engine.py:_log_activity`
- [ ] Shown on dashboard
- [ ] Includes timestamp, action, calendar, detail, level — `app/sync/engine.py:52-65`
- [ ] Last 50 events retained — `app/sync/engine.py:49`

### SMTP Configuration
- [ ] Admin sets host, port, username, password, from, alert emails — SPEC.md:81-92
- [ ] Password encrypted in database — `app/alerts/email.py:36-37`
- [ ] Test email button — SPEC.md:91, `app/alerts/email.py:209-220`

---

## 8. UI Surfaces

### Dashboard Layout
- [ ] Current user info (email, main calendar) — `app/ui/routes.py:130-142`
- [ ] Sync status summary (last sync, errors) — `app/ui/routes.py:74-102`
- [ ] List of connected client calendars with display name, account email, last sync, status icon, "Sync Now" button, Disconnect button, color picker, event/busy block counts

### Status Grid
- [ ] Total calendars, healthy / warning / error counts — `app/ui/routes.py:75-84`
- [ ] Color-coded status — `app/api/sync.py:73-79`
- [ ] Total events synced, total busy blocks
- [ ] Sync paused indicator

### Integrity Checker Live Status
- [ ] Last check timestamp
- [ ] Issues found / auto-fixed / unresolved counts
- [ ] Status icon: ok / warning / error
- [ ] Consecutive check failures tracked

### Color Picker
- [ ] Interactive dot selector for Google colors 1-11
- [ ] Click to change; immediately recolors events
- [ ] Auto-assigns unused colors on connect — `app/api/calendars.py:158-166`

### Per-Calendar Progress Bars
- [ ] During manual sync: settling countdown, syncing, complete
- [ ] Polled via `/api/client-calendars/{id}/sync-progress`
- [ ] Shows event count processed

### Sync Activity Feed
- [ ] Real-time log of recent sync events
- [ ] Newest first
- [ ] Displayed on dashboard

### Settings Page
- [ ] Main calendar selector
- [ ] Email notification preferences
- [ ] Link to sync history
- [ ] Full Re-sync, Cleanup & Re-sync, Cleanup & Pause buttons
- [ ] "Disconnect All & Start Fresh" with confirmation

### Sync Control Page
- [ ] Full Re-sync, Cleanup & Re-sync, Cleanup & Pause
- [ ] Connection Health Check (tests all OAuth tokens)
- [ ] Live progress tracking during cleanup
- [ ] Managed event prefix display

### Exports Page
- [ ] Full ICS export (ZIP, one .ics per calendar)
- [ ] Clean ICS export (BB-managed events filtered)
- [ ] Automatic daily exports
- [ ] Retention 7d/2w/6m
- [ ] Manual export creation

---

## 9. API Surface

### User Endpoints (Authenticated)
- [ ] `GET /api/me` — profile
- [ ] `PUT /api/me/main-calendar` — set main
- [ ] `GET /api/me/calendars` — list user's Google calendars
- [ ] `GET /api/me/alert-preferences` — get
- [ ] `PUT /api/me/alert-preferences` — update

### Client Calendar Management
- [ ] `GET /api/client-calendars`
- [ ] `POST /api/client-calendars` — connect after OAuth
- [ ] `DELETE /api/client-calendars/{id}` — disconnect
- [ ] `POST /api/client-calendars/{id}/sync` — manual sync with settling delay
- [ ] `GET /api/client-calendars/{id}/status` — detailed sync status
- [ ] `GET /api/client-calendars/{id}/sync-progress` — poll live sync progress
- [ ] `PATCH /api/client-calendars/{id}` — update color, display name

### Personal Calendar Management
- [ ] `GET /api/personal-calendars`
- [ ] `POST /api/personal-calendars`
- [ ] `DELETE /api/personal-calendars/{id}`
- [ ] `POST /api/personal-calendars/{id}/sync`

### Sync Status & Logs
- [ ] `GET /api/sync/status` — overall sync status, counts, paused flag
- [ ] `GET /api/sync/log` — recent activity, paginated, filterable
- [ ] `POST /api/sync/full` — clear all sync tokens
- [ ] `POST /api/sync/pause` — global pause (admin)
- [ ] `POST /api/sync/resume` — global resume (admin)
- [ ] `POST /api/sync/my/pause` — per-user pause
- [ ] `POST /api/sync/my/resume` — per-user resume
- [ ] `GET /api/sync/cleanup-progress` — poll cleanup progress
- [ ] `GET /api/sync/activity` — recent activity feed
- [ ] `GET /api/sync/integrity` — integrity check status

### Webcal/ICS Subscriptions
- [ ] `GET /api/webcal`
- [ ] `POST /api/webcal` — URL + display_prefix
- [ ] `DELETE /api/webcal/{id}`
- [ ] `POST /api/webcal/{id}/sync`
- [ ] `PATCH /api/webcal/{id}` — URL, prefix, poll interval

### Backup
- [ ] `GET /api/backups` — list (name, size, timestamp, type)
- [ ] `POST /api/backups` — create manual backup
- [ ] `POST /api/backups/restore` — restore from ZIP
- [ ] `GET /api/backups/{id}/download`
- [ ] `DELETE /api/backups/{id}`

### Admin Endpoints
- [ ] `GET /api/admin/health`
- [ ] `GET /api/admin/users`
- [ ] `GET /api/admin/users/{id}`
- [ ] `POST /api/admin/users/{id}/sync`
- [ ] `POST /api/admin/users/{id}/force-reauth`
- [ ] `DELETE /api/admin/users/{id}`
- [ ] `PUT /api/admin/users/{id}/admin`
- [ ] `GET /api/admin/logs`
- [ ] `POST /api/admin/sync/pause` / `resume`
- [ ] `GET /api/admin/settings` / `PUT`
- [ ] `POST /api/admin/settings/test-email`
- [ ] `POST /api/admin/factory-reset` (RESET)
- [ ] `GET /api/admin/export`

### Webhook Endpoint
- [ ] `POST /api/webhooks/google-calendar`
- [ ] Validates channel ID, token, resource ID, expiration
- [ ] Triggers sync with 5s debounce + 40s verification

### Authentication Endpoints
- [ ] `GET /auth/login`
- [ ] `GET /auth/callback`
- [ ] `POST /auth/logout`
- [ ] `GET /auth/connect-client` + `/callback`
- [ ] `GET /auth/connect-personal` + `/callback`
- [ ] `POST /auth/disconnect-calendar/{id}`

### Public/Setup
- [ ] `GET /` — redirect to /app or /setup
- [ ] `GET /health`
- [ ] `GET /setup`
- [ ] `POST /setup/step/{step}`

---

## 10. Data Lifecycle & Retention

### Soft-Delete vs Hard-Delete
- [ ] event_mappings uses soft-delete (deleted_at) for recurring — `app/sync/rules.py:973-977`
- [ ] event_mappings uses hard-delete for non-recurring — `app/sync/rules.py:979`
- [ ] Soft-delete prevents resurrect of intentionally-removed events — `app/sync/rules.py:69-74`
- [ ] busy_blocks hard-deleted
- [ ] client_calendars soft-deleted on disconnect — `app/api/calendars.py:240-245`

### Retention Windows
- [ ] Single (non-recurring) event mappings: 30 days after event_end — SPEC.md:315
- [ ] Recurring series mappings: kept indefinitely while series exists — SPEC.md:318
- [ ] Recurring instance modifications: kept as long as parent series exists — SPEC.md:319
- [ ] Soft-deleted recurring: 30 days after deletion — SPEC.md:320
- [ ] Audit/sync log entries: 90 days — SPEC.md:321
- [ ] Disconnected calendar records: 30 days — SPEC.md:322

### Backup Retention
- [ ] 7 daily, 2 weekly, 6 monthly — SPEC.md:665, README.md:272
- [ ] Applied by daily backup job

### ICS Export Retention
- [ ] Same 7d/2w/6m
- [ ] Triggered daily alongside backup job

### Audit Log Retention
- [ ] sync_log: 90-day retention
- [ ] Cleaned up by retention_cleanup job

### Alert Queue Lifecycle
- [ ] alert_queue stores queued emails
- [ ] sent_at on success
- [ ] attempts increments on failure
- [ ] Max 3 retries with backoff
- [ ] Stale alerts (sent or failed) older than 7 days cleaned up

---

## 11. Security Controls

### Webhook Auth
- [ ] X-Goog-Channel-Token validated with HMAC — `app/api/webhooks.py:67-71`
- [ ] Legacy channels without token accepted for rolling-deploy safety
- [ ] X-Goog-Resource-ID must match registered channel
- [ ] Expired channels detected and removed

### Rate Limiting
- [ ] Global endpoint: 60 req/min — `app/config.py:42`
- [ ] Webhook endpoint: 30 req/min — `app/config.py:43`
- [ ] Auth endpoint: 10 req/min — `app/config.py:44`
- [ ] slowapi middleware — `app/main.py:167-169`
- [ ] Google API: 5 req/s sustained, 5 burst — `app/sync/google_calendar.py:33-77`
- [ ] Global backoff on rate-limit error

### SSRF Protection
- [ ] Webcal URLs validated (no localhost/127.0.0.1) — `app/sync/webcal_sync.py`
- [ ] HTTP requests use timeouts

### OAuth State CSRF
- [ ] State token = `secrets.token_urlsafe(32)` — `app/auth/routes.py:120`
- [ ] Stored in DB with 10-minute TTL
- [ ] One-time use (deleted after read)
- [ ] Mismatch/expiry → 400

### Open-Redirect Protection
- [ ] "next" parameter sanitized (must start with "/", not "//") — `app/auth/routes.py:116-117`
- [ ] Fallback to `/app` if invalid

### Encryption-at-Rest
- [ ] OAuth tokens AES-256-GCM — `app/encryption.py`
- [ ] SMTP password encrypted — `app/alerts/email.py:36-37`
- [ ] Encryption key 32 bytes minimum — `app/config.py:122`
- [ ] Key in separate file (not DB or env vars) — `app/config.py:103-124`

### Backup ID Validation
- [ ] Restore requires valid backup ID
- [ ] Prevents arbitrary file access — SPEC.md:1039
- [ ] Backup ZIP validated before restore

### Session Security
- [ ] JWT signed with secret derived from encryption key
- [ ] 7-day expiration on inactivity
- [ ] httponly, secure, samesite=lax cookie

### Domain Restriction
- [ ] Home org domain verified at OAuth callback
- [ ] Rejects login attempts from other domains
- [ ] Test mode allows email allowlist instead
- [ ] Enforced for ALL login attempts (not just UI)

### Input Validation
- [ ] Pydantic models validate all API inputs
- [ ] Calendar IDs, event IDs, emails validated
- [ ] Factory reset requires "RESET" exact match

---

## 12. Email Alerts

### Alert Types
- [ ] Token Revoked (invalid_grant)
- [ ] Calendar Inaccessible (404/403)
- [ ] Sync Failures (5+ consecutive) — SPEC.md:719
- [ ] Webhook Registration Failed
- [ ] System Error (unhandled exceptions)
- [ ] Integrity Issues
- [ ] Circuit Breaker (sync auto-paused) — `app/jobs/sync_job.py:140-147`

### SMTP Configuration
- [ ] Host, port, username, password (encrypted), from address
- [ ] TLS connection — `app/alerts/email.py:87`
- [ ] Test email — `app/alerts/email.py:209-220`

### Alert Queue
- [ ] Queued in alert_queue table
- [ ] Processed every minute
- [ ] Retried up to 3 times with backoff
- [ ] De-duplicated: same alert type + calendar within 1 hour

### Email Content
- [ ] Plain text + HTML — `app/alerts/email.py:68-77`
- [ ] Includes: timestamp, alert type, affected calendar, error details, suggested action
- [ ] Footer: dashboard link, manage preferences

### Recipients
- [ ] Affected user (if user_id provided)
- [ ] Admin emails (from `alert_emails` setting)
- [ ] De-duplicated

### Alert Cleanup
- [ ] Stale alerts (sent or failed, >7 days) cleaned up daily at 4 AM

---

## 13. Backup & Export

### Automated Daily Backup
- [ ] Triggered at 11 PM UTC
- [ ] ZIP contains DB backup + optional ICS exports
- [ ] Stored in `/data/backups/`
- [ ] 7d/2w/6m retention

### Restore Procedure
- [ ] Startup restore: `restore-pending.zip` next to DB → restore before sync starts — `app/main.py:67-105`
- [ ] API: `POST /api/backups/restore`
- [ ] Clears sync tokens after restore — `app/main.py:124-130`
- [ ] Archives restore file to prevent re-trigger — `app/main.py:81-85`

### ICS Full Export
- [ ] ZIP with one .ics file per calendar
- [ ] Preserves: attendees + PARTSTAT, organizer, Meet/conference data, attachments, RRULE + EXDATE, event types, guest permission flags, visibility, transparency, X-APPLE-CALENDAR-COLOR

### ICS Clean Export
- [ ] Same as full but BB-managed events filtered
- [ ] Useful for migration to another tool
- [ ] Manual or automatic (alongside daily backup)

### Export Retention
- [ ] Same 7d/2w/6m policy

### Manual Backup
- [ ] User can create on-demand via `/app/settings`
- [ ] API: `POST /api/backups`

### Backup Download
- [ ] `GET /api/backups/{id}/download` returns ZIP

### Backup Deletion
- [ ] `DELETE /api/backups/{id}` removes from filesystem

---

## 14. Edge Cases

### Orphaned Events Handling
- [ ] Consistency check detects events on Google not tracked in DB
- [ ] Orphan scan every 6 hours
- [ ] Orphaned events deleted from Google
- [ ] Two-pass: DB-driven + prefix sweep — SPEC.md:701

### Stable Webcal UIDs
- [ ] Detects feeds with bare UUID v4 UIDs that change every request
- [ ] Replaces with SHA256(summary + start + end)
- [ ] Stable matching across polls — `app/sync/webcal_sync.py`, `app/sync/ics_parser.py`

### Recurring Event Re-Keying
- [ ] Rescheduled series get new event ID with `_R` suffix
- [ ] Mapping re-keyed from old to new ID
- [ ] Old instance-level mappings cleaned up
- [ ] Prevents duplicate recurring events on main

### Rescheduled "This-and-Following" Series
- [ ] Detected and re-keyed
- [ ] Old instance mappings deleted
- [ ] New parent's series generates correct instances on main

### Soft-Deleted Mappings Prevent Resurrect
- [ ] User-deleted on main → soft-delete with `deleted_at`
- [ ] Next sync skips client event (no resurrection)

### User-Forked Events on Main Not Deleted
- [ ] User edits to synced event preserved on disconnect
- [ ] Only BB-created events deleted on disconnect

### Deleted-by-Organizer Cancellation Propagation
- [ ] Organizer-deleted events become "cancelled"
- [ ] Sync skips cancelled events
- [ ] Busy blocks cleaned up
- [ ] Main calendar copy deleted

### Free/Busy Honoring
- [ ] "Show as: Free" → no busy block
- [ ] Applies to all-day and timed events

### All-Day Event Handling
- [ ] All-day status preserved
- [ ] Show-as preserved
- [ ] Free → no block; Busy → all-day block

### Sync Token Expiry (410 Gone)
- [ ] Detected in `list_events()` — `app/sync/google_calendar.py:202-206`
- [ ] Triggers full sync instead of incremental
- [ ] Sync token cleared and next run fetches all events

### Modified Instances of Recurring Series
- [ ] Modified instance becomes new mapping
- [ ] Original series instance on main is cancelled
- [ ] Corresponding busy block instances cancelled
- [ ] New instance created as separate event on main

### Instance Cancellation (EXDATE)
- [ ] Single-occurrence cancellation syncs as cancellation
- [ ] Main copy instance cancelled
- [ ] Busy block instances cancelled
- [ ] Parent series remains intact

### Service Account Fallback
**REMOVED in Phase 0** — see top of this file. There is no SA mode after Phase 0, so no fallback.

### Immovable Events
**REMOVED in Phase 0.** "Native immovability" was a SPEC.md claim that doesn't hold up — the calendar owner has implicit edit rights on their own calendar regardless of organizer or `guestsCanModify`. Replaced uniformly by 🔒 emoji + revert-on-drift.

### Event Time Revert (uniform after Phase 0)
- [ ] Move on non-editable event on main → reverts on next sync cycle
- [ ] Within 5 seconds (webhook) to 5 minutes (periodic sync)
- [ ] Applies to non-editable client copies (NEW in Phase 0), personal busy blocks, webcal events, and busy blocks on client calendars
- [ ] Prevents accidental moves of read-only events

### Instance Event ID Derivation
- [ ] Instance ID derived from parent ID + originalStartTime — `app/sync/google_calendar.py:derive_instance_event_id`
- [ ] Used for cancelling/modifying specific instances without touching parent

### Personal Calendar Event Handling
- [ ] No detailed copies (read-only sources)
- [ ] "Busy (Personal)" blocks on main + all clients
- [ ] No details shared (privacy)

### Webcal Feed UID Instability
- [ ] Detected via bare UUID v4 check
- [ ] Replaced with SHA256 content hash
- [ ] Stable matching across polls

### ETag-Based Conditional Fetch
- [ ] If-None-Match header used
- [ ] 304 Not Modified → skip processing
- [ ] last_etag stored for next poll

### Loop Prevention
- [ ] Events tagged `calendarSyncEngine=true` skipped
- [ ] Verification syncs disabled for periodic/webhook
- [ ] Prevents Main→Client busy block triggering sync back to Main

### Multiple Users with Overlapping Clients
- [ ] Each user has separate OAuth tokens for same client org
- [ ] Each user's events tracked independently per user_id
- [ ] No cross-user pollution

### Sync After Cleanup
- [ ] All sync tokens cleared
- [ ] Next sync does full fetch from scratch
- [ ] Recreates all busy blocks
- [ ] Removes orphaned entries

### Cleanup Progress Tracking
- [ ] Live progress shown to user (settling → step labels → completion)
- [ ] Polled via `/api/sync/cleanup-progress`
- [ ] Tracks status, step, events_processed, total_events

---

**END OF FEATURE INVENTORY** — 350+ discrete, testable items across 14 categories.

Use this as the acceptance gate for the rewrite (`REWRITE_PLAN.md`): every checkbox must work identically (or better) in the new implementation.
