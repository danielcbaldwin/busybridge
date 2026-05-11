# Stage-5 Cutover Runbook

This is the operator-facing runbook for moving an existing
deployment onto the ledger architecture (REWRITE_PLAN.md §13).

The migration is **clean-cut**: legacy `event_mappings` /
`busy_blocks` tables are dropped, the ledger pipeline becomes the
only sync path.  Total user-visible downtime is ~5–15 minutes
during which "free/busy" views see the user as available; the
new system repopulates from scratch immediately after.

---

## Pre-flight (the day before)

1. **Back up the database off-server.**

   ```bash
   docker exec busybridge cp /data/calendar-sync.db /data/cutover-backup.db
   docker cp busybridge:/data/cutover-backup.db ./cutover-backup-$(date +%F).db
   ```

2. **Back up the encryption key off-server.**

   ```bash
   docker cp busybridge:/data/encryption.key ./cutover-encryption-$(date +%F).key
   ```

3. **Verify the new branch builds and tests pass on a staging
   container.**

   ```bash
   git fetch && git checkout claude/build-test-infrastructure-N3CL5
   docker compose -f docker-compose.test.yml up --build --abort-on-container-exit
   ```

4. **Schedule a low-traffic window.**  Weekend morning is ideal.

---

## Cutover steps (production, ~5–15 min visible downtime)

### 1. Pause sync via the admin UI

Navigate to `/admin/settings` and click "Pause sync globally" (or
POST `/api/sync/pause`).  This stops new writes immediately.

### 2. Run "Cleanup & Pause" for every user

For each user (from `/admin/users`):

* POST `/api/admin/ledger/users/{user_id}/cleanup-and-pause`

This sets every projection's `desired_state` to `absent` and pauses
the user.  The outbox drains the deletes; within 1–2 minutes every
BusyBridge-managed event is gone from every calendar.

Verify by checking the admin dashboard — `total_busy_blocks` and
`total_events_synced` should converge to 0.

### 3. Stop the container

```bash
docker compose down
```

### 4. Fetch the new branch + restart

```bash
git fetch && git checkout claude/build-test-infrastructure-N3CL5
docker compose up -d
docker logs -f busybridge
```

Watch for `Database schema initialized`.  The schema migration
inside `init_schema` calls `DROP TABLE IF EXISTS event_mappings`
and `DROP TABLE IF EXISTS busy_blocks`; ledger tables are created
fresh.  `ENABLE_LEDGER_JOBS` defaults to `True` so the scheduler
picks up the ledger drain immediately.

### 5. Resume sync

For each user:

* POST `/api/admin/ledger/users/{user_id}/resume`

Watch the dashboard.  Within 5–15 minutes (depending on calendar
count + event count) every calendar's events repopulate.

---

## Verification

* **Total event count on the dashboard** should match what was
  there before cutover.
* **Per-calendar busy-block counts** should match the pre-cutover
  snapshot.
* **`/api/admin/ledger/permanent-failures/{user_id}`** should be
  empty.
* **`/api/sync/integrity`** should return `status=ok` for each
  user.

---

## Rollback (if anything goes catastrophically wrong)

Worst case, you've got the SQLite + encryption-key backups from
pre-flight.  Recovery:

```bash
docker compose down
git checkout <previous-main-branch>
docker cp ./cutover-backup-YYYY-MM-DD.db busybridge:/data/calendar-sync.db
docker cp ./cutover-encryption-YYYY-MM-DD.key busybridge:/data/encryption.key
docker compose up -d
```

Then run "Cleanup & Pause" via the old admin UI (which targets
the legacy tables), then "Resume" — the old system repopulates
from scratch.

No permanent data loss is possible:
* The OAuth tokens are untouched by the cutover migration.
* The user/calendar/connection metadata survives.
* Only the per-event derived state (`event_mappings`, `busy_blocks`)
  is dropped, and it's reconstructable from Google.

---

## What changed for users

Almost zero observable difference for normal operation.  The
plan's `§17 What Changes for Users` notes:

* **Service-account mode is gone.**  Non-editable events are now
  handled by the uniform 🔒-emoji + revert-on-drift mechanism.
* **OOBE setup wizard has 6 steps instead of 7** (step 5, SA
  upload, was removed).
* **Drift on non-editable events on main is now reverted
  automatically** (this was a silent gap in the legacy system).
* **Recurring-event cancellations stay cancelled** even after
  sync-token expiry (was the "recurring-cancellation amnesia"
  bug).
* **Webhook → drain latency is ~5 seconds** instead of up to
  30 (the scheduler's periodic tick interval is the fallback).

---

## After the cutover

1. **Watch logs for 24 hours.**  Specifically look for repeated
   `ledger drain processed N users (M failed)` lines — any
   non-zero `M` means investigate.
2. **Look at `/api/admin/ledger/permanent-failures/{user_id}`**
   for every user once a day for the first week.  Each entry is
   a specific event that needs manual review.
3. **Soak harness (Stage 3) is available** at `tests/soak/` for
   long-running adversarial validation.  Not required for a
   successful cutover; useful if you want extended confidence
   before retiring the rollback path.
