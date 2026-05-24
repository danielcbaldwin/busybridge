# Fake Google Calendar — quirks reference

This document is the authoritative list of real-Google-API
behaviours the fake faithfully reproduces.  Each row links to
the test(s) that pin the behaviour down.

The fake is **deliberately faithful to documented and observed
quirks**, including bugs.  Don't "fix" a quirk here without also
having a story for the real Google one (cf. REWRITE_PLAN.md §13
Stage 1).

---

## Core CRUD

| Behaviour | Test |
|---|---|
| Client-supplied `id` must be 5–1024 chars of base32hex (lowercase a-v + 0-9) | `test_insert_client_id_invalid_alphabet`, `test_insert_client_id_too_short` |
| Inserting with an existing `id` returns **409 Conflict** (even if the existing row is `status=cancelled`) | `test_insert_client_id_conflict_409`, `test_insert_client_id_conflict_after_delete_still_409` |
| Server-generated event IDs may use a richer alphabet (uppercase) — only **client-supplied** IDs are restricted | `_R` reschedule helper (see below) |
| `events.delete` flips status to `cancelled` but **does not remove the row** — subsequent incremental sync sees the cancellation | `test_delete_marks_cancelled_and_bumps_etag` |
| `events.delete` is idempotent — re-deleting a cancelled event succeeds silently | `test_delete_idempotent` |
| `events.update` on a cancelled event returns **404** | `test_update_on_cancelled_event_404` |
| Every mutation bumps `etag` + `sequence` + `updated` | `test_update_replaces_fields_and_bumps_sequence_and_etag` |
| `If-Match` precondition: stale etag → **412 Precondition Failed** | `test_update_with_stale_if_match_412`, `test_patch_with_stale_if_match_412`, `test_delete_with_stale_if_match_412` |
| `If-Match: *` matches anything | `test_update_with_wildcard_if_match_succeeds` |
| `patch` deep-merges `extendedProperties.private` and `.shared` rather than replacing | `test_patch_extended_properties_deep_merges` |

## `events.list`

| Behaviour | Test |
|---|---|
| **Full sync** (no syncToken): returns events in `[time_min, time_max]`; cancelled rows filtered out unless `show_deleted=True` | `test_full_sync_returns_all_confirmed_events`, `test_full_sync_skips_cancelled_by_default`, `test_full_sync_show_deleted_includes_cancelled` |
| **Incremental sync** (with syncToken): returns every event whose change cursor advanced past the token's cursor, including cancellations | `test_incremental_sync_returns_only_changes_since_token`, `test_incremental_sync_includes_cancellations` |
| `syncToken` + `timeMin/timeMax` is **mutually exclusive** → 400 | `test_incremental_sync_with_time_min_is_a_bad_request` |
| Unknown sync token → **410 Gone** (the client must fall back to full sync) | `test_incremental_with_unknown_token_410` |
| Sync tokens expire after ~30 days (configurable) → 410 Gone | `test_sync_token_expires_after_ttl`, `test_sync_token_still_valid_just_under_ttl`, `test_custom_ttl_honoured` |
| Pagination uses `nextPageToken`; `nextSyncToken` only appears on the **final page** | `test_pagination_splits_results` |
| Snapshot is consistent across pages — events created mid-pagination surface in the *next* incremental sync, not the current one | `test_sync_token_after_pagination_reflects_snapshot_cursor` |
| Page tokens are single-use → 410 on replay | `test_page_token_consumed_after_use` |
| Unknown page token → 410 | `test_unknown_page_token_410` |
| Stable chronological ordering across runs | `test_full_sync_returns_stable_chronological_order` |

## Recurring events

| Behaviour | Test |
|---|---|
| Instance event IDs are `{parentId}_YYYYMMDDTHHMMSSZ` (timed) or `{parentId}_YYYYMMDD` (all-day) | `test_derive_instance_id_for_timed_event`, `test_derive_instance_id_for_all_day_event`, `test_all_day_recurring_instance_id_format` |
| Offset datetimes are converted to UTC before computing the instance suffix | `test_derive_instance_id_with_offset_dt` |
| `events.get` on a derived instance ID **synthesises** the instance from the parent's RRULE when no override row exists | `test_get_synthesises_unmodified_instance` |
| `events.get` on a derived ID for a date NOT in the recurrence pattern → 404 | `test_get_synthesised_instance_outside_recurrence_404` |
| `events.update` / `patch` on a derived instance ID **materialises an exception** entry on the series | `test_update_on_derived_instance_id_materialises_override`, `test_patch_on_derived_instance_id_materialises_override` |
| `events.delete` on a derived instance ID materialises a **cancelled** exception entry | `test_delete_on_derived_instance_id_materialises_cancelled_override` |
| `events.instances` returns synthesised instances **and** overrides; overrides take precedence | `test_list_instances_uses_overrides_when_present` |
| `events.instances(showDeleted=True)` includes cancelled instance overrides | `test_list_instances_show_deleted_includes_cancelled` |
| `events.list(singleEvents=True)` expands recurring parents into per-instance results | `test_list_single_events_expands_parent` |
| `events.list(singleEvents=True)` skips cancelled overrides by default | `test_list_single_events_skips_cancelled_overrides_by_default` |
| **The recurring-cancellation amnesia bug** — even with `show_deleted=True`, full `events.list` does NOT return cancelled instance overrides. Only `events.instances(showDeleted=True)` and incremental sync surface them. | `test_full_sync_omits_cancelled_instance_overrides_with_show_deleted` (the bug), `test_incremental_sync_does_surface_cancelled_instance` (reliable path 1), `test_instances_endpoint_does_surface_cancelled_with_show_deleted` (reliable path 2) |

### The `_R` "this and following" reschedule quirk

When the user picks "this and following" in the Google Calendar UI to reschedule a recurring meeting, Google internally:

1. **Truncates** the original series's RRULE by adding `UNTIL=<boundary - 1s>`.
2. **Cancels** any modified-instance overrides on or after the boundary on the original series.
3. **Creates a new event** with id `<originalParentId>_R<YYYYMMDDTHHMMSSZ>` (server-generated; uses uppercase characters that are illegal for client-supplied IDs).

The fake exposes this through `reschedule_series_this_and_following()` as a test helper:

| Behaviour | Test |
|---|---|
| Creates `<parentId>_R<stamp>`-suffixed new series | `test_reschedule_creates_R_suffixed_new_series` |
| Original series gets truncated with UNTIL | `test_reschedule_truncates_original_series` |
| Modified-instance overrides on/after the boundary are cancelled | `test_reschedule_cancels_post_boundary_overrides` |
| 400 when called on a non-recurring event | `test_reschedule_400_for_non_recurring` |

The ledger ingest treats each `<base>_R<stamp>` segment as its **own** recurring series (an additive "this and following" split coexists with the UNTIL-truncated base and any earlier segments). It does **not** re-key the base onto the new segment: a modified instance stays parented to whichever segment its `recurringEventId` names, so its derived instance id materialises against a series that actually contains its date. (An earlier `_try_rekey_R_parent` collapsed coexisting segments and bulk-re-parented pre-boundary instances onto a later segment, causing permanent `events.update` 404s — see `test_moved_instance_survives_this_and_following`.)

## Failure injection

| Knob | Effect | Test |
|---|---|---|
| `network_error_rate` | Transport-layer `NetworkError` raised before the call reaches the store | `test_network_error_via_probability` |
| `rate_limit_rate` | HTTP 429 with `rateLimitExceeded` reason | `test_rate_limit_via_probability` |
| `server_error_rate` | HTTP 503 | `test_server_error_via_probability` |
| `sync_token_expiry_rate` | HTTP 410 on incremental list_events (independent of time-based TTL) | `test_injected_sync_token_expiry` |
| `mid_write_crash_rate` | Write completes (state mutated) but caller sees `NetworkError` — exercises idempotent retry | `test_mid_write_crash_persists_state_but_raises`, `test_mid_write_crash_on_update`, `test_mid_write_crash_on_delete` |
| `force_next(error)` | Force a specific error on the very next operation | `test_force_next_overrides_rates` |
| `force_next_crash_after_write()` | Force one mid-write crash | `test_force_next_crash_after_write_fires_once` |
| `seed=N` | Deterministic — same seed → same failure sequence | `test_deterministic_with_seed` |
| Pagination continuation is subject to injection (each page is its own HTTP request) | | `test_injection_fires_on_pagination_continuation` |
| No injector → never fails | | `test_no_injector_means_no_failures` |

## What we don't model

A short list of real-Google behaviours the fake omits, by design.  None of these affect the ledger pipeline's correctness arguments:

- **Conference data (Meet) auto-generation.** The fake passes `conferenceData` through unchanged; it doesn't generate a fresh URI.
- **Quota error subtypes** (`userRateLimitExceeded` vs `dailyLimitExceeded`).  All rate-limit-shaped errors collapse to 429.
- **Attendee notification flow.**  `sendUpdates`/`sendNotifications` parameters are accepted but the fake doesn't simulate email delivery.
- **Cross-calendar event movement.**  `events.move()` isn't implemented — the ledger pipeline doesn't use it.
- **Free/busy queries.**  `freebusy.query` isn't implemented; the ledger doesn't use it.
- **Domain-internal organizer behaviours.**  Real Google sometimes auto-adds the organizer as an attendee; the fake preserves whatever the caller supplies.
- **Daylight-saving time edge cases on RRULE expansion.**  We use `python-dateutil`'s rrule library, which is faithful but slow; deeply pathological RRULE strings may behave differently than real Google.

If a future test needs one of these, add it deliberately and pin the behaviour with a regression test.
