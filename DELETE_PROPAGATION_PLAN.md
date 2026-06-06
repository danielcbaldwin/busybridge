# Plan — Organizer-delete-on-main → propagate to source (safe re-enablement)

> **⏳ TEMPORARY working doc — delete when this feature ships.** Tracks the plan to safely
> re-enable delete propagation. Folds in the recovered RECURRING_R_DELETE_SOURCE_FIX.md brief.

## 1. Goal & semantic model
Make deleting an event **on the main calendar propagate to the source** — but only the
way it's safe and intuitive, mirroring how RSVP already works:

| You are… | Delete on main means… | Mechanism |
|----------|----------------------|-----------|
| **Organizer / full edit rights** | delete the real event at the source | (new) arm `source_delete_pending` |
| **Attendee only** | decline (RSVP "no") to the source | (existing) RSVP write-back |

Strictly scoped to `source_type='client'` events you organize or can edit. Personal/webcal
stay read-only (deleting their mirror = drift-revert, never propagate).

## 2. Current behavior (verified in code)
- RSVP "no" and time/detail edits **already propagate** for editable client events
  (`main.py:_maybe_apply_main_edit_back`, `payload.py:_render_origin_writeback`).
- **Delete propagation is disarmed.** The destructive op exists (`outbox.py:_do_delete_source`,
  gated in `diff.py:380-382` on `source_delete_pending`) but **nothing sets the flag** (commit
  `1e41a1c`, after the 2026-06-02 incident that destroyed ~166 real MLCommons occurrences).
- The genuine "user deleted our copy on main" signal already exists: `_mark_user_intentionally_deleted`
  (`main.py:272`) → planner suppresses all mirrors. Today it only **un-mirrors**; it never deletes
  the source. That's the hook to extend.

## 3. Two prerequisites the old brief required are ALREADY DONE
- **Durable provenance marker (was MANDATORY + open question O-1):** managed events now carry
  `extendedProperties.private.bb_proj_id` / `bb_target_kind`. This is the self-identifying tombstone
  marker Layer 1 needs to never mistake BB's own artifact for a user delete across sync-token resets.
- **`_R` occurrence-ownership foundation (Layer 2):** `strip_r_suffix` now matches real (no-`Z`)
  Google `_R` ids (commit `b50836a`), so segment↔family resolution works — the orphan guard is live.

## 4. Non-negotiable safety invariants (must not regress)
- `OP_DELETE_SOURCE` fires **only on positive proof of user intent** — never on "no live source
  occurrence." AND-gate of independent conditions; any miss → drift-revert (re-assert mirror).
- **Never target the series master id** with a delete (a derivation bug must not nuke a whole series).
- Personal/webcal sources strictly read-only. `_R` split stays **additive — no re-key**.
- `status=confirmed` revive, client-before-main ordering, and the no-write-loop floor all stay.
- Keep the two conservative "does-not-destructively-delete" tests passing until a phase explicitly,
  safely flips them.

## 5. Phased delivery (lowest risk first — ship value early, defer the dangerous case)

**Phase 0 — Repro + diagnosis only (no behavior change). ✅ DONE 2026-06-06.**
- O-0 ✓ confirmed: a user-deleted occurrence reaches `_ingest_managed_recurring_instance`
  (`main.py:470-581`); if the source occurrence is live it drift-reverts, else marks the instance
  `cancelled` → planner ABSENT → mirror deleted, but **never arms `source_delete_pending`**, so
  `OP_DELETE_SOURCE` (gated `diff.py:377-382`) stays unreachable.
- O-2 ✓: ABSENT comes from `status='cancelled'` (instance) / `user_intentionally_deleted` (series)
  via the planner universal overrides.
- O-3 (live DB scan): `source_delete_pending=1` count = **0** (fully disarmed). BUT the Layer-2 root
  cause is **still live** — MLCommons "MLC AIRR: AI Privacy…" rows have churned to **version 2179**
  (active; harmless — no writes/deletes, outbox empty — but real version inflation). **1,649** cancelled
  instance rows on mlcommons → Phase-3 migration scope is non-trivial; analyze before migrating.
- Smoke alarm landed: `tests/test_r_split_main_delete_source_safety.py::
  test_r_split_then_main_delete_never_deletes_source` — drives a real `_R` split + a post-boundary
  main-delete and asserts source survival + no `source_delete_pending` + churn-free. GREEN under
  current containment (the basic cancel-survival floor already existed in
  `test_main_managed_instance_cancellation.py`). Full suite: 746 passed / 3 xfailed.
- **Still open — O-1:** does a user-deleted managed-copy *tombstone* on Google preserve our
  `extendedProperties.private.bb_proj_id`? Needs a live Google API read (do at the start of Phase 1).

**Phase 1 — Single (non-recurring) event, organizer deletes on main → delete source. ✅ IMPLEMENTED (default OFF) 2026-06-06.**
- Config flag `DELETE_PROPAGATION_MODE` ∈ {off (default) | shadow | on} (`config.py`).
- Arming: `ingest/main._maybe_arm_organizer_source_delete` — in the `_mark_user_intentionally_deleted`
  path, arm `source_delete_pending` **only when** client source **AND** `user_can_edit` **AND**
  non-recurring **AND** mode==on (`shadow` logs only). Provenance here is just `proj_match` (the user
  deleted OUR managed copy, recognized by the projection's `google_event_id`) — sufficient for the
  non-recurring case, which has no `_R` churn-artifact ambiguity. (The durable `bb_proj_id` provenance
  AND-gate is still required for the recurring per-occurrence case — Phase 3.)
- Gate: `diff._decide` extended — `OP_DELETE_SOURCE` now fires for an instance OR a whole NON-recurring
  event, NEVER a recurring series master. `outbox._do_delete_source` already deletes by
  `source_event_id` (the whole event for a non-recurring row).
- Tests: `tests/test_delete_propagation_phase1.py` — off→survives, on→deletes, shadow→survives+not-armed,
  recurring-series→survives, non-editable→survives. Full suite 751 passed / 3 xfailed.
- **Default OFF → zero production behavior change** until `DELETE_PROPAGATION_MODE` is set.
- **Deferred:** attendee (not organizer) → RSVP-decline mapping (RSVP-decline itself already works when
  the user explicitly RSVPs no); a delete-by-an-attendee → auto-decline convenience is a follow-up.
- **Next:** run on live in `shadow` mode for a few days, eyeball the logged candidates, then flip to `on`.

**Phase 2 — Whole recurring SERIES delete (organizer) → delete source series. (MEDIUM risk)**
- Whole-series delete is far safer than per-occurrence; target the series master deliberately and
  only when the user deleted the whole managed series on main. Belt-and-suspenders: never derive the
  delete target from a stale/base id.

**Phase 3 — Per-occurrence & "this and following" (the `_R` case). (HIGH risk — the incident path)**
- Full **Layer 1** AND-gate (Condition A churn-floor: source instance not active at main-ingest;
  Condition B durable-provenance consumed-once + etag change; + Layer-2 "the owning segment covers
  this date") armed on the `_is_origin_writeback` projection.
- Full **Layer 2**: `owning_source_series(user_id, original_start)` helper (expand masters, honor
  `UNTIL`, match on `original_start`, **pre-boundary moved occurrence → BASE master**); fix
  `_ingest_managed_recurring_instance` (`main.py:490-496`) to parent to the owning segment and derive
  `source_event_id` from **that** segment — **at row creation only, no in-place re-key**; store no
  source id if no segment owns the date. This also stops generating the spurious main `OP_DELETE`.
- Migration (O-3) if live rows have base-derived ids that 404 — re-create, never re-key.
- Flip the 3 `@_CONSERVATIVE_FIX` xfail tests back to passing.

## 6. Test strategy (repro-first)
- Every destructive test asserts **source `client_a` survival**, not just peer survival.
- Organizer→delete vs attendee→decline; genuine-cancel-after-BB-write (recency-window ambiguity);
  parent-series-survival; pre-boundary moved occurrence stays on BASE; client-ingest-gap chaos;
  no-write-loop floor. Full suite + soak before each phase ships.

## 7. Rollout (mirrors how we deployed the _R fix)
- Feature flag `ENABLE_DELETE_PROPAGATION` (default **off**).
- **Shadow/dry-run mode first**: log exactly what WOULD be source-deleted without doing it; watch on
  live for a few days and eyeball the candidates before arming for real.
- Enable **Phase 1 only** as canary; backup + watch a reconcile cycle; then Phase 2, then Phase 3.

## 8. Open questions to settle before Phase 3
- O-0 linchpin proof; O-1 (does a user-deleted managed-copy tombstone preserve our `bb_proj_id`? —
  needs a live check); O-3 migration scope; O-4 fake fidelity; O-5 client-ingest-gap timing.

## 9. Recommendation
Ship **Phase 1** (single-event organizer delete) soon — low-risk, high everyday value, unblocks the
core ask. Treat Phase 3 (`_R` per-occurrence) as a separate, carefully-gated effort behind the
dry-run flag, since it's the exact path that caused the data loss.
