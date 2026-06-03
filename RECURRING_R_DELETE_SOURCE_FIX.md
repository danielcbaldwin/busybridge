# Design Doc — Proper Fix for the `_R` `delete_source` Data-Loss Bug

**Status (2026-06-03):** Data loss is **CONTAINED** by commits `1e41a1c` (disarm
`source_delete_pending` at both cancellation sites) and `3b1c638` (client-before-main
churn-breaker that re-asserts live occurrences as drift). The system is **safe to run now** —
destructive `OP_DELETE_SOURCE` is unreachable (zero arming sites; verified by grep). This
document specifies the remaining **proper** fix in two layers (Layer 1 = distinguish a genuine
user cancel from an `_R` artifact; Layer 2 = correct `_R` occurrence ownership) so safe
propagation can be re-enabled and the three `@xfail` tests flipped back. **Do not undo the
containment — the proper fix builds on it.**

> This doc was produced by reading the live `v2` working tree (every `file:line` checked) and
> then adversarially reviewed. The review found load-bearing issues in the first draft; its
> required corrections are folded into §4–§7 inline (marked **⚠ REVIEW**) and reproduced in full
> in the Appendix. **Read the ⚠ REVIEW callouts before implementing — two of them are
> data-loss-risk corrections.**

---

## 1. Symptom & impact

BusyBridge destructively deleted **~166 distinct real recurring occurrences** (≈2,660 delete
ops, HTTP 200 = actually deleted) on the user's MLCommons client calendar (`ag@mlcommons.org`),
across 26 series (e.g. "MLC AIRR Staff Meeting", "MLC Datasets WG"). User-visible failure mode:
the user accepts/keeps an occurrence on MLCommons → it mirrors to the main calendar correctly
(full copy, backlink, lock icon) → BusyBridge then **deletes the REAL occurrence on MLCommons**,
leaving only a busy block. One instance ledger row's `version` reached **344** from the
resulting cancel/revive oscillation (a permanent churn signature). Almost all are meetings the
user *attends* (organizer copies intact → recoverable); one the user *organizes*
("Psychosocial Sync").

**Recovery:** out of scope for this doc; tracked separately. Deleted-occurrence list at
`data/mlcommons_deleted_occurrences_20260603.json`. Containment prevents *recurrence of the
deletion*; this doc prevents *generation* of the spurious cancellation and re-enables safe
propagation only for genuine user actions.

---

## 2. Root cause — exact mechanism (verified against code)

The chain has an upstream **cause** (the `_R` split changes what the main mirror expands) and a
distinct **destructive effect** (BB deletes the source occurrence). The two are bridged by BB
re-reading **its own write** as if it were a user action.

### 2.1 WHO creates the cancelled exception on main: **BusyBridge itself**

The single most important correction to the original mechanism: the cancelled instance-exception
on the managed **main** copy is created by **BusyBridge's OWN `events.delete`**, not by Google
independently expanding `_R`.

1. An instance projection's `desired_state` goes `ABSENT` on `target_kind='main'`. `_decide`
   returns `OP_DELETE` for an instance projection (`parent_canonical_uid` set) that has a derived
   `google_event_id`, even when `current` is absent — `app/ledger/diff.py:410-420`.
2. The outbox issues an **unconditional** delete against the derived instance id —
   `app/ledger/outbox.py:626-627` (`_do_delete`). On a recurring series this **materialises a
   `status=cancelled` instance-exception** on the managed main copy, with `recurringEventId` =
   the bb-managed parent id.
3. Next pass, **main ingest re-reads that cancelled exception** and (pre-fix) treated it as a
   user cancellation → armed `source_delete_pending`.
4. `_decide`'s origin-writeback branch fired the destructive op — `app/ledger/diff.py:377-382`
   (gated by `_is_origin_writeback(proj)` at `:370` — see ⚠ REVIEW #3).
5. `_do_delete_source` deleted the **real source occurrence** by the ledger row's
   `source_event_id` — `app/ledger/outbox.py:669-671`.

**⚠ REVIEW #1 — entry-point is `_ingest_managed_recurring_instance`, NOT `proj_match`.** The
`proj_match` cancelled branch (`main.py:220-255`) fires only when a projection row's
`google_event_id` literally equals the cancelled instance id. A `_R` cancelled *instance
exception* has a derived id `<bbparent>_<stamp>` that is generally **not** stored as a projection
`google_event_id`, so control reaches `_ingest_managed_recurring_instance` via the
`recurringEventId`-is-managed check at `main.py:282-299`. **That is the live artifact path.** The
two branches have *different* churn-breaker predicates (`proj_match` checks the matched ledger
row's `status` at `:245`; `_ingest_managed_recurring_instance` checks a separately-computed
`inst_canonical` row at `:519-528`) — they query different rows and can disagree. Before Layer 1
trusts "Condition A," unify these two predicates into one helper.

**⚠ REVIEW #2 (LINCHPIN — prove first, O-0) — confirm BB's own exception is actually
re-ingested.** Ingest has a self-write guard: `is_our_write = is_managed_google_event_id(
event_id)` (`main.py:209`) returns early at `:219`. `is_managed_google_event_id` matches exactly
13 base32hex chars after `bb` (`identity.py:230-239`); an instance id has a `_<stamp>` suffix, so
it likely does **not** match — which is *why* control reaches `:282`. **The entire bug and fix
assume the cancelled `_R` instance exception (a) is delivered by incremental sync, (b) is NOT
caught by `is_our_write`/`proj_match`, and therefore (c) reaches `:282`. Prove this with a test
before instrumenting anything** — if `is_our_write` *did* match, the exception would be skipped
and there would be no loop.

### 2.2 Why `desired_state` went `ABSENT` for a LIVE occurrence

`_compute_desired_projections` returns `{main: ABSENT, ...}` when the instance's
`status == 'cancelled'`, `user_intentionally_deleted`, `parent_inactive`, or
`no_live_occurrences` — `app/ledger/planner.py:337-344`. The `_R` split is the upstream cause:
Google truncates the base series with an `UNTIL` and adds a `<base>_R<ts>` segment for
post-boundary dates (additive; modelled in the fake at
`tests/fakes/google_calendar.py` `reschedule_series_this_and_following`). If the instance row is
parented to a series whose truncated/changed expansion no longer covers the date,
`_parent_is_inactive` (`planner.py:137-154`) or `_recurring_parent_has_no_live_occurrences`
(`planner.py:180-237`) can flip the projection absent for an occurrence still **live on the
source**.

**⚠ REVIEW #6 — `diff.py:83-95` narrows the trigger.** When the *parent* projection is
desired-absent, the per-instance `OP_DELETE` is **suppressed** (`_snap_applied` + `continue`),
specifically to avoid self-cancelling a one-occurrence recurring copy. So the destructive
instance `OP_DELETE` can only fire when the **parent is still desired-PRESENT but the instance
alone goes ABSENT**. That **rules out a pure `parent_inactive` story**; the live trigger is most
likely `_recurring_parent_has_no_live_occurrences` or an instance-row `status='cancelled'` flip
while the parent stays present. (Open question **O-2**.)

### 2.3 The cancel/revive loop (version 344)

Two opposing forces oscillate:
- **Cancel side:** BB's own `OP_DELETE` materialises the cancelled main exception; main ingest
  cancels the source instance row (pre-fix: re-armed `source_delete_pending`).
- **Revive side:** client ingest re-reads the LIVE occurrence and resurrects the row —
  `app/ledger/ingest/client.py:838-839` (`resurrecting = existing["status"] == "cancelled"`);
  AND the diff sends `status=confirmed` on the instance `UPDATE`, un-cancelling the main
  exception — `app/ledger/diff.py:173-178`.

Net: perpetual cancel(by BB delete)/revive(by client re-ingest + `status=confirmed`) → `version`
climbs without bound.

### 2.4 Base-vs-segment id-ownership mismatch (Layer-2 root cause) — **main-side only**

- **Client ingest is self-consistent.** A recurring instance is parented by
  `event["recurringEventId"]` (whichever series Google says owns it — base OR segment) and its
  `source_event_id` defaults to the instance's own `event["id"]` — `client.py:353-356`,
  `:567-568`. Both the canonical parentage and the stored source id reference the **same** owning
  series. **No base-vs-segment split is introduced here.**
- **Main ingest is the bug.** `_ingest_managed_recurring_instance` derives `source_event_id` from
  the **SOURCE SERIES MASTER** row's `source_event_id` (the BASE master id captured at first
  client ingest), unconditionally — `app/ledger/ingest/main.py:490-496`.
  `derive_instance_google_event_id` blindly concatenates `<parent>_<stamp>` with no check that
  the parent's RRULE covers the date — `identity.py:188-227` (`:206`). For a **post-boundary**
  date the base series no longer covers, this addresses `<base>_<stamp>` on an `UNTIL`-truncated
  series → resolves to a 404 / cancelled tombstone, **not** the live occurrence (which now lives
  under `<base>_R<ts>`).

**⚠ REVIEW #9 — fixing parentage is necessary but NOT sufficient.** Target-side (mirror) ids
derive from the parent projection's `google_event_id` keyed by `parent_canonical_uid`
(`diff.py:75-124`), so correcting `parent_canonical_uid` fixes mirror ids "by construction." But
the **source-side** delete/patch target comes from the stored `source_event_id` derived at
`main.py:490-496`. Layer 2 requires **BOTH** (a) correct parent selection for target ids **AND**
(b) replacing the base-derived `source_event_id` with a segment-derived one. Neither alone is
sufficient.

---

## 3. What's already landed (the containment) — and its limits

**Commit `1e41a1c` — disarm the destructive path (permanent safety floor).** Both cancellation
sites stopped arming `source_delete_pending` (`main.py` `_mark_managed_instance_cancelled` and
the `_ingest_managed_recurring_instance` cancelled branch). **Verified: zero code paths set
`source_delete_pending = 1`** (grep); the sole gate `bool(proj["source_delete_pending"])`
(`diff.py:380`) can never be true → `OP_DELETE_SOURCE` is **unreachable**. `_do_delete_source`
(`outbox.py:636-683`) remains as dormant code that only reads/clears the flag.

**Commit `3b1c638` — churn-breaker (client-before-main).** The reconciler ingests clients (and
personal/webcal) **before** main each pass — `reconciler.py:213-284`. So at main-ingest time an
**active** source instance row means the occurrence is **LIVE**. At both cancellation sites, when
the source is live, the cancelled main exception is treated as **drift** and re-asserted via
`_mark_main_drift_reverted` (bumps `version` only) instead of cancelling — `main.py:244-249`
(`proj_match`), `:519-535` (`_ingest_managed_recurring_instance`).

**What the containment fixes:** the destructive source delete can never fire; the cancel/revive
oscillation is broken; convergence is one-time, not churning. Verified live: 0 `delete_source`,
looping rows' versions frozen, post-unpause writes one-time (distinct projections).

**What it does NOT fix:**
1. **It is a `delete_source` blanket, not a discriminator.** Genuine single-occurrence user
   cancellations made on **main** (the intended Option-A feature) are *also* suppressed — BB only
   un-mirrors, never deletes the source. The feature is off for everyone.
2. **The churn-breaker depends on client-ingest timing.** "Is the source row active?" is correct
   only because client ingest just ran; a transient client-ingest gap could misclassify. It is a
   heuristic floor, not positive proof of user intent.
3. **Layer-2 root cause is untouched.** BB still *generates* the spurious cancelled exception
   (the `OP_DELETE` on a desired=ABSENT instance), and the main-side `source_event_id` is still
   base-derived (latent wrong-id for any future re-enable).
4. **Three tests are `@xfail`** (`_CONSERVATIVE_FIX`, strict=False) in
   `tests/test_main_managed_instance_cancellation.py` — they assert the OLD destructive Option-A
   behavior and must flip back to passing when propagation is safely re-enabled:
   `test_main_side_instance_cancel_deletes_source_and_peer`,
   `test_main_side_instance_cancel_converges_without_redelete`,
   `test_move_then_cancel_one_instance_deletes_source_occurrence`.

---

## 4. The proper fix

Layer 1 makes destructive propagation **safe** (positive proof of user intent); Layer 2 removes
the **root cause** so the artifact is never generated and the source-side id is correct.

### 4.1 Layer 1 — distinguish genuine user cancel from `_R` artifact

**Principle:** gate `OP_DELETE_SOURCE` on a **positive "this was a genuine USER cancellation"**
signal — never on the *absence* of evidence. AND-gate of independent conditions; any failing →
treat as drift/no-op (re-assert the mirror).

- **Condition A (safety floor — keep the churn-breaker):** the source instance row is **not**
  active at main-ingest time. Implemented at `main.py:244-249` and `:519-535`.
  **⚠ REVIEW #1:** unify the two predicates (they query different rows) into one helper first.
- **Condition B (new — provenance of the exception):** the cancelled exception was **NOT authored
  by BusyBridge's own recent `OP_DELETE`.** BB knows every instance id it deletes (`diff.py:415-420`
  → `outbox._do_delete`, `:626-627`). Record them and recognize the echo.

**⚠ REVIEW #5 (DATA-LOSS RISK — durable marker is MANDATORY, not optional).** A recency-window
`bb_self_deletes` table is dropped on a sync-token reset / DB restore / window expiry. After that,
BB re-reads a cancelled exception it created earlier, finds no provenance record, Condition A may
transiently also pass (client-ingest gap) → classifies BB's own artifact as a user cancel →
**deletes the real source occurrence = the original bug, through the new code.** Therefore:
**the durable self-identifying marker is REQUIRED to re-enable destructive propagation.** When BB
writes/owns a managed instance, stamp `extendedProperties.private.bb_proj_id` (or similar) so a
tombstone BB created is self-identifying across resets. The recency-window table must **never** be
the sole provenance signal that gates a destructive delete. (Open question **O-1**: confirm a
user-deleted occurrence of a managed copy preserves our `extendedProperties` on its tombstone.)

**⚠ REVIEW #4 (false-SUPPRESS hole).** BB constantly re-touches these ids via the `status=confirmed`
revive (`diff.py:173-178`). A provenance check keyed on "BB touched this id recently" would
suppress a *genuine* user cancel of an occurrence BB just confirmed. Condition B must key on a BB
**delete** (not any write), consumed **exactly once**, AND require an **etag/sequence change**
between BB's write and the observed cancellation. Also: read provenance **before** the revive
UPDATE is enqueued in the same pass (the revive un-cancels the very exception you're reading).

**⚠ REVIEW #3 (which projection carries the flag).** `OP_DELETE_SOURCE` is reached only inside
`if _is_origin_writeback(proj):` (`diff.py:370`); origin-writeback projections keep
`google_event_id` NULL (`diff.py:73-75`). The re-arm must set `source_delete_pending` on the row
whose projection `_is_origin_writeback` returns true — distinct from the *main mirror* instance
projection that received the `OP_DELETE`. Map this explicitly before re-arming, or the re-arm
no-ops (false suppress) or arms the wrong projection.

**Re-arm site:** with all conditions satisfied (A active-source floor + B not-BB-authored with
etag change + durable marker absent + Layer-2 "segment owns the date"), arm `source_delete_pending`
in `_mark_managed_instance_cancelled` — reverting the `1e41a1c` removal **only** behind the
AND-gate. The destructive op then flows through the existing, unchanged
`diff.py:377-382` → `outbox._do_delete_source`.

### 4.2 Layer 2 — correct `_R` occurrence ownership + id derivation

**Goal:** a LIVE occurrence must never produce a desired=ABSENT main instance projection, and the
source-side delete/patch id must address the occurrence on the segment that actually owns it.

Define a helper resolving ownership from the **live owning series for the occurrence's date**, not
a base id frozen at ingest:

```
owning_source_series(user_id, original_start) ->
    the source ledger master (is_recurring=1, parent_canonical_uid IS NULL) whose expanded
    RRULE (recurrence_rule_json + start_at, honoring UNTIL) contains original_start
```

After a this-and-following split there are multiple masters: the base (`UNTIL`-truncated) plus
each `<base>_R<ts>` segment (each ingested as its own recurring master because the segment carries
`recurringEventId=None` — `client.py:371-388`). Pick the one whose expansion covers the date.

**Concrete changes:**

1. **Client path — keep as-is; add a regression assertion** that the stored `source_event_id`'s
   parent component equals the delivered `recurringEventId` (extend
   `test_moved_instance_survives_this_and_following`).
2. **Main path — the fix.** In `_ingest_managed_recurring_instance` (`main.py:490-496`), **stop
   using `parent["source_event_id"]` (base master) unconditionally.** Resolve
   `owning_source_series(user_id, original_start)`; choose **that segment** as the parent and derive
   `source_event_id = derive_instance_google_event_id(owning_segment.source_event_id, original_start,
   is_all_day)`. If no segment's RRULE contains `original_start`, store **no** source id (a main-side
   cancel for a date no source segment owns is provably not a real source occurrence — also
   Layer-1-relevant).
   **⚠ REVIEW #8 (no re-key).** §6 forbids re-keying (it caused the MLC 404 residual). Layer 2 must
   correct parent selection **at instance-row creation only** — never `UPDATE parent_canonical_uid`
   in place on an existing row. The migration (step 5 / O-3) must **re-create** rows under the
   correct parent, not mutate them.
   **⚠ REVIEW #7 (moved pre-boundary occurrence).** `owning_source_series` must match on
   `original_start` (not the moved start), handle `UNTIL` inclusivity explicitly, and **resolve a
   pre-boundary moved occurrence to the BASE master, not the `_R` segment** — otherwise it
   reintroduces the exact 404-forever corruption `test_moved_instance_survives_this_and_following`
   guards. Add an assertion that the moved occurrence's resolved owning series is the BASE master.
3. **Stop generating the spurious main `OP_DELETE`.** With the instance correctly parented to its
   owning (live) segment, `_parent_is_inactive`/`_recurring_parent_has_no_live_occurrences` reason
   about the right series, the projection stays desired=PRESENT, and the `diff.py:410-420`
   `OP_DELETE` is never emitted for a live occurrence — closing the loop at its source.
4. **Belt-and-suspenders gate on re-armed propagation:** before `OP_DELETE_SOURCE` fires,
   additionally require that `source_event_id` was derived from a segment whose **live RRULE
   contains the date** — so a stale base-derived id can never be the delete target.
5. **Migration (O-3):** existing instance rows parented to a base master for post-boundary dates
   have ids that 404. A one-time re-create pass (NOT in-place re-key) may be needed; confirm scope
   against the live DB.

---

## 5. Invariants to preserve (must not regress)

- **`OP_DELETE_SOURCE` stays gated on a positive user-intent signal.** Never re-arm purely on "no
  live source occurrence."
- **Personal calendars are strictly read-only** (`outbox.py:659-668`, `main.py:227-233`; commit
  `6bd7929`).
- **`_R` split stays additive — no re-keying** (`client.py:371-388`);
  `test_moved_instance_survives_this_and_following` must keep passing.
- **The two conservative tests must keep passing:**
  `test_main_side_cancel_does_not_destructively_delete_source`,
  `test_source_side_instance_cancel_does_not_arm_destructive_delete`.
- **`status=confirmed` revive on instance UPDATE stays** (`diff.py:173-178`).
- **Client-before-main ordering stays** (`reconciler.py:213-284`).
- **No write-loop regression** (commits `4a61244`, `85bb9bd`): `ledger_version` must not enter
  payload hashes; title-prefix render must not re-trigger writes.
- **Parent series is never deleted** (Option-A spec): a derivation bug must never target the series
  master id.

---

## 6. Reproduction & test plan

Write the **failing repro first**, in two tiers. Both must assert **source-calendar (`client_a`)
survival** — the assertion the moved-instance test omits (it only asserts `client_b` peer
survival).

**Tier 1 — no fake change; reproduces the in-recurrence cancel/revive churn + would-be destructive
delete.** New `tests/test_R_artifact_destructive_delete_repro.py`:
1. Build a recurring client series; reconcile to quiescent so main holds a bb-id managed copy.
2. Make one occurrence a live MODIFIED instance on `client_a`; reconcile so it mirrors to
   `client_b`.
3. Drive that instance's main projection to desired=ABSENT **the production way** (per ⚠ REVIEW #6,
   most likely `_recurring_parent_has_no_live_occurrences` or an instance `status='cancelled'`
   flip while the parent stays PRESENT — not `parent_inactive`).
4. Re-run client ingest (re-creates the active row — occurrence still live), then main ingest.
5. **Assert (must hold under containment; regression-locks version-344):** across N quiescent
   passes the instance `version` stays frozen, `source_delete_pending` is never armed, and the
   real `client_a` occurrence remains present (snapshot source event ids / add a call-recording
   shim on the fake `delete_event`).

**Tier 2 — small fake enhancement; reproduces the genuine out-of-boundary `_R` artifact
end-to-end.** Drive `reschedule_recurring_this_and_following` with a moved occurrence; assert no
destructive source delete. **Fake enhancement:** when a delete/override targets an out-of-recurrence
date but the id parses as an existing recurring-parent + stamp, create a `status=cancelled`
override (matching real Google keeping the exception after `UNTIL`-truncation) rather than 404.
Keep `update_event` confirmed-revive gated on in-recurrence. (Open question **O-4**: confirm real
Google keeps the exception addressable after truncation — the HTTP 200 on the real delete implies
yes.)

**Genuine-user-cancel tests (the 3 `@xfail`)** stay asserting destructive Option-A behavior; after
Layer 1 + Layer 2 land they flip back to **passing** for the genuine main-side user-cancel case.
Remove `@_CONSERVATIVE_FIX` only after both layers are verified. Tier-1/Tier-2 repros **stay** (they
assert NO destructive delete for the ARTIFACT case) — the suite then distinguishes the two cases.

**⚠ REVIEW #10 / #11 — add two missing tests:**
- **Parent-series survival:** after re-enabling, the source series master still expands all
  non-cancelled occurrences (not just that the one survived).
- **Genuine-cancel-after-BB-write:** BB writes/revives the mirror, *then* the user cancels that
  occurrence on main → the destructive delete *does* fire exactly once (catches the
  recency-window false-suppress ambiguity, ⚠ REVIEW #4).

**Soak/chaos:** run the harness with `FailureInjector` client-ingest gaps (stress Condition A
timing, O-5) + an `_R`-split scenario; assert 0 `delete_source` for artifacts, and that the genuine
path deletes exactly one source occurrence and converges with frozen version. Baseline before
changes: **4 passed, 3 xfailed**.

---

## 7. Implementation checklist (ordered)

1. **O-0 (linchpin):** prove by test that the cancelled `_R` instance exception is re-ingested and
   reaches `main.py:282` (not caught by `is_our_write`/`proj_match`). If not, the whole model is
   wrong — stop and re-diagnose.
2. **O-2:** pin which suppressor flips the MLC instance projection to ABSENT (reconcile with
   `diff.py:83-95`).
3. **O-3:** inspect the live DB (`/data/calendar-sync.db`) `version=344` row(s) (`canonical_uid`,
   `source_event_id`, `parent_canonical_uid`) — legacy artifact vs current-path; migration scope.
4. Write **Tier-1 repro**; confirm it passes under containment (locks version-344).
5. **Layer 2a:** `owning_source_series` helper (expand masters, honor `UNTIL`/segment boundaries;
   match on `original_start`; pre-boundary moved → BASE).
6. **Layer 2b:** fix `_ingest_managed_recurring_instance` (`main.py:490-496`) — owning-segment
   parent + segment-derived `source_event_id`; no source id if no segment owns the date.
   **Create-time only — no in-place re-key.**
7. Verify the spurious main `OP_DELETE` is gone for live occurrences.
8. **Layer 1:** unify Condition A predicates; add **durable** `bb_proj_id` marker (mandatory) +
   `bb_self_deletes` (recency, secondary); read provenance before the revive enqueue.
9. **Layer 1:** re-arm `source_delete_pending` in `_mark_managed_instance_cancelled` behind the full
   AND-gate, on the `_is_origin_writeback` projection.
10. Enhance the fake (Tier-2) + its unit test; write Tier-2 repro.
11. Add the parent-series-survival and genuine-cancel-after-BB-write tests; remove the 3
    `@_CONSERVATIVE_FIX` xfails; confirm they pass.
12. Full suite + soak/chaos with client-ingest-gap injection; confirm §5 invariants + write-loop
    floor.

---

## 8. Risks & open questions

- **O-0 (linchpin):** is the cancelled `_R` instance exception actually re-ingested and uncaught by
  `is_our_write`/`proj_match`? Prove before building anything.
- **O-1 (Layer-1 marker):** does a user-deleted occurrence of a managed main copy preserve our
  inherited `extendedProperties.bb_proj_id` on its tombstone? Required for the mandatory durable
  marker.
- **O-2 (trigger):** which of `no_live_occurrences` / instance `status='cancelled'` flip drives the
  ABSENT projection (NOT `parent_inactive`, per `diff.py:83-95`).
- **O-3 (migration):** are corrupt rows legacy re-key artifacts (won't regenerate) or current-path
  products needing a re-create migration? Inspect the live DB.
- **O-4 (fake fidelity):** does real Google keep a cancelled exception addressable after
  `UNTIL`-truncation? HTTP 200 on the real delete implies yes.
- **O-5 (timing):** Condition A depends on client ingest running first; a gap could misclassify —
  which is why Layer 1 must AND it with positive provenance (Condition B + durable marker).
- **Product decision:** is destructive Option-A propagation still wanted at all, or is the permanent
  floor (only un-mirror, never delete the source) acceptable? If the latter, Layer 1 reduces to
  "keep the floor; never re-arm," but **Layer 2 is still required** to stop generating the artifact
  and to fix wrong-id derivation for any future writeback/patch.

---

## Appendix — full adversarial review

The draft of §2–§7 was adversarially reviewed against the live tree; the review's verbatim
findings (the source of the ⚠ REVIEW callouts above) are preserved here for traceability.

**Verdict:** containment claims accurate and the doc is well-grounded; but it had one load-bearing
factual error (entry-point conflation, #1), one unproven linchpin assumption (#2 / O-0), Layer-1
false-propagate/false-suppress holes that are themselves data-loss risks (#4, #5), and a Layer-2
id-ownership rule that is directionally right but can break the moved-instance test if
underspecified (#7, #8).

1. Entry-point conflation — `_ingest_managed_recurring_instance` (`:282-299`) is the live artifact
   path, not `proj_match`; their churn-breaker predicates differ and must be unified.
2. Unverified linchpin — prove the cancelled exception is re-ingested and not caught by
   `is_our_write`/`proj_match` (O-0).
3. `OP_DELETE_SOURCE` is inside `_is_origin_writeback(proj)` (`diff.py:370`); origin-writeback
   projections keep `google_event_id` NULL — map which projection carries the flag before re-arming.
4. Layer-1 false-suppress: a genuine cancel of an occurrence BB just wrote/revived; key Condition B
   on a delete consumed once + an etag change; read provenance before the revive enqueue.
5. Layer-1 false-propagate = data loss after sync-token reset; the durable marker must be MANDATORY,
   not optional; the recency table must never solely gate a destructive delete.
6. §2.2 omits `diff.py:83-95` (parent-desired-absent suppresses per-instance delete) — narrows the
   trigger away from `parent_inactive`.
7. Layer-2 `owning_source_series` must match on `original_start`, handle `UNTIL` explicitly, and
   resolve a pre-boundary moved occurrence to the BASE master — else 404 corruption returns.
8. Layer-2 step 2 must select the correct parent at *creation* only; never `UPDATE
   parent_canonical_uid` in place (that is the banned re-key); migration re-creates rows.
9. "Fix parentage and ids are correct by construction" fixes TARGET ids only; the SOURCE id at
   `main.py:490-496` must also change — both are required.
10. Add a parent-series-survival assertion (the destructive op uses a derived id; a bug could target
    the master).
11. Add the genuine-cancel-after-BB-write regression (catches the recency-window ambiguity).
12. Reconcile the "~166" vs "~170" figure (distinct occurrences vs ops).

Solid as-is: §3 (containment) and §5 (invariants) are accurate and complete; §2.4's
main-side-only localization is the best-verified part; the §7 checklist ordering (repro-first,
Layer 2 before re-arming Layer 1, un-xfail last) is correct.
