# Long-Term Design Brief: Correct "Change All Events From Here Forward" (_R Split) Mirroring for Recurring Events

> **Status:** design brief for a future deep session. Grounded against the `v2` tree (HEAD `511479b`) on 2026-06-04. All `file:line` references re-verified this session unless explicitly flagged "OPEN."
> **Scope:** forward steady-state mirror correctness for the `_R` "this and following" split. Historical recovery of the ~166 lost MLCommons occurrences is a **separate deliverable** and is out of scope here. Destructive main→source occurrence-delete propagation is **deprioritized** (footnote only).

---

## 1. Problem statement

"Change all events from here forward" (Google's **"this and following"**) is one of the most common edits a user makes to a recurring meeting. On a **source (client) calendar**, this edit causes Google to perform an **additive split**: it truncates the base series RRULE with an `UNTIL` just before the boundary, creates a brand-new independent series `<base>_R<YYYYMMDDTHHMMSSZ>` (with `recurringEventId=None`) covering post-boundary dates, and cancels any modified-instance overrides on the base that fell on/after the boundary (verified: `tests/fakes/google_calendar.py:1058-1161`; truncation at `:1093`, override cancel at `:1105-1121`, new segment with `recurring_event_id=None` at `:1152`). The user's mental model is "one series, changed from date X forward"; Google's storage is "two (or more) coexisting series over disjoint date ranges."

**Correct** means BusyBridge reproduces the **user's** single-continuous-series view over Google's split storage: every live source occurrence — pre- and post-boundary — has exactly one main full copy and one peer busy block at its **current** time/details/Meet link, with **no** stale copy at the old time, **no** duplicate/ghost copies, **no** 404-forever derived ids, and convergence to quiescence with **zero churn and zero data loss**. Today the system only *contains* the data-loss/churn (it re-asserts instead of cancelling); it does **not** yet produce a correct post-boundary mirror — the post-boundary occurrences are derived against the truncated base, which no longer covers them.

---

## 2. Target behavior / acceptance criteria

### Target invariants (all cases)
- **I1 — Full coverage.** Every live source occurrence (pre- AND post-boundary) has exactly one main full copy and one peer busy block at the occurrence's *current* time/details.
- **I2 — No stale copy.** No mirror copy survives at the *old* time for any post-boundary occurrence.
- **I3 — No dup/ghost.** Never two main copies or two peer blocks for one occurrence; never a copy for a date no live segment covers.
- **I4 — Correct segment keying.** Each mirror occurrence is keyed (derived-id) under the **segment whose live RRULE actually expands to contain its date**, so `events.update`/`events.delete` on the derived id never 404s. The fake enforces this: `_materialize_instance_override` returns `None` (→404) when `_is_dt_in_recurrence(parent, instance_dt)` is false (`tests/fakes/google_calendar.py:1178-1179`; `_is_dt_in_recurrence` at `:1484-1523`).
- **I5 — Convergence.** Reconcile reaches quiescence with NO per-pass oscillation (the version-344 cancel/revive loop must not exist) and NO data loss (authoritative source occurrences never deleted; mirror copies never destructively lost and always re-creatable).

### Desired end-states by case
- **Case 1 — clean split (time/summary change):** post-boundary main copies render at the NEW time/details keyed under `_R<ts>`; pre-boundary copies UNCHANGED under the truncated base. `source=='client'` projects `{main: PRESENT_FULL, peer_clients: PRESENT_BUSY (unless show_as==free), origin_client: PRESENT_FULL_RSVP_ONLY|ABSENT}` (`planner.py:354-372`). Peer busy block on every non-origin client; origin gets no busy block.
- **Case 2 — series already had MODIFIED instances:** a modified instance stays parented to whichever segment its `recurringEventId` names. Pre-boundary moved occurrences stay on the base and keep their moved time; Google cancels post-boundary base overrides (their dates moved to `_R`); post-boundary occurrences mirror under `_R`. The OLD bug bulk-re-parented ALL modified instances onto the newest segment → `<later_segment>_<earlier_stamp>` → permanent 404 (`test_moved_instance_survives_this_and_following.py` docstring lines 9-17).
- **Case 3 — single occurrence moved across the boundary before the split:** one mirror at the occurrence's CURRENT (moved) time, re-creatable forever. (Move 02-16 → 14:00, split at 03-02; the 02-16 mirror stays at 14:00 and survives a churn-delete + re-create — `test_...py:84-105`.)
- **Case 4 — Meet link present:** post-boundary copies carry the `_R` segment's NEW conferenceData/Meet link; pre-boundary copies retain the OLD link. No cross-segment link bleed (each segment carries independent content: fake builds `_R` from `new_body` at `:1134-1159`; base keeps its own fields, only recurrence/updated/etag change at `:1095-1099`). **OPEN — confirm `payload.py` copies `conferenceData` per-segment (not yet read at field level).**

---

## 3. Verified current behavior + exact break points

**The common case currently WORKS for the recurring *parent* projections.** Client ingest treats `<base>_R<date>` as its OWN recurring master (does NOT re-key the base onto it): `ingest/client.py:371-388` — *"an ADDITIVE new series segment … We deliberately do NOT re-key the base onto it — each segment is ingested as its own recurring series."* Instance routing parents a modified instance to whichever series `recurringEventId` names: `client.py:353-356`. The planner does **not** expand RRULEs into per-occurrence projections — each series row gets ONE main + ONE peer projection carrying the RRULE, and Google expands them (`payload.py` emits `recurrence`). So the truncated base copy and the new segment copy both render as recurring parents, and single-events expansion is correct (pre-boundary old time, post-boundary new time). The grounding test passes (`pytest tests/test_moved_instance_survives_this_and_following.py` → 1 passed).

**THE CORE DEFECT — a single unconditional derivation.** Main ingest derives a managed recurring *instance's* `source_event_id` from the **base series master** unconditionally, with no RRULE-coverage check:

```
app/ledger/ingest/main.py:490-496
    source_event_id: Optional[str] = None
    src_series_id = parent["source_event_id"]
    if parent["source_type"] == "client" and src_series_id:
        original_start, instance_is_all_day = _instance_original_start(event)
        source_event_id = derive_instance_google_event_id(
            src_series_id, original_start, instance_is_all_day,
        )
```

`parent` is resolved (`main.py:463-475`) from the managed parent id via its `main` projection — **always the base master** after an `_R` split, because BB never re-keys it onto the segment. And `derive_instance_google_event_id` blind-concatenates `<parent>_<stamp>` with zero coverage check:

```
app/ledger/identity.py:202-227
    return f"{parent_google_event_id}_{stamp}"            # all-day
    return f"{parent_google_event_id}_{dt.strftime('%Y%m%dT%H%M%SZ')}"  # timed
```

For a post-boundary date the truncated base no longer expands, so the derived id `<base>_<stamp>` addresses a slot the base does not cover → Google delivers a **cancelled** exception (the fake's `_materialize_instance_override` returns `None` → `events.update` 404s forever — `tests/fakes/google_calendar.py:1178-1179`).

**This derivation site is reached only via `_ingest_managed_recurring_instance` (`main.py:434`)** — a main-side occurrence edit/cancel or the `_R` cancel-artifact BB's own delete materialises. It is **NOT** on the client-split ingest path (which routes through `client.py:399-419` and never calls it).

**The current churn-breaker masks but cannot converge.** A re-ingested cancelled `_R`-artifact exception lands in the proj_match-cancelled branch and re-asserts a drift-revert (`main.py:498-535`), re-creating a copy parented to a series that does not cover the date — so the copy is at the stale time and the underlying update keeps 404-ing. The comment admits it *"looped forever (version 344 on one row) and … destructively deleted ~170 real source occurrences"* (`main.py:498-508`). It stops data loss; it does not fix the mirror.

---

## 4. Resolved facts (the load-bearing unknowns)

### O-0 — Entry point (RESOLVED): the proj_match cancelled branch, NOT `_ingest_managed_recurring_instance`

For a re-ingested cancelled exception that **BB itself wrote** (a derived `<bb-parent>_<stamp>` id), the live path is the **proj_match cancelled branch (`main.py:220-255`)**, not the managed-instance handler at `:282`. Chain:
1. The diff pre-sets the derived instance id onto the instance projection's `google_event_id` (`diff.py:111-124`).
2. The outbox delete path (`_do_delete → _record_absent`) sets `current_state='absent'` but **does NOT null `google_event_id`** (`outbox.py:854-877`); only the `_do_update` 404 path nulls it (`outbox.py:574-583`).
3. So on the next ingest the proj_match SELECT — keyed solely on `google_event_id` (+`user_id`) at `main.py:215` — **matches**, and `main.py:219 (if proj_match is not None or is_our_write:)` short-circuits before the `recurringEventId` fallthrough at `:282`.

`_ingest_managed_recurring_instance (main.py:282)` is reachable only for a `recurringEventId`-bearing event that does NOT match any projection AND whose parent IS managed — i.e. a **native** main edit/cancel on an occurrence we never individually wrote. **Both branches are live**, selected by whether a modified-instance projection already stores the derived id.

> **CRITICAL DISCREPANCY (carry into the fix):** the two churn-breaker predicates inspect **different rows**. The proj_match guard tests `matched["status"]` — the status of whatever ledger row owns that `google_event_id` (`main.py:244-249`), which for a sticky cancelled instance-child row is `'cancelled'`, so the guard does **not** re-assert and proceeds to `_mark_managed_instance_cancelled`. The managed-instance handler re-derives `inst_canonical` and tests *that* occurrence's row for `status='active'` (`main.py:519-528`). They are **not equivalent** — the proj_match branch can mis-classify a live post-boundary occurrence as a genuine removal. The fix should unify these into one helper.

### O-2 — The ABSENT trigger (RESOLVED): row-level `status=='cancelled'` on the sticky instance child

The four suppressor flags in `_compute_desired_projections` are computed **once per ledger row** and applied to ALL roles (`planner.py:61-71`, `:337-344`), so a single row can never yield `main=ABSENT` while `peer=PRESENT`. The `diff.py:83-95` special case is about **two different ledger rows** on the same target (a child INSTANCE projection vs its PARENT series projection) — it suppresses the per-instance `OP_DELETE` **only when BOTH parent AND instance are desired-ABSENT**. The flag that flips a post-boundary instance row's main projection to ABSENT while its parent stays PRESENT is:

```
planner.py:339   or ledger["status"] == "cancelled"
```

set on the sticky cancelled instance child row. Its parent (base series master) stays desired=PRESENT (nothing cancelled the master). This is exactly the case `diff.py:83-87` does NOT suppress (it requires `parent_proj.desired_state == ABSENT AND proj.desired_state == ABSENT`), so the per-instance `OP_DELETE` fires against the derived id — which post-boundary the base no longer covers → 404/cancelled tombstone. `parent_inactive` (`planner.py:137-154`) and `no_live_occurrences` (`planner.py:180-259`) are NOT the trigger: they make the parent absent too, falling inside the both-absent suppression.

### O-3 — Live-DB artifact scope (RESOLVED, read-only, NOTHING MUTATED)

Live ledger `/data/calendar-sync.db`, 3,578 `ledger_events`:
- **351 `_R` segment masters** (`is_recurring=1`, `parent_canonical_uid IS NULL`, `source_event_id` ends `_R<ts>`): 222 client / 72 personal / 57 main_native; 277 active / 74 cancelled. **165** have NO surviving base master (base fully replaced by a single `_R` segment).
- **The wrong-id pathology is NOT persisted in source rows.** Of 43 base-parented instances under a split base, **all 43 are pre-boundary; 0 post-boundary**. Client ingest correctly parents post-boundary instances to the `_R` segment. **The defect is a runtime main-side derivation, not a stored re-parenting backlog.**
- **Churn:** 128 rows at `version>=100` (max 2179); only **44** are `_R`-artifact rows. Clear `_R` families: personal `2ekma264…` (~36 rows at v198), client `8mdo…_R` (v341/344), two `_R` segment-instances `35926/35927` at v702. The very-highest versions (2179/1824/911/908) and a webcal/tripit family (1088) are **non-`_R`** — a separate churn story.
- **Migration / orphan-guard scope:** the orphan-guard would ABORT a naive delete-based migration for **94 `_R` segment-master events** (244 present projections, 84 main) plus **87 instances-under-segments** (187 present projections). But the ~3,300 instance **tombstones** are safe to delete: of 2,051 dangling-parent instances, 2,047 are `status=cancelled` with **0** `current_state='present'` projections. Notably **780** dangling instances point at an `_R` segment master that does not exist as a ledger row; 777 are cancelled tombstones, only **3 active** (ids `35587, 36271, 39108`) — and those carry `main proj current_state='unknown', google_event_id=NULL`, so no live Google copy is at risk.

---

## 5. The long-term design

### 5.1 `owning_source_series(user_id, original_start, candidate_masters)` — route each occurrence to the covering segment

Introduce a resolver that, given an occurrence's **original_start** (the un-modified slot), returns the source master whose **live (post-`UNTIL`) RRULE actually expands to contain that date** — the base before the boundary, the `_R<ts>` segment after it.

- **Match predicate:** expand each candidate master's live RRULE via `rrulestr` (UNTIL handled natively by dateutil, so a truncated base correctly EXCLUDES post-boundary dates) and match the occurrence's `original_start` by the **same occurrence key** used elsewhere (`_occurrence_key_from_datetime`, `planner.py:306-311`), honoring all-day vs timed stamp form. Match on `original_start` (the UN-modified slot, `_instance_original_start` prefers `originalStartTime` — `client.py:500-518`), **NOT** the moved instant — this is why a PRE-boundary moved occurrence (time shifted) correctly resolves to the BASE: ownership is decided on the original slot, exactly as the fake decides override-cancellation (`tests/fakes/google_calendar.py:1111-1113`).
- **Candidate set:** all `source_type IN ('client')` recurring masters for the user sharing the same base id (base = strip trailing `_R<ts>`), i.e. base + every `<base>_R<ts>` segment. The source-side ledger already holds each `_R` segment as its own master (`client.py:369-388`), so the candidates are present as rows.
- **Feasibility — machinery already exists and can be lifted into a shared module:** `from dateutil.rrule import rrulestr` (`planner.py:25`); expansion `rrulestr("\n".join(recurrence), dtstart=start, forceset=True)` (`planner.py:213`); `_occurrence_key_from_datetime` (`:306-311`); `_looks_finite_recurrence` (`:273-282`); `_parse_recurrence_lines` (`:262-270`).
- **Tie-break (defensive):** if more than one live segment claims the same `original_start` (should be impossible given non-overlapping `UNTIL` ranges, but a malformed user RRULE could), prefer the segment whose `dtstart` is the **latest start ≤ original_start** (most-specific covering segment).

### 5.2 Create-time main-ingest parenting fix

At `main.py:490-496`, replace "derive from the base master unconditionally" with "derive from the **owning segment**, at CREATE time only":
1. Compute `original_start` (already done at `:493`).
2. `owner = owning_source_series(user_id, original_start, candidate_masters)`.
3. If `owner` is found → derive `source_event_id` from `owner`'s `source_event_id`.
4. If **no live segment covers the date** → store `source_event_id = None` (already the default at `:490`). The origin op then has no target — a deferred no-op, which is correct: **never fall back to the base id when no segment covers** (that fallback is exactly the 404-forever bug). Prefer "unresolved → no-op + retry" over "resolved-to-wrong-segment."

**Do NOT re-key `parent_canonical_uid` in place** — that re-key is the banned move guarded by `test_moved_instance_survives_this_and_following` (`client.py:371-382`: re-keying *"collapsed coexisting segments and bulk-re-parented pre-boundary instances … events.update on the derived instance id 404'd forever"*). The fix changes **which master seeds the derivation**, not the canonical parentage.

**The diff's pre-set must agree.** `diff.py:111-124` independently derives the instance id from `parent_proj["google_event_id"]` (the base master's MAIN bb id). If left as-is, the diff would re-derive off the base on the main target and re-introduce the wrong id. **OPEN/likely:** the owning-segment selection must apply at the diff's pre-set too, so it derives off the **segment's** main projection id. Both sites must agree, since the diff re-derives on the main target independently. Confirm in the deep session whether one shared helper can serve both call sites.

### 5.3 Stopping artifact generation at the root

Once an instance derives from the owning segment, Google returns a **live override** (not a cancelled tombstone), so the planner's `status=='cancelled'` suppressor never fires for a live post-boundary occurrence (O-2 trigger removed at the root), and the proj_match cancelled branch never sees a spurious cancellation to re-assert. `_recurring_parent_has_no_live_occurrences` (`planner.py:180-259`) inspects the segment that actually owns the children, so its accounting becomes correct too. The current churn-breaker drift-revert (`main.py:529-535`) becomes **unnecessary for this case** — but **keep it** as a safety net for genuine ambiguity until the new path is proven; it must remain a no-op in steady state (no regression of I5).

### 5.4 Migration that avoids the orphan-guard trigger

The orphan-guard `trg_ledger_events_block_orphaning_delete` RAISES ABORT on DELETE of any `ledger_event` with a `current_state='present'` projection (`schema.py:233-242`). The migration must be **projection-preserving (UPDATE), never a row re-create/delete**:
1. For each managed recurring instance row whose `source_event_id` points at a base that no longer covers `original_start`, recompute `owning_source_series`.
2. If a segment owns it: the segment's own MAIN projection must exist first; then `UPDATE ledger_events.source_event_id` to the segment-derived id and `UPDATE` the instance's MAIN projection `google_event_id` from `<base>_<stamp>` to `<segment_bb_parent>_<stamp>`.
3. Snap `applied_payload_hash` so the diff re-derives cleanly.
4. Leave present projections present — **never DELETE a row**, so the trigger never fires.

The ~2,047 cancelled dangling tombstones (0 present projections, O-3) are separately safe to prune without tripping the guard, but that prune is **optional cleanup**, not part of forward correctness. Scope the migration to bound risk — consider a one-shot guarded content-audit pass (like the `ical_uid` backfill) limited to affected rows; decide MLCommons-only vs all-users in the deep session.

### 5.5 Defer / retry semantics

Clients ingest **before** main in every reconcile pass (`main.py:510-511`), and client sync is incremental, so the owning `_R` segment may be absent on the first pass. The diff already has a defer primitive — *"Parent hasn't been written yet; defer this instance to the next reconcile pass"* (`diff.py:108-110`, continue with no op). `owning_source_series` returns `None` when no live segment covers the date → main-ingest stores `source_event_id=None` → no origin target, no 404 → the instance reconverges on a later pass once the segment is ingested. This is the explicit "unresolved → no-op + retry" contract.

### 5.6 Interactions with shipped work (do not undo)
- **Conference-link debounce (`e844296`):** `_resolve_conference` (`client.py:1078-1109`) keys on the stable `conferenceId` on the series-master row and adopts a new room only after **two consecutive ingests** — orthogonal to occurrence ownership, and **improved** by this fix: a correctly-parented post-boundary occurrence inherits the `_R` segment's NEW Meet link (the desired end-state), with no old-base bleed. Keep intact.
- **delete_source containment:** `source_delete_pending` is never armed — grep shows only the schema declaration, diff reads, and disarm writes (`outbox.py:664,680 SET source_delete_pending = 0`); **zero** `SET … = 1` sites. `OP_DELETE_SOURCE` (`diff.py:382`) stays unreachable. The fix changes only `source_event_id` derivation and projection ids; it must **never** arm the destructive flag.

---

## 6. Invariants that must not regress
- **No re-key.** Never re-parent `parent_canonical_uid` onto a later segment; segments coexist, each its own recurring master (`client.py:371-388`). Guarded by `test_moved_instance_survives_this_and_following`.
- **Additive `_R`.** Base is `UNTIL`-truncated and coexists with each `_R<ts>` segment over disjoint date ranges; a truncated base still expands to (and mirrors) its pre-boundary dates.
- **Moved-instance survival.** A pre-boundary moved occurrence keeps its moved time and stays re-creatable after a churn-delete (the existing test; extend it — §7).
- **No write-loop.** Reconcile reaches quiescence; no per-pass oscillation (no version-344 loop); steady state is a no-op (no churn).
- **Containment intact.** `OP_DELETE_SOURCE` unreachable; never arm `source_delete_pending`; the real source occurrence is never destructively deleted.
- **Conference debounce intact.** Two-consecutive-ingest adoption preserved; post-boundary copies carry the segment's link with no cross-segment bleed.

---

## 7. Failing-repro-first test plan

> Baseline: 732 collected / 727 selected (`pytest.ini:7`). Three `xfail` tests in `test_main_managed_instance_cancellation.py` are the destructive-propagation invariants — **leave them xfail** (out of scope). Three sibling safety tests must keep passing.

**Repro-first principle:** the production loop is NOT reproducible with the fake as shipped — all three core `_R` scenarios already converge correctly in the fake. **First** write tests that *fail* against current `main` by asserting the I2/I3/I4 properties the existing test omits; **then** make the fix turn them green.

### Fake enhancements likely required
- The fake already models the additive split (`reschedule_series_this_and_following`, `:1058-1161`), the boundary `UNTIL` (`:1093`, `:1601`), post-boundary override cancel (`:1105-1121`), and the 404-on-out-of-range-derived-id mechanism (`_materialize_instance_override` → `None` → 404; `:1178-1179`, `_is_dt_in_recurrence` `:1484-1523`). To *reproduce the runtime defect* rather than only the parent path, the test must drive the main-side instance derivation: create a modified main-copy instance (so a projection stores the derived id, forcing the proj_match branch — O-0) AND a post-boundary regular occurrence, then assert the derived ids resolve under the **segment**, not the base.
- **OPEN:** confirm whether the fake needs a helper to assert "the derived id of occurrence D resolves to series S" (a coverage probe) and to model **multi-split chains** (`<base>_R<ts1>_R<ts2>`) — today it models a single split level only.

### Scenarios + assertions
1. **Clean split, post-boundary correctness (I1/I2/I3/I4).** Split a client series at 03-02 with a time change. Assert: each post-boundary date appears **exactly once** on main and on `client_b` at the NEW time; **no** ghost at the OLD time (I2); the post-boundary main copy's derived id resolves under `_R<ts>`, not the base (I4). *(The existing test asserts none of I2/I3/I4 for the post-boundary segment — this is the gap to close.)*
2. **Pre-boundary unchanged (I1).** Same split; assert pre-boundary occurrences (e.g. 02-09) still mirrored at original time under the truncated base, single copy.
3. **Case 2 — pre-existing modified instances both sides.** Move 02-16 (pre) and 03-16 (post) before the split; split at 03-02. Assert: 02-16 mirror keeps moved time under base, re-creatable; 03-16 mirrors under `_R` at the regular time (Google cancelled the post override); no stale 03-16-at-moved-time ghost.
4. **Case 3 — moved across boundary, main-copy survival + re-create.** Extend `test_moved_instance_survives_this_and_following` to assert the **main FULL copy** (not only the `client_b` busy block) survives at 14:00 and is **re-creatable after a churn-delete** of the main copy. (Today it only churns the peer block at 02-16.)
5. **`client_a` SOURCE survival (I5 / containment).** After every split + churn cycle, assert the **real source occurrence on `client_a` is still alive** (never destructively deleted) — the data-loss invariant. Multi-round reconcile must show `change_counter delta=0` and stable ledger versions (no version-344 loop).
6. **Post-boundary re-create after churn-delete (I4).** Delete a post-boundary main copy and peer block; reconcile; assert both re-create under `_R<ts>` (proves the derived id is no longer a 404 tombstone).
7. **Defer/retry ordering.** Force main-ingest of a post-boundary instance on a pass *before* the `_R` segment is ingested; assert `source_event_id=None`, no outbox op, no 404, and convergence on the next pass.
8. **Case 4 — Meet link per-segment.** Split with conferenceData on the `_R` segment; assert post-boundary main copies carry the NEW link and pre-boundary copies retain the OLD (no bleed). *(Gated on the `payload.py` confirmation, §2 OPEN.)*

---

## 8. Ordered open questions for the deep session (hardest first)
1. **Two churn-breaker predicates disagree (O-0).** Unify the proj_match guard (`main.py:244-249`, tests `matched["status"]`) and the managed-instance handler (`main.py:519-535`, re-derives `inst_canonical`) into one helper that always tests the **occurrence's** live source row — before trusting any live-occurrence signal. Confirm a 404-on-derived-delete that nulls the instance projection's `google_event_id` (`outbox.py:574-583`) cannot re-expose the managed-instance path on a later pass and oscillate.
2. **Diff pre-set must use the owning segment too (§5.2).** Determine whether `diff.py:111-124` needs the same owning-segment selection (it derives off `parent_proj["google_event_id"]`, the base master's main id). Likely BOTH sites must agree; design one shared resolver.
3. **Boundary-occurrence ownership.** The `UNTIL` is "1 second before boundary" (fake `:1601`). Confirm `owning_source_series` routes the on-or-after-boundary occurrence to `_R`, not the base, for both all-day and timed forms.
4. **Multi-split chains** (`<base>_R<ts1>_R<ts2>`). Does coverage-routing generalize so each occurrence keys to the **latest** segment covering its date, and do older `_R` segments get `UNTIL`-truncated like the base did? The fake models only one split level — extend it.
5. **Candidate-set query robustness.** Strip the `_R<ts>` suffix (`YYYYMMDDTHHMMSSZ`) reliably; confirm no real client base id legitimately contains the literal `_R` substring before a suffix. Decide LIKE-prefix vs enumerate-all-masters.
6. **Migration scope & shape.** One-shot content-audit pass vs guarded online migration; MLCommons-only vs all-users. Verify the 165 base-replaced segments' pre-boundary instances still have live main projections before any UPDATE (O-3). Investigate why the 3 active dangling-`_R` instances (`35587/36271/39108`) have segment masters that never materialized (possible ingest-ordering gap for an `_R` segment with an immediate exception).
7. **`payload.py` conferenceData per-segment** — read at field level to confirm no cross-segment Meet-link bleed (Case 4).

---

## 9. Ready-to-run prompt for the future session

```
Solve the long-term "change all events from here forward" (_R this-and-following) recurring-mirror
correctness problem in BusyBridge (/home/pi/busybridge, branch v2). Read the design brief
RECURRING_R_THIS_AND_FOLLOWING_MIRROR_FIX.md first — it is self-contained and file:line-grounded;
trust its RESOLVED facts (O-0 entry point = proj_match cancelled branch; O-2 trigger = row-level
status=='cancelled' on the sticky instance child; O-3 live artifact counts) but re-verify any
load-bearing line before you change it.

GOAL: every live source occurrence (pre- AND post-boundary) has exactly one main full copy and one
peer busy block at its CURRENT time/details/Meet link — no stale copy at the old time, no
dup/ghost, no 404-forever derived ids, converging with zero churn and zero data loss. The real
client (client_a) source occurrence must NEVER be destructively deleted.

WORK ADVERSARIALLY, REPRO-FIRST:
1. Write tests that FAIL against current v2 by asserting the properties the existing
   test_moved_instance_survives_this_and_following omits: I2 (no stale post-boundary copy at the
   OLD time), I3 (single copy / no dup), I4 (post-boundary derived id resolves under <base>_R<ts>,
   not the truncated base → no 404), plus main-FULL-copy survival of a moved instance and
   client_a SOURCE survival across split+churn. Enhance tests/fakes/google_calendar.py as needed
   (it models the additive split + 404-on-out-of-range derived id already; you may need a
   coverage-probe helper and a multi-split-chain model). Drive the MAIN-side instance derivation
   (create a modified main-copy instance so the proj_match branch is exercised) — the parent path
   alone already passes and will not reproduce the defect.
2. Design owning_source_series(user_id, original_start, candidate_masters): pick the source master
   whose LIVE (post-UNTIL) RRULE actually expands to contain the occurrence's ORIGINAL_START
   (match on the un-modified slot via _occurrence_key_from_datetime, honoring all-day vs timed).
   Lift the rrulestr/finite-detection/occurrence-keying helpers out of planner.py into a shared
   module. Tie-break = latest dtstart <= original_start.
3. Fix create-time parenting at ingest/main.py:490-496: derive source_event_id from the OWNING
   segment, never the base unconditionally; if no live segment covers the date, store None
   (defer + retry next pass — NEVER fall back to the base id, that IS the 404-forever bug). Make
   the diff's pre-set (diff.py:111-124) agree (it derives off the base master's main id today).
   DO NOT re-key parent_canonical_uid in place (banned move, guarded by the moved-instance test).
   Unify the two disagreeing churn-breaker predicates (main.py:244-249 vs 519-535) into one helper
   that tests the OCCURRENCE's live source row.
4. Migrate existing wrong-id rows PROJECTION-PRESERVING (UPDATE source_event_id + the instance's
   main-projection google_event_id; snap applied_payload_hash; never DELETE a ledger_event) so you
   never trip trg_ledger_events_block_orphaning_delete (schema.py:233-242). Scope to bound risk.
5. Verify with the full §7 test plan AND the existing suite (727 selected baseline; keep the 3
   xfail destructive-propagation tests xfail and the 3 conservative-fix safety tests passing).
   Confirm zero churn (no version-344 loop), I1–I5 hold, and the conference-debounce (e844296)
   still adopts links only on two consecutive ingests with no cross-segment bleed.

HARD CONSTRAINTS — do NOT regress: no re-key; additive _R; moved-instance survival; no write-loop;
delete_source containment intact (source_delete_pending must stay un-armed — never SET it to 1,
OP_DELETE_SOURCE stays unreachable); conference debounce intact. Destructive main->source
occurrence-delete propagation is OUT OF SCOPE (deprioritized). Use READ-ONLY commands for any live
/data/calendar-sync.db inspection; NEVER write to the live DB or to Google.

Work the §8 open questions hardest-first; the boundary-occurrence and multi-split-chain cases and
the diff-pre-set agreement are the subtle ones. Land the fix as a focused PR off v2 with the new
tests, and report I1–I5 pass/fail explicitly.
```

---

> **Future-optional footnote (NOT a goal of this brief):** destructive main→source occurrence-delete propagation ("decline/delete-source" end-state, the deprioritized Layer-1 work) remains disabled by the conservative fix `1e41a1c` and is encoded as 3 `xfail` tests. Re-enabling it requires a sound discriminator between a genuine user cancel on main and a BB-authored `_R` tombstone — the design doc's `bb_proj_id`-on-parent marker is **unsound** (stamped on the managed parent and inherited by every cancelled exception, so it cannot distinguish the two). Solving the `_R` mirror correctness above *removes the BB-authored tombstone source entirely* for live occurrences, which should make a future discriminator far simpler — but that is separate work and must not arm `source_delete_pending`.

---

**Files referenced (all absolute):** `/home/pi/busybridge/app/ledger/ingest/main.py`, `/home/pi/busybridge/app/ledger/ingest/client.py`, `/home/pi/busybridge/app/ledger/identity.py`, `/home/pi/busybridge/app/ledger/diff.py`, `/home/pi/busybridge/app/ledger/planner.py`, `/home/pi/busybridge/app/ledger/outbox.py`, `/home/pi/busybridge/app/ledger/payload.py`, `/home/pi/busybridge/app/ledger/schema.py`, `/home/pi/busybridge/tests/fakes/google_calendar.py`, `/home/pi/busybridge/tests/test_moved_instance_survives_this_and_following.py`, `/home/pi/busybridge/tests/test_main_managed_instance_cancellation.py`. Suggested committed filename: `/home/pi/busybridge/RECURRING_R_THIS_AND_FOLLOWING_MIRROR_FIX.md`.
