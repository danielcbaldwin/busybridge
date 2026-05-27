# WebCal Placement Spec

## Goal

Let each WebCal subscription live on either the user's main calendar or one
selected client calendar.

Examples:

| WebCal | Placement | Result |
| --- | --- | --- |
| TripIt | Main | Full details on main, Busy blocks on all clients |
| ISO Events | MLCommons | Full details on main and MLCommons, Busy blocks on other clients |

## Core Model

A WebCal feed is always the source of truth. Placement controls where
BusyBridge publishes managed projections.

For ISO placed on MLCommons:

```text
Truth source: ISO WebCal
Work context / placement: MLCommons
Detailed projections: main + MLCommons
Opaque projections: other clients
```

The selected client calendar is not the source. It is a managed output
target.

## User Flow

Add/edit WebCal form:

```text
URL
Display name / prefix

Where should this WebCal live?
( ) Main calendar
( ) Client calendar: [MLCommons v]
```

WebCal list:

```text
TripIt       Main
ISO Events   MLCommons
ACM Events   ⚠ Placement target disconnected — pick a new one
```

Helper text:

```text
Main calendar:
Events appear on your main calendar. Busy blocks are created on all
client calendars.

Client calendar:
Events appear on that client calendar and your main calendar. Busy
blocks are created on other client calendars.
```

"Main calendar" means the designated main-account's primary calendar
— the same calendar BusyBridge already routes main projections to,
not "any account the user has connected." A user with multiple
connected Google accounts sees their non-main accounts only in the
Client dropdown (and only if those accounts' calendars are added as
clients).

Form behavior:

```text
Client calendar dropdown:
  - lists only currently active client calendars (is_active = 1)
  - does NOT list inactive / disconnected clients

User has zero active client calendars:
  - "Client calendar" radio is disabled
  - inline hint: "Connect a client calendar first to use this option"

Editing a subscription whose placement target is currently
disconnected (placement_target_status = 'disconnected'):
  - "Client calendar" radio is selected
  - dropdown shows: "(disconnected — pick a new one)" pre-selected,
    plus the normal list of active clients
  - submitting without picking a new value re-validates and
    rejects (per Data Model: target must exist and be active)
```

## Behavior

Busy/opaque events (`show_as != "free"`):

| Placement | Main | Selected Client | All Other Clients |
| --- | --- | --- | --- |
| Main | Full details | — | Busy |
| Client X | Full details | Full details | Busy |

Free/transparent events (`show_as == "free"`):

| Placement | Main | Selected Client | All Other Clients |
| --- | --- | --- | --- |
| Main | Full details, free | — | No projection |
| Client X | Full details, free | Full details, free | No projection |

The "Selected Client" column does not apply when placement is Main.

## Identity Rules

Placement must not affect `ledger_events.canonical_uid`.

Keep existing WebCal identity behavior (already implemented in
`app/ledger/identity.py:57-112` and `app/ledger/ingest/webcal.py`):

```text
Stable UID:
  webcal:{subscription_id}:{ics_uid}

Missing UID or UUIDv4-style unstable UID:
  webcal:{subscription_id}:hash:{sha256(start_at|end_at)}
  ordinal suffix appended for same-time collisions
```

Preserve:

- unstable hash excludes summary/title
- renaming unstable events does not create duplicates
- same-time unstable events get ordinal suffixes
- UID stability is classified per event, not per feed
- `RECURRENCE-ID` overrides remain separate instance rows

Changing placement must replan existing ledger rows, not re-ingest WebCal
events as new rows.

## Data Model

Add to `webcal_subscriptions`:

```sql
placement_kind TEXT NOT NULL DEFAULT 'main'
-- allowed: 'main', 'client'

placement_client_calendar_id INTEGER NULL
  REFERENCES client_calendars(id) ON DELETE SET NULL

placement_client_display_name_cache TEXT NULL
-- snapshot of the placement client's display_name at the time placement
-- was last set; used by the disconnect alert so the email can still
-- name the target after the row is deactivated, renamed, or hard-deleted.
```

Migration uses the existing ALTER TABLE pattern in
`app/database.py:240-257` (idempotent, duplicate-column swallow).
Existing WebCal subscriptions default to `placement_kind = 'main'`.

Validation on create/update:

```text
placement_kind = main:
  placement_client_calendar_id must be null
  placement_client_display_name_cache must be null

placement_kind = client:
  placement_client_calendar_id is required
  the referenced client_calendars row exists
  the referenced row is active (is_active = 1)
  the referenced row belongs to the same user as the subscription
  placement_client_display_name_cache is written from
    client_calendars.display_name at this moment
```

Critical ID-space rule:

```text
ledger_events.source_calendar_id remains webcal_subscriptions.id.

For source_type = webcal, never treat source_calendar_id as
client_calendars.id. The two id spaces are independent SQLite rowid
sequences; a numeric collision is normal and must not misroute
projections or trip the origin-exclusion logic in
_resolve_targets (app/ledger/planner.py:236-246, which gates on
source_type in ('client',) and must not be loosened for webcal).

Use webcal_subscriptions.placement_client_calendar_id for client
placement.
```

## Runtime Loading

When loading WebCal subscriptions for reconcile, include:

```text
id
url
display_prefix
placement_kind
placement_client_calendar_id
placement_client_display_name        (live, via JOIN, NULL if inactive)
placement_client_color_id            (live, via JOIN, NULL if inactive)
placement_client_is_active           (live, via JOIN)
placement_client_display_name_cache  (snapshot from the subscription row)
```

This metadata is for planning, rendering, and alert payloads only. It
must not change WebCal ingest identity.

## Planner Rules

For `source_type = webcal`:

```text
if placement_kind = main:
  main = present_full
  all clients = present_busy unless show_as = free
  (no per-client full copy)

if placement_kind = client AND placement target is active:
  main = present_full
  selected client = present_full
  all other clients = present_busy unless show_as = free

if placement_kind = client AND
   (placement_client_calendar_id IS NULL OR target row inactive):
  STALE PLACEMENT — see Placement Target Lifecycle
  main = present_full
  all clients = present_busy unless show_as = free
  (no per-client full copy; behaves identically to placement_kind=main
   for projection purposes; differs only in alerts/UI and color/footer)
```

If the WebCal event is cancelled or removed:

```text
all projections = absent
```

The planner does NOT auto-flip `placement_kind` on stale placements;
the user must repick. The stale state is purely a runtime condition,
detectable from the JOIN to `client_calendars`.

For ISO placed on MLCommons (active):

```text
ledger_events.source_type = webcal
ledger_events.source_calendar_id = ISO webcal subscription id

main projection:
  target_kind = main
  target_calendar_id = null
  desired_state = present_full

MLCommons projection:
  target_kind = client
  target_calendar_id = MLCommons client calendar id
  desired_state = present_full

other client projections:
  target_kind = client
  target_calendar_id = other client id
  desired_state = present_busy
```

## Payload Rules

Do not add a separate WebCal-to-Google writer.

All WebCal outputs must be normal BusyBridge ledger projections rendered
through the existing payload/diff/outbox path
(`app/ledger/payload.py`, `app/ledger/diff.py`, `app/ledger/outbox.py`).

Full-detail projections include:

```text
summary
description
location
start/end
recurrence
show-as / transparency
standard BusyBridge managed markers:
  - description appended with [BusyBridge] tag (managed_tag(),
    payload.py:302-311, 458-465)
  - extendedProperties.private.bb_proj_id
  - extendedProperties.private.bb_ledger_version
  - extendedProperties.private.bb_target_kind
  - deterministic Google event id (derive_google_event_id,
    identity.py:141-186) — per-projection id ensures different
    Google ids on different target calendars
```

Busy projections include:

```text
summary = "Busy"
start/end
recurrence
visibility = private
transparency = opaque
standard BusyBridge managed markers (same as above)
```

Selected-client full-detail copies:

```text
must not be forced private (full copies do not set visibility=private;
  only busy blocks do, payload.py:204-218)
must include the [BusyBridge] managed description tag (automatic via
  _tag_description -> _full_copy_description, payload.py:458-465 +
  140-201)
must use deterministic managed Google event ids (automatic — each
  projection row has its own auto-increment id and thus its own derived
  Google id; ledger_projections UNIQUE(ledger_event_id, target_kind,
  target_calendar_id) at schema.py:119 guarantees distinct rows per
  target)
must be recognized by cleanup, clean export, and orphan sweep
  (automatic via is_managed_google_event_id and the bb_proj_id
  extended property; see discovery.py:121-202 and
  sync/ics_export.py:448-635)
```

## Label, Footer, Color

For WebCal full-detail copies, footer:

```text
Source: {display_prefix or feed host fallback}
Placement: {placement client display name}     (omitted when
                                                placement_kind=main
                                                or stale)
[BusyBridge]
```

The Source line names the FEED (`webcal_subscriptions.display_prefix`,
falling back to the feed URL's hostname when blank). The Placement line
names the work-context client calendar. They are intentionally
distinct.

For ISO placed on MLCommons (active):

```text
Source: ISO Events
Placement: MLCommons
[BusyBridge]
```

For TripIt placed on Main:

```text
Source: TripIt
[BusyBridge]
```

Do not label the technical source as MLCommons. MLCommons is placement
/ work context; ISO is the feed source.

Color:

```text
placement_kind = main:
  main full copy: no colorId
  client busy blocks: no colorId override

placement_kind = client (target active):
  main full copy: colorId = placement client's color_id
  selected-client full copy: colorId = placement client's color_id
  other-client busy blocks: no colorId override

placement_kind = client (stale, target inactive or null):
  treat as placement_kind = main for color (no colorId on main)
```

Implementation — extend the existing source-label/color join.

Today's join at `app/ledger/planner.py:376-384` and
`app/ledger/diff.py:205-224` only handles `source_type IN ('client',
'personal')`:

```sql
LEFT JOIN client_calendars cc
  ON cc.id = e.source_calendar_id
 AND e.source_type IN ('client', 'personal')
```

Add a parallel join for webcal:

```sql
LEFT JOIN webcal_subscriptions ws
  ON ws.id = e.source_calendar_id
 AND e.source_type = 'webcal'
LEFT JOIN client_calendars cc_placement
  ON cc_placement.id = ws.placement_client_calendar_id
 AND cc_placement.is_active = 1
```

Hydrate row fields:

```text
source_label       := cc.display_name           (client/personal)
                   or ws.display_prefix          (webcal)
calendar_color_id  := cc.color_id               (client/personal)
                   or cc_placement.color_id      (webcal, placement
                                                  active)
                   or NULL                       (webcal main-placed
                                                  or stale placement)
placement_label    := cc_placement.display_name  (webcal, placement
                                                  active)
                   or NULL
```

## Triggers

A replan of all active WebCal ledger rows for a subscription must be
enqueued when:

```text
1. The subscription's placement_kind or placement_client_calendar_id
   changes (see Placement Changes).

2. The placement client calendar's display_name or color_id changes.
   Extend recolor_client_calendar (app/ledger/admin_ops.py:25-66)
   and any rename path to also bump webcal_subscriptions rows where
   placement_client_calendar_id matches the changed client. Today the
   filter is source_type IN ('client', 'personal') AND
   source_calendar_id = ?; add a second pass for source_type = 'webcal'
   AND source_calendar_id IN (subscriptions placed on this client).

3. The placement client calendar becomes inactive
   (disconnect_calendar, admin_ops.py:192-217). In addition to the
   replan, raise a placement_target_disconnected alert (see Alerts).

4. The placement client calendar's row is hard-deleted (FK fires
   ON DELETE SET NULL). The normal disconnect flow is soft-delete and
   is handled by case 3 above; the only routine hard-delete path is
   factory-reset (app/api/admin.py:343), which wipes the whole user
   account — no alert is needed there.

5. The subscription is deleted (existing behavior, see Deletion).
```

Use the existing primitives:
`_append_affected` (`app/ledger/admin_ops.py`) +
`enqueue_manual` (`app/ledger/triggers.py`) +
`record_affected_events` (`app/ledger/triggers.py:100-122`).

## Manual Changes

WebCal projections are managed output.

| User action | Expected result |
| --- | --- |
| Deletes main WebCal copy | BusyBridge recreates it |
| Deletes selected-client full copy | BusyBridge recreates it |
| Deletes other-client Busy block | BusyBridge recreates it |
| Edits main WebCal copy | BusyBridge restores feed values |
| Edits selected-client full copy | BusyBridge restores feed values |
| Edits Busy block | BusyBridge restores Busy block |

No per-event suppression in v1.

## Source-of-Truth Rules

- Do not write edits back to WebCal feed.
- Do not write edits from main to selected client.
- Do not write edits from selected client to main.
- Feed changes update all managed projections.
- Feed removals delete all managed projections.
- Feed UID/content-hash behavior remains unchanged.

## Pause vs Delete

These are deliberately different and must stay different.

```text
Subscription is_active = false (paused):
  polling stops (existing behavior, ingest/webcal.py:80-84)
  existing ledger rows remain status = 'active'
  existing projections remain in place on every target
  feed updates do NOT flow through while paused
  re-enabling resumes polling on the next ingest tick

Subscription deleted (DELETE /webcal-subscriptions/{id}):
  every ledger row from this subscription set status = 'cancelled'
  subscription set is_active = false
  affected_ledger_events appended for all cancelled rows
  enqueue_manual reconcile fires
  next reconcile drains all projections to absent
  this is the ONLY path that removes managed copies from clients
```

## Placement Changes

Changing placement must, in a single database transaction:

```text
0. No-op check: if neither placement_kind nor
   placement_client_calendar_id would change (values match the
   current row), return success without any side effects. Do NOT
   append affected rows, do NOT enqueue a reconcile, do NOT write
   to sync_log. The endpoint is idempotent.
1. Validate the target (see Data Model validation rules).
2. Update placement_kind and placement_client_calendar_id.
3. Update placement_client_display_name_cache from the new target
   (or NULL when switching to placement_kind=main).
4. Append every active ledger_event.id from this subscription to
   affected_ledger_events.
5. enqueue_manual reconcile.
6. INSERT into sync_log with action='change_placement',
   status='success', and details capturing
   {old_kind, old_target_id, new_kind, new_target_id} so the change
   is auditable alongside the existing create / disconnect_webcal
   entries.
```

If any step fails the whole transaction rolls back. Placement and the
replan queue must never disagree.

Main to Client X:

```text
main full copy remains (now colored by Client X)
client X Busy block becomes a full copy
other clients remain Busy
```

Client X to Main:

```text
main full copy remains (now uncolored)
client X full copy becomes a Busy block
other clients remain Busy
```

Client X to Client Y:

```text
main full copy remains (recolored from X's color to Y's color)
client X full copy becomes a Busy block
client Y Busy block becomes a full copy
other clients remain Busy
```

No duplicate ledger events should be created — only the per-target
`ledger_projections` rows transition state through the existing
`_mark_implicit_absent` path (`app/ledger/planner.py:77-86, 336-363`).

## Placement Target Lifecycle

When a client calendar that is the placement target of any WebCal
subscription becomes inactive (`disconnect_calendar` flips
`is_active = 0`) or is hard-deleted (FK fires `ON DELETE SET NULL`):

```text
1. For each affected webcal subscription:
   a. placement_client_calendar_id either remains pointing at an
      inactive row (soft-delete) or is set to NULL (hard-delete).
   b. placement_kind stays 'client' — the planner does NOT auto-flip
      to 'main'. The user must repick.
   c. Append every active ledger row from that subscription to
      affected_ledger_events and enqueue_manual reconcile.
   d. Raise a placement_target_disconnected alert (see Alerts).
   e. Dashboard list surfaces a "Placement target disconnected" badge
      (driven by placement_target_status = 'disconnected' in the API
      list response).
   f. INSERT into sync_log with
      action='webcal_placement_target_disconnected', status='warning',
      details capturing the subscription id, the client_calendar id,
      and the placement_client_display_name_cache value. One row per
      affected subscription, not one per ledger event.

2. While in the stale state, the planner treats the subscription
   like placement_kind = main for projections: main gets the full
   copy, all clients get busy blocks (or absent when free). Footer
   drops the Placement line; main copy renders uncolored.

3. When the user repicks a valid active placement (Client or Main):
   the alert state clears, the badge clears, the new placement's
   color and label propagate to all projections on the next
   reconcile, and a fresh future disconnect re-triggers the alert.
```

Note on capturing the target name: the alert email needs to name the
lost target. `placement_client_display_name_cache` is written every
time placement is set so the name survives the cascade. For the
soft-delete path, `disconnect_calendar` should also update the cache
(snapshot the latest display_name) before flipping `is_active = 0`,
in case the user renamed the client after originally picking it.

## Alerts

Add alert type `webcal_placement_disconnected`.

Reuse the existing alert + email infrastructure (`app/alerts/`,
patterns from `tests/test_alerts_email_*.py`,
`tests/test_alert_backoff.py`, `tests/test_failing_calendar_alert.py`).

Email must include:

```text
- The feed name (display_prefix, or the feed URL host as a fallback).
- The lost placement target name (from
  placement_client_display_name_cache so it survives row deletion
  or rename).
- An explicit description of current behavior:
    * events still appear on the user's main calendar
    * events no longer appear on the disconnected calendar
    * other clients still receive Busy blocks
- One concrete next step: open the WebCal subscription and either
  pick a new client placement or switch to Main.
```

Email throttling: reuse the existing alert backoff so a long-
disconnected placement does not email more than once per backoff
window. Clear the alert state when the user repicks a valid
placement.

Alert fanout: exactly ONE alert per (subscription, disconnect event),
regardless of how many ledger rows the subscription has. The replan
in §Placement Target Lifecycle step 1.c touches every active ledger
row from that subscription, but the alert dedupes on subscription_id
— the user gets one email per affected feed, not one per event.
Disconnecting a single client calendar that holds the placement for
N webcal subscriptions therefore raises N alerts (one per
subscription), each naming the same lost target.

Suggested copy:

```text
Subject: BusyBridge — your "{feed_name}" WebCal needs a new home

Hi {user_first_name},

You disconnected the "{placement_target_name}" calendar from
BusyBridge.

That calendar was the placement target for one of your WebCal
feeds:

  Feed:        {feed_name}
  URL:         {feed_url}
  Was placed:  {placement_target_name} (now disconnected)

What this means right now:
  • Events from {feed_name} still appear on your main calendar.
  • Events no longer appear on {placement_target_name}.
  • Other client calendars still receive Busy blocks as usual.

What to do:
  Open BusyBridge → WebCal subscriptions → {feed_name}, and either:
    • pick a different client calendar as the placement target, or
    • switch the placement to "Main calendar" only.

Until you do, the {feed_name} feed will keep working on your main
calendar — just without a client-side full copy.

— BusyBridge
```

## API Changes

Create WebCal request:

```json
{
  "url": "...",
  "display_prefix": "ISO Events",
  "placement_kind": "client",
  "placement_client_calendar_id": 123
}
```

`placement_kind` defaults to `"main"` when omitted.
`placement_client_calendar_id` is required when `placement_kind="client"`
and forbidden otherwise.

Update WebCal request:

```json
{
  "display_prefix": "ISO Events",
  "placement_kind": "client",
  "placement_client_calendar_id": 123
}
```

All fields optional. Sending only `display_prefix` must not change
placement. Sending only placement fields must not change
`display_prefix`. Changing placement triggers the §Placement Changes
transaction.

List response:

```json
{
  "id": 1,
  "url": "...",
  "display_prefix": "ISO Events",
  "placement_kind": "client",
  "placement_client_calendar_id": 123,
  "placement_client_display_name": "MLCommons",
  "placement_target_status": "active"
}
```

`placement_target_status` is one of:

```text
"not_applicable"  -- placement_kind = main
"active"          -- placement_kind = client and target is active
"disconnected"    -- placement_kind = client and target is inactive
                     or null (drives the dashboard badge)
```

The list endpoint must `LEFT JOIN client_calendars` on
`placement_client_calendar_id` to populate `placement_client_display_name`
and derive `placement_target_status`.

## Deletion

Deleting a WebCal subscription cancels every ledger event from that
subscription. The existing implementation
(`app/api/webcal.py:186-242`) already handles this correctly; placement
adds no special cases.

BusyBridge removes:

```text
main full copies
selected-client full copies
all client Busy blocks
```

## Tests

Existing tests to preserve unchanged:

```text
- webcal stable UID and unstable hash behavior (excluding summary)
- RECURRENCE-ID instance separation
- ordinal disambiguation for same-time unstable events
- managed-tag and deterministic-ID recognition by cleanup, clean
  export, and orphan sweep
```

Add/update tests:

```text
1.  Existing WebCal subscriptions default to Main placement after
    migration.
2.  Main-placed WebCal keeps current behavior (full on main, busy on
    all clients, no per-client full copy).
3.  Client-placed WebCal creates full details on main and on the
    selected client.
4.  Client-placed WebCal creates Busy blocks on every other client.
5.  Free/transparent client-placed WebCal creates full details on
    main and selected client, no Busy blocks anywhere else.
6.  Selected-client full copy includes the [BusyBridge] managed
    description tag.
7.  Selected-client full copy is not forced private.
8.  Deleting selected-client full copy in Google causes BusyBridge to
    recreate it on next reconcile.
9.  Editing selected-client full copy in Google is overwritten by
    feed values on next reconcile.
10. Removing an event from the feed deletes all projections — main,
    selected-client full copy, and every busy block.
11. Changing Main → Client X updates projections in place without
    creating duplicate ledger rows.
12. Changing Client A → Client B updates projections without duplicate
    ledger rows; B gets the full copy, A reverts to a busy block.
13. canonical_uid is unchanged after a placement change.
14. Unstable-UID feed does not duplicate events after placement is
    added.
15. Renaming an unstable-UID event updates the existing row (summary
    is excluded from the hash).
16. WebCal subscription id N and client_calendar id N for the same
    user: placements and projections are not misrouted across the
    id-space collision; _resolve_targets origin-exclusion does not
    fire for source_type='webcal'.
17. Cleanup / re-sync recognizes placed WebCal projections as managed
    (deterministic ID and bb_proj_id property).
18. Clean ICS export filters placed WebCal projections.
19. Orphan sweep recognizes placed WebCal projections.
20. Footer, label, and color for client-placed WebCal use the
    placement client metadata (via webcal_subscriptions →
    client_calendars join), not source_calendar_id.
21. Recurring series with feed-side EXDATE on a client-placed
    subscription: placed full copy on selected client and busy blocks
    elsewhere all become absent on the excluded instance.
22. RECURRENCE-ID override of a client-placed series renders as a
    placed full copy on main + selected client and busy elsewhere,
    with no duplicate ledger rows.
23. Placement target disconnected (is_active=0): replan fires, badge
    appears, placement_target_disconnected alert raised; planner
    falls back to main-only full copy with busy blocks on every
    client; footer drops the Placement line; main copy is uncolored.
24. Placement target re-picked after disconnect: alert clears, badge
    clears, new target's color and label appear in projections on
    next reconcile.
25. Hard delete of placement client (FK ON DELETE SET NULL):
    placement_client_calendar_id becomes NULL, placement_kind stays
    'client'; planner behaves identically to the disconnected case.
26. Recoloring or renaming the placement client calendar re-renders
    all placed WebCal projections on next reconcile (extension of
    recolor_client_calendar covers source_type='webcal').
27. Placement change is atomic: a simulated failure between persist
    and enqueue rolls back placement; no half-applied state.
28. Pausing a subscription (is_active=false) does not cancel
    projections; they remain on every target until unpaused or
    deleted. Re-enabling resumes feed updates.
29. Subscription deletion cancels every projection including the
    placed selected-client full copy.
30. Footer rendering: display_prefix appears as "Source:", placement
    client display name appears as "Placement:", main-placed
    subscriptions omit the Placement line entirely.
31. PATCH with placement values identical to current row is a no-op:
    no affected_ledger_events appended, no reconcile enqueued, no
    sync_log entry, response is success.
32. Disconnecting a client calendar that is the placement target of
    multiple webcal subscriptions raises exactly one alert per
    subscription (N alerts for N subscriptions), each naming the
    same lost target; the alert does not fan out per ledger event.
33. sync_log receives a 'change_placement' row on placement change
    and a 'webcal_placement_target_disconnected' row on target
    disconnect, with the expected details payload.
34. UI form: with zero active client calendars the Client radio is
    disabled and the helper text is shown; with the placement
    target in the disconnected state the dropdown shows
    "(disconnected — pick a new one)" pre-selected; the dropdown
    never lists inactive client calendars.
```

## Non-Goals

- WebCal writeback.
- WebCal RSVP propagation.
- Treating the selected client calendar as a source of truth.
- Permanently suppressing individual feed events.
- Publishing full WebCal details to every client.
- Changing WebCal UID / canonical identity behavior.
- Adding a second sync path outside ledger projections.
- Per-feed color override when `placement_kind=main` (main-placed
  WebCal copies stay uncolored in v1).
- Auto-flipping `placement_kind` to `main` when a target disconnects
  (the user must repick).

## Implementation Order

1. **Schema + migration.** Add `placement_kind`,
   `placement_client_calendar_id` (with `ON DELETE SET NULL`), and
   `placement_client_display_name_cache` to `webcal_subscriptions`
   via the idempotent ALTER TABLE pattern.
2. **API request/response.** Extend create/update validation, add the
   list-endpoint JOIN, derive `placement_target_status`. Tests 1, 27,
   30.
3. **Dashboard UI.** Placement radio + client dropdown on add/edit,
   "Placement target disconnected" badge in the list. Driven entirely
   by `placement_target_status`.
4. **Runtime load.** Wire the new `webcal_subscriptions →
   client_calendars` join into planner and diff row hydration. Tests
   16, 20.
5. **Planner.** Branch on `placement_kind` and the stale-placement
   condition; emit per-target desired states accordingly. Tests 2-5,
   21, 22, 23.
6. **Payload / footer / color.** Source vs Placement footer lines,
   colorId from placement client when active. Tests 6, 7, 20, 30.
7. **Placement-change handler.** Atomic transaction:
   validate → persist → cache display name → append affected →
   enqueue manual. Tests 11, 12, 13, 14, 15, 27.
8. **Recolor / rename trigger extension.** Extend
   `recolor_client_calendar` and any rename path to also enqueue
   webcal subscriptions placed on the changed client. Test 26.
9. **Disconnect trigger + alert.** Extend `disconnect_calendar` to
   snapshot `placement_client_display_name_cache`, append affected
   ledger rows, raise the `placement_target_disconnected` alert.
   Tests 23, 24, 25.
10. **Alert email template + backoff.** Reuse existing alert
    infrastructure. Test 23.
11. **Regression sweep.** Pause/delete semantics, identity stability
    under placement change. Tests 28, 29.
12. **Full pytest and Docker smoke.**
