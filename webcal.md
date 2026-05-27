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

The selected client calendar is not the source. It is a managed output target.

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
```

Helper text:

```text
Main calendar:
Events appear on your main calendar. Busy blocks are created on all client calendars.

Client calendar:
Events appear on that client calendar and your main calendar. Busy blocks are created on other client calendars.
```

## Behavior

| Placement | Main | Selected Client | Other Clients |
| --- | --- | --- | --- |
| Main | Full details | Busy | Busy |
| Client X | Full details | Full details | Busy |

For free/transparent WebCal events:

| Placement | Main | Selected Client | Other Clients |
| --- | --- | --- | --- |
| Main | Full details, free | No projection | No projection |
| Client X | Full details, free | Full details, free | No projection |

## Identity Rules

Placement must not affect `ledger_events.canonical_uid`.

Keep existing WebCal identity behavior:

```text
Stable UID:
  webcal:{subscription_id}:{ics_uid}

Missing UID or UUIDv4-style unstable UID:
  webcal:{subscription_id}:hash:{start_at|end_at}
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
-- references client_calendars(id)
```

Validation:

```text
placement_kind = main:
  placement_client_calendar_id must be null

placement_kind = client:
  placement_client_calendar_id is required
  client calendar exists
  client calendar belongs to same user
  client calendar is active
```

Migration:

```text
Existing WebCal subscriptions default to placement_kind = main.
```

Critical ID-space rule:

```text
ledger_events.source_calendar_id remains webcal_subscriptions.id.

For source_type = webcal, never treat source_calendar_id as client_calendars.id.

Use webcal_subscriptions.placement_client_calendar_id for client placement.
```

## Runtime Loading

When loading WebCal subscriptions for reconcile, include:

```text
id
url
display_prefix
placement_kind
placement_client_calendar_id
placement_client_display_name
placement_client_color_id
```

This metadata is for planning/rendering only. It must not change WebCal ingest
identity.

## Planner Rules

For `source_type = webcal`:

```text
if placement_kind = main:
  main = present_full
  all clients = present_busy unless show_as = free

if placement_kind = client:
  main = present_full
  selected client = present_full
  all other clients = present_busy unless show_as = free
```

If the WebCal event is cancelled or removed:

```text
all projections = absent
```

For ISO placed on MLCommons:

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

All WebCal outputs must be normal BusyBridge ledger projections rendered through
the existing payload/diff/outbox path.

Full-detail projections include:

```text
summary
description
location
start/end
recurrence
show-as / transparency
standard BusyBridge managed markers
```

Busy projections include:

```text
summary = Busy
start/end
recurrence
visibility = private
standard BusyBridge managed markers
```

Selected-client full-detail copies:

```text
must not be forced private
must include [BusyBridge] / managed description tag
must use deterministic managed Google event IDs
must be recognized by cleanup, clean export, and orphan sweep
```

## Label, Footer, Color

For WebCal full-detail copies, recommended footer:

```text
Source: {display_prefix or feed host/name}
Placement: {Main or selected client display name}
[BusyBridge]
```

For ISO placed on MLCommons:

```text
Source: ISO Events
Placement: MLCommons
[BusyBridge]
```

Do not label the technical source as MLCommons. MLCommons is placement/work
context; ISO is the feed source.

For client-placed WebCal:

```text
main full-detail copy should use selected client's color_id if available
selected-client full-detail copy may use that color_id if appropriate
other-client Busy blocks remain standard Busy blocks
```

Implementation warning:

```text
Current source label/color joins only handle source_type IN ('client', 'personal').

For source_type = webcal, join:
ledger_events.source_calendar_id -> webcal_subscriptions.id
webcal_subscriptions.placement_client_calendar_id -> client_calendars.id
```

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

## Placement Changes

Changing placement must enqueue a replan for all active ledger events from that
subscription.

Main to Client X:

```text
main full copy remains
client X Busy block becomes full copy
other clients remain Busy
```

Client X to Main:

```text
main full copy remains
client X full copy becomes Busy block
other clients remain Busy
```

Client X to Client Y:

```text
main full copy remains
client X full copy becomes Busy block
client Y Busy block becomes full copy
other clients remain Busy
```

No duplicate ledger events should be created.

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

Update WebCal request:

```json
{
  "display_prefix": "ISO Events",
  "placement_kind": "client",
  "placement_client_calendar_id": 123
}
```

List response:

```json
{
  "id": 1,
  "url": "...",
  "display_prefix": "ISO Events",
  "placement_kind": "client",
  "placement_client_calendar_id": 123,
  "placement_client_display_name": "MLCommons"
}
```

Changing placement should:

```text
validate target
persist placement fields
append affected ledger ids for all active events in that subscription
enqueue manual reconcile
```

## Deletion

Deleting a WebCal subscription cancels every ledger event from that
subscription.

BusyBridge removes:

```text
main full copies
selected-client full copies
all client Busy blocks
```

## Tests

Add/update tests for:

1. Existing WebCal subscriptions default to Main placement.
2. Main-placed WebCal keeps current behavior.
3. Client-placed WebCal creates full details on main and selected client.
4. Client-placed WebCal creates Busy blocks on other clients.
5. Free/transparent client-placed WebCal creates full details on main/selected client and no Busy blocks elsewhere.
6. Selected-client full copy includes `[BusyBridge]` managed tag.
7. Selected-client full copy is not forced private.
8. Deleting selected-client full copy recreates it.
9. Editing selected-client full copy is overwritten by feed values.
10. Removing event from feed deletes all projections.
11. Changing Main to Client updates projections without duplicate ledger rows.
12. Changing Client A to Client B updates projections without duplicate ledger rows.
13. `canonical_uid` is unchanged after placement change.
14. Unstable UID feed does not duplicate after placement is added.
15. Renaming unstable UID event updates existing row.
16. WebCal subscription id collision with client calendar id does not exclude or misroute projections.
17. Cleanup/re-sync recognizes placed WebCal projections as managed.
18. Clean ICS export filters placed WebCal projections.
19. Orphan sweep recognizes placed WebCal projections.
20. Footer/label/color for client-placed WebCal uses placement client metadata, not `source_calendar_id`.

## Non-Goals

- WebCal writeback.
- WebCal RSVP propagation.
- Treating selected client calendar as source.
- Permanently suppressing individual feed events.
- Publishing full WebCal details to every client.
- Changing WebCal UID/canonical identity behavior.
- Adding a second sync path outside ledger projections.

## Implementation Order

1. Add schema fields and migrations.
2. Extend API request/response validation.
3. Update dashboard add/edit UI.
4. Load WebCal placement metadata in runtime.
5. Extend planner target resolution for WebCal placement.
6. Extend payload metadata/color labeling for placed WebCal.
7. Add placement-change replan/enqueue behavior.
8. Add focused tests for placement, identity stability, managed tags, deletion recovery, and ID-space collisions.
9. Run full pytest and Docker smoke.
