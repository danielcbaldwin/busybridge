"""Canonical-ledger architecture for BusyBridge sync (REWRITE_PLAN.md §3).

This package owns the new sync pipeline:

* Schema (``schema.py``) — the four new tables (``ledger_events``,
  ``ledger_projections``, ``outbox_operations``, ``reconcile_requests``).
* Identity (``identity.py``) — canonical UIDs and deterministic
  Google event IDs.
* Payload rendering (``payload.py``) — turning a ledger row into
  the body we send to Google for each target/state.
* Google client (``google_client.py``) — the protocol the outbox
  drain talks to; the test fake plugs in here.
* Outbox (``outbox.py``) — idempotent, etag-gated writes.
* Planner (``planner.py``) — ledger row → desired projection set.
* Diff (``diff.py``) — desired vs. current → enqueue.
* Ingest paths (``ingest/``) — pull from sources, upsert ledger.
* Reconciler (``reconciler.py``) — orchestrates the above.

The old code under ``app/sync/`` continues to run in parallel
during Stage 2; cutover (Stage 5) is the single coordinated
swap-over described in REWRITE_PLAN.md §13.
"""
