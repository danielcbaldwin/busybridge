"""Integration tests for the new ledger/projection/outbox system.

Tests in this package run the full pipeline against the in-memory
fakes from ``tests.fakes`` (no network, no real Google).  See
``REWRITE_PLAN.md`` §14 Layer 3 for the design.
"""
