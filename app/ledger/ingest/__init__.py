"""Ingest paths: source-of-truth → ledger upserts.

One module per source kind.  The connection runs in autocommit
(see app/database.py), so a pass is NOT one enclosing transaction;
instead the sync-token update is deliberately the LAST durable
write of a pass, so a crash mid-pass simply re-ingests from the
old token next time (idempotent) — partial failures cannot lose
events.  See app/ledger/ingest/client.py's module docstring for
the full rationale.
"""

from app.ledger.ingest.client import ingest_client_calendar
from app.ledger.ingest.discovery import discover_orphans
from app.ledger.ingest.main import ingest_main_calendar
from app.ledger.ingest.personal import ingest_personal_calendar
from app.ledger.ingest.webcal import ingest_webcal_subscription

__all__ = [
    "ingest_client_calendar",
    "ingest_main_calendar",
    "ingest_personal_calendar",
    "ingest_webcal_subscription",
    "discover_orphans",
]
