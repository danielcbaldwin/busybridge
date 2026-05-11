"""Ingest paths: source-of-truth → ledger upserts.

One module per source kind (REWRITE_PLAN.md §5).  Each runs
inside a single DB transaction that includes the sync-token
update — partial failures cannot lose events.
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
