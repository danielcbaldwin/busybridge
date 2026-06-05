"""Legacy sync engine — drained out at the Stage-5 cutover.

What remains under ``app/sync/``:

* ``google_calendar`` — the Google Calendar API adapter used by
  the backup / ICS-export subsystem.  Kept; not used by the new
  ledger pipeline (the ledger has its own
  ``app/ledger/real_google_client.py``).
* ``backup`` — SQLite dump + restore.  Kept.
* ``ics_export`` — ICS file export.  Kept.

The orchestrator (``engine.py``), per-event rules (``rules.py``),
consistency checker (``consistency.py``), webcal sync
(``webcal_sync.py``) and the unused ICS parser (``ics_parser.py``)
were removed at the cutover.  Their behaviours live in
``app/ledger/`` now.
"""

__all__: list[str] = []
