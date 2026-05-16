"""Process-wide maintenance mode.

When maintenance mode is on, the sync engine performs a *hard* freeze:
no ingest, diff, outbox drain, webhook handling, or scheduled work
runs.  It is the safe-stop used while the database is being restored
from a backup.

This is an in-process flag, deliberately NOT a row in the ``settings``
table: the database file itself is what gets swapped during a restore,
so the flag that guards that swap must live outside the database.

It is distinct from the user-facing pause switches:

* ``settings.sync_paused`` (global) — a persistent admin emergency
  stop, also a hard freeze, but DB-backed and survives a restart.
* ``users.sync_paused`` (per-user) — a soft pause: ingest is skipped
  but the outbox still drains so staged cleanup converges.

Maintenance mode is transient, lives only for the duration of a
restore, and is bypassed only by the restore's own re-converge pass.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_maintenance_active = False


def enter_maintenance() -> None:
    """Begin maintenance mode — the sync engine hard-stops."""
    global _maintenance_active
    _maintenance_active = True
    logger.warning("maintenance mode ON — sync engine frozen")


def exit_maintenance() -> None:
    """End maintenance mode — normal sync resumes."""
    global _maintenance_active
    _maintenance_active = False
    logger.warning("maintenance mode OFF — sync engine resumed")


def in_maintenance() -> bool:
    """True while a maintenance operation (e.g. DB restore) holds the
    sync engine frozen."""
    return _maintenance_active
