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

import asyncio
import logging
import time
from contextlib import contextmanager

logger = logging.getLogger(__name__)

_maintenance_active = False
_active_reconciles = 0


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


@contextmanager
def track_reconcile():
    """Mark a reconcile pass as in-flight for quiescence tracking.

    Entering ``maintenance`` mode stops *new* passes, but a pass that
    was already running must finish before a DB restore swaps the
    database file.  ``reconcile_user_by_id`` enters this guard
    synchronously, immediately after clearing the maintenance gate and
    before its first ``await`` — so a restore either sees the
    maintenance flag (and the pass never starts) or sees a non-zero
    in-flight count (and waits for it).  There is no window between.
    """
    global _active_reconciles
    _active_reconciles += 1
    try:
        yield
    finally:
        _active_reconciles -= 1


def active_reconcile_count() -> int:
    """Number of reconcile passes currently in flight."""
    return _active_reconciles


async def wait_for_reconcile_quiescence(
    timeout: float = 60.0, poll: float = 0.05,
) -> None:
    """Block until no reconcile pass is in flight.

    :func:`enter_maintenance` must already be set so no *new* pass can
    start; this drains the passes already running before the caller
    (a DB restore) touches the database file.  Raises
    :class:`TimeoutError` if a pass does not finish within ``timeout``
    — the restore must then abort rather than swap the DB out from
    under a live reconcile.
    """
    deadline = time.monotonic() + timeout
    while _active_reconciles > 0:
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"{_active_reconciles} reconcile pass(es) still running "
                f"after {timeout}s — cannot safely enter maintenance"
            )
        await asyncio.sleep(poll)

