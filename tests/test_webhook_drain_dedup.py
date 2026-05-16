"""A webhook flood must not stack unbounded reconcile tasks.

Every Google Calendar webhook POST schedules a background
``_delayed_drain`` coroutine (sleep 5s, then reconcile).  ``enqueue_webhook``
already debounces the reconcile *request*, so a burst of webhooks for
one user should collapse into a single delayed drain — otherwise each
POST spawns its own sleep-then-reconcile task and a flood stacks them
without bound.  ``_claim_webhook_drain`` enforces one in-flight drain
per user.
"""

from __future__ import annotations

from app.api.webhooks import _claim_webhook_drain, _release_webhook_drain


def test_claim_webhook_drain_dedupes_per_user():
    uid = 9001
    try:
        # First webhook for the user → caller should spawn the drain.
        assert _claim_webhook_drain(uid) is True
        # Flood: every subsequent webhook folds into the pending drain.
        assert _claim_webhook_drain(uid) is False
        assert _claim_webhook_drain(uid) is False
        # The delayed drain finishes and releases the user.
        _release_webhook_drain(uid)
        # The next webhook again schedules a fresh drain.
        assert _claim_webhook_drain(uid) is True
    finally:
        _release_webhook_drain(uid)


def test_claim_webhook_drain_is_independent_per_user():
    a, b = 9002, 9003
    try:
        assert _claim_webhook_drain(a) is True
        # A different user is unaffected by user a's pending drain.
        assert _claim_webhook_drain(b) is True
        assert _claim_webhook_drain(a) is False
        assert _claim_webhook_drain(b) is False
    finally:
        _release_webhook_drain(a)
        _release_webhook_drain(b)


def test_release_of_an_unknown_user_is_a_noop():
    # Releasing a user that was never claimed must not raise.
    _release_webhook_drain(987654)
