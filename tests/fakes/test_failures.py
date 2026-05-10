"""Tests for ``tests.fakes.failures.FailureInjector`` and its
integration with :class:`FakeGoogleCalendar`."""

from __future__ import annotations

import pytest

from tests.fakes.clock import SimulatedClock
from tests.fakes.failures import FailureInjector, NetworkError
from tests.fakes.google_calendar import (
    FakeGoogleCalendar,
    GoogleApiError,
)


# ---------------------------------------------------------------------------
# Direct injector API
# ---------------------------------------------------------------------------
def test_no_op_when_all_rates_zero():
    inj = FailureInjector()
    for _ in range(1000):
        inj.maybe_fail("insert")
        inj.maybe_crash_after_write("insert")
    assert inj.total_failures == 0


def test_invalid_rate_rejected():
    with pytest.raises(ValueError):
        FailureInjector(network_error_rate=1.5)
    with pytest.raises(ValueError):
        FailureInjector(rate_limit_rate=-0.1)


def test_unknown_operation_raises():
    inj = FailureInjector()
    with pytest.raises(ValueError):
        inj.maybe_fail("teleport")


def test_force_next_overrides_rates():
    inj = FailureInjector()
    inj.force_next(GoogleApiError(409, "Conflict", "boom"))
    with pytest.raises(GoogleApiError) as exc_info:
        inj.maybe_fail("insert")
    assert exc_info.value.status == 409
    # Cleared after one use.
    inj.maybe_fail("insert")  # no raise


def test_force_next_crash_after_write_fires_once():
    inj = FailureInjector()
    inj.force_next_crash_after_write()
    with pytest.raises(NetworkError):
        inj.maybe_crash_after_write("insert")
    inj.maybe_crash_after_write("insert")  # cleared


def test_crash_after_write_only_for_writes():
    inj = FailureInjector(mid_write_crash_rate=1.0)
    # Reads shouldn't crash mid-write — there is no write.
    inj.maybe_crash_after_write("get")
    inj.maybe_crash_after_write("list")
    assert inj.mid_write_crash_count == 0


def test_sync_token_expiry_only_when_token_present():
    inj = FailureInjector(sync_token_expiry_rate=1.0)
    # Without a sync token, no 410.
    for _ in range(20):
        inj.maybe_fail("list", has_sync_token=False)
    assert inj.sync_token_expiry_count == 0
    # With a sync token, expires every time.
    with pytest.raises(GoogleApiError) as exc_info:
        inj.maybe_fail("list", has_sync_token=True)
    assert exc_info.value.status == 410


def test_deterministic_with_seed():
    a = FailureInjector(seed=7, network_error_rate=0.5)
    b = FailureInjector(seed=7, network_error_rate=0.5)
    a_results = []
    b_results = []
    for _ in range(50):
        try:
            a.maybe_fail("get")
            a_results.append("ok")
        except NetworkError:
            a_results.append("net")
        try:
            b.maybe_fail("get")
            b_results.append("ok")
        except NetworkError:
            b_results.append("net")
    assert a_results == b_results


def test_reset_counters():
    inj = FailureInjector(network_error_rate=1.0)
    for _ in range(3):
        with pytest.raises(NetworkError):
            inj.maybe_fail("get")
    assert inj.network_error_count == 3
    inj.reset_counters()
    assert inj.total_failures == 0


# ---------------------------------------------------------------------------
# Integration with FakeGoogleCalendar
# ---------------------------------------------------------------------------
@pytest.fixture
def fake_with_injector():
    clock = SimulatedClock()
    inj = FailureInjector()
    g = FakeGoogleCalendar(clock=clock, failure_injector=inj)
    g.add_calendar("primary")
    return g, inj


def _basic_body() -> dict:
    return {
        "summary": "test",
        "start": {"dateTime": "2026-02-01T09:00:00Z", "timeZone": "UTC"},
        "end": {"dateTime": "2026-02-01T09:30:00Z", "timeZone": "UTC"},
    }


def test_forced_failure_on_insert(fake_with_injector):
    fake, inj = fake_with_injector
    inj.force_next(GoogleApiError(503, "Service Unavailable", "transient"))
    with pytest.raises(GoogleApiError) as exc_info:
        fake.insert_event("primary", _basic_body())
    assert exc_info.value.status == 503


def test_mid_write_crash_persists_state_but_raises(fake_with_injector):
    """The headline subtle behaviour: after the 'crash' the change
    IS visible to the next call, even though the caller saw an
    error.  This is what idempotent retry must handle."""
    fake, inj = fake_with_injector
    body = _basic_body()
    body["id"] = "bbcrash00000001"
    inj.force_next_crash_after_write()
    with pytest.raises(NetworkError):
        fake.insert_event("primary", body)
    # Despite the raise, the event is now in the store.
    assert "bbcrash00000001" in fake.all_event_ids("primary")


def test_mid_write_crash_on_update(fake_with_injector):
    fake, inj = fake_with_injector
    body = _basic_body()
    body["id"] = "bbupdcrash00001"
    fake.insert_event("primary", body)

    inj.force_next_crash_after_write()
    with pytest.raises(NetworkError):
        fake.update_event(
            "primary", "bbupdcrash00001",
            {**_basic_body(), "summary": "after"},
        )
    refetched = fake.get_event("primary", "bbupdcrash00001")
    assert refetched["summary"] == "after"


def test_mid_write_crash_on_delete(fake_with_injector):
    fake, inj = fake_with_injector
    body = _basic_body()
    body["id"] = "bbdelcrash00001"
    fake.insert_event("primary", body)

    inj.force_next_crash_after_write()
    with pytest.raises(NetworkError):
        fake.delete_event("primary", "bbdelcrash00001")
    refetched = fake.get_event("primary", "bbdelcrash00001")
    assert refetched["status"] == "cancelled"


def test_rate_limit_via_probability(fake_with_injector):
    fake, inj = fake_with_injector
    # Boost probability to 1 to make the test deterministic.
    inj.rate_limit_rate = 1.0
    with pytest.raises(GoogleApiError) as exc_info:
        fake.insert_event("primary", _basic_body())
    assert exc_info.value.status == 429
    assert "rateLimitExceeded" in exc_info.value.message


def test_server_error_via_probability(fake_with_injector):
    fake, inj = fake_with_injector
    inj.server_error_rate = 1.0
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_events("primary")
    assert exc_info.value.status == 503


def test_network_error_via_probability(fake_with_injector):
    fake, inj = fake_with_injector
    inj.network_error_rate = 1.0
    with pytest.raises(NetworkError):
        fake.list_events("primary")


def test_injected_sync_token_expiry(fake_with_injector):
    """Even though the token's TTL has not elapsed, the injector
    can simulate the random expiry that real Google sometimes does."""
    fake, inj = fake_with_injector
    sync = fake.list_events("primary")["nextSyncToken"]
    inj.sync_token_expiry_rate = 1.0
    with pytest.raises(GoogleApiError) as exc_info:
        fake.list_events("primary", sync_token=sync)
    assert exc_info.value.status == 410


def test_injection_fires_on_pagination_continuation(fake_with_injector):
    """Pagination continuation is a separate HTTP request to Google
    in real life, so it is subject to failure injection just like
    the initial call.  Production code retrying a failed continuation
    can resume from the same page token."""
    fake, inj = fake_with_injector
    for i in range(5):
        body = _basic_body()
        body["start"]["dateTime"] = f"2026-02-{i+1:02d}T09:00:00Z"
        body["end"]["dateTime"] = f"2026-02-{i+1:02d}T09:30:00Z"
        fake.insert_event("primary", body)
    page1 = fake.list_events("primary", max_results=2)
    inj.network_error_rate = 1.0
    with pytest.raises(NetworkError):
        fake.list_events("primary", page_token=page1["nextPageToken"])


def test_no_injector_means_no_failures():
    """Backwards-compat: a fake constructed without an injector
    just works."""
    g = FakeGoogleCalendar()
    g.add_calendar("primary")
    g.insert_event("primary", _basic_body())
    g.list_events("primary")


def test_counters_track_each_failure_kind(fake_with_injector):
    fake, inj = fake_with_injector
    inj.force_next(GoogleApiError(429, "Too Many Requests", "rateLimitExceeded"))
    with pytest.raises(GoogleApiError):
        fake.insert_event("primary", _basic_body())
    assert inj.forced_failure_count == 1
    assert inj.total_failures == 1
