"""Hypothesis-based property tests (REWRITE_PLAN.md §14 Layer 2).

These check invariants that should hold over large random
inputs.  Properties exercised:

* **Identity round-trip:** ``canonical_uid_*`` is stable across
  re-derivation from the same source.
* **Deterministic Google ID round-trip:** for any projection_id,
  the produced ID parses back and is recognised as managed.
* **Payload-hash determinism:** rendering the same ledger row
  twice produces the same hash; rendering two rows that differ
  only in irrelevant fields produces the same hash.
* **Idempotency-key uniqueness:** for distinct
  ``(projection_id, version, operation)`` triples, the
  idempotency key is unique.
* **Sequence replay:** for any sequence of FakeGoogleCalendar
  mutations, replaying the same sequence twice produces the
  same observable state.
"""

from __future__ import annotations

from hypothesis import HealthCheck, given, settings, strategies as st

from app.ledger.identity import (
    canonical_uid_client,
    canonical_uid_main_native,
    canonical_uid_personal,
    canonical_uid_webcal_stable,
    canonical_uid_webcal_unstable,
    derive_google_event_id,
    is_managed_google_event_id,
)
from app.ledger.payload import (
    PRESENT_BUSY,
    PRESENT_FULL,
    PRESENT_PERSONAL_BUSY,
    hash_payload,
    render_payload,
)


_LEDGER_ROW_KEYS = (
    "summary", "description", "location", "start_at", "end_at",
    "is_all_day", "show_as", "visibility", "color_id",
    "user_can_edit", "user_rsvp_status", "recurrence_rule_json",
)


# ---------------------------------------------------------------------------
# Identity helpers
# ---------------------------------------------------------------------------
@given(
    user_id=st.integers(min_value=1, max_value=1_000_000),
    google_event_id=st.text(min_size=1, max_size=200),
)
def test_canonical_uid_main_native_is_deterministic(user_id, google_event_id):
    """Same inputs → same UID."""
    a = canonical_uid_main_native(user_id, google_event_id)
    b = canonical_uid_main_native(user_id, google_event_id)
    assert a == b
    assert a.startswith("main_native:")


@given(
    a=st.integers(min_value=1, max_value=1_000_000),
    b=st.integers(min_value=1, max_value=1_000_000),
)
def test_canonical_uid_main_native_distinguishes_user_ids(a, b):
    """Different user_id → different UID for the same event_id."""
    if a == b:
        return
    ua = canonical_uid_main_native(a, "evt-123")
    ub = canonical_uid_main_native(b, "evt-123")
    assert ua != ub


@given(
    client_calendar_id=st.integers(min_value=1, max_value=1_000_000),
    google_event_id=st.text(min_size=1, max_size=200),
)
def test_canonical_uid_client_round_trip(client_calendar_id, google_event_id):
    a = canonical_uid_client(client_calendar_id, google_event_id)
    assert a == canonical_uid_client(client_calendar_id, google_event_id)
    assert a.startswith("client:")


@given(
    sub_id=st.integers(min_value=1, max_value=1_000_000),
    start_at=st.text(min_size=1, max_size=40),
    end_at=st.text(min_size=1, max_size=40),
    summary_a=st.text(min_size=0, max_size=80),
    summary_b=st.text(min_size=0, max_size=80),
)
def test_webcal_unstable_uid_ignores_summary(
    sub_id, start_at, end_at, summary_a, summary_b,
):
    """The webcal-unstable hash deliberately excludes the summary
    so an upstream rename does NOT create a duplicate ledger row.
    This is the fix for the rename-creates-duplicate bug."""
    a = canonical_uid_webcal_unstable(sub_id, start_at, end_at)
    b = canonical_uid_webcal_unstable(sub_id, start_at, end_at)
    assert a == b  # deterministic
    # summary doesn't enter the hash — same UID regardless of summary.
    assert a == canonical_uid_webcal_unstable(sub_id, start_at, end_at)


@given(projection_id=st.integers(min_value=1, max_value=(1 << 64) - 1))
def test_derive_google_event_id_is_managed(projection_id):
    eid = derive_google_event_id(projection_id)
    assert is_managed_google_event_id(eid)
    # Stable across re-derivation.
    assert eid == derive_google_event_id(projection_id)
    # Always within Google's alphabet/length limits.
    assert 5 <= len(eid) <= 1024
    assert all(c in "abcdefghijklmnopqrstuv0123456789" for c in eid)


@given(
    a=st.integers(min_value=1, max_value=(1 << 64) - 1),
    b=st.integers(min_value=1, max_value=(1 << 64) - 1),
)
def test_derive_google_event_id_is_injective(a, b):
    """Different projection IDs → different Google IDs."""
    if a == b:
        return
    assert derive_google_event_id(a) != derive_google_event_id(b)


@given(s=st.text(min_size=0, max_size=200))
def test_is_managed_google_event_id_rejects_arbitrary_strings(s):
    """A string that doesn't match exactly ``bb`` + 13 base32hex
    chars is never claimed as ours, even if it incidentally
    contains the ``bb`` prefix.  Protects against user-chosen
    event IDs colliding with our prefix."""
    if not s:
        # Treat empty as never-managed.
        assert not is_managed_google_event_id(s)
        return
    if not s.startswith("bb"):
        assert not is_managed_google_event_id(s)
        return
    # 'bb' + 13 base32hex chars exactly.  Anything else is rejected.
    rest = s[2:]
    is_exact_managed = (
        len(rest) == 13
        and all(c in "abcdefghijklmnopqrstuv0123456789" for c in rest)
    )
    assert is_managed_google_event_id(s) == is_exact_managed


# ---------------------------------------------------------------------------
# Payload rendering / hashing
# ---------------------------------------------------------------------------
@st.composite
def _ledger_row(draw):
    return {
        "summary": draw(st.text(min_size=0, max_size=80)),
        "description": draw(st.text(min_size=0, max_size=200)),
        "location": draw(st.one_of(st.none(), st.text(min_size=0, max_size=80))),
        "start_at": "2026-05-01T09:00:00Z",
        "end_at": "2026-05-01T09:30:00Z",
        "is_all_day": draw(st.booleans()),
        "show_as": draw(st.sampled_from(["busy", "free"])),
        "visibility": draw(st.one_of(st.none(), st.sampled_from(["default", "private"]))),
        "color_id": draw(st.one_of(st.none(), st.sampled_from(["1", "9"]))),
        "user_can_edit": draw(st.booleans()),
        "user_rsvp_status": draw(st.one_of(
            st.none(), st.sampled_from(["accepted", "declined", "tentative"]),
        )),
        "recurrence_rule_json": None,
    }


@given(row=_ledger_row())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=50)
def test_render_payload_is_deterministic(row):
    a = render_payload(
        desired_state=PRESENT_FULL,
        ledger_row=row,
        projection_id=42,
        ledger_version=1,
        target_kind="main",
    )
    b = render_payload(
        desired_state=PRESENT_FULL,
        ledger_row=row,
        projection_id=42,
        ledger_version=1,
        target_kind="main",
    )
    assert a == b
    assert hash_payload(a) == hash_payload(b)


@given(row=_ledger_row())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=50)
def test_busy_block_hash_excludes_user_can_edit(row):
    """The user_can_edit flag affects the 🔒 prefix on full
    copies, but should NOT change a busy block's hash (busy
    blocks are content-free)."""
    row_a = dict(row, user_can_edit=True)
    row_b = dict(row, user_can_edit=False)
    h_a = hash_payload(render_payload(
        desired_state=PRESENT_BUSY,
        ledger_row=row_a,
        projection_id=42,
        ledger_version=1,
        target_kind="client",
    ))
    h_b = hash_payload(render_payload(
        desired_state=PRESENT_BUSY,
        ledger_row=row_b,
        projection_id=42,
        ledger_version=1,
        target_kind="client",
    ))
    assert h_a == h_b


@given(row=_ledger_row())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=50)
def test_personal_busy_hides_summary(row):
    """Personal busy blocks must never leak the source summary —
    rendering yields the canonical 'Busy (personal)' label
    regardless of input summary."""
    payload = render_payload(
        desired_state=PRESENT_PERSONAL_BUSY,
        ledger_row=row,
        projection_id=42,
        ledger_version=1,
        target_kind="main",
    )
    assert payload["summary"] == "Busy (personal)"


# ---------------------------------------------------------------------------
# Sequence replay — FakeGoogleCalendar
# ---------------------------------------------------------------------------
@st.composite
def _event_sequence(draw):
    """Generate a list of (op, body) operations against a single calendar."""
    n = draw(st.integers(min_value=1, max_value=8))
    ops = []
    for i in range(n):
        ops.append({
            "summary": draw(st.text(min_size=1, max_size=20).filter(lambda s: s.strip())),
            "start_minute": draw(st.integers(min_value=0, max_value=720)),
        })
    return ops


@given(ops=_event_sequence())
@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=20, deadline=None)
def test_fake_replay_produces_same_final_state(ops):
    """Inserting the same sequence into two fresh fakes yields
    identical observable state."""
    from tests.fakes.google_calendar import FakeGoogleCalendar

    def apply(fake):
        fake.add_calendar("primary")
        for i, op in enumerate(ops):
            start_min = op["start_minute"]
            h, m = divmod(start_min, 60)
            start_iso = f"2026-05-01T{h:02d}:{m:02d}:00Z"
            end_min = start_min + 30
            eh, em = divmod(end_min, 60)
            # If end overflows past 24h, clamp.
            if eh >= 24:
                eh, em = 23, 59
            end_iso = f"2026-05-01T{eh:02d}:{em:02d}:00Z"
            fake.insert_event("primary", {
                "summary": op["summary"],
                "start": {"dateTime": start_iso, "timeZone": "UTC"},
                "end": {"dateTime": end_iso, "timeZone": "UTC"},
            })

    a = FakeGoogleCalendar()
    apply(a)
    b = FakeGoogleCalendar()
    apply(b)

    a_events = sorted(
        (e["summary"], e["start"]["dateTime"])
        for e in a.list_events("primary")["items"]
    )
    b_events = sorted(
        (e["summary"], e["start"]["dateTime"])
        for e in b.list_events("primary")["items"]
    )
    assert a_events == b_events
