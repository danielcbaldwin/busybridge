"""A modified-instance canonical UID is timezone-representation stable.

Google returns ``originalStartTime`` for the SAME occurrence with
different timezone offsets across reads — e.g. ``-04:00`` from a
New-York-local user, ``+02:00`` after the user travels to Europe. The
canonical UID embeds the original_start string; if it embedded the raw
string, the same occurrence would fingerprint into multiple distinct
UIDs and BB would DUPLICATE the ledger row.  Live regression: when the
account moved between EDT and CEST, both ledger rows fought over the
main copy and "moves on the source reverted on main" because the older
duplicate kept re-rendering the canonical copy.
"""

from __future__ import annotations

from app.ledger.identity import canonical_uid_for_instance


def test_same_occurrence_in_two_offsets_yields_same_uid():
    parent = "client:6:abcdef"
    edt = canonical_uid_for_instance(parent, "2026-05-25T13:00:00-04:00")
    cest = canonical_uid_for_instance(parent, "2026-05-25T19:00:00+02:00")
    utc = canonical_uid_for_instance(parent, "2026-05-25T17:00:00Z")
    assert edt == cest == utc, (
        "same occurrence in different timezone representations must yield "
        f"the same canonical UID; got EDT={edt!r} CEST={cest!r} UTC={utc!r}"
    )


def test_different_occurrences_yield_different_uids():
    """Sanity: don't over-normalise away a real different start."""
    parent = "client:6:abcdef"
    a = canonical_uid_for_instance(parent, "2026-05-25T13:00:00-04:00")
    b = canonical_uid_for_instance(parent, "2026-05-25T14:00:00-04:00")
    assert a != b


def test_all_day_uid_unchanged():
    """All-day occurrences carry only a date (no offset to normalise)."""
    parent = "client:6:abcdef"
    uid = canonical_uid_for_instance(parent, "2026-05-25")
    assert uid.endswith(":inst:2026-05-25")


def test_unparsable_falls_back_deterministically():
    """A malformed input keeps producing the same UID rather than
    crashing or generating a different value each call."""
    parent = "client:6:abcdef"
    a = canonical_uid_for_instance(parent, "not-a-date")
    b = canonical_uid_for_instance(parent, "not-a-date")
    assert a == b
