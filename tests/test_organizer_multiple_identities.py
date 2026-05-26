"""``user_can_edit`` and ``user_rsvp_status`` resolve across a user's
identities, not just their home email.

A user owns multiple Google accounts — a home (workingpaper), plus one
OAuth-connected account per client / personal calendar (mlcommons, fpf,
orangechair, etc.).  An event you organise under a non-home identity
must still read as 'editable' to BB (no 🔒 prefix on the main copy),
and your self-attendee under any owned email must be matched for RSVP
extraction.  Live regression: 'Psychosocial Sync' (organised by
ag@mlcommons.org, mirrored to ag@workingpaper.co's main) shipped with
a lock icon because BB compared only against the home email.
"""

from __future__ import annotations

from app.ledger.ingest.client import _extract_event_fields, _user_is_organizer


HOME = "alice@home.example"
WORK = "alice@work.example"
OWNED = frozenset({HOME, WORK})


def _ev(*, organizer: str, attendees=None, **extra) -> dict:
    body: dict = {
        "id": "x", "summary": "S",
        "start": {"dateTime": "2026-05-28T09:00:00Z"},
        "end": {"dateTime": "2026-05-28T10:00:00Z"},
        "organizer": {"email": organizer},
        "attendees": attendees or [],
    }
    body.update(extra)
    return body


def test_organizer_under_owned_identity_is_editable():
    """Event organised under the WORK identity, but BB ingests under
    HOME — the user IS the organizer and the event must read as
    editable."""
    ev = _ev(organizer=WORK, attendees=[
        {"email": WORK, "responseStatus": "accepted"},
        {"email": "other@example.com", "responseStatus": "needsAction"},
    ])
    fields = _extract_event_fields(ev, user_email=HOME, owned_emails=OWNED)
    assert fields["user_can_edit"] is True
    # And the user's RSVP is read from the work-identity self-attendee.
    assert fields["user_rsvp_status"] == "accepted"


def test_organizer_under_unrelated_email_is_not_editable():
    """Sanity: a real third-party organizer leaves the event locked."""
    ev = _ev(organizer="boss@elsewhere.com", attendees=[
        {"email": WORK, "responseStatus": "needsAction"},
        {"email": "boss@elsewhere.com", "responseStatus": "accepted"},
    ])
    fields = _extract_event_fields(ev, user_email=HOME, owned_emails=OWNED)
    assert fields["user_can_edit"] is False


def test_guests_can_modify_still_unlocks_event():
    """``guestsCanModify=True`` makes the event editable regardless of
    who the organizer is — independent of the owned-emails check."""
    ev = _ev(
        organizer="boss@elsewhere.com",
        guestsCanModify=True,
        attendees=[{"email": HOME, "responseStatus": "needsAction"}],
    )
    fields = _extract_event_fields(ev, user_email=HOME, owned_emails=OWNED)
    assert fields["user_can_edit"] is True


def test_solo_event_is_editable():
    """No attendees → solo event you own → editable (was already true,
    making sure the refactor didn't break it)."""
    ev = _ev(organizer=HOME, attendees=[])
    fields = _extract_event_fields(ev, user_email=HOME, owned_emails=OWNED)
    assert fields["user_can_edit"] is True


def test_legacy_single_email_path_still_works():
    """Calling without ``owned_emails`` falls back to ``{user_email}``
    — back-compat for any code paths that haven't been updated yet."""
    ev = _ev(organizer=HOME, attendees=[
        {"email": HOME, "self": True, "responseStatus": "accepted"},
    ])
    fields = _extract_event_fields(ev, user_email=HOME)
    assert fields["user_can_edit"] is True
    assert fields["user_rsvp_status"] == "accepted"


def test_user_is_organizer_helper_accepts_owned_set():
    """Direct unit check on the organizer helper."""
    ev = _ev(organizer=WORK)
    assert _user_is_organizer(ev, owned_emails=OWNED) is True
    assert _user_is_organizer(ev, owned_emails={HOME}) is False
    # Back-compat positional/single-email form
    assert _user_is_organizer(ev, WORK) is True


def test_rsvp_matched_by_self_flag_or_owned_email():
    """RSVP extraction matches either the ``self`` flag (Google's own
    label) OR an email in the owned set."""
    # self flag wins
    ev1 = _ev(organizer="boss@elsewhere.com", attendees=[
        {"email": "boss@elsewhere.com", "responseStatus": "accepted"},
        {"email": WORK, "self": True, "responseStatus": "tentative"},
    ])
    fields1 = _extract_event_fields(ev1, user_email=HOME, owned_emails=OWNED)
    assert fields1["user_rsvp_status"] == "tentative"
    # owned email matched without self flag (cross-account ingest)
    ev2 = _ev(organizer="boss@elsewhere.com", attendees=[
        {"email": WORK, "responseStatus": "declined"},
    ])
    fields2 = _extract_event_fields(ev2, user_email=HOME, owned_emails=OWNED)
    assert fields2["user_rsvp_status"] == "declined"
