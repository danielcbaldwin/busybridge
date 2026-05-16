"""The OOBE wizard's progress indicator must be consistent.

The service-account step (old step 5) was removed at the Stage-5
cutover, leaving 6 real steps.  Several setup templates still rendered
a 7-dot progress indicator, so the bar's length jumped around as the
user advanced.  Each template's indicator must show exactly 6 dots,
with the filled count equal to that step's position.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_SETUP_DIR = Path(__file__).resolve().parents[1] / "app/ui/templates/setup"

# template filename → 1-based position in the 6-step wizard
_STEP_POSITION = {
    "step1_welcome.html": 1,
    "step2_credentials.html": 2,
    "step3_admin.html": 3,
    "step4_email.html": 4,
    "step6_encryption.html": 5,
    "step7_complete.html": 6,
}

_DOT = re.compile(r"h-2 w-8 rounded-full bg-(indigo-600|gray-300)")


@pytest.mark.parametrize("filename,position", sorted(_STEP_POSITION.items()))
def test_progress_indicator_has_six_dots_with_correct_fill(filename, position):
    html = (_SETUP_DIR / filename).read_text()
    dots = _DOT.findall(html)
    assert len(dots) == 6, (
        f"{filename}: progress bar has {len(dots)} dots, expected 6"
    )
    filled = dots.count("indigo-600")
    assert filled == position, (
        f"{filename}: {filled} dots filled, expected {position}"
    )


def test_step6_back_link_skips_the_removed_service_account_step():
    """Step 6's Back link must point to step 4 — step 5 was removed and
    only bounces forward, so a Back link to it would be a dead end."""
    html = (_SETUP_DIR / "step6_encryption.html").read_text()
    assert "/setup?step=4" in html
    assert "/setup?step=5" not in html
