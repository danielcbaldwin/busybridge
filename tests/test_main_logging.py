"""Import-time file logging must be best-effort.

app.main installs a rotating file handler at import time.  In
production /data is a writable volume, but dev / CI / sandbox
environments often are not — and an exception at import time would
make the whole application un-importable.  ``_install_file_logging``
must degrade to stdout-only instead of raising.
"""

from __future__ import annotations

import logging

from app.main import _install_file_logging


def test_file_logging_tolerates_an_unwritable_directory(tmp_path):
    # A path *under a regular file* can never be created as a
    # directory — os.makedirs raises NotADirectoryError (an OSError)
    # regardless of process privileges.
    blocker = tmp_path / "not_a_directory"
    blocker.write_text("x")

    installed = _install_file_logging(str(blocker / "logs"))

    assert installed is False  # declined gracefully, did not raise


def test_file_logging_installs_a_handler_on_a_writable_directory(tmp_path):
    log_dir = tmp_path / "logs"
    root = logging.getLogger()
    before = list(root.handlers)
    try:
        installed = _install_file_logging(str(log_dir))
        assert installed is True
        assert (log_dir / "busybridge.log").exists()
        assert len(root.handlers) == len(before) + 1
    finally:
        # Don't leak the file handler into other tests.
        for handler in root.handlers[len(before):]:
            handler.close()
            root.removeHandler(handler)
