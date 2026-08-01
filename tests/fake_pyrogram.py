"""Offline stand-in for the parts of pyrogram that parser.telegram imports.

Every test module must share THESE classes: parser.telegram builds
FATAL_TG_ERRORS out of whatever pyrogram.errors holds at import time, so a
second, look-alike set of fakes would silently stop being caught as fatal.
Installed by conftest.py before any test module is imported.
"""

import os
import sys
from types import SimpleNamespace


class FakeUnauthorized(Exception):
    """Stands in for pyrogram.errors.Unauthorized (dead session)."""


class FakeAuthKeyDuplicated(Exception):
    """Stands in for pyrogram.errors.AuthKeyDuplicated."""


class FakeFloodWait(Exception):
    """Stands in for pyrogram.errors.FloodWait, which carries `value` seconds."""

    def __init__(self, value: int = 0):
        super().__init__(f"A wait of {value} seconds is required")
        self.value = value


errors = SimpleNamespace(
    Unauthorized=FakeUnauthorized,
    AuthKeyDuplicated=FakeAuthKeyDuplicated,
    FloodWait=FakeFloodWait,
)

# Only the members parser.telegram compares against; the real enum members are
# objects, so tests must use these very values for `chat.type`.
enums = SimpleNamespace(
    ChatType=SimpleNamespace(
        CHANNEL="ChatType.CHANNEL",
        SUPERGROUP="ChatType.SUPERGROUP",
        GROUP="ChatType.GROUP",
        PRIVATE="ChatType.PRIVATE",
    )
)


def install():
    sys.modules.setdefault(
        "pyrogram",
        SimpleNamespace(Client=object, errors=errors, enums=enums),
    )
    sys.modules.setdefault("pyrogram.errors", errors)
    sys.modules.setdefault("pyrogram.enums", enums)
    os.environ.setdefault("API_ID", "12345")
    os.environ.setdefault("API_HASH", "hash123")
