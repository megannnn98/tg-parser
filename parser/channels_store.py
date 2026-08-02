"""Parse and persist the user-editable list of channels (channels.json)."""
from __future__ import annotations

import json
import re
from pathlib import Path

# Telegram usernames: a letter first, then letters/digits/underscores, 5-32
# total. Mirrors parser.channel_discovery._USERNAME.
_USERNAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{4,31}$")


class InvalidChannelError(ValueError):
    pass


def parse_channels_text(text: str) -> list[str]:
    channels: list[str] = []
    seen: set[str] = set()

    for lineno, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip().lstrip("@")
        if not line:
            continue

        if not _USERNAME_RE.match(line):
            raise InvalidChannelError(
                f"Строка {lineno}: «{raw_line.strip()}» — недопустимое имя канала"
            )

        key = line.casefold()
        if key in seen:
            continue
        seen.add(key)
        channels.append(line)

    return channels


def save_channels(path: Path, channels: list[str]) -> None:
    # Write through a temp file: a crash must not leave a truncated list.
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(channels, ensure_ascii=False, indent=2) + "\n")
    tmp.replace(path)
