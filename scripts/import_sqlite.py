"""One-off import of the old SQLite files into PostgreSQL.

    python -m scripts.import_sqlite --data-dir data

Reads app.db (users, channels, messages) and the per-user *.db files
(user_messages). Safe to repeat: nothing already stored is duplicated or
overwritten. The SQLite files are opened read-only.
"""
from __future__ import annotations

import argparse
import asyncio
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from db import repositories as repo
from db.engine import create_engine, session_factory


@dataclass
class ImportStats:
    users_scanned: int = 0
    users_inserted: int = 0
    channels_scanned: int = 0
    channels_inserted: int = 0
    messages_scanned: int = 0
    messages_inserted: int = 0
    duplicates: int = 0
    invalid_rows: int = 0
    legacy_scanned: int = 0
    legacy_inserted: int = 0
    failed_files: list[str] = field(default_factory=list)

    def render(self) -> str:
        lines = [
            f"users scanned:     {self.users_scanned}",
            f"users inserted:    {self.users_inserted}",
            "",
            f"channels scanned:  {self.channels_scanned}",
            f"channels inserted: {self.channels_inserted}",
            "",
            f"messages scanned:  {self.messages_scanned}",
            f"messages inserted: {self.messages_inserted}",
            f"duplicates:        {self.duplicates}",
            f"invalid rows:      {self.invalid_rows}",
            "",
            f"legacy rows (no Telegram id) scanned:  {self.legacy_scanned}",
            f"legacy rows archived:                  {self.legacy_inserted}",
        ]
        if self.failed_files:
            lines += ["", "unreadable files: " + ", ".join(self.failed_files)]
        return "\n".join(lines)


@dataclass
class _Source:
    """What the SQLite files hold, read before anything is written."""

    usernames: dict[int, str | None] = field(default_factory=dict)
    first_names: dict[int, str | None] = field(default_factory=dict)
    channels: set[str] = field(default_factory=set)
    # One list per user file: each is written in a transaction of its own.
    messages: list[list[dict]] = field(default_factory=list)
    legacy: list[dict] = field(default_factory=list)
    # tg_id of each user file -> when the file was last written.
    collected_at: dict[int, datetime] = field(default_factory=dict)


def _tables(db: sqlite3.Connection) -> set[str]:
    return {
        name
        for (name,) in db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _name_from_filename(path: Path) -> tuple[int | None, str | None]:
    """<name>_<tg_id>.db or <tg_id>.db: the id, and the name the file was given."""
    slug, _, id_part = path.stem.rpartition("_")
    if not id_part.isascii() or not id_part.isdecimal():
        return None, None
    name = " ".join(part.capitalize() for part in slug.split("_") if part)
    return int(id_part), name or None


def _parse_date(raw: object, tz: ZoneInfo) -> datetime | None:
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=tz)


def _read_app_db(db: sqlite3.Connection, name: str, source: _Source) -> None:
    tables = _tables(db)
    if "users" in tables:
        for tg_id, username in db.execute("SELECT tg_id, username FROM users"):
            if isinstance(tg_id, int):
                source.usernames.setdefault(tg_id, username or None)
    if "channels" in tables:
        for (channel,) in db.execute("SELECT name FROM channels"):
            if isinstance(channel, str) and channel:
                source.channels.add(channel)
    if "messages" in tables:
        for row_id, user, channel, text, date in db.execute(
            "SELECT id, user, channel, text, date FROM messages"
        ):
            source.legacy.append(
                {
                    "source": name,
                    "source_row_id": row_id,
                    "user_tg_id": user if isinstance(user, int) else None,
                    "channel": channel if isinstance(channel, str) else None,
                    "text": text if isinstance(text, str) else None,
                    "date_raw": date if isinstance(date, str) else None,
                }
            )


def _read_user_db(
    db: sqlite3.Connection, path: Path, tz: ZoneInfo, source: _Source, stats: ImportStats
) -> None:
    file_tg_id, file_name = _name_from_filename(path)
    if file_tg_id is not None:
        # An empty file still stands for a collected user.
        source.usernames.setdefault(file_tg_id, None)
        source.collected_at[file_tg_id] = datetime.fromtimestamp(
            path.stat().st_mtime, timezone.utc
        )

    rows: list[dict] = []
    latest_username: dict[int, str] = {}
    for tg_id, username, channel, message_id, text, raw_date in db.execute(
        "SELECT tg_id, username, channel, message_id, text, date"
        " FROM user_messages ORDER BY id"
    ):
        stats.messages_scanned += 1
        date = _parse_date(raw_date, tz)
        if (
            not isinstance(tg_id, int)
            or not isinstance(message_id, int)
            or not isinstance(channel, str)
            or not channel
            or not isinstance(text, str)
            or date is None
        ):
            stats.invalid_rows += 1
            continue

        if isinstance(username, str) and username:
            latest_username[tg_id] = username
        source.usernames.setdefault(tg_id, None)
        source.channels.add(channel)
        rows.append(
            {
                "tg_id": tg_id,
                "channel": channel,
                "tg_message_id": message_id,
                "text": text,
                "date": date,
            }
        )

    # The user's own file was filled right after resolving them in Telegram.
    source.usernames.update(latest_username)
    if file_tg_id is not None and not source.usernames.get(file_tg_id):
        source.first_names[file_tg_id] = file_name
    source.messages.append(rows)


def _read(data_dir: Path, tz: ZoneInfo, stats: ImportStats) -> _Source:
    source = _Source()
    for path in sorted(data_dir.glob("*.db")):
        try:
            db = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
            try:
                if "user_messages" in _tables(db):
                    _read_user_db(db, path, tz, source, stats)
                else:
                    _read_app_db(db, path.name, source)
            finally:
                db.close()
        except sqlite3.Error as exc:
            print(f"Cannot read {path.name}: {exc}", file=sys.stderr)
            stats.failed_files.append(path.name)
    return source


async def import_sqlite(data_dir: Path, sessions, source_timezone: str) -> ImportStats:
    """Naive SQLite dates are taken as wall-clock time in `source_timezone`."""
    stats = ImportStats()
    source = _read(data_dir, ZoneInfo(source_timezone), stats)

    stats.users_scanned = len(source.usernames)
    stats.channels_scanned = len(source.channels)
    stats.legacy_scanned = len(source.legacy)

    async with sessions.begin() as session:
        user_ids, stats.users_inserted = await repo.insert_users_if_absent(
            session,
            {
                tg_id: (username, source.first_names.get(tg_id))
                for tg_id, username in source.usernames.items()
            },
        )
        channel_ids, stats.channels_inserted = await repo.ensure_channels(
            session, source.channels
        )
        # A later user-comments run knows better: its time is kept.
        await repo.mark_profiles_collected(
            session,
            {user_ids[tg_id]: when for tg_id, when in source.collected_at.items()},
            overwrite=False,
        )

    for rows in source.messages:
        # Within one file too: a repeated (channel, message id) is a duplicate.
        unique = {
            (row["channel"], row["tg_message_id"]): {
                "tg_message_id": row["tg_message_id"],
                "user_id": user_ids[row["tg_id"]],
                "channel_id": channel_ids[row["channel"]],
                "text": row["text"],
                "date": row["date"],
            }
            for row in reversed(rows)
        }
        async with sessions.begin() as session:
            inserted = await repo.insert_messages(session, list(unique.values()))
        stats.messages_inserted += inserted
        stats.duplicates += len(rows) - inserted

    async with sessions.begin() as session:
        stats.legacy_inserted = await repo.insert_legacy_messages(session, source.legacy)

    return stats


async def _main(args: argparse.Namespace) -> int:
    engine = create_engine()
    try:
        stats = await import_sqlite(
            Path(args.data_dir), session_factory(engine), args.source_timezone
        )
    finally:
        await engine.dispose()
    print(stats.render())
    return 1 if stats.failed_files else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--data-dir", default="data")
    parser.add_argument(
        "--source-timezone",
        default="UTC",
        help="time zone of the naive dates in the SQLite files (default: UTC, "
        "which is what the Docker image wrote)",
    )
    sys.exit(asyncio.run(_main(parser.parse_args())))
