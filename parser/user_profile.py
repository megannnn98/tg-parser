from __future__ import annotations

from dataclasses import dataclass
import logging
from pathlib import Path
import sqlite3


_logger = logging.getLogger(__name__)

_CHART_COLORS = (
    "#2563eb",
    "#dc2626",
    "#16a34a",
    "#ca8a04",
    "#9333ea",
    "#0891b2",
    "#ea580c",
    "#4f46e5",
    "#be123c",
    "#15803d",
)


@dataclass(frozen=True)
class ChannelProfile:
    name: str
    message_count: int
    percent: float
    color: str
    dasharray: str
    dashoffset: float


@dataclass(frozen=True)
class UserProfile:
    db_name: str
    tg_id: int
    username: str | None
    display_name: str | None
    total_messages: int
    channel_count: int
    channels: list[ChannelProfile]

    @property
    def display_username(self) -> str:
        if self.username:
            return f"@{self.username}"
        if self.display_name:
            return self.display_name
        return "нет ника"


class UserProfileError(ValueError):
    pass


def list_user_profiles(data_dir: Path) -> list[UserProfile]:
    if not data_dir.exists():
        return []

    profiles: list[UserProfile] = []
    for db_path in sorted(data_dir.glob("*.db")):
        try:
            profiles.append(load_user_profile(db_path))
        except UserProfileError:
            continue
        except sqlite3.Error as exc:
            _logger.warning("Skipped unreadable user database %s: %s", db_path, exc)
            continue

    return profiles


def load_user_profile(db_path: Path) -> UserProfile:
    if not db_path.exists():
        raise UserProfileError(f"database does not exist: {db_path}")

    with _connect_readonly(db_path) as db:
        db.row_factory = sqlite3.Row
        if not _has_user_messages(db):
            raise UserProfileError(f"not a user comments database: {db_path}")

        user = _fetch_primary_user(db)
        channels = _fetch_channels(db, user["tg_id"], user["total_messages"])

    return UserProfile(
        db_name=db_path.name,
        tg_id=user["tg_id"],
        username=user["username"],
        display_name=(
            None
            if user["username"]
            else _display_name_from_db_name(db_path, user["tg_id"])
        ),
        total_messages=user["total_messages"],
        channel_count=len(channels),
        channels=channels,
    )


def _connect_readonly(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path.resolve()}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def _has_user_messages(db: sqlite3.Connection) -> bool:
    row = db.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table' AND name = 'user_messages'
        """
    ).fetchone()
    return row is not None


def _fetch_primary_user(db: sqlite3.Connection) -> sqlite3.Row:
    row = db.execute(
        """
        SELECT
            tg_id,
            (
                SELECT username
                FROM user_messages AS latest
                WHERE latest.tg_id = user_messages.tg_id
                  AND latest.username IS NOT NULL
                  AND latest.username != ''
                ORDER BY latest.id DESC
                LIMIT 1
            ) AS username,
            COUNT(*) AS total_messages
        FROM user_messages
        GROUP BY tg_id
        ORDER BY total_messages DESC, tg_id
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        raise UserProfileError("user comments database is empty")
    return row


def _display_name_from_db_name(db_path: Path, tg_id: int) -> str | None:
    stem = db_path.stem
    suffix = f"_{tg_id}"
    if not stem.endswith(suffix):
        return None

    raw_name = stem[: -len(suffix)]
    if not raw_name:
        return None

    return " ".join(part.capitalize() for part in raw_name.split("_") if part)


def _fetch_channels(
    db: sqlite3.Connection,
    tg_id: int,
    total_messages: int,
) -> list[ChannelProfile]:
    rows = db.execute(
        """
        SELECT channel, COUNT(*) AS message_count
        FROM user_messages
        WHERE tg_id = ?
        GROUP BY channel
        ORDER BY message_count DESC, channel
        """,
        (tg_id,),
    ).fetchall()

    offset = 0.0
    channels: list[ChannelProfile] = []
    for index, row in enumerate(rows):
        percent = round(row["message_count"] * 100 / total_messages, 1)
        if index == len(rows) - 1:
            percent = round(100 - offset, 1)
        channels.append(
            ChannelProfile(
                name=row["channel"],
                message_count=row["message_count"],
                percent=percent,
                color=_CHART_COLORS[index % len(_CHART_COLORS)],
                dasharray=f"{percent} {round(100 - percent, 1)}",
                dashoffset=-round(offset, 1),
            )
        )
        offset += percent

    return channels
