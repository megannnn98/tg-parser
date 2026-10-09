import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import select

from db.models import Channel, LegacyMessage, Message, User
from scripts.import_sqlite import import_sqlite


def _app_db(path: Path, users=(), channels=(), messages=()):
    db = sqlite3.connect(path)
    db.executescript(
        """
        CREATE TABLE users (tg_id INTEGER PRIMARY KEY NOT NULL, username TEXT);
        CREATE TABLE channels (name TEXT PRIMARY KEY NOT NULL);
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user INTEGER, channel TEXT, text TEXT, date TEXT
        );
        """
    )
    db.executemany("INSERT INTO users VALUES (?, ?)", users)
    db.executemany("INSERT INTO channels VALUES (?)", [(c,) for c in channels])
    db.executemany(
        "INSERT INTO messages (user, channel, text, date) VALUES (?, ?, ?, ?)", messages
    )
    db.commit()
    db.close()


def _user_db(path: Path, rows=()):
    db = sqlite3.connect(path)
    # No constraints: real files are well-formed, the importer must not rely on it.
    db.execute(
        """
        CREATE TABLE user_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id, username, channel, message_id, text, date
        )
        """
    )
    db.executemany(
        "INSERT INTO user_messages (tg_id, username, channel, message_id, text, date)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        rows,
    )
    db.commit()
    db.close()


async def _snapshot(sessions):
    async with sessions() as session:
        users = (
            await session.execute(
                select(User.tg_id, User.username, User.first_name).order_by(User.tg_id)
            )
        ).all()
        channels = (
            await session.scalars(select(Channel.username).order_by(Channel.username))
        ).all()
        messages = (
            await session.execute(
                select(
                    User.tg_id,
                    Channel.username,
                    Message.tg_message_id,
                    Message.text,
                    Message.date,
                )
                .join(User, User.id == Message.user_id)
                .join(Channel, Channel.id == Message.channel_id)
                .order_by(Channel.username, Message.tg_message_id)
            )
        ).all()
        legacy = (
            await session.execute(
                select(
                    LegacyMessage.source_row_id,
                    LegacyMessage.user_tg_id,
                    LegacyMessage.channel,
                    LegacyMessage.text,
                    LegacyMessage.date_raw,
                ).order_by(LegacyMessage.source_row_id)
            )
        ).all()
    return [tuple(r) for r in users], channels, [tuple(r) for r in messages], [
        tuple(r) for r in legacy
    ]


def _fixture(tmp_path: Path) -> Path:
    _app_db(
        tmp_path / "app.db",
        users=[(1, "alice_old"), (2, "bob")],
        channels=["news", "empty"],
        # The same comment as in alice's file, but with no Telegram id.
        messages=[(1, "news", "hello", "2026-01-05 10:00:00")],
    )
    _user_db(
        tmp_path / "alice_1.db",
        [
            (1, "alice", "news", 10, "hello", "2026-01-05 10:00:00"),
            (1, "alice", "tech", 10, "same id, other channel", "2026-01-06 11:30:00"),
        ],
    )
    return tmp_path


def test_imports_users_channels_and_messages(run_db, tmp_path):
    async def scenario(sessions):
        stats = await import_sqlite(_fixture(tmp_path), sessions, "UTC")
        return stats, await _snapshot(sessions)

    stats, (users, channels, messages, legacy) = run_db(scenario)

    # The user's own file knows the username better than the shared database.
    assert users == [(1, "alice", None), (2, "bob", None)]
    assert channels == ["empty", "news", "tech"]
    assert messages == [
        (1, "news", 10, "hello", datetime(2026, 1, 5, 10, 0, tzinfo=timezone.utc)),
        (
            1,
            "tech",
            10,
            "same id, other channel",
            datetime(2026, 1, 6, 11, 30, tzinfo=timezone.utc),
        ),
    ]
    # Rows without a Telegram id are archived, never turned into messages.
    assert legacy == [(1, 1, "news", "hello", "2026-01-05 10:00:00")]
    assert (stats.users_scanned, stats.users_inserted) == (2, 2)
    assert (stats.channels_scanned, stats.channels_inserted) == (3, 3)
    assert (stats.messages_scanned, stats.messages_inserted) == (2, 2)
    assert (stats.duplicates, stats.invalid_rows) == (0, 0)
    assert (stats.legacy_scanned, stats.legacy_inserted) == (1, 1)


def test_second_run_adds_nothing(run_db, tmp_path):
    async def scenario(sessions):
        data_dir = _fixture(tmp_path)
        await import_sqlite(data_dir, sessions, "UTC")
        before = await _snapshot(sessions)
        stats = await import_sqlite(data_dir, sessions, "UTC")
        return stats, before, await _snapshot(sessions)

    stats, before, after = run_db(scenario)

    assert after == before
    assert (stats.users_inserted, stats.channels_inserted) == (0, 0)
    assert (stats.messages_scanned, stats.messages_inserted, stats.duplicates) == (
        2,
        0,
        2,
    )
    assert (stats.legacy_scanned, stats.legacy_inserted) == (1, 0)


def test_naive_dates_are_read_in_the_source_timezone(run_db, tmp_path):
    _user_db(tmp_path / "alice_1.db", [(1, "alice", "news", 1, "x", "2026-01-05 02:30:00")])

    async def scenario(sessions):
        await import_sqlite(tmp_path, sessions, "Asia/Almaty")
        return (await _snapshot(sessions))[2]

    ((_, _, _, _, date),) = run_db(scenario)

    assert date == datetime(2026, 1, 4, 21, 30, tzinfo=timezone.utc)


def test_invalid_rows_are_counted_and_skipped(run_db, tmp_path):
    _user_db(
        tmp_path / "alice_1.db",
        [
            (1, "alice", "news", 1, "good", "2026-01-05 10:00:00"),
            (1, "alice", "news", None, "no message id", "2026-01-05 10:00:00"),
            (1, "alice", "news", 3, "bad date", "yesterday"),
            (1, "alice", None, 4, "no channel", "2026-01-05 10:00:00"),
            (1, "alice", "news", 5, None, "2026-01-05 10:00:00"),
            (None, "alice", "news", 6, "no user", "2026-01-05 10:00:00"),
            # Twice in one file: the second is a duplicate, not a new message.
            (1, "alice", "news", 1, "good", "2026-01-05 10:00:00"),
        ],
    )

    async def scenario(sessions):
        stats = await import_sqlite(tmp_path, sessions, "UTC")
        return stats, (await _snapshot(sessions))[2]

    stats, messages = run_db(scenario)

    assert [m[3] for m in messages] == ["good"]
    assert (stats.messages_scanned, stats.messages_inserted) == (7, 1)
    assert (stats.duplicates, stats.invalid_rows) == (1, 5)


def test_existing_users_are_not_overwritten(run_db, tmp_path):
    from db import repositories as repo

    _fixture(tmp_path)

    async def scenario(sessions):
        async with sessions.begin() as session:
            # A fresher state, written by a collection after the first import.
            await repo.upsert_user(session, 1, "alice_renamed", "Alice", None)
        stats = await import_sqlite(tmp_path, sessions, "UTC")
        return stats, (await _snapshot(sessions))[0]

    stats, users = run_db(scenario)

    assert users == [(1, "alice_renamed", "Alice"), (2, "bob", None)]
    assert (stats.users_scanned, stats.users_inserted) == (2, 1)


def test_user_without_username_keeps_the_name_from_the_file(run_db, tmp_path):
    _user_db(
        tmp_path / "хрюкало_офф_7.db", [(7, None, "news", 1, "x", "2026-01-05 10:00:00")]
    )
    # Collection creates the file even when the user has no comments.
    _user_db(tmp_path / "rotor8_8.db")
    _user_db(tmp_path / "9.db")

    async def scenario(sessions):
        stats = await import_sqlite(tmp_path, sessions, "UTC")
        return stats, (await _snapshot(sessions))[0]

    stats, users = run_db(scenario)

    assert users == [(7, None, "Хрюкало Офф"), (8, None, "Rotor8"), (9, None, None)]
    assert stats.users_inserted == 3


def test_only_users_with_a_file_become_profiles(run_db, tmp_path):
    import os

    from db import repositories as repo

    _fixture(tmp_path)
    written = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)
    os.utime(tmp_path / "alice_1.db", (written.timestamp(), written.timestamp()))

    async def scenario(sessions):
        await import_sqlite(tmp_path, sessions, "UTC")
        async with sessions() as session:
            first = (await repo.get_user_by_tg_id(session, 1)).profile_collected_at
            profiles = await repo.list_profile_users(session)
        async with sessions.begin() as session:
            later = datetime(2026, 4, 1, tzinfo=timezone.utc)
            await repo.mark_profiles_collected(session, {profiles[0].id: later})
        await import_sqlite(tmp_path, sessions, "UTC")
        async with sessions() as session:
            second = (await repo.get_user_by_tg_id(session, 1)).profile_collected_at
        return first, [p.tg_id for p in profiles], second, later

    first, profile_ids, second, later = run_db(scenario)

    # bob is only an author in app.db: no profile.
    assert profile_ids == [1]
    assert first == written
    # A collection made after the import is not pushed back by a repeated import.
    assert second == later


def test_unreadable_file_does_not_stop_the_others(run_db, tmp_path):
    _fixture(tmp_path)
    (tmp_path / "broken_5.db").write_bytes(b"not a database at all")
    (tmp_path / "other.db").write_bytes(b"")

    async def scenario(sessions):
        stats = await import_sqlite(tmp_path, sessions, "UTC")
        return stats, (await _snapshot(sessions))[2]

    stats, messages = run_db(scenario)

    assert len(messages) == 2
    assert stats.failed_files == ["broken_5.db"]
