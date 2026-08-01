from pathlib import Path
import sqlite3

import pytest

from parser.user_profile import (
    UserProfileError,
    list_user_profiles,
    load_user_profile,
)


def _create_user_db(db_path: Path, rows: list[tuple[int, str | None, str, int]]):
    with sqlite3.connect(db_path) as db:
        db.execute(
            """
            CREATE TABLE user_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                username TEXT,
                channel TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                date TEXT NOT NULL,
                UNIQUE(channel, message_id)
            )
            """
        )
        db.executemany(
            """
            INSERT INTO user_messages
            (tg_id, username, channel, message_id, text, date)
            VALUES (?, ?, ?, ?, 'text', '2026-08-01')
            """,
            rows,
        )


def test_load_user_profile_groups_channels_for_primary_user(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    _create_user_db(
        db_path,
        [
            (7, "old_name", "chan_b", 1),
            (7, "vasya", "chan_a", 2),
            (7, "vasya", "chan_a", 3),
            (8, "other", "chan_z", 4),
        ],
    )

    profile = load_user_profile(db_path)

    assert profile.db_name == "vasya_7.db"
    assert profile.tg_id == 7
    assert profile.username == "vasya"
    assert profile.display_name is None
    assert profile.display_username == "@vasya"
    assert profile.total_messages == 3
    assert profile.channel_count == 2
    assert [(c.name, c.message_count, c.percent) for c in profile.channels] == [
        ("chan_a", 2, 66.7),
        ("chan_b", 1, 33.3),
    ]
    assert profile.channels[0].dasharray == "66.7 33.3"
    assert profile.channels[1].dashoffset == -66.7


def test_load_user_profile_falls_back_to_display_name_from_db_filename(
    tmp_path: Path,
):
    db_path = tmp_path / "хрюкало_офф_7.db"
    _create_user_db(db_path, [(7, None, "chan_a", 1)])

    profile = load_user_profile(db_path)

    assert profile.username is None
    assert profile.display_name == "Хрюкало Офф"
    assert profile.display_username == "Хрюкало Офф"


def test_load_user_profile_closes_chart_rounding_gap(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    _create_user_db(
        db_path,
        [
            (7, "vasya", "chan_a", 1),
            (7, "vasya", "chan_b", 2),
            (7, "vasya", "chan_c", 3),
        ],
    )

    profile = load_user_profile(db_path)

    assert [channel.percent for channel in profile.channels] == [33.3, 33.3, 33.4]
    assert round(sum(channel.percent for channel in profile.channels), 1) == 100.0


def test_load_user_profile_handles_missing_username(tmp_path: Path):
    db_path = tmp_path / "7.db"
    _create_user_db(db_path, [(7, None, "chan_a", 1)])

    profile = load_user_profile(db_path)

    assert profile.username is None
    assert profile.display_name is None
    assert profile.display_username == "нет ника"


def test_load_user_profile_rejects_non_user_database(tmp_path: Path):
    db_path = tmp_path / "app.db"
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY)")

    with pytest.raises(UserProfileError):
        load_user_profile(db_path)


def test_list_user_profiles_skips_app_db_and_empty_directory(tmp_path: Path):
    _create_user_db(tmp_path / "vasya_7.db", [(7, "vasya", "chan_a", 1)])
    with sqlite3.connect(tmp_path / "app.db") as db:
        db.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY)")

    profiles = list_user_profiles(tmp_path)

    assert [profile.db_name for profile in profiles] == ["vasya_7.db"]
    assert list_user_profiles(tmp_path / "missing") == []


def test_list_user_profiles_logs_unreadable_databases(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    _create_user_db(tmp_path / "vasya_7.db", [(7, "vasya", "chan_a", 1)])
    (tmp_path / "broken.db").write_text("not sqlite")

    profiles = list_user_profiles(tmp_path)

    assert [profile.db_name for profile in profiles] == ["vasya_7.db"]
    assert "Skipped unreadable user database" in caplog.text
