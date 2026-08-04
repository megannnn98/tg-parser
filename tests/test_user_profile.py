from pathlib import Path
import sqlite3

import pytest

from parser.user_profile import (
    UserProfileError,
    fetch_user_comments,
    list_user_profiles,
    load_user_profile,
    render_user_comments_text,
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


def _create_comments_db(db_path: Path, rows: list[tuple[int, str, int, str, str]]):
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
            VALUES (?, 'vasya', ?, ?, ?, ?)
            """,
            rows,
        )


def test_fetch_user_comments_orders_by_date_then_channel_then_message_id(
    tmp_path: Path,
):
    db_path = tmp_path / "vasya_7.db"
    _create_comments_db(
        db_path,
        [
            (7, "chan_b", 1, "later", "2026-08-02"),
            (7, "chan_a", 2, "earlier second", "2026-08-01"),
            (7, "chan_a", 1, "earlier first", "2026-08-01"),
            (8, "chan_a", 3, "other user", "2026-08-01"),
        ],
    )

    comments = fetch_user_comments(db_path, tg_id=7)

    assert [(c.date, c.channel, c.text) for c in comments] == [
        ("2026-08-01", "chan_a", "earlier first"),
        ("2026-08-01", "chan_a", "earlier second"),
        ("2026-08-02", "chan_b", "later"),
    ]


def test_fetch_user_comments_breaks_same_date_ties_by_channel(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    _create_comments_db(
        db_path,
        [
            (7, "chan_c", 2, "chan_c later id", "2026-08-01"),
            (7, "chan_b", 1, "chan_b earlier id", "2026-08-01"),
            (7, "chan_a", 1, "chan_a earlier id", "2026-08-01"),
        ],
    )

    comments = fetch_user_comments(db_path, tg_id=7)

    assert [c.channel for c in comments] == ["chan_a", "chan_b", "chan_c"]


def test_fetch_user_comments_returns_empty_list_without_user_messages_table(
    tmp_path: Path,
):
    db_path = tmp_path / "empty_7.db"
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")

    assert fetch_user_comments(db_path, tg_id=7) == []


def test_render_user_comments_text_joins_entries_with_blank_line(tmp_path: Path):
    db_path = tmp_path / "vasya_7.db"
    _create_comments_db(
        db_path,
        [
            (7, "chan_a", 1, "hello", "2026-08-01"),
            (7, "chan_b", 2, "world", "2026-08-02"),
        ],
    )
    comments = fetch_user_comments(db_path, tg_id=7)

    text = render_user_comments_text(comments)

    assert text == "hello\n\nworld"


def test_list_user_profiles_logs_unreadable_databases(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
):
    _create_user_db(tmp_path / "vasya_7.db", [(7, "vasya", "chan_a", 1)])
    (tmp_path / "broken.db").write_text("not sqlite")

    profiles = list_user_profiles(tmp_path)

    assert [profile.db_name for profile in profiles] == ["vasya_7.db"]
    assert "Skipped unreadable user database" in caplog.text


def _create_activity_db(
    db_path: Path, rows: list[tuple[int, str, str]]
) -> None:
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
            VALUES (?, 'vasya', 'test_channel', ?, 'text', ?)
            """,
            rows,
        )


def test_fetch_hourly_activity_groups_by_hour(tmp_path: Path):
    from parser.user_profile import fetch_hourly_activity

    db_path = tmp_path / "test.db"
    _create_activity_db(
        db_path,
        [
            (7, 1, "2026-08-01 14:00:00"),
            (7, 2, "2026-08-01 14:30:00"),
            (7, 3, "2026-08-01 14:59:00"),
            (7, 4, "2026-08-01 08:00:00"),
            (7, 5, "2026-08-02 14:00:00"),
            (8, 6, "2026-08-01 23:00:00"),
        ],
    )

    result = fetch_hourly_activity(db_path, tg_id=7)

    assert len(result) == 24
    result_by_hour = {r.hour: r.count for r in result}
    assert result_by_hour[8] == 1
    assert result_by_hour[14] == 4
    assert result_by_hour[0] == 0


def test_fetch_hourly_activity_returns_empty_on_missing_table(tmp_path: Path):
    from parser.user_profile import fetch_hourly_activity

    db_path = tmp_path / "empty.db"
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")

    assert fetch_hourly_activity(db_path, tg_id=7) == []


def test_fetch_hourly_activity_returns_zeroes_for_no_matching_rows(
    tmp_path: Path,
):
    from parser.user_profile import fetch_hourly_activity

    db_path = tmp_path / "test.db"
    _create_activity_db(db_path, [(7, 1, "2026-08-01 10:00:00")])

    result = fetch_hourly_activity(db_path, tg_id=99)

    assert len(result) == 24
    assert all(r.count == 0 for r in result)


def test_fetch_daily_activity_orders_by_date(tmp_path: Path):
    from parser.user_profile import fetch_daily_activity

    db_path = tmp_path / "test.db"
    _create_activity_db(
        db_path,
        [
            (7, 1, "2026-08-03 10:00:00"),
            (7, 2, "2026-08-01 09:00:00"),
            (7, 3, "2026-08-01 10:00:00"),
            (7, 4, "2026-08-02 15:00:00"),
        ],
    )

    result = fetch_daily_activity(db_path, tg_id=7)

    assert [(r.date, r.count) for r in result] == [
        ("2026-08-01", 2),
        ("2026-08-02", 1),
        ("2026-08-03", 1),
    ]


def test_fetch_daily_activity_skips_other_users(tmp_path: Path):
    from parser.user_profile import fetch_daily_activity

    db_path = tmp_path / "test.db"
    _create_activity_db(
        db_path,
        [
            (7, 1, "2026-08-01 10:00:00"),
            (7, 2, "2026-08-01 11:00:00"),
            (8, 3, "2026-08-01 12:00:00"),
        ],
    )

    result = fetch_daily_activity(db_path, tg_id=7)

    assert [(r.date, r.count) for r in result] == [("2026-08-01", 2)]


def test_fetch_daily_activity_returns_empty_on_missing_table(tmp_path: Path):
    from parser.user_profile import fetch_daily_activity

    db_path = tmp_path / "empty.db"
    with sqlite3.connect(db_path) as db:
        db.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")

    assert fetch_daily_activity(db_path, tg_id=7) == []
