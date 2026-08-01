import sys

from parser.utils import (
    join_name,
    normalize,
    parse_args,
    parse_user_ref,
    user_db_filename,
)

import pytest


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", ""),
        ("hello", "hello"),
        ("Hello", "hello"),
        ("Hello, World!", "hello, world!"),
        ("Ｆｕｌｌｗｉｄｔｈ Ｔｅｘｔ", "fullwidth text"),
        ("① Ⅳ ﬀ", "1 iv ff"),
        ("Café", "café"),
    ],
)
def test_normalize(raw: str, expected: str):
    assert normalize(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("@vasya", "vasya"),
        ("vasya", "vasya"),
        ("  @vasya  ", "vasya"),
        ("12345", 12345),
        ("@12345", 12345),
        ("user123", "user123"),
    ],
)
def test_parse_user_ref(raw: str, expected: int | str):
    assert parse_user_ref(raw) == expected


@pytest.mark.parametrize("raw", ["", "@", "   "])
def test_parse_user_ref_rejects_empty(raw: str):
    with pytest.raises(ValueError):
        parse_user_ref(raw)


@pytest.mark.parametrize(
    ("tg_id", "username", "expected"),
    [
        (12345678, "vasya", "vasya_12345678.db"),
        (12345678, "Vasya_Pupkin", "vasya_pupkin_12345678.db"),
        (12345678, None, "12345678.db"),
        (12345678, "", "12345678.db"),
        (12345678, "../../etc/passwd", "etcpasswd_12345678.db"),
        (12345678, "!!!", "12345678.db"),
    ],
)
def test_user_db_filename(tg_id: int, username: str | None, expected: str):
    assert user_db_filename(tg_id, username) == expected


@pytest.mark.parametrize(
    ("username", "first_name", "last_name", "expected"),
    [
        # no username: the display name carries the file name
        (None, "Хрюкало", "Офф", "хрюкало_офф_555.db"),
        (None, "Хрюкало", None, "хрюкало_555.db"),
        (None, None, "Офф", "офф_555.db"),
        # username wins over the display name
        ("hryukalo", "Хрюкало", "Офф", "hryukalo_555.db"),
        # nothing usable left
        (None, None, None, "555.db"),
        (None, "", "", "555.db"),
        (None, "🐷", None, "555.db"),
        # emoji, punctuation and odd spacing must not leak into the path
        (None, "🐷 Хрюкало", "Офф!", "хрюкало_офф_555.db"),
        (None, "Хрюкало Офф", None, "хрюкало_офф_555.db"),
        (None, "  Хрюкало   Офф  ", None, "хрюкало_офф_555.db"),
        (None, "../../etc", "passwd", "etc_passwd_555.db"),
    ],
)
def test_user_db_filename_falls_back_to_display_name(
    username: str | None,
    first_name: str | None,
    last_name: str | None,
    expected: str,
):
    assert user_db_filename(555, username, first_name, last_name) == expected


@pytest.mark.parametrize(
    ("first_name", "last_name", "expected"),
    [
        ("Хрюкало", "Офф", "Хрюкало Офф"),
        ("Хрюкало", None, "Хрюкало"),
        (None, "Офф", "Офф"),
        (None, None, ""),
    ],
)
def test_join_name(first_name, last_name, expected):
    assert join_name(first_name, last_name) == expected


def test_parse_args_defaults_to_collect(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py"])
    args = parse_args()
    assert args.mode == "collect"
    assert args.user is None


def test_parse_args_user_comments_takes_user(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "user-comments", "@vasya"])
    args = parse_args()
    assert args.mode == "user-comments"
    assert args.user == "@vasya"


def test_parse_args_user_comments_requires_user(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "user-comments"])
    with pytest.raises(SystemExit):
        parse_args()


def test_parse_args_find_user_takes_query(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "find-user", "Хрюкало Офф"])
    args = parse_args()
    assert args.mode == "find-user"
    assert args.user == "Хрюкало Офф"


def test_parse_args_discover_channels_takes_no_argument(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "discover-channels"])
    args = parse_args()
    assert args.mode == "discover-channels"
    assert args.user is None


def test_parse_args_web_takes_no_argument(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "web"])
    args = parse_args()
    assert args.mode == "web"
    assert args.user is None


def test_parse_args_find_user_requires_query(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["main.py", "find-user"])
    with pytest.raises(SystemExit):
        parse_args()
