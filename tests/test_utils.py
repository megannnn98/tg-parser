from parser.utils import normalize, parse_args

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


def test_parse_args_comments_mode_with_username():
    args = parse_args(["comments", "--username", "alice"])
    assert args.mode == "comments"
    assert args.username == "alice"


def test_parse_args_defaults_username_to_none():
    args = parse_args(["collect"])
    assert args.username is None
