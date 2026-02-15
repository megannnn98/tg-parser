from parser.utils import normalize

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
