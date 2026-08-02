import json
from pathlib import Path

import pytest

from parser.channels_store import InvalidChannelError, parse_channels_text, save_channels


def test_parse_channels_text_strips_and_splits_lines():
    assert parse_channels_text("chan_a\nchan_b\n") == ["chan_a", "chan_b"]


def test_parse_channels_text_skips_blank_lines():
    assert parse_channels_text("chan_a\n\n  \nchan_b") == ["chan_a", "chan_b"]


def test_parse_channels_text_strips_leading_at():
    assert parse_channels_text("@chan_a\n@chan_b") == ["chan_a", "chan_b"]


def test_parse_channels_text_trims_whitespace_around_each_line():
    assert parse_channels_text("  chan_a  \n\tchan_b\t") == ["chan_a", "chan_b"]


def test_parse_channels_text_dedupes_case_insensitively_keeping_first():
    assert parse_channels_text("chan_a\nChan_A\nchan_b") == ["chan_a", "chan_b"]


def test_parse_channels_text_empty_input_returns_empty_list():
    assert parse_channels_text("") == []
    assert parse_channels_text("   \n\n") == []


@pytest.mark.parametrize(
    "bad_line",
    [
        "chan a",  # internal whitespace
        "1chan",  # must start with a letter
        "ab",  # too short (< 5 chars)
        "a" * 33,  # too long (> 32 chars)
        "chan-a",  # dash not allowed
        "chan/a",  # slash not allowed
    ],
)
def test_parse_channels_text_rejects_invalid_username(bad_line):
    with pytest.raises(InvalidChannelError):
        parse_channels_text(f"chan_a\n{bad_line}\nchan_b")


def test_parse_channels_text_error_names_the_line_number():
    with pytest.raises(InvalidChannelError) as exc_info:
        parse_channels_text("chan_a\nchan_b\nbad one\n")

    assert "3" in str(exc_info.value)
    assert "bad one" in str(exc_info.value)


def test_save_channels_writes_json_array(tmp_path: Path):
    path = tmp_path / "channels.json"

    save_channels(path, ["chan_a", "chan_b"])

    assert json.loads(path.read_text()) == ["chan_a", "chan_b"]


def test_save_channels_overwrites_existing_file(tmp_path: Path):
    path = tmp_path / "channels.json"
    path.write_text(json.dumps(["old_channel"]))

    save_channels(path, ["chan_a"])

    assert json.loads(path.read_text()) == ["chan_a"]


def test_save_channels_leaves_no_leftover_tmp_file(tmp_path: Path):
    path = tmp_path / "channels.json"

    save_channels(path, ["chan_a"])

    assert not path.with_suffix(".json.tmp").exists()
