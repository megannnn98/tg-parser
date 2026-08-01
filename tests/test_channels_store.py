import json
import threading
from pathlib import Path

import pytest

from parser.channels_store import InvalidChannelError, parse_channels_text, save_channels


def test_parse_channels_text_strips_and_splits_lines():
    assert parse_channels_text("chan_a\nchan_b\n") == ["chan_a", "chan_b"]


def test_parse_channels_text_skips_blank_lines():
    assert parse_channels_text("chan_a\n\n  \nchan_b") == ["chan_a", "chan_b"]


def test_parse_channels_text_strips_leading_at():
    assert parse_channels_text("@chan_a\n@chan_b") == ["chan_a", "chan_b"]


def test_parse_channels_text_strips_whitespace_around_leading_at():
    assert parse_channels_text("  @chan_a  \n  @chan_b") == ["chan_a", "chan_b"]


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


def test_save_channels_survives_concurrent_writers(tmp_path: Path):
    # POST /channels is a sync FastAPI route, run in a real threadpool thread
    # per request. Two overlapping saves writing/renaming the same .json.tmp
    # path (no shared file handle) can truncate each other mid-write. Using
    # very different list lengths makes a short write's tail-of-a-longer-
    # write corruption reproducible: a shorter save that doesn't fully
    # overwrite a longer one's bytes leaves valid-looking JSON followed by
    # garbage, which json.loads() rejects as "Extra data".
    path = tmp_path / "channels.json"
    path.write_text("[]")
    short_list = ["a" * 10]
    long_list = [f"chan_{i:03d}_padding_padding" for i in range(500)]
    errors: list[Exception] = []
    corrupt_reads: list[Exception] = []

    def writer(channels: list[str], rounds: int) -> None:
        for _ in range(rounds):
            try:
                save_channels(path, channels)
            except Exception as exc:  # noqa: BLE001 - captured for the assert below
                errors.append(exc)

    def reader(rounds: int) -> None:
        for _ in range(rounds):
            try:
                json.loads(path.read_text())
            except Exception as exc:  # noqa: BLE001
                corrupt_reads.append(exc)

    rounds = 300
    threads = [
        threading.Thread(target=writer, args=(short_list, rounds)),
        threading.Thread(target=writer, args=(long_list, rounds)),
        threading.Thread(target=reader, args=(rounds,)),
        threading.Thread(target=reader, args=(rounds,)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert corrupt_reads == []
    assert json.loads(path.read_text()) in (short_list, long_list)
