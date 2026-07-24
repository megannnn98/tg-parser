from pathlib import Path

import pytest

from parser.export import write_comments_csv


def _read_raw(path: Path) -> str:
    with path.open(newline="") as f:
        return f.read()


def test_write_comments_csv_creates_file_with_header_and_rows(tmp_path: Path):
    path = write_comments_csv(
        data_dir=tmp_path,
        channel="chan_a",
        timestamp="20260101_120000",
        rows=[("2025-02-01", "first"), ("2025-02-02", "second")],
    )

    assert path == tmp_path / "comments_chan_a_20260101_120000.csv"
    assert _read_raw(path) == "date,text\r\n2025-02-01,first\r\n2025-02-02,second\r\n"


def test_write_comments_csv_escapes_commas_and_quotes(tmp_path: Path):
    path = write_comments_csv(
        data_dir=tmp_path,
        channel="chan_a",
        timestamp="20260101_120000",
        rows=[("2025-02-01", 'hello, "world"')],
    )

    assert _read_raw(path) == 'date,text\r\n2025-02-01,"hello, ""world"""\r\n'


def test_write_comments_csv_creates_data_dir_if_missing(tmp_path: Path):
    data_dir = tmp_path / "nested"

    path = write_comments_csv(
        data_dir=data_dir,
        channel="chan_a",
        timestamp="20260101_120000",
        rows=[],
    )

    assert data_dir.exists()
    assert _read_raw(path) == "date,text\r\n"


@pytest.mark.parametrize(
    "channel",
    ["../escape", "a/b", "a\\b", ".", ".."],
)
def test_write_comments_csv_rejects_unsafe_channel_names(tmp_path: Path, channel: str):
    with pytest.raises(ValueError):
        write_comments_csv(
            data_dir=tmp_path,
            channel=channel,
            timestamp="20260101_120000",
            rows=[],
        )
