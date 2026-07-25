from __future__ import annotations

import pytest

from comment_cleaner.loaders.jsonl_loader import (
    iter_jsonl,
    load_jsonl,
    map_record,
    validate_record,
)
from comment_cleaner.models import RawMessage


def test_iter_jsonl_parses_valid_file(sample_jsonl_path):
    records = list(iter_jsonl(sample_jsonl_path))
    assert len(records) == 6
    assert records[0]["message_id"] == "1"
    assert records[0]["text"] == "@ivan ну да конечно 😂 https://example.com"


def test_iter_jsonl_skips_empty_lines(temp_dir):
    path = temp_dir / "test.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        f.write('{"a": 1}\n')
        f.write("\n")
        f.write('{"b": 2}\n')
    records = list(iter_jsonl(path))
    assert len(records) == 2


def test_iter_jsonl_handles_malformed_lines(temp_dir):
    path = temp_dir / "test.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        f.write('{"a": 1}\n')
        f.write("not json\n")
        f.write('{"b": 2}\n')
    records = list(iter_jsonl(path))
    assert len(records) == 3
    assert "__parse_error__" in records[1]


def test_iter_jsonl_nonexistent_file():
    with pytest.raises(FileNotFoundError):
        list(iter_jsonl("/nonexistent/path.jsonl"))


def test_validate_record_valid():
    record = {
        "message_id": "123",
        "user_id": "456",
        "text": "Hello",
    }
    errors = validate_record(record, {})
    assert errors == []


def test_validate_record_missing_message_id():
    record = {"user_id": "456", "text": "Hello"}
    errors = validate_record(record, {"message_id": "msgId"})
    assert len(errors) >= 1
    assert any(e.error_type == "missing_message_id" for e in errors)


def test_validate_record_missing_user_id():
    record = {"message_id": "123", "text": "Hello"}
    errors = validate_record(record, {})
    assert len(errors) >= 1
    assert any(e.error_type == "missing_user_id" for e in errors)


def test_validate_record_text_too_long():
    record = {
        "message_id": "123",
        "user_id": "456",
        "text": "x" * 2_000_000,
    }
    errors = validate_record(record, {})
    assert any(e.error_type == "text_too_long" for e in errors)


def test_map_record_basic():
    record = {
        "message_id": "123",
        "user_id": "456",
        "text": "Hello world",
        "timestamp": "2026-07-25T10:30:00+05:00",
    }
    msg = map_record(record, {})
    assert msg.message_id == "123"
    assert msg.user_id == "456"
    assert msg.text == "Hello world"
    assert msg.timestamp is not None


def test_map_record_with_column_mapping():
    record = {
        "msgId": "999",
        "usrId": "888",
        "body": "Custom text",
    }
    col_map = {
        "message_id": "msgId",
        "user_id": "usrId",
        "text": "body",
    }
    msg = map_record(record, col_map)
    assert msg.message_id == "999"
    assert msg.user_id == "888"
    assert msg.text == "Custom text"


def test_load_jsonl_yields_messages(sample_jsonl_path):
    messages = [m for m in load_jsonl(sample_jsonl_path) if m is not None]
    assert len(messages) == 6
    assert all(isinstance(m, RawMessage) for m in messages)
    assert messages[0].message_id == "1"


def test_load_jsonl_skips_invalid(temp_dir):
    path = temp_dir / "bad.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        f.write('{"message_id": "1", "text": "ok"}\n')  # missing user_id
        f.write('{"user_id": "2", "text": "ok"}\n')  # missing message_id
        f.write('{"message_id": "3", "user_id": "3", "text": "ok"}\n')
    messages = [m for m in load_jsonl(path) if m is not None]
    assert len(messages) == 1
    assert messages[0].message_id == "3"
