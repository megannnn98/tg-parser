from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from comment_cleaner.config import load_config
from comment_cleaner.models import ProcessedMessage


@pytest.fixture
def temp_dir():
    with tempfile.TemporaryDirectory() as tmp:
        yield Path(tmp)


@pytest.fixture
def sample_jsonl_path(temp_dir):
    records = [
        {
            "message_id": "1",
            "user_id": "100",
            "chat_id": "-100123",
            "timestamp": "2026-07-25T10:30:00+05:00",
            "text": "@ivan ну да конечно 😂 https://example.com",
            "reply_to_message_id": None,
            "forwarded_from": None,
            "message_type": "text",
        },
        {
            "message_id": "2",
            "user_id": "200",
            "chat_id": "-100123",
            "timestamp": "2026-07-25T10:31:00+05:00",
            "text": "Государство всегда действует в интересах граждан.",
            "reply_to_message_id": None,
            "forwarded_from": None,
            "message_type": "text",
        },
        {
            "message_id": "3",
            "user_id": "100",
            "chat_id": "-100123",
            "timestamp": "2026-07-25T10:32:00+05:00",
            "text": "Полностью согласен.",
            "reply_to_message_id": "2",
            "forwarded_from": None,
            "message_type": "text",
        },
        {
            "message_id": "4",
            "user_id": "300",
            "chat_id": "-100123",
            "timestamp": "2026-07-25T10:33:00+05:00",
            "text": "> Все проблемы из-за мигрантов\n\nВот до какого бреда они дошли.",
            "reply_to_message_id": None,
            "forwarded_from": None,
            "message_type": "text",
        },
        {
            "message_id": "5",
            "user_id": "400",
            "chat_id": "-100123",
            "timestamp": "2026-07-25T10:34:00+05:00",
            "text": "+",
            "reply_to_message_id": None,
            "forwarded_from": None,
            "message_type": "text",
        },
        {
            "message_id": "6",
            "user_id": "400",
            "chat_id": "-100123",
            "timestamp": "2026-07-25T10:35:00+05:00",
            "text": "@ivan ты прав",
            "reply_to_message_id": None,
            "forwarded_from": None,
            "message_type": "text",
        },
    ]
    path = temp_dir / "comments.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return path


@pytest.fixture
def default_config():
    return load_config(None)


@pytest.fixture
def config_with_input(sample_jsonl_path):
    from comment_cleaner.config import Config

    data = {
        "input": {
            "type": "jsonl",
            "path": str(sample_jsonl_path),
            "column_mapping": {
                "message_id": "message_id",
                "user_id": "user_id",
                "chat_id": "chat_id",
                "timestamp": "timestamp",
                "text": "text",
                "reply_to_message_id": "reply_to_message_id",
                "forwarded_from": "forwarded_from",
                "message_type": "message_type",
            },
        },
        "output": {"path": str(sample_jsonl_path.parent / "output" / "cleaned.jsonl")},
        "duplicates": {"mode": "mark"},
        "context": {"load_reply_context": True, "max_reply_depth": 1},
    }
    return Config(data)


@pytest.fixture
def empty_processed_message():
    return ProcessedMessage(
        message_id="test_1",
        user_id="100",
        original_text="",
        cleaned_text="",
    )
