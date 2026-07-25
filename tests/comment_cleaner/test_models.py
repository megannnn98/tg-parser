from __future__ import annotations

from comment_cleaner.models import (
    Features,
    ProcessedMessage,
    ProcessingStats,
    RawMessage,
    Transformation,
    UserBatch,
    ValidationError,
)


def test_raw_message_creation():
    raw = RawMessage(
        message_id="123",
        user_id="456",
        text="Hello world",
    )
    assert raw.message_id == "123"
    assert raw.user_id == "456"
    assert raw.text == "Hello world"


def test_raw_message_with_extra_fields():
    raw = RawMessage(
        message_id="1",
        user_id="2",
        text="test",
        extra_field="ignored",
    )
    assert raw.model_extra == {"extra_field": "ignored"}


def test_processed_message_defaults():
    msg = ProcessedMessage(
        message_id="1",
        user_id="100",
        original_text="test",
        cleaned_text="test",
    )
    assert msg.features.contains_url is False
    assert msg.features.contains_mention is False
    assert msg.is_duplicate is False
    assert msg.information_score == 1.0
    assert msg.transformations == []
    assert msg.quoted_text is None
    assert msg.reply_context is None


def test_processed_message_serialization():
    msg = ProcessedMessage(
        message_id="1",
        user_id="100",
        original_text="@ivan привет",
        cleaned_text="[MENTION], привет",
        features=Features(contains_mention=True),
        transformations=[
            Transformation(
                type="replace_mention",
                original="@ivan",
                replacement="[MENTION]",
            )
        ],
        mentions=["ivan"],
    )
    data = msg.model_dump()
    assert data["message_id"] == "1"
    assert data["features"]["contains_mention"] is True
    assert len(data["transformations"]) == 1
    assert data["transformations"][0]["type"] == "replace_mention"


def test_processed_message_json_roundtrip():
    msg = ProcessedMessage(
        message_id="1",
        user_id="100",
        original_text="test",
        cleaned_text="test",
        features=Features(contains_url=True),
        urls=[{"original": "https://example.com", "domain": "example.com"}],
    )
    json_str = msg.model_dump_json()
    parsed = ProcessedMessage.model_validate_json(json_str)
    assert parsed.message_id == "1"
    assert parsed.features.contains_url is True
    assert len(parsed.urls) == 1
    assert parsed.urls[0].domain == "example.com"


def test_user_batch():
    msg1 = ProcessedMessage(
        message_id="1",
        user_id="100",
        original_text="hello",
        cleaned_text="hello",
        timestamp="2026-01-01T00:00:00",
    )
    msg2 = ProcessedMessage(
        message_id="2",
        user_id="100",
        original_text="world",
        cleaned_text="world",
        timestamp="2026-01-02T00:00:00",
    )
    batch = UserBatch(
        user_id="100",
        comments_count=2,
        total_chars=10,
        first_timestamp="2026-01-01T00:00:00",
        last_timestamp="2026-01-02T00:00:00",
        comments=[msg1, msg2],
    )
    assert batch.user_id == "100"
    assert batch.comments_count == 2
    assert len(batch.comments) == 2


def test_validation_error():
    err = ValidationError(
        message_id="123",
        error_type="missing_user_id",
        error_message="User ID is missing",
    )
    data = err.model_dump()
    assert data["message_id"] == "123"
    assert data["error_type"] == "missing_user_id"


def test_processing_stats_defaults():
    stats = ProcessingStats()
    assert stats.total_records == 0
    assert stats.success_count == 0
    assert stats.error_count == 0
    assert stats.length_distribution == {}


def test_extra_fields_preserved():
    raw = RawMessage(
        message_id="1",
        user_id="2",
        text="test",
        custom_field="value",
    )
    assert raw.model_extra["custom_field"] == "value"
