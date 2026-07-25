from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RawMessage(BaseModel):
    message_id: str
    user_id: str
    chat_id: str | None = None
    timestamp: str | None = None
    text: str
    reply_to_message_id: str | None = None
    forwarded_from: str | None = None
    message_type: str | None = None

    model_config = {"extra": "allow"}


class Transformation(BaseModel):
    type: str
    original: str | None = None
    replacement: str | None = None
    details: dict[str, Any] | None = None


class UrlMetadata(BaseModel):
    original: str
    domain: str | None = None


class DetectedTerm(BaseModel):
    term: str
    category: str | None = None


class SarcasmSignal(BaseModel):
    type: str
    value: str


class ReplyChainEntry(BaseModel):
    message_id: str
    text: str | None = None
    user_id: str | None = None


class ReplyContext(BaseModel):
    message_id: str
    text: str | None = None
    user_id: str | None = None
    context_depth: int = 1
    chain: list[ReplyChainEntry] = Field(default_factory=list)


class Features(BaseModel):
    contains_url: bool = False
    contains_mention: bool = False
    contains_emoji: bool = False
    contains_quote: bool = False
    contains_reply_context: bool = False
    contains_political_terms: bool = False
    possible_sarcasm: bool = False
    low_information: bool = False
    is_duplicate: bool = False
    is_bot_message: bool = False
    is_system_message: bool = False


class ProcessedMessage(BaseModel):
    message_id: str
    user_id: str
    chat_id: str | None = None
    timestamp: str | None = None

    original_text: str
    cleaned_text: str

    reply_context: ReplyContext | None = None
    reply_context_missing: bool = False

    quoted_text: str | None = None
    author_text: str | None = None
    quote_parsing_status: str | None = None

    features: Features = Field(default_factory=Features)

    transformations: list[Transformation] = Field(default_factory=list)

    urls: list[UrlMetadata] = Field(default_factory=list)
    mentions: list[str] = Field(default_factory=list)
    hashtags: list[str] = Field(default_factory=list)
    emoji: list[str] = Field(default_factory=list)
    emoji_count: int = 0
    detected_terms: list[DetectedTerm] = Field(default_factory=list)
    sarcasm_signals: list[SarcasmSignal] = Field(default_factory=list)

    is_duplicate: bool = False
    duplicate_of: str | None = None
    duplicate_count: int = 0
    duplicate_type: str | None = None

    information_score: float = 1.0
    requires_context: bool = False

    processing_version: str = "1.0.0"

    model_config = {"extra": "allow"}


class UserBatch(BaseModel):
    user_id: str
    comments_count: int = 0
    total_chars: int = 0
    first_timestamp: str | None = None
    last_timestamp: str | None = None
    comments: list[ProcessedMessage] = Field(default_factory=list)


class ProcessingStats(BaseModel):
    total_records: int = 0
    success_count: int = 0
    error_count: int = 0
    empty_messages: int = 0
    messages_with_url: int = 0
    messages_with_mentions: int = 0
    messages_with_quotes: int = 0
    messages_with_reply: int = 0
    messages_without_reply_context: int = 0
    exact_duplicates: int = 0
    normalized_duplicates: int = 0
    fuzzy_duplicates: int = 0
    low_information: int = 0
    bot_messages: int = 0
    system_messages: int = 0
    messages_with_slang: int = 0
    sarcasm_detected: int = 0
    length_distribution: dict[str, int] = Field(default_factory=dict)
    processing_duration_seconds: float = 0.0


class ValidationError(BaseModel):
    message_id: str | None = None
    record_index: int | None = None
    error_type: str
    error_message: str
    raw_record: dict[str, Any] | None = None


class Checkpoint(BaseModel):
    last_processed_message_id: str | None = None
    last_line_number: int = 0
    total_processed: int = 0
    processing_version: str = "1.0.0"
