from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from comment_cleaner.config import Config, load_dictionary
from comment_cleaner.loaders.database_loader import create_sqlite_engine, load_database
from comment_cleaner.loaders.jsonl_loader import iter_jsonl, load_jsonl
from comment_cleaner.models import Checkpoint, ProcessedMessage, ProcessingStats, RawMessage
from comment_cleaner.privacy import mask_pii
from comment_cleaner.processors.bot_detector import BotDetector
from comment_cleaner.processors.duplicate_detector import DuplicateDetector
from comment_cleaner.processors.hashtag_emoji import HashtagEmojiProcessor
from comment_cleaner.processors.information_filter import InformationFilter
from comment_cleaner.processors.mention_processor import MentionProcessor
from comment_cleaner.processors.quote_parser import QuoteParser
from comment_cleaner.processors.reply_context import ReplyContextProcessor
from comment_cleaner.processors.sarcasm_detector import SarcasmDetector
from comment_cleaner.processors.slang_detector import SlangDetector
from comment_cleaner.processors.unicode_normalizer import UnicodeNormalizer
from comment_cleaner.processors.url_processor import UrlProcessor

logger = logging.getLogger(__name__)


def _load_dictionaries(config: Config) -> dict[str, Any]:
    dictionaries: dict[str, Any] = {}

    slang_paths = ["dictionaries/political_slang.yaml"]
    slang_dict: dict[str, Any] = {}
    for path in slang_paths:
        try:
            slang_dict.update(load_dictionary(path))
        except FileNotFoundError:
            logger.debug("Slang dictionary not found: %s", path)
    dictionaries["slang"] = slang_dict

    low_info_path = "dictionaries/low_information_phrases.yaml"
    try:
        low_info_data = load_dictionary(low_info_path)
        low_info_words: set[str] = set()
        low_info_phrases: set[str] = set()
        for w in low_info_data.get("low_information_single", []):
            if isinstance(w, str):
                low_info_words.add(w.lower().strip())
        for p in low_info_data.get("low_information_phrases", []):
            if isinstance(p, str):
                low_info_phrases.add(p.lower().strip())
        dictionaries["low_info_words"] = low_info_words
        dictionaries["low_info_phrases"] = low_info_phrases
    except FileNotFoundError:
        logger.debug("Low information dictionary not found: %s", low_info_path)

    bot_path = "dictionaries/bot_patterns.yaml"
    try:
        dictionaries["bot_patterns"] = load_dictionary(bot_path)
    except FileNotFoundError:
        logger.debug("Bot patterns dictionary not found: %s", bot_path)

    return dictionaries


def _build_pipeline(config: Config, dictionaries: dict[str, Any]) -> list[Any]:
    return [
        UnicodeNormalizer(config),
        UrlProcessor(config),
        MentionProcessor(config),
        HashtagEmojiProcessor(config),
        QuoteParser(config),
        ReplyContextProcessor(config),
        InformationFilter(
            config,
            low_info_words=dictionaries.get("low_info_words"),
            low_info_phrases=dictionaries.get("low_info_phrases"),
        ),
        SlangDetector(config, slang_dict=dictionaries.get("slang", {})),
        SarcasmDetector(config),
        BotDetector(config, bot_patterns=dictionaries.get("bot_patterns")),
        DuplicateDetector(config),
    ]


def _raw_to_processed(raw: RawMessage, config: Config) -> ProcessedMessage:
    text = mask_pii(raw.text)
    return ProcessedMessage(
        message_id=raw.message_id,
        user_id=raw.user_id,
        chat_id=raw.chat_id,
        timestamp=raw.timestamp,
        original_text=raw.text,
        cleaned_text=text,
        processing_version=config.processing_version,
    )


def _build_message_index(config: Config) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    col_map = config.column_mapping
    msg_id_col = col_map.get("message_id", "message_id")
    text_col = col_map.get("text", "text")
    user_id_col = col_map.get("user_id", "user_id")
    reply_col = col_map.get("reply_to_message_id", "reply_to_message_id")

    if config.input_type == "jsonl":
        for raw in iter_jsonl(config.input_path):
            if "__parse_error__" in raw:
                continue
            msg_id = str(raw.get(msg_id_col, ""))
            if msg_id:
                index[msg_id] = {
                    "text": str(raw.get(text_col, "")),
                    "user_id": str(raw.get(user_id_col, "")),
                    "reply_to_message_id": str(raw.get(reply_col))
                    if raw.get(reply_col) is not None
                    else None,
                }
    return index


def _load_checkpoint(checkpoint_path: str | Path) -> Checkpoint:
    path = Path(checkpoint_path)
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            return Checkpoint.model_validate(data)
        except Exception:
            return Checkpoint()
    return Checkpoint()


def _save_checkpoint(
    checkpoint_path: str | Path,
    checkpoint: Checkpoint,
) -> None:
    path = Path(checkpoint_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(checkpoint.model_dump_json())


def _get_checkpoint_path(config: Config) -> Path:
    output_path = Path(config.output_path)
    return output_path.parent / f".checkpoint_{output_path.stem}.json"


def process_messages(
    config: Config,
) -> Iterator[ProcessedMessage | None]:
    """Returns an iterator of processed messages.
    After iteration, access `.stats` attribute for ProcessingStats.
    """
    stats = ProcessingStats()
    start_time = time.perf_counter()
    dictionaries = _load_dictionaries(config)
    pipeline = _build_pipeline(config, dictionaries)

    checkpoint_path = _get_checkpoint_path(config)
    checkpoint = _load_checkpoint(checkpoint_path) if config.resume else Checkpoint()

    message_index: dict[str, dict[str, Any]] = {}
    if config.load_reply_context:
        logger.info("Building message index for reply context...")
        message_index = _build_message_index(config)
        logger.info("Index built: %d messages", len(message_index))

    for processor in pipeline:
        if isinstance(processor, ReplyContextProcessor):
            processor.set_message_index(message_index)

    if config.input_type == "sqlite":
        engine = create_sqlite_engine(config.input_path)
        loader = load_database(
            engine,
            table_name="messages",
            column_map=config.column_mapping,
            batch_size=config.batch_size,
        )
    else:
        loader = load_jsonl(config.input_path, config.column_mapping)

    skip_until_found = bool(config.resume and checkpoint.last_processed_message_id)

    def _gen() -> Iterator[ProcessedMessage | None]:
        nonlocal stats, skip_until_found, checkpoint

        for raw in loader:
            stats.total_records += 1

            if raw is None:
                stats.error_count += 1
                yield None
                continue

            if skip_until_found:
                if raw.message_id == checkpoint.last_processed_message_id:
                    skip_until_found = False
                continue

            try:
                message = _raw_to_processed(raw, config)

                for processor in pipeline:
                    if isinstance(processor, ReplyContextProcessor):
                        processor.set_current_raw(raw)
                    message = processor.process(message)

                self_id = message.message_id
                if self_id not in message_index:
                    message_index[self_id] = {
                        "text": message.original_text,
                        "user_id": message.user_id,
                    }

                _update_stats(message, stats)
                stats.success_count += 1

                checkpoint.last_processed_message_id = message.message_id
                checkpoint.total_processed += 1

                if config.remove_system_messages and message.features.is_system_message:
                    continue
                if config.remove_bot_messages and message.features.is_bot_message:
                    continue
                if config.remove_low_information and message.features.low_information:
                    continue

                yield message

            except Exception as exc:
                logger.error("Processing error for msg %s: %s", raw.message_id, exc)
                stats.error_count += 1
                yield None

        if config.resume:
            _save_checkpoint(checkpoint_path, checkpoint)

        stats.processing_duration_seconds = time.perf_counter() - start_time
        logger.info(
            "Processing completed: total=%d success=%d errors=%d duration=%.1fs",
            stats.total_records,
            stats.success_count,
            stats.error_count,
            stats.processing_duration_seconds,
        )

    iterator = _gen()

    # Wrap generator to attach stats attribute
    class _StatsWrapper:
        def __init__(self, gen: Iterator[ProcessedMessage | None], stats: ProcessingStats) -> None:
            self._gen = gen
            self.stats = stats

        def __iter__(self) -> _StatsWrapper:
            return self

        def __next__(self) -> ProcessedMessage | None:
            return next(self._gen)

    return _StatsWrapper(iterator, stats)


def _update_stats(message: ProcessedMessage, stats: ProcessingStats) -> None:
    if not message.cleaned_text.strip():
        stats.empty_messages += 1
    if message.features.contains_url:
        stats.messages_with_url += 1
    if message.features.contains_mention:
        stats.messages_with_mentions += 1
    if message.features.contains_quote:
        stats.messages_with_quotes += 1
    if message.features.contains_reply_context:
        stats.messages_with_reply += 1
    if message.reply_context_missing:
        stats.messages_without_reply_context += 1
    if message.duplicate_type == "exact":
        stats.exact_duplicates += 1
    elif message.duplicate_type == "normalized":
        stats.normalized_duplicates += 1
    elif message.duplicate_type == "fuzzy":
        stats.fuzzy_duplicates += 1
    if message.features.low_information:
        stats.low_information += 1
    if message.features.is_bot_message:
        stats.bot_messages += 1
    if message.features.is_system_message:
        stats.system_messages += 1
    if message.detected_terms:
        stats.messages_with_slang += 1
    if message.features.possible_sarcasm:
        stats.sarcasm_detected += 1

    length_bucket = _length_bucket(len(message.original_text))
    stats.length_distribution[length_bucket] = stats.length_distribution.get(length_bucket, 0) + 1


def _length_bucket(length: int) -> str:
    if length == 0:
        return "0"
    if length <= 10:
        return "1-10"
    if length <= 50:
        return "11-50"
    if length <= 200:
        return "51-200"
    if length <= 500:
        return "201-500"
    if length <= 1000:
        return "501-1000"
    if length <= 5000:
        return "1001-5000"
    return "5000+"
