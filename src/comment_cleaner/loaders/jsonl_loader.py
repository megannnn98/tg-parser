from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

from comment_cleaner.models import RawMessage, ValidationError

logger = logging.getLogger(__name__)

MAX_TEXT_LENGTH = 1_000_000


def _parse_timestamp(value: str | None) -> str | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.isoformat()
    except (ValueError, TypeError):
        return value


def _validate_encoding(text: str) -> bool:
    try:
        text.encode("utf-8")
        return True
    except UnicodeError:
        return False


def _clean_control_chars(text: str) -> str:
    result: list[str] = []
    for ch in text:
        cp = ord(ch)
        if (cp < 0x20 and cp not in (0x09, 0x0A, 0x0D)) or 0x7F <= cp <= 0x9F:
            result.append(" ")
        else:
            result.append(ch)
    return "".join(result)


def validate_record(record: dict[str, Any], column_map: dict[str, str]) -> list[ValidationError]:
    errors: list[ValidationError] = []

    msg_id_col = column_map.get("message_id", "message_id")
    user_id_col = column_map.get("user_id", "user_id")
    text_col = column_map.get("text", "text")
    ts_col = column_map.get("timestamp", "timestamp")

    message_id = record.get(msg_id_col)
    if message_id is None or str(message_id).strip() == "":
        errors.append(
            ValidationError(
                message_id=None,
                error_type="missing_message_id",
                error_message="Message ID is missing or empty",
                raw_record=record,
            )
        )

    user_id = record.get(user_id_col)
    if user_id is None or str(user_id).strip() == "":
        errors.append(
            ValidationError(
                message_id=str(message_id) if message_id else None,
                error_type="missing_user_id",
                error_message="User ID is missing or empty",
                raw_record=record,
            )
        )

    text = record.get(text_col)
    if text is None:
        errors.append(
            ValidationError(
                message_id=str(message_id) if message_id else None,
                error_type="missing_text",
                error_message="Text field is None",
                raw_record=record,
            )
        )
    elif not isinstance(text, str):
        errors.append(
            ValidationError(
                message_id=str(message_id) if message_id else None,
                error_type="invalid_text_type",
                error_message=f"Text field is {type(text).__name__}, expected str",
                raw_record=record,
            )
        )
    elif isinstance(text, str):
        if len(text) > MAX_TEXT_LENGTH:
            errors.append(
                ValidationError(
                    message_id=str(message_id) if message_id else None,
                    error_type="text_too_long",
                    error_message=f"Text length {len(text)} exceeds maximum {MAX_TEXT_LENGTH}",
                    raw_record=record,
                )
            )
        if not _validate_encoding(text):
            errors.append(
                ValidationError(
                    message_id=str(message_id) if message_id else None,
                    error_type="invalid_encoding",
                    error_message="Text contains invalid UTF-8 sequences",
                    raw_record=record,
                )
            )

    timestamp = record.get(ts_col)
    if timestamp is not None and not isinstance(timestamp, str):
        errors.append(
            ValidationError(
                message_id=str(message_id) if message_id else None,
                error_type="invalid_timestamp_type",
                error_message=f"Timestamp field is {type(timestamp).__name__}",
                raw_record=record,
            )
        )

    return errors


def map_record(record: dict[str, Any], column_map: dict[str, str]) -> RawMessage:
    msg_id_col = column_map.get("message_id", "message_id")
    user_id_col = column_map.get("user_id", "user_id")
    chat_id_col = column_map.get("chat_id", "chat_id")
    ts_col = column_map.get("timestamp", "timestamp")
    text_col = column_map.get("text", "text")
    reply_col = column_map.get("reply_to_message_id", "reply_to_message_id")
    fwd_col = column_map.get("forwarded_from", "forwarded_from")
    msg_type_col = column_map.get("message_type", "message_type")

    text = record.get(text_col, "")
    if not isinstance(text, str):
        text = str(text) if text is not None else ""
    text = _clean_control_chars(text)

    return RawMessage(
        message_id=str(record.get(msg_id_col, "")),
        user_id=str(record.get(user_id_col, "")),
        chat_id=str(record.get(chat_id_col)) if record.get(chat_id_col) is not None else None,
        timestamp=_parse_timestamp(
            str(record.get(ts_col)) if record.get(ts_col) is not None else None
        ),
        text=text,
        reply_to_message_id=str(record.get(reply_col))
        if record.get(reply_col) is not None
        else None,
        forwarded_from=str(record.get(fwd_col)) if record.get(fwd_col) is not None else None,
        message_type=str(record.get(msg_type_col))
        if record.get(msg_type_col) is not None
        else None,
    )


def iter_jsonl(jsonl_path: str | Path) -> Iterator[dict[str, Any]]:
    path = Path(jsonl_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    with open(path, encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            stripped = line.strip()
            if not stripped:
                continue

            try:
                record = json.loads(stripped)
                if not isinstance(record, dict):
                    logger.warning("Line %d: not a JSON object, skipping", line_num)
                    yield {"__parse_error__": f"Line {line_num}: not a JSON object"}
                    continue
                yield record
            except json.JSONDecodeError as exc:
                logger.warning("Line %d: JSON parse error: %s", line_num, exc)
                yield {"__parse_error__": f"Line {line_num}: {exc}"}


def load_jsonl(
    jsonl_path: str | Path, column_map: dict[str, str] | None = None
) -> Iterator[RawMessage | None]:
    col_map = column_map or {}
    for raw in iter_jsonl(jsonl_path):
        if "__parse_error__" in raw:
            logger.error("Skipping malformed record: %s", raw["__parse_error__"])
            yield None
            continue

        errors = validate_record(raw, col_map)
        if errors:
            for err in errors:
                logger.error(
                    "Validation error: msg_id=%s type=%s: %s",
                    err.message_id,
                    err.error_type,
                    err.error_message,
                )
            yield None
            continue

        yield map_record(raw, col_map)
