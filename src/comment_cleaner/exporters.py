from __future__ import annotations

import logging
from pathlib import Path

from comment_cleaner.models import ProcessedMessage, UserBatch, ValidationError

logger = logging.getLogger(__name__)


def write_jsonl(
    messages: list[ProcessedMessage],
    output_path: str | Path,
    mode: str = "a",
) -> int:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with open(path, mode, encoding="utf-8") as f:
        for msg in messages:
            f.write(msg.model_dump_json() + "\n")
            count += 1

    return count


def write_errors_jsonl(
    errors: list[ValidationError],
    error_path: str | Path,
    mode: str = "a",
) -> int:
    path = Path(error_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with open(path, mode, encoding="utf-8") as f:
        for err in errors:
            f.write(err.model_dump_json() + "\n")
            count += 1

    return count


def write_user_batches(
    batches: list[UserBatch],
    output_path: str | Path,
) -> int:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with open(path, "w", encoding="utf-8") as f:
        for batch in batches:
            f.write(batch.model_dump_json() + "\n")
            count += 1

    return count
