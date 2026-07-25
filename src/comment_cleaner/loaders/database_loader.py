from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from comment_cleaner.loaders.jsonl_loader import map_record, validate_record
from comment_cleaner.models import RawMessage

logger = logging.getLogger(__name__)


def create_sqlite_engine(db_path: str | Path) -> Engine:
    db_path_str = str(db_path)
    url = db_path_str if db_path_str.startswith("sqlite:///") else f"sqlite:///{db_path_str}"
    return create_engine(url)


def load_database(
    engine: Engine,
    table_name: str = "messages",
    column_map: dict[str, str] | None = None,
    batch_size: int = 1000,
) -> Iterator[RawMessage | None]:
    col_map = column_map or {}
    msg_id_col = col_map.get("message_id", "message_id")
    order_col = col_map.get("timestamp", msg_id_col)

    try:
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT * FROM "{table_name}" ORDER BY {order_col}'))
            keys = list(result.keys())

            batch: list[dict[str, Any]] = []
            for row in result:
                record = dict(zip(keys, row, strict=False))
                batch.append(record)
                if len(batch) >= batch_size:
                    yield from _process_batch(batch, col_map)
                    batch.clear()

            if batch:
                yield from _process_batch(batch, col_map)

    except SQLAlchemyError as exc:
        logger.error("Database error: %s", exc)
        raise


def _process_batch(
    batch: list[dict[str, Any]], col_map: dict[str, str]
) -> Iterator[RawMessage | None]:
    for record in batch:
        errors = validate_record(record, col_map)
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
        yield map_record(record, col_map)
