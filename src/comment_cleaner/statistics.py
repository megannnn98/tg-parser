from __future__ import annotations

from comment_cleaner.config import Config
from comment_cleaner.models import (
    ProcessingStats,
)
from comment_cleaner.pipeline import _build_message_index, _length_bucket


def compute_stats(
    config: Config,
) -> ProcessingStats:
    stats = ProcessingStats()

    index = _build_message_index(config)
    if not index:
        return stats

    stats.total_records = len(index)

    for _msg_id, data in index.items():
        text = str(data.get("text", ""))
        length = len(text)

        if not text.strip():
            stats.empty_messages += 1

        bucket = _length_bucket(length)
        stats.length_distribution[bucket] = stats.length_distribution.get(bucket, 0) + 1

    return stats
