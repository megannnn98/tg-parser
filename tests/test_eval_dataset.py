import json
from datetime import datetime, timezone

import pytest

from benchmarks.corpus import Comment
from benchmarks.eval_dataset import load_queries

WHEN = datetime(2026, 1, 1, tzinfo=timezone.utc)
COMMENTS = [
    Comment(id=501, tg_id=7, channel_id=1, channel="news", tg_message_id=10, date=WHEN, text="a"),
    Comment(id=502, tg_id=7, channel_id=2, channel="tech", tg_message_id=10, date=WHEN, text="b"),
]


def _files(tmp_path, messages):
    dataset = tmp_path / "dataset.jsonl"
    dataset.write_text(
        json.dumps(
            {
                "id": "q01",
                "user": "eval_user_01",
                "topic": "t",
                "query": "вопрос",
                "relevant": list(messages),
            },
            ensure_ascii=False,
        )
        + "\n\n"
    )
    mapping = tmp_path / "mapping.local.json"
    mapping.write_text(json.dumps({"users": {"eval_user_01": 7}, "messages": messages}))
    return dataset, mapping


def test_anonymous_ids_resolve_to_the_stored_comments(tmp_path):
    dataset, mapping = _files(
        tmp_path,
        {
            # The same Telegram id in two channels: two different comments.
            "eval_msg_0001": {"channel": "news", "tg_message_id": 10},
            "eval_msg_0002": {"channel": "tech", "tg_message_id": 10},
        },
    )

    (query,) = load_queries(COMMENTS, dataset, mapping)

    assert (query.id, query.topic, query.query, query.tg_id) == ("q01", "t", "вопрос", 7)
    assert query.relevant == {501, 502}


def test_a_comment_missing_from_the_database_stops_the_benchmark(tmp_path):
    dataset, mapping = _files(
        tmp_path, {"eval_msg_0001": {"channel": "news", "tg_message_id": 99}}
    )

    with pytest.raises(SystemExit, match="eval_msg_0001"):
        load_queries(COMMENTS, dataset, mapping)


def test_missing_local_mapping_is_explained(tmp_path):
    dataset, mapping = _files(tmp_path, {})
    mapping.unlink()

    with pytest.raises(SystemExit, match="not in git"):
        load_queries(COMMENTS, dataset, mapping)
