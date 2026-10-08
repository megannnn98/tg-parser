"""Loads the retrieval evaluation set for the benchmark.

benchmarks/eval/dataset.jsonl is version-controlled and holds anonymous ids
only; benchmarks/eval/mapping.local.json, kept out of git, says which stored
comment each id stands for. See benchmarks/eval/README.md.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from benchmarks.corpus import Comment

DATASET = Path(__file__).parent / "eval" / "dataset.jsonl"
MAPPING = Path(__file__).parent / "eval" / "mapping.local.json"


@dataclass(frozen=True)
class EvalQuery:
    id: str
    topic: str
    query: str
    tg_id: int
    relevant: frozenset[int]  # messages.id


def load_queries(
    comments: list[Comment], dataset: Path = DATASET, mapping: Path = MAPPING
) -> list[EvalQuery]:
    """Resolves the anonymous ids against the comments stored now."""
    if not mapping.exists():
        raise SystemExit(
            f"{mapping} is missing: it maps the anonymous ids of {dataset.name} to "
            "real comments and is not in git. Without it the retrieval benchmark "
            "cannot run on this machine."
        )
    local = json.loads(mapping.read_text())
    by_key = {(c.channel, c.tg_message_id): c.id for c in comments}

    queries = []
    for line in dataset.read_text().splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        relevant = set()
        for anonymous in item["relevant"]:
            ref = local["messages"][anonymous]
            message_id = by_key.get((ref["channel"], ref["tg_message_id"]))
            if message_id is None:
                raise SystemExit(
                    f"{item['id']}: {anonymous} is not among the stored comments"
                )
            relevant.add(message_id)
        queries.append(
            EvalQuery(
                id=item["id"],
                topic=item["topic"],
                query=item["query"],
                tg_id=local["users"][item["user"]],
                relevant=frozenset(relevant),
            )
        )
    return queries
