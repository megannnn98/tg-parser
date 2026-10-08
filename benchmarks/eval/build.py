"""Builds the evaluation dataset from its tracked sources and the stored comments.

    python -m benchmarks.eval.build counts          matches of each query's pattern
    python -m benchmarks.eval.build show q03 q04    candidates to judge by hand
    python -m benchmarks.eval.build add q03 CHANNEL:MESSAGE_ID ...
    python -m benchmarks.eval.build remove q01 CHANNEL:MESSAGE_ID ...
    python -m benchmarks.eval.build build           rewrite dataset.jsonl

Tracked, with anonymous ids only:
    queries.jsonl    the queries, their user and the keyword pattern of the topic
    judgments.jsonl  manual additions to and removals from the pattern matches
    dataset.jsonl    the result: relevant messages of every query

On this machine only (mapping.local.json): which Telegram user and which
comment each anonymous id stands for. Ids once given are kept, so rebuilding
on the same comments reproduces dataset.jsonl exactly.

`show` prints real comments to the terminal; nothing it prints is stored.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from benchmarks.corpus import Comment, load_comments, sync_engine

HERE = Path(__file__).parent
QUERIES = HERE / "queries.jsonl"
JUDGMENTS = HERE / "judgments.jsonl"
DATASET = HERE / "dataset.jsonl"
MAPPING = HERE / "mapping.local.json"


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows)
    )


class Mapping:
    """Anonymous ids of users and comments; new ones are appended, none change."""

    def __init__(self):
        if not MAPPING.exists():
            raise SystemExit(
                f"{MAPPING} is missing. It holds the real identities behind the "
                "anonymous ids and is deliberately not in git; without it the "
                "dataset cannot be rebuilt or resolved on this machine."
            )
        data = json.loads(MAPPING.read_text())
        self.users: dict[str, int] = data["users"]
        self.messages: dict[str, dict] = data["messages"]
        self._by_key = {
            (ref["channel"], ref["tg_message_id"]): name
            for name, ref in self.messages.items()
        }

    def message_name(self, comment: Comment) -> str:
        key = (comment.channel, comment.tg_message_id)
        if key not in self._by_key:
            name = f"eval_msg_{len(self.messages) + 1:04d}"
            self.messages[name] = {"channel": key[0], "tg_message_id": key[1]}
            self._by_key[key] = name
        return self._by_key[key]

    def save(self) -> None:
        MAPPING.write_text(
            json.dumps(
                {"users": self.users, "messages": self.messages},
                ensure_ascii=False,
                indent=1,
            )
        )


def _matches(query: dict, user: list[Comment]) -> list[Comment]:
    pattern = re.compile(query["pattern"], re.IGNORECASE)
    return [comment for comment in user if pattern.search(comment.text)]


def _user_comments(mapping: Mapping) -> dict[str, list[Comment]]:
    by_tg_id: dict[int, list[Comment]] = {}
    for comment in load_comments(sync_engine()):
        by_tg_id.setdefault(comment.tg_id, []).append(comment)
    return {name: by_tg_id.get(tg_id, []) for name, tg_id in mapping.users.items()}


def _judgments() -> dict[str, dict]:
    return {
        row["id"]: {"added": row.get("added", []), "removed": row.get("removed", [])}
        for row in _read_jsonl(JUDGMENTS)
    }


def _save_judgments(judgments: dict[str, dict]) -> None:
    _write_jsonl(
        JUDGMENTS,
        [
            {"id": query_id, "added": sorted(j["added"]), "removed": sorted(j["removed"])}
            for query_id, j in sorted(judgments.items())
            if j["added"] or j["removed"]
        ],
    )


def build() -> None:
    mapping = Mapping()
    users = _user_comments(mapping)
    judgments = _judgments()
    rows = []
    for query in _read_jsonl(QUERIES):
        judged = judgments.get(query["id"], {"added": [], "removed": []})
        relevant = {mapping.message_name(c) for c in _matches(query, users[query["user"]])}
        relevant = (relevant | set(judged["added"])) - set(judged["removed"])
        rows.append(
            {
                "id": query["id"],
                "user": query["user"],
                "topic": query["topic"],
                "query": query["query"],
                "relevant": sorted(relevant),
            }
        )
        print(query["id"], query["topic"], len(relevant))
    _write_jsonl(DATASET, rows)
    mapping.save()


def counts() -> None:
    mapping = Mapping()
    users = _user_comments(mapping)
    for query in _read_jsonl(QUERIES):
        print(query["id"], query["user"], query["topic"],
              len(_matches(query, users[query["user"]])))


def show(query_ids: list[str], top: int) -> None:
    """The message-level embedding top of each query, marked P on a pattern match."""
    import numpy as np

    from benchmarks.embedding_cache import PassageCache
    from embeddings.e5 import MULTILINGUAL_E5_BASE, E5Encoder

    mapping = Mapping()
    users = _user_comments(mapping)
    encoder = E5Encoder(MULTILINGUAL_E5_BASE, cache_dir=Path("data/e5-cache"))
    cache = PassageCache(encoder, Path("data/benchmark"))
    for query in _read_jsonl(QUERIES):
        if query["id"] not in query_ids:
            continue
        user = users[query["user"]]
        matched = {comment.id for comment in _matches(query, user)}
        scores = cache.passages([c.text for c in user]) @ encoder.encode_queries(
            [query["query"]]
        )[0]
        print(f"## {query['id']} {query['topic']}: {query['query']} "
              f"(pattern matches: {len(matched)})")
        for index in np.argsort(-scores)[:top]:
            comment = user[index]
            mark = "P" if comment.id in matched else "-"
            text = " ".join(comment.text.split())[:170]
            print(f"{comment.channel}:{comment.tg_message_id} {mark} {text}")


def judge(query_id: str, keys: list[str], field: str) -> None:
    mapping = Mapping()
    by_key = {
        f"{c.channel}:{c.tg_message_id}": c
        for user in _user_comments(mapping).values()
        for c in user
    }
    judgments = _judgments()
    entry = judgments.setdefault(query_id, {"added": [], "removed": []})
    for key in keys:
        if key not in by_key:
            raise SystemExit(f"{key} is not a stored comment of an evaluation user")
        name = mapping.message_name(by_key[key])
        if name not in entry[field]:
            entry[field].append(name)
    _save_judgments(judgments)
    mapping.save()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("counts")
    commands.add_parser("build")
    show_parser = commands.add_parser("show")
    show_parser.add_argument("queries", nargs="+")
    show_parser.add_argument("--top", type=int, default=12)
    for name in ("add", "remove"):
        judge_parser = commands.add_parser(name)
        judge_parser.add_argument("query")
        judge_parser.add_argument("keys", nargs="+", metavar="CHANNEL:MESSAGE_ID")
    args = parser.parse_args()

    if args.command == "counts":
        counts()
    elif args.command == "build":
        build()
    elif args.command == "show":
        show(args.queries, args.top)
    else:
        judge(args.query, args.keys, "added" if args.command == "add" else "removed")


if __name__ == "__main__":
    main()
