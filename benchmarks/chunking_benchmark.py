"""Compares chunking strategies on the stored comments with one embedding model.

    python -m benchmarks.chunking_benchmark              # retrieval and structure
    python -m benchmarks.chunking_benchmark --postgres   # + pgvector size and latency
    python -m benchmarks.chunking_benchmark --compare-models

Everything is derived from the comments in PostgreSQL, read-only. Chunks are
built in memory and their embeddings kept in data/benchmark; with --postgres
the vectors are also loaded into a throwaway schema `bench`, dropped at the
end. Results go to data/benchmark/results.json and, as Markdown, to stdout.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

from benchmarks.corpus import Comment, load_comments, sync_engine
from benchmarks.eval_dataset import EvalQuery, load_queries
from benchmarks.metrics import (
    ndcg_at_k,
    recall_at_k,
    recall_within_tokens,
    reciprocal_rank,
    reciprocal_rank_fusion,
    relevant_token_share,
)
from chunking.builder import SEPARATOR
from chunking.strategies import ChunkMessage, build_chunks
from embeddings.e5 import SPECS, E5Encoder

OUT = Path("data/benchmark")
CONTEXT_BUDGET = 1000  # tokens of retrieved text an LLM prompt gets
SHORT_TOKENS = 5
SHORT_MESSAGE_CUTOFFS = (5, 10)

MINUTE, HOUR, DAY = 60, 3600, 86400


def variants(limit: int) -> list[tuple[str, str, dict]]:
    """(name, strategy, parameters). `limit` is the model's content token limit."""
    result: list[tuple[str, str, dict]] = [("A message", "message", {})]
    for same in (False, True):
        scope = "channel" if same else "global"
        for n in (2, 3, 5, 10):
            result.append(
                (f"B fixed {n} {scope}", "fixed_messages",
                 {"max_messages": n, "same_channel": same})
            )
    for budget in (128, 256, 512):
        tokens = min(budget, limit)
        for overlap in (0, 0.1, 0.2):
            result.append(
                (f"C tokens {budget} overlap {int(overlap * 100)}% channel",
                 "token_budget",
                 {"max_tokens": tokens, "overlap": overlap, "same_channel": True})
            )
        result.append(
            (f"C tokens {budget} overlap 0% global", "token_budget",
             {"max_tokens": tokens, "overlap": 0, "same_channel": False})
        )
    for same in (False, True):
        scope = "channel" if same else "global"
        for label, gap in (("5m", 5 * MINUTE), ("30m", 30 * MINUTE),
                           ("2h", 2 * HOUR), ("1d", DAY)):
            result.append(
                (f"D window {label} {scope}", "time_window",
                 {"max_gap_seconds": gap, "same_channel": same})
            )
    for label, gap, tokens, count in (
        ("30m 256t 5msg", 30 * MINUTE, 256, 5),
        ("30m 256t 10msg", 30 * MINUTE, 256, 10),
        ("30m 128t 5msg", 30 * MINUTE, 128, 5),
        ("5m 256t 10msg", 5 * MINUTE, 256, 10),
        ("2h 256t 10msg", 2 * HOUR, 256, 10),
        ("2h 512t 20msg", 2 * HOUR, min(512, limit), 20),
    ):
        result.append(
            (f"F hybrid {label}", "hybrid",
             {"max_gap_seconds": gap, "max_tokens": tokens,
              "max_messages": count, "same_channel": True})
        )
    for label, standalone, tokens, gap in (
        ("long>=50 256t 30m", 50, 256, 30 * MINUTE),
        ("long>=30 256t 30m", 30, 256, 30 * MINUTE),
        ("long>=20 128t 30m", 20, 128, 30 * MINUTE),
        ("long>=50 256t 2h", 50, 256, 2 * HOUR),
    ):
        result.append(
            (f"F short-merge {label}", "hybrid_short",
             {"min_standalone_tokens": standalone, "max_tokens": tokens,
              "max_gap_seconds": gap, "same_channel": True})
        )
    return result


# Chunk variants paired with message embeddings in the multi-level experiment.
MULTI_LEVEL = ["F hybrid 30m 256t 10msg", "C tokens 256 overlap 0% channel",
               "D window 30m channel", "F short-merge long>=30 256t 30m"]


@dataclass
class Index:
    """One variant: units, their vectors and owners."""

    name: str
    units: list[tuple[int, ...]]  # message ids of each unit, in order
    owner: np.ndarray  # tg_id of each unit
    tokens: np.ndarray
    vectors: np.ndarray
    encode_seconds: float = 0.0
    by_user: dict[int, np.ndarray] = field(default_factory=dict)

    def __post_init__(self):
        for tg_id in np.unique(self.owner):
            self.by_user[int(tg_id)] = np.flatnonzero(self.owner == tg_id)


class Corpus:
    def __init__(self, comments: list[Comment], encoder: E5Encoder):
        self.comments = comments
        self.encoder = encoder
        self.text = {c.id: c.text for c in comments}
        self.channel = {c.id: c.channel_id for c in comments}
        counts = encoder.count_tokens([c.text for c in comments])
        self.tokens = {c.id: n for c, n in zip(comments, counts)}
        self.by_user: dict[int, list[Comment]] = {}
        for comment in comments:
            self.by_user.setdefault(comment.tg_id, []).append(comment)
        digest = hashlib.sha1()
        for comment in comments:
            digest.update(f"{comment.id}:{comment.text}\n".encode())
        self.fingerprint = digest.hexdigest()[:12]

    def build(self, name: str, strategy: str, params: dict, use_cache: bool) -> Index:
        units, owner = [], []
        for tg_id, comments in self.by_user.items():
            chunks = build_chunks(
                strategy, params,
                [ChunkMessage(c.id, c.channel_id, c.tg_message_id, c.date,
                              self.tokens[c.id]) for c in comments],
            )
            for chunk in chunks:
                units.append(tuple(m.id for m in chunk))
                owner.append(tg_id)
        tokens = np.array([
            sum(self.tokens[m] for m in unit) + len(unit) - 1 for unit in units
        ])
        vectors, seconds = self._embed(name, units, use_cache)
        return Index(name, units, np.array(owner), tokens, vectors, seconds)

    def _embed(self, name, units, use_cache):
        spec = self.encoder.spec
        directory = OUT / f"{spec.name.replace('/', '_')}@{spec.revision[:12]}"
        directory.mkdir(parents=True, exist_ok=True)
        stem = hashlib.sha1(f"{name}:{self.fingerprint}".encode()).hexdigest()[:16]
        vectors_path, meta_path = directory / f"{stem}.npy", directory / f"{stem}.json"
        if use_cache and vectors_path.exists():
            return np.load(vectors_path), json.loads(meta_path.read_text())["seconds"]
        texts = [SEPARATOR.join(self.text[m] for m in unit) for unit in units]
        self.encoder.load()
        started = time.perf_counter()
        vectors = self.encoder.encode_passages(texts)
        seconds = time.perf_counter() - started
        np.save(vectors_path, vectors)
        meta_path.write_text(json.dumps({"name": name, "seconds": seconds}))
        return vectors, seconds


def evaluate(index: Index, queries: list[EvalQuery], query_vectors: np.ndarray,
             corpus: Corpus, ranker=None) -> dict:
    """Mean metrics over the queries; each searches its own user's units."""
    rows = []
    for query, vector in zip(queries, query_vectors):
        if ranker is None:
            candidates = index.by_user[query.tg_id]
            order = candidates[np.argsort(-(index.vectors[candidates] @ vector),
                                          kind="stable")]
            ranked = [index.units[i] for i in order]
            ranked_tokens = index.tokens[order]
            all_units = [index.units[i] for i in candidates]
        else:
            ranked, ranked_tokens, all_units = ranker(query, vector)
        relevant = set(query.relevant)
        top = ranked[:10]
        rows.append({
            "recall@5": recall_at_k(ranked, relevant, 5),
            "recall@10": recall_at_k(ranked, relevant, 10),
            "mrr": reciprocal_rank(ranked, relevant),
            "ndcg@10": ndcg_at_k(ranked, relevant, 10, all_units),
            f"recall@{CONTEXT_BUDGET}tok": recall_within_tokens(
                ranked, relevant, ranked_tokens, CONTEXT_BUDGET),
            "relevant_tokens@5": relevant_token_share(
                ranked, relevant, corpus.tokens, 5),
            "short_in_top10": float(np.mean([
                sum(corpus.tokens[m] for m in unit) < SHORT_TOKENS for unit in top
            ])) if top else 0.0,
        })
    return {key: float(np.mean([row[key] for row in rows])) for key in rows[0]}


def structure(index: Index, corpus: Corpus, message_index: Index, limit: int) -> dict:
    sizes = np.array([len(unit) for unit in index.units])
    dates = {c.id: c.date for c in corpus.comments}
    spans = np.array([
        (dates[unit[-1]] - dates[unit[0]]).total_seconds() for unit in index.units
    ])
    # Coherence: how alike the messages of one unit are, by their own embeddings.
    row_of = {unit[0]: i for i, unit in enumerate(message_index.units)}
    rng = np.random.default_rng(0)
    multi = [u for u in index.units if len(u) > 1]
    sample = [multi[i] for i in rng.permutation(len(multi))[:2000]]
    coherence = []
    for unit in sample:
        vectors = message_index.vectors[[row_of[m] for m in unit[:20]]]
        sims = vectors @ vectors.T
        coherence.append((sims.sum() - len(vectors)) / (len(vectors) * (len(vectors) - 1)))
    return {
        "units": len(index.units),
        "messages_per_unit": float(sizes.mean()),
        "tokens_mean": float(index.tokens.mean()),
        "tokens_p50": float(np.percentile(index.tokens, 50)),
        "tokens_p90": float(np.percentile(index.tokens, 90)),
        "tokens_max": int(index.tokens.max()),
        "truncated_share": float((index.tokens > limit).mean()),
        "under_5_tokens_share": float((index.tokens < SHORT_TOKENS).mean()),
        "span_minutes_p90": float(np.percentile(spans, 90) / 60),
        "cross_channel_share": float(np.mean([
            len({c for c in (corpus.channel[m] for m in unit)}) > 1
            for unit in index.units
        ])),
        "coherence": float(np.mean(coherence)) if coherence else None,
        "embed_seconds": index.encode_seconds,
        "vectors_mb": index.vectors.nbytes / 2**20,
    }


def topic_separation(index: Index, queries: list[EvalQuery]) -> float | None:
    """Silhouette (cosine) of the units holding judged messages, by query topic.

    A proxy for clustering by theme: higher means the units of one topic sit
    closer to each other than to the units of other topics.
    """
    from sklearn.metrics import silhouette_score

    topic_of = {m: q.topic for q in queries for m in q.relevant}
    rows, labels = [], []
    for i, unit in enumerate(index.units):
        topics = [topic_of[m] for m in unit if m in topic_of]
        if topics:
            rows.append(i)
            labels.append(max(set(topics), key=topics.count))
    if len(set(labels)) < 2 or len(rows) <= len(set(labels)):
        return None
    return float(silhouette_score(index.vectors[rows], labels, metric="cosine"))


def multi_level_rankers(message: Index, chunk: Index, alpha: float = 0.5):
    """Two ways of using message and chunk vectors together."""
    chunks_of: dict[int, list[int]] = {}
    for i, unit in enumerate(chunk.units):
        for m in unit:
            chunks_of.setdefault(m, []).append(i)
    row_chunks = [chunks_of[unit[0]] for unit in message.units]

    def fused(query: EvalQuery, vector: np.ndarray):
        # One list of messages and chunks, merged by reciprocal rank; a unit
        # that adds no new message to what is above it is dropped.
        rankings = []
        for offset, index in ((0, message), (len(message.units), chunk)):
            candidates = index.by_user[query.tg_id]
            order = candidates[np.argsort(-(index.vectors[candidates] @ vector),
                                          kind="stable")]
            rankings.append([offset + int(i) for i in order])
        ranked, tokens, seen = [], [], set()
        for unit_id in reciprocal_rank_fusion(rankings):
            index, i = ((message, unit_id) if unit_id < len(message.units)
                        else (chunk, unit_id - len(message.units)))
            if set(index.units[i]) <= seen:
                continue
            seen.update(index.units[i])
            ranked.append(index.units[i])
            tokens.append(index.tokens[i])
        all_units = [message.units[i] for i in message.by_user[query.tg_id]] + [
            chunk.units[i] for i in chunk.by_user[query.tg_id]]
        return ranked, np.array(tokens), all_units

    def contextual(query: EvalQuery, vector: np.ndarray):
        # Messages are returned, scored by their own vector and by the best
        # chunk they belong to: context decides between look-alike replies.
        candidates = message.by_user[query.tg_id]
        own = message.vectors[candidates] @ vector
        chunk_scores = chunk.vectors @ vector
        context = np.array([chunk_scores[row_chunks[i]].max() for i in candidates])
        order = candidates[np.argsort(-(alpha * own + (1 - alpha) * context),
                                      kind="stable")]
        return ([message.units[i] for i in order], message.tokens[order],
                [message.units[i] for i in candidates])

    return {"rrf": fused, "context": contextual}


def postgres_measurements(indexes: list[Index], queries, query_vectors,
                          hnsw: set[str]) -> dict:
    """Storage size and search latency of each variant in pgvector."""
    import psycopg
    from pgvector.psycopg import register_vector
    from sqlalchemy.engine import make_url

    from db.engine import database_url

    url = make_url(database_url())
    connection = psycopg.connect(
        host=url.host, port=url.port, user=url.username, password=url.password,
        dbname=url.database, autocommit=True,
    )
    register_vector(connection)
    cursor = connection.cursor()
    cursor.execute("DROP SCHEMA IF EXISTS bench CASCADE")
    cursor.execute("CREATE SCHEMA bench")
    results: dict[str, dict] = {}

    def latency(sql: str) -> tuple[float, float, list[list[int]]]:
        times, found = [], []
        for repeat in range(3):
            for query, vector in zip(queries, query_vectors):
                started = time.perf_counter()
                cursor.execute(sql, (query.tg_id, vector))
                rows = [row[0] for row in cursor.fetchall()]
                times.append((time.perf_counter() - started) * 1000)
                if repeat == 0:
                    found.append(rows)
        return float(np.percentile(times, 50)), float(np.percentile(times, 95)), found

    try:
        for index in indexes:
            dimensions = index.vectors.shape[1]
            cursor.execute("DROP TABLE IF EXISTS bench.units")
            cursor.execute(
                f"CREATE TABLE bench.units (id bigint PRIMARY KEY, owner bigint "
                f"NOT NULL, embedding vector({dimensions}) NOT NULL)"
            )
            started = time.perf_counter()
            with cursor.copy(
                "COPY bench.units (id, owner, embedding) FROM STDIN WITH (FORMAT BINARY)"
            ) as copy:
                copy.set_types(["int8", "int8", "vector"])
                for i, (owner, vector) in enumerate(zip(index.owner, index.vectors)):
                    copy.write_row((i, int(owner), vector))
            insert_seconds = time.perf_counter() - started
            cursor.execute("CREATE INDEX ON bench.units (owner)")
            cursor.execute("ANALYZE bench.units")
            cursor.execute("SELECT pg_total_relation_size('bench.units')")
            total = cursor.fetchone()[0]
            exact_sql = ("SELECT id FROM bench.units WHERE owner = %s "
                         "ORDER BY embedding <=> %s LIMIT 10")
            p50, p95, exact = latency(exact_sql)
            row = {
                "insert_vectors_per_second": len(index.units) / insert_seconds,
                "table_mb": total / 2**20,
                "exact_p50_ms": p50,
                "exact_p95_ms": p95,
            }
            if index.name == "A message":
                # The three operators on normalized vectors: same order expected.
                for label, op in (("ip", "<#>"), ("l2", "<->")):
                    op_p50, _, found = latency(exact_sql.replace("<=>", op))
                    row[f"{label}_p50_ms"] = op_p50
                    row[f"{label}_same_top10"] = float(np.mean(
                        [a == b for a, b in zip(exact, found)]))
            if index.name in hnsw:
                started = time.perf_counter()
                cursor.execute("CREATE INDEX bench_hnsw ON bench.units "
                               "USING hnsw (embedding vector_cosine_ops)")
                row["hnsw_build_seconds"] = time.perf_counter() - started
                cursor.execute("SELECT pg_relation_size('bench.bench_hnsw')")
                row["hnsw_index_mb"] = cursor.fetchone()[0] / 2**20
                # Filtered by owner: let the index keep scanning until 10 are found.
                cursor.execute("SET hnsw.iterative_scan = strict_order")
                cursor.execute("SET hnsw.ef_search = 100")
                cursor.execute("SET enable_seqscan = off")
                cursor.execute("SET enable_bitmapscan = off")
                cursor.execute("DROP INDEX bench.units_owner_idx")
                p50, p95, approximate = latency(exact_sql)
                cursor.execute("RESET enable_seqscan")
                cursor.execute("RESET enable_bitmapscan")
                row["hnsw_p50_ms"], row["hnsw_p95_ms"] = p50, p95
                row["hnsw_recall@10"] = float(np.mean([
                    len(set(a) & set(b)) / max(1, len(a))
                    for a, b in zip(exact, approximate)
                ]))
            results[index.name] = row
    finally:
        cursor.execute("DROP SCHEMA IF EXISTS bench CASCADE")
        connection.close()
    return results


def compare_models(comments, names: list[str], cache_dir: Path, partial: bool) -> dict:
    """Message-level and one chunked variant per model: quality, speed, memory."""
    import torch

    results = {}
    for name in names:
        torch.cuda.empty_cache()
        torch.cuda.reset_peak_memory_stats()
        encoder = E5Encoder(SPECS[name], cache_dir=cache_dir)
        corpus = Corpus(comments, encoder)
        queries = load_queries(comments, partial=partial)
        query_vectors = encoder.encode_queries([q.query for q in queries])
        row = {"dimensions": encoder.spec.dimensions,
               "revision": encoder.spec.revision,
               "parameters_m": sum(p.numel() for p in encoder._model.parameters()) / 1e6}
        for label, strategy, params in (
            ("message", "message", {}),
            ("hybrid", "hybrid", {"max_gap_seconds": 1800, "max_tokens": 256,
                                  "max_messages": 10, "same_channel": True}),
        ):
            index = corpus.build(f"{label}", strategy, params, use_cache=False)
            metrics = evaluate(index, queries, query_vectors, corpus)
            row[label] = {**metrics, "units": len(index.units),
                          "units_per_second": len(index.units) / index.encode_seconds}
        row["gpu_peak_mb"] = torch.cuda.max_memory_allocated() / 2**20
        results[name] = row
        del encoder, corpus
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="intfloat/multilingual-e5-base")
    parser.add_argument("--cache-dir", default="data/e5-cache")
    parser.add_argument("--no-cache", action="store_true",
                        help="embed again, to measure embedding time")
    parser.add_argument("--postgres", action="store_true")
    parser.add_argument("--sample", type=int, default=None, metavar="N",
                        help="smoke run: only the first N comments of each user")
    parser.add_argument("--only", default=None,
                        help="regex of variant names to run; the message "
                        "baseline is always included")
    parser.add_argument("--compare-models", action="store_true")
    args = parser.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    comments = load_comments(sync_engine())
    if args.sample:
        taken: dict[int, int] = {}
        sampled = []
        for comment in comments:
            taken[comment.tg_id] = taken.get(comment.tg_id, 0) + 1
            if taken[comment.tg_id] <= args.sample:
                sampled.append(comment)
        comments = sampled
    partial = bool(args.sample)
    if args.compare_models:
        results = compare_models(
            comments, list(SPECS), Path(args.cache_dir), partial
        )
        name = "models.sample.json" if partial else "models.json"
        (OUT / name).write_text(json.dumps(results, indent=1))
        print(json.dumps(results, indent=1))
        return

    encoder = E5Encoder(SPECS[args.model], cache_dir=Path(args.cache_dir))
    corpus = Corpus(comments, encoder)
    queries = load_queries(comments, partial=partial)
    query_vectors = encoder.encode_queries([q.query for q in queries])
    limit = encoder.content_token_limit

    indexes: dict[str, Index] = {}
    results: dict[str, dict] = {}
    selected = [
        variant for variant in variants(limit)
        if args.only is None or variant[0] == "A message"
        or re.search(args.only, variant[0])
    ]
    for name, strategy, params in selected:
        index = corpus.build(name, strategy, params, use_cache=not args.no_cache)
        indexes[name] = index
        results[name] = {
            "strategy": strategy, "parameters": params,
            **evaluate(index, queries, query_vectors, corpus),
            **structure(index, corpus, indexes["A message"], limit),
            "topic_silhouette": topic_separation(index, queries),
        }
        print(f"{name}: recall@10 {results[name]['recall@10']:.3f}", flush=True)

    message = indexes["A message"]
    # What is lost and gained by not embedding very short messages at all.
    for minimum in SHORT_MESSAGE_CUTOFFS:
        if args.only is not None:
            break
        name = f"A message, only >= {minimum} tokens"
        keep = np.flatnonzero(message.tokens >= minimum)
        index = Index(name, [message.units[i] for i in keep], message.owner[keep],
                      message.tokens[keep], message.vectors[keep],
                      message.encode_seconds * len(keep) / len(message.units))
        indexes[name] = index
        results[name] = {
            "strategy": "message", "parameters": {"min_tokens": minimum},
            **evaluate(index, queries, query_vectors, corpus),
            **structure(index, corpus, message, limit),
            "topic_silhouette": topic_separation(index, queries),
        }
    for chunk_name in MULTI_LEVEL:
        if chunk_name not in indexes:
            continue
        for label, ranker in multi_level_rankers(message, indexes[chunk_name]).items():
            name = f"G message + [{chunk_name}] {label}"
            both = len(message.units) + len(indexes[chunk_name].units)
            results[name] = {
                "strategy": f"multi_level_{label}", "parameters": {"chunks": chunk_name},
                **evaluate(message, queries, query_vectors, corpus, ranker),
                "units": both,
                "embed_seconds": message.encode_seconds
                + indexes[chunk_name].encode_seconds,
                "vectors_mb": (message.vectors.nbytes
                               + indexes[chunk_name].vectors.nbytes) / 2**20,
            }

    if args.postgres:
        hnsw = {"A message", "F hybrid 30m 256t 10msg", "C tokens 256 overlap 0% channel"}
        measured = postgres_measurements(
            list(indexes.values()), queries, query_vectors, hnsw)
        for name, row in measured.items():
            results[name]["postgres"] = row

    summary = {
        "model": {"name": encoder.spec.name, "revision": encoder.spec.revision,
                  "dimensions": encoder.spec.dimensions, "pooling": encoder.spec.pooling,
                  "normalized": encoder.spec.normalized, "max_tokens": encoder.spec.max_tokens,
                  "content_token_limit": limit},
        "corpus": {"comments": len(comments), "users": len(corpus.by_user),
                   "fingerprint": corpus.fingerprint},
        "queries": len(queries),
        "relevant_messages": sum(len(q.relevant) for q in queries),
        "variants": results,
    }
    name = "results.sample.json" if partial else "results.json"
    (OUT / name).write_text(json.dumps(summary, indent=1, ensure_ascii=False))
    print_table(summary)


def print_table(summary: dict) -> None:
    budget = f"recall@{CONTEXT_BUDGET}tok"
    print("\n| variant | units | msg/unit | tok p50 | tok p90 | R@5 | R@10 | MRR "
          f"| NDCG@10 | R@{CONTEXT_BUDGET}tok | rel.tok@5 | embed s | vectors MB |")
    print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for name, row in summary["variants"].items():
        # Multi-level rows mix two unit sizes: no single size to report.
        sizes = (
            f"{row['messages_per_unit']:.1f} | {row['tokens_p50']:.0f} "
            f"| {row['tokens_p90']:.0f}"
            if "tokens_p50" in row else "– | – | –"
        )
        print(
            f"| {name} | {row['units']} | {sizes} "
            f"| {row['recall@5']:.3f} | {row['recall@10']:.3f} | {row['mrr']:.3f} "
            f"| {row['ndcg@10']:.3f} | {row[budget]:.3f} "
            f"| {row['relevant_tokens@5']:.2f} | {row['embed_seconds']:.1f} "
            f"| {row['vectors_mb']:.0f} |"
        )


if __name__ == "__main__":
    main()
