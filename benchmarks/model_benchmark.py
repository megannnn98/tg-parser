"""Compares local embedding models on the same comments, queries and chunks.

    python -m benchmarks.model_benchmark               # every model, then the report
    python -m benchmarks.model_benchmark --model NAME  # one model
    python -m benchmarks.model_benchmark --report      # table from the stored runs
    python -m benchmarks.model_benchmark --short-candidates
    python -m benchmarks.model_benchmark --short-check

Only the model changes. The chunking is fixed to the message level and one
chunked variant; both are built once with the tokenizer of the E5 base model,
so every model embeds exactly the same texts. Search is exact and in memory:
no vector index, no database table, so models of any dimensionality are
compared without touching the schema. Each model is used as its own card
prescribes: prefix or instruction, pooling, normalization, input limit.

Every model runs in a process of its own, so memory figures do not carry over
and one model that fails to load does not stop the rest. Qwen3 needs
transformers >= 4.51. Results: data/benchmark/model_runs/ and
data/benchmark/model_benchmark.json.
"""
from __future__ import annotations

import argparse
import json
import resource
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

import numpy as np

from benchmarks.chunking_benchmark import OUT, Corpus, evaluate_queries
from benchmarks.corpus import load_comments, sync_engine
from benchmarks.eval_dataset import load_queries
from embeddings.e5 import MULTILINGUAL_E5_BASE, MULTILINGUAL_E5_SMALL, E5Encoder

RUNS = OUT / "model_runs"
SHORT_CASES = OUT / "short_cases.local.json"

# Qwen3 asks for a one-sentence English description of the task before the query.
QWEN_TASK = "Given a topic, retrieve comments that discuss it"


@dataclass(frozen=True)
class ModelSpec:
    name: str
    revision: str
    dimensions: int
    pooling: str  # "mean" | "cls" | "last"
    max_tokens: int
    passage_prefix: str = ""
    query_prefix: str = ""
    padding_side: str = "right"
    normalized: bool = True


MODELS = [
    ModelSpec(
        MULTILINGUAL_E5_SMALL.name, MULTILINGUAL_E5_SMALL.revision, 384,
        pooling="mean", max_tokens=512,
        passage_prefix="passage: ", query_prefix="query: ",
    ),
    ModelSpec(
        MULTILINGUAL_E5_BASE.name, MULTILINGUAL_E5_BASE.revision, 768,
        pooling="mean", max_tokens=512,
        passage_prefix="passage: ", query_prefix="query: ",
    ),
    # Dense vector of BGE-M3: the [CLS] token, no instruction for queries.
    ModelSpec(
        "BAAI/bge-m3", "5617a9f61b028005a4858fdac845db406aefb181", 1024,
        pooling="cls", max_tokens=8192,
    ),
    # Last token with left padding; documents carry no instruction.
    ModelSpec(
        "Qwen/Qwen3-Embedding-0.6B", "97b0c614be4d77ee51c0cef4e5f07c00f9eb65b3", 1024,
        pooling="last", max_tokens=8192, padding_side="left",
        query_prefix=f"Instruct: {QWEN_TASK}\nQuery:",
    ),
]
BASELINE = MULTILINGUAL_E5_BASE.name

MESSAGE = ("A message", "message", {})
CHUNK = ("C tokens 256 overlap 0% channel", "token_budget",
         {"max_tokens": 256, "overlap": 0, "same_channel": True})
VARIANTS = [MESSAGE, CHUNK]
METRICS = ("recall@5", "recall@10", "mrr", "ndcg@10")

# Padded tokens per batch: keeps long inputs from exhausting the GPU.
BATCH_TOKENS = 20_000
BATCH_TEXTS = 64


class ModelEncoder:
    """Any encoder-style embedding model, with the interface of E5Encoder."""

    def __init__(self, spec: ModelSpec, count_tokens, cache_dir: Path):
        self.spec = spec
        # Token counts come from one shared tokenizer, so chunks do not depend
        # on the model being measured.
        self.count_tokens = count_tokens
        self._cache_dir = cache_dir
        self._tokenizer = None
        self._model = None

    def load(self) -> None:
        if self._model is not None:
            return
        import torch
        from transformers import AutoModel, AutoTokenizer

        kwargs = dict(revision=self.spec.revision, trust_remote_code=False,
                      cache_dir=self._cache_dir)
        self._tokenizer = AutoTokenizer.from_pretrained(
            self.spec.name, padding_side=self.spec.padding_side, **kwargs
        )
        self._model = AutoModel.from_pretrained(self.spec.name, **kwargs).eval().to(
            "cuda" if torch.cuda.is_available() else "cpu"
        )

    def encode_passages(self, texts: list[str]) -> np.ndarray:
        return self._encode([self.spec.passage_prefix + text for text in texts])

    def encode_queries(self, texts: list[str]) -> np.ndarray:
        return self._encode([self.spec.query_prefix + text for text in texts])

    def _encode(self, texts: list[str]) -> np.ndarray:
        import torch

        self.load()
        result = np.empty((len(texts), self.spec.dimensions), dtype=np.float32)
        encoded = self._tokenizer(
            texts, truncation=True, max_length=self.spec.max_tokens
        )["input_ids"]
        order = sorted(range(len(texts)), key=lambda i: len(encoded[i]))
        start = 0
        while start < len(order):
            # Sorted by length: the last text of a batch is its longest.
            end = start + 1
            while (
                end < len(order)
                and end - start < BATCH_TEXTS
                and (end - start + 1) * len(encoded[order[end]]) <= BATCH_TOKENS
            ):
                end += 1
            rows = order[start:end]
            inputs = self._tokenizer.pad(
                {"input_ids": [encoded[i] for i in rows]}, return_tensors="pt"
            ).to(self._model.device)
            with torch.inference_mode():
                hidden = self._model(**inputs).last_hidden_state
                mask = inputs.attention_mask
                if self.spec.pooling == "mean":
                    pooled = (hidden * mask[..., None]).sum(1) / mask.sum(1, keepdim=True)
                elif self.spec.pooling == "cls":
                    pooled = hidden[:, 0]
                elif self.spec.padding_side == "left":
                    pooled = hidden[:, -1]
                else:
                    last = mask.sum(1) - 1
                    pooled = hidden[torch.arange(len(rows)), last]
                if self.spec.normalized:
                    pooled = torch.nn.functional.normalize(pooled, p=2, dim=1)
            result[rows] = pooled.float().cpu().numpy()
            start = end
        return result


def _spec(name: str) -> ModelSpec:
    for spec in MODELS:
        if spec.name == name:
            return spec
    raise SystemExit(f"Unknown model {name}; known: {[s.name for s in MODELS]}")


def _run_path(name: str) -> Path:
    return RUNS / (name.replace("/", "_") + ".json")


def _shared_counter(cache_dir: Path):
    return E5Encoder(MULTILINGUAL_E5_BASE, cache_dir=cache_dir).count_tokens


def run_model(name: str, cache_dir: Path, use_cache: bool) -> None:
    import torch

    spec = _spec(name)
    comments = load_comments(sync_engine())
    queries = load_queries(comments)
    encoder = ModelEncoder(spec, _shared_counter(cache_dir), cache_dir)
    started = time.perf_counter()
    encoder.load()
    load_seconds = time.perf_counter() - started
    torch.cuda.reset_peak_memory_stats()
    corpus = Corpus(comments, encoder)
    query_vectors = encoder.encode_queries([q.query for q in queries])
    result = {
        "spec": asdict(spec),
        "precision": str(next(encoder._model.parameters()).dtype).replace("torch.", ""),
        "parameters_m": sum(p.numel() for p in encoder._model.parameters()) / 1e6,
        "load_seconds": load_seconds,
        "comments": len(comments),
        "queries": [q.id for q in queries],
        "variants": {},
    }
    for variant, strategy, params in VARIANTS:
        index = corpus.build(variant, strategy, params, use_cache=use_cache)
        rows = evaluate_queries(index, queries, query_vectors, corpus)
        result["variants"][variant] = {
            "units": len(index.units),
            "embed_seconds": index.encode_seconds,
            "units_per_second": len(index.units) / index.encode_seconds,
            "per_query": rows,
            **{k: float(np.mean([row[k] for row in rows])) for k in rows[0]},
        }
    result["gpu_peak_mb"] = torch.cuda.max_memory_allocated() / 2**20
    # Peak resident memory of this process, the model and the corpus included.
    result["ram_peak_mb"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    RUNS.mkdir(parents=True, exist_ok=True)
    _run_path(name).write_text(json.dumps(result, indent=1, ensure_ascii=False))
    print(name, {v: round(m["mrr"], 3) for v, m in result["variants"].items()})


def _bootstrap(delta: np.ndarray, rounds: int = 10_000) -> tuple[float, float]:
    """95% interval of the mean per-query difference, resampling the queries."""
    rng = np.random.default_rng(0)
    samples = rng.integers(0, len(delta), size=(rounds, len(delta)))
    means = delta[samples].mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def report() -> None:
    runs = {}
    failed = {}
    for spec in MODELS:
        path = _run_path(spec.name)
        error = path.with_suffix(".error.txt")
        if path.exists():
            runs[spec.name] = json.loads(path.read_text())
        elif error.exists():
            failed[spec.name] = error.read_text().strip().splitlines()[-1]
    baseline = runs[BASELINE]
    for name, run in runs.items():
        run["difference_to_baseline"] = {}
        for variant, _, _ in VARIANTS:
            ours = run["variants"][variant]["per_query"]
            theirs = baseline["variants"][variant]["per_query"]
            run["difference_to_baseline"][variant] = {}
            for metric in METRICS:
                delta = np.array([a[metric] - b[metric] for a, b in zip(ours, theirs)])
                low, high = _bootstrap(delta)
                run["difference_to_baseline"][variant][metric] = {
                    "mean": float(delta.mean()), "low": low, "high": high,
                }
    (OUT / "model_benchmark.json").write_text(
        json.dumps({"baseline": BASELINE, "models": runs, "failed": failed},
                   indent=1, ensure_ascii=False)
    )

    print("| Model | Strategy | Dim | R@5 | R@10 | MRR | NDCG@10 | Emb/s | VRAM |")
    print("|---|---|---|---|---|---|---|---|---|")
    for name, run in runs.items():
        for variant, _, _ in VARIANTS:
            m = run["variants"][variant]
            print(f"| {name} | {variant} | {run['spec']['dimensions']} "
                  f"| {m['recall@5']:.3f} | {m['recall@10']:.3f} | {m['mrr']:.3f} "
                  f"| {m['ndcg@10']:.3f} | {m['units_per_second']:.0f} "
                  f"| {run['gpu_peak_mb']:.0f} MB |")
    print(f"\nDifference to {BASELINE}, mean over queries [95% bootstrap interval]:")
    for name, run in runs.items():
        if name == BASELINE:
            continue
        for variant, _, _ in VARIANTS:
            cells = []
            for metric in METRICS:
                d = run["difference_to_baseline"][variant][metric]
                cells.append(f"{metric} {d['mean']:+.3f} [{d['low']:+.3f}, {d['high']:+.3f}]")
            print(f"  {name} | {variant}: " + "; ".join(cells))
    for name, reason in failed.items():
        print(f"\nFAILED {name}: {reason}")


def run_all(args) -> None:
    RUNS.mkdir(parents=True, exist_ok=True)
    for spec in MODELS:
        command = [sys.executable, "-m", "benchmarks.model_benchmark",
                   "--model", spec.name, "--cache-dir", args.cache_dir]
        if args.no_cache:
            command.append("--no-cache")
        done = subprocess.run(command, capture_output=True, text=True)
        error = _run_path(spec.name).with_suffix(".error.txt")
        if done.returncode == 0:
            error.unlink(missing_ok=True)
            print(done.stdout.strip().splitlines()[-1], flush=True)
        else:
            # Recorded and skipped: the other models still run.
            _run_path(spec.name).unlink(missing_ok=True)
            error.write_text(done.stderr[-4000:])
            print(f"{spec.name}: failed, see {error}", flush=True)
    report()


def _chat_profile(comments):
    by_user: dict[int, list] = {}
    for comment in comments:
        by_user.setdefault(comment.tg_id, []).append(comment)
    # The profile with the most comments is the chat-style one.
    return max(by_user.values(), key=len)


def short_candidates(args) -> None:
    """Prints short replies with the chunk around them, to choose cases by hand.

    The output is real comments and stays on this machine.
    """
    cache_dir = Path(args.cache_dir)
    comments = load_comments(sync_engine())
    encoder = E5Encoder(MULTILINGUAL_E5_BASE, cache_dir=cache_dir)
    corpus = Corpus(comments, encoder)
    index = corpus.build(*CHUNK, use_cache=True)
    wanted = ("да", "ага", "согласен", "бред", "точно", "нет", "+")
    by_id = {c.id: c for c in comments}
    user_ids = {c.id for c in _chat_profile(comments)}
    shown = dict.fromkeys(wanted, 0)
    other = 0
    for unit in index.units:
        if unit[0] not in user_ids or not 4 <= len(unit) <= 14:
            continue
        for message_id in unit:
            text = by_id[message_id].text.strip().lower().rstrip(".!")
            exact = text in wanted and shown[text] < args.per_text
            contextual = (text not in wanted and corpus.tokens[message_id] <= 10
                          and other < args.other and message_id % 97 == 0)
            if not (exact or contextual):
                continue
            if exact:
                shown[text] += 1
            else:
                other += 1
            comment = by_id[message_id]
            print(f"\n### {comment.channel}:{comment.tg_message_id} "
                  f"[{corpus.tokens[message_id]} tokens] {comment.text!r}")
            for member in unit:
                mark = ">>" if member == message_id else "  "
                print(f"  {mark} {' '.join(by_id[member].text.split())[:150]}")
            break


def short_check(args) -> None:
    """Rank of a short reply alone and of the chunk that holds it, per model.

    Cases come from a local file, [{"key": "channel:message_id", "query": "..."}]:
    the query names the topic that the surrounding chunk is about.
    """
    cache_dir = Path(args.cache_dir)
    cases = json.loads(SHORT_CASES.read_text())
    comments = load_comments(sync_engine())
    by_key = {f"{c.channel}:{c.tg_message_id}": c for c in comments}
    counter = _shared_counter(cache_dir)
    summary = {}
    for spec in MODELS:
        encoder = ModelEncoder(spec, counter, cache_dir)
        corpus = Corpus(comments, encoder)
        messages = corpus.build(*MESSAGE, use_cache=True)
        chunks = corpus.build(*CHUNK, use_cache=True)
        query_vectors = encoder.encode_queries([case["query"] for case in cases])
        rows = []
        for case, vector in zip(cases, query_vectors):
            comment = by_key[case["key"]]
            own = messages.by_user[comment.tg_id]
            scores = messages.vectors[own] @ vector
            at = next(n for n, i in enumerate(own) if messages.units[i][0] == comment.id)
            message_rank = int((scores > scores[at]).sum()) + 1
            chunk_rows = chunks.by_user[comment.tg_id]
            chunk_scores = chunks.vectors[chunk_rows] @ vector
            holder = next(n for n, i in enumerate(chunk_rows)
                          if comment.id in chunks.units[i])
            chunk_rank = int((chunk_scores > chunk_scores[holder]).sum()) + 1
            rows.append({
                "tokens": corpus.tokens[comment.id],
                "message_rank": message_rank, "of_messages": len(own),
                "chunk_rank": chunk_rank, "of_chunks": len(chunk_rows),
            })
        summary[spec.name] = rows
        print(f"\n{spec.name}")
        for case, row in zip(cases, rows):
            print(f"  [{row['tokens']:>2} tok] alone: rank {row['message_rank']:>5} of "
                  f"{row['of_messages']}; in its chunk: rank {row['chunk_rank']:>4} of "
                  f"{row['of_chunks']}   ({case['query']})")
        del encoder, corpus
    (OUT / "short_check.json").write_text(json.dumps(summary, indent=1))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--cache-dir", default="data/e5-cache")
    parser.add_argument("--no-cache", action="store_true",
                        help="embed again, to measure embedding time")
    parser.add_argument("--model", default=None)
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--short-candidates", action="store_true")
    parser.add_argument("--short-check", action="store_true")
    parser.add_argument("--per-text", type=int, default=2)
    parser.add_argument("--other", type=int, default=12)
    arguments = parser.parse_args()
    if arguments.model:
        run_model(arguments.model, Path(arguments.cache_dir), not arguments.no_cache)
    elif arguments.report:
        report()
    elif arguments.short_candidates:
        short_candidates(arguments)
    elif arguments.short_check:
        short_check(arguments)
    else:
        run_all(arguments)
