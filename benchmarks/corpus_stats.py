"""Size distribution of the stored comments, as Markdown.

    python -m benchmarks.corpus_stats [--model intfloat/multilingual-e5-base]

Prints no comment text except the most frequent very short messages, which
are generic replies ("да", "+") rather than anyone's statement.
"""
from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

import numpy as np

from benchmarks.corpus import load_comments, sync_engine
from embeddings.e5 import SPECS, E5Encoder

BUCKETS = [
    ("<5", 0, 5),
    ("5–9", 5, 10),
    ("10–19", 10, 20),
    ("20–49", 20, 50),
    ("50–127", 50, 128),
    ("128–255", 128, 256),
    ("256–511", 256, 512),
    ("≥512", 512, 10**9),
]


def _row(name: str, values: np.ndarray) -> str:
    q = np.percentile(values, [50, 75, 90, 95, 99])
    return (
        f"| {name} | {q[0]:.0f} | {values.mean():.1f} | {q[1]:.0f} | {q[2]:.0f} "
        f"| {q[3]:.0f} | {q[4]:.0f} | {values.max()} |"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="intfloat/multilingual-e5-base")
    parser.add_argument("--cache-dir", default="data/e5-cache")
    args = parser.parse_args()

    comments = load_comments(sync_engine())
    everyone = load_comments(sync_engine(), profiles_only=False)
    encoder = E5Encoder(SPECS[args.model], cache_dir=Path(args.cache_dir))
    texts = [c.text for c in comments]
    chars = np.array([len(t) for t in texts])
    words = np.array([len(t.split()) for t in texts])
    tokens = np.array(encoder.count_tokens(texts))

    print(f"Comments of collected profiles: {len(comments)} "
          f"({len({c.tg_id for c in comments})} users, "
          f"{len({c.channel_id for c in comments})} channels); "
          f"all stored comments: {len(everyone)}.")
    print(f"Tokenizer: {args.model}; content limit per input: "
          f"{encoder.content_token_limit} tokens.\n")
    print("| per message | median | mean | p75 | p90 | p95 | p99 | max |")
    print("|---|---|---|---|---|---|---|---|")
    print(_row("characters", chars))
    print(_row("words", words))
    print(_row("tokens", tokens))

    print("\n| tokens | messages | share | cumulative | share of all tokens |")
    print("|---|---|---|---|---|")
    cumulative = 0.0
    for name, low, high in BUCKETS:
        mask = (tokens >= low) & (tokens < high)
        share = mask.mean() * 100
        cumulative += share
        print(f"| {name} | {mask.sum()} | {share:.1f}% | {cumulative:.1f}% "
              f"| {tokens[mask].sum() / tokens.sum() * 100:.1f}% |")
    over = tokens > encoder.content_token_limit
    print(f"\nLonger than one model input: {over.sum()} messages "
          f"({over.mean() * 100:.2f}%).")

    print("\n| user | messages | channels | median tokens | <5 tokens |")
    print("|---|---|---|---|---|")
    by_user: dict[int, list[int]] = {}
    for index, comment in enumerate(comments):
        by_user.setdefault(comment.tg_id, []).append(index)
    ranked = sorted(by_user.values(), key=len, reverse=True)
    for number, rows in enumerate(ranked, start=1):
        user_tokens = tokens[rows]
        channels = len({comments[i].channel_id for i in rows})
        print(f"| user {number:02d} | {len(rows)} | {channels} "
              f"| {np.median(user_tokens):.0f} | {(user_tokens < 5).mean() * 100:.0f}% |")

    short = Counter(
        text.strip().lower() for text, count in zip(texts, tokens) if count < 5
    )
    print(f"\nMessages under 5 tokens: {sum(short.values())}, "
          f"distinct texts: {len(short)}. Most frequent:\n")
    print("| text | count |")
    print("|---|---|")
    for text, count in short.most_common(40):
        print(f"| `{text}` | {count} |")

    gaps = []
    same_channel_gaps = []
    for rows in by_user.values():
        for a, b in zip(rows, rows[1:]):
            gaps.append((comments[b].date - comments[a].date).total_seconds())
        last: dict[int, int] = {}
        for i in rows:
            channel = comments[i].channel_id
            if channel in last:
                same_channel_gaps.append(
                    (comments[i].date - comments[last[channel]].date).total_seconds()
                )
            last[channel] = i
    print("\n| gap to the user's previous message | <5 min | <30 min | <2 h | <1 day |")
    print("|---|---|---|---|---|")
    for name, values in (("any channel", gaps), ("same channel", same_channel_gaps)):
        values = np.array(values)
        print(f"| {name} | " + " | ".join(
            f"{(values < limit).mean() * 100:.0f}%" for limit in (300, 1800, 7200, 86400)
        ) + " |")


if __name__ == "__main__":
    main()
