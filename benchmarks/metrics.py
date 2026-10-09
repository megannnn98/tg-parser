"""Retrieval metrics over ranked units, a unit being the messages one vector stands for.

Relevance is judged per message. A unit of several messages (a chunk) is
credited with the relevant messages it is the first to bring, so strategies
with different chunk sizes are scored on the same judgments.
"""
from __future__ import annotations

import math
from collections.abc import Sequence

Unit = Sequence[int]  # message ids


def _first_covered(ranked: Sequence[Unit], relevant: set[int]) -> list[int]:
    """For each unit, how many relevant messages it is the first to contain."""
    seen: set[int] = set()
    gains = []
    for unit in ranked:
        new = (set(unit) & relevant) - seen
        seen |= new
        gains.append(len(new))
    return gains


def recall_at_k(ranked: Sequence[Unit], relevant: set[int], k: int) -> float:
    """Share of the relevant messages contained in the first k units."""
    return sum(_first_covered(ranked[:k], relevant)) / len(relevant)


def reciprocal_rank(ranked: Sequence[Unit], relevant: set[int]) -> float:
    for rank, unit in enumerate(ranked, start=1):
        if relevant.intersection(unit):
            return 1 / rank
    return 0.0


def ndcg_at_k(
    ranked: Sequence[Unit], relevant: set[int], k: int, all_units: Sequence[Unit]
) -> float:
    """Gain of a unit: the relevant messages it is the first to bring.

    The ideal ranking is built from `all_units`, the units this strategy could
    have returned, taking greedily the one that adds the most.
    """

    def dcg(gains: list[int]) -> float:
        return sum(gain / math.log2(rank + 1) for rank, gain in enumerate(gains, 1))

    candidates = [set(unit) & relevant for unit in all_units]
    candidates = [unit for unit in candidates if unit]
    ideal: list[int] = []
    covered: set[int] = set()
    while candidates and len(ideal) < k:
        best = max(candidates, key=lambda unit: len(unit - covered))
        gain = len(best - covered)
        if gain == 0:
            break
        ideal.append(gain)
        covered |= best
        candidates.remove(best)

    best_dcg = dcg(ideal)
    if best_dcg == 0:
        return 0.0
    return dcg(_first_covered(ranked[:k], relevant)) / best_dcg


def recall_within_tokens(
    ranked: Sequence[Unit],
    relevant: set[int],
    unit_tokens: Sequence[int],
    budget: int,
) -> float:
    """Recall of the leading units that fit into `budget` tokens together.

    The same budget for every strategy: what an LLM context of that size gets.
    """
    used = 0
    taken = 0
    for tokens in unit_tokens:
        if used + tokens > budget:
            break
        used += tokens
        taken += 1
    return recall_at_k(ranked, relevant, taken)


def relevant_token_share(
    ranked: Sequence[Unit],
    relevant: set[int],
    message_tokens: dict[int, int],
    k: int,
) -> float:
    """Of the tokens of the first k units, the share that is relevant messages.

    Low values mean the relevant text arrives diluted by unrelated messages.
    """
    seen: set[int] = set()
    for unit in ranked[:k]:
        seen.update(unit)
    total = sum(message_tokens[m] for m in seen)
    if total == 0:
        return 0.0
    return sum(message_tokens[m] for m in seen & relevant) / total


def reciprocal_rank_fusion(
    rankings: Sequence[Sequence[int]], constant: int = 60
) -> list[int]:
    """Merges rankings of unit indices; ties keep the order of first appearance."""
    scores: dict[int, float] = {}
    for ranking in rankings:
        for rank, unit in enumerate(ranking, start=1):
            scores[unit] = scores.get(unit, 0.0) + 1 / (constant + rank)
    return sorted(scores, key=lambda unit: -scores[unit])
