import math

import pytest

from benchmarks.metrics import (
    ndcg_at_k,
    recall_at_k,
    recall_within_tokens,
    reciprocal_rank,
    reciprocal_rank_fusion,
    relevant_token_share,
)

RELEVANT = {1, 2, 3, 4}


def test_recall_counts_relevant_messages_inside_the_first_k_units():
    ranked = [[9], [1, 2], [8], [3], [4]]

    assert recall_at_k(ranked, RELEVANT, 1) == 0
    assert recall_at_k(ranked, RELEVANT, 2) == 0.5
    assert recall_at_k(ranked, RELEVANT, 4) == 0.75
    assert recall_at_k(ranked, RELEVANT, 10) == 1


def test_recall_does_not_count_a_message_twice_across_overlapping_chunks():
    ranked = [[1, 2], [2, 3], [1, 2]]

    assert recall_at_k(ranked, RELEVANT, 3) == 0.75


def test_reciprocal_rank_is_of_the_first_unit_with_a_relevant_message():
    assert reciprocal_rank([[9], [8, 7], [6, 3]], RELEVANT) == pytest.approx(1 / 3)
    assert reciprocal_rank([[1]], RELEVANT) == 1
    assert reciprocal_rank([[9], [8]], RELEVANT) == 0


def test_ndcg_is_one_for_the_best_order_the_units_allow():
    units = [[1, 2], [3], [4], [9], [8]]

    assert ndcg_at_k(units, RELEVANT, 10, units) == pytest.approx(1)


def test_ndcg_discounts_relevant_units_ranked_lower():
    units = [[1, 2], [3], [4], [9]]
    ranked = [[9], [3], [1, 2], [4]]

    expected = (1 / math.log2(3) + 2 / math.log2(4) + 1 / math.log2(5)) / (
        2 / math.log2(2) + 1 / math.log2(3) + 1 / math.log2(4)
    )
    assert ndcg_at_k(ranked, RELEVANT, 10, units) == pytest.approx(expected)


def test_ndcg_ideal_does_not_reward_overlapping_chunks_twice():
    units = [[1, 2], [2, 3], [1, 2, 3], [4]]

    # The best two units bring 3 and 1 messages, however the others overlap.
    assert ndcg_at_k([[1, 2, 3], [4]], RELEVANT, 2, units) == pytest.approx(1)
    assert ndcg_at_k([[1, 2], [2, 3]], RELEVANT, 2, units) < 1


def test_ndcg_respects_the_cutoff_and_handles_no_relevant_units():
    units = [[9], [1], [2]]

    assert ndcg_at_k(units, RELEVANT, 1, units) == 0
    assert ndcg_at_k([[9]], RELEVANT, 10, [[9], [8]]) == 0


def test_recall_within_tokens_stops_before_the_unit_that_overflows():
    ranked = [[1], [9, 8], [2], [3]]
    tokens = [10, 50, 10, 10]

    assert recall_within_tokens(ranked, RELEVANT, tokens, 10) == 0.25
    # The 50-token unit fits; the next one would overflow 60.
    assert recall_within_tokens(ranked, RELEVANT, tokens, 60) == 0.25
    assert recall_within_tokens(ranked, RELEVANT, tokens, 70) == 0.5
    assert recall_within_tokens(ranked, RELEVANT, tokens, 5) == 0


def test_relevant_token_share_measures_dilution():
    tokens = {1: 10, 2: 10, 8: 30, 9: 50}

    assert relevant_token_share([[1], [2]], RELEVANT, tokens, 2) == 1
    assert relevant_token_share([[1, 9], [2, 8]], RELEVANT, tokens, 2) == 0.2
    assert relevant_token_share([[1, 9], [2, 8]], RELEVANT, tokens, 1) == pytest.approx(
        10 / 60
    )
    assert relevant_token_share([], RELEVANT, tokens, 5) == 0


def test_reciprocal_rank_fusion_prefers_units_high_in_both_rankings():
    fused = reciprocal_rank_fusion([[1, 2, 3], [3, 1, 4]])

    # 1 is first and second, 3 is third and first, 2 and 4 appear once.
    assert fused == [1, 3, 2, 4]
