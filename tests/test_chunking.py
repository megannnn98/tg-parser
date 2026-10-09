from datetime import datetime, timedelta, timezone

import pytest

from chunking.strategies import ChunkMessage, build_chunks, chunk_token_count

START = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _messages(*specs):
    """specs: (minutes from START, channel id, tokens); ids follow the order given."""
    return [
        ChunkMessage(
            id=index,
            channel_id=channel,
            tg_message_id=index,
            date=START + timedelta(minutes=minutes),
            tokens=tokens,
        )
        for index, (minutes, channel, tokens) in enumerate(specs, start=1)
    ]


def _ids(chunks):
    return [[message.id for message in chunk] for chunk in chunks]


def test_message_strategy_is_one_message_per_chunk():
    messages = _messages((0, 1, 3), (1, 1, 900), (2, 2, 1))

    assert _ids(build_chunks("message", {}, messages)) == [[1], [2], [3]]


def test_fixed_messages_on_the_whole_timeline():
    messages = _messages(*[(minute, minute % 2 + 1, 5) for minute in range(7)])

    chunks = build_chunks(
        "fixed_messages", {"max_messages": 3, "same_channel": False}, messages
    )

    # Chronological, whatever the channel; the last chunk takes the remainder.
    assert _ids(chunks) == [[1, 2, 3], [4, 5, 6], [7]]


def test_fixed_messages_within_a_channel_never_mix_channels():
    messages = _messages(*[(minute, minute % 2 + 1, 5) for minute in range(7)])

    chunks = build_chunks(
        "fixed_messages", {"max_messages": 3, "same_channel": True}, messages
    )

    # Channel 1 holds the odd ids, channel 2 the even ones.
    assert _ids(chunks) == [[1, 3, 5], [7], [2, 4, 6]]
    assert all(len({m.channel_id for m in chunk}) == 1 for chunk in chunks)


def test_token_budget_keeps_whole_messages():
    messages = _messages((0, 1, 40), (1, 1, 40), (2, 1, 40), (3, 1, 10), (4, 1, 300))

    chunks = build_chunks(
        "token_budget",
        {"max_tokens": 100, "overlap": 0, "same_channel": False},
        messages,
    )

    # 40 + 40 + separator fits; the third 40 would not. A message larger than
    # the budget is kept whole in a chunk of its own, never cut.
    assert _ids(chunks) == [[1, 2], [3, 4], [5]]
    assert [chunk_token_count(chunk) for chunk in chunks] == [81, 51, 300]


def test_token_budget_counts_the_separator_between_messages():
    messages = _messages((0, 1, 50), (1, 1, 50))

    chunks = build_chunks(
        "token_budget",
        {"max_tokens": 100, "overlap": 0, "same_channel": False},
        messages,
    )

    assert _ids(chunks) == [[1], [2]]


def test_token_budget_overlap_repeats_the_tail_of_the_previous_chunk():
    messages = _messages(*[(minute, 1, 20) for minute in range(9)])

    chunks = build_chunks(
        "token_budget",
        {"max_tokens": 100, "overlap": 0.2, "same_channel": False},
        messages,
    )

    # Four messages of 20 tokens fit with separators; up to 20 tokens carry over.
    assert _ids(chunks) == [[1, 2, 3, 4], [4, 5, 6, 7], [7, 8, 9]]
    seen = [m.id for chunk in chunks for m in chunk]
    assert sorted(set(seen)) == list(range(1, 10))


def test_overlap_never_emits_a_chunk_without_a_new_message():
    messages = _messages((0, 1, 10), (1, 1, 10), (2, 1, 10))

    chunks = build_chunks(
        "token_budget",
        {"max_tokens": 100, "overlap": 0.2, "same_channel": False},
        messages,
    )

    assert _ids(chunks) == [[1, 2, 3]]


def test_overlap_skips_a_tail_that_would_not_fit_with_the_next_message():
    messages = _messages((0, 1, 30), (1, 1, 30), (2, 1, 95))

    chunks = build_chunks(
        "token_budget",
        {"max_tokens": 100, "overlap": 0.5, "same_channel": False},
        messages,
    )

    assert _ids(chunks) == [[1, 2], [3]]


def test_time_window_splits_on_a_gap():
    messages = _messages((0, 1, 5), (4, 2, 5), (8, 1, 5), (14, 1, 5), (15, 1, 5))

    chunks = build_chunks(
        "time_window", {"max_gap_seconds": 300, "same_channel": False}, messages
    )

    # The gap is measured between neighbours, so a chain may span longer.
    assert _ids(chunks) == [[1, 2, 3], [4, 5]]


def test_time_window_gap_equal_to_the_limit_splits():
    messages = _messages((0, 1, 5), (5, 1, 5))

    chunks = build_chunks(
        "time_window", {"max_gap_seconds": 300, "same_channel": False}, messages
    )

    assert _ids(chunks) == [[1], [2]]


def test_time_window_within_a_channel_measures_gaps_inside_the_channel():
    messages = _messages((0, 1, 5), (4, 2, 5), (8, 1, 5), (9, 2, 5))

    chunks = build_chunks(
        "time_window", {"max_gap_seconds": 300, "same_channel": True}, messages
    )

    # In channel 1 the messages are 8 minutes apart; in channel 2, five.
    assert _ids(chunks) == [[1], [3], [2], [4]]


def test_channel_timelines_come_in_channel_order():
    messages = _messages((0, 7, 5), (1, 3, 5), (2, 7, 5))

    chunks = build_chunks(
        "fixed_messages", {"max_messages": 5, "same_channel": True}, messages
    )

    assert _ids(chunks) == [[2], [1, 3]]


def test_time_window_may_cap_tokens():
    messages = _messages((0, 1, 60), (1, 1, 60), (2, 1, 60))

    chunks = build_chunks(
        "time_window",
        {"max_gap_seconds": 300, "same_channel": False, "max_tokens": 130},
        messages,
    )

    assert _ids(chunks) == [[1, 2], [3]]


def test_hybrid_applies_every_limit():
    messages = _messages(
        (0, 1, 10),
        (1, 1, 10),
        (2, 1, 10),  # third message: over max_messages
        (3, 1, 95),  # over max_tokens together with the previous
        (60, 1, 10),  # after a gap
        (61, 2, 10),  # another channel
    )

    chunks = build_chunks(
        "hybrid",
        {
            "max_gap_seconds": 1800,
            "max_tokens": 100,
            "max_messages": 2,
            "same_channel": True,
        },
        messages,
    )

    assert _ids(chunks) == [[1, 2], [3], [4], [5], [6]]


def test_hybrid_short_aggregates_short_messages_and_keeps_long_ones_alone():
    messages = _messages(
        (0, 1, 3),
        (1, 1, 4),
        (2, 1, 60),  # long enough to stand alone
        (3, 1, 2),
        (4, 1, 5),
        (5, 2, 2),
    )

    chunks = build_chunks(
        "hybrid_short",
        {
            "min_standalone_tokens": 50,
            "max_tokens": 100,
            "max_gap_seconds": 1800,
            "same_channel": True,
        },
        messages,
    )

    # The long message also ends the run of short ones around it.
    assert _ids(chunks) == [[1, 2], [3], [4, 5], [6]]


def test_chunks_keep_chronological_order_whatever_the_input_order():
    messages = _messages((0, 1, 5), (1, 1, 5), (2, 1, 5), (3, 1, 5))

    chunks = build_chunks(
        "fixed_messages",
        {"max_messages": 2, "same_channel": False},
        list(reversed(messages)),
    )

    assert _ids(chunks) == [[1, 2], [3, 4]]


def test_messages_of_one_instant_are_ordered_by_channel_then_telegram_id():
    same = [
        ChunkMessage(id=1, channel_id=2, tg_message_id=5, date=START, tokens=1),
        ChunkMessage(id=2, channel_id=1, tg_message_id=9, date=START, tokens=1),
        ChunkMessage(id=3, channel_id=1, tg_message_id=7, date=START, tokens=1),
    ]

    chunks = build_chunks(
        "fixed_messages", {"max_messages": 3, "same_channel": False}, same
    )

    assert _ids(chunks) == [[3, 2, 1]]


def test_building_twice_gives_the_same_chunks():
    messages = _messages(*[(m * 7 % 50, m % 3, (m * 13) % 90 + 1) for m in range(60)])
    params = {
        "max_gap_seconds": 600,
        "max_tokens": 128,
        "max_messages": 5,
        "same_channel": True,
    }

    first = build_chunks("hybrid", params, messages)
    second = build_chunks("hybrid", dict(reversed(params.items())), messages[::-1])

    assert _ids(first) == _ids(second)
    assert sorted(m.id for chunk in first for m in chunk) == list(range(1, 61))


def test_every_message_lands_in_exactly_one_chunk_without_overlap():
    messages = _messages(*[(m, m % 2, m % 40 + 1) for m in range(50)])

    for name, params in [
        ("message", {}),
        ("fixed_messages", {"max_messages": 4, "same_channel": True}),
        ("token_budget", {"max_tokens": 64, "overlap": 0, "same_channel": True}),
        ("time_window", {"max_gap_seconds": 90, "same_channel": False}),
        (
            "hybrid_short",
            {
                "min_standalone_tokens": 30,
                "max_tokens": 64,
                "max_gap_seconds": 600,
                "same_channel": True,
            },
        ),
    ]:
        chunks = build_chunks(name, params, messages)
        assert sorted(m.id for chunk in chunks for m in chunk) == list(range(1, 51)), name


def test_no_messages_give_no_chunks():
    assert build_chunks("message", {}, []) == []


@pytest.mark.parametrize(
    ("name", "params"),
    [
        ("unknown", {}),
        ("message", {"max_tokens": 10}),
        ("fixed_messages", {"same_channel": True}),
        ("fixed_messages", {"max_messages": 0, "same_channel": True}),
        ("token_budget", {"max_tokens": 100, "overlap": 1, "same_channel": True}),
        ("token_budget", {"max_tokens": 100, "overlap": 0}),
        ("time_window", {"max_gap_seconds": 60, "same_channel": True, "typo": 1}),
    ],
)
def test_unknown_strategy_or_parameters_are_rejected(name, params):
    with pytest.raises(ValueError):
        build_chunks(name, params, _messages((0, 1, 5)))
