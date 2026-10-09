from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select

from chunking.builder import build_chunk_set
from db import chunk_repository as chunk_repo
from db import repositories as repo
from db.models import Chunk, ChunkMessageLink, ChunkSet, Message

START = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _words(texts: list[str]) -> list[int]:
    return [len(text.split()) for text in texts]


async def _seed(sessions, comments, tg_id=1):
    """comments: (channel, Telegram message id, minutes from START, text)."""
    async with sessions.begin() as session:
        user_id = await repo.upsert_user(session, tg_id, f"user{tg_id}")
        rows = []
        for channel, tg_message_id, minutes, text in comments:
            rows.append(
                {
                    "tg_message_id": tg_message_id,
                    "user_id": user_id,
                    "channel_id": await repo.upsert_channel(session, channel),
                    "text": text,
                    "date": START + timedelta(minutes=minutes),
                }
            )
        await repo.insert_messages(session, rows)
    return user_id


async def _stored(sessions, chunk_set_id, user_id):
    """[(chunk text, token count, channel set?, [message texts in order])]."""
    async with sessions() as session:
        texts = dict(
            (await session.execute(select(Message.id, Message.text))).tuples().all()
        )
        chunks = await chunk_repo.chunks_of_user(session, chunk_set_id, user_id)
    return [
        (
            chunk.text,
            chunk.token_count,
            chunk.channel_id is not None,
            [texts[i] for i in ids],
        )
        for chunk, ids in chunks
    ]


COMMENTS = [
    ("news", 1, 0, "one two"),
    ("tech", 1, 1, "three"),
    ("news", 2, 2, "four five six"),
    ("news", 3, 3, "seven"),
]


def test_chunks_keep_their_messages_in_order_through_position(run_db):
    async def scenario(sessions):
        user_id = await _seed(sessions, COMMENTS)
        result = await build_chunk_set(
            sessions,
            "fixed_messages",
            {"max_messages": 3, "same_channel": False},
            "words",
            _words,
        )
        async with sessions() as session:
            positions = (
                await session.execute(
                    select(ChunkMessageLink.position, Message.text)
                    .join(Message, Message.id == ChunkMessageLink.message_id)
                    .order_by(ChunkMessageLink.chunk_id, ChunkMessageLink.position)
                )
            ).all()
            chunk = (
                await session.scalars(select(Chunk).order_by(Chunk.date_from))
            ).first()
        return result, await _stored(sessions, result.chunk_set_id, user_id), [
            tuple(p) for p in positions
        ], chunk

    result, stored, positions, chunk = run_db(scenario)

    assert stored == [
        # Spans two channels, so the chunk belongs to none.
        ("one two\nthree\nfour five six", 8, False, ["one two", "three", "four five six"]),
        ("seven", 1, True, ["seven"]),
    ]
    assert positions == [
        (0, "one two"),
        (1, "three"),
        (2, "four five six"),
        (0, "seven"),
    ]
    assert (chunk.date_from, chunk.date_to) == (START, START + timedelta(minutes=2))
    assert chunk.message_count == 3
    assert (result.users_rebuilt, result.chunks_written) == (1, 2)


def test_channel_boundary_is_kept_in_the_database(run_db):
    async def scenario(sessions):
        user_id = await _seed(sessions, COMMENTS)
        result = await build_chunk_set(
            sessions,
            "fixed_messages",
            {"max_messages": 3, "same_channel": True},
            "words",
            _words,
        )
        return await _stored(sessions, result.chunk_set_id, user_id)

    stored = run_db(scenario)

    assert sorted(texts for _, _, _, texts in stored) == [
        ["one two", "four five six", "seven"],
        ["three"],
    ]
    assert all(has_channel for _, _, has_channel, _ in stored)


def test_overlap_links_a_message_to_several_chunks(run_db):
    async def scenario(sessions):
        user_id = await _seed(
            sessions, [("news", n, n, f"w{n} x y z") for n in range(1, 6)]
        )
        result = await build_chunk_set(
            sessions,
            "token_budget",
            {"max_tokens": 10, "overlap": 0.5, "same_channel": True},
            "words",
            _words,
        )
        async with sessions() as session:
            links = await session.scalar(select(func.count()).select_from(ChunkMessageLink))
        return await _stored(sessions, result.chunk_set_id, user_id), links

    stored, links = run_db(scenario)

    # Two messages of four words fit a budget of ten; one carries over.
    assert [texts for _, _, _, texts in stored] == [
        ["w1 x y z", "w2 x y z"],
        ["w2 x y z", "w3 x y z"],
        ["w3 x y z", "w4 x y z"],
        ["w4 x y z", "w5 x y z"],
    ]
    assert links == 8


def test_strategies_and_parameters_are_stored_side_by_side(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        ids = []
        for strategy, parameters in [
            ("message", {}),
            ("fixed_messages", {"max_messages": 2, "same_channel": True}),
            ("fixed_messages", {"max_messages": 3, "same_channel": True}),
            # The same parameters in another key order are the same set.
            ("fixed_messages", {"same_channel": True, "max_messages": 3}),
        ]:
            result = await build_chunk_set(sessions, strategy, parameters, "words", _words)
            ids.append(result.chunk_set_id)
        other_tokenizer = await build_chunk_set(sessions, "message", {}, "chars", _words)
        async with sessions() as session:
            sets = await chunk_repo.find_chunk_sets(session)
            fixed = await chunk_repo.find_chunk_sets(session, "fixed_messages")
            per_set = dict(
                (
                    await session.execute(
                        select(Chunk.chunk_set_id, func.count()).group_by(
                            Chunk.chunk_set_id
                        )
                    )
                )
                .tuples()
                .all()
            )
        return ids, other_tokenizer.chunk_set_id, sets, fixed, per_set

    ids, other_tokenizer, sets, fixed, per_set = run_db(scenario)

    assert ids[2] == ids[3]
    assert len({*ids, other_tokenizer}) == 4
    assert [(s.strategy, s.strategy_version, s.parameters) for s in fixed] == [
        ("fixed_messages", 1, {"max_messages": 2, "same_channel": True}),
        ("fixed_messages", 1, {"max_messages": 3, "same_channel": True}),
    ]
    assert len(sets) == 4
    assert per_set == {ids[0]: 4, ids[1]: 3, ids[2]: 2, other_tokenizer: 4}


def test_rebuilding_is_reproducible_and_only_touches_users_with_new_messages(run_db):
    params = {"max_messages": 2, "same_channel": False}

    async def scenario(sessions):
        first_user = await _seed(sessions, COMMENTS)
        second_user = await _seed(sessions, [("news", 50, 0, "other")], tg_id=2)
        first = await build_chunk_set(sessions, "fixed_messages", params, "words", _words)
        before = await _stored(sessions, first.chunk_set_id, first_user)
        async with sessions() as session:
            second_user_chunk = await session.scalar(
                select(Chunk.id).where(Chunk.user_id == second_user)
            )

        again = await build_chunk_set(sessions, "fixed_messages", params, "words", _words)
        unchanged = await _stored(sessions, first.chunk_set_id, first_user)

        await _seed(sessions, [("news", 4, 9, "eight")])
        grown = await build_chunk_set(sessions, "fixed_messages", params, "words", _words)
        after = await _stored(sessions, first.chunk_set_id, first_user)
        async with sessions() as session:
            second_user_chunk_after = await session.scalar(
                select(Chunk.id).where(Chunk.user_id == second_user)
            )

        forced = await build_chunk_set(
            sessions, "fixed_messages", params, "words", _words, force=True
        )
        forced_state = await _stored(sessions, first.chunk_set_id, first_user)
        return (
            first,
            before,
            again,
            unchanged,
            grown,
            after,
            second_user_chunk == second_user_chunk_after,
            forced,
            forced_state,
        )

    first, before, again, unchanged, grown, after, kept, forced, forced_state = run_db(
        scenario
    )

    assert (first.users_rebuilt, first.chunks_written) == (2, 3)
    assert (again.users_rebuilt, again.chunks_written) == (0, 0)
    assert unchanged == before
    # Only the user with the new message is rebuilt; the other keeps its rows.
    assert (grown.users_rebuilt, grown.chunks_written) == (1, 3)
    assert kept
    assert [texts for _, _, _, texts in after] == [
        ["one two", "three"],
        ["four five six", "seven"],
        ["eight"],
    ]
    assert (forced.users_rebuilt, forced.chunks_written) == (2, 4)
    assert forced_state == after


def test_messages_are_untouched_by_chunking(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        async with sessions() as session:
            before = (
                await session.execute(
                    select(Message.id, Message.text, Message.date).order_by(Message.id)
                )
            ).all()
        await build_chunk_set(sessions, "message", {}, "words", _words)
        await build_chunk_set(sessions, "message", {}, "words", _words, force=True)
        async with sessions() as session:
            after = (
                await session.execute(
                    select(Message.id, Message.text, Message.date).order_by(Message.id)
                )
            ).all()
            chunk_sets = await session.scalar(select(func.count()).select_from(ChunkSet))
        return before, after, chunk_sets

    before, after, chunk_sets = run_db(scenario)

    assert after == before
    assert chunk_sets == 1


def test_unknown_parameters_write_nothing(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        with pytest.raises(ValueError):
            await build_chunk_set(
                sessions, "fixed_messages", {"max_messages": 2}, "words", _words
            )
        async with sessions() as session:
            return await session.scalar(select(func.count()).select_from(ChunkSet))

    assert run_db(scenario) == 0
