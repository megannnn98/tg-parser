import zlib
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from sqlalchemy import func, select, text

from chunking.builder import build_chunk_set
from db import embedding_repository as embedding_repo
from db import repositories as repo
from db.embedding_models import (
    EMBEDDING_DIMENSIONS,
    ChunkEmbedding,
    EmbeddingModel,
    MessageEmbedding,
)
from db.models import Chunk, Message
from embeddings.e5 import E5Spec
from embeddings.pipeline import embed_chunks, embed_messages

START = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
SPEC = E5Spec(
    name="test/bag-of-words", revision="rev-1", dimensions=EMBEDDING_DIMENSIONS
)


def _vector(text_: str) -> np.ndarray:
    """A normalized bag of words: texts sharing words are close."""
    vector = np.zeros(EMBEDDING_DIMENSIONS, dtype=np.float32)
    for word in text_.lower().split():
        vector[zlib.crc32(word.encode()) % EMBEDDING_DIMENSIONS] += 1
    return vector / np.linalg.norm(vector)


class FakeEncoder:
    def __init__(self, spec: E5Spec = SPEC):
        self.spec = spec
        self.encoded: list[str] = []

    def encode_passages(self, texts):
        self.encoded.extend(texts)
        return np.stack([_vector(t) for t in texts])


def _words(texts):
    return [len(t.split()) for t in texts]


async def _seed(sessions, comments, tg_id=1):
    """comments: (Telegram message id, text); all in the channel "news"."""
    async with sessions.begin() as session:
        user_id = await repo.upsert_user(session, tg_id, f"user{tg_id}")
        channel_id = await repo.upsert_channel(session, "news")
        await repo.insert_messages(
            session,
            [
                {
                    "tg_message_id": tg_message_id,
                    "user_id": user_id,
                    "channel_id": channel_id,
                    "text": body,
                    "date": START + timedelta(minutes=tg_message_id),
                }
                for tg_message_id, body in comments
            ],
        )
    return user_id


COMMENTS = [
    (1, "профсоюз защищает рабочих"),
    (2, "сегодня хорошая погода"),
    (3, "рабочих защищает забастовка"),
]


def test_message_embeddings_are_stored_with_their_model(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        stored = await embed_messages(sessions, FakeEncoder(), batch_size=2)
        async with sessions() as session:
            model = (await session.scalars(select(EmbeddingModel))).one()
            rows = (
                await session.execute(
                    select(Message.text, MessageEmbedding.embedding, MessageEmbedding.model_id)
                    .join(MessageEmbedding, MessageEmbedding.message_id == Message.id)
                    .order_by(Message.tg_message_id)
                )
            ).all()
            texts = (await session.scalars(select(Message.text).order_by(Message.id))).all()
        return stored, model, rows, texts

    stored, model, rows, texts = run_db(scenario)

    assert stored == 3
    assert (model.name, model.revision, model.dimensions) == ("test/bag-of-words", "rev-1", EMBEDDING_DIMENSIONS)
    assert (model.pooling, model.normalized, model.max_tokens) == ("mean", True, 512)
    assert model.input_prefix == "passage: "
    assert [r[0] for r in rows] == [body for _, body in COMMENTS]
    for body, vector, model_id in rows:
        assert model_id == model.id
        assert np.allclose(vector, _vector(body), atol=1e-6)
    # The embedding is derived: the text it came from stays as it was.
    assert texts == [body for _, body in COMMENTS]


def test_chunk_embeddings_are_stored_for_the_requested_set_only(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        pairs = await build_chunk_set(
            sessions, "fixed_messages", {"max_messages": 2, "same_channel": True},
            "words", _words,
        )
        singles = await build_chunk_set(sessions, "message", {}, "words", _words)
        stored = await embed_chunks(sessions, FakeEncoder(), pairs.chunk_set_id)
        async with sessions() as session:
            rows = (
                await session.execute(
                    select(Chunk.chunk_set_id, Chunk.text, ChunkEmbedding.embedding)
                    .join(ChunkEmbedding, ChunkEmbedding.chunk_id == Chunk.id)
                    .order_by(Chunk.date_from)
                )
            ).all()
        # Forcing one set must not drop the vectors of another.
        encoder = FakeEncoder()
        await embed_chunks(sessions, encoder, singles.chunk_set_id)
        forced = await embed_chunks(sessions, encoder, pairs.chunk_set_id, force=True)
        async with sessions() as session:
            total = await session.scalar(select(func.count()).select_from(ChunkEmbedding))
        return stored, pairs.chunk_set_id, singles.chunk_set_id, rows, forced, total

    stored, pairs, _singles, rows, forced, total = run_db(scenario)

    assert stored == 2
    assert (forced, total) == (2, 5)
    assert {r[0] for r in rows} == {pairs}
    assert [r[1] for r in rows] == [
        "профсоюз защищает рабочих\nсегодня хорошая погода",
        "рабочих защищает забастовка",
    ]
    assert np.allclose(rows[0][2], _vector(rows[0][1]), atol=1e-6)


def test_repeated_run_computes_only_what_is_missing(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        encoder = FakeEncoder()
        first = await embed_messages(sessions, encoder)
        again = await embed_messages(sessions, encoder)
        encoded_before = list(encoder.encoded)

        await _seed(sessions, [(4, "новый комментарий")])
        grown = await embed_messages(sessions, encoder)
        new_texts = encoder.encoded[len(encoded_before):]

        forced = await embed_messages(sessions, encoder, force=True)
        async with sessions() as session:
            total = await session.scalar(select(func.count()).select_from(MessageEmbedding))
        return first, again, encoded_before, grown, new_texts, forced, total

    first, again, encoded_before, grown, new_texts, forced, total = run_db(scenario)

    assert (first, again) == (3, 0)
    assert len(encoded_before) == 3
    assert (grown, new_texts) == (1, ["новый комментарий"])
    # --force drops the model's vectors and computes all of them again.
    assert (forced, total) == (4, 4)


def test_rebuilt_chunks_are_embedded_again_and_the_rest_is_kept(run_db):
    params = {"max_messages": 2, "same_channel": True}

    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        await _seed(sessions, [(50, "чужой комментарий")], tg_id=2)
        built = await build_chunk_set(sessions, "fixed_messages", params, "words", _words)
        encoder = FakeEncoder()
        first = await embed_chunks(sessions, encoder, built.chunk_set_id)
        again = await embed_chunks(sessions, encoder, built.chunk_set_id)

        # A new comment of user 1 rebuilds that user's chunks only.
        await _seed(sessions, [(4, "ещё один")])
        await build_chunk_set(sessions, "fixed_messages", params, "words", _words)
        seen = len(encoder.encoded)
        after_rebuild = await embed_chunks(sessions, encoder, built.chunk_set_id)
        forced = await embed_chunks(sessions, encoder, built.chunk_set_id, force=True)
        async with sessions() as session:
            total = await session.scalar(select(func.count()).select_from(ChunkEmbedding))
        return first, again, after_rebuild, encoder.encoded[seen : seen + after_rebuild], forced, total

    first, again, after_rebuild, recomputed, forced, total = run_db(scenario)

    assert (first, again) == (3, 0)
    assert after_rebuild == 2
    assert "чужой комментарий" not in recomputed
    assert (forced, total) == (3, 3)


def test_models_and_revisions_are_kept_apart(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        first = FakeEncoder()
        newer = FakeEncoder(replace(SPEC, revision="rev-2"))
        unprefixed = FakeEncoder(replace(SPEC, passage_prefix=""))
        counts = [
            await embed_messages(sessions, first),
            await embed_messages(sessions, newer),
            await embed_messages(sessions, unprefixed),
            await embed_messages(sessions, FakeEncoder()),
        ]
        async with sessions() as session:
            models = (
                await session.execute(
                    select(EmbeddingModel.revision, EmbeddingModel.input_prefix).order_by(
                        EmbeddingModel.id
                    )
                )
            ).all()
            per_message = (
                await session.scalars(
                    select(func.count()).select_from(MessageEmbedding).group_by(
                        MessageEmbedding.message_id
                    )
                )
            ).all()
            per_model = [
                await embedding_repo.count_message_embeddings(session, model_id)
                for model_id in (
                    await session.scalars(select(EmbeddingModel.id))
                ).all()
            ]
        # Forcing one model leaves the vectors of the others alone.
        await embed_messages(sessions, newer, force=True)
        async with sessions() as session:
            total = await session.scalar(select(func.count()).select_from(MessageEmbedding))
        return counts, [tuple(m) for m in models], per_message, per_model, total

    counts, models, per_message, per_model, total = run_db(scenario)

    # The same model again adds nothing; another revision or prefix is another model.
    assert counts == [3, 3, 3, 0]
    assert models == [("rev-1", "passage: "), ("rev-2", "passage: "), ("rev-1", "")]
    assert per_message == [3, 3, 3]
    assert per_model == [3, 3, 3]
    assert total == 9


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        (replace(SPEC, dimensions=768), "migration"),
        (replace(SPEC, revision=""), "revision"),
    ],
)
def test_a_model_the_tables_cannot_hold_is_rejected_before_any_work(run_db, spec, message):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        encoder = FakeEncoder(spec)
        with pytest.raises(ValueError, match=message):
            await embed_messages(sessions, encoder)
        async with sessions() as session:
            models = await session.scalar(select(func.count()).select_from(EmbeddingModel))
        return encoder.encoded, models

    assert run_db(scenario) == ([], 0)


def test_similarity_search_returns_the_users_nearest_messages(run_db):
    async def scenario(sessions):
        user_id = await _seed(sessions, COMMENTS)
        # Another user's comment, identical to the query: the nearest of all.
        await _seed(sessions, [(60, "кто защищает рабочих")], tg_id=2)
        encoder = FakeEncoder()
        await embed_messages(sessions, encoder)
        await embed_messages(sessions, FakeEncoder(replace(SPEC, revision="rev-2")))
        query = _vector("кто защищает рабочих")
        async with sessions() as session:
            model_id = await session.scalar(
                select(EmbeddingModel.id).where(EmbeddingModel.revision == "rev-1")
            )
            found = await embedding_repo.search_messages(
                session, model_id, user_id, query, limit=2
            )
            texts = dict(
                (await session.execute(select(Message.id, Message.text))).tuples().all()
            )
        return [(texts[i], score) for i, score in found], query

    found, query = run_db(scenario)

    # Two of the user's three messages share words with the query; the other
    # user's closer message and the second model's vectors stay out.
    assert sorted(body for body, _ in found) == [
        "профсоюз защищает рабочих",
        "рабочих защищает забастовка",
    ]
    for body, score in found:
        assert score == pytest.approx(float(_vector(body) @ query), abs=1e-5)
    assert found[0][1] >= found[1][1]


def test_similarity_search_over_chunks_of_one_set(run_db):
    async def scenario(sessions):
        user_id = await _seed(sessions, COMMENTS)
        pairs = await build_chunk_set(
            sessions, "fixed_messages", {"max_messages": 2, "same_channel": True},
            "words", _words,
        )
        singles = await build_chunk_set(sessions, "message", {}, "words", _words)
        encoder = FakeEncoder()
        await embed_chunks(sessions, encoder, pairs.chunk_set_id)
        await embed_chunks(sessions, encoder, singles.chunk_set_id)
        async with sessions() as session:
            model_id = await session.scalar(select(EmbeddingModel.id))
            found = await embedding_repo.search_chunks(
                session, model_id, pairs.chunk_set_id, user_id,
                _vector("забастовка рабочих"), limit=5,
            )
            texts = dict(
                (await session.execute(select(Chunk.id, Chunk.text))).tuples().all()
            )
        return [texts[i] for i, _ in found]

    assert run_db(scenario) == [
        "рабочих защищает забастовка",
        "профсоюз защищает рабочих\nсегодня хорошая погода",
    ]


def test_cosine_inner_product_and_l2_rank_normalized_vectors_alike(run_db):
    """Which pgvector operator to use: on unit vectors all three agree."""

    async def scenario(sessions):
        # Message n holds the first n of twelve words, so every similarity to
        # the query of all twelve is different: no ties to break.
        words = [f"слово{n}" for n in range(1, 13)]
        await _seed(sessions, [(n, " ".join(words[:n])) for n in range(1, 13)])
        await embed_messages(sessions, FakeEncoder())
        query = str(_vector(" ".join(words)).tolist())
        orders = {}
        async with sessions() as session:
            for name, operator in (("cosine", "<=>"), ("inner", "<#>"), ("l2", "<->")):
                rows = await session.execute(
                    text(
                        f"SELECT message_id, embedding {operator} CAST(:q AS vector) "
                        "FROM message_embeddings "
                        f"ORDER BY embedding {operator} CAST(:q AS vector), message_id LIMIT 10"
                    ),
                    {"q": query},
                )
                orders[name] = rows.all()
        return orders

    orders = run_db(scenario)

    ids = {name: [row[0] for row in rows] for name, rows in orders.items()}
    assert ids["cosine"] == ids["inner"] == ids["l2"]
    assert len(set(ids["cosine"])) == 10
    # <#> is the negative inner product, and for unit vectors cosine distance
    # is one minus it.
    for (_, cosine), (_, inner) in zip(orders["cosine"], orders["inner"]):
        assert cosine == pytest.approx(1 + inner, abs=1e-5)


def test_a_trial_run_can_be_limited_to_one_user(run_db):
    params = {"max_messages": 2, "same_channel": True}

    async def scenario(sessions):
        first = await _seed(sessions, COMMENTS)
        await _seed(sessions, [(50, "чужой комментарий")], tg_id=2)
        built = await build_chunk_set(
            sessions, "fixed_messages", params, "words", _words, only_user_ids=[first]
        )
        async with sessions() as session:
            chunk_owners = set(await session.scalars(select(Chunk.user_id)))
        # The full build then adds only the user left out before.
        whole = await build_chunk_set(sessions, "fixed_messages", params, "words", _words)

        encoder = FakeEncoder()
        counts = [
            await embed_messages(sessions, encoder, user_id=first),
            await embed_chunks(sessions, encoder, built.chunk_set_id, user_id=first),
            # Again, and forced: still only that user's rows.
            await embed_messages(sessions, encoder, user_id=first),
            await embed_chunks(sessions, encoder, built.chunk_set_id, user_id=first),
            await embed_messages(sessions, encoder, user_id=first, force=True),
        ]
        leaked = "чужой комментарий" in encoder.encoded
        async with sessions() as session:
            message_owners = set(
                await session.scalars(
                    select(Message.user_id).join(
                        MessageEmbedding, MessageEmbedding.message_id == Message.id
                    )
                )
            )
            embedded_chunk_owners = set(
                await session.scalars(
                    select(Chunk.user_id).join(
                        ChunkEmbedding, ChunkEmbedding.chunk_id == Chunk.id
                    )
                )
            )
        # The unrestricted run embeds the rest; forcing one user afterwards
        # leaves the other user's vector in place.
        everyone = await embed_messages(sessions, encoder)
        await embed_messages(sessions, encoder, user_id=first, force=True)
        async with sessions() as session:
            total = await session.scalar(select(func.count()).select_from(MessageEmbedding))
        return (built, whole, counts, chunk_owners, message_owners,
                embedded_chunk_owners, first, leaked, everyone, total)

    (built, whole, counts, chunk_owners, message_owners, embedded_chunk_owners,
     first, leaked, everyone, total) = run_db(scenario)

    assert (built.users_rebuilt, built.chunks_written) == (1, 2)
    assert chunk_owners == {first}
    assert (whole.users_rebuilt, whole.chunks_written) == (1, 1)
    assert counts == [3, 2, 0, 0, 3]
    assert message_owners == embedded_chunk_owners == {first}
    assert not leaked
    assert (everyone, total) == (1, 4)


def test_the_tables_fit_the_production_model(run_db):
    from embeddings.e5 import PRODUCTION_MODEL

    async def scenario(sessions):
        async with sessions() as session:
            return (
                await session.execute(
                    text(
                        "SELECT c.relname, format_type(a.atttypid, a.atttypmod) "
                        "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                        "WHERE a.attname = 'embedding' AND c.relname IN "
                        "('message_embeddings', 'chunk_embeddings') ORDER BY 1"
                    )
                )
            ).all()

    columns = [tuple(row) for row in run_db(scenario)]

    size = f"vector({PRODUCTION_MODEL.dimensions})"
    assert columns == [("chunk_embeddings", size), ("message_embeddings", size)]
    assert PRODUCTION_MODEL.dimensions == EMBEDDING_DIMENSIONS


def test_a_refreshed_text_drops_what_was_derived_from_the_old_one(run_db):
    params = {"max_messages": 2, "same_channel": True}

    async def scenario(sessions):
        user_id = await _seed(sessions, COMMENTS)
        other = await _seed(sessions, [(50, "чужой комментарий")], tg_id=2)
        built = await build_chunk_set(sessions, "fixed_messages", params, "words", _words)
        encoder = FakeEncoder()
        await embed_messages(sessions, encoder)
        await embed_chunks(sessions, encoder, built.chunk_set_id)

        # The first message is taken from Telegram again with another text;
        # the second arrives unchanged.
        async with sessions.begin() as session:
            channel_id = await repo.upsert_channel(session, "news")
            new_rows = await repo.insert_messages(
                session,
                [
                    {"tg_message_id": 1, "user_id": user_id, "channel_id": channel_id,
                     "text": "Профсоюз защищает рабочих", "date": START},
                    {"tg_message_id": 2, "user_id": user_id, "channel_id": channel_id,
                     "text": COMMENTS[1][1], "date": START},
                ],
                refresh_text=True,
            )
        async with sessions() as session:
            embedded = (
                await session.scalars(
                    select(Message.tg_message_id)
                    .join(MessageEmbedding, MessageEmbedding.message_id == Message.id)
                    .order_by(Message.tg_message_id)
                )
            ).all()
            chunk_texts = (
                await session.scalars(select(Chunk.text).order_by(Chunk.date_from))
            ).all()

        seen = len(encoder.encoded)
        rebuilt = await build_chunk_set(sessions, "fixed_messages", params, "words", _words)
        messages_again = await embed_messages(sessions, encoder)
        chunks_again = await embed_chunks(sessions, encoder, built.chunk_set_id)
        return (new_rows, embedded, chunk_texts, rebuilt, messages_again,
                chunks_again, encoder.encoded[seen:], other)

    (new_rows, embedded, chunk_texts, rebuilt, messages_again, chunks_again,
     recomputed, _other) = run_db(scenario)

    assert new_rows == 0
    # Only the message whose text changed lost its vector, and only the chunk
    # that held it is gone: the user's other chunk and the other user's stay.
    assert embedded == [2, 3, 50]
    assert chunk_texts == ["рабочих защищает забастовка", "чужой комментарий"]
    assert (rebuilt.users_rebuilt, messages_again) == (1, 1)
    assert "Профсоюз защищает рабочих" in recomputed
    assert "чужой комментарий" not in recomputed
    assert chunks_again == 2
