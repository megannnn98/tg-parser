import zlib
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import numpy as np
import pytest
from fastapi.testclient import TestClient

from chunking.builder import build_chunk_set
from db import repositories as repo
from db.embedding_models import EMBEDDING_DIMENSIONS
from embeddings.e5 import E5Spec
from embeddings.pipeline import embed_chunks, embed_messages
from services.profiles import ProfileNotFound
from services.search import (
    CHUNK_PARAMETERS,
    CHUNK_STRATEGY,
    CONTEXT_WEIGHT,
    SemanticSearch,
    chunk_tokenizer,
)
from web.app import create_app

START = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
SPEC = E5Spec(name="test/bag-of-words", revision="rev-1", dimensions=EMBEDDING_DIMENSIONS)

STRIKE = "профсоюз объявил забастовку на заводе"
COMMENTS = [
    # Two messages of 100 "tokens" fit a chunk of 256, a third does not: the
    # channel is cut into pairs.
    (1, STRIKE),
    (2, "согласен"),
    (3, "вчера смотрел футбол"),
    (4, "согласен"),
    (5, "забастовку надо поддержать"),
    (6, "погода хорошая"),
]
QUERY = "профсоюз забастовку"


def _vector(text: str) -> np.ndarray:
    """A normalized bag of words: texts sharing words are close."""
    vector = np.zeros(EMBEDDING_DIMENSIONS, dtype=np.float32)
    for word in text.lower().split():
        vector[zlib.crc32(word.encode()) % EMBEDDING_DIMENSIONS] += 1
    return vector / np.linalg.norm(vector)


def _similarity(text: str, query: str = QUERY) -> float:
    return float(_vector(text) @ _vector(query))


class FakeEncoder:
    def __init__(self, spec: E5Spec = SPEC):
        self.spec = spec
        self.queries: list[str] = []
        self.passages: list[str] = []

    def encode_passages(self, texts):
        self.passages.extend(texts)
        return np.stack([_vector(t) for t in texts])

    def encode_queries(self, texts):
        self.queries.extend(texts)
        return np.stack([_vector(t) for t in texts])


def _hundred_tokens(texts):
    return [100 for _ in texts]


async def _seed(sessions, comments, tg_id=1, channel="news"):
    async with sessions.begin() as session:
        user_id = await repo.upsert_user(session, tg_id, f"user{tg_id}")
        channel_id = await repo.upsert_channel(session, channel)
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


async def _index(sessions, encoder, chunks=True):
    """Embeds everything stored, with the production chunk set unless told not to."""
    await embed_messages(sessions, encoder)
    if chunks:
        built = await build_chunk_set(
            sessions,
            CHUNK_STRATEGY,
            CHUNK_PARAMETERS,
            chunk_tokenizer(encoder.spec),
            _hundred_tokens,
        )
        await embed_chunks(sessions, encoder, built.chunk_set_id)


def test_messages_are_ranked_by_their_own_and_their_chunks_similarity(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        encoder = FakeEncoder()
        await _index(sessions, encoder)
        result = await SemanticSearch(sessions, encoder).search(1, QUERY)
        return result, encoder

    result, encoder = run_db(scenario)

    # The query goes through the query side of the model, once.
    assert encoder.queries == [QUERY]
    assert QUERY not in encoder.passages
    assert (result.indexed_messages, result.total_messages) == (6, 6)
    assert result.used_context

    by_id = {hit.tg_message_id: hit for hit in result.hits}
    strike_chunk = _similarity(STRIKE + "\nсогласен")
    support_chunk = _similarity("забастовку надо поддержать\nпогода хорошая")
    # Each message keeps its own similarity and gets that of its chunk.
    assert by_id[1].message_score == pytest.approx(_similarity(STRIKE), abs=1e-5)
    assert by_id[1].chunk_score == pytest.approx(strike_chunk, abs=1e-5)
    assert by_id[2].message_score == pytest.approx(0, abs=1e-5)
    assert by_id[2].chunk_score == pytest.approx(strike_chunk, abs=1e-5)
    assert by_id[4].chunk_score == pytest.approx(0, abs=1e-5)
    for hit in result.hits:
        assert hit.score == pytest.approx(
            (1 - CONTEXT_WEIGHT) * hit.message_score + CONTEXT_WEIGHT * hit.chunk_score,
            abs=1e-5,
        )
    assert CONTEXT_WEIGHT == 0.5

    # The reply to the strike message outranks the same word said about
    # football, and the unrelated neighbour of a relevant message rises too.
    order = [hit.tg_message_id for hit in result.hits]
    assert order[0] == 1
    assert order.index(2) < order.index(4)
    assert order.index(6) < order.index(3)
    assert order == sorted(
        by_id,
        key=lambda m: (-round(by_id[m].score, 6), by_id[m].message_id),
    )
    assert (by_id[1].channel, by_id[1].text) == ("news", STRIKE)
    assert by_id[1].date == START + timedelta(minutes=1)
    assert support_chunk > 0


def test_without_context_only_the_message_itself_counts(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        encoder = FakeEncoder()
        await _index(sessions, encoder)
        return await SemanticSearch(sessions, encoder).search(
            1, QUERY, use_context=False
        )

    result = run_db(scenario)

    assert not result.used_context
    assert [hit.tg_message_id for hit in result.hits[:2]] == [1, 5]
    assert all(hit.chunk_score is None for hit in result.hits)
    assert all(hit.score == hit.message_score for hit in result.hits)
    # Both replies score nothing alone: only the id tells them apart.
    tail = [hit.tg_message_id for hit in result.hits[2:]]
    assert tail == sorted(tail)


def test_search_stays_within_the_user(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        await _seed(sessions, [(50, QUERY)], tg_id=2, channel="other")
        encoder = FakeEncoder()
        await _index(sessions, encoder)
        search = SemanticSearch(sessions, encoder)
        return await search.search(1, QUERY), await search.search(2, QUERY)

    first, second = run_db(scenario)

    # The other user holds the query word for word; it must not leak.
    assert QUERY not in [hit.text for hit in first.hits]
    assert len(first.hits) == 6
    assert [hit.text for hit in second.hits] == [QUERY]
    assert second.hits[0].score == pytest.approx(1, abs=1e-5)


def test_limit_keeps_the_best(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        encoder = FakeEncoder()
        await _index(sessions, encoder)
        search = SemanticSearch(sessions, encoder)
        return await search.search(1, QUERY), await search.search(1, QUERY, limit=2)

    everything, limited = run_db(scenario)

    assert [h.message_id for h in limited.hits] == [
        h.message_id for h in everything.hits[:2]
    ]
    assert limited.indexed_messages == 6


def test_a_message_not_chunked_yet_is_scored_alone(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS[:2])
        encoder = FakeEncoder()
        await _index(sessions, encoder)
        # Collected and embedded after the chunks were built.
        await _seed(sessions, [(9, "профсоюз победил")])
        await embed_messages(sessions, encoder)
        return await SemanticSearch(sessions, encoder).search(1, QUERY)

    result = run_db(scenario)

    late = next(hit for hit in result.hits if hit.tg_message_id == 9)
    assert late.chunk_score is None
    assert late.score == pytest.approx(late.message_score, abs=1e-6)
    assert late.message_score == pytest.approx(_similarity("профсоюз победил"), abs=1e-5)
    assert result.used_context


def test_without_a_chunk_set_messages_are_ranked_alone(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        encoder = FakeEncoder()
        await _index(sessions, encoder, chunks=False)
        # Chunks of another strategy are not the search context.
        other = await build_chunk_set(
            sessions, "fixed_messages", {"max_messages": 2, "same_channel": True},
            chunk_tokenizer(encoder.spec), _hundred_tokens,
        )
        await embed_chunks(sessions, encoder, other.chunk_set_id)
        return await SemanticSearch(sessions, encoder).search(1, QUERY)

    result = run_db(scenario)

    assert not result.used_context
    assert all(hit.chunk_score is None for hit in result.hits)
    assert [hit.tg_message_id for hit in result.hits[:2]] == [1, 5]


def test_vectors_of_another_model_are_not_searched(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        await _index(sessions, FakeEncoder())
        newer = FakeEncoder(replace(SPEC, revision="rev-2"))
        result = await SemanticSearch(sessions, newer).search(1, QUERY)
        return result, newer

    result, newer = run_db(scenario)

    # Nothing was embedded by this revision: no hits, and no query is encoded.
    assert result.hits == []
    assert (result.indexed_messages, result.total_messages) == (0, 6)
    assert newer.queries == []


def test_user_without_embeddings_and_unknown_user(run_db):
    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        encoder = FakeEncoder()
        search = SemanticSearch(sessions, encoder)
        nothing = await search.search(1, QUERY)
        with pytest.raises(ProfileNotFound):
            await search.search(404, QUERY)
        return nothing, encoder.queries

    nothing, queries = run_db(scenario)

    assert nothing.hits == []
    assert (nothing.indexed_messages, nothing.total_messages) == (0, 6)
    assert queries == []


def test_ranking_takes_the_best_chunk_of_the_given_set_with_the_given_weight(run_db):
    from sqlalchemy import select

    from db import embedding_repository
    from db.embedding_models import EmbeddingModel

    async def scenario(sessions):
        user_id = await _seed(sessions, COMMENTS[:3])
        encoder = FakeEncoder()
        await embed_messages(sessions, encoder)
        # Half of each chunk is repeated in the next: the middle message is in two.
        overlapping = await build_chunk_set(
            sessions, "token_budget",
            {"max_tokens": 256, "overlap": 0.5, "same_channel": True},
            "test", _hundred_tokens,
        )
        # Another set, one chunk for everything: not the one asked for.
        whole = await build_chunk_set(
            sessions, "fixed_messages", {"max_messages": 9, "same_channel": True},
            "test", _hundred_tokens,
        )
        for chunk_set in (overlapping, whole):
            await embed_chunks(sessions, encoder, chunk_set.chunk_set_id)
        async with sessions() as session:
            model_id = await session.scalar(select(EmbeddingModel.id))
            return await embedding_repository.rank_messages(
                session, model_id, user_id, _vector(QUERY), 10,
                chunk_set_id=overlapping.chunk_set_id, context_weight=0.25,
            )

    hits = {hit.tg_message_id: hit for hit in run_db(scenario)}

    first_pair = _similarity(STRIKE + "\nсогласен")
    second_pair = _similarity("согласен\nвчера смотрел футбол")
    assert first_pair > second_pair
    # "согласен" is in both chunks and takes the better one; the football
    # message is only in the second.
    assert hits[2].chunk_score == pytest.approx(first_pair, abs=1e-5)
    assert hits[3].chunk_score == pytest.approx(second_pair, abs=1e-5)
    for hit in hits.values():
        assert hit.score == pytest.approx(
            0.75 * hit.message_score + 0.25 * hit.chunk_score, abs=1e-5
        )
    assert hits[1].score != pytest.approx(
        0.25 * hits[1].message_score + 0.25 * hits[1].chunk_score, abs=1e-3
    )


# --- API ---------------------------------------------------------------------


@pytest.fixture
def client(run_db, database_url, tmp_path):
    encoder = FakeEncoder()

    async def scenario(sessions):
        await _seed(sessions, COMMENTS)
        await _seed(sessions, [(50, QUERY)], tg_id=2, channel="other")
        await _seed(sessions, [(60, "не проиндексировано")], tg_id=3, channel="other")
        await embed_messages(sessions, encoder)
        built = await build_chunk_set(
            sessions, CHUNK_STRATEGY, CHUNK_PARAMETERS,
            chunk_tokenizer(encoder.spec), _hundred_tokens,
        )
        await embed_chunks(sessions, encoder, built.chunk_set_id)
        await _seed(sessions, [(61, "ещё одно")], tg_id=4, channel="other")

    run_db(scenario)
    app = create_app(
        database_url=database_url,
        channels=[],
        frontend_dist=tmp_path / "missing",
        search_encoder=encoder,
    )
    return TestClient(app), encoder


def test_search_api_returns_the_users_messages(client):
    http, encoder = client
    with http:
        resp = http.get("/api/v1/users/1/search", params={"q": QUERY, "limit": 3})

    assert resp.status_code == 200
    body = resp.json()
    assert body["query"] == QUERY
    assert (body["indexed_messages"], body["total_messages"]) == (6, 6)
    assert body["used_context"] is True
    assert body["results"][0]["tg_message_id"] == 1
    assert len(body["results"]) == 3
    first = body["results"][0]
    assert set(first) == {
        "message_id", "tg_message_id", "channel", "date", "text",
        "message_score", "chunk_score", "score",
    }
    assert (first["channel"], first["text"]) == ("news", STRIKE)
    assert first["date"].startswith("2026-01-01T12:01:00")
    assert first["score"] == pytest.approx(
        0.5 * first["message_score"] + 0.5 * first["chunk_score"], abs=1e-5
    )
    assert encoder.queries == [QUERY]


def test_search_api_can_rank_messages_alone(client):
    http, _ = client
    with http:
        body = http.get(
            "/api/v1/users/1/search", params={"q": QUERY, "context": "false"}
        ).json()

    assert body["used_context"] is False
    assert [hit["tg_message_id"] for hit in body["results"][:2]] == [1, 5]
    assert body["results"][0]["chunk_score"] is None


def test_search_api_rejects_bad_requests(client):
    http, encoder = client
    with http:
        assert http.get("/api/v1/users/404/search", params={"q": QUERY}).status_code == 404
        assert http.get("/api/v1/users/1/search").status_code == 422
        assert http.get("/api/v1/users/1/search", params={"q": "   "}).status_code == 422
        for limit in (0, 101):
            resp = http.get("/api/v1/users/1/search", params={"q": QUERY, "limit": limit})
            assert resp.status_code == 422
        not_indexed = http.get("/api/v1/users/4/search", params={"q": QUERY}).json()

    # Collected after the embeddings were built: the answer says so.
    assert not_indexed["results"] == []
    assert (not_indexed["indexed_messages"], not_indexed["total_messages"]) == (0, 1)
    assert encoder.queries == []


def test_the_app_searches_with_the_production_model():
    from embeddings.e5 import PRODUCTION_MODEL, E5Encoder

    app = create_app(database_url="postgresql+asyncpg://nobody@127.0.0.1:1/none", channels=[])

    encoder = app.state.search_encoder
    assert isinstance(encoder, E5Encoder)
    assert encoder.spec is PRODUCTION_MODEL
    assert encoder.spec.dimensions == EMBEDDING_DIMENSIONS
