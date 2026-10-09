import asyncio
from datetime import datetime, timezone

import httpx
import pytest
from sqlalchemy import text
from test_embedding_pipeline import FakeEncoder
from test_position_comparison import FakeEmbedder, FakeGateway

from db import repositories as repo
from db.analysis_store import AnalysisStore
from embeddings.e5 import PRODUCTION_MODEL
from parser.local_text_analysis import LocalTextAnalysis
from parser.position_analysis import PositionAnalysis
from web.app import create_app


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


@pytest.fixture
def store(run_db, database_url):
    # run_db empties the tables, the cache among them.
    run_db(lambda sessions: asyncio.sleep(0))
    store = AnalysisStore(database_url)
    yield store
    store.close()


def _seed(run_db, authors, collected=True):
    """authors: {tg_id: [(message id, text, date)]}, all in the channel "channel"."""

    async def scenario(sessions):
        async with sessions.begin() as session:
            channel_id = await repo.upsert_channel(session, "channel")
            for tg_id, comments in authors.items():
                user_id = await repo.upsert_user(session, tg_id, f"user{tg_id}")
                if collected:
                    await repo.mark_profiles_collected(
                        session, {user_id: _utc(2026, 9, 1)}
                    )
                await repo.insert_messages(
                    session,
                    [
                        {
                            "tg_message_id": message_id,
                            "user_id": user_id,
                            "channel_id": channel_id,
                            "text": text,
                            "date": date,
                        }
                        for message_id, text, date in comments
                    ],
                )

    return scenario


def test_cache_round_trip_and_namespace_isolation(store):
    assert store.get("wanted", "missing") is None
    assert store.get_many("wanted", []) == {}
    assert store.get_all("wanted") == {}
    assert store.count("wanted") == 0

    # More keys than one query reads.
    values = {f"key{i}": [i, 0.5] for i in range(1201)}
    store.put_many("wanted", values)
    store.put("other", "key0", {"text": "чужое", "nested": [1, None]})
    store.put_many("wanted", {})

    assert store.get_many("wanted", [*values, "key0", "missing"]) == values
    assert store.get_all("wanted") == values
    assert store.count("wanted") == 1201
    assert store.get("other", "key0") == {"text": "чужое", "nested": [1, None]}

    store.put("other", "key0", "replaced")
    assert store.get("other", "key0") == "replaced"
    assert store.count("other") == 1


def test_analysis_lock_is_exclusive_across_connections(store, database_url):
    other = AnalysisStore(database_url)
    try:
        with store.analysis_lock():
            with pytest.raises(RuntimeError, match="уже выполняется"):
                with other.analysis_lock():
                    pass
            # The holder keeps working with the cache meanwhile.
            store.put("run", "latest", {"state": "running"})
        with other.analysis_lock():
            pass
    finally:
        other.close()


def test_analysis_lock_is_released_when_the_work_fails(store):
    with pytest.raises(ValueError):
        with store.analysis_lock():
            raise ValueError("analysis failed")
    with store.analysis_lock():
        pass


def test_sources_are_the_comments_of_collected_profiles(run_db, database_url):
    async def scenario(sessions):
        await _seed(
            None,
            {
                2: [(1, "b-first", _utc(2026, 1, 1))],
                1: [
                    (5, "later", _utc(2026, 1, 3)),
                    (9, "earlier", _utc(2026, 1, 2, 10, 30)),
                ],
            },
        )(sessions)
        # An author seen in a channel is not an analysed profile.
        await _seed(None, {3: [(7, "author", _utc(2026, 1, 1))]}, collected=False)(
            sessions
        )
        async with sessions.begin() as session:
            nameless = await repo.upsert_user(session, 4, None, "Хрюкало", "Офф")
            await repo.mark_profiles_collected(session, {nameless: _utc(2026, 9, 1)})

    run_db(scenario)
    store = AnalysisStore(database_url)
    try:
        profiles, comments = store.load_sources()
        before = store.manifest()
        unchanged = store.manifest()
        run_db_append = _seed(None, {1: [(10, "new", _utc(2026, 1, 4))]})
        asyncio.run(_append(database_url, run_db_append))
        after = store.manifest()
    finally:
        store.close()

    assert {tg_id: p.display_username for tg_id, p in profiles.items()} == {
        1: "@user1",
        2: "@user2",
        4: "Хрюкало Офф",
    }
    assert [(c["tg_id"], c["message_id"], c["text"]) for c in comments] == [
        (1, 9, "earlier"),
        (1, 5, "later"),
        (2, 1, "b-first"),
    ]
    assert comments[0]["channel"] == "channel"
    assert comments[0]["date"] == "2026-01-02T10:30:00+00:00"
    assert before == unchanged
    assert after != before


async def _append(database_url, scenario):
    from sqlalchemy.pool import NullPool

    from db.engine import create_engine, session_factory

    engine = create_engine(database_url, poolclass=NullPool)
    try:
        await scenario(session_factory(engine))
    finally:
        await engine.dispose()


def test_position_analysis_runs_over_http_on_the_real_store(run_db, database_url, tmp_path):
    run_db(
        _seed(
            None,
            {
                # A message id is unique within the channel, across authors.
                user: [
                    (user * 100 + i, f"issue{i}:support", _utc(2026, 1, 1))
                    for i in range(1, 4)
                ]
                for user in (1, 2)
            },
        )
    )
    store = AnalysisStore(database_url)
    service = PositionAnalysis(store, FakeGateway(), FakeEmbedder())
    app = create_app(
        database_url=database_url,
        channels=[],
        position_service=service,
        frontend_dist=tmp_path / "missing",
    )

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        extract = service.gateway.extract

        async def waiting_extract(texts):
            entered.set()
            await release.wait()
            return await extract(texts)

        service.gateway.extract = waiting_extract
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            missing = await client.get("/api/v1/users/5/position-comparisons")
            assert missing.status_code == 404
            result = await client.get("/api/v1/users/1/position-comparisons")
            assert result.json()["progress"]["state"] == "idle"
            assert result.json()["embeddings"]["saved_vectors"] == 0
            start = await client.post("/api/v1/position-analysis")
            assert start.status_code == 202
            await asyncio.wait_for(entered.wait(), timeout=5)
            # The running analysis holds the lock: a second start is refused.
            assert (await client.post("/api/v1/position-analysis")).status_code == 409
            running = await client.get("/api/v1/users/1/position-comparisons")
            progress = running.json()["progress"]
            assert progress["state"] == "running"
            assert progress["total_comments"] == 6
            release.set()
            await app.state.position_jobs.task
            result = await client.get("/api/v1/users/1/position-comparisons")
            body = result.json()
            assert body["ranking"][0]["score"] == 100
            assert body["ranking"][0]["tg_id"] == 2
            assert "db_name" not in body["ranking"][0]
            assert body["ranking"][0]["questions"][0]["left"]["text"]
            assert body["embeddings"]["saved_vectors"] == 3
            assert "PostgreSQL" in body["embeddings"]["storage"]
        await app.state.position_jobs.close()
        await app.state.database.close()

    try:
        asyncio.run(scenario())
    finally:
        store.close()


def test_default_app_compares_stored_embeddings_of_the_profiles(
    run_db, database_url, tmp_path
):
    async def seed(sessions):
        await _seed(
            None,
            {
                1: [(1, "alpha", _utc(2026, 1, 1))],
                2: [(2, "alpha", _utc(2026, 1, 2))],
                4: [(4, "beta", _utc(2026, 1, 4))],
            },
        )(sessions)
        await _seed(
            None, {3: [(3, "not a profile", _utc(2026, 1, 3))]}, collected=False
        )(sessions)

    run_db(seed)

    encoder = FakeEncoder(PRODUCTION_MODEL)
    app = create_app(
        database_url=database_url,
        channels=[],
        frontend_dist=tmp_path / "missing",
        search_encoder=encoder,
    )
    assert isinstance(app.state.position_jobs.service, LocalTextAnalysis)

    async def scenario():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            assert (await client.post("/api/v1/position-analysis")).status_code == 202
            await app.state.position_jobs.task
            return (await client.get("/api/v1/users/1/position-comparisons")).json()

    try:
        result = asyncio.run(scenario())
    finally:
        asyncio.run(app.state.database.close())
        app.state.analysis_store.close()

    assert result["method"] == "text_similarity"
    assert result["progress"]["state"] == "done"
    assert [(a["tg_id"], a["similarity"]) for a in result["similar_authors"]] == [
        (2, 1.0),
    ]
    # The analysis embedded what was missing, and only for the profiles.
    assert encoder.encoded == ["alpha", "alpha", "beta"]
    assert result["embeddings"]["saved_vectors"] == 3
    assert "message_embeddings" in result["embeddings"]["storage"]


def test_comment_vectors_are_those_of_the_asked_model_and_of_profiles(
    run_db, database_url
):
    from dataclasses import replace

    from embeddings.pipeline import embed_messages

    other = replace(PRODUCTION_MODEL, revision="another-revision")

    async def scenario(sessions):
        await _seed(None, {1: [(1, "alpha beta", _utc(2026, 1, 1))]})(sessions)
        await _seed(None, {2: [(2, "gamma", _utc(2026, 1, 2))]}, collected=False)(
            sessions
        )
        await embed_messages(sessions, FakeEncoder(PRODUCTION_MODEL))
        async with sessions.begin() as session:
            await session.execute(
                text("DELETE FROM message_embeddings WHERE message_id = 1")
            )
        await embed_messages(sessions, FakeEncoder(other))

    run_db(scenario)
    store = AnalysisStore(database_url)
    try:
        # Message 1 has a vector of the other model only, message 2 is no profile's.
        assert store.comment_vectors(PRODUCTION_MODEL) == {}
        assert store.count_comment_vectors(PRODUCTION_MODEL) == 0
        vectors = store.comment_vectors(other)
        assert list(vectors) == [1] and len(vectors[1]) == 384
        assert store.count_comment_vectors(other) == 1
        assert [c["id"] for c in store.load_sources()[1]] == [1]
    finally:
        store.close()
