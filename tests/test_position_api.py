import asyncio

import httpx

from parser.position_analysis import PositionAnalysis
from test_position_comparison import FakeEmbedder, FakeGateway, create_source
from web.app import create_app


def test_http_start_and_results_use_background_analysis(tmp_path):
    for user_id in (1, 2):
        create_source(tmp_path, user_id, [
            (i, f"issue{i}:support", "2026-01-01") for i in range(1, 4)
        ])
    service = PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())
    app = create_app(data_dir=tmp_path, channels=[], position_service=service)

    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        extract = service.gateway.extract

        async def waiting_extract(texts):
            entered.set()
            await release.wait()
            return await extract(texts)

        service.gateway.extract = waiting_extract
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            missing = await client.get("/api/v1/users/ghost_5.db/position-comparisons")
            assert missing.status_code == 404
            result = await client.get("/api/v1/users/user_1.db/position-comparisons")
            assert result.json()["progress"]["state"] == "idle"
            assert result.json()["embeddings"]["saved_vectors"] == 0
            start = await client.post("/api/v1/position-analysis")
            assert start.status_code == 202
            await asyncio.wait_for(entered.wait(), timeout=5)
            running = await client.get("/api/v1/users/user_1.db/position-comparisons")
            progress = running.json()["progress"]
            assert progress["state"] == "running"
            assert progress["total_comments"] == 6
            assert progress["extraction_requests"] == 1
            assert "OpenRouter" in progress["activity"]
            release.set()
            await app.state.position_jobs.task
            result = await client.get("/api/v1/users/user_1.db/position-comparisons")
            assert result.status_code == 200
            assert result.json()["ranking"][0]["score"] == 100
            assert result.json()["ranking"][0]["questions"][0]["left"]["text"]
            assert result.json()["progress"]["processed_relations"] == 3
            assert result.json()["progress"]["processed_embeddings"] == 3
            assert result.json()["embeddings"]["saved_vectors"] == 3
        await app.state.position_jobs.close()

    asyncio.run(scenario())
