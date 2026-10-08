import asyncio
import json

import httpx
import pytest

from parser.llm_config import CHAT_COMPLETIONS_URL, DEFAULT_MODEL, DEFAULT_PROVIDER
from parser.position_inference import DeepSeekPositions


@pytest.mark.parametrize("entry", ["positions", "coordinates", "corpus"])
def test_all_deepseek_clients_use_openrouter_contract(monkeypatch, entry):
    monkeypatch.setenv("OPENROUTER_API_KEY", "openrouter-test-key")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "old-direct-key")
    monkeypatch.setenv("OPENROUTER_MODEL", "deepseek/custom-model")
    monkeypatch.setenv("OPENROUTER_PROVIDER", "custom-provider/variant")
    requests = []

    def respond(request):
        requests.append(request)
        assert str(request.url) == CHAT_COMPLETIONS_URL
        assert request.headers["Authorization"] == "Bearer openrouter-test-key"
        payload = json.loads(request.content)
        assert payload["model"] == "deepseek/custom-model"
        assert payload["reasoning"] == {"enabled": False}
        assert "thinking" not in payload
        assert payload["provider"] == {
            "only": ["custom-provider/variant"], "allow_fallbacks": False, "require_parameters": True
        }
        assert payload["max_tokens"] == 8192
        if entry == "positions":
            assert payload["response_format"] == {"type": "json_object"}
            content = '{"comments":[{"id":0,"status":"nonpolitical","positions":[]}]}'
        else:
            assert "response_format" not in payload  # NDJSON is not a single JSON object.
            content = '{"axes":{}}'
        return httpx.Response(200, json={"model": payload["model"], "provider": "Test", "choices": [{
            "finish_reason": "stop", "message": {"content": content}
        }]})

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            if entry == "positions":
                result = await DeepSeekPositions(client=client).extract(["hello"])
                assert result[0].status == "nonpolitical"
            elif entry == "coordinates":
                from parser.political_coords import _call_deepseek
                assert await _call_deepseek("openrouter-test-key", ["hello"], client, 0, 1) == [{"axes": {}}]
            else:
                from src.political_model.generate_corpus import _call_deepseek
                assert await _call_deepseek(client, "openrouter-test-key", "system", "user") == '{"axes":{}}'
        assert len(requests) == 1

    asyncio.run(scenario())


def test_direct_key_is_not_sent_to_openrouter_and_missing_key_fails_before_request(monkeypatch):
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    monkeypatch.setenv("DEEPSEEK_API_KEY", "old-direct-key")

    def unexpected(request):
        raise AssertionError("Missing OpenRouter credentials must never send an HTTP request")

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(unexpected)) as client:
            with pytest.raises(RuntimeError, match="OPENROUTER_API_KEY"):
                await DeepSeekPositions(client=client).extract(["hello"])

    asyncio.run(scenario())


def test_openrouter_defaults_and_route_changes_invalidate_analysis_version(tmp_path, monkeypatch):
    from parser.position_analysis import PositionAnalysis
    from test_position_comparison import FakeEmbedder
    monkeypatch.delenv("OPENROUTER_MODEL", raising=False)
    monkeypatch.delenv("OPENROUTER_PROVIDER", raising=False)
    gateway = DeepSeekPositions()
    assert gateway.model == DEFAULT_MODEL
    assert gateway.options["provider"]["only"] == [DEFAULT_PROVIDER]
    original = PositionAnalysis(tmp_path, gateway, FakeEmbedder())
    monkeypatch.setenv("OPENROUTER_PROVIDER", "another-provider")
    moved = PositionAnalysis(tmp_path, DeepSeekPositions(), FakeEmbedder())
    assert moved.version != original.version
    # Removing the pin is explicit and has its own cache version.
    monkeypatch.setenv("OPENROUTER_PROVIDER", "")
    unpinned = PositionAnalysis(tmp_path, DeepSeekPositions(), FakeEmbedder())
    assert "only" not in unpinned.gateway.options["provider"]
    assert unpinned.version not in (moved.version, original.version)


def test_openrouter_error_response_is_resumable_and_never_cached(tmp_path, monkeypatch):
    from parser.position_analysis import PositionAnalysis
    from parser.position_store import digest
    from test_position_comparison import FakeEmbedder, create_source
    monkeypatch.setenv("OPENROUTER_API_KEY", "test")
    create_source(tmp_path, 1, [(1, "hello", "2026-01-01")])

    def respond(request):
        return httpx.Response(200, json={"error": {"code": 502, "message": "provider unavailable"}})

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            service = PositionAnalysis(tmp_path, DeepSeekPositions(client=client), FakeEmbedder())
            with pytest.raises(RuntimeError, match="OpenRouter"):
                await service.run()
            assert service.results(1).progress.state == "error"
            assert service.store.get(service._namespace + ":comment", digest("hello")) is None

    asyncio.run(scenario())
