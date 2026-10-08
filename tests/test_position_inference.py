import asyncio
import json

import httpx
import pytest

from parser.position_inference import DeepSeekPositions


def gateway_response(monkeypatch, payload, finish="stop"):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test")

    def respond(request):
        assert request.url == "https://openrouter.ai/api/v1/chat/completions"
        assert json.loads(request.content)["response_format"] == {"type": "json_object"}
        return httpx.Response(200, json={"model": "test", "choices": [{
            "finish_reason": finish, "message": {"content": json.dumps(payload)}
        }]})

    return httpx.AsyncClient(transport=httpx.MockTransport(respond))


def test_validated_extraction_keeps_input_order_and_nonpolitical_records(monkeypatch):
    payload = {"comments": [
        {"id": 1, "status": "nonpolitical", "positions": []},
        {"id": 0, "status": "clear", "positions": [{
            "question": "Запрет митингов", "position": "Против",
            "quote": "против запрета", "confidence": 0.95,
        }]},
    ]}

    async def scenario():
        async with gateway_response(monkeypatch, payload) as client:
            return await DeepSeekPositions(client=client).extract(["Я против запрета", "Привет"])

    result = asyncio.run(scenario())
    assert result[0].positions[0].position == "Против"
    assert result[1].status == "nonpolitical"


@pytest.mark.parametrize("payload", [
    {"comments": []},
    {"comments": [{"id": 0, "status": "clear", "positions": [{
        "question": "Митинги", "position": "За", "quote": "invented", "confidence": 0.95,
    }]}]},
    {"comments": [{"id": 0, "status": "ambiguous", "positions": [{
        "question": "Митинги", "position": "За", "quote": "Привет", "confidence": 0.95,
    }]}]},
    {"comments": [{"id": 0, "status": "clear", "positions": []}]},
])
def test_missing_comments_fabricated_quotes_and_ambiguous_positions_are_rejected(monkeypatch, payload):
    async def scenario():
        async with gateway_response(monkeypatch, payload) as client:
            with pytest.raises(ValueError):
                await DeepSeekPositions(client=client).extract(["Привет"])

    asyncio.run(scenario())


def test_truncated_provider_response_is_not_accepted(monkeypatch):
    async def scenario():
        async with gateway_response(monkeypatch, {"comments": []}, finish="length") as client:
            with pytest.raises(RuntimeError, match="неполный"):
                await DeepSeekPositions(client=client).extract(["Привет"])

    asyncio.run(scenario())


@pytest.mark.parametrize("changed_field", ["system_fingerprint", "provider"])
def test_provider_fingerprint_change_stops_before_caching_new_analysis(tmp_path, monkeypatch, changed_field):
    from parser.position_analysis import PositionAnalysis
    from test_position_comparison import FakeEmbedder, create_source

    monkeypatch.setenv("OPENROUTER_API_KEY", "test")
    identity = {"system_fingerprint": "fingerprint1", "provider": "First"}
    requests = []

    def respond(request):
        requests.append(request)
        texts = json.loads(json.loads(request.content)["messages"][1]["content"])
        payload = {"comments": [
            {"id": value["id"], "status": "nonpolitical", "positions": []} for value in texts
        ]}
        return httpx.Response(200, json={
            "model": "test-model", **identity,
            "choices": [{"finish_reason": "stop", "message": {"content": json.dumps(payload)}}]
        })

    create_source(tmp_path, 1, [(1, "hello", "2026-01-01")])

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            service = PositionAnalysis(tmp_path, DeepSeekPositions(client=client), FakeEmbedder())
            await service.run()
            create_source(tmp_path, 1, [(2, "hello2", "2026-01-01")])
            identity[changed_field] = "changed"
            with pytest.raises(RuntimeError, match="версия модели"):
                await service.run()
            assert service.results(1).progress.processed_comments == 1
            assert not service.results(1).incomplete
            assert service.results(1).progress.state == "provider_changed"
            # Subsequent retries must fail before an additional provider request.
            count = len(requests)
            with pytest.raises(RuntimeError, match="версия модели"):
                await service.run()
            from web.position_jobs import PositionJobs
            with pytest.raises(RuntimeError, match="версия модели"):
                PositionJobs(service).start()
            assert len(requests) == count
            monkeypatch.setenv("POSITION_ANALYSIS_GENERATION", "next")
            fresh = PositionAnalysis(tmp_path, DeepSeekPositions(client=client), FakeEmbedder())
            assert fresh.results(1).progress.state == "idle"
            assert fresh.store.get_all(fresh._namespace + ":comment") == {}
            before_fresh = len(requests)
            await fresh.run()
            assert fresh.results(1).progress.state == "done"
            assert len(requests) == before_fresh + 1
            sent = json.loads(json.loads(requests[-1].content)["messages"][1]["content"])
            assert [row["text"] for row in sent] == ["hello", "hello2"]
            assert len(fresh.store.get_all(fresh._namespace + ":comment")) == 2

    asyncio.run(scenario())


def test_local_embeddings_exclude_padding_normalize_and_pin_model(monkeypatch, tmp_path):
    import sys
    from types import SimpleNamespace
    from parser.position_inference import LocalE5

    torch = pytest.importorskip("torch")
    if not hasattr(torch, "tensor"):
        pytest.skip("PyTorch is not fully installed in this environment")

    class Inputs(dict):
        @property
        def attention_mask(self):
            return self["attention_mask"]

        def to(self, device):
            return self

    class Tokenizer:
        def __call__(self, texts, **kwargs):
            assert texts == ["query: first", "query: second"]
            return Inputs(attention_mask=torch.tensor([[1, 1, 0], [1, 0, 0]]))

    class Model:
        device = "cpu"

        def eval(self):
            return self

        def to(self, device):
            return self

        def __call__(self, **kwargs):
            assert not torch.is_grad_enabled()
            return SimpleNamespace(last_hidden_state=torch.tensor([
                [[2., 0.], [2., 0.], [99., 99.]],
                [[0., 3.], [99., 99.], [99., 99.]],
            ]))

    def load_tokenizer(name, **kwargs):
        assert name == "intfloat/multilingual-e5-base"
        assert kwargs["revision"] == LocalE5.revision
        assert kwargs["trust_remote_code"] is False
        assert kwargs["cache_dir"] == tmp_path
        return Tokenizer()

    monkeypatch.setitem(sys.modules, "transformers", SimpleNamespace(
        AutoTokenizer=SimpleNamespace(from_pretrained=load_tokenizer),
        AutoModel=SimpleNamespace(from_pretrained=lambda *args, **kwargs: Model()),
    ))
    result = asyncio.run(LocalE5(cache_dir=tmp_path).encode(["first", "second"]))
    assert result == [[1., 0.], [0., 1.]]


def test_comparison_sends_canonical_and_original_questions(monkeypatch):
    from parser.position_comparison import Evidence
    monkeypatch.setenv("OPENROUTER_API_KEY", "test")

    def respond(request):
        payload = json.loads(json.loads(request.content)["messages"][1]["content"])
        assert payload["question"] == "Поставки оружия Украине"
        assert payload["left"]["question"] == "Военная помощь Украине"
        assert payload["right"]["question"] == "Поставки оружия Украине"
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "stop", "message": {"content": json.dumps({
                "result": "disagreement", "explanation": "Разные позиции по поставкам"
            })}
        }]})

    async def scenario():
        left = Evidence(text="За санкции, против поставок оружия", quote="против поставок оружия",
                        date="2026-01-01", channel="channel", message_id=1,
                        position="Против", question="Военная помощь Украине")
        right = left.model_copy(update={"position": "За", "question": "Поставки оружия Украине"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await DeepSeekPositions(client=client).compare("Поставки оружия Украине", left, right)
            assert result.result == "disagreement"

    asyncio.run(scenario())


def test_transport_timeout_has_safe_nonempty_error(monkeypatch):
    monkeypatch.setenv("OPENROUTER_API_KEY", "test")

    def respond(request):
        raise httpx.ReadTimeout("", request=request)

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            with pytest.raises(RuntimeError, match="OpenRouter недоступен: ReadTimeout"):
                await DeepSeekPositions(client=client).extract(["hello"])

    asyncio.run(scenario())


def test_owned_http_client_is_reused_and_closed_after_session(monkeypatch):
    import parser.position_inference as inference
    clients = []
    original = httpx.AsyncClient

    def respond(request):
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "stop", "message": {"content": '{"comments":[]}'}
        }]})

    def client_factory(**kwargs):
        client = original(transport=httpx.MockTransport(respond), **kwargs)
        clients.append(client)
        return client

    monkeypatch.setenv("OPENROUTER_API_KEY", "test")
    monkeypatch.setattr(inference.httpx, "AsyncClient", client_factory)

    async def scenario():
        gateway = DeepSeekPositions()
        async with gateway.session():
            await gateway.extract([])
            await gateway.extract([])
            assert len(clients) == 1
            assert not clients[0].is_closed
        assert gateway.client is None
        assert clients[0].is_closed

    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["insufficient_system_resource", "aborted", "unknown_reason", None,
                                    "broken_content_json", "broken_response_json"])
def test_transient_responses_preserve_comment_for_explicit_resume(tmp_path, monkeypatch, failure):
    from parser.position_analysis import PositionAnalysis
    from parser.position_store import digest
    from test_position_comparison import FakeEmbedder, create_source
    monkeypatch.setenv("OPENROUTER_API_KEY", "test")
    calls = []

    def respond(request):
        calls.append(request)
        if len(calls) == 1 and failure == "broken_response_json":
            return httpx.Response(200, text="{broken")
        content = '{"comments":[{"id":0,"status":"nonpolitical","positions":[]}]}'
        finish = "stop"
        if len(calls) == 1:
            if failure == "broken_content_json":
                content = '{"comments":'
            else:
                finish = failure
        return httpx.Response(200, json={"model": "test", "choices": [{
            "finish_reason": finish, "message": {"content": content}
        }]})

    create_source(tmp_path, 1, [(1, "hello", "2026-01-01")])

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            service = PositionAnalysis(tmp_path, DeepSeekPositions(client=client), FakeEmbedder())
            with pytest.raises(RuntimeError, match="продолжить анализ"):
                await service.run()
            result = service.results(1)
            assert result.progress.state == "error"
            assert result.progress.rejected_comments == 0
            assert service.store.get(service._namespace + ":comment", digest("hello")) is None
            assert len(calls) == 1  # No split-and-pay cascade on a provider outage.
            await service.run()
            assert len(calls) == 2
            assert service.results(1).progress.state == "done"
            assert service.store.get(service._namespace + ":comment", digest("hello"))["status"] == "nonpolitical"

    asyncio.run(scenario())


def test_content_filter_is_isolated_and_durably_excluded(tmp_path, monkeypatch):
    from parser.position_analysis import PositionAnalysis
    from test_position_comparison import FakeEmbedder, create_source
    create_source(tmp_path, 1, [(1, "hello", "2026-01-01")])

    async def scenario():
        async with gateway_response(monkeypatch, {}, finish="content_filter") as client:
            service = PositionAnalysis(tmp_path, DeepSeekPositions(client=client), FakeEmbedder())
            await service.run()
            assert service.results(1).progress.rejected_comments == 1
            records = service.store.get_all(service._namespace + ":comment")
            assert next(iter(records.values()))["rejection_reason"] == "content_filter"

    asyncio.run(scenario())


def test_provider_identity_is_read_once_per_run_and_reloaded_between_runs(tmp_path, monkeypatch):
    from parser.position_analysis import PositionAnalysis
    from test_position_comparison import FakeEmbedder, create_source
    create_source(tmp_path, 1, [(i, f"hello{i}", "2026-01-01") for i in range(1, 18)])
    monkeypatch.setenv("OPENROUTER_API_KEY", "test")

    def respond(request):
        rows = json.loads(json.loads(request.content)["messages"][1]["content"])
        content = json.dumps({"comments": [
            {"id": row["id"], "status": "nonpolitical", "positions": []} for row in rows
        ]})
        return httpx.Response(200, json={"model": "test", "system_fingerprint": "v1", "choices": [{
            "finish_reason": "stop", "message": {"content": content}
        }]})

    async def scenario():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            service = PositionAnalysis(tmp_path, DeepSeekPositions(client=client), FakeEmbedder())
            reads = []
            original_get = service.store.get

            def get(namespace, key):
                if key == "provider-identity":
                    reads.append(key)
                return original_get(namespace, key)

            monkeypatch.setattr(service.store, "get", get)
            await service.run()
            assert len(reads) == 1  # Three extraction requests share one saved identity.
            # Simulate another worker saving a different identity between runs.
            service.store.put(service._namespace, "provider-identity", ["test", "v2", None])
            create_source(tmp_path, 1, [(18, "hello18", "2026-01-01")])
            with pytest.raises(RuntimeError, match="версия модели"):
                await service.run()
            assert len(reads) == 2

    asyncio.run(scenario())


@pytest.mark.parametrize("payload, reason", [
    ({"comments": []}, "missing_comments"),
    ({"comments": [{"id": "0", "status": "nonpolitical", "positions": []}]}, "invalid_ids"),
    ({"comments": [{"id": 0, "status": "unknown", "positions": []}]}, "invalid_schema"),
    ({"comments": [{"id": 0, "status": "clear", "positions": [{
        "question": "Q", "position": "P", "quote": "fabricated", "confidence": 1.
    }]}]}, "quote_mismatch"),
])
def test_extraction_failure_has_specific_safe_reason(monkeypatch, payload, reason):
    from parser.position_inference import InvalidResponseError

    async def scenario():
        async with gateway_response(monkeypatch, payload) as client:
            with pytest.raises(InvalidResponseError) as failure:
                await DeepSeekPositions(client=client).extract(["private-source-text"])
            assert failure.value.reason == reason
            assert "private-source-text" not in str(failure.value)

    asyncio.run(scenario())
