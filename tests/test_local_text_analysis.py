import asyncio

import httpx
import numpy as np
import pytest

from parser.local_text_analysis import LocalTextAnalysis
from parser.position_inference import DeepSeekPositions
from test_position_comparison import create_source
from web.app import create_app


class Embedder:
    version = "local-test-v1"
    dimensions = 2
    model_name = "local-test"

    def __init__(self):
        self.calls = []
        self.fail = False

    async def encode(self, texts):
        self.calls.append(list(texts))
        if self.fail and len(self.calls) == 2:
            raise RuntimeError("local model failure")
        return [[0., 1.] if text == "beta" else [-1., 0.] if text == "opposite" else [1., 0.] for text in texts]


def test_local_vectors_rank_authors_and_cache_without_openrouter(tmp_path, monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail("OpenRouter must not be called")
    monkeypatch.setattr(DeepSeekPositions, "_request", forbidden)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01"), (2, "beta", "2026-01-02"), (3, "alpha", "2026-01-03")])
    create_source(tmp_path, 2, [(1, "alpha", "2026-01-01"), (2, "beta", "2026-01-02")])
    create_source(tmp_path, 3, [(1, "opposite", "2026-01-01")])
    embedder = Embedder()
    service = LocalTextAnalysis(tmp_path, embedder)
    asyncio.run(service.run())
    result = service.results(1)
    assert result.method == "text_similarity"
    assert not result.ranking  # No LLM agreement score is manufactured.
    assert result.similar_authors[0].tg_id == 2
    assert result.similar_authors[0].similarity == 1
    assert result.similar_authors[0].left_comments == 2
    assert result.similar_authors[1].similarity < 0
    assert result.progress.processed_comments == 6
    assert result.progress.processed_embeddings == result.embeddings.saved_vectors == 3
    assert result.progress.processed_pairs == 3
    calls = len(embedder.calls)
    asyncio.run(service.run())
    assert len(embedder.calls) == calls
    assert service.results(1).progress.cached_comments == 6
    assert service.results(1).progress.cached_embeddings == 3


def test_local_failure_resumes_from_saved_vector_batches(tmp_path):
    create_source(tmp_path, 1, [(i, f"text{i}", "2026-01-01") for i in range(1, 18)])
    embedder = Embedder()
    embedder.fail = True
    service = LocalTextAnalysis(tmp_path, embedder)
    with pytest.raises(RuntimeError, match="local model failure"):
        asyncio.run(service.run())
    assert service.results(1).progress.state == "error"
    assert service.results(1).embeddings.saved_vectors == 16
    asyncio.run(service.run())
    assert len(embedder.calls[-1]) == 1
    assert service.results(1).progress.state == "done"
    assert service.results(1).progress.cached_embeddings == 16


def test_missing_numpy_explains_how_to_rebuild_instead_of_breaking_web(tmp_path, monkeypatch):
    import builtins
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01")])
    service = LocalTextAnalysis(tmp_path, Embedder())
    original = builtins.__import__

    def without_numpy(name, *args, **kwargs):
        if name == "numpy":
            raise ModuleNotFoundError("numpy missing", name="numpy")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_numpy)
    with pytest.raises(ModuleNotFoundError):
        asyncio.run(service.run())
    result = service.results(1)
    assert result.progress.state == "error"
    assert "numpy" in result.progress.error
    assert "docker build -f ci/Dockerfile -t telegram-parser ." in result.progress.error


def test_failed_local_update_keeps_published_similarity(tmp_path):
    for user in (1, 2):
        create_source(tmp_path, user, [(1, "alpha", "2026-01-01")])
    embedder = Embedder()
    service = LocalTextAnalysis(tmp_path, embedder)
    asyncio.run(service.run())
    old = service.results(1).similar_authors
    create_source(tmp_path, 1, [(2, "beta", "2026-01-02")])
    embedder.fail = True
    with pytest.raises(RuntimeError):
        asyncio.run(service.run())
    result = service.results(1)
    assert result.similar_authors == old
    assert result.needs_update and result.progress.state == "error"
    assert not result.incomplete


def test_reverse_examples_and_old_llm_cache_are_not_mixed(tmp_path):
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01")])
    create_source(tmp_path, 2, [(1, "paraphrase", "2026-01-02")])
    service = LocalTextAnalysis(tmp_path, Embedder())
    service.store.put("published", "latest", {"version": "legacy", "progress": {"state": "done"}})
    assert not service.results(1).similar_authors
    assert service.results(1).progress.state == "idle"
    asyncio.run(service.run())
    left = service.results(1).similar_authors[0].examples[0]
    right = service.results(2).similar_authors[0].examples[0]
    assert left.left == right.right
    assert left.right == right.left


@pytest.mark.parametrize("values", [[[float("nan"), 1]], [[0, 0]], [[1, 0], [0, 1]], [[1, 0, 0]]])
def test_invalid_local_embeddings_are_not_cached(tmp_path, values):
    class Invalid(Embedder):
        async def encode(self, texts):
            return values
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01")])
    service = LocalTextAnalysis(tmp_path, Invalid())
    with pytest.raises(ValueError):
        asyncio.run(service.run())
    assert service.results(1).embeddings.saved_vectors == 0


def test_local_default_api_uses_no_gateway_and_reports_real_embedding_progress(tmp_path, monkeypatch):
    import parser.local_text_analysis as module
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01")])
    entered, release = None, None

    class Waiting(Embedder):
        def __init__(self, **kwargs):
            super().__init__()
        async def encode(self, texts):
            entered.set()
            await release.wait()
            return await super().encode(texts)

    monkeypatch.setattr(module, "LocalCommentE5", Waiting)
    # Fake embedders need not expose the real model's preparation field.
    Waiting._model = object()
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    app = create_app(data_dir=tmp_path, channels=[])
    assert isinstance(app.state.position_jobs.service, LocalTextAnalysis)

    async def scenario():
        nonlocal entered, release
        entered, release = asyncio.Event(), asyncio.Event()
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.post("/api/v1/position-analysis")).status_code == 202
            await asyncio.wait_for(entered.wait(), 5)
            result = (await client.get("/api/v1/users/user_1.db/position-comparisons")).json()
            assert result["method"] == "text_similarity"
            assert result["progress"]["phase"] == "embeddings"
            assert result["progress"]["total_embeddings"] == 1
            assert result["progress"]["extraction_requests"] == 0
            release.set()
            await app.state.position_jobs.task
        await app.state.position_jobs.close()

    asyncio.run(scenario())


def test_long_comments_include_tail_and_produce_normalized_vectors(tmp_path):
    from types import SimpleNamespace
    import torch
    from parser.comment_embeddings import LocalCommentE5

    seen = []
    class Inputs(dict):
        @property
        def attention_mask(self):
            return self["attention_mask"]
        def to(self, device):
            return self

    class Tokenizer:
        def encode(self, text, **kwargs):
            return [0] if text == "query: " else [1] * 510 + [9] * 600
        def num_special_tokens_to_add(self, **kwargs):
            return 0
        def prepare_for_model(self, ids, **kwargs):
            assert len(ids) <= 512
            seen.extend(ids[1:])
            return {"input_ids": ids}
        def pad(self, rows, **kwargs):
            size = max(len(r["input_ids"]) for r in rows)
            return Inputs(input_ids=torch.tensor([r["input_ids"] + [0] * (size - len(r["input_ids"])) for r in rows]),
                          attention_mask=torch.tensor([[1] * len(r["input_ids"]) + [0] * (size - len(r["input_ids"])) for r in rows]))

    class Model:
        device = "cpu"
        def __call__(self, input_ids, **kwargs):
            assert not torch.is_grad_enabled()
            return SimpleNamespace(last_hidden_state=torch.stack([input_ids.float(), torch.ones_like(input_ids).float()], dim=-1))

    embedder = LocalCommentE5(tmp_path)
    embedder._tokenizer, embedder._model = Tokenizer(), Model()
    vector = asyncio.run(embedder.encode(["long"]))[0]
    assert seen == [1] * 510 + [9] * 600
    assert np.linalg.norm(vector) == pytest.approx(1)
    assert vector[0] / vector[1] > 2  # Tail changes the vector; no silent truncation.
