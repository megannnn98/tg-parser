import asyncio

import pytest

from embeddings.e5 import E5Spec
from parser.local_text_analysis import LocalTextAnalysis
from parser.position_inference import DeepSeekPositions
from position_fakes import create_source, store_for

MODEL = E5Spec("local-test", "revision", 2)


def vector(text):
    return [0., 1.] if text == "beta" else [-1., 0.] if text == "opposite" else [1., 0.]


class Embedding:
    """The embedding run the web application starts before an analysis."""

    def __init__(self, store, encode=vector):
        self.store, self.encode = store, encode
        self.stored = []
        self.fail = False

    async def __call__(self):
        if self.fail:
            raise RuntimeError("local model failure")
        self.stored.append(self.store.embed(self.encode))


def analysis(tmp_path, encode=vector):
    store = store_for(tmp_path)
    return LocalTextAnalysis(store, MODEL, Embedding(store, encode))


def test_stored_vectors_rank_authors_without_openrouter(tmp_path, monkeypatch):
    async def forbidden(*args, **kwargs):
        pytest.fail("OpenRouter must not be called")
    monkeypatch.setattr(DeepSeekPositions, "_request", forbidden)
    monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01"), (2, "beta", "2026-01-02"), (3, "alpha", "2026-01-03")])
    create_source(tmp_path, 2, [(1, "alpha", "2026-01-01"), (2, "beta", "2026-01-02")])
    create_source(tmp_path, 3, [(1, "opposite", "2026-01-01")])
    service = analysis(tmp_path)
    asyncio.run(service.run())
    result = service.results(1)
    assert result.method == "text_similarity"
    assert not result.ranking  # No LLM agreement score is manufactured.
    assert result.similar_authors[0].tg_id == 2
    assert result.similar_authors[0].similarity == 1
    assert result.similar_authors[0].left_comments == 2
    assert result.similar_authors[1].similarity < 0
    assert result.progress.processed_comments == 6
    assert result.progress.processed_embeddings == 3  # One row per distinct text.
    assert result.progress.processed_pairs == 3
    assert result.embeddings.saved_vectors == 6
    assert result.embeddings.model == "local-test" and result.embeddings.dimensions == 2
    asyncio.run(service.run())
    assert service._embed_missing.stored == [6, 0]


def test_comments_without_a_vector_are_left_out(tmp_path):
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01")])
    create_source(tmp_path, 2, [(1, "alpha", "2026-01-01")])
    store = store_for(tmp_path)
    store.embed(vector)
    create_source(tmp_path, 2, [(2, "opposite", "2026-01-02")])
    create_source(tmp_path, 3, [(1, "beta", "2026-01-01")])
    service = LocalTextAnalysis(store, MODEL)  # Nothing embeds the new comments.
    asyncio.run(service.run())
    result = service.results(1)
    assert [(a.tg_id, a.similarity, a.right_comments) for a in result.similar_authors] == [(2, 1, 1)]
    assert result.progress.total_comments == 4
    assert result.progress.processed_comments == 2


def test_no_vectors_at_all_says_how_to_build_them(tmp_path):
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01")])
    service = LocalTextAnalysis(store_for(tmp_path), MODEL)
    with pytest.raises(ValueError, match="scripts.embed"):
        asyncio.run(service.run())
    assert service.results(1).progress.state == "error"
    assert "scripts.embed" in service.results(1).progress.error


def test_missing_numpy_explains_how_to_rebuild_instead_of_breaking_web(tmp_path, monkeypatch):
    import builtins
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01")])
    service = analysis(tmp_path)
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


def test_failed_update_keeps_published_similarity(tmp_path):
    for user in (1, 2):
        create_source(tmp_path, user, [(1, "alpha", "2026-01-01")])
    service = analysis(tmp_path)
    asyncio.run(service.run())
    old = service.results(1).similar_authors
    create_source(tmp_path, 1, [(2, "beta", "2026-01-02")])
    service._embed_missing.fail = True
    with pytest.raises(RuntimeError, match="local model failure"):
        asyncio.run(service.run())
    result = service.results(1)
    assert result.similar_authors == old
    assert result.needs_update and result.progress.state == "error"
    assert not result.incomplete


def test_reverse_examples_and_old_llm_cache_are_not_mixed(tmp_path):
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01")])
    create_source(tmp_path, 2, [(1, "paraphrase", "2026-01-02")])
    service = analysis(tmp_path)
    service.store.put("published", "latest", {"version": "legacy", "progress": {"state": "done"}})
    assert not service.results(1).similar_authors
    assert service.results(1).progress.state == "idle"
    asyncio.run(service.run())
    left = service.results(1).similar_authors[0].examples[0]
    right = service.results(2).similar_authors[0].examples[0]
    assert left.left == right.right
    assert left.right == right.left


@pytest.mark.parametrize("stored", [[float("nan"), 1], [0, 0], [1, 0, 0]])
def test_invalid_stored_vectors_stop_the_analysis(tmp_path, stored):
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01")])
    service = analysis(tmp_path, lambda text: stored)
    with pytest.raises(ValueError):
        asyncio.run(service.run())
    assert service.results(1).progress.state == "error"
    assert not service.results(1).similar_authors
