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
    service = LocalTextAnalysis(store, MODEL, Embedding(store, encode))
    service.example_min_chars = 0  # The comments of these tests are single words.
    return service


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
    # The third author is on the other side of the average: not a similar one.
    assert [a.tg_id for a in result.similar_authors] == [2]
    assert result.similar_authors[0].similarity == 1
    assert result.similar_authors[0].left_comments == 2
    assert result.progress.processed_comments == 6
    assert result.progress.processed_embeddings == 3  # One row per distinct text.
    assert result.progress.processed_pairs == 3
    assert result.embeddings.saved_vectors == 6
    assert result.embeddings.model == "local-test" and result.embeddings.dimensions == 2
    asyncio.run(service.run())
    assert service._embed_missing.stored == [6, 0]


def test_what_all_authors_share_is_removed_before_comparing(tmp_path):
    # Uncentered, every pair here has a cosine above 0.97.
    close = {"a": [1, 0.1], "b": [1, 0.12], "c": [1, -0.1]}
    for user, text in enumerate(close, start=1):
        create_source(tmp_path, user, [(1, text, "2026-01-01")])
    service = analysis(tmp_path, close.__getitem__)
    asyncio.run(service.run())
    [near] = service.results(1).similar_authors
    assert near.tg_id == 2 and near.similarity > 0.9
    assert not service.results(3).similar_authors


@pytest.mark.parametrize("texts_of_first", [1, 4])
def test_a_prolific_author_does_not_define_what_is_shared(tmp_path, texts_of_first):
    by_letter = {"a": [1, 0], "b": [0, 1], "c": [0.1, 1]}
    create_source(tmp_path, 1, [(i, f"a{i}", "2026-01-01") for i in range(texts_of_first)])
    create_source(tmp_path, 2, [(1, "b", "2026-01-01")])
    create_source(tmp_path, 3, [(1, "c", "2026-01-01")])
    service = analysis(tmp_path, lambda text: by_letter[text[0]])
    asyncio.run(service.run())
    # The same whether the first author wrote one such comment or four.
    assert service.results(2).similar_authors[0].similarity == 0.9888


def examples_of(tmp_path, left, right, vectors, min_chars=0):
    """(similarity, left text, right text) of the examples shown for two authors."""
    for user, texts in ((1, left), (2, right), (3, ["z"])):
        create_source(tmp_path, user, [(i, text, "2026-01-01") for i, text in enumerate(texts)])
    service = analysis(tmp_path, lambda text: vectors.get(text[0], [0, 1]))
    service.example_min_chars = min_chars
    asyncio.run(service.run())
    [author] = service.results(1).similar_authors
    return [(e.similarity, e.left.text, e.right.text) for e in author.examples]


def test_examples_are_the_closest_comments_and_none_is_shown_twice(tmp_path):
    vectors = {"a": [1, 0], "b": [1, 0.1], "c": [1, 0.5], "d": [1, 0.2], "e": [1, 0.6]}
    assert examples_of(tmp_path, ["a", "b", "c"], ["d", "e"], vectors) == [
        (0.9971, "c", "e"),
        (0.9952, "b", "d"),  # "a" is close to "d" too, but "d" is taken.
    ]


def test_one_comment_close_to_several_is_shown_once(tmp_path):
    vectors = {"a": [1, 0], "d": [1, 0.2], "e": [1, 0.6]}
    assert examples_of(tmp_path, ["a"], ["d", "e"], vectors) == [(0.9806, "a", "d")]


def test_a_text_both_authors_wrote_is_not_an_example(tmp_path):
    vectors = {"a": [1, 0], "b": [1, 0.3]}
    assert examples_of(tmp_path, ["a"], ["a", "b"], vectors) == [(0.9578, "a", "b")]


def test_short_comments_are_not_examples(tmp_path):
    vectors = {"a": [1, 0], "b": [1, 0.3], "c": [1, 0.1]}
    long_a, long_b = "a" * 80, "b" * 80
    assert examples_of(tmp_path, [long_a, "a" * 79], [long_b, "c"], vectors, min_chars=80) == [
        (0.9578, long_a, long_b)
    ]
    assert LocalTextAnalysis.example_min_chars == 80


def test_authors_who_only_say_what_everyone_says_are_not_ranked(tmp_path):
    for user in (1, 2, 3):
        create_source(tmp_path, user, [(1, "alpha", "2026-01-01")])
    service = analysis(tmp_path, lambda text: [0.6, 0.8])
    asyncio.run(service.run())
    assert service.results(1).progress.state == "done"
    assert not service.results(1).similar_authors


def test_comments_without_a_vector_are_left_out(tmp_path):
    create_source(tmp_path, 1, [(1, "alpha", "2026-01-01")])
    create_source(tmp_path, 2, [(1, "alpha", "2026-01-01")])
    create_source(tmp_path, 4, [(1, "beta", "2026-01-01")])
    store = store_for(tmp_path)
    store.embed(vector)
    create_source(tmp_path, 2, [(2, "opposite", "2026-01-02")])
    create_source(tmp_path, 3, [(1, "beta", "2026-01-01")])
    service = LocalTextAnalysis(store, MODEL)  # Nothing embeds the new comments.
    asyncio.run(service.run())
    result = service.results(1)
    assert [(a.tg_id, a.similarity, a.right_comments) for a in result.similar_authors] == [(2, 1, 1)]
    assert result.progress.total_comments == 5
    assert result.progress.processed_comments == 3


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
    create_source(tmp_path, 3, [(1, "beta", "2026-01-01")])
    service = analysis(tmp_path)
    asyncio.run(service.run())
    old = service.results(1).similar_authors
    assert [a.tg_id for a in old] == [2]
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
    create_source(tmp_path, 3, [(1, "beta", "2026-01-03")])
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
