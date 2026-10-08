from parser.position_comparison import Comparison, QuestionMatch, summarize

import asyncio
import sqlite3

import pytest


def test_agreement_score_uses_equal_question_weights_and_three_question_minimum():
    matches = [
        QuestionMatch(question=f"q{i}", result=result, explanation="e", left=None, right=None)
        for i, result in enumerate(["agreement", "agreement", "partial", "disagreement"])
    ]
    result = summarize(Comparison(tg_id=7, db_name="a_7.db", display_username="A", questions=matches))
    assert result.score == 62.5
    assert result.comparable_questions == 4
    assert result.eligible
    assert (result.agreements, result.partial, result.disagreements) == (2, 1, 1)
    result.questions = matches[:2]
    assert not summarize(result).eligible


def create_source(root, user_id, rows):
    path = root / f"user_{user_id}.db"
    with sqlite3.connect(path) as db:
        db.execute("""CREATE TABLE IF NOT EXISTS user_messages (
            id INTEGER PRIMARY KEY, tg_id INTEGER, username TEXT, channel TEXT,
            message_id INTEGER, text TEXT, date TEXT)""")
        db.executemany("INSERT INTO user_messages VALUES (?, ?, ?, ?, ?, ?, ?)",
                       [(i, user_id, f"user{user_id}", "channel", i, text, date) for i, text, date in rows])
    return path


class FakeGateway:
    model = "test-model"

    def __init__(self):
        self.calls = []
        self.fail_on = None

    async def extract(self, texts):
        from parser.position_inference import ExtractedComment, ExtractedPosition
        self.calls.append(("extract", list(texts)))
        if self.fail_on and self.fail_on in texts:
            raise RuntimeError("temporary failure")
        result = []
        for text in texts:
            if text == "unclear":
                result.append(ExtractedComment(status="ambiguous", positions=[]))
            elif text == "hello":
                result.append(ExtractedComment(status="nonpolitical", positions=[]))
            else:
                question, position = text.split(":")
                result.append(ExtractedComment(status="clear", positions=[
                    ExtractedPosition(question=question, position=position, quote=text, confidence=1)
                ]))
        return result

    async def equivalent(self, left, right):
        self.calls.append(("equivalent", left, right))
        return left == right

    async def compare(self, question, left, right):
        from parser.position_inference import PositionRelation
        self.calls.append(("compare", left.position, right.position))
        return PositionRelation(result="agreement" if left.position == right.position else "disagreement",
                                explanation="same" if left.position == right.position else "opposite")


class FakeEmbedder:
    version = "fake-embeddings-v1"

    async def encode(self, texts):
        return [[1.0, 0.0] for _ in texts]


def test_latest_clear_position_and_cached_repeated_analysis(tmp_path):
    from parser.position_analysis import PositionAnalysis
    create_source(tmp_path, 1, [
        (1, "war:support", "2025-01-01"),
        (2, "war:oppose", "2026-01-01"),
        (3, "unclear", "2026-02-01"),
        (4, "censorship:oppose", "2026-01-01"),
        (5, "tax:support", "2026-01-01"),
    ])
    create_source(tmp_path, 2, [
        (1, "war:oppose", "2026-01-01"),
        (2, "censorship:oppose", "2026-01-01"),
        (3, "tax:support", "2026-01-01"),
    ])
    gateway = FakeGateway()
    service = PositionAnalysis(tmp_path, gateway, FakeEmbedder())
    asyncio.run(service.run())
    result = service.results(1)
    assert result.ranking[0].score == 100
    assert result.ranking[0].comparable_questions == 3
    war = next(q for q in result.ranking[0].questions if q.question == "war")
    assert war.left.date == "2026-01-01"
    calls = len(gateway.calls)
    asyncio.run(service.run())
    assert len(gateway.calls) == calls


def test_failed_run_resumes_without_repaying_completed_comments(tmp_path):
    from parser.position_analysis import PositionAnalysis
    create_source(tmp_path, 1, [
        (i, f"issue{i}:support", "2026-01-01") for i in range(1, 10)
    ])
    create_source(tmp_path, 2, [(1, "issue1:support", "2026-01-01")])
    gateway = FakeGateway()
    gateway.fail_on = "issue9:support"
    service = PositionAnalysis(tmp_path, gateway, FakeEmbedder())
    with pytest.raises(RuntimeError, match="temporary"):
        asyncio.run(service.run())
    failed = service.results(1)
    assert failed.incomplete
    assert failed.progress.processed_comments >= 8
    assert failed.ranking == []
    first_batch = gateway.calls[0][1]
    gateway.fail_on = None
    asyncio.run(service.run())
    assert sum(kind == "extract" and texts == first_batch for kind, texts, *rest in gateway.calls) == 1
    assert service.results(1).progress.state == "done"


def test_new_comments_invalidate_results_and_recompute_only_affected_positions(tmp_path):
    from parser.position_analysis import PositionAnalysis
    for user_id in (1, 2, 3):
        create_source(tmp_path, user_id, [
            (i, f"issue{i}:support", "2026-01-01") for i in range(1, 4)
        ])
    gateway = FakeGateway()
    service = PositionAnalysis(tmp_path, gateway, FakeEmbedder())
    asyncio.run(service.run())
    compared = sum(call[0] == "compare" for call in gateway.calls)
    create_source(tmp_path, 1, [(4, "issue1:oppose", "2026-02-01")])
    assert service.results(1).needs_update
    asyncio.run(service.run())
    result = service.results(1)
    assert not result.needs_update
    assert result.ranking[0].score == 66.67
    # Authors 2 and 3 have identical evidence; that inference can also be reused.
    assert sum(call[0] == "compare" for call in gateway.calls) - compared == 1


def test_changed_version_retains_old_result_without_mixing(tmp_path):
    from parser.position_analysis import PositionAnalysis
    for user_id in (1, 2):
        create_source(tmp_path, user_id, [
            (i, f"issue{i}:support", "2026-01-01") for i in range(1, 4)
        ])
    service = PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())
    asyncio.run(service.run())
    gateway = FakeGateway()
    gateway.model = "test-model-v2"
    changed = PositionAnalysis(tmp_path, gateway, FakeEmbedder())
    result = changed.results(1)
    assert result.needs_update
    assert result.version == service.version
    assert result.ranking[0].score == 100
    assert gateway.calls == []
    gateway.fail_on = "issue1:support"
    with pytest.raises(RuntimeError):
        asyncio.run(changed.run())
    result = changed.results(1)
    assert result.progress.state == "error"
    assert result.version == service.version
    assert result.ranking[0].score == 100
    assert result.needs_update


def test_semantically_close_but_different_questions_are_not_merged(tmp_path):
    from parser.position_analysis import PositionAnalysis
    create_source(tmp_path, 1, [(1, "warA:support", "2026-01-01")])
    create_source(tmp_path, 2, [(1, "warB:support", "2026-01-01")])
    service = PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())
    asyncio.run(service.run())
    result = service.results(1)
    assert result.ranking == []
    assert result.insufficient[0].score is None
    assert result.insufficient[0].comparable_questions == 0


def test_equivalent_question_names_keep_stable_groups_when_new_comments_arrive(tmp_path):
    from parser.position_analysis import PositionAnalysis

    class Gateway(FakeGateway):
        async def equivalent(self, left, right):
            return {left, right} <= {"tax", "a-tax"}

    create_source(tmp_path, 1, [(1, "tax:support", "2026-01-01")])
    create_source(tmp_path, 2, [(1, "tax:support", "2026-01-01")])
    gateway = Gateway()
    service = PositionAnalysis(tmp_path, gateway, FakeEmbedder())
    asyncio.run(service.run())
    create_source(tmp_path, 1, [(2, "a-tax:oppose", "2026-02-01")])
    asyncio.run(service.run())
    result = service.results(1).insufficient[0]
    assert result.questions[0].question == "tax"
    assert result.questions[0].result == "disagreement"
    assert result.questions[0].left.date == "2026-02-01"
    assert service.results(2).insufficient[0].questions[0].right.date == "2026-02-01"


def test_distinct_positions_at_same_instant_are_excluded(tmp_path):
    from parser.position_analysis import PositionAnalysis
    create_source(tmp_path, 1, [
        (1, "tax:support", "2026-01-01"), (2, "tax:oppose", "2026-01-01")
    ])
    create_source(tmp_path, 2, [(1, "tax:support", "2026-01-01")])
    service = PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())
    asyncio.run(service.run())
    assert service.results(1).insufficient[0].comparable_questions == 0


def test_empty_profiles_do_not_require_inference(tmp_path):
    from parser.position_analysis import PositionAnalysis
    create_source(tmp_path, 1, [])
    create_source(tmp_path, 2, [])
    gateway = FakeGateway()
    service = PositionAnalysis(tmp_path, gateway, FakeEmbedder())
    asyncio.run(service.run())
    result = service.results(1)
    assert result.progress.state == "done"
    assert result.ranking == []
    assert result.insufficient[0].score is None
    assert gateway.calls == []


def test_partial_and_unclear_relations_have_separate_counts(tmp_path):
    from parser.position_analysis import PositionAnalysis
    from parser.position_inference import PositionRelation

    class Gateway(FakeGateway):
        async def compare(self, question, left, right):
            return PositionRelation(result=left.position, explanation="conditions differ")

    for user_id in (1, 2):
        create_source(tmp_path, user_id, [
            (i, f"issue{i}:{result}", "2026-01-01")
            for i, result in enumerate(["agreement", "partial", "disagreement", "unclear"], 1)
        ])
    service = PositionAnalysis(tmp_path, Gateway(), FakeEmbedder())
    asyncio.run(service.run())
    result = service.results(1).ranking[0]
    assert result.score == 50
    assert result.common_questions == 4
    assert result.comparable_questions == 3
    assert (result.agreements, result.partial, result.disagreements) == (1, 1, 1)


def test_truncated_batches_are_split_and_successful_subbatches_checkpointed(tmp_path):
    from parser.position_analysis import PositionAnalysis
    from parser.position_inference import IncompleteResponseError

    class Gateway(FakeGateway):
        async def extract(self, texts):
            if len(texts) > 2:
                raise IncompleteResponseError("output too long")
            return await super().extract(texts)

    create_source(tmp_path, 1, [
        (i, f"issue{i}:support", "2026-01-01") for i in range(1, 6)
    ])
    service = PositionAnalysis(tmp_path, Gateway(), FakeEmbedder())
    asyncio.run(service.run())
    assert service.results(1).progress.processed_comments == 5
    assert service.results(1).progress.state == "done"


def test_ranking_orders_by_score_coverage_and_id_and_keeps_only_ten(tmp_path):
    from parser.position_analysis import PositionAnalysis
    for user_id in range(1, 16):
        count = 4 if user_id in (1, 3) else 1 if user_id == 15 else 3
        create_source(tmp_path, user_id, [
            (i, f"issue{i}:support", "2026-01-01") for i in range(1, count + 1)
        ])
    service = PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())
    asyncio.run(service.run())
    result = service.results(1)
    assert [pair.tg_id for pair in result.ranking] == [3, 2, 4, 5, 6, 7, 8, 9, 10, 11]
    assert result.insufficient[0].tg_id == 15
    assert not result.insufficient[0].eligible
    assert all(pair.tg_id != 1 for pair in result.ranking)


@pytest.mark.parametrize("failure", ["quote", "truncated", "missing"])
def test_invalid_single_comment_is_isolated_cached_and_counted(tmp_path, failure, caplog):
    from parser.position_analysis import PositionAnalysis
    from parser.position_inference import IncompleteResponseError

    class Gateway(FakeGateway):
        async def extract(self, texts):
            self.calls.append(("attempt", list(texts)))
            if "bad:support" in texts:
                if failure == "truncated":
                    raise IncompleteResponseError("truncated")
                if failure == "missing":
                    return []
                values = await super().extract(texts)
                values[texts.index("bad:support")].positions[0].quote = "invented"
                return values
            return await super().extract(texts)

    create_source(tmp_path, 1, [
        (i, text, "2026-01-01") for i, text in enumerate(
            ["bad:support"] + [f"issue{i}:support" for i in range(15)], 1
        )
    ])
    gateway = Gateway()
    service = PositionAnalysis(tmp_path, gateway, FakeEmbedder())
    asyncio.run(service.run())
    result = service.results(1)
    assert result.progress.state == "done"
    assert result.progress.processed_comments == 16
    assert "Splitting extraction batch size=" in caplog.text
    assert result.progress.rejected_comments == 1
    # Splitting a bad early batch must not reduce the later normal batch.
    assert any(call[0] == "attempt" and len(call[1]) == 8 and "bad:support" not in call[1]
               for call in gateway.calls)
    count = len(gateway.calls)
    asyncio.run(service.run())
    assert len(gateway.calls) == count
    assert service.results(1).progress.rejected_comments == 1


def test_same_version_failed_update_keeps_last_complete_ranking(tmp_path):
    from parser.position_analysis import PositionAnalysis
    for user in (1, 2):
        create_source(tmp_path, user, [(i, f"issue{i}:support", "2026-01-01") for i in range(1, 4)])
    gateway = FakeGateway()
    service = PositionAnalysis(tmp_path, gateway, FakeEmbedder())
    asyncio.run(service.run())
    previous = service.results(1).ranking
    create_source(tmp_path, 1, [(4, "issue1:oppose", "2026-02-01")])
    gateway.fail_on = "issue1:oppose"
    with pytest.raises(RuntimeError):
        asyncio.run(service.run())
    result = service.results(1)
    assert result.ranking == previous
    assert not result.incomplete
    assert result.needs_update
    assert result.progress.state == "error"
    assert result.partial_results == []


def test_cached_scan_and_question_groups_use_bulk_and_individual_records(tmp_path, monkeypatch):
    from parser.position_analysis import PositionAnalysis
    create_source(tmp_path, 1, [(i, f"issue{i}:support", "2026-01-01") for i in range(1, 20)])
    service = PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())
    original_get = service.store.get

    def get(namespace, key):
        assert not namespace.endswith(":comment")
        assert not namespace.startswith("embeddings:")
        assert key != "question-groups"
        return original_get(namespace, key)

    monkeypatch.setattr(service.store, "get", get)
    asyncio.run(service.run())
    assert len(service.store.get_all(service._namespace + ":group")) == 19
    asyncio.run(service.run())


def test_legacy_invalid_response_is_retried_without_invalidating_other_cached_texts(tmp_path):
    from parser.position_analysis import PositionAnalysis
    from parser.position_store import digest
    create_source(tmp_path, 1, [(1, "hello", "2026-01-01"), (2, "unclear", "2026-01-01")])
    gateway = FakeGateway()
    service = PositionAnalysis(tmp_path, gateway, FakeEmbedder())
    service.store.put_many(service._namespace + ":comment", {
        digest("hello"): {"status": "ambiguous", "positions": [], "rejection_reason": "invalid_response"},
        digest("unclear"): {"status": "ambiguous", "positions": []},
    })
    asyncio.run(service.run())
    assert gateway.calls == [("extract", ["hello"])]
    assert service.results(1).progress.rejected_comments == 0
    assert service.store.get(service._namespace + ":comment", digest("hello"))["status"] == "nonpolitical"


def test_embeddings_read_only_requested_keys(tmp_path, monkeypatch):
    from parser.position_analysis import PositionAnalysis
    from parser.position_store import digest
    create_source(tmp_path, 1, [(1, "tax:support", "2026-01-01")])
    service = PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())
    namespace = "embeddings:" + service.embedder.version
    service.store.put(namespace, digest("tax"), [1., 0.])
    # An unrelated old cache entry must never be parsed or loaded for this run.
    with service.store.connect() as db:
        db.execute("INSERT INTO cache VALUES (?, ?, ?)", (namespace, "unused", "broken JSON"))
    original_all = service.store.get_all
    original_many = service.store.get_many
    selected = []

    def get_all(name):
        assert name != namespace
        return original_all(name)

    def get_many(name, keys):
        selected.append((name, keys))
        return original_many(name, keys)

    monkeypatch.setattr(service.store, "get_all", get_all)
    monkeypatch.setattr(service.store, "get_many", get_many)
    asyncio.run(service.run())
    assert selected == [(namespace, [digest("tax")])]
    assert service.results(1).progress.state == "done"


def test_get_many_chunks_keys_and_keeps_namespace_isolation(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from parser.position_store import PositionStore
    store = PositionStore(tmp_path)
    values = {f"key{i}": [i] for i in range(1201)}
    store.put_many("wanted", values)
    store.put("other", "key0", [999])
    queries = []
    original_connect = store.connect

    @contextmanager
    def connect():
        with original_connect() as db:
            db.set_trace_callback(queries.append)
            yield db

    monkeypatch.setattr(store, "connect", connect)
    assert store.get_many("wanted", [*values, "key0", "missing"]) == values
    assert len([q for q in queries if q.startswith("SELECT")]) == 3
    assert store.get_many("wanted", []) == {}


def test_progress_records_all_stages_counts_embeddings_and_resume(tmp_path, monkeypatch):
    import json
    from parser.position_analysis import PositionAnalysis
    for user in (1, 2):
        create_source(tmp_path, user, [(i, f"issue{i}:support", "2026-01-01") for i in range(1, 4)])

    class Embedder(FakeEmbedder):
        dimensions = 2
        model_name = "test-embedding-model"

    service = PositionAnalysis(tmp_path, FakeGateway(), Embedder())
    snapshots = []
    original_report = service._report

    async def report(checkpoint, **changes):
        await original_report(checkpoint, **changes)
        snapshots.append(json.loads(json.dumps(checkpoint["progress"])))

    monkeypatch.setattr(service, "_report", report)
    asyncio.run(service.run())
    result = service.results(1)
    assert {p["phase"] for p in snapshots} == {"comments", "embeddings", "questions", "comparisons", "done"}
    assert result.progress.processed_comments == result.progress.total_comments == 6
    assert result.progress.processed_embeddings == result.progress.total_embeddings == 3
    assert result.progress.processed_questions == result.progress.total_questions == 3
    assert result.progress.processed_relations == result.progress.total_relations == 3
    assert result.progress.processed_pairs == result.progress.total_pairs == 1
    assert result.progress.extraction_requests == 1
    assert result.progress.updated_at
    assert result.embeddings.saved_vectors == 3
    assert result.embeddings.dimensions == 2
    assert result.embeddings.storage.endswith("position-analysis.sqlite3")
    assert any(p["phase"] == "embeddings" and p["processed_embeddings"] == 0 for p in snapshots)
    assert any(p["phase"] == "embeddings" and p["processed_embeddings"] == 3 for p in snapshots)
    asyncio.run(service.run())
    progress = service.results(1).progress
    assert progress.cached_comments == 6
    assert progress.cached_embeddings == 3
    assert progress.extraction_requests == 0


def test_progress_records_safe_rejection_reason_and_split_count(tmp_path, caplog):
    from parser.position_analysis import PositionAnalysis

    class Gateway(FakeGateway):
        async def extract(self, texts):
            values = await super().extract(texts)
            if "private-secret:support" in texts:
                values[texts.index("private-secret:support")].positions[0].quote = "fabricated"
            return values

    create_source(tmp_path, 1, [(1, "private-secret:support", "2026-01-01"), (2, "hello", "2026-01-01")])
    service = PositionAnalysis(tmp_path, Gateway(), FakeEmbedder())
    asyncio.run(service.run())
    progress = service.results(1).progress
    assert progress.rejection_reasons == {"quote_mismatch": 1}
    assert progress.split_batches == 1
    assert progress.extraction_requests == 3
    assert "reason=quote_mismatch" in caplog.text
    assert "private-secret" not in caplog.text
