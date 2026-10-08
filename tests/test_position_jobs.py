import asyncio

import pytest

from parser.position_analysis import PositionAnalysis
from test_position_comparison import FakeEmbedder, FakeGateway, create_source
from web.position_jobs import PositionJobs


def test_single_job_guard_and_shutdown_preserve_resumable_state(tmp_path):
    create_source(tmp_path, 1, [(1, "tax:support", "2026-01-01")])

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()

        class Gateway(FakeGateway):
            async def extract(self, texts):
                entered.set()
                await release.wait()
                return await super().extract(texts)

        service = PositionAnalysis(tmp_path, Gateway(), FakeEmbedder())
        jobs = PositionJobs(service)
        assert jobs.start().state == "running"
        await entered.wait()
        with pytest.raises(RuntimeError, match="уже"):
            jobs.start()
        # A separate registry shares the filesystem guard.
        with pytest.raises(RuntimeError, match="уже"):
            PositionJobs(PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())).start()
        await jobs.close()
        assert service.results(1).progress.state == "interrupted"
        release.set()
        jobs.start()
        await jobs.task
        assert service.results(1).progress.state == "done"

    asyncio.run(scenario())


def test_start_owns_lock_before_task_runs_and_immediate_cancel_releases_it(tmp_path):
    async def scenario():
        service = PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())
        jobs = PositionJobs(service)
        other = PositionJobs(PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder()))
        jobs.start()
        assert service.results(1).progress.state == "running"
        # No yield yet: both POSTs can arrive before the first background coroutine starts.
        with pytest.raises(RuntimeError, match="уже"):
            other.start()
        await jobs.close()
        other.start()
        await other.task
        assert other.service.results(1).progress.state == "done"

    asyncio.run(scenario())


def test_cancelled_store_write_keeps_lock_until_writer_finishes(tmp_path, monkeypatch):
    import threading
    service = PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())
    entered, release = threading.Event(), threading.Event()
    original = service.store.put

    def blocked_put(namespace, key, value):
        if threading.current_thread() is not threading.main_thread():
            entered.set()
            release.wait(5)
        original(namespace, key, value)

    monkeypatch.setattr(service.store, "put", blocked_put)

    async def scenario():
        jobs = PositionJobs(service)
        jobs.start()
        try:
            while not entered.is_set():
                await asyncio.sleep(0.01)
            jobs.task.cancel()
            await asyncio.sleep(0.05)
            assert not jobs.task.done()
            with pytest.raises(RuntimeError, match="уже"):
                PositionJobs(PositionAnalysis(tmp_path, FakeGateway(), FakeEmbedder())).start()
        finally:
            release.set()
            await jobs.close()
        with service.store.analysis_lock():
            pass

    asyncio.run(scenario())
