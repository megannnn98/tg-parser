import asyncio
from pathlib import Path

import pytest

from web.jobs import CollectJob, JobAlreadyRunningError, JobRegistry
from parser.user_collector import ChannelProgress, UserCollectorDeps


def _task_factory(tasks: list):
    def factory(coro):
        task = asyncio.ensure_future(coro)
        tasks.append(task)
        return task

    return factory


def test_start_runs_job_to_completion_and_reports_result(tmp_path: Path):
    tasks: list = []

    async def fake_collect(data_dir, cfg, user_ref, deps):
        deps.on_channel_progress(ChannelProgress(channel="chan_a", status="started"))
        deps.on_channel_progress(
            ChannelProgress(channel="chan_a", status="done", saved=2)
        )
        return tmp_path / "vasya_555.db", 2

    registry = JobRegistry(collect_fn=fake_collect, task_factory=_task_factory(tasks))

    async def _run():
        job = registry.start(tmp_path, ["chan_a"], "@vasya")
        await tasks[0]
        return job

    job = asyncio.run(_run())

    assert job.state == "done"
    assert job.saved_total == 2
    assert job.db_name == "vasya_555.db"
    assert registry.get(job.job_id) is job
    assert job.snapshot()["channels"] == [
        {"channel": "chan_a", "status": "done", "saved": 2, "error": None}
    ]


def test_start_records_total_channels_for_progress_reporting(tmp_path: Path):
    tasks: list = []

    async def fake_collect(data_dir, cfg, user_ref, deps):
        return tmp_path / "vasya_555.db", 0

    registry = JobRegistry(collect_fn=fake_collect, task_factory=_task_factory(tasks))

    async def _run():
        job = registry.start(tmp_path, ["chan_a", "chan_b", "chan_c"], "@vasya")
        await tasks[0]
        return job

    job = asyncio.run(_run())

    assert job.total_channels == 3
    assert job.snapshot()["total_channels"] == 3


def test_start_reports_user_resolved(tmp_path: Path):
    tasks: list = []

    async def fake_collect(data_dir, cfg, user_ref, deps):
        from parser.telegram import TelegramUser

        deps.on_user_resolved(
            TelegramUser(tg_id=555, username="vasya", first_name="V", last_name=None)
        )
        return tmp_path / "vasya_555.db", 0

    registry = JobRegistry(collect_fn=fake_collect, task_factory=_task_factory(tasks))

    async def _run():
        job = registry.start(tmp_path, ["chan_a"], "@vasya")
        await tasks[0]
        return job

    job = asyncio.run(_run())

    assert job.resolved == {"tg_id": 555, "username": "vasya", "display_name": "V"}


def test_start_marks_job_as_error_on_exception(tmp_path: Path):
    tasks: list = []

    async def fake_collect(data_dir, cfg, user_ref, deps):
        raise RuntimeError("Cannot resolve user '@ghost'")

    registry = JobRegistry(collect_fn=fake_collect, task_factory=_task_factory(tasks))

    async def _run():
        job = registry.start(tmp_path, ["chan_a"], "@ghost")
        await tasks[0]
        return job

    job = asyncio.run(_run())

    assert job.state == "error"
    assert job.error == "Cannot resolve user '@ghost'"


def test_start_rejects_second_job_while_one_is_running(tmp_path: Path):
    tasks: list = []
    release = asyncio.Event()

    async def blocking_collect(data_dir, cfg, user_ref, deps):
        await release.wait()
        return tmp_path / "a.db", 0

    registry = JobRegistry(
        collect_fn=blocking_collect, task_factory=_task_factory(tasks)
    )

    async def _run():
        registry.start(tmp_path, ["chan_a"], "@first")

        with pytest.raises(JobAlreadyRunningError):
            registry.start(tmp_path, ["chan_a"], "@second")

        release.set()
        await tasks[0]

        # The slot is free again once the first job finished.
        registry.start(tmp_path, ["chan_a"], "@third")
        await tasks[1]

    asyncio.run(_run())


def test_get_returns_none_for_unknown_job(tmp_path: Path):
    registry = JobRegistry(collect_fn=None, task_factory=lambda _coro: None)

    assert registry.get("does-not-exist") is None


class _MidIterationMutatingDict(dict):
    """Deterministically simulates another thread inserting a new channel
    while something iterates this dict's values() - the exact race
    GET /collect/{id}/status (sync route, runs in a worker thread) can hit
    against on_channel_progress (runs on the event-loop thread)."""

    def values(self):
        it = iter(dict.values(self))
        first = next(it, None)
        if first is not None:
            yield first
        self["injected_mid_iteration"] = ChannelProgress(
            channel="injected_mid_iteration", status="started"
        )
        yield from it


def test_snapshot_survives_channel_added_while_reading(tmp_path: Path):
    job = CollectJob(job_id="x", user_ref="u")
    job.channels = _MidIterationMutatingDict(
        chan_a=ChannelProgress(channel="chan_a", status="started"),
        chan_b=ChannelProgress(channel="chan_b", status="started"),
    )

    # Must not raise "dictionary changed size during iteration". snapshot()
    # reads only the keys present at call time - the channel the "other
    # thread" injects mid-call is picked up by the *next* snapshot() instead.
    result = job.snapshot()

    assert [c["channel"] for c in result["channels"]] == ["chan_a", "chan_b"]


def test_start_marks_job_as_error_on_cancellation(tmp_path: Path):
    tasks: list = []
    started = asyncio.Event()

    async def blocking_collect(data_dir, cfg, user_ref, deps):
        started.set()
        await asyncio.sleep(10)
        return tmp_path / "a.db", 0

    registry = JobRegistry(
        collect_fn=blocking_collect, task_factory=_task_factory(tasks)
    )

    async def _run():
        job = registry.start(tmp_path, ["chan_a"], "@vasya")
        await started.wait()
        tasks[0].cancel()
        with pytest.raises(asyncio.CancelledError):
            await tasks[0]
        return job

    job = asyncio.run(_run())

    assert job.state == "error"
    assert job.error == "Сбор был прерван"


def test_cancel_stops_running_job_and_marks_it_as_error(tmp_path: Path):
    tasks: list = []
    started = asyncio.Event()

    async def blocking_collect(data_dir, cfg, user_ref, deps):
        started.set()
        await asyncio.sleep(10)
        return tmp_path / "a.db", 0

    registry = JobRegistry(
        collect_fn=blocking_collect, task_factory=_task_factory(tasks)
    )

    async def _run():
        job = registry.start(tmp_path, ["chan_a"], "@vasya")
        await started.wait()

        cancelled = registry.cancel(job.job_id)

        with pytest.raises(asyncio.CancelledError):
            await tasks[0]
        return job, cancelled

    job, cancelled = asyncio.run(_run())

    assert cancelled is True
    assert job.state == "error"
    assert job.error == "Сбор был прерван"


def test_cancel_returns_false_for_unknown_job(tmp_path: Path):
    registry = JobRegistry(collect_fn=None, task_factory=lambda _coro: None)

    assert registry.cancel("does-not-exist") is False


def test_cancel_returns_false_for_already_finished_job(tmp_path: Path):
    tasks: list = []

    async def fake_collect(data_dir, cfg, user_ref, deps):
        return tmp_path / "vasya_555.db", 0

    registry = JobRegistry(collect_fn=fake_collect, task_factory=_task_factory(tasks))

    async def _run():
        job = registry.start(tmp_path, ["chan_a"], "@vasya")
        await tasks[0]
        return job

    job = asyncio.run(_run())

    assert registry.cancel(job.job_id) is False


def test_start_reports_mixed_channel_outcomes(tmp_path: Path):
    tasks: list = []

    async def fake_collect(data_dir, cfg, user_ref, deps):
        deps.on_channel_progress(ChannelProgress(channel="chan_a", status="started"))
        deps.on_channel_progress(
            ChannelProgress(channel="chan_a", status="done", saved=5)
        )
        deps.on_channel_progress(ChannelProgress(channel="chan_b", status="started"))
        deps.on_channel_progress(
            ChannelProgress(channel="chan_b", status="failed", error="boom")
        )
        return tmp_path / "vasya_555.db", 5

    registry = JobRegistry(collect_fn=fake_collect, task_factory=_task_factory(tasks))

    async def _run():
        job = registry.start(tmp_path, ["chan_a", "chan_b"], "@vasya")
        await tasks[0]
        return job

    job = asyncio.run(_run())

    assert job.state == "done"
    assert job.snapshot()["channels"] == [
        {"channel": "chan_a", "status": "done", "saved": 5, "error": None},
        {"channel": "chan_b", "status": "failed", "saved": 0, "error": "boom"},
    ]
