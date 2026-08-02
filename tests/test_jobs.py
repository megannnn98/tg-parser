import asyncio
from pathlib import Path

import pytest

from web.jobs import CollectJob, JobAlreadyRunningError, JobRegistry
from parser.user_collector import ChannelProgress, UserCollectorDeps


def _task_factory(tasks: list):
    return lambda coro: tasks.append(asyncio.ensure_future(coro))


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
