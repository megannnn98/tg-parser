"""In-memory registry for background Telegram comment-collection jobs."""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from parser.logger import get_logger
from parser.telegram import TelegramUser
from parser.user_collector import (
    ChannelProgress,
    UserCollectorConfig,
    UserCollectorDeps,
    collect_user_comments,
)
from parser.utils import join_name

_logger = get_logger("jobs")


class JobAlreadyRunningError(RuntimeError):
    pass


@dataclass
class CollectJob:
    job_id: str
    user_ref: int | str
    total_channels: int = 0
    state: str = "running"  # "running" | "done" | "error"
    resolved: dict | None = None
    channels: dict[str, ChannelProgress] = field(default_factory=dict)
    saved_total: int = 0
    db_name: str | None = None
    error: str | None = None

    def snapshot(self) -> dict:
        # GET /collect/{id}/status is a sync route, so FastAPI runs it in a
        # real worker thread while _run() mutates self.channels from the
        # event-loop thread. list(self.channels) snapshots the keys as one
        # atomic C call; looping a live dict view here would risk "dictionary
        # changed size during iteration" when a new channel starts mid-read.
        keys = list(self.channels)
        return {
            "job_id": self.job_id,
            "user_ref": self.user_ref,
            "total_channels": self.total_channels,
            "state": self.state,
            "resolved": self.resolved,
            "channels": [
                {
                    "channel": progress.channel,
                    "status": progress.status,
                    "saved": progress.saved,
                    "error": progress.error,
                }
                for key in keys
                if (progress := self.channels.get(key)) is not None
            ],
            "saved_total": self.saved_total,
            "db_name": self.db_name,
            "error": self.error,
        }


class JobRegistry:
    """Runs at most one collection job at a time.

    All jobs share the single pyrogram session file (parser.telegram.get_client),
    so running two collections concurrently would corrupt it - the single-slot
    guard in start() exists for that reason, not as a generic rate limit.
    """

    def __init__(
        self,
        collect_fn: Callable = collect_user_comments,
        task_factory: Callable[[object], object] = asyncio.create_task,
    ):
        self._jobs: dict[str, CollectJob] = {}
        self._running_job_id: str | None = None
        self._collect_fn = collect_fn
        self._task_factory = task_factory

    def get(self, job_id: str) -> CollectJob | None:
        return self._jobs.get(job_id)

    def start(self, data_dir: Path, channels: list[str], user_ref: int | str) -> CollectJob:
        if self._running_job_id is not None:
            raise JobAlreadyRunningError(
                "Сбор комментариев уже выполняется, подождите его завершения"
            )

        job = CollectJob(
            job_id=uuid.uuid4().hex,
            user_ref=user_ref,
            total_channels=len(channels),
        )
        self._jobs[job.job_id] = job
        self._running_job_id = job.job_id

        self._task_factory(self._run(job, data_dir, channels, user_ref))
        return job

    async def _run(
        self,
        job: CollectJob,
        data_dir: Path,
        channels: list[str],
        user_ref: int | str,
    ) -> None:
        def on_user_resolved(user: TelegramUser) -> None:
            job.resolved = {
                "tg_id": user.tg_id,
                "username": user.username,
                "display_name": join_name(user.first_name, user.last_name) or None,
            }

        def on_channel_progress(progress: ChannelProgress) -> None:
            job.channels[progress.channel] = progress

        deps = UserCollectorDeps(
            on_user_resolved=on_user_resolved,
            on_channel_progress=on_channel_progress,
        )

        try:
            db_path, saved = await self._collect_fn(
                data_dir,
                UserCollectorConfig(channels=channels),
                user_ref,
                deps,
            )
            job.saved_total = saved
            job.db_name = db_path.name
            job.state = "done"
        except asyncio.CancelledError:
            job.error = "Сбор был прерван"
            job.state = "error"
            raise
        except Exception as exc:
            _logger.exception(f"Job {job.job_id} ({user_ref!r}) failed")
            job.error = str(exc)
            job.state = "error"
        finally:
            if self._running_job_id == job.job_id:
                self._running_job_id = None
