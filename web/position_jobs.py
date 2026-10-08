"""Single background analysis with durable checkpoints in PositionAnalysis."""
from __future__ import annotations

import asyncio
import logging

from parser.position_analysis import PositionAnalysis
from parser.position_comparison import AnalysisProgress


class PositionJobs:
    def __init__(self, service: PositionAnalysis):
        self.service = service
        self.task: asyncio.Task | None = None

    def start(self) -> AnalysisProgress:
        if self.task is not None and not self.task.done():
            raise RuntimeError("Анализ позиций уже выполняется")
        self.service.ensure_provider_unchanged()
        # Retain ownership across task scheduling: a successful POST owns the lock.
        lock = self.service.store.analysis_lock()
        lock.__enter__()
        try:
            checkpoint = self.service.initial_checkpoint()
            # GET immediately after POST must already report running, so polling starts.
            self.service.store.put("run", "latest", checkpoint)
            self.task = asyncio.create_task(self._run(checkpoint))
            # The callback also runs if cancellation happens before the coroutine starts.
            self.task.add_done_callback(lambda _: lock.__exit__(None, None, None))
        except BaseException:
            lock.__exit__(None, None, None)
            raise
        return AnalysisProgress(state="running", phase="comments")

    async def _run(self, checkpoint):
        try:
            await self.service.run(lock_held=True, checkpoint=checkpoint)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger(__name__).exception("Position analysis stopped")

    async def close(self):
        if self.task is not None and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
