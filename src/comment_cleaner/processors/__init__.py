from __future__ import annotations

from typing import Protocol

from comment_cleaner.models import ProcessedMessage


class TextProcessor(Protocol):
    def process(self, message: ProcessedMessage) -> ProcessedMessage: ...
