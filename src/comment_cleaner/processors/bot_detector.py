from __future__ import annotations

import re

from comment_cleaner.config import Config
from comment_cleaner.models import ProcessedMessage, Transformation


class BotDetector:
    def __init__(
        self,
        config: Config,
        bot_patterns: dict[str, list[str]] | None = None,
    ) -> None:
        self._remove_bots = config.remove_bot_messages
        self._remove_system = config.remove_system_messages

        self._bot_username_patterns: list[re.Pattern[str]] = []
        self._system_patterns: list[re.Pattern[str]] = []
        self._bot_message_patterns: list[re.Pattern[str]] = []

        if bot_patterns:
            for pat in bot_patterns.get("bot_username_patterns", []):
                self._bot_username_patterns.append(re.compile(pat))
            for pat in bot_patterns.get("system_message_patterns", []):
                self._system_patterns.append(re.compile(pat, re.UNICODE))
            for pat in bot_patterns.get("bot_message_patterns", []):
                self._bot_message_patterns.append(re.compile(pat, re.UNICODE))

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        text = message.cleaned_text or message.original_text

        for pattern in self._system_patterns:
            if pattern.search(text):
                message.features.is_system_message = True
                break

        for pattern in self._bot_message_patterns:
            if pattern.search(text):
                message.features.is_bot_message = True
                break

        if message.features.is_system_message or message.features.is_bot_message:
            message.transformations.append(
                Transformation(
                    type="bot_or_system_detected",
                    details={
                        "is_system": message.features.is_system_message,
                        "is_bot": message.features.is_bot_message,
                    },
                )
            )

        return message
