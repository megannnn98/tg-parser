from __future__ import annotations

import re

from comment_cleaner.config import Config
from comment_cleaner.models import ProcessedMessage, Transformation

_MENTION_PATTERN = re.compile(r"(?<!\w)@([A-Za-z0-9_]{4,})(?!\w)")


class MentionProcessor:
    def __init__(self, config: Config) -> None:
        self._replace = config.mention_replace
        self._marker = config.mention_marker

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        text = message.cleaned_text or message.original_text
        mentions = _MENTION_PATTERN.findall(text)

        if not mentions:
            return message

        message.features.contains_mention = True

        for username in mentions:
            original = f"@{username}"
            message.mentions.append(username)

            if self._replace:
                text = re.sub(
                    rf"(?<!\w)@{re.escape(username)}(?!\w)",
                    self._marker,
                    text,
                )
                message.transformations.append(
                    Transformation(
                        type="replace_mention",
                        original=original,
                        replacement=self._marker,
                    )
                )

        marker_escaped = re.escape(self._marker)
        text = re.sub(
            rf"{marker_escaped} +{marker_escaped}", f"{self._marker} {self._marker}", text
        )
        message.cleaned_text = text
        return message
