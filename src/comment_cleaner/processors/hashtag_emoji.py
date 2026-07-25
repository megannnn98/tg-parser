from __future__ import annotations

import re

from comment_cleaner.config import Config
from comment_cleaner.models import ProcessedMessage

_HASHTAG_PATTERN = re.compile(r"#([\w\u0400-\u04FF_]+)", re.UNICODE)
_EMOJI_PATTERN = re.compile(
    "["
    "\U0001f600-\U0001f64f"
    "\U0001f300-\U0001f5ff"
    "\U0001f680-\U0001f6ff"
    "\U0001f1e0-\U0001f1ff"
    "\U00002600-\U000026ff"
    "\U00002700-\U000027bf"
    "\U0000fe00-\U0000fe0f"
    "\U0001f900-\U0001f9ff"
    "\U0001fa00-\U0001fa6f"
    "\U0001fa70-\U0001faff"
    "\U0000200d"
    "\U000020e3"
    "\U0000fe0f"
    "\U0001f700-\U0001f77f"
    "\U0001f780-\U0001f7ff"
    "\U0001f800-\U0001f8ff"
    "]+",
    re.UNICODE,
)


class HashtagEmojiProcessor:
    def __init__(self, config: Config) -> None:
        self._preserve_hashtags = config.preserve_hashtags

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        text = message.cleaned_text or message.original_text

        hashtags = _HASHTAG_PATTERN.findall(text)
        if hashtags:
            message.hashtags = list(hashtags)

        emojis = _EMOJI_PATTERN.findall(text)
        if emojis:
            message.features.contains_emoji = True
            message.emoji = emojis
            message.emoji_count = len(emojis)

        return message
