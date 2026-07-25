from __future__ import annotations

import re
import unicodedata

from comment_cleaner.config import Config
from comment_cleaner.models import ProcessedMessage, Transformation
from comment_cleaner.processors.hashtag_emoji import _EMOJI_PATTERN

_CYRILLIC_RE = re.compile(r"[а-яёА-ЯЁ]")
_LATIN_RE = re.compile(r"[a-zA-Z]")
_WORD_RE = re.compile(r"[а-яёА-ЯЁa-zA-Z0-9]+", re.UNICODE)


class InformationFilter:
    def __init__(
        self,
        config: Config,
        low_info_words: set[str] | None = None,
        low_info_phrases: set[str] | None = None,
    ) -> None:
        self._remove = config.remove_low_information
        self._low_info_words = low_info_words or set()
        self._low_info_phrases = low_info_phrases or set()

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        text = message.cleaned_text or message.original_text
        stripped = text.strip()

        score = 1.0
        reasons: list[str] = []
        requires_context = False

        if not stripped:
            score = 0.0
            reasons.append("empty_text")
        elif len(stripped) <= 3 and not self._has_emoji(stripped):
            score = 0.1
            reasons.append("very_short")

        words = _WORD_RE.findall(stripped.lower())
        content_words = [w for w in words if len(w) > 1]

        if len(content_words) == 0:
            if message.emoji:
                score = 0.2
                reasons.append("emoji_only")
            elif all(ch.isspace() or unicodedata.category(ch).startswith("P") for ch in stripped):
                score = 0.05
                reasons.append("punctuation_only")
            else:
                score = 0.1
                reasons.append("no_content_words")

        if len(content_words) == 1:
            score = min(score, 0.3)
            reasons.append("single_word")

        if stripped.lower().rstrip("?!.") in self._low_info_words:
            score = min(score, 0.15)
            reasons.append("low_info_word")

        for phrase in self._low_info_phrases:
            if phrase.lower() in stripped.lower():
                score = min(score, 0.2)
                reasons.append("low_info_phrase")
                break

        if message.reply_context is not None:
            requires_context = True
        elif message.features.contains_quote:
            pass

        if message.hashtags and len(content_words) <= 2:
            score = min(score, 0.4)
            reasons.append("hashtag_dominant")

        cyrillic = bool(_CYRILLIC_RE.search(stripped))
        latin = bool(_LATIN_RE.search(stripped))
        if cyrillic and latin and len(content_words) <= 3:
            score = min(score, 0.5)
            reasons.append("mixed_script_short")

        message.information_score = max(0.0, min(1.0, score))
        message.requires_context = requires_context

        if message.information_score < 0.3 or (
            message.information_score < 0.5 and requires_context
        ):
            message.features.low_information = True

        if message.information_score < 0.8:
            message.transformations.append(
                Transformation(
                    type="information_score",
                    details={
                        "score": message.information_score,
                        "reasons": reasons,
                        "requires_context": requires_context,
                    },
                )
            )

        return message

    @staticmethod
    def _has_emoji(text: str) -> bool:
        return bool(_EMOJI_PATTERN.search(text))
