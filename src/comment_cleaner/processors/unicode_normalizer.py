from __future__ import annotations

import re
import unicodedata

from comment_cleaner.config import Config
from comment_cleaner.models import ProcessedMessage, Transformation


class UnicodeNormalizer:
    def __init__(self, config: Config) -> None:
        self._unicode_form = config.unicode_form
        self._max_repeated_letters = config.max_repeated_letters
        self._max_repeated_punctuation = config.max_repeated_punctuation
        self._max_repeated_brackets = config.max_repeated_brackets
        self._preserve_emoji = config.preserve_emoji
        self._preserve_hashtags = config.preserve_hashtags

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        source = message.cleaned_text or message.original_text

        text = self._normalize_unicode(source)
        text = self._remove_zero_width(text)
        text = self._normalize_spaces(text)
        text = self._normalize_newlines(text)
        text = self._normalize_repeats(text)
        text = text.strip()

        if text != source:
            message.transformations.append(Transformation(type="normalize_unicode"))

        message.cleaned_text = text
        return message

    def _normalize_unicode(self, text: str) -> str:
        form = self._unicode_form
        if form:
            return unicodedata.normalize(form, text)  # type: ignore[arg-type]
        return text

    @staticmethod
    def _remove_zero_width(text: str) -> str:
        result: list[str] = []
        for ch in text:
            cp = ord(ch)
            if cp in (0x200B, 0x200C, 0x200D, 0xFEFF, 0x00AD, 0x2060, 0x180E):
                continue
            result.append(ch)
        return "".join(result)

    @staticmethod
    def _normalize_spaces(text: str) -> str:
        text = text.replace("\u00a0", " ")
        text = text.replace("\u2009", " ")
        text = text.replace("\u200a", " ")
        text = text.replace("\u202f", " ")
        text = text.replace("\u205f", " ")
        text = text.replace("\u3000", " ")
        text = re.sub(r" {2,}", " ", text)
        text = re.sub(r" +\n", "\n", text)
        text = re.sub(r"\n +", "\n", text)
        return text

    @staticmethod
    def _normalize_newlines(text: str) -> str:
        text = text.replace("\r\n", "\n")
        text = text.replace("\r", "\n")
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text

    def _normalize_repeats(self, text: str) -> str:
        text = self._limit_repeated(text, r"([а-яёА-ЯЁa-zA-Z])\1{2,}", self._max_repeated_letters)
        text = self._limit_repeated(text, r"([!?])\1{2,}", self._max_repeated_punctuation)
        text = self._limit_repeated(text, r"(\))\1{2,}", self._max_repeated_brackets)
        text = self._limit_repeated(text, r"(\()\1{2,}", self._max_repeated_brackets)
        return text

    def _limit_repeated(self, text: str, pattern: str, max_count: int) -> str:
        def _replace(m: re.Match[str]) -> str:
            return m.group(1) * max_count

        return re.sub(pattern, _replace, text)
