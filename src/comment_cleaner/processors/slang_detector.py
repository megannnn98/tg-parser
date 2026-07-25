from __future__ import annotations

import re
from typing import Any

from comment_cleaner.config import Config
from comment_cleaner.models import DetectedTerm, ProcessedMessage


class SlangDetector:
    def __init__(
        self,
        config: Config,
        slang_dict: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._slang_dict: dict[str, dict[str, Any]] = slang_dict or {}
        self._patterns: dict[str, re.Pattern[str]] = {}
        self._multiwords: dict[str, dict[str, Any]] = {}

        for term, info in self._slang_dict.items():
            if not isinstance(info, dict):
                continue
            if info.get("multiword"):
                self._multiwords[term.lower()] = info
            else:
                self._patterns[term.lower()] = re.compile(
                    rf"\b{re.escape(term)}\b", re.IGNORECASE | re.UNICODE
                )

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        text = message.original_text

        for term, info in self._multiwords.items():
            if term.lower() in text.lower():
                message.detected_terms.append(
                    DetectedTerm(
                        term=term,
                        category=info.get("category"),
                    )
                )

        for term, pattern in self._patterns.items():
            if pattern.search(text):
                message.detected_terms.append(
                    DetectedTerm(
                        term=term,
                        category=self._slang_dict.get(term, {}).get("category"),
                    )
                )

        if message.detected_terms:
            message.features.contains_political_terms = True

        return message
