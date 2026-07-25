from __future__ import annotations

import re

from comment_cleaner.config import Config
from comment_cleaner.models import ProcessedMessage, SarcasmSignal, Transformation


class SarcasmDetector:
    def __init__(self, config: Config) -> None:
        self._enabled = config.sarcasm_enabled
        self._signals = config.sarcasm_signals

        self._phrase_patterns: dict[str, re.Pattern[str]] = {}
        for sig in self._signals:
            self._phrase_patterns[sig] = re.compile(
                rf"\b{re.escape(sig)}\b", re.IGNORECASE | re.UNICODE
            )

        self._quote_sarcasm = re.compile(
            r'[""'
            '"](?:хорош[ией]й?|отличн[ыо]й|прекрасн[ыо]й|замечательн[ыо]й|добр[ыо]й|честн[ыо]й)[""'
            '"'
            "]",
            re.UNICODE | re.IGNORECASE,
        )

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        if not self._enabled:
            return message

        text = message.cleaned_text or message.original_text
        signals: list[SarcasmSignal] = []

        for phrase, pattern in self._phrase_patterns.items():
            if pattern.search(text):
                signals.append(SarcasmSignal(type="phrase", value=phrase))

        if self._quote_sarcasm.search(text):
            signals.append(SarcasmSignal(type="quoted_positive_word", value="quote_sarcasm"))

        laugh_emoji = {"😂", "🤣", "😹", "🤡", "😆", "😁"}
        if message.emoji:
            laugh_count = sum(1 for e in message.emoji if e in laugh_emoji)
            if laugh_count > 0:
                signals.append(SarcasmSignal(type="emoji", value="laugh_emoji_with_text"))

        bracket_count = text.count(")")
        if bracket_count >= 2:
            signals.append(
                SarcasmSignal(type="repeated_brackets", value=f"{bracket_count} brackets")
            )

        if "/s" in text.lower():
            signals.append(SarcasmSignal(type="tone_indicator", value="/s"))

        if signals:
            message.features.possible_sarcasm = True
            message.sarcasm_signals = signals
            message.transformations.append(
                Transformation(
                    type="sarcasm_detected",
                    details={"signals": [s.model_dump() for s in signals]},
                )
            )

        return message
