from __future__ import annotations

import hashlib
import re
from collections import deque

from rapidfuzz import fuzz

from comment_cleaner.config import Config
from comment_cleaner.models import ProcessedMessage, Transformation

_MAX_FUZZY_TEXTS = 10_000


def _normalize_for_hash(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"https?://\S+", "[URL]", text)
    text = re.sub(r"@\w+", "[MENTION]", text)
    return text


def _compute_hash(text: str) -> str:
    return hashlib.blake2b(text.encode("utf-8"), digest_size=8).hexdigest()


class DuplicateDetector:
    def __init__(self, config: Config) -> None:
        self._mode = config.duplicate_mode
        self._fuzzy_enabled = config.fuzzy_enabled
        self._fuzzy_threshold = config.fuzzy_threshold
        self._seen_hashes: dict[str, str] = {}
        self._seen_normalized: dict[str, str] = {}
        self._counts: dict[str, int] = {}
        self._fuzzy_texts: deque[tuple[str, str]] = deque(maxlen=_MAX_FUZZY_TEXTS)

    def _register_hashes(
        self,
        message_id: str,
        exact_hash: str,
        norm_hash: str,
        normalized: str,
        add_to_fuzzy: bool = False,
    ) -> None:
        self._seen_hashes[exact_hash] = message_id
        self._seen_normalized[norm_hash] = message_id
        self._counts[exact_hash] = 1
        self._counts[norm_hash] = 1

        if add_to_fuzzy and self._fuzzy_enabled:
            self._fuzzy_texts.append((message_id, normalized))

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        if self._mode == "keep":
            return message

        normalized = _normalize_for_hash(message.cleaned_text or message.original_text)
        norm_hash = _compute_hash(normalized)
        exact_hash = _compute_hash(message.original_text.strip())

        if exact_hash in self._seen_hashes:
            first_id = self._seen_hashes[exact_hash]
            self._counts[exact_hash] = self._counts.get(exact_hash, 1) + 1
            message.is_duplicate = True
            message.features.is_duplicate = True
            message.duplicate_of = first_id
            message.duplicate_count = self._counts[exact_hash]
            message.duplicate_type = "exact"
            message.transformations.append(
                Transformation(
                    type="duplicate_detected",
                    details={"duplicate_of": first_id, "type": "exact"},
                )
            )
            return message

        if norm_hash in self._seen_normalized:
            first_id = self._seen_normalized[norm_hash]
            self._counts[norm_hash] = self._counts.get(norm_hash, 1) + 1
            message.is_duplicate = True
            message.features.is_duplicate = True
            message.duplicate_of = first_id
            message.duplicate_count = self._counts[norm_hash]
            message.duplicate_type = "normalized"
            message.transformations.append(
                Transformation(
                    type="duplicate_detected",
                    details={"duplicate_of": first_id, "type": "normalized"},
                )
            )
            return message

        if self._fuzzy_enabled:
            cmp_text = normalized[:500]
            for first_id, existing_text in self._fuzzy_texts:
                score = fuzz.ratio(cmp_text, existing_text[:500])
                if score >= self._fuzzy_threshold:
                    # Register hashes so future exact/normalized dupes of THIS message
                    # are still detected. Do NOT add to _fuzzy_texts pool —
                    # only canonical unique messages go there to prevent dilution.
                    self._register_hashes(message.message_id, exact_hash, norm_hash, normalized)
                    fuzzy_key = f"fuzzy_{first_id}"
                    self._counts[fuzzy_key] = self._counts.get(fuzzy_key, 1) + 1
                    message.is_duplicate = True
                    message.features.is_duplicate = True
                    message.duplicate_of = first_id
                    message.duplicate_count = self._counts[fuzzy_key]
                    message.duplicate_type = "fuzzy"
                    message.transformations.append(
                        Transformation(
                            type="duplicate_detected",
                            details={
                                "duplicate_of": first_id,
                                "type": "fuzzy",
                                "score": score,
                            },
                        )
                    )
                    return message

        self._register_hashes(
            message.message_id, exact_hash, norm_hash, normalized, add_to_fuzzy=True
        )
        return message
