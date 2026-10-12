"""Offline Russian sarcasm probability for individual comments."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SarcasmSpec:
    name: str
    revision: str
    max_tokens: int = 512


RUBERT_TINY_SARCASM = SarcasmSpec(
    "Kostya165/rubert_tiny2_russian_emotion_sentiment",
    "aee7265a88462257c559d2ae3c28d8830357ee0f",
)


class RussianSarcasmDetector:
    def __init__(self, spec: SarcasmSpec = RUBERT_TINY_SARCASM, cache_dir: Path | None = None,
                 threshold: float = 0.55):
        self.spec, self._cache_dir, self._threshold = spec, cache_dir, threshold
        self._tokenizer = self._model = None

    def __call__(self, texts: list[str]) -> list[float]:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer
        if not texts:
            return []
        if self._tokenizer is None:
            self._tokenizer = AutoTokenizer.from_pretrained(self.spec.name, revision=self.spec.revision,
                cache_dir=self._cache_dir, local_files_only=True, trust_remote_code=False)
        if self._model is None:
            self._model = AutoModelForSequenceClassification.from_pretrained(self.spec.name, revision=self.spec.revision,
                cache_dir=self._cache_dir, local_files_only=True, trust_remote_code=False, use_safetensors=True).eval()
        sarcasm_id = next(int(key) for key, label in self._model.config.id2label.items() if label.lower() == "sarcasm")
        result = []
        for start in range(0, len(texts), 32):
            inputs = self._tokenizer(texts[start:start + 32], padding=True, truncation=True,
                max_length=self.spec.max_tokens, return_tensors="pt")
            with torch.inference_mode():
                result.extend(float(row[sarcasm_id]) for row in self._model(**inputs).logits.float().softmax(dim=-1))
        return result
