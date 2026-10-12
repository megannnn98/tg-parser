"""Local cross-encoder used to order already retrieved comment pairs."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RerankerSpec:
    name: str
    revision: str
    max_tokens: int = 512


MMARCO_MINILM = RerankerSpec(
    "cross-encoder/mmarco-mMiniLMv2-L12-H384-v1",
    "1427fd652930e4ba29e8149678df786c240d8825",
)


class CrossEncoderReranker:
    """Score (query, passage) pairs with a sequence-classification model.

    The raw logits are intentionally returned: they only order candidates and
    are not probabilities or an author-similarity score.
    """

    def __init__(self, spec: RerankerSpec = MMARCO_MINILM, cache_dir: Path | None = None,
                 device: str | None = None, batch_size: int = 32):
        self.spec = spec
        self._cache_dir = cache_dir
        self._device = device
        self._batch_size = batch_size
        self._tokenizer = None
        self._model = None

    @property
    def tokenizer(self):
        if self._tokenizer is None:
            from transformers import AutoTokenizer
            self._tokenizer = AutoTokenizer.from_pretrained(
                self.spec.name, revision=self.spec.revision, trust_remote_code=False,
                cache_dir=self._cache_dir, local_files_only=True,
            )
        return self._tokenizer

    def _load_model(self):
        if self._model is None:
            import torch
            from transformers import AutoModelForSequenceClassification
            device = self._device or ("cuda" if torch.cuda.is_available() else "cpu")
            model = AutoModelForSequenceClassification.from_pretrained(
                self.spec.name, revision=self.spec.revision, trust_remote_code=False,
                use_safetensors=True, cache_dir=self._cache_dir, local_files_only=True,
            ).eval()
            self._model = model.to(device)
        return self._model

    def __call__(self, pairs: list[tuple[str, str]]) -> list[float]:
        import torch

        if not pairs:
            return []
        model = self._load_model()
        scores: list[float] = []
        for start in range(0, len(pairs), self._batch_size):
            batch = pairs[start:start + self._batch_size]
            inputs = self.tokenizer(
                [left for left, _ in batch], [right for _, right in batch],
                padding=True, truncation=True, max_length=self.spec.max_tokens,
                return_tensors="pt",
            ).to(model.device)
            with torch.inference_mode():
                logits = model(**inputs).logits.squeeze(-1)
            scores.extend(float(value) for value in logits.float().cpu())
        if len(scores) != len(pairs):
            raise ValueError("Reranker вернул неполный набор оценок")
        return scores
