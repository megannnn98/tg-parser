"""Local NLI check for whether two retrieved comments contradict each other."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


Relation = Literal["contradiction", "unclear"]


@dataclass(frozen=True)
class StanceJudgement:
    relation: Relation
    confidence: float


@dataclass(frozen=True)
class NliSpec:
    name: str
    revision: str
    max_tokens: int = 512


MDEBERTA_XNLI = NliSpec(
    "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli",
    "ee8e8145c734bf44203840b78a5996ad64660846",
)


class NliStanceVerifier:
    """Mark direct contradictions in comment pairs using locally cached NLI weights.

    NLI entailment is too narrow to prove that two political statements agree:
    two independently phrased but compatible views are often labelled neutral.
    Therefore this verifier only makes the stronger, useful decision -- whether
    a pair contradicts -- and leaves every other pair as ``unclear``.
    """

    def __init__(self, spec: NliSpec = MDEBERTA_XNLI, cache_dir: Path | None = None,
                 device: str | None = None, batch_size: int = 16, threshold: float = 0.65):
        self.spec = spec
        self._cache_dir = cache_dir
        self._device = device
        self._batch_size = batch_size
        self._threshold = threshold
        self._tokenizer = None
        self._model = None

    @property
    def tokenizer(self):
        if self._tokenizer is None:
            from transformers import DebertaV2Tokenizer
            self._tokenizer = DebertaV2Tokenizer.from_pretrained(
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
                cache_dir=self._cache_dir, local_files_only=True, use_safetensors=False,
            ).eval()
            labels = {label.lower() for label in model.config.id2label.values()}
            if "contradiction" not in labels:
                raise ValueError("NLI-модель не содержит класс contradiction")
            self._model = model.to(device)
        return self._model

    def __call__(self, pairs: list[tuple[str, str]]) -> list[StanceJudgement]:
        import torch

        if not pairs:
            return []
        model = self._load_model()
        contradiction_id = next(
            int(key) for key, label in model.config.id2label.items() if label.lower() == "contradiction"
        )
        scores: list[float] = []
        # NLI is directional. Average both directions so the displayed result
        # does not depend on which profile happened to be selected first.
        directional = [pair for pair in pairs for pair in (pair, (pair[1], pair[0]))]
        for start in range(0, len(directional), self._batch_size):
            batch = directional[start:start + self._batch_size]
            inputs = self.tokenizer(
                [left for left, _ in batch], [right for _, right in batch],
                padding=True, truncation=True, max_length=self.spec.max_tokens,
                return_tensors="pt",
            ).to(model.device)
            with torch.inference_mode():
                probabilities = model(**inputs).logits.float().softmax(dim=-1)
            scores.extend(float(value) for value in probabilities[:, contradiction_id].cpu())
        contradiction = [(scores[index] + scores[index + 1]) / 2 for index in range(0, len(scores), 2)]
        return [StanceJudgement(
            relation="contradiction" if score >= self._threshold else "unclear",
            confidence=round(score, 4),
        ) for score in contradiction]
