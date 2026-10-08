"""E5 text embeddings, used the way the model card prescribes.

Documents are encoded as "passage: ...", search queries as "query: ...";
the token embeddings are mean-pooled over the attention mask and
L2-normalized, so the cosine similarity of two vectors is their dot product.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class E5Spec:
    name: str
    # A commit of the model repository: a branch name could move under us.
    revision: str
    dimensions: int
    max_tokens: int = 512
    pooling: str = "mean"
    normalized: bool = True
    passage_prefix: str = "passage: "
    query_prefix: str = "query: "


MULTILINGUAL_E5_SMALL = E5Spec(
    "intfloat/multilingual-e5-small", "614241f622f53c4eeff9890bdc4f31cfecc418b3", 384
)
MULTILINGUAL_E5_BASE = E5Spec(
    "intfloat/multilingual-e5-base", "d128750597153bb5987e10b1c3493a34e5a4502a", 768
)
MULTILINGUAL_E5_LARGE = E5Spec(
    "intfloat/multilingual-e5-large", "3d7cfbdacd47fdda877c5cd8a79fbcc4f2a574f3", 1024
)

SPECS = {
    spec.name: spec
    for spec in (MULTILINGUAL_E5_SMALL, MULTILINGUAL_E5_BASE, MULTILINGUAL_E5_LARGE)
}


class E5Encoder:
    def __init__(
        self,
        spec: E5Spec,
        cache_dir: Path | None = None,
        device: str | None = None,
        batch_size: int = 64,
    ):
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
                self.spec.name,
                revision=self.spec.revision,
                trust_remote_code=False,
                cache_dir=self._cache_dir,
            )
        return self._tokenizer

    def count_tokens(self, texts: list[str]) -> list[int]:
        """Tokens of each text alone, without the prefix and special tokens."""
        if not texts:
            return []
        encoded = self.tokenizer(texts, add_special_tokens=False)
        return [len(ids) for ids in encoded["input_ids"]]

    @property
    def content_token_limit(self) -> int:
        """Text tokens that fit into one input next to the passage prefix."""
        prefix = len(
            self.tokenizer(self.spec.passage_prefix, add_special_tokens=False)[
                "input_ids"
            ]
        )
        specials = self.tokenizer.num_special_tokens_to_add(pair=False)
        return self.spec.max_tokens - prefix - specials

    def encode_passages(self, texts: list[str]):
        return self._encode([self.spec.passage_prefix + text for text in texts])

    def encode_queries(self, texts: list[str]):
        return self._encode([self.spec.query_prefix + text for text in texts])

    def load(self) -> None:
        """Loads the weights now, so that a timed encode does not include it."""
        self._load_model()

    def _load_model(self):
        if self._model is None:
            import torch
            from transformers import AutoModel

            device = self._device or ("cuda" if torch.cuda.is_available() else "cpu")
            model = AutoModel.from_pretrained(
                self.spec.name,
                revision=self.spec.revision,
                trust_remote_code=False,
                use_safetensors=True,
                cache_dir=self._cache_dir,
            ).eval()
            self._model = model.to(device)
        return self._model

    def _encode(self, texts: list[str]):
        """float32 array of shape (len(texts), dimensions)."""
        import numpy as np
        import torch

        result = np.empty((len(texts), self.spec.dimensions), dtype=np.float32)
        if not texts:
            return result
        model = self._load_model()
        # Texts of similar length share a batch: less padding to compute.
        order = sorted(range(len(texts)), key=lambda i: len(texts[i]))
        for start in range(0, len(order), self._batch_size):
            rows = order[start : start + self._batch_size]
            inputs = self.tokenizer(
                [texts[i] for i in rows],
                padding=True,
                truncation=True,
                max_length=self.spec.max_tokens,
                return_tensors="pt",
            ).to(model.device)
            with torch.inference_mode():
                hidden = model(**inputs).last_hidden_state
                mask = inputs.attention_mask[..., None]
                pooled = (hidden * mask).sum(1) / mask.sum(1)
                if self.spec.normalized:
                    pooled = torch.nn.functional.normalize(pooled, p=2, dim=1)
            result[rows] = pooled.float().cpu().numpy()
        return result
