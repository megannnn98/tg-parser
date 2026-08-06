from __future__ import annotations

from typing import TYPE_CHECKING

import torch
import torch.nn as nn
from transformers import AutoModel, AutoTokenizer

if TYPE_CHECKING:
    from collections.abc import Mapping

DEVICE = torch.device("cuda" if torch.cuda.is_available() else "cpu")

AXIS_NAMES = [
    "economic",
    "authority",
    "social",
    "nationalism",
    "democracy",
    "militarism",
]


class PoliticalBertRegressor(nn.Module):
    def __init__(self, model_name: str = "DeepPavlov/rubert-base-cased") -> None:
        super().__init__()
        self.bert = AutoModel.from_pretrained(model_name)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        hidden_size = self.bert.config.hidden_size

        self.dropout = nn.Dropout(0.1)
        self.regression_heads = nn.ModuleDict({
            axis: nn.Linear(hidden_size, 1) for axis in AXIS_NAMES
        })

    def forward(self, texts: list[str] | None = None, input_ids: torch.Tensor | None = None,
                attention_mask: torch.Tensor | None = None) -> dict[str, torch.Tensor]:
        if texts is not None:
            encoded = self.tokenizer(
                texts,
                padding=True,
                truncation=True,
                max_length=512,
                return_tensors="pt",
            )
            input_ids = encoded["input_ids"].to(DEVICE)
            attention_mask = encoded["attention_mask"].to(DEVICE)

        outputs = self.bert(input_ids=input_ids, attention_mask=attention_mask)
        cls_embedding = outputs.last_hidden_state[:, 0, :]
        cls_embedding = self.dropout(cls_embedding)

        predictions = {}
        for axis in AXIS_NAMES:
            predictions[axis] = self.regression_heads[axis](cls_embedding).squeeze(-1)

        return predictions

    def predict_single(self, text: str) -> dict[str, float]:
        self.eval()
        with torch.no_grad():
            predictions = self.forward(texts=[text])
            return {axis: float(predictions[axis][0].cpu()) for axis in AXIS_NAMES}
