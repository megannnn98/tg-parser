from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import torch
from transformers import AutoTokenizer

if TYPE_CHECKING:
    from collections.abc import Mapping

from src.political_model.model import AXIS_NAMES, DEVICE, PoliticalBertRegressor


def load_model(model_path: Path, model_name: str = "DeepPavlov/rubert-base-cased") -> PoliticalBertRegressor:
    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")
    try:
        model = PoliticalBertRegressor(model_name=model_name)
        model.load_state_dict(torch.load(model_path, weights_only=True, map_location=DEVICE))
        model.to(DEVICE)
        model.eval()
        return model
    except Exception as e:
        raise RuntimeError(f"Failed to load model from {model_path}: {e}") from e


def predict(text: str, model: PoliticalBertRegressor | None = None, model_path: Path | None = None) -> dict[str, float]:
    if not text or not isinstance(text, str):
        raise ValueError("Input text must be a non-empty string")
    if model is None:
        if model_path is None:
            model_path = Path("data/model.pt")
        model = load_model(model_path)

    predictions = model.predict_single(text)
    return {axis: max(-1.0, min(1.0, predictions[axis])) for axis in AXIS_NAMES}


def predict_batch(texts: list[str], model: PoliticalBertRegressor | None = None, model_path: Path | None = None) -> list[dict[str, float]]:
    if model is None:
        if model_path is None:
            model_path = Path("data/model.pt")
        model = load_model(model_path)

    model.eval()
    with torch.no_grad():
        predictions = model.forward(texts=texts)
        results = []
        for i in range(len(texts)):
            result = {}
            for axis in AXIS_NAMES:
                clipped = max(-1.0, min(1.0, float(predictions[axis][i].cpu())))
                result[axis] = clipped
            results.append(result)
        return results
