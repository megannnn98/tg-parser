from __future__ import annotations

import tempfile
from pathlib import Path

import pytest
import torch
from torch.utils.data import DataLoader

from src.political_model.model import AXIS_NAMES, PoliticalBertRegressor
from src.political_model.train import PoliticalDataset, collate_fn, _binarize_scores


def test_dataset_loading() -> None:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False, encoding="utf-8") as f:
        test_data = [
            {"text": "тест 1", "economic": 0.5, "authority": -0.3, "social": 0.0, "nationalism": 0.8, "democracy": -0.2, "militarism": 0.1},
            {"text": "тест 2", "economic": -0.7, "authority": 0.9, "social": 0.4, "nationalism": -0.5, "democracy": 0.6, "militarism": -0.8},
        ]
        for item in test_data:
            import json
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
        temp_path = Path(f.name)

    try:
        model = PoliticalBertRegressor()
        dataset = PoliticalDataset(temp_path, model.tokenizer)

        assert len(dataset) == 2

        sample = dataset[0]
        assert "input_ids" in sample
        assert "attention_mask" in sample
        assert "targets" in sample
        assert set(sample["targets"].keys()) == set(AXIS_NAMES)

        for axis in AXIS_NAMES:
            assert isinstance(sample["targets"][axis], float)
    finally:
        temp_path.unlink(missing_ok=True)


def test_collate_fn() -> None:
    batch = [
        {
            "input_ids": torch.tensor([1, 2, 3, 0, 0]),
            "attention_mask": torch.tensor([1, 1, 1, 0, 0]),
            "targets": {"economic": 0.5, "authority": -0.3, "social": 0.0, "nationalism": 0.8, "democracy": -0.2, "militarism": 0.1},
        },
        {
            "input_ids": torch.tensor([4, 5, 6, 7, 8]),
            "attention_mask": torch.tensor([1, 1, 1, 1, 1]),
            "targets": {"economic": -0.7, "authority": 0.9, "social": 0.4, "nationalism": -0.5, "democracy": 0.6, "militarism": -0.8},
        },
    ]

    result = collate_fn(batch)

    assert result["input_ids"].shape == (2, 5)
    assert result["attention_mask"].shape == (2, 5)

    for axis in AXIS_NAMES:
        assert axis in result["targets"]
        assert result["targets"][axis].shape == (2,)
        assert result["targets"][axis].dtype == torch.float32


def test_binarize_scores() -> None:
    scores = [-0.5, 0.0, 0.1, 0.5, 1.0]
    result = _binarize_scores(scores)

    assert result == [0, 2, 1, 1, 1]

    scores2 = [-1.0, -0.8, -0.2, 0.2, 0.8]
    result2 = _binarize_scores(scores2, threshold=0.3)

    assert result2 == [0, 0, 2, 2, 1]
