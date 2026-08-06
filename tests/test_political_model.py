from __future__ import annotations

import pytest
import torch

from src.political_model.model import AXIS_NAMES, PoliticalBertRegressor


@pytest.fixture(scope="module")
def model() -> PoliticalBertRegressor:
    return PoliticalBertRegressor()


def test_model_forward_output_shape(model: PoliticalBertRegressor) -> None:
    model.eval()

    texts = ["тестовый текст", "еще один текст"]
    predictions = model.forward(texts=texts)

    assert isinstance(predictions, dict)
    assert set(predictions.keys()) == set(AXIS_NAMES)

    for axis in AXIS_NAMES:
        assert predictions[axis].shape == (2,)
        assert predictions[axis].dtype == torch.float32


def test_model_single_prediction(model: PoliticalBertRegressor) -> None:
    model.eval()

    text = "тестовый текст для предсказания"
    result = model.predict_single(text)

    assert isinstance(result, dict)
    assert set(result.keys()) == set(AXIS_NAMES)

    for axis in AXIS_NAMES:
        assert isinstance(result[axis], float)


def test_model_device(model: PoliticalBertRegressor) -> None:
    assert hasattr(model, "bert")
    assert hasattr(model, "tokenizer")
    assert hasattr(model, "regression_heads")

    for axis in AXIS_NAMES:
        assert axis in model.regression_heads
