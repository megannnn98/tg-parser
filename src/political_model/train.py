from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import torch
import torch.nn as nn
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader, Dataset

if TYPE_CHECKING:
    from collections.abc import Iterator

from src.political_model.model import AXIS_NAMES, DEVICE, PoliticalBertRegressor

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger("political_model_train")


class PoliticalDataset(Dataset):
    def __init__(self, data_path: Path, tokenizer, max_length: int = 512) -> None:
        self.tokenizer = tokenizer
        self.max_length = max_length
        self.samples: list[dict[str, torch.Tensor | dict[str, float]]] = []

        with open(data_path, encoding="utf-8") as f:
            for line in f:
                item = json.loads(line)
                text = item["text"]
                axes = {axis: float(item[axis]) for axis in AXIS_NAMES}
                self.samples.append({"text": text, "axes": axes})

        if not self.samples:
            raise ValueError(f"Dataset is empty or malformed: {data_path}")

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, idx: int) -> dict[str, torch.Tensor | dict[str, float]]:
        sample = self.samples[idx]
        encoded = self.tokenizer(
            sample["text"],
            truncation=True,
            max_length=self.max_length,
            return_tensors="pt",
        )
        return {
            "input_ids": encoded["input_ids"].squeeze(0),
            "attention_mask": encoded["attention_mask"].squeeze(0),
            "targets": sample["axes"],
        }


def collate_fn(batch: list[dict[str, torch.Tensor | dict[str, float]]]) -> dict[str, torch.Tensor]:
    from torch.nn.utils.rnn import pad_sequence

    input_ids = pad_sequence([item["input_ids"] for item in batch], batch_first=True, padding_value=0)
    attention_mask = pad_sequence([item["attention_mask"] for item in batch], batch_first=True, padding_value=0)
    targets = {axis: torch.tensor([item["targets"][axis] for item in batch], dtype=torch.float32) for axis in AXIS_NAMES}
    return {"input_ids": input_ids, "attention_mask": attention_mask, "targets": targets}


def train_epoch(model: PoliticalBertRegressor, dataloader: DataLoader, optimizer: torch.optim.Optimizer, criterion: nn.MSELoss) -> dict[str, float]:
    model.train()
    total_loss = 0.0
    axis_losses = {axis: 0.0 for axis in AXIS_NAMES}

    for batch in dataloader:
        input_ids = batch["input_ids"].to(DEVICE)
        attention_mask = batch["attention_mask"].to(DEVICE)
        targets = {axis: batch["targets"][axis].to(DEVICE) for axis in AXIS_NAMES}

        optimizer.zero_grad()

        outputs = model(input_ids=input_ids, attention_mask=attention_mask)

        loss = torch.tensor(0.0, device=DEVICE)
        for axis in AXIS_NAMES:
            axis_loss = criterion(outputs[axis], targets[axis])
            loss += axis_loss
            axis_losses[axis] += axis_loss.item()

        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()

        total_loss += loss.item()

    avg_loss = total_loss / len(dataloader)
    avg_axis_losses = {axis: axis_losses[axis] / len(dataloader) for axis in AXIS_NAMES}
    return {"total": avg_loss, **avg_axis_losses}


def sign_accuracy(preds: torch.Tensor, targets: torch.Tensor, threshold: float = 0.05) -> float:
    pred_sign = (preds > threshold).float() - (preds < -threshold).float()
    target_sign = (targets > threshold).float() - (targets < -threshold).float()
    return (pred_sign == target_sign).float().mean().item()


def validate(model: PoliticalBertRegressor, dataloader: DataLoader, criterion: nn.MSELoss) -> dict[str, float]:
    model.eval()
    total_loss = 0.0
    axis_losses = {axis: 0.0 for axis in AXIS_NAMES}
    axis_sign_acc = {axis: 0.0 for axis in AXIS_NAMES}

    with torch.no_grad():
        for batch in dataloader:
            input_ids = batch["input_ids"].to(DEVICE)
            attention_mask = batch["attention_mask"].to(DEVICE)
            targets = {axis: batch["targets"][axis].to(DEVICE) for axis in AXIS_NAMES}

            outputs = model(input_ids=input_ids, attention_mask=attention_mask)

            loss = torch.tensor(0.0, device=DEVICE)
            for axis in AXIS_NAMES:
                axis_loss = criterion(outputs[axis], targets[axis])
                loss += axis_loss
                axis_losses[axis] += axis_loss.item()
                axis_sign_acc[axis] += sign_accuracy(outputs[axis], targets[axis])

            total_loss += loss.item()

    avg_loss = total_loss / len(dataloader)
    avg_axis_losses = {axis: axis_losses[axis] / len(dataloader) for axis in AXIS_NAMES}
    avg_sign_acc = {axis: axis_sign_acc[axis] / len(dataloader) for axis in AXIS_NAMES}
    return {"total": avg_loss, **avg_axis_losses, **{f"{axis}_sign_acc": avg_sign_acc[axis] for axis in AXIS_NAMES}}


def train(
    data_path: Path,
    output_path: Path,
    model_name: str = "DeepPavlov/rubert-base-cased",
    batch_size: int = 16,
    learning_rate: float = 2e-5,
    epochs: int = 10,
    early_stopping_patience: int = 3,
    test_size: float = 0.3,
    val_size: float = 0.5,
) -> None:
    _logger.info("Loading model and tokenizer...")
    model = PoliticalBertRegressor(model_name=model_name).to(DEVICE)

    _logger.info("Loading dataset from %s...", data_path)
    full_dataset = PoliticalDataset(data_path, model.tokenizer)

    democracy_scores = [sample["axes"]["democracy"] for sample in full_dataset.samples]
    train_indices, temp_indices = train_test_split(
        range(len(full_dataset)), test_size=test_size, stratify=_binarize_scores(democracy_scores), random_state=42
    )
    val_indices, test_indices = train_test_split(
        temp_indices, test_size=val_size, stratify=_binarize_scores([democracy_scores[i] for i in temp_indices]), random_state=42
    )

    train_dataset = torch.utils.data.Subset(full_dataset, train_indices)
    val_dataset = torch.utils.data.Subset(full_dataset, val_indices)
    test_dataset = torch.utils.data.Subset(full_dataset, test_indices)

    _logger.info("Dataset split: train=%d, val=%d, test=%d", len(train_dataset), len(val_dataset), len(test_dataset))

    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True, collate_fn=collate_fn)
    val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False, collate_fn=collate_fn)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False, collate_fn=collate_fn)

    optimizer = torch.optim.AdamW(model.parameters(), lr=learning_rate)
    criterion = nn.MSELoss()

    best_val_loss = float("inf")
    patience_counter = 0

    for epoch in range(epochs):
        _logger.info("Epoch %d/%d", epoch + 1, epochs)

        train_losses = train_epoch(model, train_loader, optimizer, criterion)
        val_losses = validate(model, val_loader, criterion)

        _logger.info("Train loss: %.4f", train_losses["total"])

        _logger.info("Val loss: %.4f", val_losses["total"])
        for axis in AXIS_NAMES:
            _logger.info("  Val %s: %.4f (sign_acc: %.3f)", axis, val_losses[axis], val_losses[f"{axis}_sign_acc"])

        if val_losses["total"] < best_val_loss:
            best_val_loss = val_losses["total"]
            patience_counter = 0
            torch.save(model.state_dict(), output_path)
            _logger.info("Saved best model to %s (val loss: %.4f)", output_path, best_val_loss)
        else:
            patience_counter += 1
            _logger.info("No improvement for %d epochs", patience_counter)

        if patience_counter >= early_stopping_patience:
            _logger.info("Early stopping triggered")
            break

    _logger.info("Loading best model for final test evaluation...")
    model.load_state_dict(torch.load(output_path, weights_only=True))
    test_losses = validate(model, test_loader, criterion)
    _logger.info("Test loss: %.4f", test_losses["total"])
    for axis in AXIS_NAMES:
        _logger.info("  Test %s: %.4f", axis, test_losses[axis])


def _binarize_scores(scores: list[float], threshold: float = 0.05) -> list[int]:
    result = []
    for s in scores:
        if abs(s) <= threshold:
            result.append(2)
        elif s > 0:
            result.append(1)
        else:
            result.append(0)
    return result


if __name__ == "__main__":
    train(
        data_path=Path("data/synthetic_corpus.jsonl"),
        output_path=Path("data/model.pt"),
        batch_size=16,
        learning_rate=2e-5,
        epochs=10,
        early_stopping_patience=3,
    )
