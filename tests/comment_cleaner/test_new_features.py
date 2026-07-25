"""Tests for checkpoint/resume, reply chains, and fuzzy duplicates."""

from __future__ import annotations

import json

from comment_cleaner.config import Config
from comment_cleaner.models import Checkpoint, ProcessedMessage
from comment_cleaner.pipeline import (
    _get_checkpoint_path,
    _load_checkpoint,
    _save_checkpoint,
)
from comment_cleaner.processors.duplicate_detector import DuplicateDetector
from comment_cleaner.processors.reply_context import ReplyContextProcessor


class TestCheckpoint:
    def test_save_and_load(self, temp_dir):
        path = temp_dir / ".checkpoint_test.json"
        cp = Checkpoint(
            last_processed_message_id="msg_42",
            last_line_number=100,
            total_processed=42,
        )
        _save_checkpoint(path, cp)

        loaded = _load_checkpoint(path)
        assert loaded.last_processed_message_id == "msg_42"
        assert loaded.last_line_number == 100
        assert loaded.total_processed == 42

    def test_load_missing_file(self, temp_dir):
        path = temp_dir / ".nonexistent.json"
        cp = _load_checkpoint(path)
        assert cp.last_processed_message_id is None
        assert cp.total_processed == 0

    def test_load_corrupted_file(self, temp_dir):
        path = temp_dir / ".corrupted.json"
        path.write_text("not valid json")
        cp = _load_checkpoint(path)
        assert cp.last_processed_message_id is None

    def test_checkpoint_path_from_config(self, temp_dir):
        data = {"output": {"path": str(temp_dir / "out" / "cleaned.jsonl")}}
        cfg = Config(data)
        path = _get_checkpoint_path(cfg)
        assert ".checkpoint_cleaned.json" in str(path)
        assert str(temp_dir) in str(path)


class TestReplyChain:
    def test_chain_depth_2(self, default_config):
        index = {
            "010": {
                "text": "middle message",
                "user_id": "109",
                "reply_to_message_id": "001",
            },
            "001": {
                "text": "root message",
                "user_id": "100",
                "reply_to_message_id": None,
            },
        }

        data = {
            "context": {"load_reply_context": True, "max_reply_depth": 3},
        }
        cfg = Config(data)
        proc = ReplyContextProcessor(cfg)
        proc.set_message_index(index)
        proc.set_current_raw({"reply_to_message_id": "010"})

        msg = ProcessedMessage(
            message_id="020",
            user_id="116",
            original_text="test",
            cleaned_text="test",
        )
        result = proc.process(msg)

        assert result.features.contains_reply_context is True
        assert result.reply_context is not None
        assert result.reply_context.context_depth == 2
        assert len(result.reply_context.chain) == 2
        assert result.reply_context.chain[0].message_id == "010"
        assert result.reply_context.chain[1].message_id == "001"

    def test_chain_depth_3(self, default_config):
        index = {
            "030": {
                "text": "level 2",
                "user_id": "200",
                "reply_to_message_id": "020",
            },
            "020": {
                "text": "level 1",
                "user_id": "100",
                "reply_to_message_id": "010",
            },
            "010": {
                "text": "root",
                "user_id": "50",
                "reply_to_message_id": None,
            },
        }

        data = {
            "context": {"load_reply_context": True, "max_reply_depth": 3},
        }
        cfg = Config(data)
        proc = ReplyContextProcessor(cfg)
        proc.set_message_index(index)
        proc.set_current_raw({"reply_to_message_id": "030"})

        msg = ProcessedMessage(
            message_id="040",
            user_id="300",
            original_text="test",
            cleaned_text="test",
        )
        result = proc.process(msg)
        assert result.reply_context is not None
        assert result.reply_context.context_depth == 3
        assert len(result.reply_context.chain) == 3

    def test_chain_max_depth_limited(self, default_config):
        index = {
            "030": {
                "text": "l2",
                "user_id": "200",
                "reply_to_message_id": "020",
            },
            "020": {
                "text": "l1",
                "user_id": "100",
                "reply_to_message_id": "010",
            },
            "010": {
                "text": "root",
                "user_id": "50",
                "reply_to_message_id": None,
            },
        }

        data = {
            "context": {"load_reply_context": True, "max_reply_depth": 1},
        }
        cfg = Config(data)
        proc = ReplyContextProcessor(cfg)
        proc.set_message_index(index)
        proc.set_current_raw({"reply_to_message_id": "030"})

        msg = ProcessedMessage(
            message_id="040",
            user_id="300",
            original_text="test",
            cleaned_text="test",
        )
        result = proc.process(msg)
        assert result.reply_context is not None
        assert result.reply_context.context_depth == 1
        assert len(result.reply_context.chain) == 1

    def test_cycle_detection(self, default_config):
        index = {
            "010": {
                "text": "msg 1",
                "user_id": "100",
                "reply_to_message_id": "020",
            },
            "020": {
                "text": "msg 2",
                "user_id": "200",
                "reply_to_message_id": "010",
            },
        }

        data = {
            "context": {"load_reply_context": True, "max_reply_depth": 3},
        }
        cfg = Config(data)
        proc = ReplyContextProcessor(cfg)
        proc.set_message_index(index)
        proc.set_current_raw({"reply_to_message_id": "010"})

        msg = ProcessedMessage(
            message_id="030",
            user_id="300",
            original_text="test",
            cleaned_text="test",
        )
        result = proc.process(msg)
        assert result.reply_context is not None
        assert result.reply_context.context_depth <= 2

    def test_missing_parent_in_chain(self, default_config):
        index = {
            "010": {
                "text": "middle",
                "user_id": "100",
                "reply_to_message_id": "99999",
            },
        }

        data = {
            "context": {"load_reply_context": True, "max_reply_depth": 3},
        }
        cfg = Config(data)
        proc = ReplyContextProcessor(cfg)
        proc.set_message_index(index)
        proc.set_current_raw({"reply_to_message_id": "010"})

        msg = ProcessedMessage(
            message_id="020",
            user_id="200",
            original_text="test",
            cleaned_text="test",
        )
        result = proc.process(msg)
        assert result.reply_context is not None
        assert result.reply_context.context_depth == 1


class TestFuzzyDuplicates:
    def test_fuzzy_duplicate_detected(self, default_config):
        data = {
            "duplicates": {
                "mode": "mark",
                "fuzzy_enabled": True,
                "fuzzy_threshold": 80,
            }
        }
        cfg = Config(data)
        detector = DuplicateDetector(cfg)

        msg1 = ProcessedMessage(
            message_id="1",
            user_id="100",
            original_text="Это очень длинное сообщение про политику и экономику страны",
            cleaned_text="Это очень длинное сообщение про политику и экономику страны",
        )
        msg2 = ProcessedMessage(
            message_id="2",
            user_id="200",
            original_text="Это очень длинное сообщение про политику и экономику страны немного изменён",
            cleaned_text="Это очень длинное сообщение про политику и экономику страны немного изменён",
        )

        r1 = detector.process(msg1)
        r2 = detector.process(msg2)

        assert r1.is_duplicate is False
        assert r2.is_duplicate is True
        assert r2.duplicate_type == "fuzzy"
        assert r2.duplicate_of == "1"

    def test_fuzzy_then_exact_detected(self, default_config):
        """Fuzzy duplicate should NOT prevent subsequent exact duplicate detection."""
        data = {
            "duplicates": {
                "mode": "mark",
                "fuzzy_enabled": True,
                "fuzzy_threshold": 80,
            }
        }
        cfg = Config(data)
        detector = DuplicateDetector(cfg)

        msg1 = ProcessedMessage(
            message_id="1",
            user_id="100",
            original_text="Сообщение про экономику и политику в нашей стране",
            cleaned_text="Сообщение про экономику и политику в нашей стране",
        )
        msg2 = ProcessedMessage(
            message_id="2",
            user_id="200",
            original_text="Сообщение про экономику и политику в нашей стране изменено",
            cleaned_text="Сообщение про экономику и политику в нашей стране изменено",
        )
        msg3 = ProcessedMessage(
            message_id="3",
            user_id="300",
            original_text="Сообщение про экономику и политику в нашей стране изменено",
            cleaned_text="Сообщение про экономику и политику в нашей стране изменено",
        )

        r1 = detector.process(msg1)
        r2 = detector.process(msg2)
        r3 = detector.process(msg3)

        assert r1.is_duplicate is False
        assert r2.is_duplicate is True
        assert r2.duplicate_type == "fuzzy"
        assert r3.is_duplicate is True
        assert r3.duplicate_type == "exact"
        assert r3.duplicate_of == "2"

    def test_fuzzy_below_threshold_not_duplicate(self, default_config):
        data = {
            "duplicates": {
                "mode": "mark",
                "fuzzy_enabled": True,
                "fuzzy_threshold": 95,
            }
        }
        cfg = Config(data)
        detector = DuplicateDetector(cfg)

        msg1 = ProcessedMessage(
            message_id="1",
            user_id="100",
            original_text="Сообщение про экономику",
            cleaned_text="Сообщение про экономику",
        )
        msg2 = ProcessedMessage(
            message_id="2",
            user_id="200",
            original_text="Совершенно другой текст не имеющий отношения к первому",
            cleaned_text="Совершенно другой текст не имеющий отношения к первому",
        )

        r1 = detector.process(msg1)
        r2 = detector.process(msg2)
        assert r1.is_duplicate is False
        assert r2.is_duplicate is False


class TestIdempotency:
    def test_deterministic_processing(self, temp_dir):
        from comment_cleaner.pipeline import process_messages

        records = [
            {"message_id": "a", "user_id": "1", "text": "hello world"},
            {"message_id": "b", "user_id": "2", "text": "@user check https://x.com"},
            {"message_id": "c", "user_id": "1", "text": "hello world"},
        ]
        input_path = temp_dir / "input.jsonl"
        with open(input_path, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

        cfg1 = Config(
            {
                "input": {"type": "jsonl", "path": str(input_path)},
                "output": {"path": str(temp_dir / "out1.jsonl")},
                "context": {"load_reply_context": False},
            }
        )
        cfg2 = Config(
            {
                "input": {"type": "jsonl", "path": str(input_path)},
                "output": {"path": str(temp_dir / "out2.jsonl")},
                "context": {"load_reply_context": False},
            }
        )

        results1 = [m for m in process_messages(cfg1) if m is not None]
        results2 = [m for m in process_messages(cfg2) if m is not None]

        assert len(results1) == len(results2)
        for r1, r2 in zip(results1, results2, strict=True):
            assert r1.cleaned_text == r2.cleaned_text
            assert r1.features.contains_url == r2.features.contains_url
            assert r1.features.contains_mention == r2.features.contains_mention
            assert r1.is_duplicate == r2.is_duplicate
            assert r1.duplicate_type == r2.duplicate_type
