from __future__ import annotations

import json

from comment_cleaner.config import Config
from comment_cleaner.exporters import write_jsonl, write_user_batches
from comment_cleaner.models import ProcessedMessage, UserBatch
from comment_cleaner.pipeline import process_messages


class TestPipelineIntegration:
    def test_pipeline_jsonl_processing(self, sample_jsonl_path, temp_dir):
        output_path = temp_dir / "output" / "cleaned.jsonl"
        data = {
            "input": {
                "type": "jsonl",
                "path": str(sample_jsonl_path),
                "column_mapping": {
                    "message_id": "message_id",
                    "user_id": "user_id",
                    "chat_id": "chat_id",
                    "timestamp": "timestamp",
                    "text": "text",
                    "reply_to_message_id": "reply_to_message_id",
                    "forwarded_from": "forwarded_from",
                    "message_type": "message_type",
                },
            },
            "output": {
                "path": str(output_path),
                "error_path": str(temp_dir / "output" / "errors.jsonl"),
                "batch_size": 100,
            },
            "context": {
                "load_reply_context": True,
                "max_reply_depth": 1,
            },
        }
        cfg = Config(data)

        results: list[ProcessedMessage] = []
        for msg in process_messages(cfg):
            if msg is not None:
                results.append(msg)

        assert len(results) >= 1
        assert all(isinstance(m, ProcessedMessage) for m in results)

        url_msg = [m for m in results if "example.com" in m.original_text]
        assert len(url_msg) > 0
        assert url_msg[0].features.contains_url is True

        quote_msg = [m for m in results if m.features.contains_quote]
        assert len(quote_msg) > 0

        mention_msg = [m for m in results if m.features.contains_mention]
        assert len(mention_msg) > 0

    def test_pipeline_preserves_original_text(self, sample_jsonl_path, temp_dir):
        output_path = temp_dir / "output2" / "cleaned.jsonl"
        data = {
            "input": {
                "type": "jsonl",
                "path": str(sample_jsonl_path),
            },
            "output": {"path": str(output_path)},
            "context": {"load_reply_context": False},
        }
        cfg = Config(data)

        results: list[ProcessedMessage] = []
        for msg in process_messages(cfg):
            if msg is not None:
                results.append(msg)

        for msg in results:
            original = msg.original_text
            assert original is not None
            assert isinstance(original, str)

    def test_pipeline_masks_pii(self, temp_dir):
        inp = temp_dir / "pii.jsonl"
        inp.write_text(
            json.dumps(
                {
                    "message_id": "1",
                    "user_id": "100",
                    "text": "Звони +79161234567 или пиши на a@b.com сейчас",
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        cfg = Config(
            {
                "input": {"type": "jsonl", "path": str(inp)},
                "output": {"path": str(temp_dir / "out.jsonl")},
                "context": {"load_reply_context": False},
            }
        )
        results = [m for m in process_messages(cfg) if m is not None]
        msg = results[0]

        assert "[PHONE]" in msg.cleaned_text
        assert "[EMAIL]" in msg.cleaned_text
        assert "+79161234567" not in msg.cleaned_text
        assert "a@b.com" not in msg.cleaned_text
        assert "+79161234567" in msg.original_text
        assert "a@b.com" in msg.original_text


class TestExporter:
    def test_write_jsonl(self, temp_dir):
        msgs = [
            ProcessedMessage(
                message_id="1",
                user_id="100",
                original_text="hello",
                cleaned_text="hello",
            ),
            ProcessedMessage(
                message_id="2",
                user_id="200",
                original_text="world",
                cleaned_text="world",
            ),
        ]
        path = temp_dir / "output.jsonl"
        count = write_jsonl(msgs, path, mode="w")
        assert count == 2
        assert path.exists()

        with open(path, encoding="utf-8") as f:
            lines = f.readlines()
            assert len(lines) == 2
            data = json.loads(lines[0])
            assert data["message_id"] == "1"

    def test_write_user_batches(self, temp_dir):
        batch = UserBatch(
            user_id="100",
            comments_count=1,
            total_chars=5,
            comments=[
                ProcessedMessage(
                    message_id="1",
                    user_id="100",
                    original_text="hello",
                    cleaned_text="hello",
                )
            ],
        )
        path = temp_dir / "batches.jsonl"
        count = write_user_batches([batch], path)
        assert count == 1
        assert path.exists()
