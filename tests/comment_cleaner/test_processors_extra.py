from __future__ import annotations

import json

import pytest

from comment_cleaner.group_users import group_users_from_jsonl
from comment_cleaner.models import ProcessedMessage
from comment_cleaner.privacy import Pseudonymizer, mask_pii
from comment_cleaner.processors.bot_detector import BotDetector
from comment_cleaner.processors.sarcasm_detector import SarcasmDetector
from comment_cleaner.processors.slang_detector import SlangDetector


def make_msg(text: str) -> ProcessedMessage:
    return ProcessedMessage(
        message_id="1",
        user_id="100",
        original_text=text,
        cleaned_text=text,
    )


class TestSlangDetector:
    @pytest.fixture
    def detector(self, default_config):
        slang = {
            "либераха": {"category": "political_slang"},
            "ватник": {"category": "political_slang"},
            "пятая колонна": {"category": "political_slang", "multiword": True},
        }
        return SlangDetector(default_config, slang_dict=slang)

    def test_single_term_detected(self, detector):
        msg = make_msg("Этот либераха опять пишет")
        result = detector.process(msg)
        assert result.features.contains_political_terms is True
        terms = [t.term for t in result.detected_terms]
        assert "либераха" in terms

    def test_multiword_term_detected(self, detector):
        msg = make_msg("Это все пятая колонна делает")
        result = detector.process(msg)
        terms = [t.term for t in result.detected_terms]
        assert any("пятая колонна" in t for t in terms)

    def test_no_terms(self, detector):
        msg = make_msg("Обычный текст без сленга")
        result = detector.process(msg)
        assert result.features.contains_political_terms is False
        assert result.detected_terms == []

    def test_case_insensitive(self, detector):
        msg = make_msg("Либераха опять...")
        result = detector.process(msg)
        assert result.features.contains_political_terms is True


class TestBotDetector:
    @pytest.fixture
    def detector(self, default_config):
        patterns = {
            "system_message_patterns": [
                "присоединился",
                "покинул",
            ],
            "bot_message_patterns": [
                "^/",
            ],
        }
        return BotDetector(default_config, bot_patterns=patterns)

    def test_system_join_message(self, detector):
        msg = make_msg("Иван присоединился к группе")
        result = detector.process(msg)
        assert result.features.is_system_message is True

    def test_bot_command(self, detector):
        msg = make_msg("/start command")
        result = detector.process(msg)
        assert result.features.is_bot_message is True

    def test_normal_message(self, detector):
        msg = make_msg("Обычное сообщение")
        result = detector.process(msg)
        assert result.features.is_system_message is False
        assert result.features.is_bot_message is False


class TestSarcasmDetector:
    @pytest.fixture
    def detector(self, default_config):
        return SarcasmDetector(default_config)

    def test_sarcasm_phrase_detected(self, detector):
        msg = make_msg("ну да, конечно, все отлично 😂")
        result = detector.process(msg)
        assert result.features.possible_sarcasm is True

    def test_s_tone_indicator(self, detector):
        msg = make_msg("отличная идея /s")
        result = detector.process(msg)
        assert result.features.possible_sarcasm is True

    def test_no_sarcasm(self, detector):
        msg = make_msg("Обычный текст")
        result = detector.process(msg)
        assert result.features.possible_sarcasm is False

    def test_bracket_detection(self, detector):
        msg = make_msg("конечно, все правильно)))")
        result = detector.process(msg)
        assert result.features.possible_sarcasm is True


class TestPrivacy:
    def test_pseudonymizer_inactive_without_salt(self, monkeypatch):
        monkeypatch.delenv("USER_ID_HASH_SALT", raising=False)
        p = Pseudonymizer()
        assert p.is_active is False
        assert p.hash_value("123") == "123"

    def test_pseudonymizer_active_with_salt(self, monkeypatch):
        monkeypatch.setenv("USER_ID_HASH_SALT", "test-salt")
        p = Pseudonymizer()
        assert p.is_active is True
        hashed = p.hash_value("123")
        assert hashed is not None
        assert hashed != "123"
        assert len(hashed) == 16

    def test_pseudonymizer_deterministic(self, monkeypatch):
        monkeypatch.setenv("USER_ID_HASH_SALT", "test-salt")
        p = Pseudonymizer()
        h1 = p.hash_value("123")
        h2 = p.hash_value("123")
        assert h1 == h2

    def test_pseudonymizer_none_value(self, monkeypatch):
        monkeypatch.setenv("USER_ID_HASH_SALT", "test-salt")
        p = Pseudonymizer()
        assert p.hash_value(None) is None

    def test_pseudonymize_message(self, monkeypatch):
        monkeypatch.setenv("USER_ID_HASH_SALT", "test-salt")
        p = Pseudonymizer()
        msg = {
            "user_id": "928373",
            "chat_id": "-100123456789",
            "text": "hello",
            "reply_context": {
                "message_id": "100",
                "text": "test",
                "user_id": "999",
            },
            "mentions": ["ivan"],
        }
        result = p.pseudonymize_message(msg)
        assert result["user_id"] != "928373"
        assert result["chat_id"] != "-100123456789"
        assert result["reply_context"]["user_id"] != "999"
        assert result["mentions"][0] != "ivan"

    def test_mask_pii_phone(self):
        text = "Звони +79161234567"
        result = mask_pii(text)
        assert "[PHONE]" in result
        assert "+79161234567" not in result

    def test_mask_pii_email(self):
        text = "Пиши на test@example.com"
        result = mask_pii(text)
        assert "[EMAIL]" in result
        assert "test@example.com" not in result


class TestUserGrouping:
    def test_group_users_basic(self, temp_dir):
        msgs = [
            {
                "message_id": "1",
                "user_id": "100",
                "original_text": "hello",
                "cleaned_text": "hello",
                "timestamp": "2026-01-01T00:00:00",
                "features": {},
                "is_duplicate": False,
            },
            {
                "message_id": "2",
                "user_id": "100",
                "original_text": "world",
                "cleaned_text": "world",
                "timestamp": "2026-01-02T00:00:00",
                "features": {},
                "is_duplicate": False,
            },
            {
                "message_id": "3",
                "user_id": "200",
                "original_text": "test",
                "cleaned_text": "test",
                "timestamp": "2026-01-01T00:00:00",
                "features": {},
                "is_duplicate": False,
            },
        ]
        path = temp_dir / "cleaned.jsonl"
        with open(path, "w", encoding="utf-8") as f:
            for m in msgs:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")

        batches = group_users_from_jsonl(path)
        assert len(batches) == 2
        user_100 = next(b for b in batches if b.user_id == "100")
        assert user_100.comments_count == 2
        user_200 = next(b for b in batches if b.user_id == "200")
        assert user_200.comments_count == 1

    def test_group_users_max_messages(self, temp_dir):
        msgs = []
        for i in range(10):
            msgs.append(
                {
                    "message_id": str(i + 1),
                    "user_id": "100",
                    "original_text": f"msg{i}",
                    "cleaned_text": f"msg{i}",
                    "timestamp": f"2026-01-0{i + 1}T00:00:00",
                    "features": {},
                    "is_duplicate": False,
                }
            )
        path = temp_dir / "cleaned.jsonl"
        with open(path, "w", encoding="utf-8") as f:
            for m in msgs:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")

        batches = group_users_from_jsonl(path, max_messages_per_user=5)
        assert len(batches) == 1
        assert batches[0].comments_count == 5

    def test_group_users_exclude_duplicates(self, temp_dir):
        msgs = [
            {
                "message_id": "1",
                "user_id": "100",
                "original_text": "hello",
                "cleaned_text": "hello",
                "timestamp": "2026-01-01T00:00:00",
                "features": {},
                "is_duplicate": False,
            },
            {
                "message_id": "2",
                "user_id": "100",
                "original_text": "hello",
                "cleaned_text": "hello",
                "timestamp": "2026-01-02T00:00:00",
                "features": {"is_duplicate": True},
                "is_duplicate": True,
                "duplicate_type": "exact",
                "duplicate_of": "1",
            },
        ]
        path = temp_dir / "cleaned.jsonl"
        with open(path, "w", encoding="utf-8") as f:
            for m in msgs:
                f.write(json.dumps(m, ensure_ascii=False) + "\n")

        batches = group_users_from_jsonl(path, exclude_duplicates=True)
        assert batches[0].comments_count == 1
