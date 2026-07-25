from __future__ import annotations

import pytest

from comment_cleaner.config import Config
from comment_cleaner.models import ProcessedMessage
from comment_cleaner.processors.duplicate_detector import DuplicateDetector
from comment_cleaner.processors.information_filter import InformationFilter
from comment_cleaner.processors.quote_parser import QuoteParser
from comment_cleaner.processors.reply_context import ReplyContextProcessor


def make_msg(text: str, msg_id: str = "1") -> ProcessedMessage:
    return ProcessedMessage(
        message_id=msg_id,
        user_id="1",
        original_text=text,
        cleaned_text=text,
    )


class TestQuoteParser:
    @pytest.fixture
    def parser(self, default_config):
        return QuoteParser(default_config)

    def test_blockquote_separated(self, parser):
        text = "> Все проблемы из-за мигрантов\n\nВот до какого бреда они дошли."
        msg = make_msg(text)
        result = parser.process(msg)
        assert result.features.contains_quote is True
        assert result.quoted_text == "Все проблемы из-за мигрантов"
        assert "Вот до какого бреда" in (result.author_text or "")

    def test_quote_prefix(self, parser):
        text = "Цитата: все плохо"
        msg = make_msg(text)
        result = parser.process(msg)
        assert result.features.contains_quote is True
        assert "все плохо" in (result.quoted_text or "")

    def test_author_quote_prefix(self, parser):
        text = "Иван писал: государство должно помогать"
        msg = make_msg(text)
        result = parser.process(msg)
        assert result.features.contains_quote is True
        assert "государство должно помогать" in (result.quoted_text or "")

    def test_russian_quotes(self, parser):
        text = "Он сказал «спасибо» и ушел"
        msg = make_msg(text)
        result = parser.process(msg)
        assert result.features.contains_quote is True
        assert result.quoted_text is not None

    def test_no_quote(self, parser):
        text = "Это обычный текст без цитат"
        msg = make_msg(text)
        result = parser.process(msg)
        assert result.features.contains_quote is False
        assert result.quoted_text is None


class TestReplyContextProcessor:
    @pytest.fixture
    def processor(self, default_config):
        return ReplyContextProcessor(default_config)

    def test_reply_context_found(self, processor):
        processor.set_message_index(
            {
                "2": {"text": "Родительское сообщение", "user_id": "200"},
            }
        )
        processor.set_current_raw({"reply_to_message_id": "2"})
        msg = make_msg("Ответ на сообщение")
        result = processor.process(msg)
        assert result.features.contains_reply_context is True
        assert result.reply_context is not None
        assert result.reply_context.message_id == "2"
        assert result.reply_context.text == "Родительское сообщение"

    def test_reply_context_missing_parent(self, processor):
        processor.set_message_index({})
        processor.set_current_raw({"reply_to_message_id": "999"})
        msg = make_msg("Ответ без родителя")
        result = processor.process(msg)
        assert result.reply_context_missing is True
        assert result.reply_context is None

    def test_no_reply_when_disabled(self, default_config):
        data = {
            "context": {"load_reply_context": False},
        }
        cfg = Config(data)
        processor = ReplyContextProcessor(cfg)
        msg = make_msg("some text")
        result = processor.process(msg)
        assert result.reply_context is None
        assert result.reply_context_missing is False


class TestDuplicateDetector:
    @pytest.fixture
    def detector(self, default_config):
        return DuplicateDetector(default_config)

    def test_first_message_not_duplicate(self, detector):
        msg = make_msg("Уникальный текст", msg_id="1")
        result = detector.process(msg)
        assert result.is_duplicate is False
        assert result.features.is_duplicate is False

    def test_exact_duplicate_detected(self, detector):
        msg1 = make_msg("Повторяющийся текст", msg_id="1")
        msg2 = make_msg("Повторяющийся текст", msg_id="2")

        result1 = detector.process(msg1)
        assert result1.is_duplicate is False

        result2 = detector.process(msg2)
        assert result2.is_duplicate is True
        assert result2.duplicate_type == "exact"
        assert result2.duplicate_of == "1"

    def test_normalized_duplicate_detected(self, detector):
        msg1 = make_msg("Текст С URL https://example.com", msg_id="1")
        msg2 = make_msg("текст с url https://other.com", msg_id="2")

        r1 = detector.process(msg1)
        r2 = detector.process(msg2)
        assert r1.is_duplicate is False
        assert r2.is_duplicate is True
        assert r2.duplicate_type == "normalized"

    def test_keep_mode_no_detection(self, default_config):
        data = {"duplicates": {"mode": "keep"}}
        cfg = Config(data)
        detector = DuplicateDetector(cfg)
        msg1 = make_msg("Same", msg_id="1")
        msg2 = make_msg("Same", msg_id="2")
        detector.process(msg1)
        r2 = detector.process(msg2)
        assert r2.is_duplicate is False


class TestInformationFilter:
    @pytest.fixture
    def filter_proc(self, default_config):
        return InformationFilter(default_config)

    def test_empty_text_low_score(self, filter_proc):
        msg = make_msg("")
        result = filter_proc.process(msg)
        assert result.information_score < 0.1
        assert result.features.low_information is True

    def test_single_plus_low_score(self, filter_proc):
        msg = make_msg("+")
        result = filter_proc.process(msg)
        assert result.information_score < 0.3

    def test_normal_text_high_score(self, filter_proc):
        msg = make_msg("Государство должно заботиться о гражданах и обеспечивать их права")
        result = filter_proc.process(msg)
        assert result.information_score > 0.5

    def test_emoji_only_low_score(self, filter_proc):
        msg = make_msg("😂")
        result = filter_proc.process(msg)
        assert result.information_score < 0.5

    def test_low_info_word_flagged(self, filter_proc):
        filter_proc._low_info_words = {"да", "нет"}
        msg = make_msg("да")
        result = filter_proc.process(msg)
        assert result.information_score < 0.3

    def test_with_context_sets_requires_context(self, filter_proc):
        from comment_cleaner.models import ReplyContext

        msg = make_msg("да")
        msg.reply_context = ReplyContext(
            message_id="2",
            text="Капитализм необходимо ликвидировать.",
        )
        result = filter_proc.process(msg)
        assert result.requires_context is True
