from __future__ import annotations

import pytest

from comment_cleaner.models import ProcessedMessage
from comment_cleaner.processors.mention_processor import MentionProcessor
from comment_cleaner.processors.unicode_normalizer import UnicodeNormalizer
from comment_cleaner.processors.url_processor import UrlProcessor


def make_msg(text: str) -> ProcessedMessage:
    return ProcessedMessage(
        message_id="1",
        user_id="1",
        original_text=text,
        cleaned_text=text,
    )


class TestUnicodeNormalizer:
    @pytest.fixture
    def normalizer(self, default_config):
        return UnicodeNormalizer(default_config)

    def test_nfc_normalization(self, normalizer):
        msg = make_msg("\u0041\u0301")
        result = normalizer.process(msg)
        assert result.cleaned_text == "\u00c1"

    def test_zero_width_removal(self, normalizer):
        msg = make_msg("hello\u200bworld")
        result = normalizer.process(msg)
        assert result.cleaned_text == "helloworld"
        assert "\u200b" not in result.cleaned_text

    def test_nbsp_normalization(self, normalizer):
        msg = make_msg("hello\u00a0world")
        result = normalizer.process(msg)
        assert "\u00a0" not in result.cleaned_text
        assert result.cleaned_text == "hello world"

    def test_repeated_spaces(self, normalizer):
        msg = make_msg("hello    world")
        result = normalizer.process(msg)
        assert result.cleaned_text == "hello world"

    def test_newline_normalization(self, normalizer):
        msg = make_msg("line1\r\nline2\rline3")
        result = normalizer.process(msg)
        assert "\r" not in result.cleaned_text
        assert result.cleaned_text.count("\n") == 2

    def test_repeated_newlines_limited(self, normalizer):
        msg = make_msg("a\n\n\n\n\nb")
        result = normalizer.process(msg)
        assert result.cleaned_text == "a\n\nb"

    def test_repeated_letters_limited(self, normalizer):
        msg = make_msg("дааааааааа")
        result = normalizer.process(msg)
        assert result.cleaned_text == "дааа"
        assert any(t.type == "normalize_unicode" for t in result.transformations)

    def test_repeated_punctuation_limited(self, normalizer):
        msg = make_msg("что!!!!!")
        result = normalizer.process(msg)
        assert result.cleaned_text == "что!!!"

    def test_repeated_brackets_limited(self, normalizer):
        msg = make_msg("правильно)))))")
        result = normalizer.process(msg)
        assert result.cleaned_text == "правильно)))"

    def test_emoji_preserved(self, normalizer):
        msg = make_msg("привет 😂🤡")
        result = normalizer.process(msg)
        assert "😂" in result.cleaned_text
        assert "🤡" in result.cleaned_text

    def test_empty_text(self, normalizer):
        msg = make_msg("")
        result = normalizer.process(msg)
        assert result.cleaned_text == ""

    def test_spaces_only(self, normalizer):
        msg = make_msg("   \n\n  ")
        result = normalizer.process(msg)
        assert result.cleaned_text == ""


class TestUrlProcessor:
    @pytest.fixture
    def processor(self, default_config):
        return UrlProcessor(default_config)

    def test_url_replaced(self, processor):
        msg = make_msg("Посмотри https://example.org/article")
        result = processor.process(msg)
        assert "https://example.org/article" not in result.cleaned_text
        assert "[URL]" in result.cleaned_text
        assert result.features.contains_url is True

    def test_multiple_urls(self, processor):
        msg = make_msg("a https://a.com b https://b.com")
        result = processor.process(msg)
        assert result.features.contains_url is True
        assert result.cleaned_text.count("[URL]") == 2
        assert len(result.urls) == 2

    def test_url_domain_saved(self, processor):
        msg = make_msg("Check https://example.org/path?q=1")
        result = processor.process(msg)
        assert len(result.urls) == 1
        assert result.urls[0].original == "https://example.org/path?q=1"
        assert result.urls[0].domain == "example.org"

    def test_no_url_in_text(self, processor):
        msg = make_msg("No URL here")
        result = processor.process(msg)
        assert result.features.contains_url is False
        assert len(result.urls) == 0
        assert result.cleaned_text == "No URL here"


class TestMentionProcessor:
    @pytest.fixture
    def processor(self, default_config):
        return MentionProcessor(default_config)

    def test_mention_replaced(self, processor):
        msg = make_msg("@ivan ты прав")
        result = processor.process(msg)
        assert "@ivan" not in result.cleaned_text
        assert "[MENTION]" in result.cleaned_text
        assert result.features.contains_mention is True

    def test_mentions_collected(self, processor):
        msg = make_msg("@ivan @petr привет")
        result = processor.process(msg)
        assert result.features.contains_mention is True
        assert "ivan" in result.mentions
        assert "petr" in result.mentions

    def test_no_mention(self, processor):
        msg = make_msg("Hello world")
        result = processor.process(msg)
        assert result.features.contains_mention is False
        assert result.mentions == []

    def test_short_username_not_matched(self, processor):
        msg = make_msg("@ab some text")
        result = processor.process(msg)
        assert result.cleaned_text == "@ab some text"

    def test_prefix_username_safety(self, processor):
        msg = make_msg("@ivan и @ivanov обсуждают")
        result = processor.process(msg)
        assert "@ivanov" not in result.cleaned_text
        assert result.cleaned_text.count("[MENTION]") == 2
