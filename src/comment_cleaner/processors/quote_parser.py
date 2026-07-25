from __future__ import annotations

import re

from comment_cleaner.config import Config
from comment_cleaner.models import ProcessedMessage, Transformation

_BLOCKQUOTE_LINE = re.compile(r"^\s*>\s?(.*)$")
_RUSSIAN_QUOTE = re.compile(r"«([^»]*)»|\u201C([^\u201D]*)\u201D|\u201E([^\u201C]*)\u201C")
_QUOTE_PREFIX = re.compile(r"^(?:Цитата|цитата)\s*:\s*(.*)$", re.UNICODE)
_QUOTE_AUTHOR = re.compile(
    r"^(.*?)(?:\s*писал[аи]?|написал[аи]?|сказал[аи]?|говорит|отвечает):\s*(.*)$",
    re.UNICODE | re.IGNORECASE,
)


class QuoteParser:
    def __init__(self, config: Config) -> None:
        pass

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        text = message.cleaned_text or message.original_text
        lines = text.split("\n")

        quote_lines: list[str] = []
        author_lines: list[str] = []
        in_quote = False

        for line in lines:
            m = _BLOCKQUOTE_LINE.match(line)
            if m:
                quote_lines.append(m.group(1))
                in_quote = True
            elif in_quote:
                author_lines.append(line)
            else:
                author_lines.append(line)

        if quote_lines:
            message.features.contains_quote = True
            quote_text = " ".join(quote_lines).strip()
            author_text = " ".join(author_lines).strip()

            if not author_text:
                message.quoted_text = quote_text
                message.author_text = quote_text
                message.quote_parsing_status = "quote_only"
            else:
                message.quoted_text = quote_text
                message.author_text = author_text
                message.quote_parsing_status = "certain"

            message.transformations.append(
                Transformation(type="parse_quote", details={"method": "blockquote"})
            )
            return message

        author_text = text
        q_prefix = _QUOTE_PREFIX.search(text)
        if q_prefix:
            message.features.contains_quote = True
            quote_part = q_prefix.group(1).strip()
            rest = text[: q_prefix.start()].strip() + " " + text[q_prefix.end() :].strip()
            rest = rest.strip()
            message.quoted_text = quote_part
            message.author_text = rest if rest else quote_part
            message.quote_parsing_status = "certain" if rest else "uncertain"
            message.transformations.append(
                Transformation(type="parse_quote", details={"method": "prefix"})
            )
            return message

        q_author = _QUOTE_AUTHOR.search(text)
        if q_author:
            message.features.contains_quote = True
            author_name = q_author.group(1).strip()
            quote_body = q_author.group(2).strip()
            prefix = text[: q_author.start()].strip()
            rest = text[q_author.end() :].strip()
            message.quoted_text = quote_body
            message.author_text = f"{prefix} {rest}".strip() if prefix or rest else quote_body
            message.quote_parsing_status = "certain"
            message.transformations.append(
                Transformation(
                    type="parse_quote",
                    details={"method": "author_prefix", "author": author_name},
                )
            )
            return message

        russian_quotes = _RUSSIAN_QUOTE.findall(text)
        if russian_quotes:
            all_quoted: list[str] = []
            for groups in russian_quotes:
                all_quoted.extend(g for g in groups if g)

            quoted = " ".join(all_quoted)
            remaining = text
            for q in all_quoted:
                for variant in [f"«{q}»", f"\u201c{q}\u201d", f"\u201e{q}\u201c"]:
                    remaining = remaining.replace(variant, "", 1)

            remaining = re.sub(r"\s+", " ", remaining).strip()
            message.features.contains_quote = True
            message.quoted_text = quoted
            message.author_text = remaining if remaining else quoted
            message.quote_parsing_status = "uncertain" if not remaining else "certain"
            message.transformations.append(
                Transformation(type="parse_quote", details={"method": "russian_quotes"})
            )
            return message

        if not message.quoted_text:
            message.author_text = text

        return message
