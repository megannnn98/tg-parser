from __future__ import annotations

import re
from urllib.parse import urlparse

from comment_cleaner.config import Config
from comment_cleaner.models import ProcessedMessage, Transformation, UrlMetadata

_URL_PATTERN = re.compile(
    r"https?://[^\s<>\"']+|www\.[^\s<>\"']+",
    re.IGNORECASE,
)


class UrlProcessor:
    def __init__(self, config: Config) -> None:
        self._replace = config.url_replace
        self._marker = config.url_marker
        self._save_domain = config.url_save_domain

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        text = message.cleaned_text or message.original_text
        urls = _URL_PATTERN.findall(text)

        if not urls:
            return message

        message.features.contains_url = True

        for url in urls:
            domain: str | None = None
            if self._save_domain:
                try:
                    domain = urlparse(url if "://" in url else f"https://{url}").hostname
                except Exception:
                    domain = None

            message.urls.append(UrlMetadata(original=url, domain=domain))

            if self._replace:
                text = text.replace(url, self._marker)
                message.transformations.append(
                    Transformation(
                        type="replace_url",
                        original=url,
                        replacement=self._marker,
                    )
                )

        message.cleaned_text = text
        return message
