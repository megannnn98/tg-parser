from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

_DEFAULT_CONFIG = {
    "input": {
        "type": "jsonl",
        "path": "data/comments.jsonl",
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
        "path": "output/cleaned.jsonl",
        "error_path": "output/errors.jsonl",
        "batch_size": 1000,
        "resume": False,
        "progress": True,
    },
    "normalization": {
        "unicode_form": "NFC",
        "max_repeated_letters": 3,
        "max_repeated_punctuation": 3,
        "max_repeated_brackets": 3,
        "preserve_emoji": True,
        "preserve_hashtags": True,
    },
    "urls": {
        "replace_with_marker": True,
        "marker": "[URL]",
        "save_domain": True,
    },
    "mentions": {
        "replace_with_marker": True,
        "marker": "[MENTION]",
    },
    "duplicates": {
        "mode": "mark",
        "fuzzy_enabled": False,
        "fuzzy_threshold": 95,
    },
    "context": {
        "load_reply_context": True,
        "max_reply_depth": 1,
    },
    "filtering": {
        "remove_system_messages": False,
        "remove_bot_messages": False,
        "remove_low_information": False,
    },
    "privacy": {
        "pseudonymize_user_ids": False,
        "salt_env_variable": "USER_ID_HASH_SALT",
    },
    "sarcasm_detection": {
        "enabled": True,
        "signals": [
            "/s",
            "ну да, конечно",
            "ага, конечно",
            "конечно, конечно",
            "разумеется",
        ],
    },
    "processing_version": "1.0.0",
}


class Config:
    def __init__(self, data: dict[str, Any]) -> None:
        self._data = data

    def get(self, *keys: str, default: Any = None) -> Any:
        node: Any = self._data
        for key in keys:
            if isinstance(node, dict):
                node = node.get(key)
            else:
                return default
            if node is None:
                return default
        return node

    @property
    def input_type(self) -> str:
        return str(self.get("input", "type", default="jsonl"))

    @property
    def input_path(self) -> str:
        return str(self.get("input", "path", default="data/comments.jsonl"))

    @property
    def column_mapping(self) -> dict[str, str]:
        return dict(self.get("input", "column_mapping", default={}))

    @property
    def output_path(self) -> str:
        return str(self.get("output", "path", default="output/cleaned.jsonl"))

    @property
    def error_path(self) -> str:
        return str(self.get("output", "error_path", default="output/errors.jsonl"))

    @property
    def batch_size(self) -> int:
        return int(self.get("output", "batch_size", default=1000))

    @property
    def resume(self) -> bool:
        return bool(self.get("output", "resume", default=False))

    @property
    def show_progress(self) -> bool:
        return bool(self.get("output", "progress", default=True))

    @property
    def unicode_form(self) -> str:
        return str(self.get("normalization", "unicode_form", default="NFC"))

    @property
    def max_repeated_letters(self) -> int:
        return int(self.get("normalization", "max_repeated_letters", default=3))

    @property
    def max_repeated_punctuation(self) -> int:
        return int(self.get("normalization", "max_repeated_punctuation", default=3))

    @property
    def max_repeated_brackets(self) -> int:
        return int(self.get("normalization", "max_repeated_brackets", default=3))

    @property
    def preserve_emoji(self) -> bool:
        return bool(self.get("normalization", "preserve_emoji", default=True))

    @property
    def preserve_hashtags(self) -> bool:
        return bool(self.get("normalization", "preserve_hashtags", default=True))

    @property
    def url_replace(self) -> bool:
        return bool(self.get("urls", "replace_with_marker", default=True))

    @property
    def url_marker(self) -> str:
        return str(self.get("urls", "marker", default="[URL]"))

    @property
    def url_save_domain(self) -> bool:
        return bool(self.get("urls", "save_domain", default=True))

    @property
    def mention_replace(self) -> bool:
        return bool(self.get("mentions", "replace_with_marker", default=True))

    @property
    def mention_marker(self) -> str:
        return str(self.get("mentions", "marker", default="[MENTION]"))

    @property
    def duplicate_mode(self) -> str:
        return str(self.get("duplicates", "mode", default="mark"))

    @property
    def fuzzy_enabled(self) -> bool:
        return bool(self.get("duplicates", "fuzzy_enabled", default=False))

    @property
    def fuzzy_threshold(self) -> int:
        return int(self.get("duplicates", "fuzzy_threshold", default=95))

    @property
    def load_reply_context(self) -> bool:
        return bool(self.get("context", "load_reply_context", default=True))

    @property
    def max_reply_depth(self) -> int:
        return int(self.get("context", "max_reply_depth", default=1))

    @property
    def remove_system_messages(self) -> bool:
        return bool(self.get("filtering", "remove_system_messages", default=False))

    @property
    def remove_bot_messages(self) -> bool:
        return bool(self.get("filtering", "remove_bot_messages", default=False))

    @property
    def remove_low_information(self) -> bool:
        return bool(self.get("filtering", "remove_low_information", default=False))

    @property
    def pseudonymize(self) -> bool:
        return bool(self.get("privacy", "pseudonymize_user_ids", default=False))

    @property
    def salt_env_variable(self) -> str:
        return str(self.get("privacy", "salt_env_variable", default="USER_ID_HASH_SALT"))

    @property
    def sarcasm_enabled(self) -> bool:
        return bool(self.get("sarcasm_detection", "enabled", default=True))

    @property
    def sarcasm_signals(self) -> list[str]:
        return list(self.get("sarcasm_detection", "signals", default=[]))

    @property
    def processing_version(self) -> str:
        return str(self.get("processing_version", default="1.0.0"))


def load_config(config_path: str | Path | None = None) -> Config:
    if config_path and Path(config_path).exists():
        with open(config_path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    else:
        data = {}

    merged: dict[str, Any] = _deep_merge(_DEFAULT_CONFIG, data)
    return Config(merged)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_dictionary(path: str | Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data if isinstance(data, dict) else {}
