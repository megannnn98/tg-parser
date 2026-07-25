from __future__ import annotations

from typing import Any

from comment_cleaner.models import (
    ProcessedMessage,
    ReplyChainEntry,
    ReplyContext,
    Transformation,
)


class ReplyContextProcessor:
    def __init__(self, config: Any) -> None:
        from comment_cleaner.config import Config as _Cfg

        cfg: _Cfg = config
        self._enabled = cfg.load_reply_context
        self._max_depth = cfg.max_reply_depth
        self._index: dict[str, dict[str, Any]] = {}
        self._current_raw: dict[str, Any] = {}
        self._msg_id_col = cfg.column_mapping.get("message_id", "message_id")
        self._reply_col = cfg.column_mapping.get("reply_to_message_id", "reply_to_message_id")

    def set_message_index(self, message_index: dict[str, dict[str, Any]]) -> None:
        self._index = message_index

    def set_current_raw(self, raw: Any) -> None:
        if hasattr(raw, "model_dump"):
            self._current_raw = raw.model_dump()
        elif isinstance(raw, dict):
            self._current_raw = raw
        else:
            self._current_raw = {}

    def process(self, message: ProcessedMessage) -> ProcessedMessage:
        if not self._enabled or self._max_depth == 0:
            return message

        reply_to_id = self._current_raw.get("reply_to_message_id")
        if not reply_to_id:
            reply_to_id = self._current_raw.get(self._reply_col)

        if not reply_to_id:
            return message

        reply_to_str = str(reply_to_id)
        parent = self._index.get(reply_to_str)

        if parent is None or not isinstance(parent, dict):
            message.reply_context_missing = True
            message.transformations.append(
                Transformation(
                    type="reply_context_not_found",
                    details={"reply_to_message_id": reply_to_str},
                )
            )
            return message

        max_depth = max(1, min(3, self._max_depth))
        chain: list[ReplyChainEntry] = []
        visited: set[str] = {message.message_id}
        current_id = reply_to_str
        current_depth = 0

        while current_depth < max_depth and current_id not in visited:
            visited.add(current_id)
            node = self._index.get(current_id)
            if node is None or not isinstance(node, dict):
                break

            chain.append(
                ReplyChainEntry(
                    message_id=current_id,
                    text=str(node.get("text", "")),
                    user_id=str(node.get("user_id", "")),
                )
            )
            current_depth += 1

            next_reply = node.get("reply_to_message_id")
            if not next_reply:
                break
            current_id = str(next_reply)

        parent_text = parent.get("text", "")
        parent_user = parent.get("user_id")

        message.reply_context = ReplyContext(
            message_id=reply_to_str,
            text=str(parent_text) if parent_text else None,
            user_id=str(parent_user) if parent_user else None,
            context_depth=len(chain),
            chain=chain,
        )
        message.features.contains_reply_context = True
        return message
