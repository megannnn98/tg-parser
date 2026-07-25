from __future__ import annotations

import hashlib
import hmac
import os
import re
from typing import Any


class Pseudonymizer:
    def __init__(self, salt_env_var: str = "USER_ID_HASH_SALT") -> None:
        self._salt = os.getenv(salt_env_var, "")

    @property
    def is_active(self) -> bool:
        return bool(self._salt)

    def hash_value(self, value: str | None) -> str | None:
        if value is None:
            return None
        if not self._salt:
            return value
        digest = hmac.new(
            self._salt.encode("utf-8"),
            value.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return digest[:16]

    def pseudonymize_message(self, message: dict[str, Any]) -> dict[str, Any]:
        if not self._salt:
            return message

        result = dict(message)

        if "user_id" in result and result["user_id"] is not None:
            result["user_id"] = self.hash_value(str(result["user_id"]))

        if "chat_id" in result and result["chat_id"] is not None:
            result["chat_id"] = self.hash_value(str(result["chat_id"]))

        if "reply_context" in result and isinstance(result["reply_context"], dict):
            ctx = result["reply_context"]
            if "user_id" in ctx and ctx["user_id"] is not None:
                ctx["user_id"] = self.hash_value(str(ctx["user_id"]))
            result["reply_context"] = ctx

        mentions = result.get("mentions", [])
        if mentions:
            result["mentions"] = [self.hash_value(str(m)) if m else m for m in mentions]

        return result


_PHONE_RE = re.compile(r"\+?[78][\s\-]?\(?[0-9]{3}\)?[\s\-]?[0-9]{3}[\s\-]?[0-9]{2}[\s\-]?[0-9]{2}")
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")


def mask_pii(text: str) -> str:
    text = _PHONE_RE.sub("[PHONE]", text)
    text = _EMAIL_RE.sub("[EMAIL]", text)
    return text
