from __future__ import annotations

import json
from pathlib import Path

from comment_cleaner.models import ProcessedMessage, UserBatch


def group_users_from_jsonl(
    input_path: str | Path,
    max_messages_per_user: int | None = None,
    max_chars_per_user: int | None = None,
    exclude_duplicates: bool = True,
    exclude_low_information: bool = False,
    exclude_system: bool = True,
    exclude_bots: bool = True,
) -> list[UserBatch]:
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    users: dict[str, list[ProcessedMessage]] = {}

    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                msg = ProcessedMessage.model_validate(data)
            except Exception:
                continue

            if exclude_duplicates and msg.is_duplicate:
                continue
            if exclude_low_information and msg.features.low_information:
                continue
            if exclude_system and msg.features.is_system_message:
                continue
            if exclude_bots and msg.features.is_bot_message:
                continue

            uid = str(msg.user_id)
            if uid not in users:
                users[uid] = []
            users[uid].append(msg)

    batches: list[UserBatch] = []

    for uid, comments in users.items():
        comments.sort(key=lambda m: m.timestamp or "")

        selected: list[ProcessedMessage]
        if max_messages_per_user and len(comments) > max_messages_per_user:
            step = len(comments) // max_messages_per_user
            selected = comments[:: max(step, 1)][:max_messages_per_user]
        elif max_chars_per_user:
            selected = []
            current_chars = 0
            for c in comments:
                msg_len = len(c.author_text or c.original_text)
                if current_chars + msg_len <= max_chars_per_user:
                    selected.append(c)
                    current_chars += msg_len
                else:
                    break
        else:
            selected = comments

        batch = UserBatch(
            user_id=uid,
            comments_count=len(selected),
            total_chars=sum(len(c.author_text or c.original_text) for c in selected),
            first_timestamp=selected[0].timestamp if selected else None,
            last_timestamp=selected[-1].timestamp if selected else None,
            comments=selected,
        )
        batches.append(batch)

    batches.sort(key=lambda b: b.comments_count, reverse=True)
    return batches
