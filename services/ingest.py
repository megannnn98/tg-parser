"""Stores what the Telegram collectors fetch. One call is one transaction."""
from __future__ import annotations

from datetime import datetime, timezone

from db import repositories as repo
from parser.telegram import CollectedMessage, TelegramUser, UserComment


class CommentIngest:
    def __init__(self, sessions):
        self._sessions = sessions

    async def save_channel(
        self, username: str, chat_id: int | None, linked_chat_id: int | None
    ) -> int:
        async with self._sessions.begin() as session:
            return await repo.upsert_channel(session, username, chat_id, linked_chat_id)

    async def save_history(
        self, channel_id: int, messages: list[CollectedMessage]
    ) -> int:
        """Stores a batch of a channel's comments with their authors; returns new rows."""
        if not messages:
            return 0
        async with self._sessions.begin() as session:
            user_ids = await repo.upsert_users(
                session, {msg.tg_id: msg.username for msg in messages}
            )
            return await repo.insert_messages(
                session,
                [
                    {
                        "tg_message_id": msg.message_id,
                        "user_id": user_ids[msg.tg_id],
                        "channel_id": channel_id,
                        "text": msg.text,
                        "date": msg.date,
                    }
                    for msg in messages
                ],
            )

    async def find_profile(self, user_ref: int | str) -> tuple[int, TelegramUser] | None:
        async with self._sessions() as session:
            user = await repo.find_profile(session, user_ref)
        if user is None:
            return None
        return user.id, TelegramUser(
            tg_id=user.tg_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
        )

    async def save_profile(self, user: TelegramUser) -> int:
        """Stores a freshly resolved user and makes them a profile."""
        async with self._sessions.begin() as session:
            user_id = await repo.upsert_user(
                session, user.tg_id, user.username, user.first_name, user.last_name
            )
            await repo.mark_profiles_collected(
                session, {user_id: datetime.now(timezone.utc)}
            )
            return user_id

    async def touch_profile(self, user_id: int) -> None:
        async with self._sessions.begin() as session:
            await repo.mark_profiles_collected(
                session, {user_id: datetime.now(timezone.utc)}
            )

    async def save_user_comments(
        self,
        user_id: int,
        channel_id: int,
        comments: list[UserComment],
        refresh_text: bool = False,
    ) -> int:
        if not comments:
            return 0
        # One statement must not carry the same comment twice when it updates.
        unique = {comment.message_id: comment for comment in comments}
        async with self._sessions.begin() as session:
            return await repo.insert_messages(
                session,
                [
                    {
                        "tg_message_id": comment.message_id,
                        "user_id": user_id,
                        "channel_id": channel_id,
                        "text": comment.text,
                        "date": comment.date,
                    }
                    for comment in unique.values()
                ],
                refresh_text=refresh_text,
            )
