from dataclasses import dataclass

@dataclass
class TelegramMessage:
    tg_user_id: int
    username: str
    tg_message_id: int
    discussion_id: int
    date: str
    text: str
