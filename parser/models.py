from dataclasses import dataclass

@dataclass
class TelegramMessage:
    tg_user_id: int
    username: str
    tg_message_id: int
    chat_id: int
    date: str
    text: str
