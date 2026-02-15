from dataclasses import dataclass

@dataclass
class TelegramMessage:
    tg_user_id: int
    channel: str
    text: str
    date: str
