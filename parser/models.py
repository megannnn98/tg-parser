from dataclasses import dataclass

@dataclass
class Message:
    discussion_id: int
    date: str
    user: str
    text: str

@dataclass
class User:
    user: str
