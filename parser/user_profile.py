"""What a user's profile page shows. Pure data: services/profiles.py fills it."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

_CHART_COLORS = (
    "#2563eb",
    "#dc2626",
    "#16a34a",
    "#ca8a04",
    "#9333ea",
    "#0891b2",
    "#ea580c",
    "#4f46e5",
    "#be123c",
    "#15803d",
)


@dataclass(frozen=True)
class ChannelProfile:
    name: str
    message_count: int
    percent: float
    color: str
    dasharray: str
    dashoffset: float


@dataclass(frozen=True)
class HourlyActivity:
    hour: int
    count: int


@dataclass(frozen=True)
class DailyActivity:
    date: str
    count: int


@dataclass(frozen=True)
class WeeklyActivity:
    weekday: int  # 0 is Monday
    hour: int
    count: int


@dataclass(frozen=True)
class UserComment:
    channel: str
    message_id: int
    date: datetime
    text: str


@dataclass(frozen=True)
class UserProfile:
    tg_id: int
    username: str | None
    display_name: str | None
    total_messages: int
    channel_count: int
    channels: list[ChannelProfile]

    @property
    def display_username(self) -> str:
        if self.username:
            return f"@{self.username}"
        if self.display_name:
            return self.display_name
        return "нет ника"


def channel_shares(counts: list[tuple[str, int]]) -> list[ChannelProfile]:
    """Donut segments for (channel, message count) pairs, largest first."""
    total = sum(count for _, count in counts)
    offset = 0.0
    channels: list[ChannelProfile] = []
    for index, (name, count) in enumerate(counts):
        percent = round(count * 100 / total, 1)
        # The last segment closes the gap that rounding leaves.
        if index == len(counts) - 1:
            percent = round(100 - offset, 1)
        channels.append(
            ChannelProfile(
                name=name,
                message_count=count,
                percent=percent,
                color=_CHART_COLORS[index % len(_CHART_COLORS)],
                dasharray=f"{percent} {round(100 - percent, 1)}",
                dashoffset=-round(offset, 1),
            )
        )
        offset += percent

    return channels


def render_user_comments_text(comments: list[UserComment]) -> str:
    return "\n\n".join(c.text for c in comments)
