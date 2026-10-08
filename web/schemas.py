"""Response models of the JSON API (/api/v1), the source of the frontend's types."""

from __future__ import annotations

from pydantic import BaseModel

from parser.user_profile import (
    DailyActivity,
    HourlyActivity,
    UserProfile,
    WeeklyActivity,
)


class CollectRequest(BaseModel):
    username: str


class ChannelsRequest(BaseModel):
    channels_text: str


class ChannelsResponse(BaseModel):
    channels: list[str]


class ChannelShare(BaseModel):
    name: str
    message_count: int
    percent: float
    color: str
    dasharray: str
    dashoffset: float


class Profile(BaseModel):
    db_name: str
    tg_id: int
    username: str | None
    display_name: str | None
    display_username: str
    total_messages: int
    channel_count: int
    channels: list[ChannelShare]

    @classmethod
    def of(cls, profile: UserProfile) -> Profile:
        return cls(
            db_name=profile.db_name,
            tg_id=profile.tg_id,
            username=profile.username,
            display_name=profile.display_name,
            display_username=profile.display_username,
            total_messages=profile.total_messages,
            channel_count=profile.channel_count,
            channels=[
                ChannelShare(
                    name=channel.name,
                    message_count=channel.message_count,
                    percent=channel.percent,
                    color=channel.color,
                    dasharray=channel.dasharray,
                    dashoffset=channel.dashoffset,
                )
                for channel in profile.channels
            ],
        )


class HourCount(BaseModel):
    hour: int
    count: int


class DayCount(BaseModel):
    date: str
    count: int


class WeekHourCount(BaseModel):
    weekday: int
    hour: int
    count: int


class UserDetail(BaseModel):
    profile: Profile
    hourly_activity: list[HourCount]
    daily_activity: list[DayCount]
    weekly_activity: list[WeekHourCount]

    @classmethod
    def of(
        cls,
        profile: UserProfile,
        hourly: list[HourlyActivity],
        daily: list[DailyActivity],
        weekly: list[WeeklyActivity],
    ) -> UserDetail:
        return cls(
            profile=Profile.of(profile),
            hourly_activity=[HourCount(hour=h.hour, count=h.count) for h in hourly],
            daily_activity=[DayCount(date=d.date, count=d.count) for d in daily],
            weekly_activity=[
                WeekHourCount(weekday=w.weekday, hour=w.hour, count=w.count)
                for w in weekly
            ],
        )


class JobStarted(BaseModel):
    job_id: str


class ResolvedUser(BaseModel):
    tg_id: int
    username: str | None
    display_name: str | None


class ChannelStatus(BaseModel):
    channel: str
    status: str
    saved: int
    error: str | None


class JobStatus(BaseModel):
    job_id: str
    user_ref: int | str
    total_channels: int
    state: str
    resolved: ResolvedUser | None
    channels: list[ChannelStatus]
    saved_total: int
    db_name: str | None
    error: str | None


class JobCancelled(BaseModel):
    cancelled: bool


class AxisCounts(BaseModel):
    left_count: int
    right_count: int


class PoliticalCoords(BaseModel):
    total_messages: int
    signal_count: int
    bars: str
    axes: dict[str, AxisCounts]
