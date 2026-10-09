"""Read side of the web UI: profiles, activity and comment export."""
from __future__ import annotations

from dataclasses import dataclass

from db import repositories as repo
from parser.user_profile import (
    DailyActivity,
    HourlyActivity,
    UserComment,
    UserProfile,
    WeeklyActivity,
    channel_shares,
)
from parser.utils import join_name


class ProfileNotFound(LookupError):
    pass


@dataclass(frozen=True)
class ProfileDetail:
    profile: UserProfile
    hourly: list[HourlyActivity]
    daily: list[DailyActivity]
    weekly: list[WeeklyActivity]


def _profile(user, counts: list[tuple[str, int]]) -> UserProfile:
    return UserProfile(
        tg_id=user.tg_id,
        username=user.username,
        display_name=join_name(user.first_name, user.last_name) or None,
        total_messages=sum(count for _, count in counts),
        channel_count=len(counts),
        channels=channel_shares(counts),
    )


class ProfileService:
    def __init__(self, sessions, timezone: str):
        self._sessions = sessions
        # Hours and days of the activity charts are counted in this time zone.
        self._timezone = timezone

    async def list_profiles(self) -> list[UserProfile]:
        """Users whose comments were collected on request."""
        async with self._sessions() as session:
            users = await repo.list_profile_users(session)
            counts = await repo.channel_counts_of_profiles(session)
        profiles = [_profile(user, counts.get(user.id, [])) for user in users]
        return sorted(
            profiles, key=lambda p: (p.display_username.casefold(), p.tg_id)
        )

    async def detail(self, tg_id: int) -> ProfileDetail:
        async with self._sessions() as session:
            user = await self._user(session, tg_id)
            counts = await repo.channel_counts(session, user.id)
            hourly = await repo.hourly_activity(session, user.id, self._timezone)
            daily = await repo.daily_activity(session, user.id, self._timezone)
            weekly = await repo.weekly_activity(session, user.id, self._timezone)

        return ProfileDetail(
            profile=_profile(user, counts),
            hourly=[HourlyActivity(hour=h, count=hourly.get(h, 0)) for h in range(24)],
            daily=[DailyActivity(date=day.isoformat(), count=n) for day, n in daily],
            weekly=[
                WeeklyActivity(weekday=d, hour=h, count=weekly.get((d, h), 0))
                for d in range(7)
                for h in range(24)
            ],
        )

    async def require(self, tg_id: int) -> None:
        """Raises ProfileNotFound unless the user is stored."""
        async with self._sessions() as session:
            await self._user(session, tg_id)

    async def comments(self, tg_id: int) -> tuple[UserProfile, list[UserComment]]:
        """The user's comments, oldest first."""
        async with self._sessions() as session:
            user = await self._user(session, tg_id)
            counts = await repo.channel_counts(session, user.id)
            stored = await repo.user_messages(session, user.id)
        return _profile(user, counts), [
            UserComment(
                channel=c.channel, message_id=c.tg_message_id, date=c.date, text=c.text
            )
            for c in stored
        ]

    @staticmethod
    async def _user(session, tg_id: int):
        user = await repo.get_user_by_tg_id(session, tg_id)
        if user is None:
            raise ProfileNotFound(tg_id)
        return user
