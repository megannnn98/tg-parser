import asyncio
from datetime import date, datetime, timezone

from sqlalchemy import func, select

from db import repositories as repo
from db.models import Channel, Message, User


def _utc(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def _message(user_id, channel_id, tg_message_id, when=None, text="text"):
    return {
        "tg_message_id": tg_message_id,
        "user_id": user_id,
        "channel_id": channel_id,
        "text": text,
        "date": when or _utc(2026, 1, 5, 10, 0),
    }


async def _seed(session, tg_id=1, channel="news"):
    user_id = await repo.upsert_user(session, tg_id, "alice")
    channel_id = await repo.upsert_channel(session, channel)
    return user_id, channel_id


def test_insert_user(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            user_id = await repo.upsert_user(session, 42, "alice", "Alice", "A")
        async with sessions() as session:
            user = await repo.get_user_by_tg_id(session, 42)
            return user_id, user

    user_id, user = run_db(scenario)

    assert user.id == user_id
    assert (user.tg_id, user.username, user.first_name, user.last_name) == (
        42,
        "alice",
        "Alice",
        "A",
    )


def test_upsert_user_updates_username_and_keeps_known_names(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            first = await repo.upsert_user(session, 42, "alice", "Alice", "A")
        async with sessions.begin() as session:
            # A comment author arrives without names: they must not be erased.
            second = await repo.upsert_user(session, 42, "alice_new")
        async with sessions() as session:
            users = (await session.scalars(select(User))).all()
            return first, second, users

    first, second, users = run_db(scenario)

    assert first == second
    assert len(users) == 1
    assert users[0].username == "alice_new"
    assert users[0].first_name == "Alice"
    assert users[0].updated_at > users[0].created_at


def test_upsert_users_many(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            await repo.upsert_user(session, 1, "old")
        async with sessions.begin() as session:
            ids = await repo.upsert_users(session, {1: "new", 2: None})
        async with sessions() as session:
            rows = (
                await session.execute(select(User.tg_id, User.username, User.id))
            ).all()
            return ids, rows

    ids, rows = run_db(scenario)

    assert {(tg_id, username) for tg_id, username, _ in rows} == {(1, "new"), (2, None)}
    assert ids == {tg_id: user_id for tg_id, _, user_id in rows}


def test_upsert_channel_is_idempotent_and_keeps_chat_ids(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            first = await repo.upsert_channel(session, "news", 100, 200)
        async with sessions.begin() as session:
            second = await repo.upsert_channel(session, "news")
        async with sessions() as session:
            return first, second, (await session.scalars(select(Channel))).all()

    first, second, channels = run_db(scenario)

    assert first == second
    assert len(channels) == 1
    assert (channels[0].telegram_chat_id, channels[0].linked_chat_id) == (100, 200)


def test_insert_message(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            user_id, channel_id = await _seed(session)
            inserted = await repo.insert_messages(
                session, [_message(user_id, channel_id, 7, text="привет")]
            )
        async with sessions() as session:
            return inserted, (await session.scalars(select(Message))).one()

    inserted, message = run_db(scenario)

    assert inserted == 1
    assert (message.tg_message_id, message.text) == (7, "привет")
    assert message.date == _utc(2026, 1, 5, 10, 0)


def test_duplicate_message_is_not_inserted_and_not_overwritten(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            user_id, channel_id = await _seed(session)
            other_channel = await repo.upsert_channel(session, "other")
            await repo.insert_messages(
                session, [_message(user_id, channel_id, 7, text="original")]
            )
        async with sessions.begin() as session:
            inserted = await repo.insert_messages(
                session,
                [
                    _message(user_id, channel_id, 7, text="changed"),
                    # The same Telegram id in another channel is another comment.
                    _message(user_id, other_channel, 7),
                ],
            )
        async with sessions() as session:
            texts = (
                await session.scalars(
                    select(Message.text).where(Message.channel_id == channel_id)
                )
            ).all()
            return inserted, texts

    inserted, texts = run_db(scenario)

    assert inserted == 1
    assert texts == ["original"]


def test_batch_insert_beyond_one_statement(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            user_id, channel_id = await _seed(session)
            # More rows than a single statement may carry bind parameters for.
            rows = [_message(user_id, channel_id, n) for n in range(1, 7001)]
            inserted = await repo.insert_messages(session, rows + rows[:10])
        async with sessions() as session:
            return inserted, await session.scalar(select(func.count(Message.id)))

    assert run_db(scenario) == (7000, 7000)


def test_concurrent_duplicate_insert(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            user_id, channel_id = await _seed(session)
        rows = [_message(user_id, channel_id, n) for n in range(1, 501)]

        async def insert():
            async with sessions.begin() as session:
                return await repo.insert_messages(session, rows)

        counts = await asyncio.gather(*(insert() for _ in range(6)))
        async with sessions() as session:
            return counts, await session.scalar(select(func.count(Message.id)))

    counts, total = run_db(scenario)

    assert total == 500
    assert sum(counts) == 500


def test_user_messages_are_ordered_and_limited_to_the_user(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            user_id, news = await _seed(session)
            other_user = await repo.upsert_user(session, 2, "bob")
            alpha = await repo.upsert_channel(session, "alpha")
            same = _utc(2026, 1, 5, 10, 0)
            await repo.insert_messages(
                session,
                [
                    _message(user_id, news, 2, same, "news-2"),
                    _message(user_id, news, 1, same, "news-1"),
                    _message(user_id, alpha, 9, same, "alpha-9"),
                    _message(user_id, news, 5, _utc(2026, 1, 4, 9, 0), "earlier"),
                    _message(other_user, news, 6, same, "bob"),
                ],
            )
        async with sessions() as session:
            return await repo.user_messages(session, user_id)

    comments = run_db(scenario)

    # By date, then channel, then Telegram id: a stable export order.
    assert [c.text for c in comments] == ["earlier", "alpha-9", "news-1", "news-2"]
    assert comments[1].channel == "alpha"
    assert comments[1].tg_message_id == 9


def test_channel_statistics(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            user_id, news = await _seed(session)
            other_user = await repo.upsert_user(session, 2, "bob")
            alpha = await repo.upsert_channel(session, "alpha")
            beta = await repo.upsert_channel(session, "beta")
            await repo.insert_messages(
                session,
                [
                    _message(user_id, news, 1),
                    _message(user_id, news, 2),
                    _message(user_id, beta, 1),
                    _message(user_id, alpha, 1),
                    _message(other_user, news, 3),
                ],
            )
            # Seen in a channel only: an author, not a collected profile.
            author = await repo.upsert_user(session, 3, "carol")
            await repo.insert_messages(session, [_message(author, news, 4)])
            await repo.mark_profiles_collected(
                session, {user_id: _utc(2026, 2, 1), other_user: _utc(2026, 2, 1)}
            )
        async with sessions() as session:
            return (
                user_id,
                other_user,
                await repo.channel_counts(session, user_id),
                await repo.list_profile_users(session),
                await repo.channel_counts_of_profiles(session),
            )

    user_id, other_user, counts, profiles, by_profile = run_db(scenario)

    # Largest first, ties by name.
    assert counts == [("news", 2), ("alpha", 1), ("beta", 1)]
    assert [user.tg_id for user in profiles] == [1, 2]
    assert by_profile == {user_id: counts, other_user: [("news", 1)]}


def test_collected_user_without_messages_is_listed(run_db):
    async def scenario(sessions):
        async with sessions.begin() as session:
            user_id = await repo.upsert_user(session, 5, None, "No", "Comments")
            await repo.mark_profiles_collected(session, {user_id: _utc(2026, 2, 1)})
        async with sessions() as session:
            return (
                await repo.list_profile_users(session),
                await repo.channel_counts_of_profiles(session),
            )

    (user,), counts = run_db(scenario)

    assert (user.tg_id, user.first_name) == (5, "No")
    assert counts == {}


async def _activity_scenario(sessions, query, tz):
    async with sessions.begin() as session:
        user_id, channel_id = await _seed(session)
        other_user = await repo.upsert_user(session, 2, "bob")
        await repo.insert_messages(
            session,
            [
                # Sunday 2026-01-04 21:30 UTC is Monday 02:30 in Almaty (+05).
                _message(user_id, channel_id, 1, _utc(2026, 1, 4, 21, 30)),
                _message(user_id, channel_id, 2, _utc(2026, 1, 4, 21, 59)),
                _message(user_id, channel_id, 3, _utc(2026, 1, 6, 12, 0)),
                _message(other_user, channel_id, 4, _utc(2026, 1, 6, 12, 0)),
            ],
        )
    async with sessions() as session:
        return await query(session, user_id, tz)


def test_hourly_activity_in_the_given_timezone(run_db):
    almaty = run_db(lambda s: _activity_scenario(s, repo.hourly_activity, "Asia/Almaty"))
    utc = run_db(lambda s: _activity_scenario(s, repo.hourly_activity, "UTC"))

    assert almaty == {2: 2, 17: 1}
    assert utc == {21: 2, 12: 1}


def test_daily_activity_in_the_given_timezone(run_db):
    almaty = run_db(lambda s: _activity_scenario(s, repo.daily_activity, "Asia/Almaty"))
    utc = run_db(lambda s: _activity_scenario(s, repo.daily_activity, "UTC"))

    assert almaty == [(date(2026, 1, 5), 2), (date(2026, 1, 6), 1)]
    assert utc == [(date(2026, 1, 4), 2), (date(2026, 1, 6), 1)]


def test_weekly_activity_in_the_given_timezone(run_db):
    almaty = run_db(lambda s: _activity_scenario(s, repo.weekly_activity, "Asia/Almaty"))
    utc = run_db(lambda s: _activity_scenario(s, repo.weekly_activity, "UTC"))

    # Monday is 0.
    assert almaty == {(0, 2): 2, (1, 17): 1}
    assert utc == {(6, 21): 2, (1, 12): 1}
