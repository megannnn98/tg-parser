import asyncio
from types import SimpleNamespace

from parser.collector import collect_channel


def test_collect_channel_enqueues_user_even_with_null_username():
    async def _run():
        queue: asyncio.Queue = asyncio.Queue()

        class FakeClient:
            async def get_chat(self, channel_username):
                assert channel_username == "chan_a"
                return SimpleNamespace(linked_chat=SimpleNamespace(id=42))

        async def fake_fetch_messages(_tg_client, _chat_id):
            yield {
                "tg_id": 123,
                "username": None,
                "text": "first",
                "date": "2025-02-01",
            }
            yield {
                "tg_id": 123,
                "username": None,
                "text": "second",
                "date": "2025-02-02",
            }

        class FakeLogger:
            def warning(self, *_args, **_kwargs):
                pass

        await collect_channel(
            tg_client=FakeClient(),
            queue=queue,
            channel_username="chan_a",
            fetch_messages_fn=fake_fetch_messages,
            logger=FakeLogger(),
        )

        items = []
        while not queue.empty():
            items.append(queue.get_nowait())
        return items

    items = asyncio.run(_run())
    assert items == [
        ("channel", "chan_a"),
        ("user", 123, None),
        ("message", (123, "chan_a", "first", "2025-02-01")),
        ("message", (123, "chan_a", "second", "2025-02-02")),
    ]
