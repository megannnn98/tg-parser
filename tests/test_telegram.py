import asyncio
import importlib
import sys
from types import SimpleNamespace


def _load_telegram_module(monkeypatch):
    monkeypatch.setenv("API_ID", "12345")
    monkeypatch.setenv("API_HASH", "hash123")
    monkeypatch.setenv("LIMIT", "2")

    fake_pyrogram = SimpleNamespace(Client=object)
    monkeypatch.setitem(sys.modules, "pyrogram", fake_pyrogram)

    sys.modules.pop("config", None)
    sys.modules.pop("parser.telegram", None)
    return importlib.import_module("parser.telegram")


def test_get_client_uses_config_values(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)
    captured: dict[str, object] = {}

    class FakeClient:
        def __init__(self, session_name, api_id, api_hash):
            captured["session_name"] = session_name
            captured["api_id"] = api_id
            captured["api_hash"] = api_hash

    monkeypatch.setattr(telegram, "Client", FakeClient)
    client = telegram.get_client()

    assert isinstance(client, FakeClient)
    assert captured == {
        "session_name": "my_session",
        "api_id": 12345,
        "api_hash": "hash123",
    }


def test_fetch_messages_filters_invalid_and_maps_fields(monkeypatch):
    telegram = _load_telegram_module(monkeypatch)

    valid_msg = SimpleNamespace(
        text="Hello",
        from_user=SimpleNamespace(id=10, username="alice"),
        id=777,
        date="2025-02-15 10:00:00",
    )
    no_text_msg = SimpleNamespace(
        text=None,
        from_user=SimpleNamespace(id=11, username="bob"),
        id=778,
        date="2025-02-15 10:01:00",
    )
    no_user_msg = SimpleNamespace(
        text="World",
        from_user=None,
        id=779,
        date="2025-02-15 10:02:00",
    )

    class FakeTGClient:
        def __init__(self):
            self.calls = []

        async def get_chat_history(self, chat_id, limit):
            self.calls.append((chat_id, limit))
            for msg in [no_text_msg, no_user_msg, valid_msg]:
                yield msg

    async def _run():
        tg_client = FakeTGClient()
        result = []
        async for row in telegram.fetch_messages(tg_client, 42):
            result.append(row)
        return tg_client.calls, result

    calls, result = asyncio.run(_run())
    assert calls == [(42, 2)]
    assert result == [
        {
            "tg_id": 10,
            "username": "alice",
            "message_id": 777,
            "date": "2025-02-15 10:00:00",
            "text": "Hello",
        }
    ]
