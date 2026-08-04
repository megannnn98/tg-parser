import json
import sqlite3
from pathlib import Path

import httpx
import pytest

from parser.political_coords import (
    AggregatedCoords,
    AxisStats,
    PoliticalCoordsError,
    _aggregate,
    _filter_messages,
    _parse_ndjson_lines,
    _split_batches,
    analyze_political_coords,
)
from parser.user_profile import UserComment


class FakeResponse:
    def __init__(self, status_code: int, json_data: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("error", request=object(), response=self)


class FakeClient:
    def __init__(self, response_data: dict | None = None, status_code: int = 200):
        self._response_data = response_data
        self._status_code = status_code
        self.posts: list[dict] = []
        self.closed = False

    async def post(self, url, **kwargs):
        self.posts.append(kwargs)
        return FakeResponse(
            self._status_code,
            self._response_data,
        )

    async def aclose(self):
        self.closed = True


def _ndjson_response(*analyses: dict) -> dict:
    lines = []
    for ax in analyses:
        obj = {
            "text": "msg",
            "language": "ru",
            "analysis_unit": "statement",
            "axes": ax,
            "metadata": {
                "is_quote": False,
                "is_ironic": False,
                "is_question": False,
                "contains_multiple_positions": False,
                "targets": [],
                "mentioned_ideologies": [],
                "overall_confidence": 0.5,
                "requires_context": False,
            },
            "summary": "",
        }
        lines.append(json.dumps(obj, ensure_ascii=False))
    return {
        "choices": [{"message": {"content": "\n".join(lines)}}],
    }


def _create_user_db(db_path: Path, rows: list[tuple[int, str, int, str, str]]):
    with sqlite3.connect(db_path) as db:
        db.execute(
            """
            CREATE TABLE user_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER NOT NULL,
                username TEXT,
                channel TEXT NOT NULL,
                message_id INTEGER NOT NULL,
                text TEXT NOT NULL,
                date TEXT NOT NULL,
                UNIQUE(channel, message_id)
            )
            """
        )
        db.executemany(
            "INSERT INTO user_messages (tg_id, username, channel, message_id, text, date) VALUES (?, ?, ?, ?, ?, ?)",
            rows,
        )


def _axis_dict(
    score: float = 0.0,
    confidence: float = 0.0,
    status: str = "insufficient",
) -> dict:
    return {
        "score": score,
        "confidence": confidence,
        "evidence": [],
        "reasoning": "",
        "status": status,
    }


def _insufficient_axes() -> dict:
    return {k: _axis_dict() for k in ["economic", "authority", "social", "nationalism", "democracy", "militarism"]}


class TestFilterMessages:
    def test_skips_short_messages(self):
        comments = [
            UserComment(channel="a", date="2026-01-01", text="ok"),
            UserComment(channel="b", date="2026-01-02", text="x" * 40),
            UserComment(channel="c", date="2026-01-03", text="short"),
        ]
        result = _filter_messages(comments, min_length=40)
        assert result == ["x" * 40]

    def test_returns_all_when_all_long_enough(self):
        comments = [
            UserComment(channel="a", date="2026-01-01", text="x" * 50),
            UserComment(channel="b", date="2026-01-02", text="y" * 50),
        ]
        result = _filter_messages(comments, min_length=40)
        assert len(result) == 2


class TestSplitBatches:
    def test_splits_evenly(self):
        msgs = [f"msg{i}" for i in range(10)]
        batches = _split_batches(msgs, 3)
        assert len(batches) == 4
        assert batches[0] == ["msg0", "msg1", "msg2"]
        assert batches[-1] == ["msg9"]

    def test_single_batch(self):
        msgs = ["a", "b"]
        assert _split_batches(msgs, 10) == [["a", "b"]]


class TestParseNdjsonLines:
    def test_parses_valid_json_lines(self):
        raw = '{"a":1}\n{"b":2}\n'
        result = _parse_ndjson_lines(raw)
        assert result == [{"a": 1}, {"b": 2}]

    def test_strips_markdown_fences(self):
        raw = '```json\n{"a":1}\n```\n\n{"b":2}\n'
        result = _parse_ndjson_lines(raw)
        assert result == [{"a": 1}, {"b": 2}]

    def test_skips_invalid_json(self):
        raw = '{"a":1}\nnot json\n{"b":2}\n'
        result = _parse_ndjson_lines(raw)
        assert result == [{"a": 1}, {"b": 2}]


class TestAggregate:
    def test_counts_signals_per_axis(self):
        analyses = [
            {
                "axes": {
                    "economic": {"score": -0.5, "status": "expressed"},
                    "authority": {"score": 0.3, "status": "weak_signal"},
                    "social": {"score": 0.0, "status": "insufficient"},
                    "nationalism": {"score": 0.0, "status": "insufficient"},
                    "democracy": {"score": 0.0, "status": "insufficient"},
                    "militarism": {"score": 0.0, "status": "insufficient"},
                },
            },
        ]
        result = _aggregate(analyses, total_comments=10)
        assert result.axes["economic"].left_count == 1
        assert result.axes["economic"].right_count == 0
        assert result.axes["authority"].left_count == 0
        assert result.axes["authority"].right_count == 1
        assert result.signal_count == 1

    def test_mixed_status_counts_both_sides(self):
        analyses = [
            {
                "axes": {
                    "economic": {"score": -0.2, "status": "mixed"},
                    "authority": {"score": 0.0, "status": "insufficient"},
                    "social": {"score": 0.0, "status": "insufficient"},
                    "nationalism": {"score": 0.0, "status": "insufficient"},
                    "democracy": {"score": 0.0, "status": "insufficient"},
                    "militarism": {"score": 0.0, "status": "insufficient"},
                },
            },
        ]
        result = _aggregate(analyses, total_comments=10)
        assert result.axes["economic"].left_count == 1
        assert result.axes["economic"].right_count == 1

    def test_ignores_insufficient(self):
        analyses = [{"axes": _insufficient_axes()}]
        result = _aggregate(analyses, total_comments=10)
        assert result.signal_count == 0
        for ax in result.axes.values():
            assert ax.left_count == 0
            assert ax.right_count == 0

    def test_total_messages_independent_of_analysis_count(self):
        analyses = [{"axes": _insufficient_axes()}]
        result = _aggregate(analyses, total_comments=100)
        assert result.total_messages == 100


class TestRenderBars:
    def test_all_insufficient_shows_empty_bars(self):
        result = AggregatedCoords(
            total_messages=100,
            signal_count=0,
            axes={key: AxisStats() for key in ["economic", "authority", "social", "nationalism", "democracy", "militarism"]},
        )
        bars = result.render_bars()
        assert "Левая" in bars
        assert "Правая" in bars
        assert "Свобода" in bars
        assert "Авторитаризм" in bars
        assert "Итого: 0 из 100 сообщений (0%)" in bars

    def test_shows_filled_bars_with_signals(self):
        axes = {
            "economic": AxisStats(left_count=5, right_count=1),
            "authority": AxisStats(left_count=0, right_count=0),
            "social": AxisStats(left_count=0, right_count=3),
            "nationalism": AxisStats(left_count=0, right_count=0),
            "democracy": AxisStats(left_count=0, right_count=0),
            "militarism": AxisStats(left_count=1, right_count=1),
        }
        result = AggregatedCoords(total_messages=50, signal_count=6, axes=axes)
        bars = result.render_bars()
        assert "Итого: 6 из 50 сообщений (12%)" in bars
        assert "████" in bars
        assert "Левая" in bars
        assert "Консерватор" in bars

    def test_right_dominant_bar_fills_from_left(self):
        axes = {
            "economic": AxisStats(left_count=1, right_count=9),
            "authority": AxisStats(left_count=0, right_count=0),
            "social": AxisStats(left_count=0, right_count=0),
            "nationalism": AxisStats(left_count=0, right_count=0),
            "democracy": AxisStats(left_count=0, right_count=0),
            "militarism": AxisStats(left_count=0, right_count=0),
        }
        result = AggregatedCoords(total_messages=10, signal_count=10, axes=axes)
        bars = result.render_bars()
        assert "██░░░░░░░░░░░░░░░░░░] Правая" in bars


class TestAnalyzePoliticalCoords:
    @pytest.mark.asyncio
    async def test_returns_aggregated_result(self, tmp_path: Path):
        db_path = tmp_path / "user_7.db"
        _create_user_db(
            db_path,
            [
                (7, "user1", "chan_a", 1, "A" * 50, "2026-01-01"),
                (7, "user1", "chan_a", 2, "B" * 50, "2026-01-02"),
            ],
        )

        axes = {
            "economic": _axis_dict(-0.6, 0.8, "expressed"),
            "authority": _axis_dict(0.0, 0.0, "insufficient"),
            "social": _axis_dict(0.0, 0.0, "insufficient"),
            "nationalism": _axis_dict(0.0, 0.0, "insufficient"),
            "democracy": _axis_dict(0.0, 0.0, "insufficient"),
            "militarism": _axis_dict(0.0, 0.0, "insufficient"),
        }
        client = FakeClient(response_data=_ndjson_response(axes, axes))

        result = await analyze_political_coords(
            db_path, tg_id=7, api_key="test-key", http_client=client
        )

        assert result.total_messages == 2
        assert result.axes["economic"].left_count == 2

    @pytest.mark.asyncio
    async def test_short_messages_are_filtered_out(self, tmp_path: Path):
        db_path = tmp_path / "user_7.db"
        _create_user_db(
            db_path,
            [
                (7, "user1", "chan_a", 1, "hi", "2026-01-01"),
                (7, "user1", "chan_a", 2, "A" * 50, "2026-01-02"),
            ],
        )

        axes = {
            "economic": _axis_dict(-0.6, 0.8, "expressed"),
            "authority": _axis_dict(0.0, 0.0, "insufficient"),
            "social": _axis_dict(0.0, 0.0, "insufficient"),
            "nationalism": _axis_dict(0.0, 0.0, "insufficient"),
            "democracy": _axis_dict(0.0, 0.0, "insufficient"),
            "militarism": _axis_dict(0.0, 0.0, "insufficient"),
        }
        client = FakeClient(response_data=_ndjson_response(axes))

        result = await analyze_political_coords(
            db_path, tg_id=7, api_key="test-key", http_client=client
        )

        assert result.total_messages == 2

    @pytest.mark.asyncio
    async def test_raises_on_api_error(self, tmp_path: Path):
        db_path = tmp_path / "user_7.db"
        _create_user_db(db_path, [(7, "user1", "chan_a", 1, "A" * 50, "2026-01-01")])

        client = FakeClient(status_code=401, response_data={"error": "unauthorized"})

        with pytest.raises(PoliticalCoordsError, match="401"):
            await analyze_political_coords(
                db_path, tg_id=7, api_key="test-key", http_client=client
            )

    @pytest.mark.asyncio
    async def test_no_filtered_messages_returns_empty(self, tmp_path: Path):
        db_path = tmp_path / "user_7.db"
        _create_user_db(db_path, [(7, "user1", "chan_a", 1, "hi", "2026-01-01")])

        result = await analyze_political_coords(
            db_path, tg_id=7, api_key="test-key",
            http_client=FakeClient(response_data=_ndjson_response()),
        )

        assert result.total_messages == 1
        assert result.signal_count == 0

    @pytest.mark.asyncio
    async def test_no_api_key_raises(self, tmp_path: Path):
        import os
        old_key = os.environ.pop("DEEPSEEK_API_KEY", None)
        try:
            db_path = tmp_path / "user_7.db"
            _create_user_db(db_path, [(7, "user1", "chan_a", 1, "A" * 50, "2026-01-01")])

            with pytest.raises(PoliticalCoordsError, match="DEEPSEEK_API_KEY"):
                await analyze_political_coords(db_path, tg_id=7, api_key="")
        finally:
            if old_key is not None:
                os.environ["DEEPSEEK_API_KEY"] = old_key
