from __future__ import annotations

import asyncio
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from parser.logger import get_logger
from parser.user_profile import UserComment, fetch_user_comments

_logger = get_logger("political_coords")

_MIN_MESSAGE_LENGTH = 40
_BATCH_SIZE = 15
_MAX_CONCURRENT_BATCHES = 3

AXIS_LABELS: dict[str, str] = {
    "economic": "Экономика",
    "authority": "Авторитаризм",
    "social": "Общество",
    "nationalism": "Нации",
    "democracy": "Демократия",
    "militarism": "Милитаризм",
}

AXIS_POLES: dict[str, tuple[str, str]] = {
    "economic": ("Левая", "Правая"),
    "authority": ("Свобода", "Авторитаризм"),
    "social": ("Прогресс", "Консерватор"),
    "nationalism": ("Космополит", "Национал"),
    "democracy": ("Демократия", "Автократия"),
    "militarism": ("Пацифизм", "Милитарист"),
}


@dataclass
class AxisStats:
    left_count: int = 0
    right_count: int = 0


@dataclass
class AggregatedCoords:
    total_messages: int
    signal_count: int
    axes: dict[str, AxisStats] = field(default_factory=dict)

    def render_bars(self) -> str:
        lines: list[str] = []
        for axis_key in AXIS_LABELS:
            stats = self.axes.get(axis_key, AxisStats())
            total = stats.left_count + stats.right_count
            pole_left, pole_right = AXIS_POLES[axis_key]
            if total == 0:
                filled = 0
            else:
                left_ratio = stats.left_count / total
                filled = max(1, round(left_ratio * 20))
                filled = min(filled, 20)
            bar = "█" * filled + "░" * (20 - filled)
            lines.append(f"{pole_left:<10} [{bar}] {pole_right}")

        total = self.total_messages
        pct = self.signal_count * 100 / total if total else 0
        lines.append("")
        lines.append(f"Итого: {self.signal_count} из {total} сообщений ({pct:.0f}%) содержат политические сигналы.")
        return "\n".join(lines)


SYSTEM_PROMPT = """Ты — анализатор политических координат. Проанализируй каждое высказывание ниже и верни результат строго в формате JSON, по одному объекту на строку (JSON Lines).

Для каждого высказывания примени алгоритм:
1. Определи язык (ru/en).
2. Выдели собственную позицию автора (отдели цитаты, иронию, вопросы, описания).
3. По каждой из 6 осей (economic, authority, social, nationalism, democracy, militarism):
   - Найди прямые текстовые признаки из определений осей.
   - Рассчитай score [-1.0, +1.0] по калибровке: 0-0.19 крайне слабый, 0.20-0.39 слабый, 0.40-0.59 умеренный, 0.60-0.79 сильный, 0.80-1.00 радикальный.
   - Рассчитай confidence [0.0, 1.0].
   - Выбери status: expressed | weak_signal | mixed | insufficient | not_applicable.
   - Добавь точные дословные evidence-фрагменты.
4. Оси НЕ выводятся друг из друга.
5. Отсутствие данных = score 0.0 + status insufficient, НЕ центристская позиция.

ОСИ:
- economic [-1=левая, +1=правая]: национализация/перераспределение ↔ приватизация/свободный рынок
- authority [-1=антиавторитарная, +1=авторитарная]: свобода слова/собраний ↔ цензура/репрессии/жёсткая рука
- social [-1=прогрессизм, +1=консерватизм]: ЛГБТ/гендерное равенство ↔ традиционная семья/религиозная мораль
- nationalism [-1=космополитизм, +1=национализм]: интернационализм/открытость ↔ приоритет нации/ограничение миграции
- democracy [-1=демократия, +1=автократия]: выборы/плюрализм ↔ несменяемый лидер/запрет оппозиции
- militarism [-1=пацифизм, +1=милитаризм]: дипломатия/антивоенная ↔ поддержка войны/силовое решение

Формат объекта JSON (один на строку, без переноса):
{
  "text": "<дословный текст>",
  "language": "ru|en",
  "analysis_unit": "statement",
  "axes": {
    "economic": {"score": 0.0, "confidence": 0.0, "evidence": [], "reasoning": "...", "status": "insufficient"},
    ...
  },
  "metadata": {"is_quote": false, "is_ironic": false, "is_question": false, "contains_multiple_positions": false, "targets": [], "mentioned_ideologies": [], "overall_confidence": 0.0, "requires_context": false},
  "summary": "..."
}

Верни ТОЛЬКО JSON Lines (один объект на строку), без комментариев, без markdown-блоков."""


async def analyze_political_coords(
    db_path: Path,
    tg_id: int,
    api_key: str | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> AggregatedCoords:
    api_key = api_key or os.getenv("DEEPSEEK_API_KEY", "")
    if not api_key:
        raise PoliticalCoordsError("DEEPSEEK_API_KEY is not set")

    comments = fetch_user_comments(db_path, tg_id)
    messages = _filter_messages(comments)
    total_comments = len(comments)

    if not messages:
        return AggregatedCoords(total_messages=total_comments, signal_count=0)

    batches = _split_batches(messages, _BATCH_SIZE)

    close_client = http_client is None
    if http_client is None:
        http_client = httpx.AsyncClient(timeout=httpx.Timeout(120.0))

    try:
        semaphore = asyncio.Semaphore(_MAX_CONCURRENT_BATCHES)

        async def _limited_call(idx: int, batch: list[str]) -> list[dict[str, Any]]:
            async with semaphore:
                return await _call_deepseek(api_key, batch, http_client, idx, len(batches))

        tasks = [_limited_call(i, b) for i, b in enumerate(batches)]
        results_lists = await asyncio.gather(*tasks)

        all_analyses: list[dict[str, Any]] = []
        for results in results_lists:
            all_analyses.extend(results)

        return _aggregate(all_analyses, total_comments)
    finally:
        if close_client:
            await http_client.aclose()


class PoliticalCoordsError(RuntimeError):
    pass


def _filter_messages(comments: list[UserComment], min_length: int = _MIN_MESSAGE_LENGTH) -> list[str]:
    return [c.text for c in comments if len(c.text) >= min_length]


def _split_batches(messages: list[str], batch_size: int) -> list[list[str]]:
    return [messages[i : i + batch_size] for i in range(0, len(messages), batch_size)]


async def _call_deepseek(
    api_key: str,
    messages: list[str],
    client: httpx.AsyncClient,
    batch_idx: int,
    total_batches: int,
) -> list[dict[str, Any]]:
    user_content_parts: list[str] = []
    for i, msg in enumerate(messages):
        user_content_parts.append(f"--- MESSAGE {i + 1} ---")
        user_content_parts.append(msg)

    user_content = "\n\n".join(user_content_parts)
    user_content += f"\n\nПроанализируй все {len(messages)} высказываний выше. Верни {len(messages)} JSON Lines (по одному на строку)."

    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_content},
        ],
        "temperature": 0.0,
        "max_tokens": 8192,
    }

    _logger.info(
        "political_coords batch %d/%d: sending %d messages", batch_idx + 1, total_batches, len(messages)
    )

    resp = await client.post(
        "https://api.deepseek.com/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
    )

    if resp.status_code != 200:
        raise PoliticalCoordsError(
            f"DeepSeek API error {resp.status_code}: {resp.text[:500]}"
        )

    data = resp.json()
    content = data["choices"][0]["message"]["content"]
    return _parse_ndjson_lines(content)


def _parse_ndjson_lines(raw: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for line in raw.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        line = re.sub(r"^```json\s*", "", line)
        line = re.sub(r"\s*```$", "", line)
        if not line:
            continue
        try:
            obj = json.loads(line)
            results.append(obj)
        except json.JSONDecodeError:
            _logger.warning("political_coords: failed to parse line: %s", line[:120])
    return results


def _aggregate(analyses: list[dict[str, Any]], total_comments: int) -> AggregatedCoords:
    axes: dict[str, AxisStats] = {key: AxisStats() for key in AXIS_LABELS}
    signal_count = 0

    for analysis in analyses:
        analysis_axes = analysis.get("axes", {})
        msg_has_signal = False
        for axis_key in AXIS_LABELS:
            ax = analysis_axes.get(axis_key, {})
            status = ax.get("status", "insufficient")
            score = ax.get("score", 0.0)

            if status in ("insufficient", "not_applicable"):
                continue

            msg_has_signal = True
            if status == "mixed":
                axes[axis_key].left_count += 1
                axes[axis_key].right_count += 1
            elif score < -0.05:
                axes[axis_key].left_count += 1
            elif score > 0.05:
                axes[axis_key].right_count += 1

        if msg_has_signal:
            signal_count += 1

    return AggregatedCoords(
        total_messages=total_comments,
        signal_count=signal_count,
        axes=axes,
    )
