from __future__ import annotations

import asyncio
import json
import os
import random
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from parser.logger import get_logger
from parser.political_coords import AXIS_LABELS, SYSTEM_PROMPT as SCORING_SYSTEM_PROMPT

_logger = get_logger("generate_corpus")

OUTPUT_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "synthetic_corpus.jsonl"
API_URL = "https://api.deepseek.com/v1/chat/completions"
SAMPLES_PER_CALL = 10
MAX_CONCURRENT = 4

COMMENT_TOPICS: list[str] = [
    "экономический кризис и цены на продукты",
    "пенсионная реформа и социальная поддержка",
    "миграционная политика и беженцы",
    "свобода слова и блокировки в интернете",
    "военный конфликт и мобилизация",
    "выборы и политическая система",
    "ЛГБТ и традиционные ценности",
    "образование и здравоохранение",
    "коррупция и борьба с ней",
    "международные отношения и санкции",
    "налоги и бизнес",
    "национализация или приватизация",
    "права человека и репрессии",
    "армия, вооружение, безопасность",
    "религия и государство",
    "глобализация и суверенитет",
    "протесты и методы борьбы",
    "историческая память и пропаганда",
    "экология и промышленность",
    "молодёжь и будущее страны",
]


@dataclass
class CorpusSample:
    text: str
    economic: float
    authority: float
    social: float
    nationalism: float
    democracy: float
    militarism: float


def _axis_score_to_label(axis_key: str, score: float) -> str:
    if axis_key == "economic":
        return f"Левая экономика (score={score})" if score < 0 else f"Правая экономика (score={score})"
    if axis_key == "authority":
        return f"Антиавторитарный (score={score})" if score < 0 else f"Авторитарный (score={score})"
    if axis_key == "social":
        return f"Прогрессивный (score={score})" if score < 0 else f"Консервативный (score={score})"
    if axis_key == "nationalism":
        return f"Космополит (score={score})" if score < 0 else f"Националист (score={score})"
    if axis_key == "democracy":
        return f"Демократ (score={score})" if score < 0 else f"Автократ (score={score})"
    if axis_key == "militarism":
        return f"Пацифист (score={score})" if score < 0 else f"Милитарист (score={score})"
    return f"score={score}"


PERSONA_SYSTEM_PROMPT = """Ты — генератор синтетических данных для обучения ML-модели. Твоя задача — создавать короткие комментарии от лица пользователей Telegram в политических чатах.

ПРАВИЛА:
1. Пиши ТОЛЬКО на русском языке.
2. Каждый комментарий — 40-300 символов.
3. Используй разговорный стиль: неформальная лексика, возможны сокращения, эмодзи (иногда), lowercase.
4. Комментарий должен звучать как реплика в споре/обсуждении, а не как эссе.
5. Отражай ТОЛЬКО те взгляды, которые указаны в параметрах пользователя. Не добавляй противоречащих сигналов по другим осям.
6. Если по какой-то оси стоит 0.0 — пользователь НЕЙТРАЛЕН/НЕ ИМЕЕТ ПОЗИЦИИ по этой оси. Не генерируй сигналов по этой оси.
7. Тема комментария указана отдельно — придерживайся темы.
8. ВАЖНО: комментарий должен точно соответствовать указанным политическим координатам. Если economic=-0.7 — пользователь жёстко за левую экономику. Если nationalism=0.8 — он радикальный националист. НЕЛЬЗЯ генерировать текст с противоположными взглядами.

ФОРМАТ ВЫВОДА: JSON Lines (один объект на строку), без markdown, без пояснений.
{"text": "текст комментария"}
"""

PERSONA_USER_TEMPLATE = """ПАРАМЕТРЫ ПОЛЬЗОВАТЕЛЯ:
{persona_description}

ТЕМА: {topic}

Сгенерируй ровно {n} разных комментариев от лица этого пользователя на указанную тему. Каждый комментарий должен отражать заданные взгляды.

Верни {n} JSON-объектов (по одному на строку)."""


def _describe_persona(scores: dict[str, float]) -> str:
    lines: list[str] = []
    for axis_key in ("economic", "authority", "social", "nationalism", "democracy", "militarism"):
        score = scores.get(axis_key, 0.0)
        lines.append(f"- {AXIS_LABELS[axis_key]}: {_axis_score_to_label(axis_key, score)}")

    return "\n".join(lines)


def _build_persona_user_prompt(scores: dict[str, float], topic: str, n: int = SAMPLES_PER_CALL) -> str:
    persona_desc = _describe_persona(scores)
    return PERSONA_USER_TEMPLATE.format(persona_description=persona_desc, topic=topic, n=n)


TWO_PASS_TOPICS: list[str] = [
    "ЛГБТ, трансгендеры и традиционные ценности",
    "мигранты, национальная идентичность и границы",
    "война, армия и мобилизация",
    "свобода слова, цензура и репрессии",
    "выборы, оппозиция и авторитаризм",
    "национализация, приватизация и олигархи",
    "пенсии, налоги и социальная справедливость",
    "религия, церковь и государство",
]


def _build_two_pass_user_prompt(topic: str, n: int) -> str:
    return (
        f"Сгенерируй {n} разных комментариев на русском языке от разных пользователей "
        f"политического Telegram-чата на тему «{topic}».\n"
        f"Стиль: разговорный, 40-300 символов, неформальный, возможны эмоции и эмодзи.\n"
        f"ВАЖНО: у каждого пользователя должны быть ЯРКО ВЫРАЖЕННЫЕ полярные взгляды — "
        f"одни радикально ЗА, другие радикально ПРОТИВ. "
        f"Избегай нейтральных и умеренных комментариев. "
        f"Половина — за, половина — против, с разной аргументацией.\n"
        f"Верни {n} JSON-объектов (по одному на строку) с полем text."
    )


def _build_scoring_user_prompt(texts: list[str]) -> str:
    parts: list[str] = []
    for i, text in enumerate(texts):
        parts.append(f"--- MESSAGE {i + 1} ---")
        parts.append(text)
    user_content = "\n\n".join(parts)
    user_content += f"\n\nПроанализируй все {len(texts)} высказываний выше. Верни {len(texts)} JSON Lines (по одному на строку)."
    return user_content


async def _call_deepseek(
    client: httpx.AsyncClient,
    api_key: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.7,
) -> str:
    resp = await client.post(
        API_URL,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": 8192,
        },
    )

    if resp.status_code != 200:
        raise RuntimeError(f"DeepSeek API error {resp.status_code}: {resp.text[:500]}")

    return resp.json()["choices"][0]["message"]["content"]


def _parse_jsonl(raw: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    cleaned = raw.strip()
    if not cleaned:
        return results
    cleaned = cleaned.replace("```json", "").replace("```", "")
    try:
        parsed = json.loads(cleaned)
        if isinstance(parsed, list):
            return [it for it in parsed if isinstance(it, dict)]
        if isinstance(parsed, dict):
            return [parsed]
    except json.JSONDecodeError:
        pass

    for line in cleaned.split("\n"):
        line = line.strip()
        if not line:
            continue
        if line in ("[", "]"):
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                results.append(obj)
        except json.JSONDecodeError:
            _logger.warning("failed to parse JSON line: %s", line[:120])
    return results


async def _generate_persona_batch(
    client: httpx.AsyncClient,
    api_key: str,
    scores: dict[str, float],
    topic: str,
    semaphore: asyncio.Semaphore,
) -> list[CorpusSample]:
    async with semaphore:
        user_prompt = _build_persona_user_prompt(scores, topic)
        raw = await _call_deepseek(client, api_key, PERSONA_SYSTEM_PROMPT, user_prompt)
        items = _parse_jsonl(raw)
        return [CorpusSample(text=it["text"], **{k: scores[k] for k in scores}) for it in items if it.get("text")]


async def _generate_two_pass_batch(
    client: httpx.AsyncClient,
    api_key: str,
    topic: str,
    n: int,
    semaphore: asyncio.Semaphore,
) -> list[CorpusSample]:
    async with semaphore:
        user_prompt = _build_two_pass_user_prompt(topic, n)
        raw = await _call_deepseek(client, api_key, PERSONA_SYSTEM_PROMPT, user_prompt, temperature=1.0)
        items = _parse_jsonl(raw)
        texts = [it["text"] for it in items if it.get("text")]
        if not texts:
            return []

    scoring_prompt = _build_scoring_user_prompt(texts)
    raw = await _call_deepseek(client, api_key, SCORING_SYSTEM_PROMPT, scoring_prompt, temperature=0.0)
    analyses = _parse_jsonl(raw)

    results: list[CorpusSample] = []
    for text, analysis in zip(texts, analyses):
        axes = analysis.get("axes", {})
        scores: dict[str, float] = {}
        for axis_key in ("economic", "authority", "social", "nationalism", "democracy", "militarism"):
            ax = axes.get(axis_key, {})
            status = ax.get("status", "insufficient")
            score = ax.get("score", 0.0)
            scores[axis_key] = score if status not in ("insufficient", "not_applicable") else 0.0
        results.append(CorpusSample(text=text, **scores))
    return results


def _build_combo_scores(
    primary_axis: str | None = None,
    primary_score: float = 0.0,
    secondary_axes: dict[str, float] | None = None,
) -> dict[str, float]:
    scores: dict[str, float] = {
        "economic": 0.0,
        "authority": 0.0,
        "social": 0.0,
        "nationalism": 0.0,
        "democracy": 0.0,
        "militarism": 0.0,
    }
    if primary_axis:
        scores[primary_axis] = primary_score
    if secondary_axes:
        scores.update(secondary_axes)
    return scores


def _build_single_axis_combos() -> list[dict[str, float]]:
    combos: list[dict[str, float]] = []
    intensities = [-0.2, -0.4, -0.6, -0.8, 0.2, 0.4, 0.6, 0.8]
    for axis in AXIS_LABELS:
        for intensity in intensities:
            combos.append(_build_combo_scores(primary_axis=axis, primary_score=intensity))
    return combos


def _build_multi_axis_combos() -> list[dict[str, float]]:
    combos = [
        _build_combo_scores("economic", -0.7, {"social": -0.6}),
        _build_combo_scores("economic", -0.6, {"militarism": -0.7}),
        _build_combo_scores("economic", -0.5, {"authority": -0.5}),
        _build_combo_scores("economic", -0.6, {"democracy": -0.6}),
        _build_combo_scores("economic", 0.7, {"nationalism": 0.6}),
        _build_combo_scores("economic", 0.6, {"authority": 0.6}),
        _build_combo_scores("economic", 0.7, {"nationalism": 0.5, "authority": 0.5}),
        _build_combo_scores("economic", -0.6, {"social": -0.5, "nationalism": -0.5}),
        _build_combo_scores("nationalism", 0.7, {"authority": 0.6, "militarism": 0.5}),
        _build_combo_scores("nationalism", -0.6, {"social": -0.5, "democracy": -0.5}),
        _build_combo_scores("authority", -0.7, {"democracy": -0.7, "social": -0.5}),
        _build_combo_scores("authority", 0.6, {"democracy": 0.5, "militarism": 0.4}),
        _build_combo_scores("social", 0.7, {"authority": 0.5}),
        _build_combo_scores("social", -0.6, {"economic": -0.5, "democracy": -0.5}),
        _build_combo_scores("democracy", -0.7, {"authority": -0.6}),
        _build_combo_scores("democracy", 0.6, {"authority": 0.5, "nationalism": 0.5}),
        _build_combo_scores("militarism", -0.7, {"economic": -0.5, "democracy": -0.5}),
        _build_combo_scores("militarism", 0.6, {"nationalism": 0.6, "authority": 0.5}),
        _build_combo_scores("economic", 0.5, {"social": 0.6, "nationalism": 0.5}),
        _build_combo_scores("economic", -0.5, {"authority": -0.4, "militarism": -0.5}),
        _build_combo_scores("economic", 0.4, {"democracy": -0.5}),
        _build_combo_scores("authority", -0.5, {"social": -0.6}),
        _build_combo_scores("nationalism", -0.5, {"militarism": -0.6}),
        _build_combo_scores("social", -0.5, {"economic": -0.4}),
        _build_combo_scores("nationalism", 0.5, {"social": 0.6}),
        _build_combo_scores("militarism", 0.5, {"democracy": 0.4}),
        _build_combo_scores("social", -0.7, {"authority": -0.5, "economic": -0.4}),
        _build_combo_scores("economic", 0.6, {"militarism": 0.5}),
        _build_combo_scores("authority", 0.5, {"social": 0.5}),
        _build_combo_scores("democracy", -0.6, {"social": -0.5, "authority": -0.5}),
    ]
    mirrored: list[dict[str, float]] = []
    for scores in combos:
        mirror = {k: -v for k, v in scores.items()}
        if mirror not in combos and mirror not in mirrored:
            mirrored.append(mirror)
    return combos + mirrored


def _build_apolitical_combos() -> list[dict[str, float]]:
    return [_build_combo_scores() for _ in range(8)]


async def _append_to_file(samples: list[CorpusSample], path: Path):
    with open(path, "a") as f:
        for s in samples:
            f.write(
                json.dumps(
                    {
                        "text": s.text,
                        "economic": s.economic,
                        "authority": s.authority,
                        "social": s.social,
                        "nationalism": s.nationalism,
                        "democracy": s.democracy,
                        "militarism": s.militarism,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


async def generate_corpus(
    api_key: str,
    output_path: Path | None = None,
    persona_ratio: float = 0.7,
    max_samples: int = 5000,
    resume: bool = False,
) -> Path:
    output_path = output_path or OUTPUT_PATH
    if output_path.exists():
        if resume:
            with open(output_path) as f:
                existing_count = sum(1 for _ in f)
            _logger.info("resuming: %d existing samples in %s", existing_count, output_path)
            if existing_count >= max_samples:
                _logger.info("already have %d >= %d samples, nothing to do", existing_count, max_samples)
                return output_path
            max_samples = max(max_samples, existing_count + 1)
        else:
            output_path.unlink()
    elif resume:
        existing_count = 0

    single_combos = _build_single_axis_combos()
    multi_combos = _build_multi_axis_combos()
    apolitical_combos = _build_apolitical_combos()
    all_combos = single_combos + multi_combos + apolitical_combos
    random.shuffle(all_combos)

    total_generated = existing_count if resume and output_path.exists() else 0
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0)) as client:
        combo_idx = 0
        while total_generated < max_samples:
            combo_idx = combo_idx % len(all_combos)
            if combo_idx == 0:
                random.shuffle(all_combos)
            scores = all_combos[combo_idx]
            topic = random.choice(COMMENT_TOPICS)

            is_apolitical = all(abs(v) < 0.05 for v in scores.values())
            use_persona = random.random() < persona_ratio if not is_apolitical else True

            try:
                if use_persona:
                    samples = await _generate_persona_batch(client, api_key, scores, topic, semaphore)
                else:
                    n_two_pass = min(SAMPLES_PER_CALL, max_samples - total_generated)
                    two_pass_topic = random.choice(TWO_PASS_TOPICS)
                    samples = await _generate_two_pass_batch(client, api_key, two_pass_topic, n_two_pass, semaphore)
            except Exception as e:
                _logger.warning("batch failed (combo %d, topic=%s): %s", combo_idx, topic, e)
                combo_idx += 1
                continue

            if samples:
                await _append_to_file(samples, output_path)
                total_generated += len(samples)
                _logger.info(
                    "[%d/%d] generated %d samples (combo %d, %s, topic=%s, persona=%s)",
                    total_generated,
                    max_samples,
                    len(samples),
                    combo_idx,
                    "apolitical" if is_apolitical else "political",
                    topic,
                    use_persona,
                )

            combo_idx += 1

    _logger.info("corpus generation complete: %d samples written to %s", total_generated, output_path)
    return output_path


async def main():
    api_key = os.getenv("DEEPSEEK_API_KEY", "")
    if not api_key:
        _logger.error("DEEPSEEK_API_KEY is not set")
        return

    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic political corpus")
    parser.add_argument("--resume", action="store_true", help="Append to existing file instead of overwriting")
    parser.add_argument("--max-samples", type=int, default=1000, help="Maximum samples to generate")
    parser.add_argument("--persona-ratio", type=float, default=0.85, help="Fraction of persona-based vs two-pass")
    parser.add_argument("--output", type=str, default=str(OUTPUT_PATH), help="Output file path")
    args = parser.parse_args()

    _logger.info(
        "starting corpus generation: max=%d, persona_ratio=%.1f, output=%s",
        args.max_samples,
        args.persona_ratio,
        args.output,
    )

    await generate_corpus(
        api_key=api_key,
        output_path=Path(args.output),
        persona_ratio=args.persona_ratio,
        max_samples=args.max_samples,
        resume=args.resume,
    )


if __name__ == "__main__":
    asyncio.run(main())
