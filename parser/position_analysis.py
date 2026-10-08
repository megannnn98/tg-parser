"""Incremental political-position analysis across the collected authors."""
from __future__ import annotations

import asyncio
from collections import Counter, deque
from contextlib import nullcontext
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
import os
import logging
import time

from parser.position_comparison import (
    AnalysisProgress, Comparison, EmbeddingDetails, Evidence, PositionResults, QuestionMatch, summarize,
)
from parser.position_inference import (
    DeepSeekPositions, ExtractedComment, IncompleteResponseError, LocalE5, RULES_VERSION,
    ProviderChangedError,
    InvalidResponseError,
)
from db.analysis_store import digest


def date_key(date: str) -> datetime:
    value = datetime.fromisoformat(date.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class PositionAnalysis:
    def __init__(self, store, gateway=None, embedder=None, model_cache_dir: Path | None = None):
        self.store = store
        self.gateway = gateway or DeepSeekPositions()
        self.embedder = embedder or LocalE5(cache_dir=model_cache_dir)
        self.version = digest([
            getattr(self.gateway, "version", self.gateway.model), self.embedder.version, RULES_VERSION,
            os.getenv("POSITION_ANALYSIS_GENERATION", "1"),
        ])
        self._namespace = f"analysis:{self.version}"
        self._provider_identity = None
        self._identity_loaded = False
        if isinstance(self.gateway, DeepSeekPositions):
            self.gateway.identity_guard = self._check_provider_identity

    async def _check_provider_identity(self, identity):
        if not self._identity_loaded:
            self._provider_identity = await asyncio.to_thread(self.store.get, self._namespace, "provider-identity")
            self._identity_loaded = True
        previous = self._provider_identity
        if previous is not None and previous != identity:
            await self._store_write(self.store.put, self._namespace, "provider-changed", True)
            raise ProviderChangedError("Изменилась версия модели DeepSeek; требуется новая версия анализа (POSITION_ANALYSIS_GENERATION)")
        if previous is None:
            await self._store_write(self.store.put, self._namespace, "provider-identity", identity)
            self._provider_identity = identity

    def _load_sources(self):
        profiles, comments = self.store.load_sources()
        for comment in comments:
            date_key(comment["date"])  # Fail explicitly instead of picking a false latest position.
        return profiles, comments

    async def _store_write(self, method, *args):
        # A cancelled job must wait for its writer before releasing the analysis lock.
        task = asyncio.create_task(asyncio.to_thread(method, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _report(self, checkpoint, **changes):
        checkpoint["progress"].update(changes, updated_at=datetime.now(timezone.utc).isoformat())
        progress = checkpoint["progress"]
        now = time.monotonic()
        key = (id(checkpoint), progress["state"], progress["phase"], progress["activity"])
        # Cached comparisons can advance thousands of counters per second. Persist
        # at most twice a second, always flushing a new operation, phase or state
        # before a potentially long API request or model preparation.
        if key == getattr(self, "_report_key", None) and now - self._report_time < 0.5:
            return
        await self._store_write(self.store.put, "run", "latest", checkpoint)
        self._report_key, self._report_time = key, now
        logging.getLogger(__name__).info(
            "Analysis phase=%s comments=%s/%s embeddings=%s/%s questions=%s/%s relations=%s/%s pairs=%s/%s activity=%s",
            progress["phase"], progress["processed_comments"], progress["total_comments"],
            progress["processed_embeddings"], progress["total_embeddings"],
            progress["processed_questions"], progress["total_questions"],
            progress["processed_relations"], progress["total_relations"],
            progress["processed_pairs"], progress["total_pairs"], progress["activity"],
        )

    @staticmethod
    def _rejection_reason(exc):
        if isinstance(exc, IncompleteResponseError):
            return "output_truncated"
        if isinstance(exc, InvalidResponseError):
            return exc.reason
        # Only controlled codes are exposed; Pydantic exceptions can contain source text.
        known = {"comment_too_long", "missing_comments", "unexpected_rejection_reason",
                 "ambiguous_with_positions", "clear_without_positions", "quote_mismatch"}
        return str(exc) if str(exc) in known else "invalid_contract"

    async def _cached(self, kind, value, call, *, checkpoint=None, activity=""):
        key = digest(value)
        namespace = self._namespace + ":" + kind
        cached = await asyncio.to_thread(self.store.get, namespace, key)
        if cached is not None:
            return cached
        if checkpoint is not None:
            await self._report(checkpoint, activity=activity)
        result = await call()
        if hasattr(result, "model_dump"):
            result = result.model_dump()
        await self._store_write(self.store.put, namespace, key, result)
        return result

    def _prepare_extraction(self, comments):
        extracted = {}
        pending = {}
        frequencies = Counter()
        texts = {}
        for comment in comments:
            key = digest(comment["text"])
            texts[key] = comment["text"]
            frequencies[key] += 1
        cached_comments = self.store.get_all(self._namespace + ":comment")
        for key, text in texts.items():
            cached = cached_comments.get(key)
            if cached is None or cached.get("rejection_reason") == "invalid_response":
                # Legacy entries conflated transient failures and contract violations.
                pending[key] = text
            else:
                extracted[key] = ExtractedComment.model_validate(cached)
        return extracted, pending, frequencies

    async def _extract(self, comments, checkpoint):
        extracted, pending, frequencies = await asyncio.to_thread(self._prepare_extraction, comments)
        processed = sum(frequencies[key] for key in extracted)
        rejected = sum(frequencies[key] for key, value in extracted.items() if value.rejection_reason)
        reasons = Counter()
        for key, value in extracted.items():
            if value.rejection_reason:
                reasons[value.rejection_reason] += frequencies[key]
        await self._report(checkpoint, processed_comments=processed, cached_comments=processed,
                           rejected_comments=rejected, rejection_reasons=dict(reasons),
                           activity="Загружены ранее обработанные комментарии")
        # Queue whole batches: splitting one response does not shrink all later batches.
        batches = deque()
        batch, size = [], 0
        for key, text in pending.items():
            if batch and (len(batch) == 8 or size + len(text) > 16000):
                batches.append(batch)
                batch, size = [], 0
            batch.append((key, text))
            size += len(text)
        if batch:
            batches.append(batch)
        while batches:
            batch = batches.popleft()
            try:
                if any(len(text) > 60000 for _, text in batch):
                    raise ValueError("comment_too_long")
                await self._report(
                    checkpoint, extraction_requests=checkpoint["progress"]["extraction_requests"] + 1,
                    activity=f"OpenRouter: извлечение политических позиций из пакета {len(batch)} комментариев",
                )
                values = await self.gateway.extract([text for _, text in batch])
                if len(values) != len(batch):
                    raise ValueError("missing_comments")
                values = [ExtractedComment.model_validate(v) for v in values]
                for (_, text), value in zip(batch, values):
                    if value.rejection_reason:
                        raise ValueError("unexpected_rejection_reason")
                    if value.status != "clear" and value.positions:
                        raise ValueError("ambiguous_with_positions")
                    if value.status == "clear" and not value.positions:
                        raise ValueError("clear_without_positions")
                    if any(p.quote not in text for p in value.positions):
                        raise ValueError("quote_mismatch")
            except (IncompleteResponseError, ValueError) as exc:
                if len(batch) > 1:
                    logging.getLogger(__name__).warning(
                        "Splitting extraction batch size=%d reason=%s", len(batch), self._rejection_reason(exc)
                    )
                    await self._report(
                        checkpoint, split_batches=checkpoint["progress"]["split_batches"] + 1,
                        activity="Проверка ответа не пройдена; пакет делится для изоляции ошибки",
                    )
                    middle = len(batch) // 2
                    batches.appendleft(batch[middle:])
                    batches.appendleft(batch[:middle])
                    continue
                reason = self._rejection_reason(exc)
                if len(batch[0][1]) > 60000:
                    reason = "comment_too_long"
                values = [ExtractedComment(status="ambiguous", positions=[], rejection_reason=reason)]
                logging.getLogger(__name__).warning("Rejected comment digest=%s reason=%s", batch[0][0], reason)
            await self._store_write(self.store.put_many, self._namespace + ":comment", {
                key: value.model_dump() for (key, _), value in zip(batch, values)
            })
            for (key, _), value in zip(batch, values):
                extracted[key] = value
                processed += frequencies[key]
                if value.rejection_reason:
                    rejected += frequencies[key]
                    reasons[value.rejection_reason] += frequencies[key]
            await self._report(checkpoint, processed_comments=processed, rejected_comments=rejected,
                               rejection_reasons=dict(reasons), activity="Ответ проверен и сохранён")
            await asyncio.sleep(0)
        return extracted

    async def _questions(self, extracted, checkpoint):
        import numpy as np
        current_texts = {
            p.question for value in extracted.values() if value.status == "clear"
            for p in value.positions if p.confidence >= 0.8
        }
        if not current_texts:
            await self._report(checkpoint, phase="embeddings", activity="Ясных политических вопросов нет; векторы не требуются")
            await self._report(checkpoint, phase="questions", activity="Нет вопросов для сопоставления")
            return {}
        groups = {row["text"]: row["group"] for row in (
            await asyncio.to_thread(self.store.get_all, self._namespace + ":group")
        ).values()}
        representatives = sorted(set(groups.values()))
        texts = sorted(current_texts | set(representatives))
        await self._report(checkpoint, phase="embeddings", total_embeddings=len(texts),
                           activity="Проверка сохранённых эмбеддингов политических вопросов")
        vectors = {}
        missing = []
        cached_vectors = await asyncio.to_thread(
            self.store.get_many, "embeddings:" + self.embedder.version, [digest(text) for text in texts]
        )
        for text in texts:
            vector = cached_vectors.get(digest(text))
            if vector is None:
                missing.append(text)
            else:
                vectors[text] = vector
        await self._report(checkpoint, processed_embeddings=len(vectors), cached_embeddings=len(vectors),
                           activity="Загружены сохранённые векторы; новые будут вычислены локально")
        for start in range(0, len(missing), 16):
            batch = missing[start:start + 16]
            preparing = isinstance(self.embedder, LocalE5) and self.embedder._model is None
            await self._report(checkpoint, activity=(
                "Подготовка E5: при первом запуске скачиваются веса модели" if preparing else
                f"E5: вычисление эмбеддингов для {len(batch)} политических вопросов"
            ))
            output = await self.embedder.encode(batch)
            if len(output) != len(batch):
                raise ValueError("Модель пропустила эмбеддинги")
            values = np.asarray(output, dtype=float)
            if (
                values.ndim != 2 or values.shape[1] == 0
                or not np.isfinite(values).all()
                or (np.linalg.norm(values, axis=1) == 0).any()
            ):
                raise ValueError("Модель вернула некорректные эмбеддинги")
            await self._store_write(self.store.put_many, "embeddings:" + self.embedder.version, {
                digest(text): vector for text, vector in zip(batch, output)
            })
            for text, vector in zip(batch, output):
                vectors[text] = vector
            await self._report(checkpoint, processed_embeddings=len(vectors),
                               activity="Новые эмбеддинги сохранены в PostgreSQL")
            await asyncio.sleep(0)

        await self._report(checkpoint, phase="questions", total_questions=len(current_texts),
                           processed_questions=len(current_texts & set(groups)),
                           activity="Поиск похожих формулировок по эмбеддингам")
        indices = {text: i for i, text in enumerate(texts)}
        representative_indices = [indices[text] for text in representatives]
        matrix = np.asarray([vectors[text] for text in texts], dtype=float)
        if matrix.ndim != 2 or not np.isfinite(matrix).all():
            raise ValueError("Некорректные эмбеддинги")
        for index, text in enumerate(texts):
            if text in groups or text in representatives:
                continue
            # No semantic cutoff is used as proof; only DeepSeek can approve a merge.
            scores = matrix[representative_indices] @ matrix[index]
            candidates = [representatives[i] for i in np.argsort(-scores, kind="stable")[:8]]
            group = text
            for other in candidates:
                pair = sorted([text, other])
                equivalent = await self._cached(
                    "question-pair", pair,
                    lambda pair=pair: self.gateway.equivalent(*pair),
                    checkpoint=checkpoint, activity="OpenRouter: проверка совпадения политических вопросов",
                )
                if equivalent:
                    group = other
                    break
            if group == text:
                representatives.append(text)
                representative_indices.append(index)
            groups[text] = group
            await self._store_write(self.store.put, self._namespace + ":group", digest(text), {"text": text, "group": group})
            await self._report(checkpoint, processed_questions=checkpoint["progress"]["processed_questions"] + 1,
                               activity="Сопоставление вопроса сохранено")
            await asyncio.sleep(0)
        return groups

    def _latest_positions(self, comments, extracted, groups):
        positions = {}
        ambiguous = set()
        for comment in comments:
            value = extracted[digest(comment["text"])]
            if value.status != "clear":
                continue
            for position in value.positions:
                if position.confidence < 0.8:
                    continue
                question = groups[position.question]
                key = (comment["tg_id"], question)
                evidence = Evidence(
                    **{k: comment[k] for k in ("text", "date", "channel", "message_id")},
                    position=position.position, quote=position.quote, question=position.question,
                )
                previous = positions.get(key)
                if previous is None or date_key(evidence.date) > date_key(previous.date):
                    positions[key] = evidence
                    ambiguous.discard(key)
                elif date_key(evidence.date) == date_key(previous.date) and evidence.position != previous.position:
                    # No defensible chronology between distinct positions at the same instant.
                    ambiguous.add(key)
        return {key: value for key, value in positions.items() if key not in ambiguous}

    def ensure_provider_unchanged(self):
        if self.store.get(self._namespace, "provider-changed"):
            raise ProviderChangedError("Изменилась версия модели DeepSeek; требуется новая версия анализа (POSITION_ANALYSIS_GENERATION)")

    def initial_checkpoint(self):
        return {
            "version": self.version, "manifest": self.store.manifest(),
            "pairs": {}, "progress": AnalysisProgress(
                state="running", phase="comments", activity="Чтение истории всех собранных авторов",
                updated_at=datetime.now(timezone.utc).isoformat(),
            ).model_dump(),
        }

    async def run(self, *, lock_held=False, checkpoint=None):
        with nullcontext() if lock_held else self.store.analysis_lock():
            self.ensure_provider_unchanged()
            # Another worker may have populated the identity since this instance ran.
            self._identity_loaded = False
            async with self.gateway.session() if isinstance(self.gateway, DeepSeekPositions) else nullcontext():
                checkpoint = checkpoint if checkpoint is not None else self.initial_checkpoint()
                await self._store_write(self.store.put, "run", "latest", checkpoint)
                try:
                    profiles, comments = await asyncio.to_thread(self._load_sources)
                    await self._report(checkpoint, total_comments=len(comments),
                                       activity="Проверка кеша комментариев; затем извлечение позиций")
                    extracted = await self._extract(comments, checkpoint)
                    groups = await self._questions(extracted, checkpoint)
                    positions = await asyncio.to_thread(self._latest_positions, comments, extracted, groups)
                    users = sorted(profiles)
                    questions_by_user = {user: set() for user in users}
                    for user, question in positions:
                        questions_by_user[user].add(question)
                    checkpoint["progress"].update(
                        phase="comparisons", total_pairs=len(users) * (len(users) - 1) // 2,
                        total_relations=sum(len(questions_by_user[a] & questions_by_user[b])
                                            for a, b in combinations(users, 2)),
                    )
                    await self._report(checkpoint, activity="Выбор последних ясных позиций и сравнение авторов")
                    for left, right in combinations(users, 2):
                        questions = []
                        common = sorted(
                            questions_by_user[left] & questions_by_user[right]
                        )
                        for question in common:
                            a, b = positions[left, question], positions[right, question]
                            relation = await self._cached(
                                "position-pair", [question, a.model_dump(), b.model_dump()],
                                lambda question=question, a=a, b=b: self.gateway.compare(question, a, b),
                                checkpoint=checkpoint,
                                activity="OpenRouter: сравнение позиций двух авторов по общему вопросу",
                            )
                            questions.append(QuestionMatch(
                                question=question, left=a, right=b, **relation
                            ).model_dump())
                            await self._report(
                                checkpoint, processed_relations=checkpoint["progress"]["processed_relations"] + 1,
                                activity="Отношение позиций определено и сохранено",
                            )
                        checkpoint["pairs"][f"{left}:{right}"] = {
                            "left": left, "right": right, "questions": questions,
                            "profiles": {
                                str(user): {
                                    "tg_id": user,
                                    "display_username": profiles[user].display_username,
                                } for user in (left, right)
                            },
                        }
                        checkpoint["progress"]["processed_pairs"] += 1
                        await self._report(checkpoint, activity="Сравнение пары авторов сохранено")
                        await asyncio.sleep(0)
                    checkpoint["progress"].update(state="done", phase="done")
                    await self._report(checkpoint, activity="Анализ завершён; результаты опубликованы")
                    await self._store_write(self.store.put, "published", "latest", checkpoint)
                except BaseException as exc:
                    checkpoint["progress"].update(
                        state=("interrupted" if isinstance(exc, asyncio.CancelledError) else
                               "provider_changed" if isinstance(exc, ProviderChangedError) else "error"),
                        error="Анализ прерван; можно продолжить" if isinstance(exc, asyncio.CancelledError) else (str(exc) or type(exc).__name__),
                    )
                    await self._report(checkpoint, activity="Обработка остановлена; сохранённый прогресс можно продолжить")
                    raise

    def results(self, tg_id: int) -> PositionResults:
        run = self.store.get("run", "latest")
        published = self.store.get("published", "latest")
        current = run
        if run is None or run["version"] != self.version:
            current = published or run
        elif published and run["progress"]["state"] != "done":
            # Keep the last complete result visible during any replacement run.
            current = published
        progress = AnalysisProgress.model_validate(run["progress"]) if run else AnalysisProgress()
        if run and run["version"] != self.version and progress.state == "provider_changed":
            # A newly configured generation is allowed to start a fresh analysis.
            progress.state = "idle"
            progress.error = None
        if progress.state == "running":
            # A persisted job can outlive its process. Probe the cross-process lock.
            try:
                with self.store.analysis_lock():
                    progress.state = "interrupted"
                    progress.error = "Анализ прерван; можно продолжить"
            except RuntimeError:
                pass
        response = PositionResults(
            version=current["version"] if current else self.version,
            progress=progress,
            embeddings=EmbeddingDetails(
                model=getattr(self.embedder, "model_name", self.embedder.version),
                version=self.embedder.version, dimensions=getattr(self.embedder, "dimensions", 0),
                storage=self.store.location, saved_vectors=self.store.count("embeddings:" + self.embedder.version),
            ),
            incomplete=bool(current and current["progress"]["state"] != "done"),
            needs_update=bool(current and (
                current["version"] != self.version
                or current["manifest"] != self.store.manifest()
            )),
        )
        if not current:
            return response
        comparisons = []
        for pair in current["pairs"].values():
            if tg_id not in (pair["left"], pair["right"]):
                continue
            other = pair["right"] if pair["left"] == tg_id else pair["left"]
            questions = [QuestionMatch.model_validate(q) for q in pair["questions"]]
            if pair["right"] == tg_id:
                questions = [q.model_copy(update={"left": q.right, "right": q.left}) for q in questions]
            comparisons.append(summarize(Comparison(
                **pair["profiles"][str(other)], questions=questions
            )))
        comparisons.sort(key=lambda c: (
            -(c.score if c.score is not None else -1), -c.comparable_questions, c.tg_id
        ))
        if response.incomplete:
            for comparison in comparisons:
                comparison.eligible = False
            response.partial_results = comparisons
        else:
            response.ranking = [c for c in comparisons if c.eligible][:10]
            response.insufficient = [c for c in comparisons if not c.eligible]
        return response
