"""Cached local comment embeddings and author similarity without external inference."""
from __future__ import annotations

import asyncio
from collections import Counter
from contextlib import nullcontext
from itertools import combinations

from parser.comment_embeddings import LocalCommentE5
from parser.position_analysis import PositionAnalysis
from parser.position_comparison import AnalysisProgress, EmbeddingDetails, Evidence, PositionResults, SimilarAuthor
from db.analysis_store import digest


class LocalTextAnalysis(PositionAnalysis):
    # Reuse source loading, cancellation-safe persistence and progress reporting.
    # This constructor deliberately does not create a gateway or provider guard.
    def __init__(self, store, embedder=None, model_cache_dir=None):
        self.store = store
        self.embedder = embedder or LocalCommentE5(cache_dir=model_cache_dir)
        self.version = digest(["local-comment-similarity:centroid:32-examples:v1", self.embedder.version])
        self._vectors = "comment-embeddings:" + self.embedder.version

    def ensure_provider_unchanged(self):
        pass

    def initial_checkpoint(self):
        return {
            "method": "text_similarity", "version": self.version,
            "manifest": self.store.manifest(), "pairs": {},
            "progress": AnalysisProgress(state="running", phase="comments",
                                         activity="Чтение комментариев из базы").model_dump(),
        }

    @staticmethod
    def _validated_vectors(values, count, dimension=None):
        import numpy as np
        matrix = np.asarray(values, dtype=np.float32)
        if (matrix.ndim != 2 or matrix.shape[0] != count or matrix.shape[1] == 0
                or (dimension is not None and matrix.shape[1] != dimension)
                or not np.isfinite(matrix).all()):
            raise ValueError("Локальная модель вернула некорректные эмбеддинги")
        norms = np.linalg.norm(matrix, axis=1, keepdims=True)
        if not np.isfinite(norms).all() or (norms <= 0).any():
            raise ValueError("Локальная модель вернула нулевой эмбеддинг")
        return matrix / norms

    async def _embed_comments(self, comments, checkpoint):
        import numpy as np
        texts = {digest(c["text"]): c["text"] for c in comments}
        keys = list(texts)
        indices = {key: i for i, key in enumerate(keys)}
        frequencies = Counter(digest(c["text"]) for c in comments)
        matrix = None
        missing = []
        processed = cached_count = processed_comments = 0
        await self._report(checkpoint, phase="embeddings", total_embeddings=len(keys),
                           activity="Проверка кеша эмбеддингов самих комментариев")
        # Decode only a small cache page at a time; retain compact float32 vectors.
        for start in range(0, len(keys), 256):
            batch = keys[start:start + 256]
            cache = await asyncio.to_thread(self.store.get_many, self._vectors, batch)
            for key in batch:
                if key not in cache:
                    missing.append(key)
                    continue
                vector = self._validated_vectors([cache[key]], 1,
                                                 getattr(self.embedder, "dimensions", None) if matrix is None else matrix.shape[1])[0]
                if matrix is None:
                    matrix = np.empty((len(keys), len(vector)), dtype=np.float32)
                matrix[indices[key]] = vector
                processed += 1
                cached_count += frequencies[key]
                processed_comments += frequencies[key]
            await self._report(checkpoint, processed_embeddings=processed, cached_embeddings=processed,
                               processed_comments=processed_comments, cached_comments=cached_count,
                               activity="Загрузка сохранённых эмбеддингов комментариев")
        for start in range(0, len(missing), 16):
            batch = missing[start:start + 16]
            preparing = isinstance(self.embedder, LocalCommentE5) and self.embedder._model is None
            await self._report(checkpoint, activity=(
                "Подготовка локальной E5: при первом запуске скачиваются веса модели" if preparing else
                f"Локальная E5: создание эмбеддингов {len(batch)} комментариев"
            ))
            values = self._validated_vectors(
                await self.embedder.encode([texts[key] for key in batch]), len(batch),
                getattr(self.embedder, "dimensions", None) if matrix is None else matrix.shape[1],
            )
            await self._store_write(self.store.put_many, self._vectors,
                                    {key: vector.tolist() for key, vector in zip(batch, values)})
            if matrix is None:
                matrix = np.empty((len(keys), values.shape[1]), dtype=np.float32)
            for key, vector in zip(batch, values):
                matrix[indices[key]] = vector
                processed_comments += frequencies[key]
            processed += len(batch)
            await self._report(checkpoint, processed_embeddings=processed, processed_comments=processed_comments,
                               activity="Эмбеддинги комментариев сохранены в PostgreSQL")
        return indices, matrix

    @staticmethod
    def _author_profiles(comments, indices, matrix):
        import numpy as np
        by_user = {}
        for comment in comments:
            # Identical repeated comments have equal weight to one unique text.
            by_user.setdefault(comment["tg_id"], {})[indices[digest(comment["text"])]] = comment
        vectors, examples = {}, {}
        for user, items in by_user.items():
            rows = list(items)
            mean = matrix[rows].mean(axis=0)
            norm = np.linalg.norm(mean)
            if norm <= 1e-8:
                continue
            mean /= norm
            vectors[user] = mean
            representatives = sorted(rows, key=lambda i: (-float(matrix[i] @ mean), i))[:32]
            examples[user] = [(i, items[i]) for i in representatives]
        return vectors, examples, {user: len(items) for user, items in by_user.items()}

    @staticmethod
    def _pair(left, right, vectors, examples, counts, matrix):
        import numpy as np
        a, b = examples[left], examples[right]
        scores = matrix[[i for i, _ in a]] @ matrix[[i for i, _ in b]].T
        matches, used_left, used_right = [], set(), set()
        for flat in np.argsort(-scores.ravel(), kind="stable"):
            i, j = divmod(int(flat), len(b))
            if i in used_left or j in used_right:
                continue
            used_left.add(i)
            used_right.add(j)
            def evidence(comment):
                return Evidence(**{k: comment[k] for k in ("text", "date", "channel", "message_id")},
                                quote="", position="").model_dump()
            matches.append({"similarity": round(float(np.clip(scores[i, j], -1, 1)), 4),
                            "left": evidence(a[i][1]), "right": evidence(b[j][1])})
            if len(matches) == 5:
                break
        return {"left": left, "right": right,
                "similarity": round(float(np.clip(vectors[left] @ vectors[right], -1, 1)), 4),
                "left_comments": counts[left], "right_comments": counts[right], "examples": matches}

    async def run(self, *, lock_held=False, checkpoint=None):
        with nullcontext() if lock_held else self.store.analysis_lock():
            checkpoint = checkpoint or self.initial_checkpoint()
            try:
                await self._report(checkpoint)
                profiles, comments = await asyncio.to_thread(self._load_sources)
                comments = [c for c in comments if c["text"] and c["text"].strip()]
                await self._report(checkpoint, total_comments=len(comments), activity="Комментарии прочитаны")
                indices, matrix = await self._embed_comments(comments, checkpoint)
                vectors, examples, counts = await asyncio.to_thread(self._author_profiles, comments, indices, matrix)
                users = sorted(vectors)
                await self._report(checkpoint, phase="comparisons", total_pairs=len(users) * (len(users) - 1) // 2,
                                   total_relations=len(users) * (len(users) - 1) // 2,
                                   activity="Локальное сравнение средних эмбеддингов авторов")
                for left, right in combinations(users, 2):
                    pair = await asyncio.to_thread(self._pair, left, right, vectors, examples, counts, matrix)
                    pair["profiles"] = {str(u): {"tg_id": u,
                                               "display_username": profiles[u].display_username} for u in (left, right)}
                    checkpoint["pairs"][f"{left}:{right}"] = pair
                    await self._report(checkpoint, processed_pairs=checkpoint["progress"]["processed_pairs"] + 1,
                                       processed_relations=checkpoint["progress"]["processed_relations"] + 1)
                await self._report(checkpoint, state="done", phase="done", activity="Локальное сравнение завершено")
                await self._store_write(self.store.put, "published", "latest", checkpoint)
            except BaseException as exc:
                error = (f"Отсутствует пакет {exc.name} для локального анализа. Пересоберите образ: "
                         "docker build -f ci/Dockerfile -t telegram-parser ."
                         if isinstance(exc, ModuleNotFoundError) else str(exc) or type(exc).__name__)
                await self._report(checkpoint,
                                   state="interrupted" if isinstance(exc, asyncio.CancelledError) else "error",
                                   error="Анализ прерван; можно продолжить" if isinstance(exc, asyncio.CancelledError) else error,
                                   activity="Локальный анализ остановлен; вычисленные векторы сохранены")
                raise

    def results(self, tg_id):
        run = self.store.get("run", "latest")
        published = self.store.get("published", "latest")
        # An earlier LLM agreement score is never presented as embedding similarity.
        run = run if run and run.get("method") == "text_similarity" else None
        published = published if published and published.get("method") == "text_similarity" else None
        current = published if published and (not run or run["progress"]["state"] != "done") else run
        progress = AnalysisProgress.model_validate(run["progress"]) if run else AnalysisProgress()
        if run and run["version"] != self.version:
            progress = AnalysisProgress()
        if progress.state == "running":
            try:
                with self.store.analysis_lock():
                    progress.state = "interrupted"
                    progress.error = "Анализ прерван; можно продолжить"
            except RuntimeError:
                pass
        result = PositionResults(
            method="text_similarity", version=current["version"] if current else self.version, progress=progress,
            incomplete=bool(current and current["progress"]["state"] != "done"),
            needs_update=bool(current and (current["version"] != self.version or current["manifest"] != self.store.manifest())),
            embeddings=EmbeddingDetails(model=getattr(self.embedder, "model_name", self.embedder.version),
                                        version=self.embedder.version, dimensions=getattr(self.embedder, "dimensions", 0),
                                        storage=self.store.location, saved_vectors=self.store.count(self._vectors)),
        )
        for pair in (current or {}).get("pairs", {}).values():
            if tg_id not in (pair["left"], pair["right"]):
                continue
            reverse = tg_id == pair["right"]
            other = pair["left"] if reverse else pair["right"]
            matches = [{**m, "left": m["right"], "right": m["left"]} if reverse else m for m in pair["examples"]]
            result.similar_authors.append(SimilarAuthor(
                **pair["profiles"][str(other)], similarity=pair["similarity"],
                left_comments=pair["right_comments"] if reverse else pair["left_comments"],
                right_comments=pair["left_comments"] if reverse else pair["right_comments"], examples=matches,
            ))
        result.similar_authors.sort(key=lambda a: (-a.similarity, a.tg_id))
        result.similar_authors = result.similar_authors[:10]
        return result
