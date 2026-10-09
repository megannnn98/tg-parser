"""Author similarity from the stored comment embeddings, without external inference."""
from __future__ import annotations

import asyncio
from contextlib import nullcontext
from itertools import combinations

from embeddings.e5 import PRODUCTION_MODEL
from parser.position_analysis import PositionAnalysis
from parser.position_comparison import AnalysisProgress, EmbeddingDetails, Evidence, PositionResults, SimilarAuthor
from db.analysis_store import digest


_EXAMPLES = 5
# Comments of one author compared at a time; bounds the memory of the scores.
_EXAMPLE_BLOCK = 1024


class LocalTextAnalysis(PositionAnalysis):
    # Shorter comments are close by form ("да", "согласен"), not by content,
    # and are not shown as similar statements.
    example_min_chars = 80

    # Reuse source loading, cancellation-safe persistence and progress reporting.
    # This constructor deliberately does not create a gateway or provider guard.
    def __init__(self, store, model=PRODUCTION_MODEL, embed_missing=None):
        """`model` names the vectors of message_embeddings to compare.

        `embed_missing` is awaited before they are read, so that comments
        collected after the last embedding run take part.
        """
        self.store = store
        self.model = model
        self._embed_missing = embed_missing
        self._vectors = f"{model.name}@{model.revision}:{model.passage_prefix.strip()}{model.pooling}:l2"
        self.version = digest(["local-comment-similarity:centered-centroid:closest-comments:v4", self._vectors])

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
        """The comments that have a vector, the row of each text and the rows."""
        await self._report(checkpoint, phase="embeddings",
                           activity="Создание эмбеддингов новых комментариев")
        if self._embed_missing is not None:
            await self._embed_missing()
        await self._report(checkpoint, activity="Загрузка эмбеддингов комментариев из PostgreSQL")
        stored = await asyncio.to_thread(self.store.comment_vectors, self.model)
        comments = [c for c in comments if c["id"] in stored]
        indices, rows = {}, []
        for comment in comments:
            key = digest(comment["text"])
            if key not in indices:
                indices[key] = len(rows)
                rows.append(stored[comment["id"]])
        if not rows:
            raise ValueError("Нет эмбеддингов комментариев. Постройте их: python -m scripts.embed --messages")
        matrix = self._validated_vectors(rows, len(rows), self.model.dimensions)
        await self._report(checkpoint, total_embeddings=len(rows), processed_embeddings=len(rows),
                           cached_embeddings=len(rows), processed_comments=len(comments),
                           cached_comments=len(comments), activity="Эмбеддинги комментариев загружены")
        return comments, indices, matrix

    @staticmethod
    def _centered(comments, indices, matrix):
        """The rows without what all authors share, at unit length again.

        E5 vectors of any two texts are close, and averaging an author's
        comments leaves little else: uncentered, every pair of authors scores
        about 0.99. The shared part is the mean of the authors' own means, so
        that a prolific author does not define it. A row equal to it carries
        no signal and becomes zero.
        """
        import numpy as np
        by_user = {}
        for comment in comments:
            by_user.setdefault(comment["tg_id"], set()).add(indices[digest(comment["text"])])
        shared = np.mean([matrix[sorted(rows)].mean(axis=0) for rows in by_user.values()], axis=0)
        centered = matrix - shared
        norms = np.linalg.norm(centered, axis=1, keepdims=True)
        return np.divide(centered, norms, out=np.zeros_like(centered), where=norms > 1e-6)

    def _author_profiles(self, comments, indices, centered):
        """Each author's mean centered vector and the comments usable as examples."""
        import numpy as np
        by_user = {}
        for comment in comments:
            # Identical repeated comments have equal weight to one unique text.
            by_user.setdefault(comment["tg_id"], {})[indices[digest(comment["text"])]] = comment
        vectors, candidates = {}, {}
        for user, items in by_user.items():
            mean = centered[list(items)].mean(axis=0)
            norm = np.linalg.norm(mean)
            if norm <= 1e-8:
                continue
            vectors[user] = mean / norm
            candidates[user] = [(row, comment) for row, comment in items.items()
                                if len(comment["text"].strip()) >= self.example_min_chars]
        return vectors, candidates, {user: len(items) for user, items in by_user.items()}

    @staticmethod
    def _closest_comments(a, b, matrix):
        """Up to five (similarity, a index, b index): the closest comments of two authors.

        Every comment of one author is compared with every comment of the
        other, by the similarity the search uses (the stored vectors, not the
        centered ones). No comment appears twice.
        """
        import numpy as np
        if not a or not b:
            return []
        right_rows = np.array([row for row, _ in b])
        right = matrix[right_rows]
        keep = min(_EXAMPLES, len(b))
        found = []
        for start in range(0, len(a), _EXAMPLE_BLOCK):
            rows = np.array([row for row, _ in a[start:start + _EXAMPLE_BLOCK]])
            scores = matrix[rows] @ right.T
            # Both authors wrote this very text: a copy shows nothing.
            scores[rows[:, None] == right_rows[None, :]] = -np.inf
            # Its `keep` closest for every comment, so that one comment close
            # to many cannot crowd the others out.
            closest = np.argpartition(-scores, keep - 1, axis=1)[:, :keep]
            found.extend((float(scores[i, j]), start + i, int(j))
                         for i, columns in enumerate(closest) for j in columns)
        matches, used_left, used_right = [], set(), set()
        for score, i, j in sorted(found, key=lambda match: (-match[0], match[1], match[2])):
            if i in used_left or j in used_right or score == -np.inf:
                continue
            used_left.add(i)
            used_right.add(j)
            matches.append((score, i, j))
            if len(matches) == _EXAMPLES:
                break
        return matches

    @classmethod
    def _pair(cls, left, right, vectors, candidates, counts, matrix):
        import numpy as np
        a, b = candidates[left], candidates[right]

        def evidence(comment):
            return Evidence(**{k: comment[k] for k in ("text", "date", "channel", "message_id")},
                            quote="", position="").model_dump()
        matches = [{"similarity": round(float(np.clip(score, -1, 1)), 4),
                    "left": evidence(a[i][1]), "right": evidence(b[j][1])}
                   for score, i, j in cls._closest_comments(a, b, matrix)]
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
                comments, indices, matrix = await self._embed_comments(comments, checkpoint)
                centered = await asyncio.to_thread(self._centered, comments, indices, matrix)
                vectors, examples, counts = await asyncio.to_thread(self._author_profiles, comments, indices, centered)
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
            embeddings=EmbeddingDetails(model=self.model.name, version=self._vectors,
                                        dimensions=self.model.dimensions,
                                        storage="PostgreSQL, таблица message_embeddings",
                                        saved_vectors=self.store.count_comment_vectors(self.model)),
        )
        for pair in (current or {}).get("pairs", {}).values():
            # An author on the other side of the average is not a similar one.
            if tg_id not in (pair["left"], pair["right"]) or pair["similarity"] <= 0:
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
