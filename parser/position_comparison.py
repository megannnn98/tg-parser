"""Public comparison values and agreement rules, independent of inference."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ResponseValue(BaseModel):
    model_config = ConfigDict(json_schema_serialization_defaults_required=True)


class Evidence(ResponseValue):
    text: str
    quote: str
    date: str
    channel: str
    message_id: int
    position: str
    question: str = ""


class QuestionMatch(ResponseValue):
    question: str
    result: Literal["agreement", "partial", "disagreement", "unclear"]
    explanation: str
    left: Evidence | None
    right: Evidence | None


class Comparison(ResponseValue):
    tg_id: int
    display_username: str
    score: float | None = None
    comparable_questions: int = 0
    common_questions: int = 0
    agreements: int = 0
    partial: int = 0
    disagreements: int = 0
    eligible: bool = False
    questions: list[QuestionMatch] = Field(default_factory=list)


def summarize(comparison: Comparison) -> Comparison:
    comparison.common_questions = len(comparison.questions)
    comparison.agreements = sum(q.result == "agreement" for q in comparison.questions)
    comparison.partial = sum(q.result == "partial" for q in comparison.questions)
    comparison.disagreements = sum(q.result == "disagreement" for q in comparison.questions)
    count = comparison.agreements + comparison.partial + comparison.disagreements
    comparison.comparable_questions = count
    comparison.score = (
        round(100 * (comparison.agreements + 0.5 * comparison.partial) / count, 2)
        if count else None
    )
    comparison.eligible = count >= 3
    return comparison


class AnalysisProgress(ResponseValue):
    state: Literal["idle", "running", "done", "error", "interrupted", "provider_changed"] = "idle"
    phase: str = ""
    processed_comments: int = 0
    total_comments: int = 0
    cached_comments: int = 0
    rejected_comments: int = 0
    rejection_reasons: dict[str, int] = Field(default_factory=dict)
    processed_embeddings: int = 0
    total_embeddings: int = 0
    cached_embeddings: int = 0
    processed_questions: int = 0
    total_questions: int = 0
    extraction_requests: int = 0
    split_batches: int = 0
    activity: str = ""
    updated_at: str | None = None
    processed_pairs: int = 0
    total_pairs: int = 0
    processed_relations: int = 0
    total_relations: int = 0
    error: str | None = None


class EmbeddingDetails(ResponseValue):
    model: str
    version: str
    dimensions: int
    storage: str
    saved_vectors: int


class TextMatch(ResponseValue):
    similarity: float
    left: Evidence
    right: Evidence


class SimilarAuthor(ResponseValue):
    tg_id: int
    display_username: str
    similarity: float
    left_comments: int
    right_comments: int
    examples: list[TextMatch] = Field(default_factory=list)


class PositionResults(ResponseValue):
    version: str
    needs_update: bool = False
    incomplete: bool = False
    progress: AnalysisProgress = Field(default_factory=AnalysisProgress)
    embeddings: EmbeddingDetails | None = None
    method: Literal["position_agreement", "text_similarity"] = "position_agreement"
    similar_authors: list[SimilarAuthor] = Field(default_factory=list)
    ranking: list[Comparison] = Field(default_factory=list)
    insufficient: list[Comparison] = Field(default_factory=list)
    partial_results: list[Comparison] = Field(default_factory=list)
