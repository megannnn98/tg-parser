import { render, screen } from "@testing-library/react";
import { expect, it } from "vitest";
import { type AnalysisProgress, type EmbeddingDetails } from "@/api/generated";
import { PositionAnalysisProgress } from "./PositionAnalysisProgress";

function progress(): AnalysisProgress {
  return {
    state: "running", phase: "comments", processed_comments: 104, total_comments: 28814,
    cached_comments: 0, rejected_comments: 3, rejection_reasons: { quote_mismatch: 3 },
    processed_embeddings: 0, total_embeddings: 0, cached_embeddings: 0,
    processed_questions: 0, total_questions: 0, processed_pairs: 0, total_pairs: 0,
    processed_relations: 0, total_relations: 0, extraction_requests: 20, split_batches: 6,
    activity: "OpenRouter: извлечение позиций из 8 комментариев", updated_at: null, error: null
  };
}
const embeddings: EmbeddingDetails = {
  model: "intfloat/multilingual-e5-base", version: "e5@revision", dimensions: 768,
  storage: "data/position-analysis.sqlite3", saved_vectors: 0
};

it("explains extraction, exclusions and why embeddings do not exist yet", () => {
  render(<PositionAnalysisProgress progress={progress()} embeddings={embeddings} />);
  expect(screen.getByRole("progressbar").getAttribute("aria-valuenow")).toBe("0.4");
  expect(screen.getByRole("status").textContent).toContain("OpenRouter");
  expect(screen.getByText(/Цитата не совпала с исходным текстом: 3/)).toBeTruthy();
  expect(screen.getByText(/Векторов пока нет/)).toBeTruthy();
  expect(screen.getByText(/из 768 чисел/)).toBeTruthy();
  expect(screen.getByText("data/position-analysis.sqlite3")).toBeTruthy();
  expect(screen.getByText(/Процент относится к текущему этапу/)).toBeTruthy();
});

it("updates the bar for each stage and distinguishes questions from author pairs", () => {
  const p = progress();
  p.phase = "embeddings";
  p.total_embeddings = 32;
  p.processed_embeddings = 16;
  const view = render(<PositionAnalysisProgress progress={p} embeddings={{ ...embeddings, saved_vectors: 16 }} />);
  expect(screen.getByRole("progressbar").getAttribute("aria-valuenow")).toBe("50");
  expect(screen.getByRole("progressbar").getAttribute("aria-label")).toContain("Создание эмбеддингов");
  p.phase = "questions";
  p.total_questions = 5;
  p.processed_questions = 2;
  view.rerender(<PositionAnalysisProgress progress={{ ...p }} embeddings={embeddings} />);
  expect(screen.getByRole("progressbar").getAttribute("aria-valuenow")).toBe("40");
  p.phase = "comparisons";
  p.processed_relations = 3;
  p.total_relations = 6;
  p.total_pairs = 1;
  view.rerender(<PositionAnalysisProgress progress={{ ...p }} embeddings={embeddings} />);
  expect(screen.getByRole("progressbar").getAttribute("aria-valuenow")).toBe("50");
  expect(screen.getByText(/Завершённых пар авторов: 0 \/ 1/)).toBeTruthy();
});

it("keeps saved progress visible after interruption and explains resuming", () => {
  const p = progress();
  p.state = "interrupted";
  p.cached_comments = 104;
  p.activity = "";
  render(<PositionAnalysisProgress progress={p} embeddings={embeddings} />);
  expect(screen.getByRole("progressbar").getAttribute("aria-valuenow")).toBe("0.4");
  expect(screen.getByText(/Приостановлен/)).toBeTruthy();
  expect(screen.getByText(/Из кеша при этом запуске: 104/)).toBeTruthy();
  expect(screen.getByText(/Нажмите «Продолжить анализ»/)).toBeTruthy();
});

it("shows comment embedding progress and three local stages without paid extraction", () => {
  const p = progress();
  p.phase = "embeddings";
  p.processed_embeddings = 16;
  p.total_embeddings = 32;
  p.activity = "Локальная E5: создание эмбеддингов 16 комментариев";
  p.rejected_comments = 0;
  render(<PositionAnalysisProgress progress={p} embeddings={embeddings} local />);
  expect(screen.getByText("Этап 2 из 3: Создание эмбеддингов")).toBeTruthy();
  expect(screen.getByRole("progressbar").getAttribute("aria-valuenow")).toBe("50");
  expect(screen.queryByText(/Запросов извлечения/)).toBeNull();
  expect(screen.queryByText(/Сопоставление вопросов/)).toBeNull();
  expect(screen.getByText(/Кодируются сами комментарии/)).toBeTruthy();
  expect(screen.getByText("comment-embeddings:e5@revision")).toBeTruthy();
});
