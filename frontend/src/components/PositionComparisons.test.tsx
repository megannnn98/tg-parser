import { fireEvent, screen, waitFor } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";

import { getPositionComparisons, startPositionAnalysis, type PositionResults } from "@/api/generated";
import { PositionComparisons } from "@/components/PositionComparisons";
import { ok, renderPage } from "@/test/render";

vi.mock("@/api/generated", () => ({
  getPositionComparisons: vi.fn(),
  startPositionAnalysis: vi.fn()
}));

function response(): PositionResults {
  return {
    version: "v1", needs_update: false, incomplete: false, method: "position_agreement", similar_authors: [],
    progress: { state: "done", phase: "done", processed_comments: 20, total_comments: 20,
      processed_pairs: 1, total_pairs: 1, rejected_comments: 0, error: null,
      cached_comments: 0, rejection_reasons: {}, processed_embeddings: 4, total_embeddings: 4,
      cached_embeddings: 0, processed_questions: 4, total_questions: 4,
      extraction_requests: 3, split_batches: 0, activity: "Анализ завершён",
      updated_at: "2026-10-08T11:00:00Z", processed_relations: 3, total_relations: 3 },
    ranking: [{
      tg_id: 2, display_username: "@user2", score: 83.33,
      common_questions: 3, comparable_questions: 3, agreements: 2, partial: 1,
      disagreements: 0, eligible: true,
      questions: [{ question: "Запрет политических митингов", result: "partial",
        explanation: "Оба против запрета, но условия различаются",
        left: { text: "Я против запрета митингов", quote: "против запрета", date: "2026-01-01",
          channel: "channel", message_id: 1, position: "Против запрета", question: "Митинги" },
        right: { text: "Запрет допустим лишь при угрозе", quote: "лишь при угрозе", date: "2026-01-02",
          channel: "channel", message_id: 2, position: "Против с оговорками", question: "Митинги" }
      }]
    }],
    embeddings: { model: "intfloat/multilingual-e5-base", version: "e5@revision", dimensions: 768,
      storage: "data/position-analysis.sqlite3", saved_vectors: 4 },
    insufficient: [], partial_results: []
  };
}

beforeEach(() => {
  vi.mocked(getPositionComparisons).mockReset().mockReturnValue(ok(response()) as never);
  vi.mocked(startPositionAnalysis).mockReset().mockReturnValue(ok({ state: "running" }) as never);
});

it("shows local text similarity without presenting it as political agreement", async () => {
  const value = response();
  value.method = "text_similarity";
  const evidence = value.ranking[0].questions[0];
  value.similar_authors = [{ tg_id: 2, display_username: "@user2",
    similarity: 0.87, left_comments: 12, right_comments: 18,
    examples: [{ similarity: 0.92,
      left: { ...evidence.left!, position: "", quote: "" }, right: { ...evidence.right!, position: "", quote: "" } }] }];
  value.ranking = [];
  vi.mocked(getPositionComparisons).mockReturnValue(ok(value) as never);
  renderPage(<PositionComparisons tgId={1} />);
  expect(await screen.findByText("Похожие высказывания авторов")).toBeTruthy();
  expect(screen.getByText(/Косинусное сходство: 0,87/)).toBeTruthy();
  expect(screen.getByText(/политическое согласие не определяется/)).toBeTruthy();
  expect(screen.queryByText(/3 сравнимых/)).toBeNull();
  expect(screen.getByText(/Кодируются сами комментарии/)).toBeTruthy();
  fireEvent.click(screen.getByText("Похожие высказывания: @user2"));
  expect(screen.getByText("Я против запрета митингов")).toBeTruthy();
  expect(screen.queryByText(/Подтверждение:/)).toBeNull();
  expect(startPositionAnalysis).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Создать эмбеддинги и сравнить" }));
  await waitFor(() => expect(startPositionAnalysis).toHaveBeenCalledTimes(1));
});

it("shows agreement coverage and original statements in pair details", async () => {
  renderPage(<PositionComparisons tgId={1} />);
  expect(await screen.findByRole("link", { name: "@user2" })).toBeTruthy();
  expect(screen.getByText(/83,33/)).toBeTruthy();
  expect(screen.getByText(/3 сравнимых/)).toBeTruthy();
  fireEvent.click(screen.getByText(/Подробнее/));
  expect(screen.getByText("Я против запрета митингов")).toBeTruthy();
  expect(screen.getByText("Запрет допустим лишь при угрозе")).toBeTruthy();
  expect(screen.getByText(/Оба против запрета, но условия различаются/)).toBeTruthy();
});

it("does not mix partial comparisons with the completed ranking", async () => {
  const value = response();
  value.incomplete = true;
  value.progress.state = "error";
  value.progress.error = "DeepSeek недоступен";
  value.partial_results = value.ranking;
  value.ranking = [];
  vi.mocked(getPositionComparisons).mockReturnValue(ok(value) as never);
  renderPage(<PositionComparisons tgId={1} />);
  expect(await screen.findByText(/Неполный результат/)).toBeTruthy();
  expect(screen.getByText("DeepSeek недоступен")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: /Продолжить анализ/ }));
  await waitFor(() => expect(startPositionAnalysis).toHaveBeenCalledTimes(1));
});

it("starts analysis only on click, polls progress and shows the saved result", async () => {
  const idle = response();
  idle.progress.state = "idle";
  idle.ranking = [];
  const running = response();
  running.progress.state = "running";
  running.progress.phase = "comments";
  running.ranking = [];
  running.incomplete = true;
  vi.mocked(getPositionComparisons)
    .mockReturnValueOnce(ok(idle) as never)
    .mockReturnValueOnce(ok(running) as never)
    .mockReturnValue(ok(response()) as never);
  renderPage(<PositionComparisons tgId={1} />);
  await waitFor(() => expect(screen.getByRole("button", { name: "Найти близкие позиции" }).hasAttribute("disabled")).toBe(false));
  expect(startPositionAnalysis).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Найти близкие позиции" }));
  expect(await screen.findByText(/Неполный результат/)).toBeTruthy();
  expect(await screen.findByRole("link", { name: "@user2" }, { timeout: 4000 })).toBeTruthy();
  expect(startPositionAnalysis).toHaveBeenCalledTimes(1);
});

it("keeps the saved ranking visible after a failed update", async () => {
  const value = response();
  value.needs_update = true;
  value.progress.state = "error";
  value.progress.error = "DeepSeek недоступен: ReadTimeout";
  value.progress.rejected_comments = 2;
  vi.mocked(getPositionComparisons).mockReturnValue(ok(value) as never);
  renderPage(<PositionComparisons tgId={1} />);
  expect(await screen.findByRole("link", { name: "@user2" })).toBeTruthy();
  expect(screen.getByText(/показан ранее сохранённый результат/)).toBeTruthy();
  expect(screen.getByText(/Исключено из-за ошибок ответа: 2/)).toBeTruthy();
  expect(screen.queryByText(/Неполный результат/)).toBeNull();
});

it("blocks retries after a provider change until the analysis generation changes", async () => {
  const value = response();
  value.progress.state = "provider_changed";
  value.progress.error = "Изменилась версия модели DeepSeek; требуется новая версия анализа";
  vi.mocked(getPositionComparisons).mockReturnValue(ok(value) as never);
  renderPage(<PositionComparisons tgId={1} />);
  expect(await screen.findByText(value.progress.error)).toBeTruthy();
  expect(screen.getByRole("button", { name: /Найти близкие позиции/ }).hasAttribute("disabled")).toBe(true);
  expect(startPositionAnalysis).not.toHaveBeenCalled();
});
