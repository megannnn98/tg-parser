import { fireEvent, screen, waitFor } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";

import { searchUserComments, type SearchResponse } from "@/api/generated";
import { CommentSearch } from "@/components/CommentSearch";
import { failed, ok, renderPage } from "@/test/render";

vi.mock("@/api/generated", () => ({ searchUserComments: vi.fn() }));

function response(overrides: Partial<SearchResponse> = {}): SearchResponse {
  return {
    query: "профсоюзы",
    indexed_messages: 3,
    total_messages: 3,
    used_context: true,
    results: [
      {
        message_id: 11, tg_message_id: 501, channel: "chan_a", date: "2026-01-05T10:00:00Z",
        text: "Профсоюз объявил забастовку", message_score: 0.9, chunk_score: 0.86, score: 0.8812
      },
      {
        message_id: 12, tg_message_id: 502, channel: "chan_b", date: "2026-01-05T10:01:30Z",
        text: "согласен", message_score: 0.7, chunk_score: 0.86, score: 0.78
      }
    ],
    ...overrides
  };
}

function ask(text: string) {
  fireEvent.change(screen.getByLabelText("Запрос"), { target: { value: text } });
  fireEvent.click(screen.getByRole("button", { name: "Найти" }));
}

beforeEach(() => {
  vi.mocked(searchUserComments).mockReset();
});

it("asks nothing until a query is submitted", () => {
  renderPage(<CommentSearch tgId={7} />);

  expect((screen.getByRole("button", { name: "Найти" }) as HTMLButtonElement).disabled).toBe(true);
  expect(searchUserComments).not.toHaveBeenCalled();
});

it("searches the user's comments and lists them with channel, date and score", async () => {
  vi.mocked(searchUserComments).mockReturnValue(ok(response()) as never);

  renderPage(<CommentSearch tgId={7} />);
  ask("  профсоюзы  ");

  expect(await screen.findByText("Профсоюз объявил забастовку")).toBeTruthy();
  expect(searchUserComments).toHaveBeenCalledWith({ path: { tg_id: 7 }, query: { q: "профсоюзы", limit: 20 } });
  const items = screen.getAllByRole("listitem");
  expect(items).toHaveLength(2);
  expect(items[0].textContent).toContain("chan_a · 2026-01-05 10:00 · 0.88");
  expect(items[1].textContent).toContain("согласен");
});

it("submits on Enter", async () => {
  vi.mocked(searchUserComments).mockReturnValue(ok(response()) as never);

  renderPage(<CommentSearch tgId={7} />);
  const input = screen.getByLabelText("Запрос");
  fireEvent.change(input, { target: { value: "забастовка" } });
  fireEvent.submit(input.closest("form")!);

  await waitFor(() =>
    expect(searchUserComments).toHaveBeenCalledWith({ path: { tg_id: 7 }, query: { q: "забастовка", limit: 20 } })
  );
});

it("says when nothing matches", async () => {
  vi.mocked(searchUserComments).mockReturnValue(ok(response({ results: [] })) as never);

  renderPage(<CommentSearch tgId={7} />);
  ask("космос");

  expect(await screen.findByText("Ничего не найдено.")).toBeTruthy();
});

it("says when the embeddings are missing or incomplete", async () => {
  vi.mocked(searchUserComments).mockReturnValue(
    ok(response({ results: [], indexed_messages: 0, total_messages: 5 })) as never
  );
  const view = renderPage(<CommentSearch tgId={7} />);
  ask("профсоюзы");
  expect(await screen.findByText(/ещё не построены эмбеддинги/)).toBeTruthy();
  expect(screen.queryByText("Ничего не найдено.")).toBeNull();
  view.unmount();

  vi.mocked(searchUserComments).mockReturnValue(ok(response({ indexed_messages: 2, total_messages: 5 })) as never);
  renderPage(<CommentSearch tgId={7} />);
  ask("профсоюзы");
  expect(await screen.findByText(/Поиск идёт по 2 из 5 комментариев/)).toBeTruthy();
  expect(screen.getAllByRole("listitem")).toHaveLength(2);
});

it("shows why the search failed", async () => {
  vi.mocked(searchUserComments).mockReturnValue(failed(404, "User not found") as never);

  renderPage(<CommentSearch tgId={7} />);
  ask("профсоюзы");

  expect(await screen.findByText("User not found")).toBeTruthy();
});
