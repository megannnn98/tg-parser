import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";

import { analyzePolitical, cancelCollect, collectStatus, getUser, startCollect } from "@/api/generated";
import { ProfilePage } from "@/pages/ProfilePage";
import { failed, ok, renderPage } from "@/test/render";

vi.mock("@/components/PositionComparisons", () => ({
  PositionComparisons: () => <div>Близкие политические позиции</div>
}));

vi.mock("@/api/generated", () => ({
  getUser: vi.fn(),
  analyzePolitical: vi.fn(),
  startCollect: vi.fn(),
  collectStatus: vi.fn(),
  cancelCollect: vi.fn()
}));

function detail(username: string | null) {
  return {
    profile: {
      tg_id: 7,
      username,
      display_name: null,
      display_username: username ? `@${username}` : "нет ника",
      total_messages: 3,
      channel_count: 2,
      channels: [
        { name: "chan_a", message_count: 2, percent: 66.7, color: "#2563eb", dasharray: "66.7 33.3", dashoffset: 0 },
        { name: "chan_b", message_count: 1, percent: 33.3, color: "#dc2626", dasharray: "33.3 66.7", dashoffset: -66.7 }
      ]
    },
    hourly_activity: [
      { hour: 8, count: 1 },
      { hour: 14, count: 2 }
    ],
    daily_activity: [
      { date: "2026-08-01", count: 2 },
      { date: "2026-08-02", count: 1 }
    ],
    weekly_activity: [{ weekday: 5, hour: 8, count: 1 }]
  };
}

beforeEach(() => {
  vi.mocked(getUser).mockReset().mockReturnValue(ok(detail("vasya")) as never);
  vi.mocked(analyzePolitical).mockReset();
  vi.mocked(startCollect).mockReset();
  vi.mocked(collectStatus).mockReset();
  vi.mocked(cancelCollect).mockReset();
});

function renderProfile() {
  return renderPage(<ProfilePage />, { path: "/users/:tgId", url: "/users/7" });
}

it("shows the profile, its channels and the export link", async () => {
  renderProfile();

  expect(await screen.findByRole("heading", { name: "@vasya" })).toBeTruthy();
  expect(getUser).toHaveBeenCalledWith({ path: { tg_id: 7 } });
  expect(screen.getByText("chan_a")).toBeTruthy();
  expect(screen.getByText("66.7%")).toBeTruthy();
  expect(screen.getByRole("link", { name: "Скачать .txt" }).getAttribute("href")).toBe(
    "/api/v1/users/7/comments.txt"
  );
  // Search by meaning is offered for a user with comments.
  expect(screen.getByLabelText("Запрос")).toBeTruthy();
});

it("does not ask the API about an address that is not a Telegram id", async () => {
  renderPage(<ProfilePage />, { path: "/users/:tgId", url: "/users/vasya_7.db" });

  expect(await screen.findByText(/Страница не найдена/)).toBeTruthy();
  expect(getUser).not.toHaveBeenCalled();
});

it("shows an empty collection without an error and disables political analysis", async () => {
  const empty = detail(null);
  empty.profile.total_messages = 0;
  empty.profile.channel_count = 0;
  empty.profile.channels = [];
  empty.hourly_activity = [];
  empty.daily_activity = [];
  empty.weekly_activity = [];
  vi.mocked(getUser).mockReturnValue(ok(empty) as never);

  renderProfile();

  expect(await screen.findByText("Комментарии в выбранных каналах не найдены.")).toBeTruthy();
  // Nothing to search.
  expect(screen.queryByLabelText("Запрос")).toBeNull();
  expect(screen.getByRole("button", { name: "Определить полит взгляды" }).hasAttribute("disabled")).toBe(true);
  expect(screen.getByRole("button", { name: "Обновить комментарии" }).hasAttribute("disabled")).toBe(false);
});

it("says when the database is unknown", async () => {
  vi.mocked(getUser).mockReturnValue(failed(404, "Unknown user database") as never);

  renderProfile();

  expect(await screen.findByText("Unknown user database")).toBeTruthy();
});

it("draws both activity charts", async () => {
  renderProfile();

  const hourly = await screen.findByRole("img", { name: "График активности по часам" });
  expect(hourly.querySelectorAll("rect")).toHaveLength(2);
  expect(hourly.textContent).toContain("2 сообщений в 14:00");
  expect(screen.getByRole("img", { name: "График активности по дням" }).textContent).toContain(
    "1 сообщений за 2026-08-02"
  );
});

it("selects the latest day, then the day clicked or chosen with the keyboard", async () => {
  renderProfile();

  expect(await screen.findByText("2026-08-02: 1 сообщений")).toBeTruthy();
  const first = screen.getByRole("button", { name: "2026-08-01: 2 сообщений" });
  const last = screen.getByRole("button", { name: "2026-08-02: 1 сообщений" });
  expect(last.getAttribute("aria-pressed")).toBe("true");

  fireEvent.click(first);
  expect(screen.getByText("2026-08-01: 2 сообщений")).toBeTruthy();
  expect(first.getAttribute("aria-pressed")).toBe("true");
  expect(last.getAttribute("aria-pressed")).toBe("false");

  fireEvent.keyDown(last, { key: "Enter" });
  expect(screen.getByText("2026-08-02: 1 сообщений")).toBeTruthy();
});

it("refreshes the comments by username, or by id without one", async () => {
  vi.mocked(getUser).mockReturnValue(ok(detail(null)) as never);
  vi.mocked(startCollect).mockReturnValue(ok({ job_id: "j1" }) as never);
  vi.mocked(collectStatus).mockReturnValue(
    ok({
      job_id: "j1",
      user_ref: 7,
      total_channels: 2,
      state: "running",
      resolved: null,
      channels: [{ channel: "chan_a", status: "started", saved: 0, error: null }],
      saved_total: 0,
      tg_id: null,
      error: null
    }) as never
  );
  vi.mocked(cancelCollect).mockReturnValue(ok({ cancelled: true }) as never);

  renderProfile();
  fireEvent.click(await screen.findByRole("button", { name: "Обновить комментарии" }));

  expect(await screen.findByText("Читаю канал: chan_a")).toBeTruthy();
  expect(startCollect).toHaveBeenCalledWith({ body: { username: "7" } });

  fireEvent.click(screen.getByRole("button", { name: "Остановить" }));
  await waitFor(() => expect(cancelCollect).toHaveBeenCalledWith({ path: { job_id: "j1" } }));
});

it("shows the political coordinates on demand", async () => {
  vi.mocked(analyzePolitical).mockReturnValue(
    ok({
      total_messages: 40,
      signal_count: 10,
      bars: "",
      axes: { economic: { left_count: 3, right_count: 1 } }
    }) as never
  );

  renderProfile();
  fireEvent.click(await screen.findByRole("button", { name: "Определить полит взгляды" }));

  const meter = await screen.findByRole("meter", { name: "Левая — Правая" });
  expect(meter.getAttribute("aria-valuenow")).toBe("75");
  expect(screen.getByText(/Итого: 10 из 40 сообщений \(25%\)/)).toBeTruthy();
  expect(analyzePolitical).toHaveBeenCalledWith({ path: { tg_id: 7 } });
});

it("shows why the political analysis failed", async () => {
  vi.mocked(analyzePolitical).mockReturnValue(failed(400, "OPENROUTER_API_KEY is not set") as never);

  renderProfile();
  fireEvent.click(await screen.findByRole("button", { name: "Определить полит взгляды" }));

  expect(await screen.findByText("OPENROUTER_API_KEY is not set")).toBeTruthy();
  expect(within(document.body).getByRole("button", { name: "Определить полит взгляды" })).toBeTruthy();
});

it("drops the political result once a refresh brings new comments", async () => {
  vi.mocked(analyzePolitical).mockReturnValue(
    ok({ total_messages: 40, signal_count: 10, bars: "", axes: {} }) as never
  );
  vi.mocked(startCollect).mockReturnValue(ok({ job_id: "j1" }) as never);
  vi.mocked(collectStatus).mockReturnValue(
    ok({
      job_id: "j1",
      user_ref: "@vasya",
      total_channels: 1,
      state: "done",
      resolved: null,
      channels: [],
      saved_total: 60,
      tg_id: 7,
      error: null
    }) as never
  );

  renderProfile();
  fireEvent.click(await screen.findByRole("button", { name: "Определить полит взгляды" }));
  expect(await screen.findByText(/Итого: 10 из 40/)).toBeTruthy();

  fireEvent.click(screen.getByRole("button", { name: "Обновить комментарии" }));

  expect(await screen.findByText("Готово: сохранено 60 новых сообщений")).toBeTruthy();
  await waitFor(() => expect(screen.queryByText(/Итого: 10 из 40/)).toBeNull());
});
