import { fireEvent, screen } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";

import { collectStatus, getChannels, listProfiles, startCollect } from "@/api/generated";
import { UsersPage } from "@/pages/UsersPage";
import { failed, ok, renderPage } from "@/test/render";

vi.mock("@/api/generated", () => ({
  listProfiles: vi.fn(),
  getChannels: vi.fn(),
  startCollect: vi.fn(),
  collectStatus: vi.fn(),
  cancelCollect: vi.fn()
}));

const profile = {
  tg_id: 7,
  username: "vasya",
  display_name: null,
  display_username: "@vasya",
  total_messages: 3,
  channel_count: 2,
  channels: []
};

function doneJob(tg_id: number) {
  return {
    job_id: "j1",
    user_ref: "@petya",
    total_channels: 1,
    state: "done",
    resolved: { tg_id: 8, username: "petya", display_name: null },
    channels: [{ channel: "chan_a", status: "done", saved: 2, error: null }],
    saved_total: 2,
    tg_id,
    error: null
  };
}

beforeEach(() => {
  vi.mocked(listProfiles).mockReset().mockReturnValue(ok([profile]) as never);
  vi.mocked(getChannels).mockReset().mockReturnValue(ok({ channels: ["chan_a", "chan_b"] }) as never);
  vi.mocked(startCollect).mockReset();
  vi.mocked(collectStatus).mockReset();
});

function renderUsers() {
  return renderPage(<UsersPage />, { routes: { "/users/:tgId": <p>profile page</p> } });
}

it("links every profile to its page", async () => {
  renderUsers();

  const link = await screen.findByRole("link", { name: /@vasya/ });
  expect(link.getAttribute("href")).toBe("/users/7");
  expect(link.textContent).toContain("3 сообщений, 2 каналов");
});

it("says when no user database is downloaded", async () => {
  vi.mocked(listProfiles).mockReturnValue(ok([]) as never);

  renderUsers();

  expect(await screen.findByText(/пока не скачаны/)).toBeTruthy();
});

it("collects a user's comments and opens the new profile", async () => {
  vi.mocked(startCollect).mockReturnValue(ok({ job_id: "j1" }) as never);
  vi.mocked(collectStatus).mockReturnValue(ok(doneJob(8)) as never);

  renderUsers();
  fireEvent.change(screen.getByLabelText("Юзернейм"), { target: { value: "@petya" } });
  fireEvent.click(screen.getByRole("button", { name: "Скачать" }));

  expect(await screen.findByText("profile page")).toBeTruthy();
  expect(startCollect).toHaveBeenCalledWith({ body: { username: "@petya" } });
  expect(collectStatus).toHaveBeenCalledWith({ path: { job_id: "j1" } });
});

it("shows why the collection did not start", async () => {
  vi.mocked(startCollect).mockReturnValue(failed(409, "Сбор комментариев уже выполняется") as never);

  renderUsers();
  fireEvent.change(screen.getByLabelText("Юзернейм"), { target: { value: "@petya" } });
  fireEvent.click(screen.getByRole("button", { name: "Скачать" }));

  expect(await screen.findByText("Сбор комментариев уже выполняется")).toBeTruthy();
  expect((screen.getByRole("button", { name: "Скачать" }) as HTMLButtonElement).disabled).toBe(false);
});

it("shows the job's error and the failed channels", async () => {
  vi.mocked(startCollect).mockReturnValue(ok({ job_id: "j1" }) as never);
  vi.mocked(collectStatus).mockReturnValue(
    ok({
      ...doneJob(8),
      state: "error",
      error: "FLOOD_WAIT",
      channels: [{ channel: "chan_a", status: "failed", saved: 0, error: "private" }]
    }) as never
  );

  renderUsers();
  fireEvent.change(screen.getByLabelText("Юзернейм"), { target: { value: "@petya" } });
  fireEvent.click(screen.getByRole("button", { name: "Скачать" }));

  expect(await screen.findByText("Ошибка: FLOOD_WAIT")).toBeTruthy();
  expect(screen.getByText("chan_a — private")).toBeTruthy();
});

it("sums up what is collected", async () => {
  vi.mocked(listProfiles).mockReturnValue(
    ok([profile, { ...profile, tg_id: 8, display_username: "@petya", total_messages: 1200 }]) as never
  );

  renderUsers();

  const totals = (await screen.findByText("профилей")).closest("dl")!;
  const pairs = Array.from(totals.querySelectorAll("div")).map((tile) => tile.textContent);
  expect(pairs).toEqual(["2профилей", "1\u00a0203комментариев", "2каналов в списке"]);
});

it("draws where a user wrote, a stripe per channel", async () => {
  vi.mocked(listProfiles).mockReturnValue(
    ok([
      {
        ...profile,
        channels: [
          { name: "chan_a", message_count: 2, percent: 66.7, color: "#111111", dasharray: "", dashoffset: 0 },
          { name: "chan_b", message_count: 1, percent: 33.3, color: "#222222", dasharray: "", dashoffset: 0 }
        ]
      }
    ]) as never
  );

  renderUsers();

  const link = await screen.findByRole("link", { name: /@vasya/ });
  const stripes = Array.from(link.querySelectorAll<HTMLElement>("span[title]"));
  expect(stripes.map((stripe) => [stripe.title, stripe.style.width])).toEqual([
    ["chan_a: 66.7%", "66.7%"],
    ["chan_b: 33.3%", "33.3%"]
  ]);
});

it("does not offer the channel list on the page itself", async () => {
  renderUsers();

  await screen.findByRole("link", { name: /@vasya/ });
  expect(screen.queryByLabelText("Список каналов")).toBeNull();
});
