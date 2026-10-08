import { fireEvent, screen, waitFor } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";

import { collectStatus, getChannels, listProfiles, saveChannelsList, startCollect } from "@/api/generated";
import { UsersPage } from "@/pages/UsersPage";
import { failed, ok, renderPage } from "@/test/render";

vi.mock("@/api/generated", () => ({
  listProfiles: vi.fn(),
  getChannels: vi.fn(),
  saveChannelsList: vi.fn(),
  startCollect: vi.fn(),
  collectStatus: vi.fn(),
  cancelCollect: vi.fn()
}));

const profile = {
  db_name: "vasya_7.db",
  tg_id: 7,
  username: "vasya",
  display_name: null,
  display_username: "@vasya",
  total_messages: 3,
  channel_count: 2,
  channels: []
};

function doneJob(db_name: string) {
  return {
    job_id: "j1",
    user_ref: "@petya",
    total_channels: 1,
    state: "done",
    resolved: { tg_id: 8, username: "petya", display_name: null },
    channels: [{ channel: "chan_a", status: "done", saved: 2, error: null }],
    saved_total: 2,
    db_name,
    error: null
  };
}

beforeEach(() => {
  vi.mocked(listProfiles).mockReset().mockReturnValue(ok([profile]) as never);
  vi.mocked(getChannels).mockReset().mockReturnValue(ok({ channels: ["chan_a", "chan_b"] }) as never);
  vi.mocked(saveChannelsList).mockReset();
  vi.mocked(startCollect).mockReset();
  vi.mocked(collectStatus).mockReset();
});

function renderUsers() {
  return renderPage(<UsersPage />, { routes: { "/users/:dbName": <p>profile page</p> } });
}

it("links every profile to its page", async () => {
  renderUsers();

  const link = await screen.findByRole("link", { name: /@vasya/ });
  expect(link.getAttribute("href")).toBe("/users/vasya_7.db");
  expect(link.textContent).toContain("3 сообщений, 2 каналов");
});

it("says when no user database is downloaded", async () => {
  vi.mocked(listProfiles).mockReturnValue(ok([]) as never);

  renderUsers();

  expect(await screen.findByText(/нет скачанных user DB/)).toBeTruthy();
});

it("collects a user's comments and opens the new profile", async () => {
  vi.mocked(startCollect).mockReturnValue(ok({ job_id: "j1" }) as never);
  vi.mocked(collectStatus).mockReturnValue(ok(doneJob("petya_8.db")) as never);

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
      ...doneJob("x.db"),
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

it("saves the channel list", async () => {
  vi.mocked(saveChannelsList).mockReturnValue(ok({ channels: ["chan_a", "chan_b", "chan_c"] }) as never);

  renderUsers();
  const textarea = await screen.findByLabelText("Список каналов");
  fireEvent.change(textarea, { target: { value: "chan_a\nchan_b\n@chan_c" } });
  fireEvent.click(screen.getByRole("button", { name: "Сохранить список каналов" }));

  expect(await screen.findByText("Сохранено: 3 канал(ов)")).toBeTruthy();
  expect(saveChannelsList).toHaveBeenCalledWith({ body: { channels_text: "chan_a\nchan_b\n@chan_c" } });
});

it("asks before saving an empty channel list", async () => {
  vi.mocked(saveChannelsList).mockReturnValue(ok({ channels: [] }) as never);

  renderUsers();
  fireEvent.change(await screen.findByLabelText("Список каналов"), { target: { value: "" } });
  fireEvent.click(screen.getByRole("button", { name: "Сохранить список каналов" }));

  expect(await screen.findByText("Список каналов станет пустым. Сохранить?")).toBeTruthy();
  expect(saveChannelsList).not.toHaveBeenCalled();

  fireEvent.click(screen.getByRole("button", { name: "Сохранить" }));
  await waitFor(() => expect(saveChannelsList).toHaveBeenCalled());
});
