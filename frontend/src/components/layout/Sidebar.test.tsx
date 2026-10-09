import { fireEvent, screen } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";

import { getChannels, listProfiles } from "@/api/generated";
import { Sidebar } from "@/components/layout/Sidebar";
import { failed, ok, renderPage } from "@/test/render";

vi.mock("@/api/generated", () => ({ listProfiles: vi.fn(), getChannels: vi.fn(), logout: vi.fn() }));

const vasya = {
  tg_id: 7,
  username: "vasya",
  display_name: "Вася Пупкин",
  display_username: "@vasya",
  total_messages: 1234,
  channel_count: 2,
  channels: []
};
const petya = { ...vasya, tg_id: 8, username: "petya", display_name: null, display_username: "@petya", total_messages: 5 };

beforeEach(() => {
  vi.mocked(listProfiles).mockReset().mockReturnValue(ok([vasya, petya]) as never);
  vi.mocked(getChannels).mockReset().mockReturnValue(ok({ channels: ["chan_a", "chan_b"] }) as never);
});

function renderSidebar(url = "/") {
  return renderPage(<Sidebar />, { path: "*", url });
}

function users() {
  return screen.getAllByRole("link").filter((link) => link.getAttribute("href")?.startsWith("/users/"));
}

it("lists the users, each a link to its page", async () => {
  renderSidebar();

  await screen.findByText("@vasya");
  expect(users().map((link) => [link.getAttribute("href"), link.textContent])).toEqual([
    ["/users/7", "ВП@vasya1 234 сообщ."],
    ["/users/8", "PE@petya5 сообщ."]
  ]);
});

it("marks the user whose page is open", async () => {
  renderSidebar("/users/8");

  await screen.findByText("@petya");
  expect(users().map((link) => link.getAttribute("aria-current"))).toEqual([null, "page"]);
});

it("narrows the list as the search is typed", async () => {
  renderSidebar();
  await screen.findByText("@vasya");

  fireEvent.change(screen.getByLabelText("Поиск пользователя"), { target: { value: "пупк" } });
  expect(users().map((link) => link.getAttribute("href"))).toEqual(["/users/7"]);

  fireEvent.change(screen.getByLabelText("Поиск пользователя"), { target: { value: "nobody" } });
  expect(users()).toEqual([]);
  expect(screen.getByText("Никого не найдено.")).toBeTruthy();
});

it("says when nobody is collected yet", async () => {
  vi.mocked(listProfiles).mockReturnValue(ok([]) as never);

  renderSidebar();

  expect(await screen.findByText("Пользователей пока нет.")).toBeTruthy();
});

it("shows why the list did not load", async () => {
  vi.mocked(listProfiles).mockReturnValue(failed(500, "boom") as never);

  renderSidebar();

  expect(await screen.findByText("boom")).toBeTruthy();
});

it("offers the channel list and the logout", async () => {
  renderSidebar();

  expect(await screen.findByRole("button", { name: "Каналы · 2" })).toBeTruthy();
  expect(screen.getByRole("button", { name: "Выйти" })).toBeTruthy();
});
