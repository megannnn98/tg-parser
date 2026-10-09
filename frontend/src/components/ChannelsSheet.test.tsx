import { fireEvent, screen, waitFor } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";

import { getChannels, saveChannelsList } from "@/api/generated";
import { ChannelsSheet } from "@/components/ChannelsSheet";
import { ok, renderPage } from "@/test/render";

vi.mock("@/api/generated", () => ({ getChannels: vi.fn(), saveChannelsList: vi.fn() }));

beforeEach(() => {
  vi.mocked(getChannels).mockReset().mockReturnValue(ok({ channels: ["chan_a", "chan_b"] }) as never);
  vi.mocked(saveChannelsList).mockReset();
});

function renderSheet() {
  return renderPage(
    <ChannelsSheet trigger={(count) => <button type="button">Каналы{count === undefined ? "" : ` · ${count}`}</button>} />
  );
}

async function open() {
  fireEvent.click(await screen.findByRole("button", { name: "Каналы · 2" }));
  return screen.findByRole("textbox", { name: "Список каналов" });
}

it("keeps the list out of sight until it is asked for", async () => {
  renderSheet();

  await screen.findByRole("button", { name: "Каналы · 2" });
  expect(screen.queryByRole("textbox", { name: "Список каналов" })).toBeNull();

  expect(((await open()) as HTMLTextAreaElement).value).toBe("chan_a\nchan_b");
});

it("saves the channel list and shows the new number on the button", async () => {
  vi.mocked(saveChannelsList).mockReturnValue(ok({ channels: ["chan_a", "chan_b", "chan_c"] }) as never);
  renderSheet();

  fireEvent.change(await open(), { target: { value: "chan_a\nchan_b\n@chan_c" } });
  fireEvent.click(screen.getByRole("button", { name: "Сохранить список каналов" }));

  expect(await screen.findByText("Сохранено: 3 канал(ов)")).toBeTruthy();
  expect(saveChannelsList).toHaveBeenCalledWith({ body: { channels_text: "chan_a\nchan_b\n@chan_c" } });
  expect(screen.getByRole("button", { name: "Каналы · 3", hidden: true })).toBeTruthy();
});

it("asks before saving an empty channel list", async () => {
  vi.mocked(saveChannelsList).mockReturnValue(ok({ channels: [] }) as never);
  renderSheet();

  fireEvent.change(await open(), { target: { value: "" } });
  fireEvent.click(screen.getByRole("button", { name: "Сохранить список каналов" }));

  expect(await screen.findByText("Список каналов станет пустым. Сохранить?")).toBeTruthy();
  expect(saveChannelsList).not.toHaveBeenCalled();

  fireEvent.click(screen.getByRole("button", { name: "Сохранить" }));
  await waitFor(() => expect(saveChannelsList).toHaveBeenCalled());
});
