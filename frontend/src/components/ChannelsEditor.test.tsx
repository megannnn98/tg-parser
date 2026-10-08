import { fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, expect, it, vi } from "vitest";

import { saveChannelsList } from "@/api/generated";
import { ChannelsEditor } from "@/components/ChannelsEditor";
import { ok } from "@/test/render";

vi.mock("@/api/generated", () => ({ saveChannelsList: vi.fn() }));

beforeEach(() => vi.mocked(saveChannelsList).mockReset().mockReturnValue(ok({ channels: [] }) as never));

const client = new QueryClient();
const editor = (saved: string[]) => (
  <QueryClientProvider client={client}>
    <ChannelsEditor saved={saved} />
  </QueryClientProvider>
);
const TEN = Array.from({ length: 10 }, (_, i) => `chan_${i}`);

it("shows a newer saved list while the text is untouched", () => {
  const { rerender } = render(editor(["chan_0", "chan_1"]));

  rerender(editor(TEN));

  expect((screen.getByLabelText("Список каналов") as HTMLTextAreaElement).value).toBe(TEN.join("\n"));
});

it("keeps an edited draft and asks before it cuts the newer saved list", async () => {
  const { rerender } = render(editor(["chan_0", "chan_1"]));
  fireEvent.change(screen.getByLabelText("Список каналов"), { target: { value: "chan_0\nchan_1\n" } });

  rerender(editor(TEN));
  fireEvent.click(screen.getByRole("button", { name: "Сохранить список каналов" }));

  expect((screen.getByLabelText("Список каналов") as HTMLTextAreaElement).value).toBe("chan_0\nchan_1\n");
  expect(await screen.findByText("Список уменьшится с 10 до 2 каналов. Сохранить?")).toBeTruthy();
  expect(saveChannelsList).not.toHaveBeenCalled();
});
