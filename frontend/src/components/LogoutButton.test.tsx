import { fireEvent, screen } from "@testing-library/react";
import { QueryClient } from "@tanstack/react-query";
import { beforeEach, expect, it, vi } from "vitest";

import { logout } from "@/api/generated";
import { LogoutButton } from "@/components/LogoutButton";
import { ok, renderPage } from "@/test/render";

vi.mock("@/api/generated", () => ({ logout: vi.fn() }));

beforeEach(() => vi.mocked(logout).mockReset().mockReturnValue(ok(undefined) as never));

it("signs out, opens the login and drops what was loaded", async () => {
  const client = new QueryClient();
  client.setQueryData(["profiles"], [{ tg_id: 7 }]);
  renderPage(<LogoutButton />, { routes: { "/login": <p>вход</p> }, client });

  fireEvent.click(screen.getByRole("button", { name: "Выйти" }));

  expect(await screen.findByText("вход")).toBeTruthy();
  expect(logout).toHaveBeenCalledTimes(1);
  expect(client.getQueryData(["profiles"])).toBeUndefined();
});
