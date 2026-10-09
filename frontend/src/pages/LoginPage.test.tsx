import { fireEvent, screen, waitFor } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";

import { login } from "@/api/generated";
import { LoginPage } from "@/pages/LoginPage";
import { renderPage } from "@/test/render";

vi.mock("@/api/generated", () => ({ login: vi.fn() }));

function answer(status: number, detail?: string, headers?: Record<string, string>) {
  return Promise.resolve({
    data: undefined,
    error: detail === undefined ? undefined : { detail },
    request: new Request("http://test"),
    response: new Response(null, { status, headers })
  });
}

function submit(password: string) {
  fireEvent.change(screen.getByLabelText("Пароль"), { target: { value: password } });
  fireEvent.click(screen.getByRole("button", { name: "Войти" }));
}

const routes = { "/": <p>все пользователи</p>, "/users/:tgId": <p>профиль</p> };

beforeEach(() => vi.mocked(login).mockReset());

it("signs in and returns to the page the visitor came from", async () => {
  vi.mocked(login).mockReturnValue(answer(204) as never);
  renderPage(<LoginPage />, { path: "/login", url: "/login?next=%2Fusers%2F7", routes });

  submit("secret");

  expect(await screen.findByText("профиль")).toBeTruthy();
  expect(login).toHaveBeenCalledWith({ body: { password: "secret" } });
});

it("opens the first page when there is nowhere to return to", async () => {
  vi.mocked(login).mockReturnValue(answer(204) as never);
  renderPage(<LoginPage />, { path: "/login", url: "/login", routes });

  submit("secret");

  expect(await screen.findByText("все пользователи")).toBeTruthy();
});

it("does not follow an address of another site", async () => {
  vi.mocked(login).mockReturnValue(answer(204) as never);
  renderPage(<LoginPage />, { path: "/login", url: "/login?next=%2F%2Fevil.example%2F", routes });

  submit("secret");

  expect(await screen.findByText("все пользователи")).toBeTruthy();
});

it("says that the password is wrong and stays on the page", async () => {
  vi.mocked(login).mockReturnValue(answer(401, "Wrong password") as never);
  renderPage(<LoginPage />, { path: "/login", url: "/login", routes });

  submit("guess");

  expect(await screen.findByText("Неверный пароль")).toBeTruthy();
  expect(screen.queryByText("все пользователи")).toBeNull();
  expect((screen.getByLabelText("Пароль") as HTMLInputElement).value).toBe("");
});

it("says how long to wait after too many wrong passwords", async () => {
  vi.mocked(login).mockReturnValue(answer(429, "Too many wrong passwords", { "Retry-After": "170" }) as never);
  renderPage(<LoginPage />, { path: "/login", url: "/login", routes });

  submit("guess");

  expect(await screen.findByText("Слишком много неверных паролей. Повторите через 3 мин.")).toBeTruthy();
});

it("shows the server's own words for any other failure", async () => {
  vi.mocked(login).mockReturnValue(answer(500, "boom") as never);
  renderPage(<LoginPage />, { path: "/login", url: "/login", routes });

  submit("secret");

  expect(await screen.findByText("boom")).toBeTruthy();
});

it("does not send a second request while the first is on its way", async () => {
  let finish: (value: unknown) => void = () => {};
  vi.mocked(login).mockReturnValue(new Promise((resolve) => (finish = resolve)) as never);
  renderPage(<LoginPage />, { path: "/login", url: "/login", routes });

  submit("secret");

  await waitFor(() => expect((screen.getByRole("button", { name: "Войти" }) as HTMLButtonElement).disabled).toBe(true));
  finish(await answer(204));
  expect(await screen.findByText("все пользователи")).toBeTruthy();
  expect(login).toHaveBeenCalledTimes(1);
});
