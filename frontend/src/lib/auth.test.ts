import { expect, it, vi } from "vitest";

import { loginAddress, safeNext, sendToLoginOn401 } from "@/lib/auth";

it("keeps a page of this site as the address to return to", () => {
  expect(safeNext("/users/7")).toBe("/users/7");
  expect(safeNext("/users/7?tab=search")).toBe("/users/7?tab=search");
});

it("does not return to another site, or to the login itself", () => {
  expect(safeNext(null)).toBe("/");
  expect(safeNext("")).toBe("/");
  expect(safeNext("https://evil.example/")).toBe("/");
  expect(safeNext("//evil.example/")).toBe("/");
  expect(safeNext("/\\evil.example/")).toBe("/");
  expect(safeNext("users/7")).toBe("/");
  expect(safeNext("/login")).toBe("/");
  expect(safeNext("/login?next=%2Fusers%2F7")).toBe("/");
});

it("names the page to return to in the login address", () => {
  expect(loginAddress("/users/7", "?tab=search")).toBe("/login?next=%2Fusers%2F7%3Ftab%3Dsearch");
  expect(loginAddress("/", "")).toBe("/login");
});

it("sends the browser to the login when the API says the session is gone", () => {
  const location = { pathname: "/users/7", search: "", assign: vi.fn() };
  const response = new Response(null, { status: 401 });

  expect(sendToLoginOn401(response, location)).toBe(response);

  expect(location.assign).toHaveBeenCalledWith("/login?next=%2Fusers%2F7");
});

it("leaves every other answer alone", () => {
  const location = { pathname: "/users/7", search: "", assign: vi.fn() };

  sendToLoginOn401(new Response(null, { status: 200 }), location);
  sendToLoginOn401(new Response(null, { status: 403 }), location);
  sendToLoginOn401(new Response(null, { status: 404 }), location);

  expect(location.assign).not.toHaveBeenCalled();
});

it("does not reload the login page for a wrong password", () => {
  const location = { pathname: "/login", search: "?next=%2Fusers%2F7", assign: vi.fn() };

  sendToLoginOn401(new Response(null, { status: 401 }), location);

  expect(location.assign).not.toHaveBeenCalled();
});
