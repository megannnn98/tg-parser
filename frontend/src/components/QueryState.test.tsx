import { render, screen } from "@testing-library/react";
import type { UseQueryResult } from "@tanstack/react-query";
import { expect, it } from "vitest";

import { QueryState } from "@/components/QueryState";

function query(state: Partial<UseQueryResult<string>>) {
  return { isPending: false, isError: false, data: undefined, error: null, ...state } as UseQueryResult<string>;
}

it("replaces the content with the error when nothing was loaded", () => {
  render(<QueryState query={query({ isError: true, error: new Error("down") })}>{(data) => <p>{data}</p>}</QueryState>);

  expect(screen.getByText("down")).toBeTruthy();
});

it("keeps the loaded content when a refetch fails", () => {
  render(
    <QueryState query={query({ isError: true, error: new Error("down"), data: "loaded" })}>
      {(data) => <p>{data}</p>}
    </QueryState>
  );

  expect(screen.getByText("down")).toBeTruthy();
  expect(screen.getByText("loaded")).toBeTruthy();
});
