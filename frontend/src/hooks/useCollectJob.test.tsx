import { act, fireEvent, render, screen } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, beforeEach, expect, it, vi } from "vitest";

import { collectStatus, startCollect } from "@/api/generated";
import { MAX_POLL_FAILURES, useCollectJob } from "@/hooks/useCollectJob";
import { failed, ok } from "@/test/render";

vi.mock("@/api/generated", () => ({ startCollect: vi.fn(), collectStatus: vi.fn(), cancelCollect: vi.fn() }));

function Probe() {
  const collect = useCollectJob(() => {});
  return (
    <>
      <button onClick={() => collect.start("@vasya")}>start</button>
      <p>{collect.error ?? "no error"}</p>
    </>
  );
}

beforeEach(() => vi.useFakeTimers());
afterEach(() => vi.useRealTimers());

it("stops polling after the failures in a row run out", async () => {
  vi.mocked(startCollect).mockReset().mockReturnValue(ok({ job_id: "j1" }) as never);
  const running = {
    job_id: "j1",
    user_ref: "@vasya",
    total_channels: 1,
    state: "running",
    resolved: null,
    channels: [],
    saved_total: 0,
    db_name: null,
    error: null
  };
  vi.mocked(collectStatus)
    .mockReset()
    .mockReturnValueOnce(ok(running) as never)
    .mockImplementation(() => failed(503, "down") as never);

  render(
    <QueryClientProvider client={new QueryClient()}>
      <Probe />
    </QueryClientProvider>
  );
  fireEvent.click(screen.getByText("start"));
  for (let second = 0; second < 40; second += 1) {
    await act(() => vi.advanceTimersByTimeAsync(1000));
  }

  expect(collectStatus).toHaveBeenCalledTimes(1 + MAX_POLL_FAILURES);
  expect(screen.getByText("Не удалось получить статус сбора, попробуйте ещё раз")).toBeTruthy();
});
