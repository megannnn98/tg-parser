import { describe, expect, it } from "vitest";

import type { JobStatus } from "@/api/generated";
import { describeUser, failedChannels, progressOf } from "@/lib/collect";

function job(overrides: Partial<JobStatus> = {}): JobStatus {
  return {
    job_id: "j",
    user_ref: "@vasya",
    total_channels: 4,
    state: "running",
    resolved: null,
    channels: [],
    saved_total: 0,
    db_name: null,
    error: null,
    ...overrides
  };
}

describe("describeUser", () => {
  it("says the user is being resolved until Telegram answers", () => {
    expect(describeUser(null)).toBe("Резолвим пользователя…");
  });

  it("prefers the username, then the name, then the id", () => {
    expect(describeUser({ tg_id: 7, username: "vasya", display_name: "Вася" })).toBe("Пользователь: @vasya");
    expect(describeUser({ tg_id: 7, username: null, display_name: "Вася" })).toBe("Пользователь: Вася");
    expect(describeUser({ tg_id: 7, username: null, display_name: null })).toBe("Пользователь: 7");
  });
});

describe("progressOf", () => {
  it("shows the channel being read as an indeterminate bar", () => {
    const progress = progressOf(
      job({
        channels: [
          { channel: "a", status: "done", saved: 1, error: null },
          { channel: "b", status: "started", saved: 0, error: null }
        ]
      })
    );

    expect(progress.currentLabel).toBe("Читаю канал: b");
    expect(progress.indeterminate).toBe(true);
    expect(progress.overallLabel).toBe("1 из 4 каналов");
    expect(progress.overallPercent).toBe(25);
  });

  it("fills the top bar only when the job is done, not on an error", () => {
    expect(progressOf(job({ state: "done" }))).toMatchObject({ currentLabel: "Готово", currentPercent: 100 });
    expect(progressOf(job({ state: "error" }))).toMatchObject({ currentLabel: "", currentPercent: 0 });
  });

  it("counts channels without a total when the list was empty", () => {
    const progress = progressOf(
      job({ total_channels: 0, channels: [{ channel: "a", status: "done", saved: 0, error: null }] })
    );

    expect(progress.overallLabel).toBe("1 из 1 каналов");
    expect(progressOf(job({ total_channels: 0 }))).toMatchObject({ overallLabel: "0 каналов", overallPercent: 0 });
  });
});

it("lists the failed channels with their errors", () => {
  expect(
    failedChannels([
      { channel: "a", status: "failed", saved: 0, error: "private" },
      { channel: "b", status: "failed", saved: 0, error: null },
      { channel: "c", status: "done", saved: 3, error: null }
    ])
  ).toEqual(["a — private", "b"]);
});
