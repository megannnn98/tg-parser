import { expect, it } from "vitest";

import { leftPercent, politicalSummary } from "@/lib/political";

const result = {
  total_messages: 40,
  signal_count: 10,
  bars: "",
  axes: { economic: { left_count: 1, right_count: 2 } }
};

it("leans to the left pole by the share of left signals", () => {
  expect(leftPercent(result, "economic")).toBe(33);
  expect(leftPercent(result, "social")).toBe(0);
});

it("sums up the signals", () => {
  expect(politicalSummary(result)).toBe("Итого: 10 из 40 сообщений (25%) содержат политические сигналы.");
  expect(politicalSummary({ ...result, total_messages: 0, signal_count: 0 })).toContain("(0%)");
});
