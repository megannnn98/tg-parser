import { describe, expect, it } from "vitest";

import { barHeight, dailyLayout, scaleMax, ticks } from "@/lib/charts";

describe("scale", () => {
  it("is the largest count, never zero", () => {
    expect(scaleMax([3, 8, 1])).toBe(8);
    expect(scaleMax([0, 0])).toBe(1);
    expect(scaleMax([])).toBe(1);
  });

  it("gives whole-pixel bars and whole ticks", () => {
    expect(barHeight(1, 3)).toBe(63);
    expect(ticks(10).map((tick) => tick.label)).toEqual([0, 2, 5, 7, 10]);
  });
});

describe("dailyLayout", () => {
  const days = [
    { date: "2025-12-30", count: 1 },
    { date: "2025-12-31", count: 2 },
    { date: "2026-01-01", count: 4 }
  ];

  it("is at least 680 wide and grows with the number of days", () => {
    expect(dailyLayout(days).width).toBe(680);
    expect(dailyLayout(Array.from({ length: 40 }, (_, i) => ({ date: `2026-01-${i}`, count: 1 }))).width).toBe(1142);
  });

  it("marks the first day of each year and of each month", () => {
    const bars = dailyLayout(days).bars;

    expect(bars.map((bar) => bar.year)).toEqual(["2025", null, "2026"]);
    expect(bars.map((bar) => bar.monthLabel)).toEqual(["12-30", null, "01-01"]);
    expect(bars.map((bar) => bar.x)).toEqual([50, 76, 102]);
    expect(bars.map((bar) => bar.height)).toEqual([47, 95, 190]);
  });
});
