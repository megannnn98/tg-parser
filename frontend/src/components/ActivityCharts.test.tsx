import { fireEvent, render, screen } from "@testing-library/react";
import { expect, it } from "vitest";

import { DailyChart } from "@/components/ActivityCharts";

const pressed = () => screen.getAllByRole("button").find((bar) => bar.getAttribute("aria-pressed") === "true");

it("keeps the latest day selected when an earlier day is added", () => {
  const { rerender } = render(
    <DailyChart
      days={[
        { date: "2026-08-01", count: 1 },
        { date: "2026-08-03", count: 1 }
      ]}
    />
  );

  rerender(
    <DailyChart
      days={[
        { date: "2026-08-01", count: 1 },
        { date: "2026-08-02", count: 4 },
        { date: "2026-08-03", count: 2 }
      ]}
    />
  );

  expect(pressed()?.getAttribute("aria-label")).toBe("2026-08-03: 2 сообщений");
});

it("keeps the clicked day selected when an earlier day is added", () => {
  const { rerender } = render(
    <DailyChart
      days={[
        { date: "2026-08-01", count: 1 },
        { date: "2026-08-03", count: 1 }
      ]}
    />
  );
  fireEvent.click(screen.getByRole("button", { name: "2026-08-01: 1 сообщений" }));

  rerender(
    <DailyChart
      days={[
        { date: "2026-07-31", count: 5 },
        { date: "2026-08-01", count: 1 },
        { date: "2026-08-03", count: 1 }
      ]}
    />
  );

  expect(pressed()?.getAttribute("aria-label")).toBe("2026-08-01: 1 сообщений");
  expect(screen.getByText("2026-08-01: 1 сообщений", { selector: "p" })).toBeTruthy();
});
