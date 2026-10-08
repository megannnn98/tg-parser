import { fireEvent, render, screen } from "@testing-library/react";
import { expect, it } from "vitest";

import type { WeekHourCount } from "@/api/generated";
import { DailyChart, WeekHeatmap } from "@/components/ActivityCharts";

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

function week(counts: Record<string, number>): WeekHourCount[] {
  return Array.from({ length: 7 * 24 }, (_, index) => {
    const weekday = Math.floor(index / 24);
    const hour = index % 24;
    return { weekday, hour, count: counts[`${weekday}-${hour}`] ?? 0 };
  });
}

/** The data cells, by their tooltip text; legend swatches have no tooltip. */
function cells(container: HTMLElement): Map<string, Element> {
  return new Map(
    Array.from(container.querySelectorAll("rect > title"), (title) => [title.textContent ?? "", title.parentElement!])
  );
}

it("shades each weekday-hour cell by its share of the busiest one", () => {
  const { container } = render(<WeekHeatmap cells={week({ "0-14": 10, "2-9": 5 })} />);

  const byTitle = cells(container);
  const busiest = byTitle.get("Пн, 14:00 — 10 сообщений")!;
  const half = byTitle.get("Ср, 9:00 — 5 сообщений")!;
  const empty = byTitle.get("Вс, 23:00 — 0 сообщений")!;

  expect(busiest.getAttribute("fill-opacity")).toBe("1");
  expect(Number(half.getAttribute("fill-opacity"))).toBeGreaterThan(0);
  expect(Number(half.getAttribute("fill-opacity"))).toBeLessThan(1);
  expect(busiest.classList.contains("fill-primary")).toBe(true);
  expect(empty.classList.contains("fill-primary")).toBe(false);
});

it("draws all 168 cells even when nobody wrote anything", () => {
  const { container } = render(<WeekHeatmap cells={week({})} />);

  const all = Array.from(cells(container).values());
  expect(all).toHaveLength(7 * 24);
  expect(all.some((cell) => cell.classList.contains("fill-primary"))).toBe(false);
});
