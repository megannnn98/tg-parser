import type { DayCount } from "@/api/generated";

/** The plot area of both bar charts, in SVG units: bars stand on BASELINE_Y. */
export const BASELINE_Y = 250;
export const PLOT_LEFT = 50;
const PLOT_HEIGHT = 190;

export const DAILY_BAR_WIDTH = 18;
const DAILY_STEP = DAILY_BAR_WIDTH + 8;
const DAILY_MIN_WIDTH = 680;

/** The count at the top of the scale; 1 when there is nothing, so bars stay at 0. */
export function scaleMax(counts: number[]): number {
  return Math.max(1, ...counts);
}

export function barHeight(count: number, max: number): number {
  return Math.trunc((count / max) * PLOT_HEIGHT);
}

export function ticks(max: number): { label: number; y: number }[] {
  return [
    { label: 0, y: 254 },
    { label: Math.trunc(max * 0.25), y: 202 },
    { label: Math.trunc(max * 0.5), y: 155 },
    { label: Math.trunc(max * 0.75), y: 107 },
    { label: max, y: 60 }
  ];
}

export type DailyBar = DayCount & {
  x: number;
  height: number;
  /** The year, on the first day of a year in the list. */
  year: string | null;
  /** "MM-DD", on the first day of a month in the list. */
  monthLabel: string | null;
};

export function dailyLayout(days: DayCount[]): { width: number; max: number; bars: DailyBar[] } {
  const max = scaleMax(days.map((day) => day.count));
  const bars = days.map((day, index) => {
    const previous = index > 0 ? days[index - 1].date : "";
    return {
      ...day,
      x: PLOT_LEFT + index * DAILY_STEP,
      height: barHeight(day.count, max),
      year: day.date.slice(0, 4) !== previous.slice(0, 4) ? day.date.slice(0, 4) : null,
      monthLabel: day.date.slice(0, 7) !== previous.slice(0, 7) ? day.date.slice(5, 10) : null
    };
  });
  return { width: Math.max(DAILY_MIN_WIDTH, 62 + days.length * DAILY_STEP + 40), max, bars };
}
