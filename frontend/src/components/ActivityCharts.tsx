import { type KeyboardEvent, useLayoutEffect, useRef, useState } from "react";

import type { DayCount, HourCount } from "@/api/generated";
import { BASELINE_Y, barHeight, DAILY_BAR_WIDTH, dailyLayout, PLOT_LEFT, scaleMax, ticks } from "@/lib/charts";
import { cn } from "@/lib/utils";

const AXIS_COLOR = "#667085";
const GRID_COLOR = "#d9dee8";
const HOUR_STEP = 27;

function YAxis({ max }: { max: number }) {
  return (
    <>
      {ticks(max).map((tick) => (
        <text key={tick.y} x="40" y={tick.y} fill={AXIS_COLOR} fontSize="11" textAnchor="end">
          {tick.label}
        </text>
      ))}
      <text x="15" y="155" fill={AXIS_COLOR} fontSize="9" textAnchor="middle" transform="rotate(-90, 15, 155)">
        сообщ.
      </text>
    </>
  );
}

export function HourlyChart({ hours }: { hours: HourCount[] }) {
  const max = scaleMax(hours.map((hour) => hour.count));
  return (
    <svg className="w-full" viewBox="0 0 720 280" role="img" aria-label="График активности по часам">
      {hours.map((hour, index) => {
        const height = barHeight(hour.count, max);
        return (
          <rect
            key={hour.hour}
            x={PLOT_LEFT + index * HOUR_STEP}
            y={BASELINE_Y - height}
            width="25"
            height={height}
            rx="2"
            className="fill-primary"
          >
            <title>{`${hour.count} сообщений в ${hour.hour}:00`}</title>
          </rect>
        );
      })}
      <line x1="48" y1={BASELINE_Y} x2="708" y2={BASELINE_Y} stroke={GRID_COLOR} />
      <YAxis max={max} />
      {[0, 6, 12, 18, 23].map((hour) => (
        <text
          key={hour}
          x={PLOT_LEFT + hour * HOUR_STEP + 12}
          y="268"
          fill={AXIS_COLOR}
          fontSize="10"
          textAnchor="middle"
        >
          {hour}
        </text>
      ))}
      <text x="524" y="276" fill={AXIS_COLOR} fontSize="9" textAnchor="middle">
        час
      </text>
    </svg>
  );
}

/** Messages per day, scrolled to the latest days; a bar, clicked or Enter/Space on it,
 * shows its date and count below. Until one is chosen, the latest day is. */
export function DailyChart({ days }: { days: DayCount[] }) {
  const layout = dailyLayout(days);
  // A date, not an index: a refetch can add days before the chosen one.
  const [chosen, setChosen] = useState<string | null>(null);
  const current = days.find((day) => day.date === chosen) ?? days.at(-1);
  const scroll = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    if (scroll.current) {
      scroll.current.scrollLeft = scroll.current.scrollWidth;
    }
  }, [days]);

  function onKeyDown(event: KeyboardEvent, date: string) {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      setChosen(date);
    }
  }

  return (
    <>
      <div ref={scroll} className="overflow-x-auto overscroll-x-contain pb-2" data-testid="daily-scroll">
        <svg
          viewBox={`0 0 ${layout.width} 280`}
          style={{ width: `max(100%, ${layout.width}px)` }}
          className="block h-auto"
          role="img"
          aria-label="График активности по дням"
        >
          {layout.bars.map((bar) => (
            <g key={bar.date}>
              {bar.year ? (
                <>
                  <line x1={bar.x} y1="32" x2={bar.x} y2={BASELINE_Y} stroke={GRID_COLOR} strokeDasharray="5,3" />
                  <text x={bar.x + 4} y="26" fill={AXIS_COLOR} fontSize="11" fontWeight="700">
                    {bar.year}
                  </text>
                </>
              ) : null}
              {bar.monthLabel ? (
                <text x={bar.x + DAILY_BAR_WIDTH / 2} y="264" fill={AXIS_COLOR} fontSize="9" textAnchor="middle">
                  {bar.monthLabel}
                </text>
              ) : null}
              <rect
                x={bar.x}
                y={BASELINE_Y - bar.height}
                width={DAILY_BAR_WIDTH}
                height={bar.height}
                rx="2"
                role="button"
                tabIndex={0}
                aria-label={`${bar.date}: ${bar.count} сообщений`}
                aria-pressed={bar.date === current?.date}
                onClick={() => setChosen(bar.date)}
                onKeyDown={(event) => onKeyDown(event, bar.date)}
                className={cn(
                  "cursor-pointer fill-primary transition-[fill] outline-none hover:fill-accent-foreground focus-visible:fill-accent-foreground focus-visible:outline-2 focus-visible:outline-foreground",
                  bar.date === current?.date && "fill-accent-foreground stroke-foreground stroke-[1.5]"
                )}
              >
                <title>{`${bar.count} сообщений за ${bar.date}`}</title>
              </rect>
            </g>
          ))}
          <line x1="48" y1={BASELINE_Y} x2={layout.width - 10} y2={BASELINE_Y} stroke={GRID_COLOR} />
          <YAxis max={layout.max} />
        </svg>
      </div>
      <p className="min-h-6 text-sm font-medium" aria-live="polite">
        {current ? `${current.date}: ${current.count} сообщений` : "Нет сообщений по дням"}
      </p>
    </>
  );
}
