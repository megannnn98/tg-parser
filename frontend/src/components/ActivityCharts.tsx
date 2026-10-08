import { type KeyboardEvent, useLayoutEffect, useRef, useState } from "react";

import type { DayCount, HourCount, WeekHourCount } from "@/api/generated";
import {
  BASELINE_Y,
  barHeight,
  DAILY_BAR_WIDTH,
  dailyLayout,
  heatOpacity,
  PLOT_LEFT,
  scaleMax,
  ticks,
  WEEKDAYS
} from "@/lib/charts";
import { cn } from "@/lib/utils";

const AXIS_COLOR = "#667085";
const GRID_COLOR = "#d9dee8";
const HOUR_STEP = 27;
const HEAT_ROW_STEP = 24;
const HEAT_TOP = 8;
const HEAT_LEGEND = [0, 0.25, 0.5, 0.75, 1];

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

/** Messages by weekday and hour: the darker a cell, the closer it is to the busiest one. */
export function WeekHeatmap({ cells }: { cells: WeekHourCount[] }) {
  const max = scaleMax(cells.map((cell) => cell.count));
  const hoursY = HEAT_TOP + 7 * HEAT_ROW_STEP + 12;
  return (
    <svg className="w-full" viewBox="0 0 720 216" role="img" aria-label="Активность по дням недели и часам">
      {WEEKDAYS.map((day, weekday) => (
        <text
          key={day}
          x="40"
          y={HEAT_TOP + weekday * HEAT_ROW_STEP + 15}
          fill={AXIS_COLOR}
          fontSize="11"
          textAnchor="end"
        >
          {day}
        </text>
      ))}
      {cells.map((cell) => (
        <rect
          key={`${cell.weekday}-${cell.hour}`}
          x={PLOT_LEFT + cell.hour * HOUR_STEP}
          y={HEAT_TOP + cell.weekday * HEAT_ROW_STEP}
          width="25"
          height="22"
          rx="2"
          className={cell.count > 0 ? "fill-primary" : "fill-muted"}
          fillOpacity={cell.count > 0 ? heatOpacity(cell.count, max) : undefined}
        >
          <title>{`${WEEKDAYS[cell.weekday]}, ${cell.hour}:00 — ${cell.count} сообщений`}</title>
        </rect>
      ))}
      {[0, 6, 12, 18, 23].map((hour) => (
        <text
          key={hour}
          x={PLOT_LEFT + hour * HOUR_STEP + 12}
          y={hoursY}
          fill={AXIS_COLOR}
          fontSize="10"
          textAnchor="middle"
        >
          {hour}
        </text>
      ))}
      <text x="524" y={hoursY + 12} fill={AXIS_COLOR} fontSize="9" textAnchor="middle">
        час
      </text>
      <text x={PLOT_LEFT} y={hoursY + 14} fill={AXIS_COLOR} fontSize="9">
        меньше
      </text>
      {HEAT_LEGEND.map((share, index) => (
        <rect
          key={share}
          x={PLOT_LEFT + 38 + index * 14}
          y={hoursY + 5}
          width="12"
          height="12"
          rx="2"
          className={share > 0 ? "fill-primary" : "fill-muted"}
          fillOpacity={share > 0 ? heatOpacity(share, 1) : undefined}
        />
      ))}
      <text x={PLOT_LEFT + 38 + HEAT_LEGEND.length * 14 + 4} y={hoursY + 14} fill={AXIS_COLOR} fontSize="9">
        {`больше (до ${max})`}
      </text>
    </svg>
  );
}
