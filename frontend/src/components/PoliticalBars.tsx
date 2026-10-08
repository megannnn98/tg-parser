import type { PoliticalCoords } from "@/api/generated";
import { AXES, leftPercent, politicalSummary } from "@/lib/political";

/** Six axes, each a gradient between its poles with a thumb at the left share. */
export function PoliticalBars({ result }: { result: PoliticalCoords }) {
  return (
    <div className="space-y-3">
      {AXES.map((axis) => {
        const percent = leftPercent(result, axis.key);
        return (
          <div key={axis.key} className="grid grid-cols-[7rem_1fr_7rem] items-center gap-3 text-sm">
            <span className="text-right text-muted-foreground">{axis.poles[0]}</span>
            <div
              className="relative h-2 rounded-full bg-[linear-gradient(to_right,#2563eb,#9333ea_50%,#dc2626)]"
              role="meter"
              aria-label={`${axis.poles[0]} — ${axis.poles[1]}`}
              aria-valuenow={percent}
              aria-valuemin={0}
              aria-valuemax={100}
            >
              <span
                className="absolute top-1/2 size-5 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-white bg-foreground shadow"
                style={{ left: `${percent}%` }}
              />
            </div>
            <span className="text-muted-foreground">{axis.poles[1]}</span>
          </div>
        );
      })}
      <p className="text-sm text-muted-foreground">{politicalSummary(result)}</p>
    </div>
  );
}
