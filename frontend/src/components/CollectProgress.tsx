import type { JobStatus } from "@/api/generated";
import { describeUser, failedChannels, progressOf } from "@/lib/collect";
import { cn } from "@/lib/utils";

function Bar({ label, percent, indeterminate = false }: { label: string; percent: number; indeterminate?: boolean }) {
  return (
    <div className="space-y-1">
      <p className="min-h-5 text-sm text-muted-foreground">{label}</p>
      <div className="relative h-2 overflow-hidden rounded-full bg-muted">
        <div
          className={cn(
            "absolute inset-y-0 left-0 rounded-full bg-primary",
            indeterminate ? "w-[30%] animate-[bar-slide_1.2s_ease-in-out_infinite]" : "transition-[width] duration-300"
          )}
          style={indeterminate ? undefined : { width: `${percent}%` }}
        />
      </div>
    </div>
  );
}

/** The progress of one collection: who, the channel being read, how many are done. */
export function CollectProgress({ job, starting }: { job: JobStatus | undefined; starting: boolean }) {
  if (!job) {
    return <p className="text-sm">{starting ? "Запуск…" : "Резолвим пользователя…"}</p>;
  }
  const progress = progressOf(job);
  const failed = failedChannels(job.channels);
  return (
    <div className="space-y-3" aria-live="polite">
      <p className="text-sm font-medium">
        {job.state === "done" ? `Готово: сохранено ${job.saved_total} новых сообщений` : describeUser(job.resolved)}
      </p>
      <Bar label={progress.currentLabel} percent={progress.currentPercent} indeterminate={progress.indeterminate} />
      <Bar label={progress.overallLabel} percent={progress.overallPercent} />
      {failed.length > 0 ? (
        <ul className="space-y-1 text-sm text-destructive">
          {failed.map((line) => (
            <li key={line}>{line}</li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}
