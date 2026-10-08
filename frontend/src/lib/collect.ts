import type { ChannelStatus, JobStatus, ResolvedUser } from "@/api/generated";

export function describeUser(resolved: ResolvedUser | null): string {
  if (!resolved) {
    return "Резолвим пользователя…";
  }
  const name = resolved.username ? `@${resolved.username}` : (resolved.display_name ?? String(resolved.tg_id));
  return `Пользователь: ${name}`;
}

export type Progress = {
  currentLabel: string;
  /** A channel is being read: its size is unknown, so the bar only moves. */
  indeterminate: boolean;
  currentPercent: number;
  overallLabel: string;
  overallPercent: number;
};

export function progressOf(job: JobStatus): Progress {
  const current = job.channels.find((channel) => channel.status === "started");
  // Only "done" fills the top bar: on "error" a full bar would contradict the error.
  const done = job.state === "done";
  const total = job.total_channels || job.channels.length;
  const completed = job.channels.filter((channel) => channel.status !== "started").length;

  return {
    currentLabel: current ? `Читаю канал: ${current.channel}` : done ? "Готово" : "",
    indeterminate: current !== undefined,
    currentPercent: done ? 100 : 0,
    // total_channels is 0 when the job started with no channels: no "N of M" then.
    overallLabel: total > 0 ? `${completed} из ${total} каналов` : `${completed} каналов`,
    overallPercent: total > 0 ? Math.round((completed / total) * 100) : 0
  };
}

export function failedChannels(channels: ChannelStatus[]): string[] {
  return channels
    .filter((channel) => channel.status === "failed")
    .map((channel) => channel.channel + (channel.error ? ` — ${channel.error}` : ""));
}
