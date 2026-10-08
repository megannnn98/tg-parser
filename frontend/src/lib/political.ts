import type { PoliticalCoords } from "@/api/generated";

export const AXES = [
  { key: "economic", poles: ["Левая", "Правая"] },
  { key: "authority", poles: ["Свобода", "Авторитаризм"] },
  { key: "social", poles: ["Прогресс", "Консерватор"] },
  { key: "nationalism", poles: ["Космополит", "Национал"] },
  { key: "democracy", poles: ["Демократия", "Автократия"] },
  { key: "militarism", poles: ["Пацифизм", "Милитарист"] }
] as const;

/** The share of the axis's signals that lean to its left pole, 0 without signals. */
export function leftPercent(result: PoliticalCoords, axis: string): number {
  const stats = result.axes[axis];
  const total = stats ? stats.left_count + stats.right_count : 0;
  return total > 0 ? Math.round((stats.left_count / total) * 100) : 0;
}

export function politicalSummary(result: PoliticalCoords): string {
  const percent = result.total_messages > 0 ? Math.round((result.signal_count / result.total_messages) * 100) : 0;
  return `Итого: ${result.signal_count} из ${result.total_messages} сообщений (${percent}%) содержат политические сигналы.`;
}
