export function channelLines(text: string): string[] {
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean);
}

/** The question to ask before a save that empties the list or cuts it by more than half
 * (a select-all + delete without the paste back), or null when the save is safe. */
export function shrinkQuestion(savedCount: number, newCount: number): string | null {
  if (newCount === 0) {
    return "Список каналов станет пустым. Сохранить?";
  }
  if (savedCount > 0 && newCount < savedCount * 0.5) {
    return `Список уменьшится с ${savedCount} до ${newCount} каналов. Сохранить?`;
  }
  return null;
}
