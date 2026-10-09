type Named = { tg_id: number; display_name: string | null; display_username: string };

/** Two letters for the avatar circle: of the name's words, else of the username. */
export function initials(profile: Named): string {
  const name = profile.display_name?.trim() || profile.display_username;
  const words = name.split(/[^\p{L}\p{N}]+/u).filter(Boolean);
  const letters = words.length > 1 ? words[0][0] + words[1][0] : (words[0] ?? "").slice(0, 2);
  return letters.toUpperCase() || "?";
}

/** The avatar's colour, a hue of 0..359 that depends on the id alone. */
export function avatarHue(tgId: number): number {
  return (Math.abs(tgId) * 47) % 360;
}

/** Whether `query` is a part of the username, the name or the id. */
export function matchesProfile(profile: Named, query: string): boolean {
  const wanted = query.trim().toLowerCase();
  if (!wanted) {
    return true;
  }
  return [profile.display_username, profile.display_name ?? "", String(profile.tg_id)].some((value) =>
    value.toLowerCase().includes(wanted)
  );
}
