import { avatarHue, initials } from "@/lib/profiles";
import { cn } from "@/lib/utils";

type Props = {
  profile: { tg_id: number; display_name: string | null; display_username: string };
  className?: string;
};

/** Telegram photos are not collected: the circle shows initials on the user's own colour. */
export function UserAvatar({ profile, className }: Props) {
  return (
    <span
      aria-hidden="true"
      className={cn(
        "inline-flex size-9 shrink-0 items-center justify-center rounded-full text-xs font-semibold text-white",
        className
      )}
      style={{ backgroundColor: `hsl(${avatarHue(profile.tg_id)} 45% 42%)` }}
    >
      {initials(profile)}
    </span>
  );
}
