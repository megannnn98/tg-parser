import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { ListIcon } from "lucide-react";
import { Link, NavLink } from "react-router-dom";

import { listProfiles } from "@/api/generated";
import { ChannelsSheet } from "@/components/ChannelsSheet";
import { LogoutButton } from "@/components/LogoutButton";
import { UserAvatar } from "@/components/UserAvatar";
import { unwrap } from "@/lib/api";
import { matchesProfile } from "@/lib/profiles";
import { cn } from "@/lib/utils";

const footerButton =
  "flex w-full items-center gap-2 rounded-md px-3 py-2 text-left text-sm text-sidebar-foreground hover:bg-sidebar-hover";

/** The users to choose from, on every page; the channel list and the logout below them. */
export function Sidebar() {
  const [search, setSearch] = useState("");
  const profiles = useQuery({ queryKey: ["profiles"], queryFn: () => unwrap(listProfiles()) });
  const found = (profiles.data ?? []).filter((profile) => matchesProfile(profile, search));

  return (
    <aside className="flex shrink-0 flex-col bg-sidebar text-sidebar-foreground lg:sticky lg:top-0 lg:h-screen lg:w-72">
      <Link to="/" className="px-4 pt-5 pb-3 text-base font-semibold tracking-tight">
        Telegram-комментарии
      </Link>

      <div className="px-3 pb-2">
        <input
          type="search"
          aria-label="Поиск пользователя"
          placeholder="Поиск: ник, имя или id"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
          className="h-9 w-full rounded-md border border-sidebar-hover bg-sidebar-hover/60 px-3 text-sm text-sidebar-foreground placeholder:text-sidebar-muted focus-visible:ring-2 focus-visible:ring-ring focus-visible:outline-none"
        />
      </div>

      <nav aria-label="Пользователи" className="max-h-64 flex-1 overflow-y-auto px-2 lg:max-h-none">
        {profiles.isError ? <p className="px-2 py-2 text-sm text-sidebar-muted">{profiles.error.message}</p> : null}
        {profiles.data && found.length === 0 ? (
          <p className="px-2 py-2 text-sm text-sidebar-muted">
            {profiles.data.length === 0 ? "Пользователей пока нет." : "Никого не найдено."}
          </p>
        ) : null}
        <ul className="space-y-0.5">
          {found.map((profile) => (
            <li key={profile.tg_id}>
              <NavLink
                to={`/users/${profile.tg_id}`}
                className={({ isActive }) =>
                  cn(
                    "flex items-center gap-3 rounded-md px-2 py-1.5 hover:bg-sidebar-hover",
                    isActive && "bg-sidebar-hover"
                  )
                }
              >
                <UserAvatar profile={profile} className="size-8" />
                <span className="min-w-0">
                  <span className="block truncate text-sm font-medium">{profile.display_username}</span>
                  <span className="block truncate text-xs text-sidebar-muted">
                    {profile.total_messages.toLocaleString("ru-RU")} сообщ.
                  </span>
                </span>
              </NavLink>
            </li>
          ))}
        </ul>
      </nav>

      <div className="space-y-0.5 border-t border-sidebar-hover p-2">
        <ChannelsSheet
          trigger={(count) => (
            <button type="button" className={footerButton}>
              <ListIcon className="size-4" />
              Каналы{count === undefined ? "" : ` · ${count}`}
            </button>
          )}
        />
        <LogoutButton className={footerButton} />
      </div>
    </aside>
  );
}
