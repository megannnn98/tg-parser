import { type FormEvent, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";

import { getChannels, listProfiles } from "@/api/generated";
import { CollectProgress } from "@/components/CollectProgress";
import { PageHeader } from "@/components/layout/PageHeader";
import { isEmptyList, QueryState } from "@/components/QueryState";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { UserAvatar } from "@/components/UserAvatar";
import { Input } from "@/components/ui/input";
import { useCollectJob } from "@/hooks/useCollectJob";
import { unwrap } from "@/lib/api";

export function UsersPage() {
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [username, setUsername] = useState("");

  const profiles = useQuery({ queryKey: ["profiles"], queryFn: () => unwrap(listProfiles()) });
  const channels = useQuery({ queryKey: ["channels"], queryFn: () => unwrap(getChannels()) });

  const collect = useCollectJob((job) => {
    void queryClient.invalidateQueries({ queryKey: ["profiles"] });
    navigate(`/users/${job.tg_id!}`);
  });

  function onSubmit(event: FormEvent) {
    event.preventDefault();
    collect.start(username);
  }

  const totals = profiles.data
    ? [
        { label: "профилей", value: profiles.data.length },
        { label: "комментариев", value: profiles.data.reduce((sum, profile) => sum + profile.total_messages, 0) },
        { label: "каналов в списке", value: channels.data?.channels.length }
      ]
    : [];

  return (
    <>
      <PageHeader title="Пользователи" />

      {totals.length > 0 ? (
        <dl className="mb-4 grid grid-cols-3 gap-3">
          {totals.map((total) => (
            <div key={total.label} className="rounded-lg border bg-card px-4 py-3">
              <dd className="text-2xl font-semibold tracking-tight tabular-nums">
                {total.value === undefined ? "—" : total.value.toLocaleString("ru-RU")}
              </dd>
              <dt className="text-sm text-muted-foreground">{total.label}</dt>
            </div>
          ))}
        </dl>
      ) : null}

      <Card className="mb-6">
        <CardHeader>
          <CardTitle>Скачать комментарии по юзернейму</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <form className="flex gap-2" onSubmit={onSubmit}>
            <Input
              aria-label="Юзернейм"
              placeholder="@username или tg_id"
              autoComplete="off"
              required
              value={username}
              onChange={(event) => setUsername(event.target.value)}
            />
            <Button type="submit" disabled={collect.running}>
              Скачать
            </Button>
          </form>
          {collect.running || collect.job ? <CollectProgress job={collect.job} starting={collect.starting} /> : null}
          {collect.error ? <p className="text-sm text-destructive">{collect.error}</p> : null}
        </CardContent>
      </Card>

      <QueryState
        query={profiles}
        isEmpty={isEmptyList}
        empty="Комментарии пока не скачаны ни для одного пользователя."
      >
        {(data) => (
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            {data.map((profile) => (
              <Link
                key={profile.tg_id}
                to={`/users/${profile.tg_id}`}
                className="rounded-lg border bg-card p-4 transition-shadow hover:border-primary hover:shadow-md"
              >
                <div className="flex items-center gap-3">
                  <UserAvatar profile={profile} className="size-11 text-sm" />
                  <div className="min-w-0">
                    <p className="truncate font-semibold">{profile.display_username}</p>
                    <p className="truncate text-sm text-muted-foreground">id: {profile.tg_id}</p>
                  </div>
                </div>
                <p className="mt-3 text-sm text-muted-foreground">
                  {profile.total_messages} сообщений, {profile.channel_count} каналов
                </p>
                {/* Where the comments were written: one stripe per channel, as wide as its share. */}
                <div aria-hidden="true" className="mt-2 flex h-1.5 overflow-hidden rounded-full bg-muted">
                  {profile.channels.map((channel) => (
                    <span
                      key={channel.name}
                      title={`${channel.name}: ${channel.percent}%`}
                      style={{ width: `${channel.percent}%`, backgroundColor: channel.color }}
                    />
                  ))}
                </div>
              </Link>
            ))}
          </div>
        )}
      </QueryState>
    </>
  );
}
