import { type FormEvent, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "react-router-dom";

import { getChannels, listProfiles } from "@/api/generated";
import { ChannelsEditor } from "@/components/ChannelsEditor";
import { CollectProgress } from "@/components/CollectProgress";
import { PageHeader } from "@/components/layout/PageHeader";
import { isEmptyList, QueryState } from "@/components/QueryState";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
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

  return (
    <>
      <PageHeader title="Пользователи" />

      <div className="mb-6 grid gap-4 lg:grid-cols-2">
        <Card>
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

        <Card>
          <CardHeader>
            <CardTitle>Список каналов</CardTitle>
          </CardHeader>
          <CardContent>
            <QueryState query={channels}>{(data) => <ChannelsEditor saved={data.channels} />}</QueryState>
          </CardContent>
        </Card>
      </div>

      <QueryState
        query={profiles}
        isEmpty={isEmptyList}
        empty="Комментарии пока не скачаны ни для одного пользователя."
      >
        {(data) => (
          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
            {data.map((profile) => (
              <Link
                key={profile.tg_id}
                to={`/users/${profile.tg_id}`}
                className="rounded-lg border bg-card p-4 transition-colors hover:border-primary"
              >
                <p className="font-semibold">{profile.display_username}</p>
                <p className="text-sm text-muted-foreground">id: {profile.tg_id}</p>
                <p className="text-sm text-muted-foreground">
                  {profile.total_messages} сообщений, {profile.channel_count} каналов
                </p>
              </Link>
            ))}
          </div>
        )}
      </QueryState>
    </>
  );
}
