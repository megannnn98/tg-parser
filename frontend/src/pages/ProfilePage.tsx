import { useEffect } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate, useParams } from "react-router-dom";

import { analyzePolitical, getUser, type Profile } from "@/api/generated";
import { DailyChart, HourlyChart, WeekHeatmap } from "@/components/ActivityCharts";
import { ChannelShares } from "@/components/ChannelShares";
import { CollectProgress } from "@/components/CollectProgress";
import { PoliticalBars } from "@/components/PoliticalBars";
import { PositionComparisons } from "@/components/PositionComparisons";
import { QueryState } from "@/components/QueryState";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { useCollectJob } from "@/hooks/useCollectJob";
import { unwrap } from "@/lib/api";

/** What the collector is asked for on a refresh: the username, or the id without one. */
function userRef(profile: Profile): string {
  return profile.username ? `@${profile.username}` : String(profile.tg_id);
}

export function ProfilePage() {
  const { dbName = "" } = useParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  const user = useQuery({
    queryKey: ["user", dbName],
    queryFn: () => unwrap(getUser({ path: { db_name: dbName } }))
  });

  const political = useMutation({
    mutationFn: () => unwrap(analyzePolitical({ path: { db_name: dbName } }))
  });
  const resetPolitical = political.reset;
  // The result is of the comments it was made from: another user's page drops it.
  useEffect(() => resetPolitical(), [dbName, resetPolitical]);

  const collect = useCollectJob((job) => {
    // New comments make the political result stale; the page stays mounted.
    political.reset();
    void queryClient.invalidateQueries({ queryKey: ["user"] });
    void queryClient.invalidateQueries({ queryKey: ["profiles"] });
    void queryClient.invalidateQueries({ queryKey: ["position-comparisons"] });
    navigate(`/users/${encodeURIComponent(job.db_name!)}`);
  });

  return (
    <QueryState query={user}>
      {({ profile, hourly_activity, daily_activity, weekly_activity }) => (
        <div className="space-y-6">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <Link to="/" className="text-sm text-primary hover:underline">
              ← все пользователи
            </Link>
            <div className="flex flex-wrap gap-2">
              <Button variant="outline" asChild>
                <a href={`/api/v1/users/${encodeURIComponent(dbName)}/comments.txt`}>Скачать .txt</a>
              </Button>
              <Button variant="outline" onClick={() => political.mutate()} disabled={political.isPending || profile.total_messages === 0}>
                {political.isPending ? "Анализирую…" : "Определить полит взгляды"}
              </Button>
              {collect.running ? (
                <Button variant="outline" onClick={collect.cancel} disabled={!collect.canCancel}>
                  Остановить
                </Button>
              ) : (
                <Button variant="outline" onClick={() => collect.start(userRef(profile))}>
                  Обновить комментарии
                </Button>
              )}
            </div>
          </div>

          {collect.running || collect.job ? (
            <Card>
              <CardContent>
                <CollectProgress job={collect.job} starting={collect.starting} />
              </CardContent>
            </Card>
          ) : null}
          {collect.error ? <p className="text-sm text-destructive">{collect.error}</p> : null}

          {political.isSuccess ? (
            <Card>
              <CardHeader>
                <CardTitle>Политические координаты</CardTitle>
              </CardHeader>
              <CardContent>
                <PoliticalBars result={political.data} />
              </CardContent>
            </Card>
          ) : null}
          {political.isError ? <p className="text-sm text-destructive">{political.error.message}</p> : null}

          <Card>
            <CardContent className="flex flex-wrap items-center justify-between gap-4">
              <div>
                <h1 className="text-2xl font-semibold tracking-tight break-all">{profile.display_username}</h1>
                <p className="text-sm text-muted-foreground">id: {profile.tg_id}</p>
              </div>
              <div className="flex gap-6" aria-label="Сводка">
                <div>
                  <strong className="block text-2xl tabular-nums">{profile.total_messages}</strong>
                  <span className="text-sm text-muted-foreground">сообщений</span>
                </div>
                <div>
                  <strong className="block text-2xl tabular-nums">{profile.channel_count}</strong>
                  <span className="text-sm text-muted-foreground">каналов</span>
                </div>
              </div>
            </CardContent>
          </Card>

          {profile.total_messages === 0 ? (
            <p className="text-sm text-muted-foreground">Комментарии в выбранных каналах не найдены.</p>
          ) : null}

          <PositionComparisons dbName={dbName} hasComments={profile.total_messages > 0} />

          <Card>
            <CardContent>
              <ChannelShares profile={profile} />
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Активность</CardTitle>
            </CardHeader>
            <CardContent className="grid gap-6 xl:grid-cols-2">
              <div className="min-w-0 space-y-2">
                <h3 className="font-medium">По часам</h3>
                <HourlyChart hours={hourly_activity} />
              </div>
              <div className="min-w-0 space-y-2">
                <h3 className="font-medium">По дням</h3>
                <DailyChart key={dbName} days={daily_activity} />
              </div>
              <div className="min-w-0 space-y-2">
                <h3 className="font-medium">По дням недели и часам</h3>
                <WeekHeatmap cells={weekly_activity} />
              </div>
            </CardContent>
          </Card>
        </div>
      )}
    </QueryState>
  );
}
