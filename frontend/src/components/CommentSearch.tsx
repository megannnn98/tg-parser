import { type FormEvent, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { searchUserComments } from "@/api/generated";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { unwrap } from "@/lib/api";

/** "2026-01-05T10:00:00Z" as "2026-01-05 10:00": the stored instant, in UTC. */
function shortDate(date: string): string {
  return date.slice(0, 16).replace("T", " ");
}

/** Search by meaning over one user's comments. The results are the comments
 * themselves; their chunks only help to rank them. */
export function CommentSearch({ tgId }: { tgId: number }) {
  const [text, setText] = useState("");
  // What was asked last; the field may already hold the next query.
  const [query, setQuery] = useState("");

  const search = useQuery({
    queryKey: ["comment-search", tgId, query],
    queryFn: () => unwrap(searchUserComments({ path: { tg_id: tgId }, query: { q: query, limit: 20 } })),
    enabled: query !== ""
  });

  function onSubmit(event: FormEvent) {
    event.preventDefault();
    setQuery(text.trim());
  }

  const data = search.data;
  return (
    <Card>
      <CardHeader>
        <CardTitle>Поиск по смыслу</CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <form onSubmit={onSubmit} className="flex flex-wrap items-end gap-2">
          <div className="min-w-0 flex-1 space-y-1">
            <Label htmlFor="comment-search">Запрос</Label>
            <Input
              id="comment-search"
              value={text}
              onChange={(event) => setText(event.target.value)}
              placeholder="например: отношение к профсоюзам"
              maxLength={500}
            />
          </div>
          <Button type="submit" disabled={text.trim() === "" || search.isFetching}>
            Найти
          </Button>
        </form>

        {search.isFetching ? <p className="text-sm text-muted-foreground">Поиск…</p> : null}
        {search.isError ? <p className="text-sm text-destructive">{search.error.message}</p> : null}

        {data && !search.isFetching ? (
          data.indexed_messages === 0 ? (
            <p className="text-sm text-muted-foreground">
              Для комментариев этого пользователя ещё не построены эмбеддинги.
            </p>
          ) : (
            <>
              {data.indexed_messages < data.total_messages ? (
                <p className="text-sm text-muted-foreground">
                  Поиск идёт по {data.indexed_messages} из {data.total_messages} комментариев: для остальных
                  эмбеддинги ещё не построены.
                </p>
              ) : null}
              {data.results.length === 0 ? (
                <p className="text-sm text-muted-foreground">Ничего не найдено.</p>
              ) : (
                <ol className="space-y-3" aria-label="Результаты поиска">
                  {data.results.map((hit) => (
                    <li key={hit.message_id} className="rounded-lg border p-3">
                      <p className="text-xs text-muted-foreground">
                        {hit.channel} · {shortDate(hit.date)} ·{" "}
                        <span title="Итоговая оценка: сходство сообщения и его окружения с запросом">
                          {hit.score.toFixed(2)}
                        </span>
                      </p>
                      <p className="text-sm break-words whitespace-pre-wrap">{hit.text}</p>
                    </li>
                  ))}
                </ol>
              )}
            </>
          )
        ) : null}
      </CardContent>
    </Card>
  );
}
