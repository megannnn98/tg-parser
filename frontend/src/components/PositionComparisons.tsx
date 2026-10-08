import { useEffect } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import {
  getPositionComparisons, startPositionAnalysis, type Comparison, type Evidence, type SimilarAuthor
} from "@/api/generated";
import { PositionAnalysisProgress } from "@/components/PositionAnalysisProgress";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { unwrap } from "@/lib/api";

const labels = {
  agreement: "Совпадение",
  partial: "Частичное совпадение",
  disagreement: "Расхождение",
  unclear: "Неясно — исключено из оценки"
};

function Statement({ value, label }: { value: Evidence | null; label: string }) {
  if (!value) return null;
  return (
    <div className="min-w-0 space-y-2 rounded-md border p-3">
      <p className="font-medium">{label}</p>
      <p className="text-sm">{value.position}</p>
      <blockquote className="whitespace-pre-wrap break-words border-l-2 pl-3 text-sm">
        {value.text}
      </blockquote>
      <p className="text-xs text-muted-foreground">
        {value.date} · @{value.channel.replace(/^@/, "")} · сообщение {value.message_id}
      </p>
      {value.quote ? <p className="text-sm text-muted-foreground">Подтверждение: «{value.quote}»</p> : null}
    </div>
  );
}

function SimilarPair({ value }: { value: SimilarAuthor }) {
  return <div className="space-y-3 rounded-md border p-4">
    <div className="flex flex-wrap justify-between gap-2">
      <Link className="font-medium text-primary hover:underline" to={`/users/${value.tg_id}`}>{value.display_username}</Link>
      <span className="tabular-nums">Косинусное сходство: {value.similarity.toLocaleString("ru-RU", { maximumFractionDigits: 4 })}</span>
    </div>
    <p className="text-sm text-muted-foreground">Уникальных комментариев: {value.left_comments} у выбранного автора, {value.right_comments} у сравниваемого.</p>
    <details>
      <summary className="cursor-pointer text-sm text-primary">Похожие высказывания: {value.display_username}</summary>
      <p className="my-3 text-sm text-muted-foreground">Примеры выбраны среди 32 наиболее типичных комментариев каждого автора.</p>
      <div className="space-y-4">{value.examples.map((match, i) => <div key={i} className="space-y-2">
        <p className="text-sm">Сходство текстов: {match.similarity.toLocaleString("ru-RU", { maximumFractionDigits: 4 })}</p>
        <div className="grid gap-3 lg:grid-cols-2">
          <Statement value={match.left} label="Выбранный автор" />
          <Statement value={match.right} label={value.display_username} />
        </div>
      </div>)}</div>
    </details>
  </div>;
}

function Pair({ value }: { value: Comparison }) {
  return (
    <div className="space-y-3 rounded-md border p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <Link className="font-medium text-primary hover:underline" to={`/users/${value.tg_id}`}>
          {value.display_username}
        </Link>
        <span className="font-semibold tabular-nums">
          {value.score === null ? "Нет оценки" : `${value.score.toLocaleString("ru-RU")} из 100`}
        </span>
      </div>
      <p className="text-sm text-muted-foreground">
        {value.comparable_questions} сравнимых вопросов · {value.agreements} совпадений ·{" "}
        {value.partial} частичных · {value.disagreements} расхождений
      </p>
      <details>
        <summary className="cursor-pointer text-sm text-primary">
          Подробнее о сравнении с {value.display_username}
        </summary>
        <div className="mt-4 space-y-5">
          {value.questions.length === 0 ? (
            <p className="text-sm text-muted-foreground">Общие ясно выраженные позиции не найдены.</p>
          ) : value.questions.map((question, index) => (
            <div key={index} className="space-y-3">
              <h4 className="font-medium">{question.question}</h4>
              <p className="text-sm">{labels[question.result]}: {question.explanation}</p>
              <div className="grid gap-3 lg:grid-cols-2">
                <Statement value={question.left} label="Выбранный автор" />
                <Statement value={question.right} label={value.display_username} />
              </div>
            </div>
          ))}
        </div>
      </details>
    </div>
  );
}

export function PositionComparisons({ tgId, hasComments = true }: { tgId: number; hasComments?: boolean }) {
  const results = useQuery({
    queryKey: ["position-comparisons", tgId],
    queryFn: () => unwrap(getPositionComparisons({ path: { tg_id: tgId } })),
    refetchInterval: (query) => query.state.data?.progress.state === "running" ? 2000 : false,
    retry: 2
  });
  const start = useMutation({
    mutationFn: () => unwrap(startPositionAnalysis()),
    onSuccess: () => { void results.refetch(); }
  });
  const reset = start.reset;
  useEffect(() => reset(), [tgId, reset]);
  const value = results.data;
  const local = value?.method === "text_similarity";
  const running = start.isPending || value?.progress.state === "running";
  const resume = value?.progress.state === "error" || value?.progress.state === "interrupted";
  const providerChanged = value?.progress.state === "provider_changed";

  return (
    <Card>
      <CardHeader><CardTitle>{local ? "Похожие высказывания авторов" : "Близкие политические позиции"}</CardTitle></CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <p className="text-sm text-muted-foreground">
            {local ? "Локальные эмбеддинги всех сохранённых комментариев. Рейтинг по сходству средних векторов авторов; политическое согласие не определяется." : "Согласие по конкретным вопросам среди собранных авторов. Для рейтинга нужны минимум 3 вопроса."}
          </p>
          <Button variant="outline" disabled={running || providerChanged || !hasComments || results.isPending || results.isError} onClick={() => start.mutate()}>
            {running ? "Анализ выполняется…" : resume ? "Продолжить анализ" :
              value?.needs_update ? "Обновить сравнение" : local ? "Создать эмбеддинги и сравнить" : "Найти близкие позиции"}
          </Button>
        </div>
        {!hasComments ? <p className="text-sm text-muted-foreground">Для сравнения нужны комментарии автора.</p> : null}
        {results.isPending ? <p role="status">Загрузка сравнения…</p> : null}
        {results.isError ? (
          <div role="alert">
            <p className="text-sm text-destructive">{results.error.message}</p>
            <Button variant="outline" onClick={() => { void results.refetch(); }}>Повторить загрузку</Button>
          </div>
        ) : null}
        {start.isError ? <p role="alert" className="text-sm text-destructive">{start.error.message}</p> : null}
        {value?.needs_update ? (
          <p className="text-sm text-muted-foreground">
            {value.incomplete ? "Требуется обновление: показаны предварительные сравнения." :
              "Требуется обновление: показан ранее сохранённый результат."}
          </p>
        ) : null}
        {value ? <PositionAnalysisProgress progress={value.progress} embeddings={value.embeddings} local={local} /> : null}
        {value?.progress.error ? <p role="alert" className="text-sm text-destructive">{value.progress.error}</p> : null}
        {value?.incomplete ? (
          <p className="text-sm text-muted-foreground">Неполный результат — завершённый рейтинг пока недоступен.</p>
        ) : null}
        {value && value.ranking.length > 0 ? (
          <div className="space-y-3">
            <h3 className="font-medium">Ближайшие авторы</h3>
            {value.ranking.map(pair => <Pair key={pair.tg_id} value={pair} />)}
          </div>
        ) : null}
        {local && value.similar_authors.length > 0 ? <div className="space-y-3">
          <h3 className="font-medium">{value.incomplete ? "Предварительное сходство авторов" : "Ближайшие авторы по текстам"}</h3>
          {value.similar_authors.map(pair => <SimilarPair key={pair.tg_id} value={pair} />)}
        </div> : null}
        {local && value.progress.state === "done" && value.similar_authors.length === 0 ? <p className="text-sm text-muted-foreground">Для сравнения нужны непустые комментарии хотя бы двух авторов.</p> : null}
        {!local && value?.progress.state === "done" && !value.incomplete && value.ranking.length === 0 ? (
          <p className="text-sm text-muted-foreground">Нет авторов с достаточным количеством сравнимых вопросов.</p>
        ) : null}
        {value && value.insufficient.length > 0 ? (
          <div className="space-y-3">
            <h3 className="font-medium">Недостаточно данных для рейтинга</h3>
            {value.insufficient.map(pair => <Pair key={pair.tg_id} value={pair} />)}
          </div>
        ) : null}
        {value && value.partial_results.length > 0 ? (
          <div className="space-y-3">
            <h3 className="font-medium">Предварительные сравнения</h3>
            {value.partial_results.map(pair => <Pair key={pair.tg_id} value={pair} />)}
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}
