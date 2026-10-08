import { type AnalysisProgress, type EmbeddingDetails } from "@/api/generated";

const stages = [
  { key: "comments", title: "Извлечение позиций", description: "DeepSeek через OpenRouter читает историю всех собранных авторов и выделяет собственные политические позиции. Эмбеддинги появятся на следующем этапе." },
  { key: "embeddings", title: "Создание эмбеддингов", description: "Локальная модель E5 преобразует формулировки политических вопросов в числовые векторы. При первом запуске загружаются веса модели." },
  { key: "questions", title: "Сопоставление вопросов", description: "Эмбеддинги находят похожие формулировки, а DeepSeek проверяет, действительно ли речь об одном политическом вопросе." },
  { key: "comparisons", title: "Сравнение позиций", description: "По общим вопросам сравниваются последние ясно выраженные позиции авторов. Завершённые пары формируют рейтинг." }
];
const localStages = [
  { key: "comments", title: "Чтение комментариев", description: "Читаются уже собранные тексты из локальных SQLite-баз всех авторов." },
  { key: "embeddings", title: "Создание эмбеддингов", description: "Локальная E5 кодирует сами комментарии. Длинные тексты обрабатываются частями. При первом запуске скачиваются веса; тексты остаются на вашем компьютере." },
  { key: "comparisons", title: "Сравнение авторов", description: "Сравниваются средние эмбеддинги уникальных комментариев авторов. Сходство текстов само по себе не подтверждает совпадение политических позиций." }
];

const reasonLabels: Record<string, string> = {
  quote_mismatch: "Цитата не совпала с исходным текстом",
  invalid_ids: "Модель вернула неверные идентификаторы комментариев",
  missing_comments: "В ответе модели отсутствуют комментарии",
  invalid_schema: "Ответ модели не соответствует схеме",
  invalid_comment: "Неверный формат комментария в ответе",
  invalid_contract: "Ответ не прошёл проверку; точная причина не сохранена",
  ambiguous_with_positions: "Неясному высказыванию приписана позиция",
  clear_without_positions: "Для ясной позиции не указано подтверждение",
  unexpected_rejection_reason: "Модель вернула недопустимое поле исключения",
  output_truncated: "Ответ на одиночный комментарий обрезан",
  comment_too_long: "Комментарий превышает допустимый размер",
  content_filter: "Ответ исключён фильтром провайдера"
};

const number = (value: number) => value.toLocaleString("ru-RU");

export function PositionAnalysisProgress({ progress: p, embeddings, local = false }: {
  progress: AnalysisProgress; embeddings: EmbeddingDetails | null; local?: boolean;
}) {
  const steps = local ? localStages : stages;
  const counts = local ? [
    [p.total_comments, p.total_comments],
    [p.processed_embeddings, p.total_embeddings],
    [p.processed_pairs, p.total_pairs]
  ] : [
    [p.processed_comments, p.total_comments],
    [p.processed_embeddings, p.total_embeddings],
    [p.processed_questions, p.total_questions],
    [p.processed_relations, p.total_relations]
  ];
  const current = p.state === "idle" ? -1 : p.state === "done" ? steps.length - 1 : steps.findIndex(stage => stage.key === p.phase);
  const stage = steps[current];
  const [processed, total] = counts[current] ?? [0, 0];
  const percent = p.state === "done" ? 100 : total > 0 ? Math.min(100, Math.round(1000 * processed / total) / 10) : null;
  const paused = p.state === "interrupted";
  const updated = p.updated_at ? new Date(p.updated_at) : null;

  return (
    <div className="space-y-4 rounded-lg border bg-muted/20 p-4">
      <ol className="grid gap-2 sm:grid-cols-2 xl:grid-cols-4" aria-label="Этапы анализа">
        {steps.map((item, index) => {
          const done = p.state === "done" || index < current;
          const active = index === current && !done;
          return (
            <li key={item.key} aria-current={active ? "step" : undefined}
              className={`rounded-md border p-3 ${active ? "border-primary bg-primary/5" : "border-border"}`}>
              <p className="text-sm font-medium">{index + 1}. {item.title}</p>
              <p className="mt-1 text-xs text-muted-foreground">
                {done ? "Завершён" : active ? paused ? "Приостановлен" : p.state === "running" ? "Выполняется" : "Остановлен" : "Ожидает"}
                {counts[index][1] > 0 ? ` · ${number(counts[index][0])} / ${number(counts[index][1])}` : ""}
              </p>
            </li>
          );
        })}
      </ol>
      {stage ? (
        <div className="space-y-2">
          <div className="flex justify-between gap-3 text-sm font-medium">
            <span>{p.state === "done" ? "Анализ завершён" : `Этап ${current + 1} из ${steps.length}: ${stage.title}`}</span>
            <span className="tabular-nums">{percent === null ? "Подготовка…" : `${percent.toLocaleString("ru-RU")}%`}</span>
          </div>
          <div role="progressbar" aria-label={`Прогресс этапа: ${stage.title}`} aria-valuemin={0} aria-valuemax={100}
            aria-valuenow={percent ?? undefined} aria-valuetext={p.state === "done" ? "Анализ завершён" : total > 0 ? `${processed} из ${total}` : "Подготовка этапа"}
            className="h-3 overflow-hidden rounded-full bg-muted">
            <div className={`h-full rounded-full bg-primary transition-all ${percent === null && p.state === "running" ? "motion-safe:animate-pulse" : ""}`}
              style={{ width: `${percent ?? 20}%` }} />
          </div>
          <p className="text-sm text-muted-foreground">{stage.description}</p>
          <p className="text-xs text-muted-foreground">Процент относится к текущему этапу, а не ко всему времени анализа.</p>
        </div>
      ) : <p className="text-sm text-muted-foreground">{local ? "Анализ ещё не запускался. Эмбеддинги комментариев вычисляются локально, без OpenRouter." : "Анализ ещё не запускался. Первым этапом обрабатывается вся история комментариев."}</p>}
      {p.state !== "idle" ? (
        <div className="space-y-1 text-sm">
          <p role="status">{p.activity || (paused ? "Прогресс сохранён; можно продолжить анализ." : "Ожидание обновления прогресса…")}</p>
          <p className="text-muted-foreground">Сохранено комментариев: {number(p.processed_comments)} / {number(p.total_comments)}.
            {p.cached_comments > 0 ? ` Из кеша при этом запуске: ${number(p.cached_comments)}.` : ""}</p>
          {!local ? <p className="text-muted-foreground">Запросов извлечения в этом запуске: {number(p.extraction_requests)} · делений пакетов: {number(p.split_batches)}.</p> : null}
          {p.phase === "comparisons" || p.state === "done" ? <p className="text-muted-foreground">Завершённых пар авторов: {number(p.processed_pairs)} / {number(p.total_pairs)}.</p> : null}
          {updated && !Number.isNaN(updated.getTime()) ? <p className="text-xs text-muted-foreground">Последнее обновление: {updated.toLocaleTimeString("ru-RU")}.</p> : null}
          {paused ? <p className="text-muted-foreground">Нажмите «Продолжить анализ»: сохранённые комментарии и векторы будут использованы повторно.</p> : null}
        </div>
      ) : null}
      {p.rejected_comments > 0 ? (
        <div className="space-y-1 text-sm">
          <p>Исключено из-за ошибок ответа: {number(p.rejected_comments)}.</p>
          <ul className="list-inside list-disc text-muted-foreground">
            {Object.entries(p.rejection_reasons).map(([reason, count]) => (
              <li key={reason}>{reasonLabels[reason] ?? "Ответ не прошёл проверку"}: {number(count)}</li>
            ))}
          </ul>
        </div>
      ) : null}
      {embeddings ? (
        <details className="rounded-md border p-3 text-sm">
          <summary className="cursor-pointer font-medium">Эмбеддинги: что создаётся и где хранится</summary>
          <div className="mt-3 space-y-2 break-words">
            <p>{local ? "Кодируются сами комментарии." : "Кодируются формулировки политических вопросов."} Каждый вектор — JSON-массив
              {embeddings.dimensions > 0 ? ` из ${embeddings.dimensions} чисел` : " чисел"}, нормализованный по L2.</p>
            <p>Модель: <code>{embeddings.model}</code>. Вычисление происходит локально.</p>
            <p>Сохранено векторов этой версии модели: <strong>{number(embeddings.saved_vectors)}</strong>.</p>
            {embeddings.saved_vectors === 0 ? <p>{p.state === "done"
              ? local ? "Нет непустых комментариев для создания векторов." : "Векторы не понадобились: политические вопросы не найдены."
              : local ? "Векторов пока нет. Их создание начнётся после чтения локальных комментариев." : "Векторов пока нет. Их создание начнётся после извлечения политических позиций."}</p> : null}
            <p>Хранилище: <code>{embeddings.storage}</code>, таблица <code>cache</code>, namespace <code>{local ? "comment-embeddings:" : "embeddings:"}{embeddings.version}</code>.</p>
            {p.cached_embeddings > 0 ? <p>Из кеша для текущего анализа: {number(p.cached_embeddings)} векторов.</p> : null}
          </div>
        </details>
      ) : null}
    </div>
  );
}
