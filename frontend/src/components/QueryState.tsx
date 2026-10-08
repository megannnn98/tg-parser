import type { ReactNode } from "react";
import type { UseQueryResult } from "@tanstack/react-query";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";

type Props<T> = {
  query: UseQueryResult<T>;
  /** Whether the answer has nothing to show; the empty state is shown instead. */
  isEmpty?: (data: T) => boolean;
  empty?: ReactNode;
  children: (data: T) => ReactNode;
};

/** Loading, error and empty for one query, the same on every page. The error is the
 * API's own message, not a general phrase. */
export function QueryState<T>({ query, isEmpty, empty = "Ничего не найдено.", children }: Props<T>) {
  if (query.isPending) {
    return (
      <div className="space-y-2" role="status" aria-label="Загрузка">
        <Skeleton className="h-6 w-1/3" />
        <Skeleton className="h-24 w-full" />
      </div>
    );
  }
  const error = query.isError ? (
    <Alert variant="destructive">
      <AlertTitle>Ошибка загрузки</AlertTitle>
      <AlertDescription>{query.error.message}</AlertDescription>
    </Alert>
  ) : null;
  // A failed refetch keeps what was loaded: unmounting it would lose a draft or
  // the controls of a running action.
  if (query.data === undefined) {
    return error;
  }
  return (
    <>
      {error ? <div className="mb-4">{error}</div> : null}
      {isEmpty?.(query.data) ? <p className="text-sm text-muted-foreground">{empty}</p> : children(query.data)}
    </>
  );
}

export function isEmptyList(data: readonly unknown[]): boolean {
  return data.length === 0;
}
