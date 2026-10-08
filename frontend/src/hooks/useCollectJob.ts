import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";

import { cancelCollect, collectStatus, type JobStatus, startCollect } from "@/api/generated";
import { unwrap } from "@/lib/api";

const POLL_MS = 1000;
/** The status poll gives up after this many failures in a row. */
export const MAX_POLL_FAILURES = 10;

/** One comment collection: start it, poll its status every second until it ends,
 * cancel it. `onDone` runs once, with the final status, when it succeeds. */
export function useCollectJob(onDone: (job: JobStatus) => void) {
  const [jobId, setJobId] = useState<string | null>(null);
  const onDoneRef = useRef(onDone);
  onDoneRef.current = onDone;

  const start = useMutation({
    mutationFn: (username: string) => unwrap(startCollect({ body: { username } })),
    onSuccess: (started) => setJobId(started.job_id)
  });

  const status = useQuery({
    queryKey: ["collect", jobId],
    queryFn: () => unwrap(collectStatus({ path: { job_id: jobId! } })),
    enabled: jobId !== null,
    refetchInterval: (query) => (query.state.data?.state === "running" ? POLL_MS : false),
    retry: MAX_POLL_FAILURES - 1,
    retryDelay: POLL_MS
  });

  const cancel = useMutation({
    // Its errors are left to the poll: the job's own status says how it ended.
    mutationFn: () => unwrap(cancelCollect({ path: { job_id: jobId! } }))
  });

  const job = jobId !== null ? status.data : undefined;
  const finalState = job?.state;
  useEffect(() => {
    if (job && finalState === "done") {
      onDoneRef.current(job);
    }
    // Once per job: the poll stops at "done", so the status stops changing.
  }, [job, finalState]);

  const running =
    start.isPending || (jobId !== null && !status.isError && (status.isPending || job?.state === "running"));

  let error: string | null = null;
  if (start.isError) {
    error = start.error.message;
  } else if (status.isError) {
    error = "Не удалось получить статус сбора, попробуйте ещё раз";
  } else if (job?.state === "error") {
    error = `Ошибка: ${job.error}`;
  }

  return {
    /** Something to show: the collection was asked for at least once. */
    started: start.isPending || start.isSuccess || start.isError,
    starting: start.isPending,
    running,
    canCancel: jobId !== null && running && !cancel.isPending,
    job,
    error,
    start: (username: string) => {
      setJobId(null);
      cancel.reset();
      start.mutate(username);
    },
    cancel: () => cancel.mutate()
  };
}
