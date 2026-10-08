import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";

import { saveChannelsList } from "@/api/generated";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle
} from "@/components/ui/alert-dialog";
import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { unwrap } from "@/lib/api";
import { channelLines, shrinkQuestion } from "@/lib/channels";

/** The channel list, one per line. A save that empties it or cuts it by more than half
 * asks first. */
export function ChannelsEditor({ saved }: { saved: string[] }) {
  const queryClient = useQueryClient();
  const [text, setText] = useState(saved.join("\n"));
  const [savedCount, setSavedCount] = useState(saved.length);
  const [question, setQuestion] = useState<string | null>(null);

  const save = useMutation({
    mutationFn: () => unwrap(saveChannelsList({ body: { channels_text: text } })),
    onSuccess: (result) => {
      setText(result.channels.join("\n"));
      setSavedCount(result.channels.length);
      queryClient.setQueryData(["channels"], result);
    }
  });

  const count = channelLines(text).length;

  function onSave() {
    const ask = shrinkQuestion(savedCount, count);
    if (ask) {
      setQuestion(ask);
    } else {
      save.mutate();
    }
  }

  return (
    <div className="space-y-3">
      <Textarea
        aria-label="Список каналов"
        className="min-h-48 font-mono"
        rows={10}
        value={text}
        onChange={(event) => setText(event.target.value)}
      />
      <div className="flex flex-wrap items-center gap-3">
        <Button type="button" onClick={onSave} disabled={save.isPending}>
          Сохранить список каналов
        </Button>
        <span className="text-sm text-muted-foreground">{count} канал(ов)</span>
      </div>
      {save.isError ? <p className="text-sm text-destructive">{save.error.message}</p> : null}
      {save.isSuccess ? (
        <p className="text-sm text-emerald-700">Сохранено: {save.data.channels.length} канал(ов)</p>
      ) : null}

      <AlertDialog open={question !== null} onOpenChange={(open) => !open && setQuestion(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Сохранить список каналов?</AlertDialogTitle>
            <AlertDialogDescription>{question}</AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Отмена</AlertDialogCancel>
            <AlertDialogAction onClick={() => save.mutate()}>Сохранить</AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}
