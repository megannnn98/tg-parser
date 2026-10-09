import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";

import { getChannels } from "@/api/generated";
import { ChannelsEditor } from "@/components/ChannelsEditor";
import { QueryState } from "@/components/QueryState";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { unwrap } from "@/lib/api";

/** The channel list is edited rarely, so it stays behind a button: `trigger` gets the
 * number of channels, once known, and opens the panel with the editor. */
export function ChannelsSheet({ trigger }: { trigger: (count: number | undefined) => ReactNode }) {
  const channels = useQuery({ queryKey: ["channels"], queryFn: () => unwrap(getChannels()) });

  return (
    <Sheet>
      <SheetTrigger asChild>{trigger(channels.data?.channels.length)}</SheetTrigger>
      <SheetContent>
        <SheetHeader>
          <SheetTitle>Список каналов</SheetTitle>
          <SheetDescription>Каналы, в которых ищутся комментарии. По одному в строке.</SheetDescription>
        </SheetHeader>
        <QueryState query={channels}>{(data) => <ChannelsEditor saved={data.channels} />}</QueryState>
      </SheetContent>
    </Sheet>
  );
}
