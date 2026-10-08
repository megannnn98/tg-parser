import type { Profile } from "@/api/generated";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

/** The user's messages by channel: a donut (its segments come from the API) and a table. */
export function ChannelShares({ profile }: { profile: Profile }) {
  return (
    <div className="grid items-start gap-6 md:grid-cols-[minmax(0,260px)_1fr]">
      <svg className="mx-auto w-full max-w-[260px]" viewBox="0 0 42 42" role="img" aria-label="Круговая диаграмма каналов">
        <circle cx="21" cy="21" r="15.915" fill="none" stroke="#e5e7eb" strokeWidth="7" />
        {profile.channels.map((channel) => (
          <circle
            key={channel.name}
            cx="21"
            cy="21"
            r="15.915"
            fill="none"
            stroke={channel.color}
            strokeWidth="7"
            strokeDasharray={channel.dasharray}
            strokeDashoffset={channel.dashoffset}
            transform="rotate(-90 21 21)"
          />
        ))}
        <text x="21" y="20.5" textAnchor="middle" fontSize="5" fontWeight="700" className="fill-foreground">
          {profile.total_messages}
        </text>
        <text x="21" y="25" textAnchor="middle" fontSize="3" className="fill-muted-foreground">
          сообщений
        </text>
      </svg>

      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Канал</TableHead>
            <TableHead className="text-right">Сообщений</TableHead>
            <TableHead className="text-right">Доля</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {profile.channels.map((channel) => (
            <TableRow key={channel.name}>
              <TableCell>
                <span className="inline-flex items-center gap-2">
                  <span className="size-2.5 rounded-full" style={{ background: channel.color }} />
                  {channel.name}
                </span>
              </TableCell>
              <TableCell className="text-right tabular-nums">{channel.message_count}</TableCell>
              <TableCell className="text-right tabular-nums">{channel.percent}%</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
