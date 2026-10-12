import { useMemo, useState } from "react";
import { Link } from "react-router-dom";

export type SimilarityGraph = {
  nodes: { tg_id: number; display_username: string }[];
  edges: { left: number; right: number; similarity: number }[];
};

const MAX_NEIGHBOURS = 10;

function percentage(value: number) {
  return `${Math.round(value * 100)}%`;
}

export function AuthorSimilarityGraph({ graph }: { graph: SimilarityGraph }) {
  const [selectedId, setSelectedId] = useState<number | null>(null);
  const selected = graph.nodes.find((node) => node.tg_id === selectedId) ?? graph.nodes[0];
  const { links, totalLinks } = useMemo(() => {
    if (!selected) return { links: [] as SimilarityGraph["edges"], totalLinks: 0 };
    const all = graph.edges
      .filter((edge) => edge.left === selected.tg_id || edge.right === selected.tg_id)
      .sort((a, b) => b.similarity - a.similarity);
    return { links: all.slice(0, MAX_NEIGHBOURS), totalLinks: all.length };
  }, [graph.edges, selected]);

  if (!graph.nodes.length) return <p className="text-sm text-muted-foreground">Сначала запустите сравнение авторов.</p>;
  if (!selected) return null;

  const nodeById = new Map(graph.nodes.map((node) => [node.tg_id, node]));
  const size = 720, center = size / 2, radius = 220;
  const strongest = links[0]?.similarity ?? 1;
  const weakest = links.at(-1)?.similarity ?? strongest;
  const neighbours = links.map((edge, index) => {
    const id = edge.left === selected.tg_id ? edge.right : edge.left;
    const angle = (index / Math.max(links.length, 1)) * Math.PI * 2 - Math.PI / 2;
    return { edge, node: nodeById.get(id)!, x: center + Math.cos(angle) * radius, y: center + Math.sin(angle) * radius, angle };
  });

  return <div className="space-y-4">
    <div className="flex flex-wrap items-center gap-3">
      <label className="text-sm font-medium" htmlFor="similarity-author">Автор в центре</label>
      <select id="similarity-author" className="h-9 rounded-md border border-input bg-background px-3 text-sm" value={selected.tg_id} onChange={(event) => setSelectedId(Number(event.target.value))}>
        {[...graph.nodes].sort((a, b) => a.display_username.localeCompare(b.display_username, "ru")).map((node) => <option key={node.tg_id} value={node.tg_id}>{node.display_username}</option>)}
      </select>
      <span className="text-sm text-muted-foreground">Показаны наиболее близкие авторы</span>
    </div>
    {links.length ? <>
      <svg viewBox={`0 0 ${size} ${size}`} className="w-full max-w-3xl rounded-md border bg-muted/20" aria-label={`Авторы, похожие на ${selected.display_username}`}>
        {neighbours.map(({ edge, x, y, angle, node }) => {
          const relative = strongest === weakest ? 1 : (edge.similarity - weakest) / (strongest - weakest);
          const labelX = x + Math.cos(angle) * 32, labelY = y + Math.sin(angle) * 32;
          const anchor = Math.cos(angle) > 0.3 ? "start" : Math.cos(angle) < -0.3 ? "end" : "middle";
          return <g key={node.tg_id}>
            <line x1={center} y1={center} x2={x} y2={y} stroke="#2563eb" strokeOpacity={0.35 + relative * 0.65} strokeWidth={2 + relative * 5} />
            <title>{`${selected.display_username} ↔ ${node.display_username}: ${percentage(edge.similarity)}`}</title>
            <a href={`/users/${node.tg_id}`}><circle cx={x} cy={y} r="22" fill="#0f172a" /><text x={x} y={y + 5} textAnchor="middle" fill="white" fontSize="12">{percentage(edge.similarity)}</text><text x={labelX} y={labelY} textAnchor={anchor} dominantBaseline="middle" fill="#0f172a" fontSize="16" fontWeight="600">{node.display_username}</text></a>
          </g>;
        })}
        <a href={`/users/${selected.tg_id}`}><circle cx={center} cy={center} r="58" fill="#0f172a" /><text x={center} y={center - 3} textAnchor="middle" fill="white" fontSize="17" fontWeight="700">{selected.display_username}</text><text x={center} y={center + 20} textAnchor="middle" fill="#bfdbfe" fontSize="13">выбранный автор</text></a>
      </svg>
      <p className="text-xs text-muted-foreground">Толще и ярче линия — выше сходство. Нажмите на имя или круг, чтобы открыть профиль.</p>
      {totalLinks > links.length ? <p className="text-xs text-muted-foreground">Показаны {links.length} из {totalLinks} связей выбранного автора.</p> : null}
      <div className="grid gap-2 text-sm sm:grid-cols-2">{neighbours.map(({ edge, node }) => <Link key={`pair-${node.tg_id}`} className="rounded-md border px-3 py-2 hover:border-primary hover:bg-muted/40" to={`/users/${node.tg_id}`}><span className="font-medium">{node.display_username}</span><span className="text-muted-foreground"> · {percentage(edge.similarity)}</span></Link>)}</div>
    </> : <p className="text-sm text-muted-foreground">Для {selected.display_username} пока нет опубликованных сравнений с другими авторами.</p>}
  </div>;
}
