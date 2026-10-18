// Data logic for the App Inspector page (/_gopherstack/apptrace API).

export type TraceDetail = {
  requestHeaders?: Record<string, string>;
  responseHeaders?: Record<string, string>;
  query?: string;
  requestBody?: string;
  responseBody?: string;
  requestBodyTruncated?: boolean;
  responseBodyTruncated?: boolean;
};

export type TraceSpan = {
  id: string;
  traceId: string;
  parentId?: string;
  kind: "request" | "internal";
  time: string;
  service: string;
  operation: string;
  resource?: string;
  region?: string;
  account?: string;
  principal?: string;
  accessKeyId?: string;
  method?: string;
  path?: string;
  status: number;
  errorCode?: string;
  latencyMs: number;
  requestBytes: number;
  responseBytes: number;
  seq: number;
  hasDetail?: boolean;
  detail?: TraceDetail;
};

export type TraceList = {
  items: TraceSpan[];
  total: number;
  capacity: number;
  recorded: number;
};

export type StatusFilter = "" | "errors" | "2xx" | "4xx" | "5xx";

export type TraceFilters = {
  q: string;
  service: string;
  operation: string;
  region: string;
  status: StatusFilter;
  kind: "" | "request" | "internal";
};

export const emptyFilters: TraceFilters = {
  q: "",
  service: "",
  operation: "",
  region: "",
  status: "",
  kind: "",
};

export function buildQuery(
  filters: TraceFilters,
  extra: { limit?: number; afterSeq?: number } = {},
): string {
  const p = new URLSearchParams();
  if (filters.q.trim()) p.set("q", filters.q.trim());
  if (filters.service) p.set("service", filters.service);
  if (filters.operation.trim()) p.set("operation", filters.operation.trim());
  if (filters.region) p.set("region", filters.region);
  if (filters.kind) p.set("kind", filters.kind);

  switch (filters.status) {
    case "errors":
      p.set("errorsOnly", "true");
      break;
    case "2xx":
      p.set("minStatus", "200");
      p.set("maxStatus", "299");
      break;
    case "4xx":
      p.set("minStatus", "400");
      p.set("maxStatus", "499");
      break;
    case "5xx":
      p.set("minStatus", "500");
      p.set("maxStatus", "599");
      break;
    default:
      break;
  }

  if (extra.limit) p.set("limit", String(extra.limit));
  if (extra.afterSeq) p.set("afterSeq", String(extra.afterSeq));
  const s = p.toString();
  return s ? `?${s}` : "";
}

export type StatusClass = "ok" | "redirect" | "client" | "server";

export function statusClass(status: number): StatusClass {
  if (status >= 500) return "server";
  if (status >= 400) return "client";
  if (status >= 300) return "redirect";
  return "ok";
}

export function formatLatency(ms: number): string {
  if (!Number.isFinite(ms)) return "—";
  if (ms < 1) return `${(ms * 1000).toFixed(0)} µs`;
  if (ms < 1000) return `${ms.toFixed(ms < 10 ? 1 : 0)} ms`;
  return `${(ms / 1000).toFixed(2)} s`;
}

export function maxSeq(spans: TraceSpan[]): number {
  return spans.reduce((m, s) => Math.max(m, s.seq), 0);
}

// Prepend freshly polled spans (newest first) and keep at most cap rows.
export function mergeNew(existing: TraceSpan[], incoming: TraceSpan[], cap: number): TraceSpan[] {
  if (incoming.length === 0) return existing;
  const seen = new Set(existing.map((s) => s.seq));
  const fresh = incoming.filter((s) => !seen.has(s.seq));
  return [...fresh, ...existing].toSorted((a, b) => b.seq - a.seq).slice(0, cap);
}

export function distinctValues(spans: TraceSpan[], key: "service" | "region"): string[] {
  const out = new Set<string>();
  for (const s of spans) {
    const v = s[key];
    if (v) out.add(v);
  }
  return [...out].toSorted();
}

export type TreeRow = { span: TraceSpan; depth: number };

// Order a trace as a tree: parents before children, siblings by seq. Spans
// whose parent was evicted from the ring are treated as roots.
export function buildTraceTree(spans: TraceSpan[]): TreeRow[] {
  const ids = new Set(spans.map((s) => s.id));
  const children = new Map<string, TraceSpan[]>();
  const roots: TraceSpan[] = [];

  for (const s of [...spans].toSorted((a, b) => a.seq - b.seq)) {
    if (s.parentId && ids.has(s.parentId)) {
      const list = children.get(s.parentId) ?? [];
      list.push(s);
      children.set(s.parentId, list);
    } else {
      roots.push(s);
    }
  }

  const rows: TreeRow[] = [];
  const visit = (s: TraceSpan, depth: number) => {
    rows.push({ span: s, depth });
    for (const c of children.get(s.id) ?? []) visit(c, depth + 1);
  };
  for (const r of roots) visit(r, 0);
  return rows;
}
