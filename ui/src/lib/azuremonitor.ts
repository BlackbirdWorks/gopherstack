/** Client for the Azure Monitor Logs emulator (workspaces, tables, DCEs/DCRs, KQL) via the dashboard's in-process proxy. */

import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const MONITOR_PROXY_BASE = "/dashboard/api/v1/azure/monitor";
export const QUERY_HISTORY_KEY = "gopherstack_monitor_query_history";
export const QUERY_HISTORY_MAX = 20;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json =>
  v && typeof v === "object" && !Array.isArray(v) ? (v as Json) : {};
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type WorkspaceInfo = {
  id: string;
  name: string;
  customerId: string;
  location: string;
  sku: string;
  retentionDays: number;
  created: string;
  raw: Json;
};

export type ColumnInfo = { name: string; type: string; description: string };

export type TableInfo = {
  name: string;
  custom: boolean;
  tableType: string;
  plan: string;
  retentionDays: number;
  totalRetentionDays: number;
  columns: ColumnInfo[];
};

export type EndpointInfo = {
  id: string;
  name: string;
  location: string;
  kind: string;
  immutableId: string;
  logsIngestion: string;
};

export type DataFlow = {
  streams: string[];
  destinations: string[];
  transformKql: string;
  outputStream: string;
};

export type RuleInfo = {
  id: string;
  name: string;
  location: string;
  kind: string;
  immutableId: string;
  streams: { name: string; columns: ColumnInfo[] }[];
  destinations: string[];
  dataFlows: DataFlow[];
};

export type QueryColumn = { name: string; type: string };
export type QueryTable = { name: string; columns: QueryColumn[]; rows: unknown[][] };
export type QueryResult = { tables: QueryTable[] };

/** A rejected query, carrying the BadArgumentError chain from the data-plane body. */
export class QueryError extends Error {
  readonly httpStatus: number;
  readonly code: string;
  readonly details: string[];

  constructor(httpStatus: number, code: string, message: string, details: string[]) {
    super(message);
    this.name = "QueryError";
    this.httpStatus = httpStatus;
    this.code = code;
    this.details = details;
  }
}

export type TimespanOption = { value: string; label: string };

export const TIMESPANS: readonly TimespanOption[] = [
  { value: "PT1H", label: "Last hour" },
  { value: "PT24H", label: "Last 24 hours" },
  { value: "P7D", label: "Last 7 days" },
  { value: "P30D", label: "Last 30 days" },
  { value: "", label: "All time" },
];

export const EXAMPLE_QUERIES: readonly { label: string; query: string }[] = [
  { label: "Print a literal", query: "print Hello = 'world', Answer = 42" },
  { label: "Count rows of a table", query: "Heartbeat | count" },
  { label: "Recent rows", query: "Heartbeat\n| where TimeGenerated > ago(1h)\n| take 10" },
  {
    label: "Summarize by hour",
    query:
      "Heartbeat\n| summarize Beats = count() by bin(TimeGenerated, 1h)\n| order by TimeGenerated asc",
  },
  { label: "Generate a range", query: "range x from 1 to 5 step 1\n| extend Square = x * x" },
];

function columns(raw: unknown): ColumnInfo[] {
  return list(raw).map((c) => ({
    name: str(c.name),
    type: str(c.type),
    description: str(c.description),
  }));
}

export function parseWorkspace(raw: Json): WorkspaceInfo {
  const p = obj(raw.properties);
  return {
    id: str(raw.id),
    name: str(raw.name),
    customerId: str(p.customerId),
    location: str(raw.location),
    sku: str(obj(p.sku).name),
    retentionDays: num(p.retentionInDays),
    created: str(p.createdDate),
    raw,
  };
}

export function parseTable(raw: Json): TableInfo {
  const p = obj(raw.properties);
  const schema = obj(p.schema);
  return {
    name: str(raw.name),
    custom: str(schema.tableType) === "CustomLog",
    tableType: str(schema.tableType),
    plan: str(p.plan),
    retentionDays: num(p.retentionInDays),
    totalRetentionDays: num(p.totalRetentionInDays),
    columns: columns(schema.columns),
  };
}

export function parseEndpoint(raw: Json): EndpointInfo {
  const p = obj(raw.properties);
  return {
    id: str(raw.id),
    name: str(raw.name),
    location: str(raw.location),
    kind: str(raw.kind),
    immutableId: str(p.immutableId),
    logsIngestion: str(obj(p.logsIngestion).endpoint) || str(obj(p.endpoints).logsIngestion),
  };
}

export function parseRule(raw: Json): RuleInfo {
  const p = obj(raw.properties);
  const las = list(obj(p.destinations).logAnalytics);
  return {
    id: str(raw.id),
    name: str(raw.name),
    location: str(raw.location),
    kind: str(raw.kind),
    immutableId: str(p.immutableId),
    streams: Object.entries(obj(p.streamDeclarations)).map(([name, v]) => ({
      name,
      columns: columns(obj(v).columns),
    })),
    destinations: las.map((d) => str(d.name)),
    dataFlows: list(p.dataFlows).map((f) => ({
      streams: (Array.isArray(f.streams) ? f.streams : []).map(String),
      destinations: (Array.isArray(f.destinations) ? f.destinations : []).map(String),
      transformKql: str(f.transformKql),
      outputStream: str(f.outputStream),
    })),
  };
}

export async function listWorkspaces(fetchFn: FetchFn = fetch): Promise<WorkspaceInfo[]> {
  const body = await restCall(MONITOR_PROXY_BASE, "GET", "/workspaces", undefined, fetchFn);
  return list(body.value).map((w) => parseWorkspace(w));
}

export async function listTables(
  workspace: string,
  fetchFn: FetchFn = fetch,
): Promise<TableInfo[]> {
  const body = await restCall(
    MONITOR_PROXY_BASE,
    "GET",
    `/workspaces/${enc(workspace)}/tables`,
    undefined,
    fetchFn,
  );
  return list(body.value).map((t) => parseTable(t));
}

export async function listEndpoints(fetchFn: FetchFn = fetch): Promise<EndpointInfo[]> {
  const body = await restCall(MONITOR_PROXY_BASE, "GET", "/endpoints", undefined, fetchFn);
  return list(body.value).map((e) => parseEndpoint(e));
}

export async function listRules(fetchFn: FetchFn = fetch): Promise<RuleInfo[]> {
  const body = await restCall(MONITOR_PROXY_BASE, "GET", "/rules", undefined, fetchFn);
  return list(body.value).map((r) => parseRule(r));
}

function innerChain(inner: unknown): string[] {
  const out: string[] = [];
  let cur = obj(inner);
  while (Object.keys(cur).length > 0) {
    const line = [str(cur.code), str(cur.message)].filter(Boolean).join(": ");
    if (line) out.push(line);
    cur = obj(cur.innererror);
  }
  return out;
}

/** Builds a QueryError from a data-plane error body ({error:{code,message,innererror}}). */
export function queryErrorFrom(httpStatus: number, body: unknown): QueryError {
  const e = obj(obj(body).error);
  return new QueryError(
    httpStatus,
    str(e.code) || str(e.status) || "Error",
    str(e.message) || `HTTP ${httpStatus}`,
    innerChain(e.innererror),
  );
}

/** Runs a KQL query against one workspace in the in-process backend. */
export async function runQuery(
  workspace: string,
  query: string,
  timespan: string,
  fetchFn: FetchFn = fetch,
): Promise<QueryResult> {
  let res: Response;
  try {
    res = await fetchFn(`${MONITOR_PROXY_BASE}/workspaces/${enc(workspace)}/query`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ query, timespan }),
    });
  } catch (e) {
    throw new QueryError(0, "NetworkError", e instanceof Error ? e.message : String(e), []);
  }
  let body: unknown = {};
  try {
    body = await res.json();
  } catch {
    // non-JSON body: fall back to the status line
  }
  if (!res.ok) throw queryErrorFrom(res.status, body);
  const tables = list(obj(body).tables).map((t) => ({
    name: str(t.name),
    columns: list(t.columns).map((c) => ({ name: str(c.name), type: str(c.type) })),
    rows: Array.isArray(t.rows) ? (t.rows as unknown[][]) : [],
  }));
  return { tables };
}

/** Renders one result cell for display. */
export function formatCell(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export type HistoryEntry = { query: string; timespan: string; at: string };

type ReadStorage = Pick<Storage, "getItem">;
type WriteStorage = Pick<Storage, "setItem">;

function defaultStorage(): Storage | undefined {
  try {
    return typeof window === "undefined" ? undefined : window.localStorage;
  } catch {
    return undefined;
  }
}

/** Per-viewer query history; empty when storage is unavailable or holds junk. */
export function readHistory(storage?: ReadStorage): HistoryEntry[] {
  try {
    const raw = (storage ?? defaultStorage())?.getItem(QUERY_HISTORY_KEY);
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    return parsed
      .filter(
        (e): e is HistoryEntry =>
          !!e && typeof e.query === "string" && typeof e.timespan === "string",
      )
      .slice(0, QUERY_HISTORY_MAX);
  } catch {
    return [];
  }
}

/** Newest-first history with the same query+timespan deduplicated and a fixed cap. */
export function addToHistory(history: HistoryEntry[], entry: HistoryEntry): HistoryEntry[] {
  const rest = history.filter((h) => !(h.query === entry.query && h.timespan === entry.timespan));
  return [entry, ...rest].slice(0, QUERY_HISTORY_MAX);
}

export function writeHistory(history: HistoryEntry[], storage?: WriteStorage): void {
  try {
    (storage ?? defaultStorage())?.setItem(QUERY_HISTORY_KEY, JSON.stringify(history));
  } catch {
    // storage unavailable (private mode / blocked): history lives for this session only
  }
}
