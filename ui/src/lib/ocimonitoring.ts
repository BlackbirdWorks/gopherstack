/** Client for the Oracle Cloud Monitoring emulator: metrics, MQL queries and alarms via the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  ociPostOne,
  ociPostPages,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const MONITORING_PROXY_BASE = "/dashboard/api/v1/oci/monitoring";
export const MONITORING_COMPARTMENT_KEY = "gopherstack_ocimonitoring_compartment";

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type MetricDef = {
  namespace: string;
  name: string;
  resourceGroup: string;
  dimensions: Record<string, string>;
};

export type NamespaceInfo = { namespace: string; metrics: string[] };

export type Point = { t: number; v: number };

export type Series = {
  name: string;
  namespace: string;
  dimensions: Record<string, string>;
  resolution: string;
  points: Point[];
};

export type AlarmInfo = {
  id: string;
  name: string;
  state: string;
  severity: string;
  namespace: string;
  query: string;
  enabled: boolean;
  status: string;
  triggered: string;
  destinations: string[];
};

export type HistoryEntry = { timestamp: string; summary: string; triggered: string };

export type TimeRange = { id: string; label: string; seconds: number };

export const TIME_RANGES: TimeRange[] = [
  { id: "15m", label: "Last 15 minutes", seconds: 900 },
  { id: "1h", label: "Last hour", seconds: 3600 },
  { id: "6h", label: "Last 6 hours", seconds: 21600 },
  { id: "24h", label: "Last 24 hours", seconds: 86400 },
];

export function parseMetric(raw: Json): MetricDef {
  return {
    namespace: str(raw.namespace),
    name: str(raw.name),
    resourceGroup: str(raw.resourceGroup),
    dimensions: strMap(raw.dimensions),
  };
}

/** Distinct namespaces with their sorted distinct metric names. */
export function groupNamespaces(defs: MetricDef[]): NamespaceInfo[] {
  const by = new Map<string, Set<string>>();
  for (const d of defs) {
    if (!by.has(d.namespace)) by.set(d.namespace, new Set());
    by.get(d.namespace)?.add(d.name);
  }
  return [...by.entries()]
    .map(([namespace, names]) => ({ namespace, metrics: [...names].toSorted() }))
    .toSorted((a, b) => a.namespace.localeCompare(b.namespace));
}

export function parseSeries(raw: Json): Series {
  return {
    name: str(raw.name),
    namespace: str(raw.namespace),
    dimensions: strMap(raw.dimensions),
    resolution: str(raw.resolution),
    points: list(raw.aggregatedDatapoints)
      .map((p) => ({ t: Date.parse(str(p.timestamp)), v: num(p.value) }))
      .filter((p) => !Number.isNaN(p.t)),
  };
}

export function parseAlarm(raw: Json, status?: Json): AlarmInfo {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    severity: str(raw.severity),
    namespace: str(raw.namespace),
    query: str(raw.query),
    enabled: raw.isEnabled === true,
    status: str(status?.status),
    triggered: str(status?.timestampTriggered),
    destinations: list(raw.destinations).map(String),
  };
}

export function parseHistory(raw: Json): HistoryEntry {
  return {
    timestamp: str(raw.timestamp),
    summary: str(raw.summary),
    triggered: str(raw.timestampTriggered),
  };
}

/** Label for a series: its dimensions, or the metric name when it has none. */
export function seriesLabel(s: Pick<Series, "name" | "dimensions">): string {
  const dims = Object.entries(s.dimensions).map(([k, v]) => `${k}=${v}`);
  return dims.length > 0 ? `${s.name} {${dims.join(", ")}}` : s.name;
}

/** SVG polyline points for a series scaled into a w x h box (value axis includes zero). */
export function chartPoints(points: Point[], w: number, h: number, pad = 4): string {
  if (points.length === 0) return "";
  const ts = points.map((p) => p.t);
  const vs = points.map((p) => p.v);
  const t0 = Math.min(...ts);
  const tSpan = Math.max(...ts) - t0 || 1;
  const v0 = Math.min(...vs, 0);
  const vSpan = Math.max(...vs, 0) - v0 || 1;
  return points
    .map(
      (p) =>
        `${(((p.t - t0) / tSpan) * (w - 2 * pad) + pad).toFixed(1)},${(h - pad - ((p.v - v0) / vSpan) * (h - 2 * pad)).toFixed(1)}`,
    )
    .join(" ");
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(MONITORING_PROXY_BASE, fetchFn);

export async function listMetrics(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<MetricDef[]> {
  const items = await ociPostPages(
    MONITORING_PROXY_BASE,
    "/api/metrics/actions/listMetrics",
    `compartmentId=${enc(compartment)}`,
    {},
    fetchFn,
  );
  return items.map((r) => parseMetric(r));
}

export async function summarizeMetrics(
  compartment: string,
  namespace: string,
  query: string,
  rangeSeconds: number,
  fetchFn: FetchFn = fetch,
  now: Date = new Date(),
): Promise<Series[]> {
  const { body } = await ociPostOne(
    MONITORING_PROXY_BASE,
    `/api/metrics/actions/summarizeMetricsData?compartmentId=${enc(compartment)}`,
    {
      namespace,
      query,
      startTime: new Date(now.getTime() - rangeSeconds * 1000).toISOString(),
      endTime: new Date(now.getTime() + 60_000).toISOString(),
    },
    fetchFn,
  );
  return list(body).map((r) => parseSeries(r));
}

export async function listAlarms(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<AlarmInfo[]> {
  const q = `compartmentId=${enc(compartment)}`;
  const [alarms, statuses] = await Promise.all([
    ociListPages(MONITORING_PROXY_BASE, "/api/alarms", q, "", fetchFn),
    ociListPages(MONITORING_PROXY_BASE, "/api/alarms/status", q, "", fetchFn),
  ]);
  const byId = new Map(statuses.map((s) => [str(s.id), s]));
  return alarms.map((a) => parseAlarm(a, byId.get(str(a.id))));
}

export async function getAlarmHistory(
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<HistoryEntry[]> {
  const body = await ociGetOne(MONITORING_PROXY_BASE, `/api/alarms/${enc(id)}/history`, fetchFn);
  return list(body.entries).map((e) => parseHistory(e));
}

export const readStoredMonitoringCompartment = () => readStoredValue(MONITORING_COMPARTMENT_KEY);
export const writeStoredMonitoringCompartment = (id: string) =>
  writeStoredValue(MONITORING_COMPARTMENT_KEY, id);
