/** Client for the Cloud Monitoring emulator via the dashboard's in-process REST proxy. */

import { listAll, restCall, shortName, str, type FetchFn, type Json } from "./gcprest.ts";

export const MONITORING_PROXY_BASE = "/dashboard/api/v1/gcp/monitoring/v3";
export const MONITORING_PROJECT_KEY = "gopherstack_monitoring_project";

const enc = encodeURIComponent;
const DEFAULT_PROJECT = "gopherstack-local";
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type MetricDescriptor = {
  type: string;
  name: string;
  displayName: string;
  description: string;
  metricKind: string;
  valueType: string;
  unit: string;
  labels: string[];
  raw: Json;
};
export type SeriesPoint = { time: string; value: number | null; text: string };
export type TimeSeries = {
  metricLabels: Record<string, string>;
  resourceType: string;
  resourceLabels: Record<string, string>;
  points: SeriesPoint[];
};
export type AlertPolicy = {
  name: string;
  id: string;
  displayName: string;
  enabled: boolean;
  combiner: string;
  conditions: string[];
  channels: number;
  raw: Json;
};
export type UptimeCheck = {
  name: string;
  id: string;
  displayName: string;
  target: string;
  protocol: string;
  periodSeconds: number;
  raw: Json;
};
export type Snooze = {
  name: string;
  id: string;
  displayName: string;
  start: string;
  end: string;
  policies: string[];
  raw: Json;
};

const strList = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);

const strMapOf = (v: unknown): Record<string, string> =>
  Object.fromEntries(
    Object.entries(obj(v)).filter((e): e is [string, string] => typeof e[1] === "string"),
  );

export function parseDescriptor(raw: Json): MetricDescriptor {
  return {
    type: str(raw.type),
    name: str(raw.name),
    displayName: str(raw.displayName),
    description: str(raw.description),
    metricKind: str(raw.metricKind),
    valueType: str(raw.valueType),
    unit: str(raw.unit),
    labels: list(raw.labels).map((l) => str(l.key)),
    raw,
  };
}

/** Numeric value and display text of a TypedValue (int64 values arrive as strings). */
export function typedValue(raw: unknown): { value: number | null; text: string } {
  const v = obj(raw);
  if (v.int64Value !== undefined) return { value: Number(v.int64Value), text: str(v.int64Value) };
  if (typeof v.doubleValue === "number")
    return { value: v.doubleValue, text: String(v.doubleValue) };
  if (typeof v.boolValue === "boolean")
    return { value: v.boolValue ? 1 : 0, text: String(v.boolValue) };
  if (typeof v.stringValue === "string") return { value: null, text: v.stringValue };
  if (v.distributionValue) {
    const d = obj(v.distributionValue);
    return { value: null, text: `distribution (count ${str(d.count) || "0"})` };
  }
  return { value: null, text: "" };
}

export function parseSeries(raw: Json): TimeSeries {
  const points = list(raw.points)
    .map((p) => {
      const tv = typedValue(p.value);
      const iv = obj(p.interval);
      return { time: str(iv.endTime) || str(iv.startTime), ...tv };
    })
    .toSorted((a, b) => a.time.localeCompare(b.time));
  return {
    metricLabels: strMapOf(obj(raw.metric).labels),
    resourceType: str(obj(raw.resource).type),
    resourceLabels: strMapOf(obj(raw.resource).labels),
    points,
  };
}

export function parseAlertPolicy(raw: Json): AlertPolicy {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    displayName: str(raw.displayName),
    enabled: raw.enabled !== false,
    combiner: str(raw.combiner),
    conditions: list(raw.conditions).map((c) => str(c.displayName) || shortName(str(c.name))),
    channels: Array.isArray(raw.notificationChannels) ? raw.notificationChannels.length : 0,
    raw,
  };
}

export function describeUptimeTarget(raw: Json): { target: string; protocol: string } {
  const labels = strMapOf(obj(obj(raw.monitoredResource).labels));
  const host = labels.host ?? "";
  if (raw.httpCheck) {
    const h = obj(raw.httpCheck);
    const scheme = h.useSsl === true ? "https" : "http";
    return { target: `${scheme}://${host}${str(h.path) || "/"}`, protocol: "HTTP" };
  }
  if (raw.tcpCheck)
    return { target: `${host}:${String(obj(raw.tcpCheck).port ?? "")}`, protocol: "TCP" };
  return { target: host, protocol: "" };
}

export function parseUptimeCheck(raw: Json): UptimeCheck {
  const name = str(raw.name);
  const t = describeUptimeTarget(raw);
  return {
    name,
    id: shortName(name),
    displayName: str(raw.displayName),
    target: t.target,
    protocol: t.protocol,
    periodSeconds: Number(str(raw.period).replace(/s$/, "")) || 0,
    raw,
  };
}

export function parseSnooze(raw: Json): Snooze {
  const name = str(raw.name);
  const iv = obj(raw.interval);
  return {
    name,
    id: shortName(name),
    displayName: str(raw.displayName),
    start: str(iv.startTime),
    end: str(iv.endTime),
    policies: strList(obj(raw.criteria).policies),
    raw,
  };
}

/** Snooze lifecycle relative to now: scheduled, active or expired. */
export function snoozeState(
  s: Snooze,
  now: number = Date.now(),
): "scheduled" | "active" | "expired" {
  const start = Date.parse(s.start);
  const end = Date.parse(s.end);
  if (!Number.isNaN(end) && now >= end) return "expired";
  if (!Number.isNaN(start) && now < start) return "scheduled";
  return "active";
}

const projectPath = (p: string) => `/projects/${enc(p)}`;

export async function listMetricDescriptors(
  p: string,
  filter = "",
  fetchFn: FetchFn = fetch,
): Promise<MetricDescriptor[]> {
  const q = filter ? `filter=${enc(filter)}` : "";
  return (
    await listAll(
      MONITORING_PROXY_BASE,
      `${projectPath(p)}/metricDescriptors`,
      "metricDescriptors",
      fetchFn,
      q,
    )
  ).map((r) => parseDescriptor(r));
}

export const WINDOWS: readonly { id: string; label: string; seconds: number }[] = [
  { id: "1h", label: "Last hour", seconds: 3600 },
  { id: "6h", label: "Last 6 hours", seconds: 6 * 3600 },
  { id: "24h", label: "Last 24 hours", seconds: 24 * 3600 },
];

/** Time series of one metric type over a trailing window ending at `now`. */
export async function queryTimeSeries(
  p: string,
  metricType: string,
  windowSeconds: number,
  now: Date = new Date(),
  fetchFn: FetchFn = fetch,
): Promise<TimeSeries[]> {
  const end = now.toISOString();
  const start = new Date(now.getTime() - windowSeconds * 1000).toISOString();
  const q = new URLSearchParams({
    filter: `metric.type="${metricType.replaceAll('"', "")}"`,
    "interval.startTime": start,
    "interval.endTime": end,
  });
  const rows = await listAll(
    MONITORING_PROXY_BASE,
    `${projectPath(p)}/timeSeries`,
    "timeSeries",
    fetchFn,
    q.toString(),
  );
  return rows.map((r) => parseSeries(r));
}

export async function listAlertPolicies(
  p: string,
  fetchFn: FetchFn = fetch,
): Promise<AlertPolicy[]> {
  return (
    await listAll(
      MONITORING_PROXY_BASE,
      `${projectPath(p)}/alertPolicies`,
      "alertPolicies",
      fetchFn,
    )
  ).map((r) => parseAlertPolicy(r));
}

export async function listUptimeChecks(
  p: string,
  fetchFn: FetchFn = fetch,
): Promise<UptimeCheck[]> {
  return (
    await listAll(
      MONITORING_PROXY_BASE,
      `${projectPath(p)}/uptimeCheckConfigs`,
      "uptimeCheckConfigs",
      fetchFn,
    )
  ).map((r) => parseUptimeCheck(r));
}

export async function listSnoozes(p: string, fetchFn: FetchFn = fetch): Promise<Snooze[]> {
  return (
    await listAll(MONITORING_PROXY_BASE, `${projectPath(p)}/snoozes`, "snoozes", fetchFn)
  ).map((r) => parseSnooze(r));
}

export async function deleteResource(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(MONITORING_PROXY_BASE, "DELETE", `/${name}`, undefined, fetchFn);
}

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredMonitoringProject = () =>
  readStored(MONITORING_PROJECT_KEY, DEFAULT_PROJECT);

export function writeStoredMonitoringProject(project: string): void {
  try {
    localStorage.setItem(MONITORING_PROJECT_KEY, project);
  } catch {
    // storage unavailable: project applies for this session only
  }
}
