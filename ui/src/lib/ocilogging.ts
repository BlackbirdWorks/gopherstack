/** Client for the Oracle Cloud Logging emulator: log groups, logs, search and test ingestion via the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  ociPostOne,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const LOGGING_PROXY_BASE = "/dashboard/api/v1/oci/logging";
export const LOGGING_COMPARTMENT_KEY = "gopherstack_ocilogging_compartment";
export const SEARCH_LIMIT = 100;

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});

export type LogGroup = {
  id: string;
  name: string;
  description: string;
  state: string;
  created: string;
};

export type LogInfo = {
  id: string;
  groupId: string;
  name: string;
  type: string;
  state: string;
  enabled: boolean;
  retentionDays: number;
  created: string;
};

export type SearchResult = {
  columns: string[];
  rows: Record<string, string>[];
  count: number;
  more: boolean;
};

export type TimeRange = { id: string; label: string; seconds: number };

export const TIME_RANGES: TimeRange[] = [
  { id: "15m", label: "Last 15 minutes", seconds: 900 },
  { id: "1h", label: "Last hour", seconds: 3600 },
  { id: "24h", label: "Last 24 hours", seconds: 86400 },
  { id: "7d", label: "Last 7 days", seconds: 604800 },
];

export function parseLogGroup(raw: Json): LogGroup {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    description: str(raw.description),
    state: str(raw.lifecycleState),
    created: str(raw.timeCreated),
  };
}

export function parseLog(raw: Json): LogInfo {
  return {
    id: str(raw.id),
    groupId: str(raw.logGroupId),
    name: str(raw.displayName) || str(raw.id),
    type: str(raw.logType),
    state: str(raw.lifecycleState),
    enabled: raw.isEnabled === true,
    retentionDays: num(raw.retentionDuration),
    created: str(raw.timeCreated),
  };
}

/** Cell text for a search value: strings as-is, everything else as compact JSON. */
export function cellText(v: unknown): string {
  if (v === null || v === undefined) return "";
  return typeof v === "string" ? v : JSON.stringify(v);
}

/** Turns a SearchLogs response into table columns and rows; `datetime` (epoch ms) becomes an ISO time. */
export function parseSearch(body: Json, more = false): SearchResult {
  const results = list(body.results).map((r) => obj(r.data));
  const declared = list(body.fields)
    .map((f) => str(f.fieldName))
    .filter(Boolean);
  const columns =
    declared.length > 0 ? declared : [...new Set(results.flatMap((r) => Object.keys(r)))];
  const rows = results.map((r) =>
    Object.fromEntries(
      columns.map((c) => [
        c,
        c === "datetime" && typeof r[c] === "number"
          ? new Date(r[c] as number).toISOString()
          : cellText(r[c]),
      ]),
    ),
  );
  const total = num(obj(body.summary).resultCount);
  return { columns, rows, count: total > 0 ? total : rows.length, more };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(LOGGING_PROXY_BASE, fetchFn);

export async function listLogGroups(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<LogGroup[]> {
  const items = await ociListPages(
    LOGGING_PROXY_BASE,
    "/api/logGroups",
    `compartmentId=${enc(compartment)}`,
    "",
    fetchFn,
  );
  return items.map((r) => parseLogGroup(r));
}

export async function listLogs(groupId: string, fetchFn: FetchFn = fetch): Promise<LogInfo[]> {
  const items = await ociListPages(
    LOGGING_PROXY_BASE,
    `/api/logGroups/${enc(groupId)}/logs`,
    "",
    "",
    fetchFn,
  );
  return items.map((r) => parseLog(r));
}

export async function getLog(
  groupId: string,
  logId: string,
  fetchFn: FetchFn = fetch,
): Promise<LogInfo> {
  return parseLog(
    await ociGetOne(
      LOGGING_PROXY_BASE,
      `/api/logGroups/${enc(groupId)}/logs/${enc(logId)}`,
      fetchFn,
    ),
  );
}

export async function setLogEnabled(
  groupId: string,
  logId: string,
  enabled: boolean,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    LOGGING_PROXY_BASE,
    "PUT",
    `/api/logGroups/${enc(groupId)}/logs/${enc(logId)}`,
    { isEnabled: enabled },
    fetchFn,
  );
}

/** A query that selects everything below a compartment, a group or a log, newest first. */
export function defaultQuery(compartment: string, group = "", log = ""): string {
  const parts = group ? (log ? [compartment, group, log] : [compartment, group]) : [compartment];
  return `search "${parts.join("/")}" | sort by datetime desc`;
}

export async function searchLogs(
  query: string,
  start: Date,
  end: Date,
  fetchFn: FetchFn = fetch,
): Promise<SearchResult> {
  const { body, next } = await ociPostOne(
    LOGGING_PROXY_BASE,
    `/api/search?limit=${SEARCH_LIMIT}`,
    { searchQuery: query, timeStart: start.toISOString(), timeEnd: end.toISOString() },
    fetchFn,
  );
  return parseSearch(obj(body), next !== "");
}

export async function pushTestEntry(
  logId: string,
  data: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    LOGGING_PROXY_BASE,
    "POST",
    `/api/logs/${enc(logId)}/actions/push`,
    { data },
    fetchFn,
  );
}

export const readStoredLoggingCompartment = () => readStoredValue(LOGGING_COMPARTMENT_KEY);
export const writeStoredLoggingCompartment = (id: string) =>
  writeStoredValue(LOGGING_COMPARTMENT_KEY, id);
