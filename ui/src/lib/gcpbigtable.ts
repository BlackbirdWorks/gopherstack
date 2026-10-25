/** Client for the Cloud Bigtable emulator via the dashboard's in-process REST proxy. */

import { readStoredProject, writeStoredProject } from "./gcpproject.ts";
import {
  base64ToUtf8,
  listAll,
  restCall,
  shortName,
  str,
  type FetchFn,
  type Json,
} from "./gcprest.ts";

export const BIGTABLE_PROXY_BASE = "/dashboard/api/v1/gcp/bigtable/v1";
export const DEFAULT_ROW_LIMIT = 100;

const enc = encodeURIComponent;
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type InstanceInfo = {
  name: string;
  id: string;
  displayName: string;
  state: string;
  type: string;
  raw: Json;
};
export type ClusterInfo = {
  name: string;
  id: string;
  location: string;
  state: string;
  serveNodes: number;
  storageType: string;
  raw: Json;
};
export type FamilyInfo = { name: string; gcRule: string };
export type TableInfo = {
  name: string;
  id: string;
  granularity: string;
  families: FamilyInfo[];
  raw: Json;
};
export type CellInfo = { timestamp: Date | null; micros: string; value: string; binary: boolean };
export type ColumnInfo = { qualifier: string; cells: CellInfo[] };
export type FamilyRows = { name: string; columns: ColumnInfo[] };
export type RowInfo = { key: string; families: FamilyRows[]; cellCount: number };

export const instancePath = (project: string) => `/projects/${enc(project)}/instances`;

export function parseInstance(raw: Json): InstanceInfo {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    displayName: str(raw.displayName),
    state: str(raw.state) || "STATE_NOT_KNOWN",
    type: str(raw.type) || "TYPE_UNSPECIFIED",
    raw,
  };
}

export function parseCluster(raw: Json): ClusterInfo {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    location: shortName(str(raw.location)),
    state: str(raw.state) || "STATE_NOT_KNOWN",
    serveNodes: Number(raw.serveNodes ?? 0),
    storageType: str(raw.defaultStorageType) || "STORAGE_TYPE_UNSPECIFIED",
    raw,
  };
}

/** One-line summary of a GC rule ("max 3 versions", "max age 7d", unions and intersections). */
export function describeGcRule(rule: unknown): string {
  const r = obj(rule);
  if (r.maxNumVersions !== undefined) return `max ${String(r.maxNumVersions)} versions`;
  if (typeof r.maxAge === "string") return `max age ${r.maxAge}`;
  const union = obj(r.union);
  const inter = obj(r.intersection);
  if (Array.isArray(union.rules)) return `any of (${union.rules.map(describeGcRule).join("; ")})`;
  if (Array.isArray(inter.rules)) return `all of (${inter.rules.map(describeGcRule).join("; ")})`;
  return "keep forever";
}

export function parseTable(raw: Json): TableInfo {
  const name = str(raw.name);
  const families = Object.entries(obj(raw.columnFamilies))
    .map(([fam, def]) => ({ name: fam, gcRule: describeGcRule(obj(def).gcRule) }))
    .toSorted((a, b) => a.name.localeCompare(b.name));
  return {
    name,
    id: shortName(name),
    granularity: str(raw.granularity) || "MILLIS",
    families,
    raw,
  };
}

/** Bytes (base64 on the wire) as text; binary when not valid UTF-8. */
export function decodeBytes(b64: string): { text: string; binary: boolean } {
  if (!b64) return { text: "", binary: false };
  const text = base64ToUtf8(b64);
  if (text !== null) return { text, binary: false };
  const bin = atob(b64);
  const hex = Array.from(bin, (c) => (c.codePointAt(0) ?? 0).toString(16).padStart(2, "0")).join(
    "",
  );
  return { text: `0x${hex}`, binary: true };
}

export function micros(ts: string): Date | null {
  if (!/^\d+$/.test(ts)) return null;
  const ms = Number(ts) / 1000;
  return Number.isFinite(ms) ? new Date(ms) : null;
}

export function parseRow(raw: Json): RowInfo {
  let cellCount = 0;
  const families = list(raw.families).map((f) => ({
    name: str(f.name),
    columns: list(f.columns).map((c) => ({
      qualifier: decodeBytes(str(c.qualifier)).text,
      cells: list(c.cells).map((cell) => {
        const ts = str(cell.timestampMicros) || "0";
        const v = decodeBytes(str(cell.value));
        cellCount += 1;
        return { timestamp: micros(ts), micros: ts, value: v.text, binary: v.binary };
      }),
    })),
  }));
  return { key: decodeBytes(str(raw.key)).text, families, cellCount };
}

function toB64(bytes: Uint8Array): string {
  let bin = "";
  for (const b of bytes) bin += String.fromCodePoint(b);
  return btoa(bin);
}

/** The ReadRows row set selecting every key starting with `prefix` (all rows when empty). */
export function prefixRowSet(prefix: string): Json | undefined {
  if (!prefix) return undefined;
  const start = new TextEncoder().encode(prefix);
  const end = Uint8Array.from(start);
  end[end.length - 1] += 1;
  return { rowRanges: [{ startKeyClosed: toB64(start), endKeyOpen: toB64(end) }] };
}

export function readRowsBody(prefix: string, limit: number): Json {
  const body: Json = { rowsLimit: String(limit) };
  const rows = prefixRowSet(prefix);
  if (rows) body.rows = rows;
  return body;
}

export async function listInstances(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<InstanceInfo[]> {
  return (await listAll(BIGTABLE_PROXY_BASE, instancePath(project), "instances", fetchFn)).map(
    (r) => parseInstance(r),
  );
}

export async function listClusters(
  instance: string,
  fetchFn: FetchFn = fetch,
): Promise<ClusterInfo[]> {
  const body = await restCall<Json>(
    BIGTABLE_PROXY_BASE,
    "GET",
    `/${instance}/clusters`,
    undefined,
    fetchFn,
  );
  return list(body.clusters).map((r) => parseCluster(r));
}

export async function listTables(instance: string, fetchFn: FetchFn = fetch): Promise<TableInfo[]> {
  return (
    await listAll(BIGTABLE_PROXY_BASE, `/${instance}/tables`, "tables", fetchFn, "view=FULL")
  ).map((r) => parseTable(r));
}

export async function readRows(
  table: string,
  prefix: string,
  limit = DEFAULT_ROW_LIMIT,
  fetchFn: FetchFn = fetch,
): Promise<RowInfo[]> {
  const body = await restCall<Json>(
    BIGTABLE_PROXY_BASE,
    "POST",
    `/${table}:readRows`,
    readRowsBody(prefix, limit),
    fetchFn,
  );
  return list(body.rows).map((r) => parseRow(r));
}

export async function deleteTable(table: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(BIGTABLE_PROXY_BASE, "DELETE", `/${table}`, undefined, fetchFn);
}

export async function deleteInstance(instance: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(BIGTABLE_PROXY_BASE, "DELETE", `/${instance}`, undefined, fetchFn);
}

const PROJECT_KEY = "gopherstack_bigtable_project";

export const readStoredBigtableProject = () => readStoredProject(PROJECT_KEY);
export const writeStoredBigtableProject = (project: string) =>
  writeStoredProject(PROJECT_KEY, project);
