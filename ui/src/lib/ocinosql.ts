/** Client for the Oracle NoSQL Database emulator: tables, indexes, rows and SQL queries via the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  ociPostOne,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const NOSQL_PROXY_BASE = "/dashboard/api/v1/oci/nosql";
export const NOSQL_COMPARTMENT_KEY = "gopherstack_ocinosql_compartment";
export const NOSQL_PAGE_SIZE = 20;

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);

export type NosqlColumn = {
  name: string;
  type: string;
  nullable: boolean;
  generated: boolean;
  defaultValue: string;
};

export type NosqlTable = {
  id: string;
  name: string;
  state: string;
  schemaState: string;
  mode: string;
  readUnits: number;
  writeUnits: number;
  storageGb: number;
  ttlDays: number;
  autoReclaimable: boolean;
  ddl: string;
  columns: NosqlColumn[];
  primaryKey: string[];
  shardKey: string[];
  created: string;
};

export type NosqlIndex = { name: string; state: string; keys: string[] };

export type NosqlUsage = {
  started: string;
  seconds: number;
  readUnits: number;
  writeUnits: number;
  storageGb: number;
};

export type NosqlQueryPage = { rows: Json[]; columns: string[]; next: string; readUnits: number };

export type NosqlRow = { value: Json; expires: string; readUnits: number };

export function parseColumn(raw: Json): NosqlColumn {
  return {
    name: str(raw.name),
    type: str(raw.type),
    nullable: raw.isNullable !== false,
    generated: raw.isGenerated === true || raw.isAsUuid === true,
    defaultValue: str(raw.defaultValue),
  };
}

export function parseTable(raw: Json): NosqlTable {
  const limits = obj(raw.tableLimits);
  const schema = obj(raw.schema);
  return {
    id: str(raw.id),
    name: str(raw.name) || str(raw.id),
    state: str(raw.lifecycleState),
    schemaState: str(raw.schemaState),
    mode: str(limits.capacityMode),
    readUnits: num(limits.maxReadUnits),
    writeUnits: num(limits.maxWriteUnits),
    storageGb: num(limits.maxStorageInGBs),
    ttlDays: num(schema.ttl),
    autoReclaimable: raw.isAutoReclaimable === true,
    ddl: str(raw.ddlStatement),
    columns: list(schema.columns).map((c) => parseColumn(c)),
    primaryKey: strs(schema.primaryKey),
    shardKey: strs(schema.shardKey),
    created: str(raw.timeCreated),
  };
}

export function parseIndex(raw: Json): NosqlIndex {
  return {
    name: str(raw.name),
    state: str(raw.lifecycleState),
    keys: list(raw.keys).map((k) => {
      const path = str(k.jsonPath);
      return path ? `${str(k.columnName)}.${path}` : str(k.columnName);
    }),
  };
}

export function parseUsage(raw: Json): NosqlUsage {
  return {
    started: str(raw.timeStarted),
    seconds: num(raw.secondsInPeriod),
    readUnits: num(raw.readUnits),
    writeUnits: num(raw.writeUnits),
    storageGb: num(raw.storageInGBs),
  };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(NOSQL_PROXY_BASE, fetchFn);

const cq = (compartment: string) => `compartmentId=${enc(compartment)}`;
const tablePath = (compartment: string, table: string, sub = "") =>
  `/api/tables/${enc(table)}${sub}?${cq(compartment)}`;

export async function listTables(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<NosqlTable[]> {
  const items = await ociListPages(
    NOSQL_PROXY_BASE,
    "/api/tables",
    cq(compartment),
    "items",
    fetchFn,
  );
  return items.map((r) => parseTable(r));
}

export async function getTable(
  compartment: string,
  table: string,
  fetchFn: FetchFn = fetch,
): Promise<NosqlTable> {
  return parseTable(await ociGetOne(NOSQL_PROXY_BASE, tablePath(compartment, table), fetchFn));
}

export async function listIndexes(
  compartment: string,
  table: string,
  fetchFn: FetchFn = fetch,
): Promise<NosqlIndex[]> {
  const items = await ociListPages(
    NOSQL_PROXY_BASE,
    `/api/tables/${enc(table)}/indexes`,
    cq(compartment),
    "items",
    fetchFn,
  );
  return items.map((r) => parseIndex(r));
}

export async function listUsage(
  compartment: string,
  table: string,
  fetchFn: FetchFn = fetch,
): Promise<NosqlUsage[]> {
  const items = await ociListPages(
    NOSQL_PROXY_BASE,
    `/api/tables/${enc(table)}/usage`,
    cq(compartment),
    "items",
    fetchFn,
  );
  return items.map((r) => parseUsage(r));
}

export type TableLimitsInput = {
  mode: "PROVISIONED" | "ON_DEMAND";
  readUnits: number;
  writeUnits: number;
  storageGb: number;
};

/** The table name a CREATE TABLE statement declares, or "". */
export function ddlTableName(ddl: string): string {
  const m = /^\s*create\s+table\s+(?:if\s+not\s+exists\s+)?("[^"]+"|[A-Za-z_][\w.]*)/i.exec(ddl);
  return m ? m[1].replaceAll(/^"|"$/g, "") : "";
}

export async function createTable(
  compartment: string,
  ddl: string,
  limits: TableLimitsInput,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const name = ddlTableName(ddl);
  if (!name) throw new Error("The DDL must be a CREATE TABLE statement");
  const tableLimits: Json = { capacityMode: limits.mode, maxStorageInGBs: limits.storageGb };
  if (limits.mode === "PROVISIONED") {
    tableLimits.maxReadUnits = limits.readUnits;
    tableLimits.maxWriteUnits = limits.writeUnits;
  }
  await restCall(
    NOSQL_PROXY_BASE,
    "POST",
    "/api/tables",
    { compartmentId: compartment, name, ddlStatement: ddl.trim(), tableLimits },
    fetchFn,
  );
}

export async function dropTable(
  compartment: string,
  table: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(NOSQL_PROXY_BASE, "DELETE", tablePath(compartment, table), undefined, fetchFn);
}

/** One page of SELECT results; `next` is the opc-next-page token for the following page. */
export async function runQuery(
  compartment: string,
  statement: string,
  page = "",
  limit = NOSQL_PAGE_SIZE,
  fetchFn: FetchFn = fetch,
): Promise<NosqlQueryPage> {
  const qs = [`limit=${limit}`, page ? `page=${enc(page)}` : ""].filter(Boolean).join("&");
  const { body, next } = await ociPostOne(
    NOSQL_PROXY_BASE,
    `/api/query?${qs}`,
    { compartmentId: compartment, statement },
    fetchFn,
  );
  const b = obj(body);
  const rows = list(b.items);
  return {
    rows,
    columns: resultColumns(rows),
    next,
    readUnits: num(obj(b.usage).readUnitsConsumed),
  };
}

/** Union of the keys of all rows, in first-seen order. */
export function resultColumns(rows: Json[]): string[] {
  const seen = new Set<string>();
  for (const r of rows) for (const k of Object.keys(r)) seen.add(k);
  return [...seen];
}

/** Renders a cell value for the results grid. */
export function cellText(v: unknown): string {
  if (v === null || v === undefined) return "NULL";
  return typeof v === "object" ? JSON.stringify(v) : String(v);
}

/** Builds the `col:value` key parameters of a GetRow/DeleteRow call. */
export function keyParams(key: Record<string, string>): string {
  return Object.entries(key)
    .filter(([, v]) => v !== "")
    .map(([k, v]) => `key=${enc(`${k}:${v}`)}`)
    .join("&");
}

export async function getRow(
  compartment: string,
  table: string,
  key: Record<string, string>,
  fetchFn: FetchFn = fetch,
): Promise<NosqlRow | null> {
  const body = await ociGetOne(
    NOSQL_PROXY_BASE,
    `/api/tables/${enc(table)}/rows?${cq(compartment)}&${keyParams(key)}`,
    fetchFn,
  );
  if (body.value === null || body.value === undefined) return null;
  return {
    value: obj(body.value),
    expires: str(body.timeOfExpiration),
    readUnits: num(obj(body.usage).readUnitsConsumed),
  };
}

/** Puts a row; `json` must be a JSON object. */
export async function putRow(
  compartment: string,
  table: string,
  json: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  let value: unknown;
  try {
    value = JSON.parse(json);
  } catch {
    throw new Error("Row is not valid JSON");
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Row must be a JSON object");
  }
  await restCall(
    NOSQL_PROXY_BASE,
    "PUT",
    `/api/tables/${enc(table)}/rows`,
    { compartmentId: compartment, value },
    fetchFn,
  );
}

/** Deletes a row by primary key; resolves to whether a row was removed. */
export async function deleteRow(
  compartment: string,
  table: string,
  key: Record<string, string>,
  fetchFn: FetchFn = fetch,
): Promise<boolean> {
  const body = await restCall<Json>(
    NOSQL_PROXY_BASE,
    "DELETE",
    `/api/tables/${enc(table)}/rows?${cq(compartment)}&${keyParams(key)}`,
    undefined,
    fetchFn,
  );
  return body.isSuccess === true;
}

/** Which actions a table state allows. */
export const canDrop = (t: Pick<NosqlTable, "state">): boolean =>
  t.state === "ACTIVE" || t.state === "UPDATING";
export const rowsUsable = (t: Pick<NosqlTable, "state">): boolean => t.state === "ACTIVE";

export const defaultQuery = (table: string): string => `SELECT * FROM ${table} LIMIT 50`;

export const readStoredNosqlCompartment = () => readStoredValue(NOSQL_COMPARTMENT_KEY);
export const writeStoredNosqlCompartment = (id: string) =>
  writeStoredValue(NOSQL_COMPARTMENT_KEY, id);
