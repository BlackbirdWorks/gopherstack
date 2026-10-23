/** Client for the BigQuery emulator via the dashboard's in-process REST proxy. */

import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const BQ_PROXY_BASE = "/dashboard/api/v1/gcp/bigquery/bigquery/v2";
export const BQ_PROJECT_KEY = "gopherstack_bigquery_project";
export const DEFAULT_BQ_PROJECT = "gopherstack-local";
export const PREVIEW_PAGE = 25;
export const HISTORY_LIMIT = 30;
const JOB_POLLS = 40;

export const FIELD_TYPES = [
  "STRING",
  "BYTES",
  "INTEGER",
  "FLOAT",
  "NUMERIC",
  "BIGNUMERIC",
  "BOOLEAN",
  "TIMESTAMP",
  "DATE",
  "TIME",
  "DATETIME",
  "JSON",
  "GEOGRAPHY",
  "RECORD",
] as const;
export const FIELD_MODES = ["NULLABLE", "REQUIRED", "REPEATED"] as const;
export const LOAD_FORMATS = [
  { id: "CSV", label: "CSV" },
  { id: "NEWLINE_DELIMITED_JSON", label: "NDJSON" },
  { id: "PARQUET", label: "Parquet" },
] as const;

export type SchemaField = {
  name: string;
  type: string;
  mode: string;
  description?: string;
  fields?: SchemaField[];
};

/** Editable schema row; children only apply to RECORD fields. */
export type FieldDraft = {
  id: number;
  name: string;
  type: string;
  mode: string;
  children: FieldDraft[];
};

export type DatasetInfo = { id: string; location: string };
export type TableInfo = { id: string; kind: string };
export type TableDetail = {
  id: string;
  kind: string;
  schema: SchemaField[];
  numRows: string;
  numBytes: string;
  location: string;
  created: string;
};

export type TablePage = { rows: Cell[][]; total: number; nextToken: string };
export type Cell = unknown;

export type QueryResult = {
  schema: SchemaField[];
  rows: Cell[][];
  totalRows: number;
  jobId: string;
  cacheHit: boolean;
  bytesProcessed: string;
  dmlAffected: string;
  complete: boolean;
  elapsedMs: number;
};

export type HistoryEntry = {
  sql: string;
  at: number;
  ok: boolean;
  summary: string;
};

export type LoadInput = {
  uri: string;
  format: string;
  autodetect: boolean;
  dataset: string;
  table: string;
  skipRows: number;
  append: boolean;
};

const projectPath = (p: string) => `/projects/${encodeURIComponent(p)}`;
const datasetPath = (p: string, d: string) => `${projectPath(p)}/datasets/${encodeURIComponent(d)}`;
const tablePath = (p: string, d: string, t: string) =>
  `${datasetPath(p, d)}/tables/${encodeURIComponent(t)}`;

const call = <T = Json>(method: string, path: string, body?: unknown, f?: FetchFn) =>
  restCall<T>(BQ_PROXY_BASE, method, path, body, f);

export function validateBqId(id: string, what: string): string | null {
  if (!id) return `${what} is required`;
  if (!/^[A-Za-z0-9_]{1,1024}$/.test(id)) return `${what}: use letters, digits and _ only`;
  return null;
}

export function validateFieldName(name: string): string | null {
  if (!name) return "field name is required";
  if (!/^[A-Za-z_][A-Za-z0-9_]{0,299}$/.test(name)) return `invalid field name "${name}"`;
  return null;
}

export function validateGsUri(uri: string): string | null {
  const m = /^gs:\/\/([^/]+)\/(.+)$/.exec(uri.trim());
  return m ? null : "use gs://bucket/object (a * wildcard is allowed)";
}

let draftSeq = 0;

export function newDraft(name = "", type = "STRING", mode = "NULLABLE"): FieldDraft {
  draftSeq += 1;
  return { id: draftSeq, name, type, mode, children: [] };
}

/** First problem in a draft tree, or null; duplicate names are checked per level. */
export function validateDrafts(drafts: FieldDraft[]): string | null {
  const seen = new Set<string>();
  for (const d of drafts) {
    const bad = validateFieldName(d.name.trim());
    if (bad) return bad;
    const key = d.name.trim().toLowerCase();
    if (seen.has(key)) return `duplicate field "${d.name.trim()}"`;
    seen.add(key);
    const inner = validateRecord(d);
    if (inner) return inner;
  }
  return null;
}

function validateRecord(d: FieldDraft): string | null {
  if (d.type !== "RECORD") return null;
  if (d.children.length === 0) return `RECORD "${d.name.trim()}" needs at least one field`;
  return validateDrafts(d.children);
}

export function draftsToSchema(drafts: FieldDraft[]): SchemaField[] {
  return drafts.map((d) => {
    const f: SchemaField = { name: d.name.trim(), type: d.type, mode: d.mode };
    if (d.type === "RECORD") f.fields = draftsToSchema(d.children);
    return f;
  });
}

export function schemaToDrafts(fields: SchemaField[]): FieldDraft[] {
  return fields.map((f) => {
    const d = newDraft(f.name, f.type, f.mode);
    d.children = schemaToDrafts(f.fields ?? []);
    return d;
  });
}

export function parseSchema(raw: unknown): SchemaField[] {
  const fields = (raw as { fields?: Json[] } | undefined)?.fields;
  return (fields ?? []).map((f) => parseField(f));
}

function parseField(f: Json): SchemaField {
  const out: SchemaField = {
    name: str(f.name),
    type: str(f.type).toUpperCase() || "STRING",
    mode: str(f.mode) || "NULLABLE",
  };
  const kids = f.fields as Json[] | undefined;
  if (kids?.length) out.fields = kids.map((k) => parseField(k));
  return out;
}

/** "RECORD<a:INT, b:STRING>[]"-style label for a schema field. */
export function typeLabel(f: SchemaField): string {
  const base = f.type === "RECORD" ? "RECORD" : f.type;
  return f.mode === "REPEATED" ? `ARRAY<${base}>` : base;
}

/** Flattens a nested schema into dotted paths with depth for display. */
export function flattenSchema(
  fields: SchemaField[],
  prefix = "",
  depth = 0,
): { path: string; field: SchemaField; depth: number }[] {
  return fields.flatMap((f) => {
    const path = prefix ? `${prefix}.${f.name}` : f.name;
    return [{ path, field: f, depth }, ...flattenSchema(f.fields ?? [], path, depth + 1)];
  });
}

/** Decodes the wire cell ({v: ...} / {f: [...]}) into plain JS using the field schema. */
export function decodeCell(field: SchemaField, cell: unknown): unknown {
  const v = unwrap(cell);
  if (v === null || v === undefined) return null;
  if (field.mode === "REPEATED") {
    const items = Array.isArray(v) ? v : [];
    return items.map((item) => decodeValue({ ...field, mode: "NULLABLE" }, unwrap(item)));
  }
  return decodeValue(field, v);
}

function unwrap(cell: unknown): unknown {
  if (cell && typeof cell === "object" && "v" in cell) return (cell as { v: unknown }).v;
  return cell;
}

function decodeValue(field: SchemaField, v: unknown): unknown {
  if (v === null || v === undefined) return null;
  if (field.type !== "RECORD") return v;
  const cells = (v as { f?: unknown[] }).f ?? [];
  const kids = field.fields ?? [];
  return Object.fromEntries(kids.map((k, i) => [k.name, decodeCell(k, cells[i])]));
}

export function decodeRows(schema: SchemaField[], rows: unknown): Cell[][] {
  const list = (rows as { f?: unknown[] }[] | undefined) ?? [];
  return list.map((r) => schema.map((f, i) => decodeCell(f, r.f?.[i])));
}

/** Human-readable cell text: scalars verbatim, NULL for null, compact JSON-like for nested. */
export function renderCell(value: unknown): string {
  if (value === null || value === undefined) return "NULL";
  if (Array.isArray(value)) return `[${value.map((v) => renderCell(v)).join(", ")}]`;
  if (typeof value === "object") {
    const parts = Object.entries(value as Json).map(([k, v]) => `${k}: ${renderCell(v)}`);
    return `{${parts.join(", ")}}`;
  }
  return String(value);
}

export const isNested = (value: unknown) => value !== null && typeof value === "object";

export const toList = <T>(raw: Json, key: string, parse: (j: Json) => T): T[] =>
  ((raw[key] as Json[] | undefined) ?? []).map((j) => parse(j));

async function listAll<T>(
  path: string,
  key: string,
  parse: (j: Json) => T,
  f?: FetchFn,
): Promise<T[]> {
  const out: T[] = [];
  let token = "";
  do {
    const q = token ? `?pageToken=${encodeURIComponent(token)}` : "";
    const res = await call(`GET`, `${path}${q}`, undefined, f);
    out.push(...toList(res, key, parse));
    token = str(res.nextPageToken);
  } while (token);
  return out;
}

const refOf = (j: Json, key: string) => (j[key] as Json | undefined) ?? {};

export const parseDataset = (j: Json): DatasetInfo => ({
  id: str(refOf(j, "datasetReference").datasetId),
  location: str(j.location),
});

export const parseTableInfo = (j: Json): TableInfo => ({
  id: str(refOf(j, "tableReference").tableId),
  kind: str(j.type) || "TABLE",
});

export function parseTableDetail(j: Json): TableDetail {
  return {
    id: str(refOf(j, "tableReference").tableId),
    kind: str(j.type) || "TABLE",
    schema: parseSchema(j.schema),
    numRows: str(j.numRows) || "0",
    numBytes: str(j.numBytes) || "0",
    location: str(j.location),
    created: j.creationTime ? new Date(Number(j.creationTime)).toISOString() : "",
  };
}

export const listDatasets = (project: string, f?: FetchFn) =>
  listAll(`${projectPath(project)}/datasets`, "datasets", parseDataset, f);

export const listTables = (project: string, dataset: string, f?: FetchFn) =>
  listAll(`${datasetPath(project, dataset)}/tables`, "tables", parseTableInfo, f);

export async function createDataset(project: string, id: string, location: string, f?: FetchFn) {
  const body: Json = { datasetReference: { projectId: project, datasetId: id } };
  if (location.trim()) body.location = location.trim();
  await call("POST", `${projectPath(project)}/datasets`, body, f);
}

export async function deleteDataset(project: string, id: string, f?: FetchFn) {
  await call("DELETE", `${datasetPath(project, id)}?deleteContents=true`, undefined, f);
}

export async function createTable(
  project: string,
  dataset: string,
  id: string,
  schema: SchemaField[],
  f?: FetchFn,
) {
  const body = {
    tableReference: { projectId: project, datasetId: dataset, tableId: id },
    schema: { fields: schema },
  };
  await call("POST", `${datasetPath(project, dataset)}/tables`, body, f);
}

export async function deleteTable(project: string, dataset: string, id: string, f?: FetchFn) {
  await call("DELETE", tablePath(project, dataset, id), undefined, f);
}

export async function getTable(project: string, dataset: string, id: string, f?: FetchFn) {
  return parseTableDetail(await call("GET", tablePath(project, dataset, id), undefined, f));
}

export async function previewTable(
  project: string,
  dataset: string,
  table: string,
  schema: SchemaField[],
  token = "",
  f?: FetchFn,
): Promise<TablePage> {
  const q = new URLSearchParams({ maxResults: String(PREVIEW_PAGE) });
  if (token) q.set("pageToken", token);
  const res = await call("GET", `${tablePath(project, dataset, table)}/data?${q}`, undefined, f);
  return {
    rows: decodeRows(schema, res.rows),
    total: Number(res.totalRows ?? 0),
    nextToken: str(res.pageToken),
  };
}

export async function runQuery(
  project: string,
  sql: string,
  defaultDataset: string,
  f?: FetchFn,
): Promise<QueryResult> {
  const body: Json = { query: sql, useLegacySql: false, timeoutMs: 20000 };
  if (defaultDataset) body.defaultDataset = { projectId: project, datasetId: defaultDataset };
  const started = Date.now();
  const res = await call("POST", `${projectPath(project)}/queries`, body, f);
  return parseQueryResult(res, Date.now() - started);
}

export function parseQueryResult(res: Json, elapsedMs: number): QueryResult {
  const schema = parseSchema(res.schema);
  return {
    schema,
    rows: decodeRows(schema, res.rows),
    totalRows: Number(res.totalRows ?? 0),
    jobId: str(refOf(res, "jobReference").jobId),
    cacheHit: res.cacheHit === true,
    bytesProcessed: str(res.totalBytesProcessed) || "0",
    dmlAffected: str(res.numDmlAffectedRows),
    complete: res.jobComplete !== false,
    elapsedMs,
  };
}

/** One-line summary of a finished query for history and the stats bar. */
export function summarizeQuery(r: QueryResult): string {
  if (r.dmlAffected) return `${r.dmlAffected} row(s) affected`;
  if (r.schema.length === 0) return "statement completed";
  return `${r.totalRows} row(s)`;
}

export function pushHistory(
  list: HistoryEntry[],
  entry: HistoryEntry,
  limit = HISTORY_LIMIT,
): HistoryEntry[] {
  return [entry, ...list.filter((h) => h.sql !== entry.sql)].slice(0, limit);
}

export function loadJobBody(project: string, i: LoadInput): Json {
  const load: Json = {
    sourceUris: [i.uri.trim()],
    sourceFormat: i.format,
    autodetect: i.autodetect,
    destinationTable: { projectId: project, datasetId: i.dataset, tableId: i.table },
    createDisposition: "CREATE_IF_NEEDED",
    writeDisposition: i.append ? "WRITE_APPEND" : "WRITE_TRUNCATE",
  };
  if (i.format === "CSV" && i.skipRows > 0) load.skipLeadingRows = i.skipRows;
  return { configuration: { load } };
}

export type JobOutcome = { id: string; state: string; error: string; outputRows: string };

export function parseJob(j: Json): JobOutcome {
  const status = refOf(j, "status");
  const stats = refOf(refOf(j, "statistics"), "load");
  return {
    id: str(refOf(j, "jobReference").jobId),
    state: str(status.state),
    error: str(refOf(status, "errorResult").message),
    outputRows: str(stats.outputRows),
  };
}

export async function runLoad(project: string, i: LoadInput, f?: FetchFn): Promise<JobOutcome> {
  let job = parseJob(
    await call("POST", `${projectPath(project)}/jobs`, loadJobBody(project, i), f),
  );
  for (let n = 0; job.state !== "DONE" && n < JOB_POLLS; n++) {
    await new Promise((r) => {
      setTimeout(r, 250);
    });
    job = parseJob(await call("GET", `${projectPath(project)}/jobs/${job.id}`, undefined, f));
  }
  return job;
}

export function readStoredBqProject(): string {
  try {
    return localStorage.getItem(BQ_PROJECT_KEY)?.trim() || DEFAULT_BQ_PROJECT;
  } catch {
    return DEFAULT_BQ_PROJECT;
  }
}

export function writeStoredBqProject(p: string): void {
  try {
    localStorage.setItem(BQ_PROJECT_KEY, p);
  } catch {
    // storage unavailable: the choice is session-only
  }
}
