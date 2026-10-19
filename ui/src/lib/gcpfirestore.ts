/** Client and value model for the Firestore emulator via the dashboard's in-process REST proxy. */

import { isBase64, restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const FIRESTORE_PROXY_BASE = "/dashboard/api/v1/gcp/firestore/v1";
export const FIRESTORE_PROJECT_KEY = "gopherstack_firestore_project";
export const FIRESTORE_DATABASE_KEY = "gopherstack_firestore_database";
export const DEFAULT_FIRESTORE_PROJECT = "gopherstack-local";
export const DEFAULT_FIRESTORE_DATABASE = "(default)";
export const PAGE_SIZE = 50;

export type FsCtx = { project: string; database: string };

export type FsType =
  | "string"
  | "integer"
  | "double"
  | "boolean"
  | "timestamp"
  | "geopoint"
  | "reference"
  | "array"
  | "map"
  | "null"
  | "bytes";

export const FS_TYPES: readonly FsType[] = [
  "string",
  "integer",
  "double",
  "boolean",
  "timestamp",
  "geopoint",
  "reference",
  "array",
  "map",
  "null",
  "bytes",
];

/** Editable form of one Firestore value (array/map carry children). */
export type FsNode = {
  id: number;
  key: string;
  type: FsType;
  value: string;
  lat: string;
  lng: string;
  children: FsNode[];
};

let nextId = 1;

export function newNode(type: FsType = "string", key = ""): FsNode {
  const value = type === "boolean" ? "true" : "";
  return { id: nextId++, key, type, value, lat: "", lng: "", children: [] };
}

export const docsRoot = (c: FsCtx) => `projects/${c.project}/databases/${c.database}/documents`;

// ---------- value conversion ----------

const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;
const RFC3339 = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$/;
const SPECIAL_DOUBLES = new Set(["NaN", "Infinity", "-Infinity"]);

export function normalizeReference(value: string, c: FsCtx): string {
  const v = value.trim();
  if (v.startsWith("projects/")) return v;
  return `${docsRoot(c)}/${v.replace(/^\/+/, "")}`;
}

function validateInteger(v: string): string | null {
  if (!/^[+-]?\d+$/.test(v)) return "integer must be a whole number";
  const big = BigInt(v);
  return big < INT64_MIN || big > INT64_MAX ? "integer is outside the 64-bit range" : null;
}

function validateDouble(v: string): string | null {
  if (SPECIAL_DOUBLES.has(v)) return null;
  return v !== "" && Number.isFinite(Number(v)) ? null : "double must be a number";
}

function validateTimestamp(v: string): string | null {
  return RFC3339.test(v) && !Number.isNaN(Date.parse(v))
    ? null
    : "timestamp must be RFC 3339, e.g. 2024-01-02T03:04:05Z";
}

function outOfRange(raw: string, limit: number): boolean {
  const num = Number(raw);
  return raw.trim() === "" || !Number.isFinite(num) || num < -limit || num > limit;
}

function validateGeopoint(n: FsNode): string | null {
  if (outOfRange(n.lat, 90)) return "latitude must be between -90 and 90";
  if (outOfRange(n.lng, 180)) return "longitude must be between -180 and 180";
  return null;
}

function validateReference(v: string, c: FsCtx): string | null {
  if (!v) return "reference path is required";
  const rel = normalizeReference(v, c).split("/documents/")[1] ?? "";
  const segs = rel.split("/").filter(Boolean);
  return segs.length > 0 && segs.length % 2 === 0
    ? null
    : "reference must point at a document (collection/doc)";
}

/** Returns a problem description for a single scalar/leaf node, or null. */
function validateLeaf(n: FsNode, c: FsCtx): string | null {
  const v = n.value.trim();
  switch (n.type) {
    case "integer":
      return validateInteger(v);
    case "boolean":
      return v === "true" || v === "false" ? null : "choose true or false";
    case "double":
      return validateDouble(v);
    case "timestamp":
      return validateTimestamp(v);
    case "geopoint":
      return validateGeopoint(n);
    case "reference":
      return validateReference(v, c);
    case "bytes":
      return v === "" || isBase64(v) ? null : "bytes must be base64";
    default:
      return null;
  }
}

function duplicateKey(nodes: FsNode[]): string | null {
  const seen = new Set<string>();
  for (const n of nodes) {
    if (seen.has(n.key)) return n.key;
    seen.add(n.key);
  }
  return null;
}

/** Returns the first problem found in a field list (names, duplicates, values), or null. */
export function validateFields(nodes: FsNode[], c: FsCtx): string | null {
  for (const n of nodes) {
    if (!n.key) return "field names cannot be empty";
    if (n.key.startsWith("__") && n.key.endsWith("__")) return `"${n.key}" is a reserved name`;
  }
  const dup = duplicateKey(nodes);
  if (dup !== null) return `duplicate field "${dup}"`;
  for (const n of nodes) {
    const p = validateNode(n, c);
    if (p) return `${n.key}: ${p}`;
  }
  return null;
}

export function validateNode(n: FsNode, c: FsCtx): string | null {
  if (n.type === "array") {
    for (const [i, ch] of n.children.entries()) {
      const p = validateNode(ch, c);
      if (p) return `[${i}] ${p}`;
    }
    return null;
  }
  if (n.type === "map") return validateFields(n.children, c);
  return validateLeaf(n, c);
}

export function nodeToValue(n: FsNode, c: FsCtx): Json {
  const v = n.value.trim();
  switch (n.type) {
    case "string":
      return { stringValue: n.value };
    case "integer":
      return { integerValue: String(BigInt(v)) };
    case "double":
      return { doubleValue: SPECIAL_DOUBLES.has(v) ? v : Number(v) };
    case "boolean":
      return { booleanValue: v === "true" };
    case "timestamp":
      return { timestampValue: v };
    case "geopoint":
      return { geoPointValue: { latitude: Number(n.lat), longitude: Number(n.lng) } };
    case "reference":
      return { referenceValue: normalizeReference(v, c) };
    case "bytes":
      return { bytesValue: v };
    case "null":
      return { nullValue: "NULL_VALUE" };
    case "array":
      return { arrayValue: { values: n.children.map((ch) => nodeToValue(ch, c)) } };
    case "map":
      return { mapValue: { fields: nodesToFields(n.children, c) } };
  }
}

export function nodesToFields(nodes: FsNode[], c: FsCtx): Json {
  return Object.fromEntries(nodes.map((n) => [n.key, nodeToValue(n, c)]));
}

const VALUE_KEYS: Record<string, FsType> = {
  stringValue: "string",
  integerValue: "integer",
  doubleValue: "double",
  booleanValue: "boolean",
  timestampValue: "timestamp",
  geoPointValue: "geopoint",
  referenceValue: "reference",
  arrayValue: "array",
  mapValue: "map",
  nullValue: "null",
  bytesValue: "bytes",
};

export function valueType(raw: unknown): FsType | null {
  if (!raw || typeof raw !== "object") return null;
  for (const [k, t] of Object.entries(VALUE_KEYS)) if (k in raw) return t;
  return null;
}

export function parseValue(raw: Json, key = ""): FsNode {
  const type = valueType(raw) ?? "null";
  const n = newNode(type, key);
  switch (type) {
    case "string":
      n.value = str(raw.stringValue);
      break;
    case "integer":
      n.value = String(raw.integerValue ?? "0");
      break;
    case "double":
      n.value = String(raw.doubleValue ?? "0");
      break;
    case "boolean":
      n.value = raw.booleanValue === true ? "true" : "false";
      break;
    case "timestamp":
      n.value = str(raw.timestampValue);
      break;
    case "reference":
      n.value = str(raw.referenceValue);
      break;
    case "bytes":
      n.value = str(raw.bytesValue);
      break;
    case "geopoint": {
      const g = raw.geoPointValue as { latitude?: number; longitude?: number };
      n.lat = String(g.latitude ?? 0);
      n.lng = String(g.longitude ?? 0);
      break;
    }
    case "array": {
      const a = raw.arrayValue as { values?: Json[] };
      n.children = (a.values ?? []).map((v) => parseValue(v));
      break;
    }
    case "map":
      n.children = fieldsToNodes((raw.mapValue as { fields?: Json }).fields ?? {});
      break;
    default:
      break;
  }
  return n;
}

export function fieldsToNodes(fields: Json): FsNode[] {
  return Object.entries(fields).map(([k, v]) => parseValue(v as Json, k));
}

/** Parses the raw-JSON editor text ({name: Value}); returns an error string on failure. */
export function nodesFromJsonText(text: string): { nodes: FsNode[] } | { error: string } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text.trim() || "{}");
  } catch (e) {
    return { error: e instanceof Error ? e.message : "invalid JSON" };
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
    return { error: "expected a JSON object of field name to Value" };
  const bad = Object.entries(parsed).find(([, v]) => valueType(v) === null);
  if (bad)
    return { error: `field "${bad[0]}" is not a Firestore Value (e.g. {"stringValue":"x"})` };
  return { nodes: fieldsToNodes(parsed as Json) };
}

/** Serializes either editor mode into a Firestore fields map, or reports what is wrong. */
export function resolveFields(
  mode: "typed" | "raw",
  nodes: FsNode[],
  rawText: string,
  c: FsCtx,
): { fields: Json } | { error: string } {
  let list = nodes;
  if (mode === "raw") {
    const parsed = nodesFromJsonText(rawText);
    if ("error" in parsed) return parsed;
    list = parsed.nodes;
  }
  const problem = validateFields(list, c);
  return problem ? { error: problem } : { fields: nodesToFields(list, c) };
}

/** Quotes a field name for use in an updateMask / FieldReference. */
export function fieldPath(name: string): string {
  if (/^[A-Za-z_][A-Za-z_0-9]*$/.test(name)) return name;
  return `\`${name.replaceAll("\\", "\\\\").replaceAll("`", "\\`")}\``;
}

export function valueSummary(raw: unknown): string {
  const t = valueType(raw);
  const r = (raw ?? {}) as Json;
  switch (t) {
    case "string":
      return str(r.stringValue);
    case "integer":
      return String(r.integerValue);
    case "double":
      return String(r.doubleValue);
    case "boolean":
      return String(r.booleanValue === true);
    case "timestamp":
      return str(r.timestampValue);
    case "reference":
      return `→ ${(str(r.referenceValue).split("/documents/")[1] ?? str(r.referenceValue)) || ""}`;
    case "geopoint": {
      const g = r.geoPointValue as { latitude?: number; longitude?: number };
      return `${g.latitude ?? 0}, ${g.longitude ?? 0}`;
    }
    case "array":
      return `[${((r.arrayValue as { values?: unknown[] }).values ?? []).length} items]`;
    case "map":
      return `{${Object.keys((r.mapValue as { fields?: Json }).fields ?? {}).length} fields}`;
    case "bytes":
      return `bytes(${atobLength(str(r.bytesValue))})`;
    default:
      return "null";
  }
}

function atobLength(b64: string): number {
  try {
    return atob(b64).length;
  } catch {
    return 0;
  }
}

// ---------- documents ----------

export type FsDoc = {
  name: string;
  /** Path relative to the database's documents root, e.g. users/alice. */
  segs: string[];
  id: string;
  createTime: string;
  updateTime: string;
  fields: Json;
};

export function relSegments(name: string): string[] {
  const rel = name.split("/documents/")[1] ?? "";
  return rel.split("/").filter(Boolean);
}

export function parseDoc(raw: Json): FsDoc {
  const name = str(raw.name);
  const segs = relSegments(name);
  return {
    name,
    segs,
    id: segs.at(-1) ?? "",
    createTime: str(raw.createTime),
    updateTime: str(raw.updateTime),
    fields: (raw.fields as Json | undefined) ?? {},
  };
}

/** Field names ordered by how many documents carry them, capped at max. */
export function gridColumns(docs: FsDoc[], max = 6): string[] {
  const counts = new Map<string, number>();
  for (const d of docs)
    for (const k of Object.keys(d.fields)) counts.set(k, (counts.get(k) ?? 0) + 1);
  return [...counts.entries()]
    .toSorted((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .slice(0, max)
    .map(([k]) => k);
}

/** Signature that changes whenever any listed document is added, removed or modified. */
export function docsSignature(docs: FsDoc[]): string {
  return docs.map((d) => `${d.name}@${d.updateTime}`).join("|");
}

// ---------- REST client ----------

const enc = (segs: string[]) => segs.map((s) => encodeURIComponent(s)).join("/");

function dbPath(c: FsCtx, segs: string[] = []): string {
  const base = `/projects/${encodeURIComponent(c.project)}/databases/${encodeURIComponent(c.database)}/documents`;
  return segs.length > 0 ? `${base}/${enc(segs)}` : base;
}

const call = <T = Json>(
  method: string,
  path: string,
  body?: unknown,
  fetchFn: FetchFn = fetch,
): Promise<T> => restCall<T>(FIRESTORE_PROXY_BASE, method, path, body, fetchFn);

export async function listCollectionIds(
  c: FsCtx,
  parent: string[],
  fetchFn: FetchFn = fetch,
): Promise<string[]> {
  const out: string[] = [];
  let token = "";
  do {
    const body = await call<{ collectionIds?: string[]; nextPageToken?: string }>(
      "POST",
      `${dbPath(c, parent)}:listCollectionIds`,
      { pageSize: 300, ...(token ? { pageToken: token } : {}) },
      fetchFn,
    );
    out.push(...(body.collectionIds ?? []));
    token = body.nextPageToken ?? "";
  } while (token);
  return out.toSorted((a, b) => a.localeCompare(b));
}

export async function listDocuments(
  c: FsCtx,
  collection: string[],
  pageToken = "",
  pageSize = PAGE_SIZE,
  fetchFn: FetchFn = fetch,
): Promise<{ docs: FsDoc[]; next: string }> {
  const q = `?pageSize=${pageSize}${pageToken ? `&pageToken=${encodeURIComponent(pageToken)}` : ""}`;
  const body = await call<{ documents?: Json[]; nextPageToken?: string }>(
    "GET",
    `${dbPath(c, collection)}${q}`,
    undefined,
    fetchFn,
  );
  return { docs: (body.documents ?? []).map((r) => parseDoc(r)), next: body.nextPageToken ?? "" };
}

export async function getDocument(c: FsCtx, doc: string[], fetchFn: FetchFn = fetch) {
  return parseDoc(await call("GET", dbPath(c, doc), undefined, fetchFn));
}

export async function createDocument(
  c: FsCtx,
  collection: string[],
  id: string,
  fields: Json,
  fetchFn: FetchFn = fetch,
): Promise<FsDoc> {
  const q = id ? `?documentId=${encodeURIComponent(id)}` : "";
  return parseDoc(await call("POST", `${dbPath(c, collection)}${q}`, { fields }, fetchFn));
}

/** merge touches only the supplied top-level fields; replace overwrites the whole document. */
export async function saveDocument(
  c: FsCtx,
  doc: string[],
  fields: Json,
  mode: "merge" | "replace",
  mustExist: boolean,
  fetchFn: FetchFn = fetch,
): Promise<FsDoc> {
  const params: string[] = [];
  if (mode === "merge") {
    const keys = Object.keys(fields);
    // An empty mask would replace everything, so merging nothing is a plain read.
    if (keys.length === 0) return getDocument(c, doc, fetchFn);
    for (const k of keys) params.push(`updateMask.fieldPaths=${encodeURIComponent(fieldPath(k))}`);
  }
  if (mustExist) params.push("currentDocument.exists=true");
  const q = params.length > 0 ? `?${params.join("&")}` : "";
  return parseDoc(await call("PATCH", `${dbPath(c, doc)}${q}`, { fields }, fetchFn));
}

export async function deleteDocument(c: FsCtx, doc: string[], fetchFn: FetchFn = fetch) {
  await call("DELETE", dbPath(c, doc), undefined, fetchFn);
}

export async function runQuery(
  c: FsCtx,
  scope: string[],
  structuredQuery: Json,
  fetchFn: FetchFn = fetch,
): Promise<FsDoc[]> {
  const res = await call<Json[]>(
    "POST",
    `${dbPath(c, scope)}:runQuery`,
    { structuredQuery },
    fetchFn,
  );
  return res.filter((r) => r.document).map((r) => parseDoc(r.document as Json));
}

export type AggOp = "count" | "sum" | "avg";

export function aggregationBody(op: AggOp, field: string, structuredQuery: Json): Json {
  const agg: Json = { alias: "result" };
  if (op === "count") agg.count = {};
  else agg[op] = { field: { fieldPath: field.trim() } };
  return { structuredAggregationQuery: { structuredQuery, aggregations: [agg] } };
}

export async function runAggregation(
  c: FsCtx,
  scope: string[],
  op: AggOp,
  field: string,
  structuredQuery: Json,
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const res = await call<{ result?: { aggregateFields?: Json } }[]>(
    "POST",
    `${dbPath(c, scope)}:runAggregationQuery`,
    aggregationBody(op, field, structuredQuery),
    fetchFn,
  );
  const v = res.find((r) => r.result)?.result?.aggregateFields?.result;
  return v === undefined ? "no result" : valueSummary(v);
}

// ---------- query builder ----------

export type FsOp =
  | "EQUAL"
  | "NOT_EQUAL"
  | "LESS_THAN"
  | "LESS_THAN_OR_EQUAL"
  | "GREATER_THAN"
  | "GREATER_THAN_OR_EQUAL"
  | "ARRAY_CONTAINS"
  | "ARRAY_CONTAINS_ANY"
  | "IN"
  | "NOT_IN"
  | "IS_NULL"
  | "IS_NOT_NULL"
  | "IS_NAN"
  | "IS_NOT_NAN";

export const FS_OPS: readonly FsOp[] = [
  "EQUAL",
  "NOT_EQUAL",
  "LESS_THAN",
  "LESS_THAN_OR_EQUAL",
  "GREATER_THAN",
  "GREATER_THAN_OR_EQUAL",
  "ARRAY_CONTAINS",
  "ARRAY_CONTAINS_ANY",
  "IN",
  "NOT_IN",
  "IS_NULL",
  "IS_NOT_NULL",
  "IS_NAN",
  "IS_NOT_NAN",
];

export const FILTER_VALUE_TYPES: readonly FsType[] = [
  "string",
  "integer",
  "double",
  "boolean",
  "timestamp",
  "reference",
  "bytes",
  "null",
];

const UNARY = new Set<FsOp>(["IS_NULL", "IS_NOT_NULL", "IS_NAN", "IS_NOT_NAN"]);
const LIST_OPS = new Set<FsOp>(["IN", "NOT_IN", "ARRAY_CONTAINS_ANY"]);

export const isUnaryOp = (op: FsOp) => UNARY.has(op);
export const isListOp = (op: FsOp) => LIST_OPS.has(op);

export type FsFilter = {
  kind: "filter";
  id: number;
  field: string;
  op: FsOp;
  type: FsType;
  value: string;
};

export type FsGroup = {
  kind: "group";
  id: number;
  op: "AND" | "OR";
  items: (FsFilter | FsGroup)[];
};

export const newFilter = (): FsFilter => ({
  kind: "filter",
  id: nextId++,
  field: "",
  op: "EQUAL",
  type: "string",
  value: "",
});

export const newGroup = (op: "AND" | "OR" = "AND"): FsGroup => ({
  kind: "group",
  id: nextId++,
  op,
  items: [],
});

export type CursorKind = "none" | "startAt" | "startAfter" | "endAt" | "endBefore";

export type FsQuerySpec = {
  where: FsGroup;
  orderBy: { id: number; field: string; dir: "ASCENDING" | "DESCENDING" }[];
  limit: string;
  offset: string;
  select: string;
  allDescendants: boolean;
  startKind: "none" | "startAt" | "startAfter";
  startValues: string;
  endKind: "none" | "endAt" | "endBefore";
  endValues: string;
};

export const newQuerySpec = (): FsQuerySpec => ({
  where: newGroup(),
  orderBy: [],
  limit: "",
  offset: "",
  select: "",
  allDescendants: false,
  startKind: "none",
  startValues: "",
  endKind: "none",
  endValues: "",
});

function leafValue(type: FsType, text: string, c: FsCtx): Json | string {
  const n = newNode(type);
  n.value = text;
  const p = validateLeaf(n, c);
  return p ?? nodeToValue(n, c);
}

type Built<T> = { ok: T } | { error: string };

function buildFilter(f: FsFilter, c: FsCtx): Built<Json> {
  const field = { fieldPath: f.field.trim() };
  if (!f.field.trim()) return { error: "every filter needs a field" };
  if (isUnaryOp(f.op)) return { ok: { unaryFilter: { op: f.op, field } } };
  if (isListOp(f.op)) {
    const parts = f.value.split(",").map((s) => s.trim());
    const values: Json[] = [];
    for (const part of parts) {
      const v = leafValue(f.type, part, c);
      if (typeof v === "string") return { error: `${f.field}: ${v}` };
      values.push(v);
    }
    if (parts.length === 0 || (parts.length === 1 && parts[0] === "")) {
      return { error: `${f.field}: ${f.op} needs a comma-separated list` };
    }
    return { ok: { fieldFilter: { field, op: f.op, value: { arrayValue: { values } } } } };
  }
  const v = leafValue(f.type, f.value, c);
  if (typeof v === "string") return { error: `${f.field}: ${v}` };
  return { ok: { fieldFilter: { field, op: f.op, value: v } } };
}

function buildGroup(g: FsGroup, c: FsCtx): Built<Json | null> {
  const parts: Json[] = [];
  for (const item of g.items) {
    const r = item.kind === "filter" ? buildFilter(item, c) : buildGroup(item, c);
    if ("error" in r) return r;
    if (r.ok) parts.push(r.ok);
  }
  if (parts.length === 0) return { ok: null };
  if (parts.length === 1) return { ok: parts[0] };
  return { ok: { compositeFilter: { op: g.op, filters: parts } } };
}

/** Infers a Firestore Value from a JSON scalar (cursor values are typed by their JSON form). */
export function inferValue(v: unknown): Json | null {
  if (v === null) return { nullValue: "NULL_VALUE" };
  if (typeof v === "string") return { stringValue: v };
  if (typeof v === "boolean") return { booleanValue: v };
  if (typeof v === "number")
    return Number.isInteger(v) ? { integerValue: String(v) } : { doubleValue: v };
  return null;
}

function buildCursor(text: string, before: boolean): Built<Json> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { error: 'cursor values must be a JSON array, e.g. ["Alice", 30]' };
  }
  if (!Array.isArray(parsed) || parsed.length === 0)
    return { error: 'cursor values must be a non-empty JSON array, e.g. ["Alice", 30]' };
  const values: Json[] = [];
  for (const item of parsed) {
    const v = inferValue(item);
    if (!v) return { error: "cursor values must be strings, numbers, booleans or null" };
    values.push(v);
  }
  return { ok: { values, before } };
}

/** Builds a StructuredQuery for the given collection, or an error string. */
export function buildStructuredQuery(
  collectionId: string,
  spec: FsQuerySpec,
  c: FsCtx,
): Built<Json> {
  const q: Json = { from: [{ collectionId, allDescendants: spec.allDescendants }] };

  const where = buildGroup(spec.where, c);
  if ("error" in where) return where;
  if (where.ok) q.where = where.ok;

  const order = spec.orderBy.filter((o) => o.field.trim());
  if (order.length > 0)
    q.orderBy = order.map((o) => ({ field: { fieldPath: o.field.trim() }, direction: o.dir }));

  const select = spec.select
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  if (select.length > 0) q.select = { fields: select.map((path) => ({ fieldPath: path })) };

  for (const [name, raw] of [
    ["limit", spec.limit],
    ["offset", spec.offset],
  ] as const) {
    const t = raw.trim();
    if (!t) continue;
    if (!/^\d+$/.test(t) || Number(t) > 2147483647)
      return { error: `${name} must be a whole number` };
    q[name] = Number(t);
  }

  if (spec.startKind !== "none") {
    const cur = buildCursor(spec.startValues, spec.startKind === "startAt");
    if ("error" in cur) return cur;
    q.startAt = cur.ok;
  }
  if (spec.endKind !== "none") {
    const cur = buildCursor(spec.endValues, spec.endKind === "endBefore");
    if ("error" in cur) return cur;
    q.endAt = cur.ok;
  }
  return { ok: q };
}

// ---------- storage ----------

function readKey(key: string, fallback: string, storage?: Pick<Storage, "getItem">): string {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    return s?.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredFirestoreProject = (storage?: Pick<Storage, "getItem">) =>
  readKey(FIRESTORE_PROJECT_KEY, DEFAULT_FIRESTORE_PROJECT, storage);

export const readStoredFirestoreDatabase = (storage?: Pick<Storage, "getItem">) =>
  readKey(FIRESTORE_DATABASE_KEY, DEFAULT_FIRESTORE_DATABASE, storage);

export function writeStoredFirestore(project: string, database: string): void {
  try {
    window.localStorage.setItem(FIRESTORE_PROJECT_KEY, project);
    window.localStorage.setItem(FIRESTORE_DATABASE_KEY, database);
  } catch {
    // storage unavailable: the choice just won't persist
  }
}
