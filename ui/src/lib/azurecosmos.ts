/** Client for the Cosmos DB (SQL API) emulator via the dashboard's in-process proxy. */

export const COSMOS_PROXY_BASE = "/dashboard/api/v1/azure/cosmos";
export const PAGE_SIZES = [10, 25, 50, 100] as const;
export const DEFAULT_PAGE_SIZE = 25;
export const DEFAULT_QUERY = "SELECT * FROM c";
export const SYSTEM_FIELDS = ["_rid", "_self", "_etag", "_ts", "_attachments"] as const;

export type DatabaseInfo = { id: string; rid: string };

export type ContainerInfo = {
  id: string;
  rid: string;
  partitionKeyPath: string;
  partitionKeyKind: string;
  partitionKeyVersion: number | null;
  etag: string;
  ts: number | null;
  indexingPolicy: unknown;
  raw: Record<string, unknown>;
};

export type CosmosDocument = Record<string, unknown>;

export type QueryParam = { name: string; value: string };

export type QueryResult = { documents: CosmosDocument[]; charge: string };

export class CosmosError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "CosmosError";
    this.status = status;
    this.code = code;
  }
}

type FetchFn = typeof fetch;

export function dbUrl(db = ""): string {
  return db ? `${COSMOS_PROXY_BASE}/dbs/${encodeURIComponent(db)}` : `${COSMOS_PROXY_BASE}/dbs`;
}

export function collsUrl(db: string, coll = ""): string {
  const base = `${dbUrl(db)}/colls`;
  return coll ? `${base}/${encodeURIComponent(coll)}` : base;
}

export function docsUrl(db: string, coll: string, id = ""): string {
  const base = `${collsUrl(db, coll)}/docs`;
  return id ? `${base}/${encodeURIComponent(id)}` : base;
}

const FORBIDDEN_ID_CHARS = /[/\\#?]/;

function validateResourceId(label: string, id: string): string | null {
  if (!id) return `${label} is required`;
  if (id.length > 255) return `${label} must be at most 255 characters`;
  if (FORBIDDEN_ID_CHARS.test(id) || id.endsWith(" ")) {
    return `${label} cannot contain / \\ # ? or end with a space`;
  }
  return null;
}

export const validateDatabaseId = (id: string) => validateResourceId("Database id", id);
export const validateContainerId = (id: string) => validateResourceId("Container id", id);

/** The emulator supports exactly one path; it must look like /field or /a/b. */
export function validatePartitionKeyPath(path: string): string | null {
  if (!path) return "Partition key path is required";
  if (!path.startsWith("/")) return "Must start with /";
  if (path.length === 1) return "Name a property after the /";
  if (
    path
      .split("/")
      .slice(1)
      .some((s) => s === "")
  )
    return "Path segments cannot be empty";
  if (/[\s\\]/.test(path)) return "Path cannot contain whitespace or backslashes";
  return null;
}

/** Reads the value at a partition key path like /a/b; undefined when absent. */
export function partitionKeyValue(doc: CosmosDocument, path: string): unknown {
  let cur: unknown = doc;
  for (const seg of path.split("/").slice(1)) {
    if (cur === null || typeof cur !== "object" || Array.isArray(cur)) return undefined;
    cur = (cur as Record<string, unknown>)[seg];
  }
  return cur;
}

/** Escapes non-ASCII so the JSON survives being an HTTP header value. */
function asciiJson(v: unknown): string {
  return JSON.stringify(v).replaceAll(
    /[\u007F-\uFFFF]/g,
    (ch) => `\\u${(ch.codePointAt(0) ?? 0).toString(16).padStart(4, "0")}`,
  );
}

/** The x-ms-documentdb-partitionkey header value: a one-element JSON array. */
export function partitionKeyHeader(value: unknown): string {
  return asciiJson([value === undefined ? null : value]);
}

export type DocCheck = { error: string | null; doc: CosmosDocument | null };

/** Parses editor text into a document; it must be an object with a non-empty string id. */
export function parseDocumentText(text: string): DocCheck {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (e) {
    return {
      error: `Invalid JSON: ${e instanceof Error ? e.message : String(e)}`,
      doc: null,
    };
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { error: "A document must be a JSON object", doc: null };
  }
  const doc = parsed as CosmosDocument;
  if (typeof doc.id !== "string" || doc.id === "") {
    return { error: 'The document needs a non-empty string "id"', doc: null };
  }
  return { error: null, doc };
}

/** The document without server-managed system properties, as editable JSON text. */
export function editableText(doc: CosmosDocument): string {
  const out: CosmosDocument = {};
  for (const [k, v] of Object.entries(doc)) {
    if (!(SYSTEM_FIELDS as readonly string[]).includes(k)) out[k] = v;
  }
  return JSON.stringify(out, null, 2);
}

export function newDocumentText(partitionKeyPath: string): string {
  const field = partitionKeyPath.split("/").filter(Boolean)[0] ?? "pk";
  return JSON.stringify({ id: "", [field]: "" }, null, 2);
}

/** A parameter value is JSON when it parses (numbers, booleans, null, arrays) else a string. */
export function parseParamValue(text: string): unknown {
  const t = text.trim();
  if (t === "") return "";
  try {
    return JSON.parse(t);
  } catch {
    return text;
  }
}

export function validateParam(p: QueryParam): string | null {
  if (!/^@[A-Za-z_][A-Za-z0-9_]*$/.test(p.name)) return "Name must look like @name";
  return null;
}

export function buildQueryBody(
  query: string,
  params: QueryParam[],
): { query: string; parameters: { name: string; value: unknown }[] } {
  return {
    query,
    parameters: params.map((p) => ({
      name: p.name,
      value: parseParamValue(p.value),
    })),
  };
}

/** Parameters referenced by the query text, in first-use order. */
export function referencedParams(query: string): string[] {
  const found = new Set<string>();
  for (const m of query.matchAll(/@[A-Za-z_][A-Za-z0-9_]*/g)) found.add(m[0]);
  return [...found];
}

export function paginate<T>(items: T[], page: number, size: number): T[] {
  return items.slice(page * size, page * size + size);
}

export function pageCount(total: number, size: number): number {
  return Math.max(1, Math.ceil(total / size));
}

/** The union of top-level non-system field names, id first, for grid columns. */
export function documentColumns(docs: CosmosDocument[], max = 8): string[] {
  const names = new Set<string>();
  for (const d of docs) {
    for (const k of Object.keys(d)) {
      if (k !== "id" && !(SYSTEM_FIELDS as readonly string[]).includes(k)) names.add(k);
    }
  }
  return ["id", ...[...names].toSorted((a, b) => a.localeCompare(b)).slice(0, max)];
}

export function cellText(v: unknown): string {
  if (v === undefined) return "";
  if (typeof v === "string") return v;
  return JSON.stringify(v);
}

export function parseContainer(raw: Record<string, unknown>): ContainerInfo {
  const pk = (raw.partitionKey ?? {}) as {
    paths?: string[];
    kind?: string;
    version?: number;
  };
  return {
    id: String(raw.id ?? ""),
    rid: String(raw["_rid"] ?? ""),
    partitionKeyPath: pk.paths?.[0] ?? "",
    partitionKeyKind: pk.kind ?? "Hash",
    partitionKeyVersion: typeof pk.version === "number" ? pk.version : null,
    etag: String(raw["_etag"] ?? ""),
    ts: typeof raw["_ts"] === "number" ? raw["_ts"] : null,
    indexingPolicy: raw.indexingPolicy ?? null,
    raw,
  };
}

async function toCosmosError(res: Response): Promise<CosmosError> {
  const body = await res.text().catch(() => "");
  try {
    const parsed = JSON.parse(body) as { code?: string; message?: string };
    if (parsed.code || parsed.message) {
      return new CosmosError(
        res.status,
        parsed.code ?? `HTTP${res.status}`,
        parsed.message ?? parsed.code ?? `HTTP ${res.status}`,
      );
    }
  } catch {
    // fall through to the raw body
  }
  return new CosmosError(res.status, `HTTP${res.status}`, body.trim() || `HTTP ${res.status}`);
}

const JSON_HEADERS = {
  Accept: "application/json",
  "Content-Type": "application/json",
};

async function send(url: string, init: RequestInit, fetchFn: FetchFn): Promise<Response> {
  let res: Response;
  try {
    res = await fetchFn(url, init);
  } catch (e) {
    throw new CosmosError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await toCosmosError(res);
  return res;
}

export async function listDatabases(fetchFn: FetchFn = fetch): Promise<DatabaseInfo[]> {
  const res = await send(dbUrl(), { headers: JSON_HEADERS }, fetchFn);
  const data = (await res.json()) as {
    Databases?: { id: string; _rid?: string }[];
  };
  return (data.Databases ?? []).map((d) => ({
    id: d.id,
    rid: d["_rid"] ?? "",
  }));
}

export async function createDatabase(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await send(
    dbUrl(),
    { method: "POST", headers: JSON_HEADERS, body: JSON.stringify({ id }) },
    fetchFn,
  );
}

export async function deleteDatabase(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await send(dbUrl(id), { method: "DELETE", headers: JSON_HEADERS }, fetchFn);
}

export async function listContainers(
  db: string,
  fetchFn: FetchFn = fetch,
): Promise<ContainerInfo[]> {
  const res = await send(collsUrl(db), { headers: JSON_HEADERS }, fetchFn);
  const data = (await res.json()) as {
    DocumentCollections?: Record<string, unknown>[];
  };
  return (data.DocumentCollections ?? []).map((c) => parseContainer(c));
}

export async function getContainer(
  db: string,
  coll: string,
  fetchFn: FetchFn = fetch,
): Promise<ContainerInfo> {
  const res = await send(collsUrl(db, coll), { headers: JSON_HEADERS }, fetchFn);
  return parseContainer((await res.json()) as Record<string, unknown>);
}

export async function createContainer(
  db: string,
  id: string,
  partitionKeyPath: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(
    collsUrl(db),
    {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify({
        id,
        partitionKey: { paths: [partitionKeyPath], kind: "Hash" },
      }),
    },
    fetchFn,
  );
}

export async function deleteContainer(
  db: string,
  coll: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(collsUrl(db, coll), { method: "DELETE", headers: JSON_HEADERS }, fetchFn);
}

export async function listDocuments(
  db: string,
  coll: string,
  fetchFn: FetchFn = fetch,
): Promise<CosmosDocument[]> {
  const res = await send(docsUrl(db, coll), { headers: JSON_HEADERS }, fetchFn);
  return ((await res.json()) as { Documents?: CosmosDocument[] }).Documents ?? [];
}

export async function queryDocuments(
  db: string,
  coll: string,
  query: string,
  params: QueryParam[] = [],
  fetchFn: FetchFn = fetch,
): Promise<QueryResult> {
  const res = await send(
    docsUrl(db, coll),
    {
      method: "POST",
      headers: {
        ...JSON_HEADERS,
        "Content-Type": "application/query+json",
        "x-ms-documentdb-isquery": "true",
      },
      body: JSON.stringify(buildQueryBody(query, params)),
    },
    fetchFn,
  );
  const data = (await res.json()) as { Documents?: CosmosDocument[] };
  return {
    documents: data.Documents ?? [],
    charge: res.headers.get("x-ms-request-charge") ?? "",
  };
}

export async function getDocument(
  db: string,
  coll: string,
  id: string,
  pkValue: unknown,
  fetchFn: FetchFn = fetch,
): Promise<CosmosDocument> {
  const res = await send(
    docsUrl(db, coll, id),
    {
      headers: {
        ...JSON_HEADERS,
        "x-ms-documentdb-partitionkey": partitionKeyHeader(pkValue),
      },
    },
    fetchFn,
  );
  return (await res.json()) as CosmosDocument;
}

/** Creates (or, with upsert, inserts-or-replaces) a document; the body carries its own pk. */
export async function saveDocument(
  db: string,
  coll: string,
  doc: CosmosDocument,
  upsert: boolean,
  fetchFn: FetchFn = fetch,
): Promise<CosmosDocument> {
  const headers: Record<string, string> = { ...JSON_HEADERS };
  if (upsert) headers["x-ms-documentdb-is-upsert"] = "true";
  const res = await send(
    docsUrl(db, coll),
    { method: "POST", headers, body: JSON.stringify(doc) },
    fetchFn,
  );
  return (await res.json()) as CosmosDocument;
}

export async function replaceDocument(
  db: string,
  coll: string,
  id: string,
  pkValue: unknown,
  doc: CosmosDocument,
  etag = "",
  fetchFn: FetchFn = fetch,
): Promise<CosmosDocument> {
  const headers: Record<string, string> = {
    ...JSON_HEADERS,
    "x-ms-documentdb-partitionkey": partitionKeyHeader(pkValue),
  };
  if (etag) headers["If-Match"] = etag;
  const res = await send(
    docsUrl(db, coll, id),
    { method: "PUT", headers, body: JSON.stringify(doc) },
    fetchFn,
  );
  return (await res.json()) as CosmosDocument;
}

export async function deleteDocument(
  db: string,
  coll: string,
  id: string,
  pkValue: unknown,
  etag = "",
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const headers: Record<string, string> = {
    ...JSON_HEADERS,
    "x-ms-documentdb-partitionkey": partitionKeyHeader(pkValue),
  };
  if (etag) headers["If-Match"] = etag;
  await send(docsUrl(db, coll, id), { method: "DELETE", headers }, fetchFn);
}

/** The server-managed _etag of a document, or empty. */
export function etagOf(doc: CosmosDocument): string {
  return String(doc["_etag"] ?? "");
}

/** The document's _ts epoch seconds, or null. */
export function tsOf(doc: CosmosDocument): number | null {
  const v = doc["_ts"];
  return typeof v === "number" ? v : null;
}
