/** Client for the Azure Table emulator via the dashboard's in-process proxy. */

export const TABLE_PROXY_BASE = "/dashboard/api/v1/azure/table";
export const COSMOS_TABLE_PROXY_BASE = "/dashboard/api/v1/azure/cosmos-table";
export const DEFAULT_ACCOUNT = "devstoreaccount1";
/** Selects the Cosmos DB Table API proxy; Cosmos has no account path segment, the name is a placeholder. */
export const COSMOS_TABLE_ACCOUNT = "cosmosdb";
export const PAGE_SIZES = [25, 50, 100, 250] as const;
export const DEFAULT_PAGE_SIZE = 25;
export const SYSTEM_PROPERTIES = ["PartitionKey", "RowKey", "Timestamp"] as const;

export const EDM_TYPES = [
  "Edm.String",
  "Edm.Int32",
  "Edm.Int64",
  "Edm.Double",
  "Edm.Boolean",
  "Edm.DateTime",
  "Edm.Guid",
  "Edm.Binary",
] as const;
export type EdmType = (typeof EDM_TYPES)[number];

export type TableInfo = { name: string };

/** A property with its value held as editable text, whatever its EDM type. */
export type TypedValue = { type: EdmType; value: string };

export type Entity = {
  partitionKey: string;
  rowKey: string;
  timestamp: string;
  etag: string;
  props: Record<string, TypedValue>;
};

export type Continuation = { partitionKey: string; rowKey: string };

export type EntityPage = {
  entities: Entity[];
  next: Continuation | null;
};

export type PropertyDraft = { name: string; type: EdmType; value: string };

export class TableError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "TableError";
    this.status = status;
    this.code = code;
  }
}

type FetchFn = typeof fetch;

const INT32_MIN = -2147483648n;
const INT32_MAX = 2147483647n;
const INT64_MIN = -9223372036854775808n;
const INT64_MAX = 9223372036854775807n;
const GUID_RE = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
const BASE64_RE = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;
const INTEGER_RE = /^[+-]?\d+$/;
const DECIMAL_RE = /^[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?$/;

/** Doubles a single quote, the OData escape for string literals and keys. */
export function escapeOData(s: string): string {
  return s.replaceAll("'", "''");
}

export function keyPredicate(partitionKey: string, rowKey: string): string {
  return `PartitionKey='${escapeOData(partitionKey)}',RowKey='${escapeOData(rowKey)}'`;
}

export function tableUrl(account: string, resource = "", query = ""): string {
  const base = account === COSMOS_TABLE_ACCOUNT ? COSMOS_TABLE_PROXY_BASE : TABLE_PROXY_BASE;
  let url = `${base}/${encodeURIComponent(account)}`;
  if (resource) url += `/${resource}`;
  return query ? `${url}?${query}` : url;
}

export function entityUrl(
  account: string,
  table: string,
  partitionKey: string,
  rowKey: string,
): string {
  const predicate = encodeURIComponent(keyPredicate(partitionKey, rowKey));
  return tableUrl(account, `${encodeURIComponent(table)}(${predicate})`);
}

export function tableItemUrl(account: string, table: string): string {
  return tableUrl(account, `Tables('${encodeURIComponent(escapeOData(table))}')`);
}

export function validateTableName(name: string): string | null {
  if (!name) return "Name is required";
  if (name.length < 3 || name.length > 63) return "Must be 3-63 characters";
  if (!/^[A-Za-z]/.test(name)) return "Must start with a letter";
  if (!/^[A-Za-z0-9]+$/.test(name)) return "Use letters and numbers only";
  if (name.toLowerCase() === "tables") return '"tables" is a reserved name';
  return null;
}

export function validateKey(label: string, value: string): string | null {
  if (value === "") return null;
  if (value.length > 512) return `${label} must be at most 512 characters`;
  if (/[/\\#?]/.test(value) || [...value].some((ch) => /\p{Cc}/u.test(ch))) {
    return `${label} cannot contain / \\ # ? or control characters`;
  }
  return null;
}

export function validatePropertyName(name: string): string | null {
  if (!name) return "Property name is required";
  if (name.length > 255) return "Must be at most 255 characters";
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    return "Use letters, numbers and underscores, starting with a letter or underscore";
  }
  if ((SYSTEM_PROPERTIES as readonly string[]).includes(name))
    return `${name} is a system property`;
  return null;
}

/** Validates a property's text for its EDM type; returns null when valid. */
export function validateTypedValue(type: EdmType, value: string): string | null {
  const v = value.trim();
  switch (type) {
    case "Edm.String":
      return null;
    case "Edm.Int32":
      if (!INTEGER_RE.test(v)) return "Must be a whole number";
      return BigInt(v) < INT32_MIN || BigInt(v) > INT32_MAX
        ? "Must fit in a 32-bit signed integer"
        : null;
    case "Edm.Int64":
      if (!INTEGER_RE.test(v)) return "Must be a whole number";
      return BigInt(v) < INT64_MIN || BigInt(v) > INT64_MAX
        ? "Must fit in a 64-bit signed integer"
        : null;
    case "Edm.Double":
      return DECIMAL_RE.test(v) && Number.isFinite(Number(v)) ? null : "Must be a finite number";
    case "Edm.Boolean":
      return v === "true" || v === "false" ? null : "Must be true or false";
    case "Edm.DateTime":
      return v !== "" && !Number.isNaN(Date.parse(v)) ? null : "Must be a valid date and time";
    case "Edm.Guid":
      return GUID_RE.test(v) ? null : "Must be a GUID like 550e8400-e29b-41d4-a716-446655440000";
    case "Edm.Binary":
      return BASE64_RE.test(v) ? null : "Must be base64 text";
  }
}

type WireValue = string | number | boolean;

/**
 * Encodes validated text as the Table wire pair: the JSON value plus the
 * @odata.type annotation needed to keep it from being read back as another type.
 */
export function encodeValue(
  type: EdmType,
  value: string,
): { value: WireValue; annotation?: EdmType } {
  const v = value.trim();
  switch (type) {
    case "Edm.String":
      return { value };
    case "Edm.Int32":
      return { value: Number(v) };
    case "Edm.Boolean":
      return { value: v === "true" };
    case "Edm.Double":
      return { value: Number(v), annotation: type };
    case "Edm.Int64":
      return { value: BigInt(v).toString(), annotation: type };
    case "Edm.DateTime":
      return { value: new Date(v).toISOString(), annotation: type };
    default:
      return { value: v, annotation: type };
  }
}

export function buildEntityBody(
  partitionKey: string,
  rowKey: string,
  props: PropertyDraft[],
): Record<string, WireValue> {
  const body: Record<string, WireValue> = { PartitionKey: partitionKey, RowKey: rowKey };
  for (const p of props) {
    const enc = encodeValue(p.type, p.value);
    body[p.name] = enc.value;
    if (enc.annotation) body[`${p.name}@odata.type`] = enc.annotation;
  }
  return body;
}

function isEdmType(s: unknown): s is EdmType {
  return typeof s === "string" && (EDM_TYPES as readonly string[]).includes(s);
}

/** Number without an annotation: a whole number reads as Int32, any other as Double. */
function inferType(v: unknown): TypedValue {
  if (typeof v === "boolean") return { type: "Edm.Boolean", value: String(v) };
  if (typeof v === "number") {
    const whole = Number.isInteger(v) && BigInt(v) >= INT32_MIN && BigInt(v) <= INT32_MAX;
    return { type: whole ? "Edm.Int32" : "Edm.Double", value: String(v) };
  }
  return { type: "Edm.String", value: String(v ?? "") };
}

export function parseEntity(raw: Record<string, unknown>): Entity {
  const props: Record<string, TypedValue> = {};
  for (const [key, val] of Object.entries(raw)) {
    if (key.startsWith("odata.") || key.includes("@odata.")) continue;
    if ((SYSTEM_PROPERTIES as readonly string[]).includes(key) || val === null) continue;
    const annotation = raw[`${key}@odata.type`];
    props[key] = isEdmType(annotation) ? { type: annotation, value: String(val) } : inferType(val);
  }
  return {
    partitionKey: String(raw.PartitionKey ?? ""),
    rowKey: String(raw.RowKey ?? ""),
    timestamp: String(raw.Timestamp ?? ""),
    etag: String(raw["odata.etag"] ?? ""),
    props,
  };
}

/** The union of property names across entities, sorted, for dynamic columns. */
export function propertyColumns(entities: Entity[]): string[] {
  const names = new Set<string>();
  for (const e of entities) for (const n of Object.keys(e.props)) names.add(n);
  return [...names].toSorted((a, b) => a.localeCompare(b));
}

export function entityToDrafts(e: Entity): PropertyDraft[] {
  return Object.entries(e.props)
    .toSorted(([a], [b]) => a.localeCompare(b))
    .map(([name, tv]) => ({ name, type: tv.type, value: tv.value }));
}

export function odataLiteral(type: EdmType, value: string): string {
  const v = value.trim();
  switch (type) {
    case "Edm.String":
      return `'${escapeOData(value)}'`;
    case "Edm.Int64":
      return `${v}L`;
    case "Edm.DateTime":
      return `datetime'${new Date(v).toISOString()}'`;
    case "Edm.Guid":
      return `guid'${v}'`;
    case "Edm.Binary":
      return `binary'${v}'`;
    default:
      return v;
  }
}

export type FilterCondition = { property: string; op: string; type: EdmType; value: string };

export const FILTER_OPS = ["eq", "ne", "gt", "ge", "lt", "le"] as const;

export function buildFilter(conds: FilterCondition[], join: "and" | "or" = "and"): string {
  return conds
    .map((c) => `${c.property} ${c.op} ${odataLiteral(c.type, c.value)}`)
    .join(` ${join} `);
}

export function queryString(opts: {
  filter?: string;
  select?: string[];
  top?: number;
  next?: Continuation | null;
}): string {
  const params: string[] = [];
  const f = opts.filter?.trim();
  if (f) params.push(`$filter=${encodeURIComponent(f)}`);
  if (opts.select?.length) params.push(`$select=${encodeURIComponent(opts.select.join(","))}`);
  if (opts.top) params.push(`$top=${opts.top}`);
  if (opts.next) {
    params.push(
      `NextPartitionKey=${encodeURIComponent(opts.next.partitionKey)}`,
      `NextRowKey=${encodeURIComponent(opts.next.rowKey)}`,
    );
  }
  return params.join("&");
}

export function continuationFrom(headers: Headers): Continuation | null {
  const pk = headers.get("x-ms-continuation-NextPartitionKey");
  if (pk === null) return null;
  return { partitionKey: pk, rowKey: headers.get("x-ms-continuation-NextRowKey") ?? "" };
}

async function toTableError(res: Response): Promise<TableError> {
  const body = await res.text().catch(() => "");
  try {
    const err = JSON.parse(body)["odata.error"];
    if (err) {
      const code = String(err.code ?? `HTTP${res.status}`);
      return new TableError(res.status, code, String(err.message?.value ?? code));
    }
  } catch {
    // fall through to the raw body
  }
  return new TableError(res.status, `HTTP${res.status}`, body.trim() || `HTTP ${res.status}`);
}

const JSON_HEADERS = {
  Accept: "application/json;odata=minimalmetadata",
  "Content-Type": "application/json",
};

async function send(url: string, init: RequestInit, fetchFn: FetchFn): Promise<Response> {
  let res: Response;
  try {
    res = await fetchFn(url, init);
  } catch (e) {
    throw new TableError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await toTableError(res);
  return res;
}

export async function listTables(account: string, fetchFn: FetchFn = fetch): Promise<TableInfo[]> {
  const res = await send(tableUrl(account, "Tables"), { headers: JSON_HEADERS }, fetchFn);
  const data = (await res.json()) as { value?: { TableName: string }[] };
  return (data.value ?? []).map((t) => ({ name: t.TableName }));
}

/** Resolves true when created, false when the table already existed. */
export async function createTable(
  account: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<boolean> {
  try {
    await send(
      tableUrl(account, "Tables"),
      { method: "POST", headers: JSON_HEADERS, body: JSON.stringify({ TableName: name }) },
      fetchFn,
    );
    return true;
  } catch (e) {
    if (e instanceof TableError && e.code === "TableAlreadyExists") return false;
    throw e;
  }
}

export async function deleteTable(
  account: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(tableItemUrl(account, name), { method: "DELETE", headers: JSON_HEADERS }, fetchFn);
}

export async function queryEntities(
  account: string,
  table: string,
  opts: { filter?: string; select?: string[]; top?: number; next?: Continuation | null } = {},
  fetchFn: FetchFn = fetch,
): Promise<EntityPage> {
  const res = await send(
    tableUrl(account, encodeURIComponent(table), queryString(opts)),
    { headers: JSON_HEADERS },
    fetchFn,
  );
  const data = (await res.json()) as { value?: Record<string, unknown>[] };
  return {
    entities: (data.value ?? []).map((raw) => parseEntity(raw)),
    next: continuationFrom(res.headers),
  };
}

export async function getEntity(
  account: string,
  table: string,
  partitionKey: string,
  rowKey: string,
  fetchFn: FetchFn = fetch,
): Promise<Entity> {
  const res = await send(
    entityUrl(account, table, partitionKey, rowKey),
    { headers: JSON_HEADERS },
    fetchFn,
  );
  const entity = parseEntity((await res.json()) as Record<string, unknown>);
  return { ...entity, etag: entity.etag || (res.headers.get("ETag") ?? "") };
}

export async function insertEntity(
  account: string,
  table: string,
  partitionKey: string,
  rowKey: string,
  props: PropertyDraft[],
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(
    tableUrl(account, encodeURIComponent(table)),
    {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify(buildEntityBody(partitionKey, rowKey, props)),
    },
    fetchFn,
  );
}

export type UpdateMode = "merge" | "replace";

/** Merge keeps unlisted properties (PATCH); replace discards them (PUT). */
export async function updateEntity(
  account: string,
  table: string,
  partitionKey: string,
  rowKey: string,
  props: PropertyDraft[],
  mode: UpdateMode,
  etag = "*",
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(
    entityUrl(account, table, partitionKey, rowKey),
    {
      method: mode === "merge" ? "PATCH" : "PUT",
      headers: { ...JSON_HEADERS, "If-Match": etag || "*" },
      body: JSON.stringify(buildEntityBody(partitionKey, rowKey, props)),
    },
    fetchFn,
  );
}

export async function deleteEntity(
  account: string,
  table: string,
  partitionKey: string,
  rowKey: string,
  etag = "*",
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(
    entityUrl(account, table, partitionKey, rowKey),
    { method: "DELETE", headers: { ...JSON_HEADERS, "If-Match": etag || "*" } },
    fetchFn,
  );
}
