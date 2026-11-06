/** Client for Azure AI Search through the dashboard's in-process proxies (Microsoft.Search and the data plane). */

import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const SEARCH_ARM_BASE = "/dashboard/api/v1/azure/search";
export const SEARCH_KEYS_BASE = "/dashboard/api/v1/azure/searchkeys";
export const SEARCH_DATA_BASE = "/dashboard/api/v1/azure/searchdata";
export const SEARCH_ARM_VERSION = "2025-05-01";
export const MAX_TOP = 1000;
export const DEFAULT_TOP = 10;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const bool = (v: unknown): boolean => v === true;

export type SearchTarget = { subscription: string; group: string; service: string };

export type SearchService = {
  id: string;
  name: string;
  location: string;
  sku: string;
  replicas: number;
  partitions: number;
  hostingMode: string;
  status: string;
  provisioning: string;
  endpoint: string;
  publicAccess: string;
};

export function parseService(raw: Json): SearchService {
  const p = obj(raw.properties);
  return {
    id: str(raw.id),
    name: str(raw.name),
    location: str(raw.location),
    sku: str(obj(raw.sku).name),
    replicas: num(p.replicaCount),
    partitions: num(p.partitionCount),
    hostingMode: str(p.hostingMode),
    status: str(p.status),
    provisioning: str(p.provisioningState),
    endpoint: str(p.endpoint),
    publicAccess: str(p.publicNetworkAccess),
  };
}

/** Search units are what the service is billed on: replicas times partitions. */
export const searchUnits = (s: Pick<SearchService, "replicas" | "partitions">): number =>
  s.replicas * s.partitions;

const servicesPath = (sub: string, rg: string) =>
  `/subscriptions/${enc(sub)}/resourceGroups/${enc(rg)}/providers/Microsoft.Search/searchServices`;

export async function listServices(
  subscription: string,
  group: string,
  fetchFn: FetchFn = fetch,
): Promise<SearchService[]> {
  const body = await restCall(
    SEARCH_ARM_BASE,
    "GET",
    `${servicesPath(subscription, group)}?api-version=${SEARCH_ARM_VERSION}`,
    undefined,
    fetchFn,
  );
  return list(body.value)
    .map((s) => parseService(s))
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

const idPath = (t: SearchTarget) => `/${enc(t.subscription)}/${enc(t.group)}/${enc(t.service)}`;

export type MaskedKey = { name: string; fingerprint: string; masked: string };
export type KeyOverview = { primary: string; secondary: string; query: MaskedKey[] };

const maskedKey = (raw: Json): MaskedKey => ({
  name: str(raw.name),
  fingerprint: str(raw.fp),
  masked: str(raw.masked),
});

/** Keys as the server shows them: masked, never in the clear. */
export async function getKeys(t: SearchTarget, fetchFn: FetchFn = fetch): Promise<KeyOverview> {
  const body = await restCall(SEARCH_KEYS_BASE, "GET", idPath(t), undefined, fetchFn);
  const admin = obj(body.admin);
  return {
    primary: str(admin.primary),
    secondary: str(admin.secondary),
    query: list(body.query).map((k) => maskedKey(k)),
  };
}

export type AdminKeys = { primary: string; secondary: string };

/** Fetches the admin keys. Only call this from an explicit reveal action. */
export async function revealAdminKeys(
  t: SearchTarget,
  fetchFn: FetchFn = fetch,
): Promise<AdminKeys> {
  const body = await restCall(
    SEARCH_KEYS_BASE,
    "POST",
    `${idPath(t)}/admin/reveal`,
    undefined,
    fetchFn,
  );
  return { primary: str(body.primaryKey), secondary: str(body.secondaryKey) };
}

/** Fetches one query key value. Only call this from an explicit reveal action. */
export async function revealQueryKey(
  t: SearchTarget,
  fingerprint: string,
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const body = await restCall(
    SEARCH_KEYS_BASE,
    "POST",
    `${idPath(t)}/query/${enc(fingerprint)}/reveal`,
    undefined,
    fetchFn,
  );
  return str(body.key);
}

export async function createQueryKey(
  t: SearchTarget,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<MaskedKey> {
  return maskedKey(
    await restCall(SEARCH_KEYS_BASE, "POST", `${idPath(t)}/query`, { name }, fetchFn),
  );
}

export async function deleteQueryKey(
  t: SearchTarget,
  fingerprint: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    SEARCH_KEYS_BASE,
    "DELETE",
    `${idPath(t)}/query/${enc(fingerprint)}`,
    undefined,
    fetchFn,
  );
}

const QUERY_KEY_NAME = /^[A-Za-z0-9 _-]{1,64}$/;

export function validateKeyName(name: string): string {
  return QUERY_KEY_NAME.test(name) ? "" : "Use 1-64 letters, digits, spaces, dashes or underscores";
}

export type IndexField = {
  name: string;
  type: string;
  key: boolean;
  searchable: boolean;
  filterable: boolean;
  sortable: boolean;
  facetable: boolean;
  retrievable: boolean;
  analyzer: string;
  dimensions: number;
};

export type IndexDef = {
  name: string;
  fields: IndexField[];
  suggesters: string[];
  scoringProfiles: string[];
  semantic: boolean;
  vector: boolean;
  raw: Json;
};

export function parseField(raw: Json): IndexField {
  return {
    name: str(raw.name),
    type: str(raw.type),
    key: bool(raw.key),
    searchable: bool(raw.searchable),
    filterable: bool(raw.filterable),
    sortable: bool(raw.sortable),
    facetable: bool(raw.facetable),
    retrievable: raw.retrievable !== false,
    analyzer: str(raw.analyzer),
    dimensions: num(raw.dimensions),
  };
}

export function parseIndex(raw: Json): IndexDef {
  const fields = list(raw.fields).map((f) => parseField(f));
  return {
    name: str(raw.name),
    fields,
    suggesters: list(raw.suggesters).map((s) => str(s.name)),
    scoringProfiles: list(raw.scoringProfiles).map((s) => str(s.name)),
    semantic: list(obj(raw.semantic).configurations).length > 0,
    vector: fields.some((f) => f.dimensions > 0),
    raw,
  };
}

/** The attributes that are switched on for a field, in a fixed order, for compact display. */
export function fieldFlags(f: IndexField): string[] {
  const flags: [boolean, string][] = [
    [f.key, "key"],
    [f.searchable, "searchable"],
    [f.filterable, "filterable"],
    [f.sortable, "sortable"],
    [f.facetable, "facetable"],
    [!f.retrievable, "not retrievable"],
  ];
  return flags.filter(([on]) => on).map(([, label]) => label);
}

export type IndexStats = { documents: number; storage: number; vector: number };

export function parseStats(raw: Json): IndexStats {
  return {
    documents: num(raw.documentCount),
    storage: num(raw.storageSize),
    vector: num(raw.vectorIndexSize),
  };
}

const dataPath = (t: SearchTarget, rest: string) => `${idPath(t)}/${rest}`;
const dataCall = (
  t: SearchTarget,
  method: string,
  rest: string,
  body: unknown,
  fetchFn: FetchFn,
): Promise<Json> => restCall(SEARCH_DATA_BASE, method, dataPath(t, rest), body, fetchFn);

export async function listIndexes(t: SearchTarget, fetchFn: FetchFn = fetch): Promise<IndexDef[]> {
  const body = await dataCall(t, "GET", "indexes", undefined, fetchFn);
  return list(body.value)
    .map((i) => parseIndex(i))
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

export async function getIndexStats(
  t: SearchTarget,
  index: string,
  fetchFn: FetchFn = fetch,
): Promise<IndexStats> {
  return parseStats(
    await dataCall(t, "GET", `indexes/${enc(index)}/search.stats`, undefined, fetchFn),
  );
}

export type ServiceStats = {
  documents: number;
  storage: number;
  indexes: number;
  indexesQuota: number | undefined;
  indexers: number;
  dataSources: number;
  skillsets: number;
  synonymMaps: number;
};

export function parseServiceStats(raw: Json): ServiceStats {
  const c = obj(raw.counters);
  const used = (k: string): number => num(obj(c[k]).usage);
  const quota = obj(c.indexesCount).quota;
  return {
    documents: used("documentCount"),
    storage: used("storageSize"),
    indexes: used("indexesCount"),
    indexesQuota: typeof quota === "number" ? quota : undefined,
    indexers: used("indexersCount"),
    dataSources: used("dataSourcesCount"),
    skillsets: used("skillsetCount"),
    synonymMaps: used("synonymMaps"),
  };
}

export async function getServiceStats(
  t: SearchTarget,
  fetchFn: FetchFn = fetch,
): Promise<ServiceStats> {
  return parseServiceStats(await dataCall(t, "GET", "servicestats", undefined, fetchFn));
}

const UNITS = ["B", "KB", "MB", "GB", "TB"];

export function formatBytes(n: number): string {
  let v = n;
  let i = 0;
  while (v >= 1024 && i < UNITS.length - 1) {
    v /= 1024;
    i++;
  }
  return `${i === 0 ? v : v.toFixed(v < 10 ? 2 : 1)} ${UNITS[i]}`;
}

export type QueryForm = {
  search: string;
  filter: string;
  orderby: string;
  select: string;
  facets: string;
  top: string;
  skip: string;
  queryType: "simple" | "full";
  searchMode: "any" | "all";
  count: boolean;
};

export const emptyQuery: QueryForm = {
  search: "*",
  filter: "",
  orderby: "",
  select: "",
  facets: "",
  top: String(DEFAULT_TOP),
  skip: "",
  queryType: "simple",
  searchMode: "any",
  count: true,
};

/** Facet expressions, one per line; a facet can itself contain commas (`price,interval:10`). */
export const parseFacets = (text: string): string[] =>
  text
    .split("\n")
    .map((l) => l.trim())
    .filter((l) => l !== "");

function wholeNumber(text: string, max: number, label: string): string {
  if (text.trim() === "") return "";
  if (!/^\d+$/.test(text.trim()) || Number(text) > max)
    return `${label} must be a whole number up to ${max}`;
  return "";
}

/** The first problem with a query form, or empty when it can be sent. */
export function validateQuery(q: QueryForm): string {
  return (
    wholeNumber(q.top, MAX_TOP, "Top") ||
    wholeNumber(q.skip, 100000, "Skip") ||
    (parseFacets(q.facets).length > 32 ? "At most 32 facets" : "")
  );
}

/** Builds the request body from fixed fields; empty fields are left out. */
export function buildQueryBody(q: QueryForm): Json {
  const body: Json = { search: q.search.trim() === "" ? "*" : q.search, count: q.count };
  const text: [string, string][] = [
    ["filter", q.filter],
    ["orderby", q.orderby],
    ["select", q.select],
  ];
  for (const [k, v] of text) if (v.trim() !== "") body[k] = v.trim();
  if (q.queryType !== "simple") body.queryType = q.queryType;
  if (q.searchMode !== "any") body.searchMode = q.searchMode;
  const facets = parseFacets(q.facets);
  if (facets.length > 0) body.facets = facets;
  if (q.top.trim() !== "") body.top = Number(q.top);
  if (q.skip.trim() !== "") body.skip = Number(q.skip);
  return body;
}

export type FacetValue = { label: string; count: number };
export type FacetGroup = { field: string; values: FacetValue[] };

function facetLabel(v: Json): string {
  if (v.value !== undefined) return String(v.value);
  const from = v.from === undefined ? "" : String(v.from);
  const to = v.to === undefined ? "" : String(v.to);
  return `${from || "…"} – ${to || "…"}`;
}

export function parseFacetResults(raw: unknown): FacetGroup[] {
  return Object.entries(obj(raw)).map(([field, values]) => ({
    field,
    values: list(values).map((v) => ({ label: facetLabel(v), count: num(v.count) })),
  }));
}

export type SearchResult = {
  count: number | undefined;
  rows: Json[];
  facets: FacetGroup[];
  elapsedMs: number;
  raw: Json;
};

export async function runSearch(
  t: SearchTarget,
  index: string,
  q: QueryForm,
  fetchFn: FetchFn = fetch,
  now: () => number = () => performance.now(),
): Promise<SearchResult> {
  const started = now();
  const raw = await dataCall(
    t,
    "POST",
    `indexes/${enc(index)}/docs/search.post.search`,
    buildQueryBody(q),
    fetchFn,
  );
  const count = raw["@odata.count"];
  return {
    count: typeof count === "number" ? count : undefined,
    rows: list(raw.value),
    facets: parseFacetResults(raw["@search.facets"]),
    elapsedMs: Math.round(now() - started),
    raw,
  };
}

export type Indexer = {
  name: string;
  dataSource: string;
  target: string;
  skillset: string;
  schedule: string;
  disabled: boolean;
};

export function parseIndexer(raw: Json): Indexer {
  return {
    name: str(raw.name),
    dataSource: str(raw.dataSourceName),
    target: str(raw.targetIndexName),
    skillset: str(raw.skillsetName),
    schedule: str(obj(raw.schedule).interval),
    disabled: bool(raw.disabled),
  };
}

export type Execution = {
  status: string;
  start: string;
  end: string;
  processed: number;
  failed: number;
  error: string;
};

export type IndexerStatus = { status: string; last: Execution | undefined; history: Execution[] };

function parseExecution(raw: Json): Execution {
  return {
    status: str(raw.status),
    start: str(raw.startTime),
    end: str(raw.endTime),
    processed: num(raw.itemsProcessed),
    failed: num(raw.itemsFailed),
    error: str(raw.errorMessage),
  };
}

export function parseIndexerStatus(raw: Json): IndexerStatus {
  const last = obj(raw.lastResult);
  return {
    status: str(raw.status),
    last: Object.keys(last).length > 0 ? parseExecution(last) : undefined,
    history: list(raw.executionHistory).map((e) => parseExecution(e)),
  };
}

export type Tone = "good" | "bad" | "busy" | "muted";

export function executionTone(status: string): Tone {
  switch (status) {
    case "success":
    case "running":
      return "good";
    case "transientFailure":
    case "persistentFailure":
    case "error":
      return "bad";
    case "inProgress":
      return "busy";
    default:
      return "muted";
  }
}

export async function listIndexers(t: SearchTarget, fetchFn: FetchFn = fetch): Promise<Indexer[]> {
  const body = await dataCall(t, "GET", "indexers", undefined, fetchFn);
  return list(body.value)
    .map((i) => parseIndexer(i))
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

export async function getIndexerStatus(
  t: SearchTarget,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<IndexerStatus> {
  return parseIndexerStatus(
    await dataCall(t, "GET", `indexers/${enc(name)}/search.status`, undefined, fetchFn),
  );
}

export async function runIndexer(
  t: SearchTarget,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await dataCall(t, "POST", `indexers/${enc(name)}/search.run`, undefined, fetchFn);
}

export async function resetIndexer(
  t: SearchTarget,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await dataCall(t, "POST", `indexers/${enc(name)}/search.reset`, undefined, fetchFn);
}

export type ObjectKind = "datasources" | "skillsets" | "synonymmaps";

export type SearchObject = { name: string; summary: string; raw: Json };

function summarize(kind: ObjectKind, raw: Json): string {
  switch (kind) {
    case "datasources": {
      const container = str(obj(raw.container).name);
      return `${str(raw.type)}${container ? ` · ${container}` : ""}`;
    }
    case "skillsets":
      return `${list(raw.skills).length} skill${list(raw.skills).length === 1 ? "" : "s"}`;
    default: {
      const rules = str(raw.synonyms)
        .split("\n")
        .filter((r) => r.trim() !== "").length;
      return `${rules} rule${rules === 1 ? "" : "s"}`;
    }
  }
}

export async function listObjects(
  t: SearchTarget,
  kind: ObjectKind,
  fetchFn: FetchFn = fetch,
): Promise<SearchObject[]> {
  const body = await dataCall(t, "GET", kind, undefined, fetchFn);
  return list(body.value)
    .map((o) => ({ name: str(o.name), summary: summarize(kind, o), raw: o }))
    .toSorted((a, b) => a.name.localeCompare(b.name));
}
