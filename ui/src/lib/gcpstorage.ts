/** Client for the Google Cloud Storage emulator via the dashboard's in-process proxy. */

export const GCS_PROXY_BASE = "/dashboard/api/v1/gcp/storage";
export const DELIMITER = "/";
export const MAX_PAGE = 100;
export const DEFAULT_PROJECT = "gopherstack-local";
export const PROJECT_STORAGE_KEY = "gopherstack_gcs_project";

export const STORAGE_CLASSES = ["STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"] as const;
export type StorageClass = (typeof STORAGE_CLASSES)[number];

export const LOCATIONS = [
  "US",
  "EU",
  "ASIA",
  "US-CENTRAL1",
  "US-EAST1",
  "US-WEST1",
  "EUROPE-WEST1",
  "ASIA-EAST1",
] as const;

export type Labels = Record<string, string>;

export type BucketInfo = {
  name: string;
  location: string;
  locationType: string;
  storageClass: string;
  timeCreated: string;
  updated: string;
  metageneration: string;
  versioning: boolean;
  labels: Labels;
};

export type ObjectInfo = {
  name: string;
  bucket: string;
  size: number;
  contentType: string;
  contentEncoding: string;
  contentDisposition: string;
  contentLanguage: string;
  cacheControl: string;
  md5Hash: string;
  crc32c: string;
  etag: string;
  generation: string;
  metageneration: string;
  storageClass: string;
  timeCreated: string;
  updated: string;
  timeDeleted: string;
  metadata: Labels;
};

export type BrowserEntry =
  | { kind: "folder"; name: string; prefix: string }
  | { kind: "object"; name: string; object: ObjectInfo };

export type ObjectListing = { entries: BrowserEntry[]; nextToken: string };

export type IamBinding = {
  role: string;
  members: string[];
  condition?: unknown;
};
export type IamPolicy = {
  etag: string;
  version: number;
  bindings: IamBinding[];
};

export type MetaRow = { key: string; value: string };

export class GcsError extends Error {
  readonly status: number;
  readonly reason: string;

  constructor(status: number, reason: string, message: string) {
    super(message);
    this.name = "GcsError";
    this.status = status;
    this.reason = reason;
  }
}

type FetchFn = typeof fetch;
type Json = Record<string, unknown>;

export function encodePath(path: string): string {
  return encodeURIComponent(path);
}

export function jsonUrl(bucket?: string, object?: string, query = ""): string {
  let url = `${GCS_PROXY_BASE}/storage/v1/b`;
  if (bucket) url += `/${encodeURIComponent(bucket)}`;
  if (bucket && object !== undefined) url += `/o/${encodePath(object)}`;
  return query ? `${url}?${query}` : url;
}

export function downloadUrl(bucket: string, object: string, generation = ""): string {
  const gen = generation ? `&generation=${encodeURIComponent(generation)}` : "";
  return `${GCS_PROXY_BASE}/download/storage/v1/b/${encodeURIComponent(bucket)}/o/${encodePath(object)}?alt=media${gen}`;
}

export function uploadUrl(bucket: string, name: string): string {
  return `${GCS_PROXY_BASE}/upload/storage/v1/b/${encodeURIComponent(bucket)}/o?uploadType=media&name=${encodeURIComponent(name)}`;
}

/** The gs:// URI users pass to gsutil and client libraries. */
export function gsUri(bucket: string, object = ""): string {
  return `gs://${bucket}${object ? `/${object}` : ""}`;
}

async function failure(res: Response): Promise<GcsError> {
  let reason = "";
  let message = res.statusText || `HTTP ${res.status}`;
  try {
    const body = (await res.json()) as {
      error?: { message?: string; errors?: { reason?: string }[] };
    };
    message = body.error?.message ?? message;
    reason = body.error?.errors?.[0]?.reason ?? "";
  } catch {
    // non-JSON error body: keep the status text
  }
  return new GcsError(res.status, reason, message);
}

async function send(url: string, init: RequestInit, fetchFn: FetchFn): Promise<Response> {
  let res: Response;
  try {
    res = await fetchFn(url, init);
  } catch (e) {
    throw new GcsError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await failure(res);
  return res;
}

async function sendJson<T>(
  url: string,
  method: string,
  body: unknown,
  fetchFn: FetchFn,
): Promise<T> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers = { "Content-Type": "application/json" };
  }
  const res = await send(url, init, fetchFn);
  const text = await res.text();
  return (text ? JSON.parse(text) : {}) as T;
}

function str(v: unknown): string {
  return typeof v === "string" ? v : "";
}

function labelsOf(v: unknown): Labels {
  if (!v || typeof v !== "object") return {};
  return Object.fromEntries(
    Object.entries(v as Json).filter((e): e is [string, string] => typeof e[1] === "string"),
  );
}

export function parseBucket(raw: Json): BucketInfo {
  const versioning = raw.versioning as { enabled?: boolean } | undefined;
  return {
    name: str(raw.name),
    location: str(raw.location),
    locationType: str(raw.locationType),
    storageClass: str(raw.storageClass),
    timeCreated: str(raw.timeCreated),
    updated: str(raw.updated),
    metageneration: str(raw.metageneration),
    versioning: versioning?.enabled === true,
    labels: labelsOf(raw.labels),
  };
}

export function parseObject(raw: Json): ObjectInfo {
  return {
    name: str(raw.name),
    bucket: str(raw.bucket),
    size: Number(raw.size ?? 0) || 0,
    contentType: str(raw.contentType),
    contentEncoding: str(raw.contentEncoding),
    contentDisposition: str(raw.contentDisposition),
    contentLanguage: str(raw.contentLanguage),
    cacheControl: str(raw.cacheControl),
    md5Hash: str(raw.md5Hash),
    crc32c: str(raw.crc32c),
    etag: str(raw.etag),
    generation: str(raw.generation),
    metageneration: str(raw.metageneration),
    storageClass: str(raw.storageClass),
    timeCreated: str(raw.timeCreated),
    updated: str(raw.updated),
    timeDeleted: str(raw.timeDeleted),
    metadata: labelsOf(raw.metadata),
  };
}

export async function listBuckets(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<BucketInfo[]> {
  const out: BucketInfo[] = [];
  let token = "";
  do {
    const q = `project=${encodeURIComponent(project)}&maxResults=${MAX_PAGE}${token ? `&pageToken=${encodeURIComponent(token)}` : ""}`;
    const body = await sendJson<{ items?: Json[]; nextPageToken?: string }>(
      jsonUrl(undefined, undefined, q),
      "GET",
      undefined,
      fetchFn,
    );
    out.push(...(body.items ?? []).map((b) => parseBucket(b)));
    token = body.nextPageToken ?? "";
  } while (token);
  return out;
}

export type CreateBucketOptions = {
  location: string;
  storageClass: string;
  versioning: boolean;
  labels: Labels;
};

export async function createBucket(
  project: string,
  name: string,
  opts: CreateBucketOptions,
  fetchFn: FetchFn = fetch,
): Promise<BucketInfo> {
  const body: Json = {
    name,
    location: opts.location,
    storageClass: opts.storageClass,
  };
  if (opts.versioning) body.versioning = { enabled: true };
  if (Object.keys(opts.labels).length > 0) body.labels = opts.labels;
  const raw = await sendJson<Json>(
    jsonUrl(undefined, undefined, `project=${encodeURIComponent(project)}`),
    "POST",
    body,
    fetchFn,
  );
  return parseBucket(raw);
}

export async function getBucket(name: string, fetchFn: FetchFn = fetch): Promise<BucketInfo> {
  return parseBucket(await sendJson<Json>(jsonUrl(name), "GET", undefined, fetchFn));
}

export async function deleteBucket(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await send(jsonUrl(name), { method: "DELETE" }, fetchFn);
}

/** Merge-patch body: keys present in `before` but not `after` are sent as null. */
export function diffMap(before: Labels, after: Labels): Record<string, string | null> {
  const out: Record<string, string | null> = {};
  for (const k of Object.keys(before)) if (!(k in after)) out[k] = null;
  for (const [k, v] of Object.entries(after)) if (before[k] !== v) out[k] = v;
  return out;
}

export async function patchBucket(
  name: string,
  patch: Json,
  fetchFn: FetchFn = fetch,
): Promise<BucketInfo> {
  return parseBucket(await sendJson<Json>(jsonUrl(name), "PATCH", patch, fetchFn));
}

export async function getIamPolicy(name: string, fetchFn: FetchFn = fetch): Promise<IamPolicy> {
  const raw = await sendJson<{
    etag?: string;
    version?: number;
    bindings?: IamBinding[];
  }>(`${jsonUrl(name)}/iam`, "GET", undefined, fetchFn);
  return {
    etag: raw.etag ?? "",
    version: raw.version ?? 0,
    bindings: raw.bindings ?? [],
  };
}

export type ListOptions = {
  prefix?: string;
  delimiter?: string;
  pageToken?: string;
  maxResults?: number;
  versions?: boolean;
};

export async function listObjects(
  bucket: string,
  opts: ListOptions = {},
  fetchFn: FetchFn = fetch,
): Promise<ObjectListing & { prefixes: string[] }> {
  const q = new URLSearchParams();
  if (opts.prefix) q.set("prefix", opts.prefix);
  if (opts.delimiter) q.set("delimiter", opts.delimiter);
  if (opts.pageToken) q.set("pageToken", opts.pageToken);
  if (opts.maxResults) q.set("maxResults", String(opts.maxResults));
  if (opts.versions) q.set("versions", "true");
  const body = await sendJson<{
    items?: Json[];
    prefixes?: string[];
    nextPageToken?: string;
  }>(`${jsonUrl(bucket)}/o?${q.toString()}`, "GET", undefined, fetchFn);
  const prefix = opts.prefix ?? "";
  const prefixes = body.prefixes ?? [];
  const entries: BrowserEntry[] = [
    ...prefixes.map<BrowserEntry>((p) => ({
      kind: "folder",
      prefix: p,
      name: p.slice(prefix.length).replace(/\/$/, ""),
    })),
    ...(body.items ?? []).map<BrowserEntry>((raw) => {
      const object = parseObject(raw);
      return { kind: "object", name: object.name.slice(prefix.length), object };
    }),
  ];
  return { entries, prefixes, nextToken: body.nextPageToken ?? "" };
}

export async function listVersions(
  bucket: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<ObjectInfo[]> {
  const out: ObjectInfo[] = [];
  let token = "";
  do {
    const page = await listObjects(
      bucket,
      { prefix: name, versions: true, pageToken: token, maxResults: MAX_PAGE },
      fetchFn,
    );
    for (const e of page.entries)
      if (e.kind === "object" && e.object.name === name) out.push(e.object);
    token = page.nextToken;
  } while (token);
  return out.toSorted((a, b) => Number(BigInt(b.generation) - BigInt(a.generation)));
}

export async function getObject(
  bucket: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<ObjectInfo> {
  return parseObject(await sendJson<Json>(jsonUrl(bucket, name), "GET", undefined, fetchFn));
}

export async function patchObject(
  bucket: string,
  name: string,
  patch: Json,
  fetchFn: FetchFn = fetch,
): Promise<ObjectInfo> {
  return parseObject(await sendJson<Json>(jsonUrl(bucket, name), "PATCH", patch, fetchFn));
}

export async function deleteObject(
  bucket: string,
  name: string,
  generation = "",
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const q = generation ? `generation=${encodeURIComponent(generation)}` : "";
  await send(jsonUrl(bucket, name, q), { method: "DELETE" }, fetchFn);
}

export async function uploadObject(
  bucket: string,
  name: string,
  data: Blob,
  contentType: string,
  fetchFn: FetchFn = fetch,
): Promise<ObjectInfo> {
  const res = await send(
    uploadUrl(bucket, name),
    {
      method: "POST",
      body: data,
      headers: { "Content-Type": contentType || "application/octet-stream" },
    },
    fetchFn,
  );
  return parseObject((await res.json()) as Json);
}

export async function downloadObject(
  bucket: string,
  name: string,
  generation = "",
  fetchFn: FetchFn = fetch,
): Promise<Blob> {
  const res = await send(downloadUrl(bucket, name, generation), { method: "GET" }, fetchFn);
  return res.blob();
}

export function breadcrumbs(prefix: string): { label: string; prefix: string }[] {
  const parts = prefix.split(DELIMITER).filter(Boolean);
  return parts.map((label, i) => ({
    label,
    prefix: `${parts.slice(0, i + 1).join(DELIMITER)}${DELIMITER}`,
  }));
}

/** https://cloud.google.com/storage/docs/buckets#naming (dots and underscores allowed). */
export function validateBucketName(name: string): string | null {
  if (!name) return "Name is required";
  if (name.length < 3 || name.length > 63) return "Must be 3-63 characters";
  if (!/^[a-z0-9][a-z0-9._-]*[a-z0-9]$/.test(name))
    return "Use lowercase letters, numbers, dashes, underscores and dots; start and end with a letter or number";
  if (name.startsWith("goog") || name.includes("google"))
    return 'Must not start with "goog" or contain "google"';
  return null;
}

export function validateObjectName(name: string): string | null {
  if (!name) return "Name is required";
  if (new TextEncoder().encode(name).length > 1024) return "Must be at most 1024 bytes";
  if (name.startsWith(DELIMITER) || name.endsWith(DELIMITER)) return "Must not start or end with /";
  if (name.split(DELIMITER).some((s) => s === "." || s === ".."))
    return "Path segments cannot be . or ..";
  return null;
}

/** Label keys: lowercase letter first, then lowercase letters, digits, "_" or "-"; max 63. */
export function validateLabelKey(key: string): string | null {
  if (!key) return "key is required";
  if (!/^[a-z][a-z0-9_-]{0,62}$/.test(key))
    return "keys start with a lowercase letter; use a-z, 0-9, _ and -";
  return null;
}

export function validateMetadataKey(key: string): string | null {
  return key ? null : "key is required";
}

export function metadataToRows(md: Labels): MetaRow[] {
  return Object.entries(md)
    .toSorted(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => ({ key, value }));
}

export function rowsToMap(rows: MetaRow[]): Labels {
  return Object.fromEntries(rows.map((r) => [r.key, r.value]));
}

export function validateRows(
  rows: MetaRow[],
  validateKey: (key: string) => string | null,
): string | null {
  const seen = new Set<string>();
  for (const row of rows) {
    const bad = validateKey(row.key);
    if (bad) return `${row.key || "(empty)"}: ${bad}`;
    if (seen.has(row.key)) return `${row.key}: duplicate key`;
    seen.add(row.key);
  }
  return null;
}

export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 ? 2 : 1)} ${units[i]}`;
}

/** Base64 (as GCS reports md5Hash) to lowercase hex, for comparing against md5sum. */
export function base64ToHex(b64: string): string {
  if (!b64) return "";
  try {
    return Array.from(atob(b64), (c) => (c.codePointAt(0) ?? 0).toString(16).padStart(2, "0")).join(
      "",
    );
  } catch {
    return "";
  }
}

export function readStoredProject(storage?: Pick<Storage, "getItem">): string {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    return s?.getItem(PROJECT_STORAGE_KEY)?.trim() || DEFAULT_PROJECT;
  } catch {
    return DEFAULT_PROJECT;
  }
}

export function writeStoredProject(project: string, storage?: Pick<Storage, "setItem">): void {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    s?.setItem(PROJECT_STORAGE_KEY, project);
  } catch {
    // storage unavailable: the project applies for this session only
  }
}
