/** Client for the Oracle Cloud Object Storage emulator via the dashboard's in-process proxy. */

import { restCall, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const OS_PROXY_BASE = "/dashboard/api/v1/oci/objectstorage";
export const DELIMITER = "/";
export const PAGE_SIZE = 200;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type BucketInfo = {
  name: string;
  namespace: string;
  compartmentId: string;
  createdBy: string;
  timeCreated: string;
  publicAccessType: string;
  storageTier: string;
  versioning: string;
  objectCount: number;
  sizeBytes: number;
  freeformTags: Record<string, string>;
  raw: Json;
};

export type ObjectSummary = {
  name: string;
  size: number;
  etag: string;
  md5: string;
  versionId: string;
  contentType: string;
  storageTier: string;
  archivalState: string;
  timeCreated: string;
  timeModified: string;
  metadata: Record<string, string>;
};

export type ObjectEntry =
  | { kind: "folder"; name: string; prefix: string }
  | { kind: "object"; name: string; object: ObjectSummary };

export type ObjectListing = { entries: ObjectEntry[]; nextStartWith: string };

export type ParInfo = {
  id: string;
  name: string;
  accessType: string;
  objectName: string;
  listingAction: string;
  timeCreated: string;
  timeExpires: string;
};

export type LifecycleRule = {
  name: string;
  action: string;
  target: string;
  amount: number;
  unit: string;
  enabled: boolean;
  prefixes: string[];
};

export type Lifecycle = { configured: boolean; rules: LifecycleRule[] };

export type RetentionRule = {
  id: string;
  displayName: string;
  duration: string;
  locked: boolean;
  timeCreated: string;
};

export function parseBucket(raw: Json): BucketInfo {
  return {
    name: str(raw.name),
    namespace: str(raw.namespace),
    compartmentId: str(raw.compartmentId),
    createdBy: str(raw.createdBy),
    timeCreated: str(raw.timeCreated),
    publicAccessType: str(raw.publicAccessType),
    storageTier: str(raw.storageTier),
    versioning: str(raw.versioning),
    objectCount: num(raw.approximateCount),
    sizeBytes: num(raw.approximateSize),
    freeformTags: strMap(raw.freeformTags),
    raw,
  };
}

export function parseObject(raw: Json): ObjectSummary {
  return {
    name: str(raw.name),
    size: num(raw.size),
    etag: str(raw.etag),
    md5: str(raw.md5),
    versionId: str(raw.versionId),
    contentType: str(raw.contentType),
    storageTier: str(raw.storageTier),
    archivalState: str(raw.archivalState),
    timeCreated: str(raw.timeCreated),
    timeModified: str(raw.timeModified),
    metadata: strMap(raw.metadata),
  };
}

export function parsePar(raw: Json): ParInfo {
  return {
    id: str(raw.id),
    name: str(raw.name),
    accessType: str(raw.accessType),
    objectName: str(raw.objectName),
    listingAction: str(raw.bucketListingAction),
    timeCreated: str(raw.timeCreated),
    timeExpires: str(raw.timeExpires),
  };
}

export function parseLifecycleRule(raw: Json): LifecycleRule {
  const filter = (raw.objectNameFilter ?? {}) as Json;
  return {
    name: str(raw.name),
    action: str(raw.action),
    target: str(raw.target) || "objects",
    amount: num(raw.timeAmount),
    unit: str(raw.timeUnit),
    enabled: raw.isEnabled === true,
    prefixes: list(filter.inclusionPrefixes).map(String),
  };
}

export function parseRetention(raw: Json): RetentionRule {
  const d = raw.duration as Json | undefined;
  return {
    id: str(raw.id),
    displayName: str(raw.displayName),
    duration: d ? `${num(d.timeAmount)} ${str(d.timeUnit).toLowerCase()}` : "indefinite",
    locked: Boolean(raw.timeRuleLocked),
    timeCreated: str(raw.timeCreated),
  };
}

/** Folder entries first, then objects, each alphabetically; `prefix` is stripped from display names. */
export function buildEntries(prefix: string, objects: ObjectSummary[], prefixes: string[]) {
  const folders: ObjectEntry[] = prefixes.map((p) => ({
    kind: "folder",
    name: p.slice(prefix.length).replace(/\/$/, ""),
    prefix: p,
  }));
  const files: ObjectEntry[] = objects.map((o) => ({
    kind: "object",
    name: o.name.slice(prefix.length),
    object: o,
  }));
  return [...folders, ...files];
}

/** Breadcrumb trail for a prefix such as "a/b/": [{label:"a",prefix:"a/"},{label:"b",prefix:"a/b/"}]. */
export function prefixCrumbs(prefix: string): { label: string; prefix: string }[] {
  const parts = prefix.split(DELIMITER).filter(Boolean);
  return parts.map((label, i) => ({
    label,
    prefix: `${parts.slice(0, i + 1).join(DELIMITER)}${DELIMITER}`,
  }));
}

const bucketPath = (bucket: string) => `/buckets/${enc(bucket)}`;

export async function getNamespace(fetchFn: FetchFn = fetch): Promise<string> {
  const body = await restCall(OS_PROXY_BASE, "GET", "/namespace", undefined, fetchFn);
  return str(body.namespace);
}

export async function listBuckets(fetchFn: FetchFn = fetch): Promise<BucketInfo[]> {
  const body = await restCall(OS_PROXY_BASE, "GET", "/buckets", undefined, fetchFn);
  return list(body.buckets).map((b) => parseBucket(b));
}

export async function getBucket(name: string, fetchFn: FetchFn = fetch): Promise<BucketInfo> {
  return parseBucket(await restCall(OS_PROXY_BASE, "GET", bucketPath(name), undefined, fetchFn));
}

export async function deleteBucket(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(OS_PROXY_BASE, "DELETE", bucketPath(name), undefined, fetchFn);
}

export async function listObjects(
  bucket: string,
  opts: { prefix?: string; delimiter?: string; start?: string; limit?: number } = {},
  fetchFn: FetchFn = fetch,
): Promise<ObjectListing> {
  const prefix = opts.prefix ?? "";
  const q = new URLSearchParams({ limit: String(opts.limit ?? PAGE_SIZE) });
  if (prefix) q.set("prefix", prefix);
  if (opts.delimiter !== "") q.set("delimiter", opts.delimiter ?? DELIMITER);
  if (opts.start) q.set("start", opts.start);
  const body = await restCall(
    OS_PROXY_BASE,
    "GET",
    `${bucketPath(bucket)}/objects?${q.toString()}`,
    undefined,
    fetchFn,
  );
  const objects = list(body.objects).map((o) => parseObject(o));
  const prefixes = (Array.isArray(body.prefixes) ? body.prefixes : []).map(String);
  return {
    entries: buildEntries(prefix, objects, prefixes),
    nextStartWith: str(body.nextStartWith),
  };
}

export async function getObject(
  bucket: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<ObjectSummary> {
  return parseObject(
    await restCall(
      OS_PROXY_BASE,
      "GET",
      `${bucketPath(bucket)}/object?name=${enc(name)}`,
      undefined,
      fetchFn,
    ),
  );
}

export async function deleteObject(
  bucket: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    OS_PROXY_BASE,
    "DELETE",
    `${bucketPath(bucket)}/object?name=${enc(name)}`,
    undefined,
    fetchFn,
  );
}

/** Same-origin download URL; the server answers with Content-Disposition: attachment. */
export function objectDownloadUrl(bucket: string, name: string): string {
  return `${OS_PROXY_BASE}${bucketPath(bucket)}/object/content?name=${enc(name)}`;
}

export async function listPars(bucket: string, fetchFn: FetchFn = fetch): Promise<ParInfo[]> {
  const body = await restCall(
    OS_PROXY_BASE,
    "GET",
    `${bucketPath(bucket)}/pars`,
    undefined,
    fetchFn,
  );
  return list(body.pars).map((p) => parsePar(p));
}

export async function getLifecycle(bucket: string, fetchFn: FetchFn = fetch): Promise<Lifecycle> {
  const body = await restCall(
    OS_PROXY_BASE,
    "GET",
    `${bucketPath(bucket)}/lifecycle`,
    undefined,
    fetchFn,
  );
  return {
    configured: body.configured === true,
    rules: list(body.rules).map((r) => parseLifecycleRule(r)),
  };
}

export async function listRetention(
  bucket: string,
  fetchFn: FetchFn = fetch,
): Promise<RetentionRule[]> {
  const body = await restCall(
    OS_PROXY_BASE,
    "GET",
    `${bucketPath(bucket)}/retention`,
    undefined,
    fetchFn,
  );
  return list(body.rules).map((r) => parseRetention(r));
}
