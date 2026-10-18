/** Client for the Azure Blob emulator via the dashboard's in-process proxy. */

export const BLOB_PROXY_BASE = "/dashboard/api/v1/azure/blob";
export const DEFAULT_ACCOUNT = "devstoreaccount1";
export const DELIMITER = "/";

export type PublicAccess = "" | "blob" | "container";
export type AccessTier = "Hot" | "Cool" | "Cold" | "Archive";
export const ACCESS_TIERS: AccessTier[] = ["Hot", "Cool", "Cold", "Archive"];
export const MAX_PAGE = 100;

export type Metadata = Record<string, string>;

export type ContainerInfo = {
  name: string;
  lastModified: string;
  etag: string;
  publicAccess: PublicAccess;
  metadata: Metadata;
};

export type BlobItem = {
  name: string;
  lastModified: string;
  etag: string;
  contentType: string;
  contentLength: number;
  snapshot: string;
  accessTier: string;
  metadata: Metadata;
};

export type BrowserEntry =
  | { kind: "folder"; name: string; prefix: string }
  | { kind: "blob"; name: string; blob: BlobItem };

export type BlobListing = { entries: BrowserEntry[]; nextMarker: string };

export type ListOptions = {
  prefix?: string;
  delimiter?: string;
  marker?: string;
  maxResults?: number;
  includeSnapshots?: boolean;
};

export type ContentProperties = {
  contentType: string;
  contentEncoding: string;
  contentLanguage: string;
  contentDisposition: string;
  cacheControl: string;
};

export type BlobProperties = ContentProperties & {
  etag: string;
  lastModified: string;
  contentLength: number;
  blobType: string;
  contentMd5: string;
  accessTier: string;
  accessTierInferred: boolean;
  metadata: Metadata;
};

export type ContainerProperties = {
  etag: string;
  lastModified: string;
  publicAccess: PublicAccess;
  metadata: Metadata;
};

export type SignedIdentifier = {
  id: string;
  start: string;
  expiry: string;
  permission: string;
};

export class BlobError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "BlobError";
    this.status = status;
    this.code = code;
  }
}

type FetchFn = typeof fetch;

/** Encodes each path segment but keeps "/" separators. */
export function encodePath(path: string): string {
  return path
    .split("/")
    .map((seg) => encodeURIComponent(seg))
    .join("/");
}

export function blobUrl(account: string, container?: string, blob?: string, query = ""): string {
  let url = `${BLOB_PROXY_BASE}/${encodeURIComponent(account)}`;
  if (container) url += `/${encodeURIComponent(container)}`;
  if (container && blob) url += `/${encodePath(blob)}`;
  return query ? `${url}?${query}` : url;
}

function text(parent: Element, tag: string): string {
  return parent.querySelector(tag)?.textContent ?? "";
}

function parseXml(body: string): Document {
  const doc = new DOMParser().parseFromString(body, "application/xml");
  if (doc.querySelector("parsererror")) {
    throw new BlobError(0, "InvalidResponse", "Blob service returned malformed XML");
  }
  return doc;
}

function metadataOf(parent: Element): Metadata {
  const out: Metadata = {};
  const md = Array.from(parent.children).find((c) => c.tagName === "Metadata");
  for (const child of Array.from(md?.children ?? [])) out[child.tagName] = child.textContent ?? "";
  return out;
}

function accessOf(v: string | null | undefined): PublicAccess {
  return v === "blob" || v === "container" ? v : "";
}

export function parseContainerList(body: string): ContainerInfo[] {
  const doc = parseXml(body);
  return Array.from(doc.querySelectorAll("Container")).map((c) => ({
    name: text(c, "Name"),
    lastModified: text(c, "Last-Modified"),
    etag: text(c, "Etag"),
    publicAccess: accessOf(text(c, "PublicAccess")),
    metadata: metadataOf(c),
  }));
}

/** Parses a List Blobs body: BlobPrefix entries become folders, Blob entries become blobs. */
export function parseBlobListing(body: string, prefix = "", delimiter = DELIMITER): BlobListing {
  const doc = parseXml(body);
  const entries: BrowserEntry[] = [];
  const blobs = doc.querySelector("Blobs");
  for (const el of Array.from(blobs?.children ?? [])) {
    if (el.tagName === "BlobPrefix") {
      const full = text(el, "Name");
      const rest = full.slice(prefix.length);
      const name = rest.endsWith(delimiter) ? rest.slice(0, -delimiter.length) : rest;
      entries.push({ kind: "folder", name, prefix: full });
    } else if (el.tagName === "Blob") {
      const full = text(el, "Name");
      entries.push({
        kind: "blob",
        name: full.slice(prefix.length),
        blob: {
          name: full,
          lastModified: text(el, "Last-Modified"),
          etag: text(el, "Etag"),
          contentType: text(el, "Content-Type"),
          contentLength: Number(text(el, "Content-Length")) || 0,
          snapshot: text(el, "Snapshot"),
          accessTier: text(el, "AccessTier"),
          metadata: metadataOf(el),
        },
      });
    }
  }
  return {
    entries,
    nextMarker: doc.querySelector("EnumerationResults > NextMarker")?.textContent ?? "",
  };
}

async function toBlobError(res: Response): Promise<BlobError> {
  const body = await res.text().catch(() => "");
  if (body.trim().startsWith("<")) {
    try {
      const doc = parseXml(body);
      const code = doc.querySelector("Code")?.textContent;
      const message = doc.querySelector("Message")?.textContent;
      if (code || message) {
        return new BlobError(
          res.status,
          code ?? `HTTP${res.status}`,
          message ?? code ?? `HTTP ${res.status}`,
        );
      }
    } catch {
      // fall through to the raw body
    }
  }
  return new BlobError(res.status, `HTTP${res.status}`, body.trim() || `HTTP ${res.status}`);
}

async function send(url: string, init: RequestInit, fetchFn: FetchFn): Promise<Response> {
  let res: Response;
  try {
    res = await fetchFn(url, init);
  } catch (e) {
    throw new BlobError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await toBlobError(res);
  return res;
}

export async function listContainers(
  account: string,
  fetchFn: FetchFn = fetch,
): Promise<ContainerInfo[]> {
  const res = await send(
    blobUrl(account, undefined, undefined, "comp=list&include=metadata"),
    { method: "GET" },
    fetchFn,
  );
  return parseContainerList(await res.text());
}

function metaHeaders(md: Metadata): Record<string, string> {
  return Object.fromEntries(Object.entries(md).map(([k, v]) => [`x-ms-meta-${k}`, v]));
}

function metaFrom(headers: Headers): Metadata {
  const out: Metadata = {};
  headers.forEach((value, key) => {
    if (key.toLowerCase().startsWith(META_PREFIX))
      out[key.slice(META_PREFIX.length).toLowerCase()] = value;
  });
  return out;
}

const META_PREFIX = "x-ms-meta-";

export async function createContainer(
  account: string,
  name: string,
  opts: { publicAccess?: PublicAccess; metadata?: Metadata } = {},
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const headers = metaHeaders(opts.metadata ?? {});
  if (opts.publicAccess) headers["x-ms-blob-public-access"] = opts.publicAccess;
  await send(
    blobUrl(account, name, undefined, "restype=container"),
    { method: "PUT", headers },
    fetchFn,
  );
}

export async function deleteContainer(
  account: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(blobUrl(account, name, undefined, "restype=container"), { method: "DELETE" }, fetchFn);
}

export async function getContainerProperties(
  account: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<ContainerProperties> {
  const res = await send(
    blobUrl(account, name, undefined, "restype=container"),
    { method: "GET" },
    fetchFn,
  );
  return {
    etag: res.headers.get("ETag") ?? "",
    lastModified: res.headers.get("Last-Modified") ?? "",
    publicAccess: accessOf(res.headers.get("x-ms-blob-public-access")),
    metadata: metaFrom(res.headers),
  };
}

export async function setContainerMetadata(
  account: string,
  name: string,
  metadata: Metadata,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(
    blobUrl(account, name, undefined, "restype=container&comp=metadata"),
    { method: "PUT", headers: metaHeaders(metadata) },
    fetchFn,
  );
}

export async function getContainerAcl(
  account: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<{ publicAccess: PublicAccess; identifiers: SignedIdentifier[] }> {
  const res = await send(
    blobUrl(account, name, undefined, "restype=container&comp=acl"),
    { method: "GET" },
    fetchFn,
  );
  const doc = parseXml(await res.text());
  const identifiers = Array.from(doc.querySelectorAll("SignedIdentifier")).map((e) => ({
    id: text(e, "Id"),
    start: text(e, "Start"),
    expiry: text(e, "Expiry"),
    permission: text(e, "Permission"),
  }));
  return { publicAccess: accessOf(res.headers.get("x-ms-blob-public-access")), identifiers };
}

function escapeXml(s: string): string {
  return s
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function xmlTag(name: string, v: string): string {
  return v ? `<${name}>${escapeXml(v)}</${name}>` : "";
}

export function aclBody(ids: SignedIdentifier[]): string {
  const items = ids
    .map(
      (i) =>
        `<SignedIdentifier><Id>${escapeXml(i.id)}</Id><AccessPolicy>${xmlTag("Start", i.start)}${xmlTag("Expiry", i.expiry)}${xmlTag("Permission", i.permission)}</AccessPolicy></SignedIdentifier>`,
    )
    .join("");
  return `<?xml version="1.0" encoding="utf-8"?><SignedIdentifiers>${items}</SignedIdentifiers>`;
}

/** Sets the access level; pass the existing identifiers so they are preserved. */
export async function setContainerAcl(
  account: string,
  name: string,
  publicAccess: PublicAccess,
  identifiers: SignedIdentifier[],
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const headers: Record<string, string> = { "Content-Type": "application/xml" };
  if (publicAccess) headers["x-ms-blob-public-access"] = publicAccess;
  await send(
    blobUrl(account, name, undefined, "restype=container&comp=acl"),
    { method: "PUT", headers, body: aclBody(identifiers) },
    fetchFn,
  );
}

export async function listBlobs(
  account: string,
  container: string,
  opts: ListOptions = {},
  fetchFn: FetchFn = fetch,
): Promise<BlobListing> {
  const q = new URLSearchParams({ restype: "container", comp: "list" });
  const prefix = opts.prefix ?? "";
  const delimiter = opts.delimiter ?? DELIMITER;
  if (prefix) q.set("prefix", prefix);
  if (delimiter) q.set("delimiter", delimiter);
  if (opts.marker) q.set("marker", opts.marker);
  if (opts.maxResults) q.set("maxresults", String(opts.maxResults));
  if (opts.includeSnapshots) q.set("include", "snapshots,metadata");
  else q.set("include", "metadata");
  const res = await send(
    blobUrl(account, container, undefined, q.toString()),
    { method: "GET" },
    fetchFn,
  );
  return parseBlobListing(await res.text(), prefix, delimiter);
}

/** Lists the snapshots of one blob, oldest first. */
export async function listSnapshots(
  account: string,
  container: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<BlobItem[]> {
  const out: BlobItem[] = [];
  let marker = "";
  do {
    const page = await listBlobs(
      account,
      container,
      { prefix: name, delimiter: "", marker, includeSnapshots: true },
      fetchFn,
    );
    for (const e of page.entries) {
      if (e.kind === "blob" && e.blob.name === name && e.blob.snapshot) out.push(e.blob);
    }
    marker = page.nextMarker;
  } while (marker);
  return out;
}

export async function uploadBlob(
  account: string,
  container: string,
  name: string,
  data: Blob,
  contentType?: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(
    blobUrl(account, container, name),
    {
      method: "PUT",
      headers: {
        "x-ms-blob-type": "BlockBlob",
        "Content-Type": contentType || data.type || "application/octet-stream",
      },
      body: data,
    },
    fetchFn,
  );
}

export async function downloadBlob(
  account: string,
  container: string,
  name: string,
  snapshot = "",
  fetchFn: FetchFn = fetch,
): Promise<Blob> {
  const res = await send(
    blobUrl(account, container, name, snapshotQuery(snapshot)),
    { method: "GET" },
    fetchFn,
  );
  return res.blob();
}

function snapshotQuery(snapshot: string): string {
  return snapshot ? `snapshot=${encodeURIComponent(snapshot)}` : "";
}

/** Deletes a blob, or one snapshot; deleteSnapshots removes the base blob's snapshots too. */
export async function deleteBlob(
  account: string,
  container: string,
  name: string,
  opts: { snapshot?: string; deleteSnapshots?: "include" | "only" } = {},
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const headers: Record<string, string> = {};
  if (opts.deleteSnapshots) headers["x-ms-delete-snapshots"] = opts.deleteSnapshots;
  await send(
    blobUrl(account, container, name, snapshotQuery(opts.snapshot ?? "")),
    { method: "DELETE", headers },
    fetchFn,
  );
}

export async function getBlobProperties(
  account: string,
  container: string,
  name: string,
  snapshot = "",
  fetchFn: FetchFn = fetch,
): Promise<BlobProperties> {
  const res = await send(
    blobUrl(account, container, name, snapshotQuery(snapshot)),
    { method: "HEAD" },
    fetchFn,
  );
  const h = res.headers;
  return {
    etag: h.get("ETag") ?? "",
    lastModified: h.get("Last-Modified") ?? "",
    contentType: h.get("Content-Type") ?? "",
    contentEncoding: h.get("Content-Encoding") ?? "",
    contentLanguage: h.get("Content-Language") ?? "",
    contentDisposition: h.get("Content-Disposition") ?? "",
    cacheControl: h.get("Cache-Control") ?? "",
    contentMd5: h.get("Content-MD5") ?? "",
    contentLength: Number(h.get("Content-Length")) || 0,
    blobType: h.get("x-ms-blob-type") ?? "",
    accessTier: h.get("x-ms-access-tier") ?? "",
    accessTierInferred: h.get("x-ms-access-tier-inferred") === "true",
    metadata: metaFrom(h),
  };
}

/** Replaces the blob's content properties; empty values are cleared, as in Azure. */
export async function setBlobProperties(
  account: string,
  container: string,
  name: string,
  props: ContentProperties & { contentMd5?: string },
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const map: [string, string | undefined][] = [
    ["x-ms-blob-content-type", props.contentType],
    ["x-ms-blob-content-encoding", props.contentEncoding],
    ["x-ms-blob-content-language", props.contentLanguage],
    ["x-ms-blob-content-disposition", props.contentDisposition],
    ["x-ms-blob-cache-control", props.cacheControl],
    ["x-ms-blob-content-md5", props.contentMd5],
  ];
  const headers = Object.fromEntries(map.filter(([, v]) => v)) as Record<string, string>;
  await send(
    blobUrl(account, container, name, "comp=properties"),
    { method: "PUT", headers },
    fetchFn,
  );
}

export async function setBlobMetadata(
  account: string,
  container: string,
  name: string,
  metadata: Metadata,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(
    blobUrl(account, container, name, "comp=metadata"),
    { method: "PUT", headers: metaHeaders(metadata) },
    fetchFn,
  );
}

export async function setBlobTier(
  account: string,
  container: string,
  name: string,
  tier: AccessTier,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(
    blobUrl(account, container, name, "comp=tier"),
    { method: "PUT", headers: { "x-ms-access-tier": tier } },
    fetchFn,
  );
}

/** Creates a snapshot and returns its id. */
export async function createSnapshot(
  account: string,
  container: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const res = await send(
    blobUrl(account, container, name, "comp=snapshot"),
    { method: "PUT" },
    fetchFn,
  );
  return res.headers.get("x-ms-snapshot") ?? "";
}

export function breadcrumbs(prefix: string): { label: string; prefix: string }[] {
  const parts = prefix.split(DELIMITER).filter(Boolean);
  return parts.map((label, i) => ({
    label,
    prefix: `${parts.slice(0, i + 1).join(DELIMITER)}${DELIMITER}`,
  }));
}

export function validateContainerName(name: string): string | null {
  if (!name) return "Name is required";
  if (name.length < 3 || name.length > 63) return "Must be 3-63 characters";
  if (!/^[a-z0-9-]+$/.test(name)) return "Use lowercase letters, numbers and hyphens only";
  if (!/^[a-z0-9]/.test(name) || name.endsWith("-"))
    return "Must start with a letter or number and not end with a hyphen";
  if (name.includes("--")) return "Consecutive hyphens are not allowed";
  return null;
}

export function validateMetadataKey(key: string): string | null {
  if (!key) return "Key is required";
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(key))
    return "Use letters, digits and underscores; must not start with a digit";
  return null;
}

export type MetaRow = { key: string; value: string };

export function metadataToRows(md: Metadata): MetaRow[] {
  return Object.entries(md)
    .toSorted(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => ({ key, value }));
}

/** Returns the first problem with the rows, or null; keys compare case-insensitively. */
export function validateRows(rows: MetaRow[]): string | null {
  const seen = new Set<string>();
  for (const row of rows) {
    const bad = validateMetadataKey(row.key);
    if (bad) return `${row.key || "(empty)"}: ${bad}`;
    const k = row.key.toLowerCase();
    if (seen.has(k)) return `${row.key}: duplicate key`;
    seen.add(k);
  }
  return null;
}

export function rowsToMetadata(rows: MetaRow[]): Metadata {
  return Object.fromEntries(rows.map((r) => [r.key.toLowerCase(), r.value]));
}

export function validateBlobName(name: string): string | null {
  if (!name) return "Name is required";
  if (name.length > 1024) return "Must be at most 1024 characters";
  if (name.startsWith(DELIMITER) || name.endsWith(DELIMITER)) return "Must not start or end with /";
  if (name.split(DELIMITER).some((s) => s === "." || s === ".."))
    return "Path segments cannot be . or ..";
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
