/** Client for the Azure Blob emulator via the dashboard's in-process proxy. */

export const BLOB_PROXY_BASE = "/dashboard/api/v1/azure/blob";
export const DEFAULT_ACCOUNT = "devstoreaccount1";
export const DELIMITER = "/";

export type ContainerInfo = { name: string; lastModified: string; etag: string };

export type BlobItem = {
  name: string;
  lastModified: string;
  etag: string;
  contentType: string;
  contentLength: number;
};

export type BrowserEntry =
  | { kind: "folder"; name: string; prefix: string }
  | { kind: "blob"; name: string; blob: BlobItem };

export type BlobProperties = {
  etag: string;
  lastModified: string;
  contentType: string;
  contentLength: number;
  blobType: string;
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

export function parseContainerList(body: string): ContainerInfo[] {
  const doc = parseXml(body);
  return Array.from(doc.querySelectorAll("Container")).map((c) => ({
    name: text(c, "Name"),
    lastModified: text(c, "Last-Modified"),
    etag: text(c, "Etag"),
  }));
}

export function parseBlobList(body: string): BlobItem[] {
  const doc = parseXml(body);
  return Array.from(doc.querySelectorAll("Blob")).map((b) => ({
    name: text(b, "Name"),
    lastModified: text(b, "Last-Modified"),
    etag: text(b, "Etag"),
    contentType: text(b, "Content-Type"),
    contentLength: Number(text(b, "Content-Length")) || 0,
  }));
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
    blobUrl(account, undefined, undefined, "comp=list"),
    { method: "GET" },
    fetchFn,
  );
  return parseContainerList(await res.text());
}

export async function createContainer(
  account: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(blobUrl(account, name, undefined, "restype=container"), { method: "PUT" }, fetchFn);
}

export async function deleteContainer(
  account: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(blobUrl(account, name, undefined, "restype=container"), { method: "DELETE" }, fetchFn);
}

export async function listBlobs(
  account: string,
  container: string,
  fetchFn: FetchFn = fetch,
): Promise<BlobItem[]> {
  const res = await send(
    blobUrl(account, container, undefined, "restype=container&comp=list"),
    { method: "GET" },
    fetchFn,
  );
  return parseBlobList(await res.text());
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
  fetchFn: FetchFn = fetch,
): Promise<Blob> {
  const res = await send(blobUrl(account, container, name), { method: "GET" }, fetchFn);
  return res.blob();
}

export async function deleteBlob(
  account: string,
  container: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(blobUrl(account, container, name), { method: "DELETE" }, fetchFn);
}

export async function getBlobProperties(
  account: string,
  container: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<BlobProperties> {
  const res = await send(blobUrl(account, container, name), { method: "HEAD" }, fetchFn);
  return {
    etag: res.headers.get("ETag") ?? "",
    lastModified: res.headers.get("Last-Modified") ?? "",
    contentType: res.headers.get("Content-Type") ?? "",
    contentLength: Number(res.headers.get("Content-Length")) || 0,
    blobType: res.headers.get("x-ms-blob-type") ?? "",
  };
}

/** Lists one virtual-directory level of a flat blob list under prefix. */
export function browse(blobs: BlobItem[], prefix: string): BrowserEntry[] {
  const folders = new Map<string, BrowserEntry>();
  const files: BrowserEntry[] = [];
  for (const blob of blobs) {
    if (!blob.name.startsWith(prefix)) continue;
    const rest = blob.name.slice(prefix.length);
    const idx = rest.indexOf(DELIMITER);
    if (idx >= 0) {
      const name = rest.slice(0, idx);
      if (name && !folders.has(name)) {
        folders.set(name, { kind: "folder", name, prefix: `${prefix}${name}${DELIMITER}` });
      }
    } else if (rest) {
      files.push({ kind: "blob", name: rest, blob });
    }
  }
  const byName = (a: BrowserEntry, b: BrowserEntry) => a.name.localeCompare(b.name);
  return [...folders.values().toArray().toSorted(byName), ...files.toSorted(byName)];
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
