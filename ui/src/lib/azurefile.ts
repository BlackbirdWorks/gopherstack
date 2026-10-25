/** Client for the Azure Files emulator via the dashboard's in-process proxy (XML list responses, raw downloads). */

export const FILE_PROXY_BASE = "/dashboard/api/v1/azure/file";
export const FILE_ACCOUNTS_URL = "/dashboard/api/v1/azure/fileaccounts";
export const DEFAULT_ACCOUNT = "devstoreaccount1";
export const ACCOUNT_KEY = "gopherstack_azurefile_account";

type FetchFn = typeof fetch;

export class FileError extends Error {
  readonly httpStatus: number;
  readonly code: string;

  constructor(httpStatus: number, code: string, message: string) {
    super(message);
    this.name = "FileError";
    this.httpStatus = httpStatus;
    this.code = code;
  }
}

export type ShareInfo = {
  name: string;
  snapshot: string;
  quotaGb: number;
  accessTier: string;
  lastModified: string;
  etag: string;
};

export type DirEntry =
  | { kind: "directory"; name: string }
  | { kind: "file"; name: string; size: number };

export type DirListing = { entries: DirEntry[]; nextMarker: string };

const enc = encodeURIComponent;

function parseXml(body: string): Document {
  const doc = new DOMParser().parseFromString(body, "application/xml");
  if (doc.querySelector("parsererror")) throw new FileError(0, "InvalidXml", "unreadable response");
  return doc;
}

const text = (el: Element | Document, tag: string): string =>
  el.querySelector(tag)?.textContent ?? "";

/** Path under the proxy for an account, optional share and in-share path (each segment encoded). */
export function filePath(account: string, share?: string, path?: string): string {
  const segs = [account, share, ...(path ? path.split("/") : [])]
    .filter((s): s is string => Boolean(s))
    .map((s) => enc(s));
  return `${FILE_PROXY_BASE}/${segs.join("/")}`;
}

/** The download URL of a file; same origin, so the browser saves it directly. */
export const fileDownloadUrl = (
  account: string,
  share: string,
  path: string,
  snapshot = "",
): string =>
  `${filePath(account, share, path)}${snapshot ? `?sharesnapshot=${enc(snapshot)}` : ""}`;

async function toError(res: Response): Promise<FileError> {
  const body = await res.text().catch(() => "");
  if (body.trim().startsWith("<")) {
    try {
      const doc = parseXml(body);
      const code = text(doc, "Code");
      if (code) return new FileError(res.status, code, text(doc, "Message") || code);
    } catch {
      // fall through to the raw body
    }
  }
  return new FileError(res.status, `HTTP${res.status}`, body.trim() || `HTTP ${res.status}`);
}

async function send(url: string, init: RequestInit, fetchFn: FetchFn): Promise<Response> {
  let res: Response;
  try {
    res = await fetchFn(url, init);
  } catch (e) {
    throw new FileError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await toError(res);
  return res;
}

export function describeFileError(e: unknown): string {
  if (e instanceof FileError) return e.code ? `${e.code}: ${e.message}` : e.message;
  return e instanceof Error ? e.message : String(e);
}

export function parseShares(body: string): ShareInfo[] {
  return [...parseXml(body).querySelectorAll("Shares > Share")].map((el) => ({
    name: text(el, "Name"),
    snapshot: text(el, "Snapshot"),
    quotaGb: Number(text(el, "Properties > Quota")) || 0,
    accessTier: text(el, "Properties > AccessTier"),
    lastModified: text(el, "Properties > Last-Modified"),
    etag: text(el, "Properties > Etag"),
  }));
}

export function parseDirectory(body: string): DirListing {
  const doc = parseXml(body);
  const entries: DirEntry[] = [];
  for (const el of doc.querySelectorAll("Entries > Directory")) {
    entries.push({ kind: "directory", name: text(el, "Name") });
  }
  for (const el of doc.querySelectorAll("Entries > File")) {
    entries.push({
      kind: "file",
      name: text(el, "Name"),
      size: Number(text(el, "Properties > Content-Length")) || 0,
    });
  }
  return {
    entries,
    nextMarker: doc.querySelector("EnumerationResults > NextMarker")?.textContent ?? "",
  };
}

export async function listAccounts(fetchFn: FetchFn = fetch): Promise<string[]> {
  const res = await fetchFn(FILE_ACCOUNTS_URL);
  if (!res.ok) {
    let message = res.statusText || `HTTP ${res.status}`;
    try {
      message = ((await res.json()) as { error?: { message?: string } }).error?.message ?? message;
    } catch {
      // keep the status text
    }
    throw new FileError(res.status, `HTTP${res.status}`, message);
  }
  const body = (await res.json()) as { accounts?: unknown };
  return Array.isArray(body.accounts) ? body.accounts.map(String) : [];
}

/** Shares of an account, plus their snapshots when requested. */
export async function listShares(
  account: string,
  includeSnapshots = false,
  fetchFn: FetchFn = fetch,
): Promise<ShareInfo[]> {
  const q = includeSnapshots ? "comp=list&include=snapshots" : "comp=list";
  const res = await send(`${filePath(account)}?${q}`, { method: "GET" }, fetchFn);
  return parseShares(await res.text());
}

export async function listDirectory(
  account: string,
  share: string,
  path: string,
  opts: { snapshot?: string; marker?: string } = {},
  fetchFn: FetchFn = fetch,
): Promise<DirListing> {
  const q = new URLSearchParams({ restype: "directory", comp: "list" });
  if (opts.snapshot) q.set("sharesnapshot", opts.snapshot);
  if (opts.marker) q.set("marker", opts.marker);
  const res = await send(
    `${filePath(account, share, path)}?${q.toString()}`,
    { method: "GET" },
    fetchFn,
  );
  return parseDirectory(await res.text());
}

export async function deleteFile(
  account: string,
  share: string,
  path: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await send(filePath(account, share, path), { method: "DELETE" }, fetchFn);
}

export function readStoredAccount(): string {
  try {
    return localStorage.getItem(ACCOUNT_KEY)?.trim() || "";
  } catch {
    return "";
  }
}

export function writeStoredAccount(account: string): void {
  try {
    localStorage.setItem(ACCOUNT_KEY, account);
  } catch {
    // storage unavailable: the account applies for this session only
  }
}
