/** Shared client for the dashboard's in-process GCP REST proxies (Pub/Sub, Secret Manager). */

export class GcpRestError extends Error {
  readonly httpStatus: number;
  readonly code: string;

  constructor(httpStatus: number, code: string, message: string) {
    super(message);
    this.name = "GcpRestError";
    this.httpStatus = httpStatus;
    this.code = code;
  }
}

export type FetchFn = typeof fetch;
export type Json = Record<string, unknown>;

async function failure(res: Response): Promise<GcpRestError> {
  let code = "";
  let message = res.statusText || `HTTP ${res.status}`;
  try {
    const body = (await res.json()) as {
      error?: { message?: string; status?: string };
      code?: string;
      message?: string;
    };
    message = body.error?.message ?? body.message ?? message;
    code = body.error?.status ?? body.code ?? "";
  } catch {
    // non-JSON error body: keep the status text
  }
  return new GcpRestError(res.status, code, message);
}

/** JSON request against a proxy base; resolves to the parsed body ({} when empty). */
export async function restCall<T = Json>(
  base: string,
  method: string,
  path: string,
  body?: unknown,
  fetchFn: FetchFn = fetch,
): Promise<T> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers = { "Content-Type": "application/json" };
  }
  let res: Response;
  try {
    res = await fetchFn(`${base}${path}`, init);
  } catch (e) {
    throw new GcpRestError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await failure(res);
  const text = await res.text();
  return (text ? JSON.parse(text) : {}) as T;
}

export function describeGcpError(e: unknown): string {
  if (e instanceof GcpRestError) return e.code ? `${e.code}: ${e.message}` : e.message;
  return e instanceof Error ? e.message : String(e);
}

export function str(v: unknown): string {
  return typeof v === "string" ? v : "";
}

export function strMap(v: unknown): Record<string, string> {
  if (!v || typeof v !== "object") return {};
  return Object.fromEntries(
    Object.entries(v as Json).filter((e): e is [string, string] => typeof e[1] === "string"),
  );
}

/** Last path segment of a resource name (projects/p/topics/t -> t). */
export function shortName(name: string): string {
  return name.slice(name.lastIndexOf("/") + 1);
}

export function utf8ToBase64(text: string): string {
  const bytes = new TextEncoder().encode(text);
  let bin = "";
  for (const b of bytes) bin += String.fromCodePoint(b);
  return btoa(bin);
}

/** Decodes base64 to text when it is valid UTF-8, else null. */
export function base64ToUtf8(b64: string): string | null {
  try {
    const bin = atob(b64);
    const bytes = Uint8Array.from(bin, (c) => c.codePointAt(0) ?? 0);
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    return null;
  }
}

export function isBase64(s: string): boolean {
  try {
    atob(s);
    return s.length % 4 === 0 && /^[A-Za-z0-9+/]*={0,2}$/.test(s);
  } catch {
    return false;
  }
}

/** Seconds to a protobuf Duration string ("600s"); null when empty or invalid. */
export function secondsToDuration(input: string): string | null {
  const t = input.trim();
  if (!/^\d+$/.test(t)) return null;
  return `${t}s`;
}

export function durationToSeconds(d: string): string {
  return d.endsWith("s") ? d.slice(0, -1) : d;
}

/** Follows nextPageToken through a list endpoint and concatenates the `key` arrays. */
export async function listAll(
  base: string,
  path: string,
  key: string,
  fetchFn: FetchFn = fetch,
  query = "",
): Promise<Json[]> {
  const out: Json[] = [];
  let token = "";
  do {
    const qs = [query, token ? `pageToken=${encodeURIComponent(token)}` : ""]
      .filter(Boolean)
      .join("&");
    const body = await restCall<Json>(base, "GET", qs ? `${path}?${qs}` : path, undefined, fetchFn);
    const items = body[key];
    if (Array.isArray(items)) out.push(...(items as Json[]));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}
