/** Shared client plumbing for the dashboard's in-process Oracle Cloud proxies (opc-next-page paging, compartments). */

import { GcpRestError, restCall, str, type FetchFn, type Json } from "./gcprest.ts";

const enc = encodeURIComponent;
const arr = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const rec = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});

async function ociGet(
  base: string,
  path: string,
  fetchFn: FetchFn,
): Promise<{ body: unknown; next: string }> {
  let res: Response;
  try {
    res = await fetchFn(`${base}${path}`);
  } catch (e) {
    throw new GcpRestError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  const text = await res.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    // non-JSON error body: keep the status text
  }
  if (!res.ok) {
    const b = rec(body);
    throw new GcpRestError(
      res.status,
      str(b.code),
      str(b.message) || res.statusText || `HTTP ${res.status}`,
    );
  }
  return { body, next: res.headers.get("opc-next-page") ?? "" };
}

/** Follows opc-next-page through a list endpoint; `key` names the array when the body is an envelope. */
export async function ociListPages(
  base: string,
  path: string,
  query: string,
  key: string,
  fetchFn: FetchFn = fetch,
): Promise<Json[]> {
  const out: Json[] = [];
  let page = "";
  do {
    const qs = [query, page ? `page=${enc(page)}` : ""].filter(Boolean).join("&");
    const { body, next } = await ociGet(base, qs ? `${path}?${qs}` : path, fetchFn);
    out.push(...arr(key ? rec(body)[key] : body));
    page = next;
  } while (page);
  return out;
}

export async function ociGetOne(
  base: string,
  path: string,
  fetchFn: FetchFn = fetch,
): Promise<Json> {
  return rec((await ociGet(base, path, fetchFn)).body);
}

export async function ociCompartments(base: string, fetchFn: FetchFn = fetch): Promise<string[]> {
  const body = await restCall(base, "GET", "/compartments", undefined, fetchFn);
  return (Array.isArray(body.compartments) ? body.compartments : []).map(String);
}

export function readStoredValue(key: string): string {
  try {
    return localStorage.getItem(key)?.trim() ?? "";
  } catch {
    return "";
  }
}

export function writeStoredValue(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    // storage unavailable: selection applies for this session only
  }
}

/** Compact display form of an OCID. */
export const shortOcid = (ocid: string): string =>
  ocid.length > 20 ? `…${ocid.slice(-12)}` : ocid;
