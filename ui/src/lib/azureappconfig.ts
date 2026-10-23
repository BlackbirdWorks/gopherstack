import { restCall, str, type FetchFn, type Json } from "./gcprest";

export const AC_PROXY_BASE = "/dashboard/api/v1/azure/appconfig";
export const MAX_KEY_LEN = 10240;
export const MAX_LABEL_LEN = 256;
/** Label filter value that selects key-values with no label. */
export const NULL_LABEL = "\0";

export type AcKeyValue = {
  key: string;
  label: string;
  value: string;
  contentType: string;
  etag: string;
  locked: boolean;
  lastModified: string;
};

const seg = (s: string) => encodeURIComponent(s);

export function parseKeyValue(raw: Json): AcKeyValue {
  return {
    key: str(raw.key),
    label: str(raw.label),
    value: str(raw.value),
    contentType: str(raw.contentType),
    etag: str(raw.etag),
    locked: raw.locked === true,
    lastModified: str(raw.lastModified),
  };
}

export async function listStores(fetchFn?: FetchFn): Promise<string[]> {
  const body = await restCall(AC_PROXY_BASE, "GET", "/stores", undefined, fetchFn);
  return Array.isArray(body.stores) ? (body.stores as string[]) : [];
}

export async function listLabels(store: string, fetchFn?: FetchFn): Promise<string[]> {
  const body = await restCall(
    AC_PROXY_BASE,
    "GET",
    `/stores/${seg(store)}/labels`,
    undefined,
    fetchFn,
  );
  return Array.isArray(body.labels) ? (body.labels as string[]) : [];
}

/** Key-values matching the App Configuration key/label filters (empty = all). */
export async function listKeyValues(
  store: string,
  keyFilter: string,
  labelFilter: string,
  fetchFn?: FetchFn,
): Promise<AcKeyValue[]> {
  const q = new URLSearchParams();
  if (keyFilter) q.set("key", keyFilter);
  if (labelFilter) q.set("label", labelFilter);
  const qs = q.size > 0 ? `?${q}` : "";
  const body = await restCall(
    AC_PROXY_BASE,
    "GET",
    `/stores/${seg(store)}/kv${qs}`,
    undefined,
    fetchFn,
  );
  return Array.isArray(body.items) ? (body.items as Json[]).map((x) => parseKeyValue(x)) : [];
}

export async function putKeyValue(
  store: string,
  kv: Pick<AcKeyValue, "key" | "label" | "value" | "contentType">,
  fetchFn?: FetchFn,
): Promise<AcKeyValue> {
  const raw = await restCall(AC_PROXY_BASE, "PUT", `/stores/${seg(store)}/kv`, kv, fetchFn);
  return parseKeyValue(raw);
}

export async function deleteKeyValue(
  store: string,
  key: string,
  label: string,
  fetchFn?: FetchFn,
): Promise<void> {
  const q = new URLSearchParams({ key, label });
  await restCall(AC_PROXY_BASE, "DELETE", `/stores/${seg(store)}/kv?${q}`, undefined, fetchFn);
}

export function validateKey(key: string): string {
  if (!key) return "Key is required";
  if (key === "." || key === "..") return "Key cannot be . or ..";
  if (key.length > MAX_KEY_LEN) return "Key is too long";
  return "";
}

export function validateLabel(label: string): string {
  if (label.includes("\0")) return "Label cannot contain NUL";
  return label.length > MAX_LABEL_LEN ? "Label is too long" : "";
}
