/** Client for the Azure Key Vault emulator via the dashboard's in-process proxy. */

export const KV_PROXY_BASE = "/dashboard/api/v1/azure/keyvault";
export const VAULT_NAME_RE = /^[A-Za-z][A-Za-z0-9-]{1,22}[A-Za-z0-9]$/;
export const OBJECT_NAME_RE = /^[A-Za-z0-9-]{1,127}$/;
export const MAX_VALIDITY_MONTHS = 1200;
const PAGE_SIZE = 100;
const MAX_PAGES = 50;

export type Kind = "secrets" | "keys" | "certificates";

export type KvItem = {
  name: string;
  version: string;
  enabled: boolean;
  created: number | null;
  updated: number | null;
  deletedDate: number | null;
  scheduledPurgeDate: number | null;
  contentType: string;
  managed: boolean;
  tags: Record<string, string>;
};

export type KeyDetail = KvItem & { kty: string; crv: string; keyOps: string[]; keySize: number | null };
export type CertDetail = KvItem & { x5t: string; cer: string; subject: string };

export class KvError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "KvError";
    this.status = status;
    this.code = code;
  }
}

type FetchFn = typeof fetch;
type Json = Record<string, unknown>;

const seg = (s: string) => encodeURIComponent(s);

export function validateVaultName(name: string): string {
  if (!name) return "Vault name is required";
  return VAULT_NAME_RE.test(name) ? "" : "3-24 characters: letters, digits and hyphens, starting with a letter";
}

export function validateObjectName(name: string): string {
  if (!name) return "Name is required";
  return OBJECT_NAME_RE.test(name) ? "" : "1-127 characters: letters, digits and hyphens";
}

export function validateMonths(raw: string): string {
  const n = Number(raw);
  if (!raw.trim() || !Number.isInteger(n) || n < 1 || n > MAX_VALIDITY_MONTHS) {
    return `Whole number of months, 1-${MAX_VALIDITY_MONTHS}`;
  }
  return "";
}

export function vaultUrl(vault: string, path: string, query = ""): string {
  const url = `${KV_PROXY_BASE}/vaults/${seg(vault)}/${path}`;
  return query ? `${url}?${query}` : url;
}

export function objectUrl(vault: string, area: string, name: string, tail = "", query = ""): string {
  return vaultUrl(vault, `${area}/${seg(name)}${tail ? `/${tail}` : ""}`, query);
}

/** Base64url (no padding) encodes bytes. */
export function b64uEncode(bytes: Uint8Array): string {
  let bin = "";
  for (const b of bytes) bin += String.fromCodePoint(b);
  return btoa(bin).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

export function b64uDecode(s: string): Uint8Array {
  const std = s.replaceAll("-", "+").replaceAll("_", "/");
  const bin = atob(std + "=".repeat((4 - (std.length % 4)) % 4));
  return Uint8Array.from(bin, (c) => c.codePointAt(0) ?? 0);
}

export const textBytes = (s: string): Uint8Array => new TextEncoder().encode(s);
export const bytesText = (b: Uint8Array): string => new TextDecoder().decode(b);

/** Splits a Key Vault object id into its name and (optional) version. */
export function parseId(id: string): { name: string; version: string } {
  const parts = id.split("/").filter(Boolean);
  const i = parts.findIndex((p) => ["secrets", "keys", "certificates", "deletedsecrets", "deletedkeys", "deletedcertificates"].includes(p));
  if (i < 0) return { name: parts.at(-1) ?? "", version: "" };
  return { name: decodeURIComponent(parts[i + 1] ?? ""), version: parts[i + 2] ?? "" };
}

const num = (v: unknown) => (typeof v === "number" ? v : null);

function item(raw: Json, idField: string): KvItem {
  const attrs = (raw.attributes ?? {}) as Json;
  const { name, version } = parseId(String(raw[idField] ?? ""));
  return {
    name,
    version,
    enabled: attrs.enabled !== false,
    created: num(attrs.created),
    updated: num(attrs.updated),
    deletedDate: num(raw.deletedDate),
    scheduledPurgeDate: num(raw.scheduledPurgeDate),
    managed: raw.managed === true,
    contentType: typeof raw.contentType === "string" ? raw.contentType : "",
    tags: (raw.tags ?? {}) as Record<string, string>,
  };
}

export function parseKeyDetail(raw: Json): KeyDetail {
  const jwk = (raw.key ?? {}) as Json;
  const base = item({ ...raw, kid: raw.kid ?? jwk.kid }, "kid");
  let keySize: number | null = null;
  if (typeof jwk.n === "string") keySize = b64uDecode(jwk.n).length * 8;
  return {
    ...base,
    kty: String(jwk.kty ?? ""),
    crv: String(jwk.crv ?? ""),
    keyOps: Array.isArray(jwk.key_ops) ? (jwk.key_ops as string[]) : [],
    keySize,
  };
}

/** Subject common name from the policy, when present. */
export function parseCertDetail(raw: Json): CertDetail {
  const policy = (raw.policy ?? {}) as Json;
  const x509 = (policy.x509_props ?? {}) as Json;
  return {
    ...item(raw, "id"),
    x5t: typeof raw.x5t === "string" ? raw.x5t : "",
    cer: typeof raw.cer === "string" ? raw.cer : "",
    subject: typeof x509.subject === "string" ? x509.subject : "",
  };
}

/** Wraps the base64 DER certificate in a PEM block. */
export function certPem(cerBase64: string): string {
  const std = cerBase64.replaceAll("-", "+").replaceAll("_", "/");
  const lines = std.match(/.{1,64}/g) ?? [];
  return `-----BEGIN CERTIFICATE-----\n${lines.join("\n")}\n-----END CERTIFICATE-----\n`;
}

/** Converts the base64url x5t thumbprint into colon-separated uppercase hex. */
export function thumbprintHex(x5t: string): string {
  if (!x5t) return "";
  return Array.from(b64uDecode(x5t), (b) => b.toString(16).padStart(2, "0").toUpperCase()).join(":");
}

export function formatEpoch(epoch: number | null): string {
  return epoch === null ? "—" : new Date(epoch * 1000).toLocaleString();
}

export type KeyAlgs = { encrypt: string[]; sign: string[] };

/** Algorithms a key type supports for the try-it panel. */
export function algsFor(kty: string, crv: string): KeyAlgs {
  if (kty.startsWith("RSA")) return { encrypt: ["RSA-OAEP", "RSA-OAEP-256"], sign: ["RS256", "RS384", "RS512", "PS256"] };
  if (kty.startsWith("EC")) {
    const sign = { "P-384": "ES384", "P-521": "ES512" }[crv] ?? "ES256";
    return { encrypt: [], sign: [sign] };
  }
  return { encrypt: [], sign: [] };
}

const HASH_BY_SUFFIX: Record<string, string> = { "256": "SHA-256", "384": "SHA-384", "512": "SHA-512" };

export async function digestFor(alg: string, text: string): Promise<string> {
  const hash = HASH_BY_SUFFIX[alg.slice(-3)] ?? "SHA-256";
  const buf = await crypto.subtle.digest(hash, textBytes(text) as BufferSource);
  return b64uEncode(new Uint8Array(buf));
}

async function toKvError(res: Response): Promise<KvError> {
  const body = (await res.text().catch(() => "")).trim();
  try {
    const json = JSON.parse(body) as { error?: { code?: string; message?: string } };
    if (json.error) {
      return new KvError(res.status, json.error.code ?? `HTTP${res.status}`, json.error.message ?? `HTTP ${res.status}`);
    }
  } catch {
    // fall through to the raw body
  }
  return new KvError(res.status, `HTTP${res.status}`, body || `HTTP ${res.status}`);
}

async function call(url: string, method: string, body: unknown, fetchFn: FetchFn): Promise<Json | null> {
  const init: RequestInit = { method };
  if (body !== undefined) {
    init.body = JSON.stringify(body);
    init.headers = { "Content-Type": "application/json" };
  }
  let res: Response;
  try {
    res = await fetchFn(url, init);
  } catch (e) {
    throw new KvError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await toKvError(res);
  const text = await res.text();
  return text ? (JSON.parse(text) as Json) : null;
}

async function listAll(vault: string, path: string, fetchFn: FetchFn): Promise<Json[]> {
  const out: Json[] = [];
  let token = "";
  for (let i = 0; i < MAX_PAGES; i++) {
    const q = `maxresults=${PAGE_SIZE}${token ? `&$skiptoken=${encodeURIComponent(token)}` : ""}`;
    const res = (await call(vaultUrl(vault, path, q), "GET", undefined, fetchFn)) ?? {};
    out.push(...((res.value as Json[] | undefined) ?? []));
    const next = typeof res.nextLink === "string" ? new URL(res.nextLink).searchParams.get("$skiptoken") : null;
    if (!next) break;
    token = next;
  }
  return out;
}

const idFieldFor = (kind: Kind) => (kind === "keys" ? "kid" : "id");
const deletedArea = (kind: Kind) => `deleted${kind}`;

export async function listVaults(fetchFn: FetchFn = fetch): Promise<string[]> {
  const res = await call(`${KV_PROXY_BASE}/vaults`, "GET", undefined, fetchFn);
  return (res?.vaults as string[] | undefined) ?? [];
}

export async function createVault(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await call(`${KV_PROXY_BASE}/vaults`, "POST", { name }, fetchFn);
}

/** Lists objects; secrets and keys backing a certificate (managed) are hidden. */
export async function listItems(vault: string, kind: Kind, deleted = false, fetchFn: FetchFn = fetch): Promise<KvItem[]> {
  const raws = await listAll(vault, deleted ? deletedArea(kind) : kind, fetchFn);
  return raws
    .map((r) => item(r, idFieldFor(kind)))
    .filter((i) => !i.managed)
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

export async function listVersions(vault: string, kind: Kind, name: string, fetchFn: FetchFn = fetch): Promise<KvItem[]> {
  const raws = await listAll(vault, `${kind}/${seg(name)}/versions`, fetchFn);
  return raws.map((r) => item(r, idFieldFor(kind))).toSorted((a, b) => (b.created ?? 0) - (a.created ?? 0));
}

export async function recoverItem(vault: string, kind: Kind, name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await call(objectUrl(vault, deletedArea(kind), name, "recover"), "POST", undefined, fetchFn);
}

export async function purgeItem(vault: string, kind: Kind, name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await call(objectUrl(vault, deletedArea(kind), name), "DELETE", undefined, fetchFn);
}

export async function deleteItem(vault: string, kind: Kind, name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await call(objectUrl(vault, kind, name), "DELETE", undefined, fetchFn);
}

export async function setSecret(
  vault: string,
  name: string,
  value: string,
  contentType = "",
  fetchFn: FetchFn = fetch,
): Promise<KvItem> {
  const body: Json = { value };
  if (contentType) body.contentType = contentType;
  return item((await call(objectUrl(vault, "secrets", name), "PUT", body, fetchFn)) ?? {}, "id");
}

/** Reads a secret's value; callers must not log or persist it. */
export async function revealSecret(
  vault: string,
  name: string,
  version = "",
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const res = await call(objectUrl(vault, "secrets", name, version), "GET", undefined, fetchFn);
  return typeof res?.value === "string" ? res.value : "";
}

export type KeySpec = { name: string; kty: "RSA" | "EC"; size?: number; crv?: string };

export async function createKey(vault: string, spec: KeySpec, fetchFn: FetchFn = fetch): Promise<KeyDetail> {
  const body: Json = { kty: spec.kty };
  if (spec.kty === "RSA") body.key_size = spec.size ?? 2048;
  else body.crv = spec.crv ?? "P-256";
  return parseKeyDetail((await call(objectUrl(vault, "keys", spec.name, "create"), "POST", body, fetchFn)) ?? {});
}

export async function rotateKey(vault: string, name: string, fetchFn: FetchFn = fetch): Promise<KeyDetail> {
  return parseKeyDetail((await call(objectUrl(vault, "keys", name, "rotate"), "POST", undefined, fetchFn)) ?? {});
}

export async function getKey(vault: string, name: string, version = "", fetchFn: FetchFn = fetch): Promise<KeyDetail> {
  return parseKeyDetail((await call(objectUrl(vault, "keys", name, version), "GET", undefined, fetchFn)) ?? {});
}

async function keyOp(vault: string, name: string, version: string, op: string, body: Json, fetchFn: FetchFn): Promise<Json> {
  const tail = version ? `${seg(version)}/${op}` : op;
  return (await call(objectUrl(vault, "keys", name, tail), "POST", body, fetchFn)) ?? {};
}

export async function encryptText(vault: string, name: string, version: string, alg: string, text: string, fetchFn: FetchFn = fetch): Promise<string> {
  const res = await keyOp(vault, name, version, "encrypt", { alg, value: b64uEncode(textBytes(text)) }, fetchFn);
  return String(res.value ?? "");
}

export async function decryptText(vault: string, name: string, version: string, alg: string, cipher: string, fetchFn: FetchFn = fetch): Promise<string> {
  const res = await keyOp(vault, name, version, "decrypt", { alg, value: cipher.trim() }, fetchFn);
  return bytesText(b64uDecode(String(res.value ?? "")));
}

export async function signText(vault: string, name: string, version: string, alg: string, text: string, fetchFn: FetchFn = fetch): Promise<string> {
  const res = await keyOp(vault, name, version, "sign", { alg, value: await digestFor(alg, text) }, fetchFn);
  return String(res.value ?? "");
}

export async function verifyText(vault: string, name: string, version: string, alg: string, text: string, signature: string, fetchFn: FetchFn = fetch): Promise<boolean> {
  const res = await keyOp(vault, name, version, "verify", { alg, digest: await digestFor(alg, text), value: signature.trim() }, fetchFn);
  return res.value === true;
}

export type CertSpec = { name: string; subject: string; validityMonths: number };

export function certBody(spec: CertSpec): Json {
  return {
    policy: {
      key_props: { exportable: true, kty: "RSA", key_size: 2048, reuse_key: false },
      secret_props: { contentType: "application/x-pem-file" },
      x509_props: { subject: spec.subject, validity_months: spec.validityMonths },
      issuer: { name: "Self" },
    },
  };
}

export async function createCert(vault: string, spec: CertSpec, fetchFn: FetchFn = fetch): Promise<CertDetail> {
  await call(objectUrl(vault, "certificates", spec.name, "create"), "POST", certBody(spec), fetchFn);
  return getCert(vault, spec.name, "", fetchFn);
}

export async function getCert(vault: string, name: string, version = "", fetchFn: FetchFn = fetch): Promise<CertDetail> {
  return parseCertDetail((await call(objectUrl(vault, "certificates", name, version), "GET", undefined, fetchFn)) ?? {});
}
