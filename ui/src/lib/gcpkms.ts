/** Client for the Cloud KMS emulator via the dashboard's in-process REST proxy. */

import {
  base64ToUtf8,
  durationToSeconds,
  restCall,
  shortName,
  str,
  strMap,
  utf8ToBase64,
  type FetchFn,
  type Json,
} from "./gcprest.ts";

export const KMS_PROXY_BASE = "/dashboard/api/v1/gcp/kms/v1";
export const KMS_PROJECT_KEY = "gopherstack_kms_project";
export const KMS_LOCATION_KEY = "gopherstack_kms_location";
export const DEFAULT_KMS_PROJECT = "gopherstack-local";
export const DEFAULT_KMS_LOCATION = "global";
const PAGE = 100;
const DAY_SECONDS = 86400;

export type Purpose = "ENCRYPT_DECRYPT" | "ASYMMETRIC_SIGN" | "ASYMMETRIC_DECRYPT" | "MAC";

export const PURPOSES: readonly { id: Purpose; label: string }[] = [
  { id: "ENCRYPT_DECRYPT", label: "Symmetric encrypt/decrypt" },
  { id: "ASYMMETRIC_SIGN", label: "Asymmetric sign" },
  { id: "ASYMMETRIC_DECRYPT", label: "Asymmetric decrypt" },
  { id: "MAC", label: "MAC sign/verify" },
];

export const ALGORITHMS: Record<Purpose, readonly string[]> = {
  ENCRYPT_DECRYPT: ["GOOGLE_SYMMETRIC_ENCRYPTION"],
  ASYMMETRIC_SIGN: [
    "EC_SIGN_P256_SHA256",
    "EC_SIGN_P384_SHA384",
    "EC_SIGN_ED25519",
    "RSA_SIGN_PSS_2048_SHA256",
    "RSA_SIGN_PSS_3072_SHA256",
    "RSA_SIGN_PSS_4096_SHA256",
    "RSA_SIGN_PKCS1_2048_SHA256",
    "RSA_SIGN_PKCS1_3072_SHA256",
    "RSA_SIGN_PKCS1_4096_SHA256",
  ],
  ASYMMETRIC_DECRYPT: [
    "RSA_DECRYPT_OAEP_2048_SHA256",
    "RSA_DECRYPT_OAEP_3072_SHA256",
    "RSA_DECRYPT_OAEP_4096_SHA256",
  ],
  MAC: ["HMAC_SHA256", "HMAC_SHA384", "HMAC_SHA512"],
};

export const PROTECTION_LEVELS = ["SOFTWARE", "HSM"] as const;

export type Labels = Record<string, string>;

export type KeyRingInfo = { name: string; id: string; createTime: string };

export type KeyInfo = {
  name: string;
  id: string;
  purpose: string;
  algorithm: string;
  protectionLevel: string;
  primaryId: string;
  rotationPeriod: string;
  nextRotation: string;
  createTime: string;
  labels: Labels;
};

export type VersionInfo = {
  name: string;
  id: string;
  state: string;
  algorithm: string;
  protectionLevel: string;
  createTime: string;
  destroyTime: string;
};

export type VersionAction = "enable" | "disable" | "destroy" | "restore";

const enc = encodeURIComponent;
const locationPath = (p: string, l: string) => `/projects/${enc(p)}/locations/${enc(l)}`;
const ringPath = (p: string, l: string, r: string) => `${locationPath(p, l)}/keyRings/${enc(r)}`;
const keyPath = (p: string, l: string, r: string, k: string) =>
  `${ringPath(p, l, r)}/cryptoKeys/${enc(k)}`;
const versionPath = (p: string, l: string, r: string, k: string, v: string) =>
  `${keyPath(p, l, r, k)}/cryptoKeyVersions/${enc(v)}`;

export function validateKmsId(id: string): string | null {
  if (!id) return "name is required";
  if (!/^[A-Za-z0-9_-]{1,63}$/.test(id)) return "use letters, digits, - and _ (max 63)";
  return null;
}

export function validateLabelKey(key: string): string | null {
  if (!key) return "key is required";
  if (!/^[a-z][a-z0-9_-]{0,62}$/.test(key))
    return "keys start with a lowercase letter; use a-z, 0-9, _ and -";
  return null;
}

export function parseKeyRing(raw: Json): KeyRingInfo {
  const name = str(raw.name);
  return { name, id: shortName(name), createTime: str(raw.createTime) };
}

export function parseKey(raw: Json): KeyInfo {
  const name = str(raw.name);
  const primary = (raw.primary ?? {}) as Json;
  const template = (raw.versionTemplate ?? {}) as Json;
  const period = str(raw.rotationPeriod);
  return {
    name,
    id: shortName(name),
    purpose: str(raw.purpose),
    algorithm: str(primary.algorithm) || str(template.algorithm),
    protectionLevel: str(primary.protectionLevel) || str(template.protectionLevel),
    primaryId: primary.name ? shortName(str(primary.name)) : "",
    rotationPeriod: period ? describeRotation(period) : "",
    nextRotation: str(raw.nextRotationTime),
    createTime: str(raw.createTime),
    labels: strMap(raw.labels),
  };
}

export function describeRotation(period: string): string {
  const secs = Number(durationToSeconds(period));
  if (!Number.isFinite(secs) || secs <= 0) return period;
  return secs % DAY_SECONDS === 0 ? `${secs / DAY_SECONDS} days` : `${secs}s`;
}

export function parseVersion(raw: Json): VersionInfo {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    state: str(raw.state),
    algorithm: str(raw.algorithm),
    protectionLevel: str(raw.protectionLevel),
    createTime: str(raw.createTime),
    destroyTime: str(raw.destroyScheduledTime) || str(raw.destroyEventTime),
  };
}

async function listAll<T>(
  path: string,
  key: string,
  parse: (raw: Json) => T,
  fetchFn: FetchFn,
): Promise<T[]> {
  const out: T[] = [];
  let token = "";
  do {
    const url = `${path}?pageSize=${PAGE}${token ? `&pageToken=${encodeURIComponent(token)}` : ""}`;
    const body = await restCall<Json>(KMS_PROXY_BASE, "GET", url, undefined, fetchFn);
    out.push(...((body[key] as Json[] | undefined) ?? []).map((r) => parse(r)));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export const listKeyRings = (project: string, location: string, fetchFn: FetchFn = fetch) =>
  listAll(`${locationPath(project, location)}/keyRings`, "keyRings", parseKeyRing, fetchFn);

export async function createKeyRing(
  project: string,
  location: string,
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<KeyRingInfo> {
  return parseKeyRing(
    await restCall(
      KMS_PROXY_BASE,
      "POST",
      `${locationPath(project, location)}/keyRings?keyRingId=${enc(id)}`,
      {},
      fetchFn,
    ),
  );
}

export const listKeys = (
  project: string,
  location: string,
  ring: string,
  fetchFn: FetchFn = fetch,
) => listAll(`${ringPath(project, location, ring)}/cryptoKeys`, "cryptoKeys", parseKey, fetchFn);

export async function getKey(
  project: string,
  location: string,
  ring: string,
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<KeyInfo> {
  return parseKey(
    await restCall(KMS_PROXY_BASE, "GET", keyPath(project, location, ring, id), undefined, fetchFn),
  );
}

export type CreateKeyInput = {
  purpose: Purpose;
  algorithm: string;
  protectionLevel: string;
  rotationDays: string;
  labels: Labels;
};

/** Days to a rotation schedule; null when blank, an error string when invalid. */
export function rotationFields(
  days: string,
  now: Date = new Date(),
): { rotationPeriod: string; nextRotationTime: string } | null | { error: string } {
  const t = days.trim();
  if (!t) return null;
  if (!/^\d+$/.test(t) || Number(t) < 1)
    return { error: "Rotation must be a whole number of days (min 1)" };
  const secs = Number(t) * DAY_SECONDS;
  return {
    rotationPeriod: `${secs}s`,
    nextRotationTime: new Date(now.getTime() + secs * 1000).toISOString(),
  };
}

export function keyBody(i: CreateKeyInput, now: Date = new Date()): Json {
  const body: Json = {
    purpose: i.purpose,
    versionTemplate: { algorithm: i.algorithm, protectionLevel: i.protectionLevel },
  };
  if (Object.keys(i.labels).length > 0) body.labels = i.labels;
  const rot = rotationFields(i.rotationDays, now);
  if (rot && !("error" in rot)) Object.assign(body, rot);
  return body;
}

export async function createKey(
  project: string,
  location: string,
  ring: string,
  id: string,
  input: CreateKeyInput,
  fetchFn: FetchFn = fetch,
): Promise<KeyInfo> {
  return parseKey(
    await restCall(
      KMS_PROXY_BASE,
      "POST",
      `${ringPath(project, location, ring)}/cryptoKeys?cryptoKeyId=${enc(id)}`,
      keyBody(input),
      fetchFn,
    ),
  );
}

export const listVersions = (
  project: string,
  location: string,
  ring: string,
  key: string,
  fetchFn: FetchFn = fetch,
) =>
  listAll(
    `${keyPath(project, location, ring, key)}/cryptoKeyVersions`,
    "cryptoKeyVersions",
    parseVersion,
    fetchFn,
  );

export async function createVersion(
  project: string,
  location: string,
  ring: string,
  key: string,
  fetchFn: FetchFn = fetch,
): Promise<VersionInfo> {
  return parseVersion(
    await restCall(
      KMS_PROXY_BASE,
      "POST",
      `${keyPath(project, location, ring, key)}/cryptoKeyVersions`,
      {},
      fetchFn,
    ),
  );
}

export async function versionAction(
  project: string,
  location: string,
  ring: string,
  key: string,
  version: string,
  action: VersionAction,
  fetchFn: FetchFn = fetch,
): Promise<VersionInfo> {
  const path = versionPath(project, location, ring, key, version);
  if (action === "enable" || action === "disable") {
    return parseVersion(
      await restCall(
        KMS_PROXY_BASE,
        "PATCH",
        `${path}?updateMask=state`,
        { state: action === "enable" ? "ENABLED" : "DISABLED" },
        fetchFn,
      ),
    );
  }
  return parseVersion(await restCall(KMS_PROXY_BASE, "POST", `${path}:${action}`, {}, fetchFn));
}

export async function setPrimary(
  project: string,
  location: string,
  ring: string,
  key: string,
  version: string,
  fetchFn: FetchFn = fetch,
): Promise<KeyInfo> {
  return parseKey(
    await restCall(
      KMS_PROXY_BASE,
      "POST",
      `${keyPath(project, location, ring, key)}:updatePrimaryVersion`,
      { cryptoKeyVersionId: version },
      fetchFn,
    ),
  );
}

export type KeyRef = { project: string; location: string; ring: string; key: string };

export async function encryptText(
  ref: KeyRef,
  plaintext: string,
  aad: string,
  fetchFn: FetchFn = fetch,
): Promise<{ ciphertext: string; version: string }> {
  const body: Json = { plaintext: utf8ToBase64(plaintext) };
  if (aad) body.additionalAuthenticatedData = utf8ToBase64(aad);
  const res = await restCall<Json>(
    KMS_PROXY_BASE,
    "POST",
    `${keyPath(ref.project, ref.location, ref.ring, ref.key)}:encrypt`,
    body,
    fetchFn,
  );
  return { ciphertext: str(res.ciphertext), version: shortName(str(res.name)) };
}

export async function decryptText(
  ref: KeyRef,
  ciphertextB64: string,
  aad: string,
  fetchFn: FetchFn = fetch,
): Promise<{ text: string | null; base64: string }> {
  const body: Json = { ciphertext: ciphertextB64.trim() };
  if (aad) body.additionalAuthenticatedData = utf8ToBase64(aad);
  const res = await restCall<Json>(
    KMS_PROXY_BASE,
    "POST",
    `${keyPath(ref.project, ref.location, ref.ring, ref.key)}:decrypt`,
    body,
    fetchFn,
  );
  const b64 = str(res.plaintext);
  return { text: base64ToUtf8(b64), base64: b64 };
}

export async function signText(
  ref: KeyRef,
  version: string,
  text: string,
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const res = await restCall<Json>(
    KMS_PROXY_BASE,
    "POST",
    `${versionPath(ref.project, ref.location, ref.ring, ref.key, version)}:asymmetricSign`,
    { data: utf8ToBase64(text) },
    fetchFn,
  );
  return str(res.signature);
}

export async function macSignText(
  ref: KeyRef,
  version: string,
  text: string,
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const res = await restCall<Json>(
    KMS_PROXY_BASE,
    "POST",
    `${versionPath(ref.project, ref.location, ref.ring, ref.key, version)}:macSign`,
    { data: utf8ToBase64(text) },
    fetchFn,
  );
  return str(res.mac);
}

export async function getPublicKey(
  ref: KeyRef,
  version: string,
  fetchFn: FetchFn = fetch,
): Promise<{ pem: string; algorithm: string }> {
  const res = await restCall<Json>(
    KMS_PROXY_BASE,
    "GET",
    `${versionPath(ref.project, ref.location, ref.ring, ref.key, version)}/publicKey`,
    undefined,
    fetchFn,
  );
  return { pem: str(res.pem), algorithm: str(res.algorithm) };
}

function readStored(key: string, fallback: string, storage?: Pick<Storage, "getItem">): string {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    return s?.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

function writeStored(key: string, value: string, storage?: Pick<Storage, "setItem">): void {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    s?.setItem(key, value);
  } catch {
    // storage unavailable: the choice applies for this session only
  }
}

export const readStoredKmsProject = (storage?: Pick<Storage, "getItem">) =>
  readStored(KMS_PROJECT_KEY, DEFAULT_KMS_PROJECT, storage);
export const readStoredKmsLocation = (storage?: Pick<Storage, "getItem">) =>
  readStored(KMS_LOCATION_KEY, DEFAULT_KMS_LOCATION, storage);
export const writeStoredKmsScope = (
  project: string,
  location: string,
  storage?: Pick<Storage, "setItem">,
) => {
  writeStored(KMS_PROJECT_KEY, project, storage);
  writeStored(KMS_LOCATION_KEY, location, storage);
};
