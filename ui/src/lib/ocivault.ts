/** Client for the Oracle Cloud Vault emulator (keys and secrets) via the dashboard's in-process proxy. */

import { restCall, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const VAULT_PROXY_BASE = "/dashboard/api/v1/oci/vault";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type VaultInfo = {
  id: string;
  displayName: string;
  state: string;
  vaultType: string;
  compartmentId: string;
  timeCreated: string;
  cryptoEndpoint: string;
  managementEndpoint: string;
  raw: Json;
};

export type KeyInfo = {
  id: string;
  displayName: string;
  state: string;
  algorithm: string;
  length: number;
  protectionMode: string;
  compartmentId: string;
  timeCreated: string;
  raw: Json;
};

export type KeyVersionInfo = {
  id: string;
  state: string;
  origin: string;
  timeCreated: string;
};

export type SecretInfo = {
  id: string;
  name: string;
  state: string;
  vaultId: string;
  keyId: string;
  description: string;
  timeCreated: string;
  freeformTags: Record<string, string>;
  raw: Json;
};

export type SecretVersionInfo = {
  versionNumber: number;
  stages: string[];
  name: string;
  contentType: string;
  timeCreated: string;
  timeOfExpiry: string;
  timeOfDeletion: string;
};

export type RevealedSecret = { text: string | null; base64: string; versionNumber: number };

export function parseVault(raw: Json): VaultInfo {
  return {
    id: str(raw.id),
    displayName: str(raw.displayName),
    state: str(raw.lifecycleState),
    vaultType: str(raw.vaultType),
    compartmentId: str(raw.compartmentId),
    timeCreated: str(raw.timeCreated),
    cryptoEndpoint: str(raw.cryptoEndpoint),
    managementEndpoint: str(raw.managementEndpoint),
    raw,
  };
}

export function parseKey(raw: Json): KeyInfo {
  const shape = (raw.keyShape ?? {}) as Json;
  return {
    id: str(raw.id),
    displayName: str(raw.displayName),
    state: str(raw.lifecycleState),
    algorithm: str(shape.algorithm) || str(raw.algorithm),
    length: num(shape.length ?? raw.length),
    protectionMode: str(raw.protectionMode),
    compartmentId: str(raw.compartmentId),
    timeCreated: str(raw.timeCreated),
    raw,
  };
}

export function parseKeyVersion(raw: Json): KeyVersionInfo {
  return {
    id: str(raw.id),
    state: str(raw.lifecycleState),
    origin: str(raw.origin),
    timeCreated: str(raw.timeCreated),
  };
}

export function parseSecret(raw: Json): SecretInfo {
  return {
    id: str(raw.id),
    name: str(raw.secretName),
    state: str(raw.lifecycleState),
    vaultId: str(raw.vaultId),
    keyId: str(raw.keyId),
    description: str(raw.description),
    timeCreated: str(raw.timeCreated),
    freeformTags: strMap(raw.freeformTags),
    raw,
  };
}

export function parseSecretVersion(raw: Json): SecretVersionInfo {
  return {
    versionNumber: num(raw.versionNumber),
    stages: (Array.isArray(raw.stages) ? raw.stages : []).map(String),
    name: str(raw.name),
    contentType: str(raw.contentType),
    timeCreated: str(raw.timeCreated),
    timeOfExpiry: str(raw.timeOfExpiry),
    timeOfDeletion: str(raw.timeOfDeletion),
  };
}

export async function listVaults(fetchFn: FetchFn = fetch): Promise<VaultInfo[]> {
  const body = await restCall(VAULT_PROXY_BASE, "GET", "/vaults", undefined, fetchFn);
  return list(body.vaults).map((v) => parseVault(v));
}

export async function listKeys(vaultId: string, fetchFn: FetchFn = fetch): Promise<KeyInfo[]> {
  const body = await restCall(
    VAULT_PROXY_BASE,
    "GET",
    `/vaults/${enc(vaultId)}/keys`,
    undefined,
    fetchFn,
  );
  return list(body.keys).map((k) => parseKey(k));
}

export async function listKeyVersions(
  vaultId: string,
  keyId: string,
  fetchFn: FetchFn = fetch,
): Promise<KeyVersionInfo[]> {
  const body = await restCall(
    VAULT_PROXY_BASE,
    "GET",
    `/vaults/${enc(vaultId)}/keys/${enc(keyId)}/versions`,
    undefined,
    fetchFn,
  );
  return list(body.versions).map((v) => parseKeyVersion(v));
}

export async function listSecrets(vaultId = "", fetchFn: FetchFn = fetch): Promise<SecretInfo[]> {
  const q = vaultId ? `?vaultId=${enc(vaultId)}` : "";
  const body = await restCall(VAULT_PROXY_BASE, "GET", `/secrets${q}`, undefined, fetchFn);
  return list(body.secrets).map((s) => parseSecret(s));
}

export async function listSecretVersions(
  secretId: string,
  fetchFn: FetchFn = fetch,
): Promise<SecretVersionInfo[]> {
  const body = await restCall(
    VAULT_PROXY_BASE,
    "GET",
    `/secrets/${enc(secretId)}/versions`,
    undefined,
    fetchFn,
  );
  return list(body.versions).map((v) => parseSecretVersion(v));
}

/** Fetches secret content. Only call this from an explicit user "reveal" action. */
export async function revealSecretVersion(
  secretId: string,
  versionNumber: number,
  fetchFn: FetchFn = fetch,
): Promise<RevealedSecret> {
  const body = await restCall(
    VAULT_PROXY_BASE,
    "POST",
    `/secrets/${enc(secretId)}/versions/${versionNumber}/reveal`,
    undefined,
    fetchFn,
  );
  return {
    text: body.isText === true ? str(body.text) : null,
    base64: str(body.base64),
    versionNumber: num(body.versionNumber),
  };
}

/** Compact OCID for display: keeps the kind and the last characters. */
export function shortOcid(ocid: string): string {
  const parts = ocid.split(".");
  if (parts.length < 4) return ocid;
  const tail = parts.at(-1) ?? "";
  return `${parts[1]}…${tail.slice(-8)}`;
}
