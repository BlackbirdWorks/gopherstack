/** Client for the Secret Manager emulator via the dashboard's in-process REST proxy. */

import {
  base64ToUtf8,
  restCall,
  shortName,
  str,
  strMap,
  utf8ToBase64,
  type FetchFn,
  type Json,
} from "./gcprest.ts";

export const SM_PROXY_BASE = "/dashboard/api/v1/gcp/secretmanager/v1";
export const SM_PROJECT_KEY = "gopherstack_secretmanager_project";
export const DEFAULT_SM_PROJECT = "gopherstack-local";
const PAGE = 100;

export type Labels = Record<string, string>;

export type SecretInfo = {
  name: string;
  id: string;
  createTime: string;
  labels: Labels;
  replication: string;
  expiry: string;
  aliases: Record<string, string>;
  etag: string;
};

export type VersionInfo = {
  name: string;
  id: string;
  state: string;
  createTime: string;
  destroyTime: string;
};

export type IamBinding = { role: string; members: string[] };
export type IamPolicy = { etag: string; version: number; bindings: IamBinding[] };

export type Replication = { kind: "automatic" } | { kind: "user"; locations: string[] };

const projectPath = (p: string) => `/projects/${encodeURIComponent(p)}`;
const secretPath = (p: string, id: string) => `${projectPath(p)}/secrets/${encodeURIComponent(id)}`;
const versionPath = (p: string, id: string, v: string) =>
  `${secretPath(p, id)}/versions/${encodeURIComponent(v)}`;

export function validateSecretId(id: string): string | null {
  if (!id) return "name is required";
  if (!/^[A-Za-z0-9_-]{1,255}$/.test(id)) return "use letters, digits, - and _ (max 255)";
  return null;
}

export function validateAliasKey(key: string): string | null {
  if (!key) return "alias is required";
  if (!/^[A-Za-z][A-Za-z0-9_-]{0,62}$/.test(key))
    return "start with a letter; use letters, digits, - and _";
  return null;
}

export function validateLabelKey(key: string): string | null {
  if (!key) return "key is required";
  if (!/^[a-z][a-z0-9_-]{0,62}$/.test(key))
    return "keys start with a lowercase letter; use a-z, 0-9, _ and -";
  return null;
}

export function describeReplication(raw: Json | undefined): string {
  if (!raw) return "";
  if (raw.automatic) return "automatic";
  const um = raw.userManaged as { replicas?: { location?: string }[] } | undefined;
  if (um) return `user-managed: ${(um.replicas ?? []).map((r) => r.location ?? "").join(", ")}`;
  return "";
}

export function parseSecret(raw: Json): SecretInfo {
  const aliases = (raw.versionAliases ?? {}) as Record<string, unknown>;
  return {
    name: str(raw.name),
    id: shortName(str(raw.name)),
    createTime: str(raw.createTime),
    labels: strMap(raw.labels),
    replication: describeReplication(raw.replication as Json | undefined),
    expiry: str(raw.expireTime) || (raw.ttl ? `ttl ${str(raw.ttl)}` : ""),
    aliases: Object.fromEntries(Object.entries(aliases).map(([k, v]) => [k, String(v)])),
    etag: str(raw.etag),
  };
}

export function parseVersion(raw: Json): VersionInfo {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    state: str(raw.state),
    createTime: str(raw.createTime),
    destroyTime: str(raw.destroyTime),
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
    const body = await restCall<Json>(SM_PROXY_BASE, "GET", url, undefined, fetchFn);
    out.push(...((body[key] as Json[] | undefined) ?? []).map((r) => parse(r)));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export const listSecrets = (project: string, fetchFn: FetchFn = fetch) =>
  listAll(`${projectPath(project)}/secrets`, "secrets", parseSecret, fetchFn);

export type CreateSecretInput = {
  replication: Replication;
  labels: Labels;
  ttlSeconds: string;
  expireTime: string;
};

export function secretBody(i: CreateSecretInput): Json {
  const body: Json = {
    replication:
      i.replication.kind === "automatic"
        ? { automatic: {} }
        : { userManaged: { replicas: i.replication.locations.map((location) => ({ location })) } },
  };
  if (Object.keys(i.labels).length > 0) body.labels = i.labels;
  if (i.ttlSeconds.trim()) body.ttl = `${i.ttlSeconds.trim()}s`;
  else if (i.expireTime) body.expireTime = new Date(i.expireTime).toISOString();
  return body;
}

export async function createSecret(
  project: string,
  id: string,
  input: CreateSecretInput,
  fetchFn: FetchFn = fetch,
): Promise<SecretInfo> {
  return parseSecret(
    await restCall(
      SM_PROXY_BASE,
      "POST",
      `${projectPath(project)}/secrets?secretId=${encodeURIComponent(id)}`,
      secretBody(input),
      fetchFn,
    ),
  );
}

export async function getSecret(project: string, id: string, fetchFn: FetchFn = fetch) {
  return parseSecret(
    await restCall(SM_PROXY_BASE, "GET", secretPath(project, id), undefined, fetchFn),
  );
}

export async function deleteSecret(project: string, id: string, fetchFn: FetchFn = fetch) {
  await restCall(SM_PROXY_BASE, "DELETE", secretPath(project, id), undefined, fetchFn);
}

/** Replaces the alias map (alias -> version number) and optionally labels. */
export async function updateSecret(
  project: string,
  id: string,
  patch: { aliases?: Record<string, number>; labels?: Labels },
  fetchFn: FetchFn = fetch,
): Promise<SecretInfo> {
  const masks: string[] = [];
  const body: Json = {};
  if (patch.aliases) {
    masks.push("versionAliases");
    body.versionAliases = patch.aliases;
  }
  if (patch.labels) {
    masks.push("labels");
    body.labels = patch.labels;
  }
  return parseSecret(
    await restCall(
      SM_PROXY_BASE,
      "PATCH",
      `${secretPath(project, id)}?updateMask=${masks.join(",")}`,
      body,
      fetchFn,
    ),
  );
}

export const listVersions = (project: string, id: string, fetchFn: FetchFn = fetch) =>
  listAll(`${secretPath(project, id)}/versions`, "versions", parseVersion, fetchFn);

export async function addVersion(
  project: string,
  id: string,
  payload: { mode: "text" | "base64"; value: string },
  fetchFn: FetchFn = fetch,
): Promise<VersionInfo> {
  const data = payload.mode === "text" ? utf8ToBase64(payload.value) : payload.value.trim();
  return parseVersion(
    await restCall(
      SM_PROXY_BASE,
      "POST",
      `${secretPath(project, id)}:addVersion`,
      { payload: { data } },
      fetchFn,
    ),
  );
}

export type AccessedPayload = { text: string | null; base64: string };

export async function accessVersion(
  project: string,
  id: string,
  version: string,
  fetchFn: FetchFn = fetch,
): Promise<AccessedPayload> {
  const res = await restCall<{ payload?: { data?: string } }>(
    SM_PROXY_BASE,
    "GET",
    `${versionPath(project, id, version)}:access`,
    undefined,
    fetchFn,
  );
  const b64 = res.payload?.data ?? "";
  return { text: base64ToUtf8(b64), base64: b64 };
}

export type VersionAction = "enable" | "disable" | "destroy";

export async function versionAction(
  project: string,
  id: string,
  version: string,
  action: VersionAction,
  fetchFn: FetchFn = fetch,
): Promise<VersionInfo> {
  return parseVersion(
    await restCall(
      SM_PROXY_BASE,
      "POST",
      `${versionPath(project, id, version)}:${action}`,
      {},
      fetchFn,
    ),
  );
}

export async function getIamPolicy(
  project: string,
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<IamPolicy> {
  const raw = await restCall<{ etag?: string; version?: number; bindings?: IamBinding[] }>(
    SM_PROXY_BASE,
    "GET",
    `${secretPath(project, id)}:getIamPolicy`,
    undefined,
    fetchFn,
  );
  return { etag: raw.etag ?? "", version: raw.version ?? 0, bindings: raw.bindings ?? [] };
}

export function aliasesToRows(aliases: Record<string, string>): { key: string; value: string }[] {
  return Object.entries(aliases)
    .toSorted(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => ({ key, value }));
}

/** Alias rows to a map of alias -> version number; null names the first invalid value. */
export function rowsToAliases(
  rows: { key: string; value: string }[],
): { aliases: Record<string, number> } | { error: string } {
  const aliases: Record<string, number> = {};
  for (const r of rows) {
    if (!/^\d+$/.test(r.value.trim()) || Number(r.value) < 1) {
      return { error: `${r.key || "(empty)"}: value must be a version number` };
    }
    aliases[r.key] = Number(r.value);
  }
  return { aliases };
}

export function readStoredSmProject(storage?: Pick<Storage, "getItem">): string {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    return s?.getItem(SM_PROJECT_KEY)?.trim() || DEFAULT_SM_PROJECT;
  } catch {
    return DEFAULT_SM_PROJECT;
  }
}

export function writeStoredSmProject(project: string, storage?: Pick<Storage, "setItem">): void {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    s?.setItem(SM_PROJECT_KEY, project);
  } catch {
    // storage unavailable: the project applies for this session only
  }
}
