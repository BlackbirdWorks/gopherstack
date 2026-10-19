/** Client for the Cloud IAM emulator via the dashboard's in-process REST proxy. */

import {
  base64ToUtf8,
  restCall,
  shortName,
  str,
  utf8ToBase64,
  type FetchFn,
  type Json,
} from "./gcprest.ts";

export const IAM_PROXY_BASE = "/dashboard/api/v1/gcp/iam/v1";
export const IAM_PROJECT_KEY = "gopherstack_iam_project";
export const DEFAULT_IAM_PROJECT = "gopherstack-local";
const PAGE = 300;

export const ROLE_STAGES = ["GA", "BETA", "ALPHA", "DEPRECATED", "DISABLED", "EAP"] as const;
export type RoleStage = (typeof ROLE_STAGES)[number];

export type AccountInfo = {
  name: string;
  email: string;
  uniqueId: string;
  displayName: string;
  description: string;
  disabled: boolean;
};

export type KeyInfo = {
  name: string;
  id: string;
  keyType: "USER_MANAGED" | "SYSTEM_MANAGED";
  algorithm: string;
  validAfter: string;
  validBefore: string;
  disabled: boolean;
};

export type RoleInfo = {
  name: string;
  id: string;
  title: string;
  description: string;
  stage: string;
  deleted: boolean;
  permissions: string[];
  predefined: boolean;
};

/** A freshly created key: the credentials file text exists only in this response. */
export type CreatedKey = { key: KeyInfo; file: string; filename: string };
export type NewAccount = { id: string; displayName: string; description: string };
export type NewRole = {
  id: string;
  title: string;
  description: string;
  stage: RoleStage;
  permissions: string[];
};
export type RoleEdit = Omit<NewRole, "id">;
export type SignResult = { keyId: string; signature: string };

const enc = encodeURIComponent;
const accountsPath = (p: string) => `/projects/${enc(p)}/serviceAccounts`;
const accountPath = (p: string, email: string) => `${accountsPath(p)}/${enc(email)}`;
const keyPath = (p: string, email: string, id: string) =>
  `${accountPath(p, email)}/keys/${enc(id)}`;
const rolesPath = (p: string) => `/projects/${enc(p)}/roles`;
const call = (method: string, path: string, body: unknown, fetchFn: FetchFn) =>
  restCall<Json>(IAM_PROXY_BASE, method, path, body, fetchFn);

export function validateAccountId(id: string): string | null {
  if (!id) return "account ID is required";
  if (!/^[a-z][-a-z0-9]{4,28}[a-z0-9]$/.test(id)) {
    return "6-30 chars: lowercase letters, digits and -, starting with a letter";
  }
  return null;
}

export function validateRoleId(id: string): string | null {
  if (!id) return "role ID is required";
  if (!/^[A-Za-z0-9_.]{3,64}$/.test(id)) return "3-64 chars: letters, digits, _ and .";
  return null;
}

const PERMISSION = /^[a-z][a-zA-Z0-9]*(\.[a-zA-Z0-9]+){2,}$/;

/** Splits free text (newline or comma separated) into sorted, unique permission names. */
export function parsePermissions(text: string): string[] {
  const parts = text
    .split(/[\s,]+/)
    .map((p) => p.trim())
    .filter(Boolean);
  return [...new Set(parts)].toSorted();
}

export function validatePermissions(perms: string[]): string | null {
  const bad = perms.find((p) => !PERMISSION.test(p));
  return bad ? `"${bad}" is not a valid permission (service.resource.verb)` : null;
}

export function parseAccount(raw: Json): AccountInfo {
  return {
    name: str(raw.name),
    email: str(raw.email),
    uniqueId: str(raw.uniqueId),
    displayName: str(raw.displayName),
    description: str(raw.description),
    disabled: raw.disabled === true,
  };
}

export function parseKey(raw: Json): KeyInfo {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    keyType: raw.keyType === "SYSTEM_MANAGED" ? "SYSTEM_MANAGED" : "USER_MANAGED",
    algorithm: str(raw.keyAlgorithm).replace("KEY_ALG_", ""),
    validAfter: str(raw.validAfterTime),
    validBefore: str(raw.validBeforeTime),
    disabled: raw.disabled === true,
  };
}

export function parseRole(raw: Json): RoleInfo {
  const name = str(raw.name);
  const perms = Array.isArray(raw.includedPermissions)
    ? raw.includedPermissions.filter((p): p is string => typeof p === "string")
    : [];
  return {
    name,
    id: name.startsWith("roles/") ? name.slice("roles/".length) : shortName(name),
    title: str(raw.title),
    description: str(raw.description),
    stage: str(raw.stage) || "GA",
    deleted: raw.deleted === true,
    permissions: perms,
    predefined: name.startsWith("roles/"),
  };
}

/** True once the key's validity window has ended. */
export function keyExpired(k: Pick<KeyInfo, "validBefore">, now: number = Date.now()): boolean {
  const t = Date.parse(k.validBefore);
  return !Number.isNaN(t) && t <= now;
}

/** "active", "disabled" or "expired". */
export function keyStatus(k: KeyInfo, now: number = Date.now()): "active" | "disabled" | "expired" {
  if (k.disabled) return "disabled";
  return keyExpired(k, now) ? "expired" : "active";
}

/** Case-insensitive match on role ID, title or any permission. */
export function matchesRole(r: RoleInfo, query: string): boolean {
  const q = query.trim().toLowerCase();
  if (!q) return true;
  return (
    r.id.toLowerCase().includes(q) ||
    r.title.toLowerCase().includes(q) ||
    r.permissions.some((p) => p.toLowerCase().includes(q))
  );
}

export const keyFileName = (project: string, k: KeyInfo): string =>
  `${project}-${k.id.slice(0, 12)}.json`;

async function listAll<T>(
  path: string,
  field: string,
  parse: (raw: Json) => T,
  fetchFn: FetchFn,
  extra = "",
): Promise<T[]> {
  const out: T[] = [];
  let token = "";
  do {
    const url = `${path}?pageSize=${PAGE}${extra}${token ? `&pageToken=${enc(token)}` : ""}`;
    const body = await call("GET", url, undefined, fetchFn);
    out.push(...((body[field] as Json[] | undefined) ?? []).map((r) => parse(r)));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export const listAccounts = (project: string, fetchFn: FetchFn = fetch) =>
  listAll(accountsPath(project), "accounts", parseAccount, fetchFn);

export async function createAccount(
  project: string,
  a: NewAccount,
  fetchFn: FetchFn = fetch,
): Promise<AccountInfo> {
  const serviceAccount: Json = { displayName: a.displayName.trim() };
  if (a.description.trim()) serviceAccount.description = a.description.trim();
  const body = { accountId: a.id.trim(), serviceAccount };
  return parseAccount(await call("POST", accountsPath(project), body, fetchFn));
}

export async function setAccountDisabled(
  project: string,
  email: string,
  disabled: boolean,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const verb = disabled ? "disable" : "enable";
  await call("POST", `${accountPath(project, email)}:${verb}`, {}, fetchFn);
}

export async function deleteAccount(
  project: string,
  email: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await call("DELETE", accountPath(project, email), undefined, fetchFn);
}

/** Restores a deleted account by unique ID (the only identifier AWS-style undelete accepts). */
export async function undeleteAccount(
  uniqueId: string,
  fetchFn: FetchFn = fetch,
): Promise<AccountInfo> {
  const path = `/projects/-/serviceAccounts/${enc(uniqueId)}:undelete`;
  const body = await call("POST", path, {}, fetchFn);
  return parseAccount((body.restoredAccount as Json | undefined) ?? {});
}

export const listKeys = (project: string, email: string, fetchFn: FetchFn = fetch) =>
  call("GET", `${accountPath(project, email)}/keys`, undefined, fetchFn).then((b) =>
    ((b.keys as Json[] | undefined) ?? []).map((r) => parseKey(r)),
  );

export async function createKey(
  project: string,
  email: string,
  fetchFn: FetchFn = fetch,
): Promise<CreatedKey> {
  const raw = await call("POST", `${accountPath(project, email)}/keys`, {}, fetchFn);
  const key = parseKey(raw);
  const file = base64ToUtf8(str(raw.privateKeyData)) ?? "";
  return { key, file, filename: keyFileName(project, key) };
}

export async function keyAction(
  project: string,
  email: string,
  id: string,
  action: "enable" | "disable",
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await call("POST", `${keyPath(project, email, id)}:${action}`, {}, fetchFn);
}

export async function deleteKey(
  project: string,
  email: string,
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await call("DELETE", keyPath(project, email, id), undefined, fetchFn);
}

export const listPredefinedRoles = (fetchFn: FetchFn = fetch) =>
  listAll("/roles", "roles", parseRole, fetchFn);

export const listCustomRoles = (project: string, fetchFn: FetchFn = fetch) =>
  listAll(rolesPath(project), "roles", parseRole, fetchFn, "&showDeleted=true");

export async function createRole(
  project: string,
  r: NewRole,
  fetchFn: FetchFn = fetch,
): Promise<RoleInfo> {
  const role = {
    title: r.title.trim(),
    description: r.description.trim(),
    stage: r.stage,
    includedPermissions: r.permissions,
  };
  return parseRole(await call("POST", rolesPath(project), { roleId: r.id.trim(), role }, fetchFn));
}

export async function updateRole(
  project: string,
  id: string,
  r: RoleEdit,
  fetchFn: FetchFn = fetch,
): Promise<RoleInfo> {
  const path = `${rolesPath(project)}/${enc(id)}?updateMask=title,description,stage,includedPermissions`;
  const role = {
    title: r.title.trim(),
    description: r.description.trim(),
    stage: r.stage,
    includedPermissions: r.permissions,
  };
  return parseRole(await call("PATCH", path, role, fetchFn));
}

export async function deleteRole(
  project: string,
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<RoleInfo> {
  return parseRole(await call("DELETE", `${rolesPath(project)}/${enc(id)}`, undefined, fetchFn));
}

export async function undeleteRole(
  project: string,
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<RoleInfo> {
  return parseRole(await call("POST", `${rolesPath(project)}/${enc(id)}:undelete`, {}, fetchFn));
}

export async function signBlob(
  project: string,
  email: string,
  text: string,
  fetchFn: FetchFn = fetch,
): Promise<SignResult> {
  const body = { bytesToSign: utf8ToBase64(text) };
  const raw = await call("POST", `${accountPath(project, email)}:signBlob`, body, fetchFn);
  return { keyId: str(raw.keyId), signature: str(raw.signature) };
}

export function readStoredIamProject(): string {
  try {
    return localStorage.getItem(IAM_PROJECT_KEY)?.trim() || DEFAULT_IAM_PROJECT;
  } catch {
    return DEFAULT_IAM_PROJECT;
  }
}

export function writeStoredIamProject(project: string): void {
  try {
    localStorage.setItem(IAM_PROJECT_KEY, project);
  } catch {
    // storage unavailable: the project applies for this session only
  }
}
