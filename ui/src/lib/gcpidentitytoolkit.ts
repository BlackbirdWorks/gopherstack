/** Client for the Identity Platform emulator (users, tenants, codes, sign-in config) via the dashboard's in-process proxy. */

import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const IDP_PROXY_BASE = "/dashboard/api/v1/gcp/identitytoolkit";
export const IDP_PROJECT_KEY = "gopherstack_idp_project";
export const IDP_USER_LIMIT = 500;
export const IDP_MAX_CLAIMS_BYTES = 1000;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type IdpProvider = { providerId: string; rawId: string; email: string };

export type IdpUser = {
  uid: string;
  email: string;
  displayName: string;
  phoneNumber: string;
  emailVerified: boolean;
  disabled: boolean;
  hasPassword: boolean;
  providers: IdpProvider[];
  claims: Record<string, unknown>;
  createdAt: number;
  lastLoginAt: number;
  lastRefreshAt: number;
  validSince: number;
};

export type IdpTenant = {
  id: string;
  displayName: string;
  allowPasswordSignup: boolean;
  enableEmailLinkSignin: boolean;
  enableAnonymousUser: boolean;
};

export type IdpOobCode = {
  email: string;
  oobCode: string;
  oobLink: string;
  requestType: string;
};

export type IdpConfig = {
  emailEnabled: boolean;
  anonymousEnabled: boolean;
  authorizedDomains: string[];
};

export type UserList = { users: IdpUser[]; total: number };

export type UserAction = "disable" | "enable" | "revoke";

/** Milliseconds since the epoch from a string or number; 0 when absent. */
const millis = (v: unknown): number => {
  const n = typeof v === "number" ? v : Number(str(v));
  if (Number.isFinite(n) && n > 0) return n;
  const t = typeof v === "string" ? Date.parse(v) : NaN;
  return Number.isFinite(t) ? t : 0;
};

export function parseClaims(raw: unknown): Record<string, unknown> {
  if (typeof raw !== "string" || raw === "") return {};
  try {
    const v: unknown = JSON.parse(raw);
    return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

export function parseUser(raw: Json): IdpUser {
  return {
    uid: str(raw.localId),
    email: str(raw.email),
    displayName: str(raw.displayName),
    phoneNumber: str(raw.phoneNumber),
    emailVerified: raw.emailVerified === true,
    disabled: raw.disabled === true,
    hasPassword: raw.hasPassword === true,
    providers: list(raw.providers).map((p) => ({
      providerId: str(p.providerId),
      rawId: str(p.rawId),
      email: str(p.email),
    })),
    claims: parseClaims(raw.customAttributes),
    createdAt: millis(raw.createdAt),
    lastLoginAt: millis(raw.lastLoginAt),
    lastRefreshAt: millis(raw.lastRefreshAt),
    validSince: millis(raw.validSince),
  };
}

export function parseTenant(raw: Json): IdpTenant {
  return {
    id: str(raw.id),
    displayName: str(raw.displayName),
    allowPasswordSignup: raw.allowPasswordSignup === true,
    enableEmailLinkSignin: raw.enableEmailLinkSignin === true,
    enableAnonymousUser: raw.enableAnonymousUser === true,
  };
}

export const parseOobCode = (raw: Json): IdpOobCode => ({
  email: str(raw.email),
  oobCode: str(raw.oobCode),
  oobLink: str(raw.oobLink),
  requestType: str(raw.requestType),
});

export function parseConfig(raw: Json): IdpConfig {
  return {
    emailEnabled: raw.emailEnabled === true,
    anonymousEnabled: raw.anonymousEnabled === true,
    authorizedDomains: Array.isArray(raw.authorizedDomains)
      ? raw.authorizedDomains.filter((d): d is string => typeof d === "string")
      : [],
  };
}

export function readStoredProject(): string {
  try {
    return localStorage.getItem(IDP_PROJECT_KEY) ?? "";
  } catch {
    return "";
  }
}

export function writeStoredProject(project: string): void {
  try {
    localStorage.setItem(IDP_PROJECT_KEY, project);
  } catch {
    // storage unavailable: the choice just is not remembered
  }
}

const projectPath = (project: string) => `/projects/${enc(project)}`;
const tenantQuery = (tenant: string, extra: Record<string, string> = {}): string => {
  const q = new URLSearchParams(extra);
  if (tenant) q.set("tenantId", tenant);
  const s = q.toString();
  return s ? `?${s}` : "";
};

export async function listProjects(fetchFn: FetchFn = fetch): Promise<string[]> {
  const body = await restCall(IDP_PROXY_BASE, "GET", "/projects", undefined, fetchFn);
  return Array.isArray(body.projects)
    ? body.projects.filter((p): p is string => typeof p === "string")
    : [];
}

export async function listUsers(
  project: string,
  tenant: string,
  fetchFn: FetchFn = fetch,
): Promise<UserList> {
  const body = await restCall(
    IDP_PROXY_BASE,
    "GET",
    `${projectPath(project)}/users${tenantQuery(tenant, { limit: String(IDP_USER_LIMIT) })}`,
    undefined,
    fetchFn,
  );
  const users = list(body.users).map((u) => parseUser(u));
  return { users, total: typeof body.total === "number" ? body.total : users.length };
}

/** Case-insensitive match on email, display name or uid; an empty query keeps everything. */
export function filterUsers(users: IdpUser[], query: string): IdpUser[] {
  const q = query.trim().toLowerCase();
  if (!q) return users;
  return users.filter(
    (u) =>
      u.email.toLowerCase().includes(q) ||
      u.uid.toLowerCase().includes(q) ||
      u.displayName.toLowerCase().includes(q),
  );
}

/** The password is sent once to the backend and never returned or kept. */
export async function createUser(
  project: string,
  tenant: string,
  input: { email: string; password: string; displayName?: string },
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const body = await restCall(
    IDP_PROXY_BASE,
    "POST",
    `${projectPath(project)}/users${tenantQuery(tenant)}`,
    { email: input.email, password: input.password, displayName: input.displayName ?? "" },
    fetchFn,
  );
  return str(body.localId);
}

export async function userAction(
  project: string,
  tenant: string,
  uid: string,
  action: UserAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    IDP_PROXY_BASE,
    "POST",
    `${projectPath(project)}/users/${enc(uid)}/${action}${tenantQuery(tenant)}`,
    undefined,
    fetchFn,
  );
}

export async function setClaims(
  project: string,
  tenant: string,
  uid: string,
  claims: Record<string, unknown>,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    IDP_PROXY_BASE,
    "POST",
    `${projectPath(project)}/users/${enc(uid)}/claims${tenantQuery(tenant)}`,
    { claims },
    fetchFn,
  );
}

export async function deleteUser(
  project: string,
  tenant: string,
  uid: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    IDP_PROXY_BASE,
    "DELETE",
    `${projectPath(project)}/users/${enc(uid)}${tenantQuery(tenant)}`,
    undefined,
    fetchFn,
  );
}

export async function listOobCodes(
  project: string,
  tenant: string,
  fetchFn: FetchFn = fetch,
): Promise<IdpOobCode[]> {
  const body = await restCall(
    IDP_PROXY_BASE,
    "GET",
    `${projectPath(project)}/oobcodes${tenantQuery(tenant)}`,
    undefined,
    fetchFn,
  );
  return list(body.oobCodes).map((c) => parseOobCode(c));
}

export async function listTenants(project: string, fetchFn: FetchFn = fetch): Promise<IdpTenant[]> {
  const body = await restCall(
    IDP_PROXY_BASE,
    "GET",
    `${projectPath(project)}/tenants`,
    undefined,
    fetchFn,
  );
  return list(body.tenants).map((t) => parseTenant(t));
}

export async function getConfig(project: string, fetchFn: FetchFn = fetch): Promise<IdpConfig> {
  return parseConfig(
    await restCall(IDP_PROXY_BASE, "GET", `${projectPath(project)}/config`, undefined, fetchFn),
  );
}

export async function setSignIn(
  project: string,
  toggles: { emailEnabled?: boolean; anonymousEnabled?: boolean },
  fetchFn: FetchFn = fetch,
): Promise<IdpConfig> {
  return parseConfig(
    await restCall(IDP_PROXY_BASE, "PATCH", `${projectPath(project)}/config`, toggles, fetchFn),
  );
}

export type ClaimsCheck =
  | { ok: true; claims: Record<string, unknown> }
  | { ok: false; error: string };

/** Validates the claims editor text: a JSON object under the backend's size limit. */
export function validateClaims(text: string): ClaimsCheck {
  const t = text.trim() || "{}";
  let v: unknown;
  try {
    v = JSON.parse(t);
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Invalid JSON" };
  }
  if (!v || typeof v !== "object" || Array.isArray(v)) {
    return { ok: false, error: "Claims must be a JSON object" };
  }
  if (new TextEncoder().encode(JSON.stringify(v)).length > IDP_MAX_CLAIMS_BYTES) {
    return { ok: false, error: `Claims must be under ${IDP_MAX_CLAIMS_BYTES} bytes` };
  }
  return { ok: true, claims: v as Record<string, unknown> };
}

export const validEmail = (v: string): boolean => /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v.trim());
export const validPassword = (v: string): boolean => v.length >= 6;

export const providerLabel = (id: string): string =>
  ({ password: "Email/Password", anonymous: "Anonymous", phone: "Phone", custom: "Custom token" })[
    id
  ] ?? id;

export function userProviders(u: IdpUser): string[] {
  const ids = u.providers.map((p) => p.providerId);
  if (u.hasPassword && !ids.includes("password")) ids.unshift("password");
  if (ids.length === 0 && !u.email) ids.push("anonymous");
  return ids;
}

export function formatTime(ms: number): string {
  return ms > 0 ? new Date(ms).toISOString().replace("T", " ").slice(0, 19) + "Z" : "—";
}
