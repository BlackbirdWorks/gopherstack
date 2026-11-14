/** Client for the Entra ID & access page: Microsoft Graph and Microsoft.Authorization via in-process dashboard proxies. */

import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const GRAPH_PROXY_BASE = "/dashboard/api/v1/azure/graph";
export const AUTHZ_PROXY_BASE = "/dashboard/api/v1/azure/authz";

export const AUTHZ_VERSIONS = {
  roleDefinitions: "2022-04-01",
  roleAssignments: "2022-04-01",
  locks: "2020-05-01",
  policyDefinitions: "2021-06-01",
  policyAssignments: "2022-06-01",
} as const;

export const LOCK_LEVELS = ["CanNotDelete", "ReadOnly"] as const;
export type LockLevel = (typeof LOCK_LEVELS)[number];

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const strs = (v: unknown): string[] =>
  list(v)
    .map((x) => str(x))
    .filter(Boolean);
const GUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
export const isGuid = (v: string): boolean => GUID_RE.test(v.trim());

/** The last path segment of an ARM id. */
export const lastSegment = (id: string): string => id.slice(id.lastIndexOf("/") + 1);

export type Secret = {
  keyId: string;
  displayName: string;
  hint: string;
  start: string;
  end: string;
  expired: boolean;
};

export type Application = {
  id: string;
  appId: string;
  displayName: string;
  created: string;
  secrets: Secret[];
};

export type ResourceAccess = { resourceAppId: string; scopes: number; roles: number };
export type AppRole = { id: string; displayName: string; value: string; enabled: boolean };
export type DirObject = { id: string; type: string; name: string; detail: string };

export type ApplicationDetail = Application & {
  redirectUris: string[];
  resourceAccess: ResourceAccess[];
  appRoles: AppRole[];
  owners: DirObject[];
};

/** A new client secret as returned once by addPassword; keep it in memory only. */
export type NewSecret = { keyId: string; displayName: string; value: string; end: string };

export type ServicePrincipal = {
  id: string;
  appId: string;
  displayName: string;
  type: string;
  enabled: boolean;
};

export type AppRoleAssignment = {
  id: string;
  principal: string;
  principalType: string;
  resource: string;
  appRoleId: string;
};

export type DirUser = { id: string; upn: string; displayName: string };
export type DirGroup = { id: string; displayName: string; members: DirObject[] };

export type RoleDefinition = { id: string; name: string; roleName: string; roleType: string };
export type RoleAssignment = {
  name: string;
  id: string;
  scope: string;
  roleDefinitionId: string;
  principalId: string;
  principalType: string;
};
export type Lock = { name: string; id: string; level: string; notes: string };
export type PolicyDefinition = {
  name: string;
  displayName: string;
  policyType: string;
  mode: string;
};
export type PolicyAssignment = {
  name: string;
  displayName: string;
  scope: string;
  definition: string;
};

export function parseSecret(raw: Json, now: Date = new Date()): Secret {
  const end = str(raw.endDateTime);
  const endMs = Date.parse(end);
  return {
    keyId: str(raw.keyId),
    displayName: str(raw.displayName),
    hint: str(raw.hint),
    start: str(raw.startDateTime),
    end,
    expired: !Number.isNaN(endMs) && endMs < now.getTime(),
  };
}

export function parseApplication(raw: Json, now?: Date): Application {
  return {
    id: str(raw.id),
    appId: str(raw.appId),
    displayName: str(raw.displayName),
    created: str(raw.createdDateTime),
    secrets: list(raw.passwordCredentials).map((s) => parseSecret(s, now)),
  };
}

export function parseDirObject(raw: Json): DirObject {
  const odata = str(raw["@odata.type"]).replace("#microsoft.graph.", "");
  const type =
    (
      {
        user: "User",
        group: "Group",
        servicePrincipal: "ServicePrincipal",
        application: "Application",
      } as Record<string, string>
    )[odata] ?? odata;
  return {
    id: str(raw.id),
    type,
    name: str(raw.displayName),
    detail: str(raw.userPrincipalName) || str(raw.appId),
  };
}

export function parseApplicationDetail(raw: Json, owners: Json[], now?: Date): ApplicationDetail {
  const access = list(raw.requiredResourceAccess).map((r) => {
    const items = list(r.resourceAccess);
    return {
      resourceAppId: str(r.resourceAppId),
      scopes: items.filter((i) => str(i.type) === "Scope").length,
      roles: items.filter((i) => str(i.type) === "Role").length,
    };
  });
  return {
    ...parseApplication(raw, now),
    redirectUris: [
      ...strs(obj(raw.web).redirectUris),
      ...strs(obj(raw.spa).redirectUris),
      ...strs(obj(raw.publicClient).redirectUris),
    ],
    resourceAccess: access,
    appRoles: list(raw.appRoles).map((r) => ({
      id: str(r.id),
      displayName: str(r.displayName),
      value: str(r.value),
      enabled: r.isEnabled !== false,
    })),
    owners: owners.map((row) => parseDirObject(row)),
  };
}

export const parseServicePrincipal = (raw: Json): ServicePrincipal => ({
  id: str(raw.id),
  appId: str(raw.appId),
  displayName: str(raw.displayName),
  type: str(raw.servicePrincipalType),
  enabled: raw.accountEnabled !== false,
});

export const parseAppRoleAssignment = (raw: Json): AppRoleAssignment => ({
  id: str(raw.id),
  principal: str(raw.principalDisplayName) || str(raw.principalId),
  principalType: str(raw.principalType),
  resource: str(raw.resourceDisplayName) || str(raw.resourceId),
  appRoleId: str(raw.appRoleId),
});

export const parseUser = (raw: Json): DirUser => ({
  id: str(raw.id),
  upn: str(raw.userPrincipalName),
  displayName: str(raw.displayName),
});

export const parseRoleDefinition = (raw: Json): RoleDefinition => {
  const p = obj(raw.properties);
  return {
    id: str(raw.id),
    name: str(raw.name),
    roleName: str(p.roleName),
    roleType: str(p.type),
  };
};

export const parseRoleAssignment = (raw: Json): RoleAssignment => {
  const p = obj(raw.properties);
  return {
    name: str(raw.name),
    id: str(raw.id),
    scope: str(p.scope),
    roleDefinitionId: str(p.roleDefinitionId),
    principalId: str(p.principalId),
    principalType: str(p.principalType),
  };
};

export const parseLock = (raw: Json): Lock => ({
  name: str(raw.name),
  id: str(raw.id),
  level: str(obj(raw.properties).level),
  notes: str(obj(raw.properties).notes),
});

export const parsePolicyDefinition = (raw: Json): PolicyDefinition => {
  const p = obj(raw.properties);
  return {
    name: str(raw.name),
    displayName: str(p.displayName),
    policyType: str(p.policyType),
    mode: str(p.mode),
  };
};

export const parsePolicyAssignment = (raw: Json): PolicyAssignment => {
  const p = obj(raw.properties);
  return {
    name: str(raw.name),
    displayName: str(p.displayName),
    scope: str(p.scope),
    definition: lastSegment(str(p.policyDefinitionId)),
  };
};

const sortBy = <T>(items: T[], key: (t: T) => string): T[] =>
  items.toSorted((a, b) => key(a).localeCompare(key(b)));

const graph = <T = Json>(
  method: string,
  path: string,
  body: unknown,
  fetchFn: FetchFn,
): Promise<T> => restCall<T>(GRAPH_PROXY_BASE, method, path, body, fetchFn);

const COLLECTION_TOP = "?$top=999";

export async function listApplications(fetchFn: FetchFn = fetch): Promise<Application[]> {
  const r = await graph("GET", `/applications${COLLECTION_TOP}`, undefined, fetchFn);
  return sortBy(
    list(r.value).map((a) => parseApplication(a)),
    (a) => a.displayName,
  );
}

export async function getApplication(
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<ApplicationDetail> {
  const [app, owners] = await Promise.all([
    graph("GET", `/applications/${enc(id)}`, undefined, fetchFn),
    graph("GET", `/applications/${enc(id)}/owners`, undefined, fetchFn),
  ]);
  return parseApplicationDetail(app, list(owners.value));
}

export async function createApplication(
  displayName: string,
  fetchFn: FetchFn = fetch,
): Promise<Application> {
  return parseApplication(
    await graph("POST", "/applications", { displayName: displayName.trim() }, fetchFn),
  );
}

export const deleteApplication = async (id: string, fetchFn: FetchFn = fetch): Promise<void> => {
  await graph("DELETE", `/applications/${enc(id)}`, undefined, fetchFn);
};

/** Adds a client secret; the returned value is the only time the secret is available. */
export async function addSecret(
  id: string,
  displayName: string,
  endDateTime?: string,
  fetchFn: FetchFn = fetch,
): Promise<NewSecret> {
  const body: Record<string, string> = {};
  if (displayName.trim()) body.displayName = displayName.trim();
  if (endDateTime) body.endDateTime = endDateTime;
  const r = await graph("POST", `/applications/${enc(id)}/addPassword`, body, fetchFn);
  return {
    keyId: str(r.keyId),
    displayName: str(r.displayName),
    value: str(r.secretText),
    end: str(r.endDateTime),
  };
}

export const removeSecret = async (
  id: string,
  keyId: string,
  fetchFn: FetchFn = fetch,
): Promise<void> => {
  await graph("POST", `/applications/${enc(id)}/removePassword`, { keyId }, fetchFn);
};

export async function listServicePrincipals(fetchFn: FetchFn = fetch): Promise<ServicePrincipal[]> {
  const r = await graph("GET", `/servicePrincipals${COLLECTION_TOP}`, undefined, fetchFn);
  return sortBy(
    list(r.value).map((row) => parseServicePrincipal(row)),
    (s) => s.displayName,
  );
}

export const createServicePrincipal = async (appId: string, fetchFn: FetchFn = fetch) =>
  parseServicePrincipal(
    await graph("POST", "/servicePrincipals", { appId: appId.trim() }, fetchFn),
  );

export async function listAppRoleAssignments(
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<AppRoleAssignment[]> {
  const r = await graph(
    "GET",
    `/servicePrincipals/${enc(id)}/appRoleAssignments`,
    undefined,
    fetchFn,
  );
  return list(r.value).map((row) => parseAppRoleAssignment(row));
}

export async function listUsers(fetchFn: FetchFn = fetch): Promise<DirUser[]> {
  const r = await graph(
    "GET",
    `/users${COLLECTION_TOP}&$select=id,displayName,userPrincipalName`,
    undefined,
    fetchFn,
  );
  return sortBy(
    list(r.value).map((row) => parseUser(row)),
    (u) => u.upn,
  );
}

export type NewUser = {
  displayName: string;
  mailNickname: string;
  userPrincipalName: string;
  password: string;
  forceChangePasswordNextSignIn: boolean;
};

/** Creates a user; the password goes to the server once and is not part of the answer. */
export const createUser = async (u: NewUser, fetchFn: FetchFn = fetch): Promise<DirUser> =>
  parseUser(
    await graph(
      "POST",
      "/users",
      {
        displayName: u.displayName.trim(),
        mailNickname: u.mailNickname.trim(),
        userPrincipalName: u.userPrincipalName.trim(),
        password: u.password,
        forceChangePasswordNextSignIn: u.forceChangePasswordNextSignIn,
      },
      fetchFn,
    ),
  );

export async function listGroups(fetchFn: FetchFn = fetch): Promise<DirGroup[]> {
  const r = await graph("GET", `/groups${COLLECTION_TOP}`, undefined, fetchFn);
  const groups = list(r.value).map((g) => ({ id: str(g.id), displayName: str(g.displayName) }));
  const members = await Promise.all(groups.map((g) => listMembers(g.id, fetchFn)));
  return sortBy(
    groups.map((g, i) => ({ ...g, members: members[i] })),
    (g) => g.displayName,
  );
}

export async function listMembers(groupId: string, fetchFn: FetchFn = fetch): Promise<DirObject[]> {
  const r = await graph("GET", `/groups/${enc(groupId)}/members`, undefined, fetchFn);
  return list(r.value).map((row) => parseDirObject(row));
}

export const createGroup = (displayName: string, mailNickname: string, fetchFn: FetchFn = fetch) =>
  graph(
    "POST",
    "/groups",
    { displayName: displayName.trim(), mailNickname: mailNickname.trim() },
    fetchFn,
  );

export const addMember = async (
  groupId: string,
  memberId: string,
  fetchFn: FetchFn = fetch,
): Promise<void> => {
  await graph("POST", `/groups/${enc(groupId)}/members/$ref`, { id: memberId }, fetchFn);
};

export const removeMember = async (
  groupId: string,
  memberId: string,
  fetchFn: FetchFn = fetch,
): Promise<void> => {
  await graph(
    "DELETE",
    `/groups/${enc(groupId)}/members/${enc(memberId)}/$ref`,
    undefined,
    fetchFn,
  );
};

/** The proxy rejects getByIds bodies with more ids than this (graphPxMaxIDs). */
const GET_BY_IDS_MAX = 100;

/** Resolves directory object ids to names; unknown ids are absent from the result. */
export async function resolvePrincipals(
  ids: string[],
  fetchFn: FetchFn = fetch,
): Promise<Map<string, DirObject>> {
  const unique = [...new Set(ids.filter((i) => isGuid(i)))];
  const out = new Map<string, DirObject>();
  if (unique.length === 0) return out;
  for (let i = 0; i < unique.length; i += GET_BY_IDS_MAX) {
    const chunk = unique.slice(i, i + GET_BY_IDS_MAX);
    const r = await graph("POST", "/directoryObjects/getByIds", { ids: chunk }, fetchFn);
    for (const o of list(r.value).map((row) => parseDirObject(row))) out.set(o.id, o);
  }
  return out;
}

const tagged = (rows: unknown, type: string): DirObject[] =>
  list(rows).map((r) => ({ ...parseDirObject(r), type }));

/** Everything that can be assigned a role: users, groups and service principals. */
export async function listPrincipals(fetchFn: FetchFn = fetch): Promise<DirObject[]> {
  const [users, groups, sps] = await Promise.all([
    graph("GET", `/users${COLLECTION_TOP}`, undefined, fetchFn),
    graph("GET", `/groups${COLLECTION_TOP}`, undefined, fetchFn),
    graph("GET", `/servicePrincipals${COLLECTION_TOP}`, undefined, fetchFn),
  ]);
  return [
    ...tagged(users.value, "User"),
    ...tagged(groups.value, "Group"),
    ...tagged(sps.value, "ServicePrincipal"),
  ];
}

export const subscriptionScope = (sub: string): string => `/subscriptions/${enc(sub)}`;
export const groupScope = (sub: string, rg: string): string =>
  `${subscriptionScope(sub)}/resourceGroups/${enc(rg)}`;

const authz = <T = Json>(
  method: string,
  scope: string,
  type: string,
  name: string,
  version: string,
  body: unknown,
  fetchFn: FetchFn,
): Promise<T> =>
  restCall<T>(
    AUTHZ_PROXY_BASE,
    method,
    `${scope}/providers/Microsoft.Authorization/${type}${name ? `/${enc(name)}` : ""}?api-version=${version}`,
    body,
    fetchFn,
  );

export async function listRoleDefinitions(
  scope: string,
  fetchFn: FetchFn = fetch,
): Promise<RoleDefinition[]> {
  const r = await authz(
    "GET",
    scope,
    "roleDefinitions",
    "",
    AUTHZ_VERSIONS.roleDefinitions,
    undefined,
    fetchFn,
  );
  return sortBy(
    list(r.value).map((row) => parseRoleDefinition(row)),
    (d) => d.roleName,
  );
}

export async function listRoleAssignments(
  scope: string,
  fetchFn: FetchFn = fetch,
): Promise<RoleAssignment[]> {
  const r = await authz(
    "GET",
    scope,
    "roleAssignments",
    "",
    AUTHZ_VERSIONS.roleAssignments,
    undefined,
    fetchFn,
  );
  return list(r.value).map((row) => parseRoleAssignment(row));
}

export type NewAssignment = {
  roleDefinitionId: string;
  principalId: string;
  principalType?: string;
};

/** A v4 UUID; crypto.randomUUID is missing on non-secure (plain http) origins. */
export function newUuid(): string {
  if (typeof crypto.randomUUID === "function") return crypto.randomUUID();
  const b = crypto.getRandomValues(new Uint8Array(16));
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  const h = Array.from(b, (x) => x.toString(16).padStart(2, "0")).join("");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

export async function createRoleAssignment(
  scope: string,
  a: NewAssignment,
  name: string = newUuid(),
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const properties: Record<string, string> = {
    roleDefinitionId: a.roleDefinitionId,
    principalId: a.principalId.trim(),
  };
  if (a.principalType) properties.principalType = a.principalType;
  await authz(
    "PUT",
    scope,
    "roleAssignments",
    name,
    AUTHZ_VERSIONS.roleAssignments,
    { properties },
    fetchFn,
  );
}

export const deleteRoleAssignment = async (
  scope: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> => {
  await authz(
    "DELETE",
    scope,
    "roleAssignments",
    name,
    AUTHZ_VERSIONS.roleAssignments,
    undefined,
    fetchFn,
  );
};

export async function listLocks(scope: string, fetchFn: FetchFn = fetch): Promise<Lock[]> {
  const r = await authz("GET", scope, "locks", "", AUTHZ_VERSIONS.locks, undefined, fetchFn);
  return sortBy(
    list(r.value).map((row) => parseLock(row)),
    (l) => l.name,
  );
}

export const createLock = async (
  scope: string,
  name: string,
  level: LockLevel,
  notes: string,
  fetchFn: FetchFn = fetch,
): Promise<void> => {
  const properties: Record<string, string> = { level };
  if (notes.trim()) properties.notes = notes.trim();
  await authz("PUT", scope, "locks", name, AUTHZ_VERSIONS.locks, { properties }, fetchFn);
};

export const deleteLock = async (
  scope: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> => {
  await authz("DELETE", scope, "locks", name, AUTHZ_VERSIONS.locks, undefined, fetchFn);
};

export async function listPolicyDefinitions(
  scope: string,
  fetchFn: FetchFn = fetch,
): Promise<PolicyDefinition[]> {
  const r = await authz(
    "GET",
    scope,
    "policyDefinitions",
    "",
    AUTHZ_VERSIONS.policyDefinitions,
    undefined,
    fetchFn,
  );
  return sortBy(
    list(r.value).map((row) => parsePolicyDefinition(row)),
    (d) => d.displayName || d.name,
  );
}

export async function listPolicyAssignments(
  scope: string,
  fetchFn: FetchFn = fetch,
): Promise<PolicyAssignment[]> {
  const r = await authz(
    "GET",
    scope,
    "policyAssignments",
    "",
    AUTHZ_VERSIONS.policyAssignments,
    undefined,
    fetchFn,
  );
  return sortBy(
    list(r.value).map((row) => parsePolicyAssignment(row)),
    (a) => a.displayName || a.name,
  );
}

export const validLockName = (v: string): boolean => /^[A-Za-z0-9][A-Za-z0-9._-]{0,89}$/.test(v);
export const validNickname = (v: string): boolean => /^[A-Za-z0-9._-]{1,64}$/.test(v.trim());
export const validUpn = (v: string): boolean =>
  /^[A-Za-z0-9._'+-]{1,64}@[A-Za-z0-9.-]{1,253}$/.test(v.trim());

/** A usable password is non-empty; the server enforces the directory's complexity rules. */
export const validPassword = (v: string): boolean => v.length > 0 && v.length <= 256;

export const mailNicknameFrom = (displayName: string): string =>
  displayName
    .toLowerCase()
    .replaceAll(/[^a-z0-9._-]+/g, "")
    .slice(0, 64);

/** Expiry of a new secret, `months` from `from`, as an RFC 3339 UTC string. */
export function secretExpiry(months: number, from: Date = new Date()): string {
  const d = new Date(from.getTime());
  d.setUTCMonth(d.getUTCMonth() + months);
  return d.toISOString().replace(/\.\d{3}Z$/, "Z");
}

export function roleNameOf(defs: RoleDefinition[], roleDefinitionId: string): string {
  const guid = lastSegment(roleDefinitionId).toLowerCase();
  return defs.find((d) => d.name.toLowerCase() === guid)?.roleName ?? guid;
}
