/** Client for the Oracle Cloud Identity emulator via the dashboard's in-process proxy. */

import { restCall, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const IDENTITY_PROXY_BASE = "/dashboard/api/v1/oci/identity";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type Compartment = {
  id: string;
  parentId: string;
  name: string;
  description: string;
  state: string;
  timeCreated: string;
};

export type CompartmentNode = Compartment & { children: CompartmentNode[]; depth: number };

export type UserInfo = {
  id: string;
  name: string;
  description: string;
  email: string;
  state: string;
  timeCreated: string;
  mfa: boolean;
  freeformTags: Record<string, string>;
};

export type GroupInfo = {
  id: string;
  name: string;
  description: string;
  state: string;
  timeCreated: string;
};

export type Membership = { id: string; userId: string; groupId: string };

export type PolicyInfo = {
  id: string;
  name: string;
  description: string;
  compartmentId: string;
  statements: string[];
  state: string;
  timeCreated: string;
};

export type ApiKeyInfo = { fingerprint: string; keyId: string; state: string; timeCreated: string };

export function parseCompartment(raw: Json): Compartment {
  return {
    id: str(raw.id),
    parentId: str(raw.compartmentId),
    name: str(raw.name),
    description: str(raw.description),
    state: str(raw.lifecycleState),
    timeCreated: str(raw.timeCreated),
  };
}

export function parseUser(raw: Json): UserInfo {
  return {
    id: str(raw.id),
    name: str(raw.name),
    description: str(raw.description),
    email: str(raw.email),
    state: str(raw.lifecycleState),
    timeCreated: str(raw.timeCreated),
    mfa: raw.isMfaActivated === true,
    freeformTags: strMap(raw.freeformTags),
  };
}

export function parseGroup(raw: Json): GroupInfo {
  return {
    id: str(raw.id),
    name: str(raw.name),
    description: str(raw.description),
    state: str(raw.lifecycleState),
    timeCreated: str(raw.timeCreated),
  };
}

export function parsePolicy(raw: Json): PolicyInfo {
  return {
    id: str(raw.id),
    name: str(raw.name),
    description: str(raw.description),
    compartmentId: str(raw.compartmentId),
    statements: (Array.isArray(raw.statements) ? raw.statements : []).map(String),
    state: str(raw.lifecycleState),
    timeCreated: str(raw.timeCreated),
  };
}

export function parseApiKey(raw: Json): ApiKeyInfo {
  return {
    fingerprint: str(raw.fingerprint),
    keyId: str(raw.keyId),
    state: str(raw.lifecycleState),
    timeCreated: str(raw.timeCreated),
  };
}

/**
 * Builds the compartment tree rooted at the tenancy (the compartment with no known parent).
 * Orphans whose parent is missing attach to the root so nothing disappears from view.
 */
export function buildCompartmentTree(items: Compartment[]): CompartmentNode[] {
  const nodes = new Map<string, CompartmentNode>(
    items.map((c) => [c.id, { ...c, children: [], depth: 0 }]),
  );
  const roots: CompartmentNode[] = [];
  for (const node of nodes.values()) {
    const parent = nodes.get(node.parentId);
    if (parent && parent !== node) parent.children.push(node);
    else roots.push(node);
  }
  const sortRec = (level: CompartmentNode[], depth: number) => {
    level.sort((a, b) => a.name.localeCompare(b.name));
    for (const n of level) {
      n.depth = depth;
      sortRec(n.children, depth + 1);
    }
  };
  sortRec(roots, 0);
  return roots;
}

/** Depth-first flattening of a compartment tree for table display. */
export function flattenTree(roots: CompartmentNode[]): CompartmentNode[] {
  return roots.flatMap((n) => [n, ...flattenTree(n.children)]);
}

export async function listCompartments(fetchFn: FetchFn = fetch): Promise<Compartment[]> {
  const body = await restCall(IDENTITY_PROXY_BASE, "GET", "/compartments", undefined, fetchFn);
  return list(body.compartments).map((c) => parseCompartment(c));
}

export async function listUsers(fetchFn: FetchFn = fetch): Promise<UserInfo[]> {
  const body = await restCall(IDENTITY_PROXY_BASE, "GET", "/users", undefined, fetchFn);
  return list(body.users).map((u) => parseUser(u));
}

export async function listGroups(fetchFn: FetchFn = fetch): Promise<GroupInfo[]> {
  const body = await restCall(IDENTITY_PROXY_BASE, "GET", "/groups", undefined, fetchFn);
  return list(body.groups).map((g) => parseGroup(g));
}

export async function listMemberships(fetchFn: FetchFn = fetch): Promise<Membership[]> {
  const body = await restCall(IDENTITY_PROXY_BASE, "GET", "/memberships", undefined, fetchFn);
  return list(body.memberships).map((m) => ({
    id: str(m.id),
    userId: str(m.userId),
    groupId: str(m.groupId),
  }));
}

export async function listPolicies(fetchFn: FetchFn = fetch): Promise<PolicyInfo[]> {
  const body = await restCall(IDENTITY_PROXY_BASE, "GET", "/policies", undefined, fetchFn);
  return list(body.policies).map((p) => parsePolicy(p));
}

export async function listApiKeys(userId: string, fetchFn: FetchFn = fetch): Promise<ApiKeyInfo[]> {
  const body = await restCall(
    IDENTITY_PROXY_BASE,
    "GET",
    `/users/${enc(userId)}/keys`,
    undefined,
    fetchFn,
  );
  return list(body.keys).map((k) => parseApiKey(k));
}
