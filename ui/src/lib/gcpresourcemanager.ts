/** Client for the Resource Manager and Service Usage emulator via the dashboard's in-process proxy. */

import { listAll, restCall, shortName, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const RM_PROXY_BASE = "/dashboard/api/v1/gcp/resourcemanager/v1";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});

export type RmKind = "organizations" | "folders" | "projects";

export type RmEnforcement = { projects: boolean; serviceEnablement: boolean };

export type RmNode = {
  kind: "organization" | "folder" | "project";
  /** Full resource name: organizations/N, folders/N or projects/NUMBER. */
  name: string;
  /** Path id used with the proxy: org/folder number or the project id. */
  id: string;
  label: string;
  parent: string;
  state: string;
  createTime: string;
  deleteTime: string;
  number: string;
  labels: Record<string, string>;
  etag: string;
  children: RmNode[];
};

export type RmService = {
  name: string;
  id: string;
  title: string;
  summary: string;
  state: string;
};

export type IamBinding = {
  role: string;
  members: string[];
  condition: string;
};

export type IamPolicy = { etag: string; bindings: IamBinding[] };

export type TagKey = {
  name: string;
  shortName: string;
  namespacedName: string;
  description: string;
  values: TagValue[];
};

export type TagValue = {
  name: string;
  shortName: string;
  namespacedName: string;
  description: string;
};

export type TagBinding = { name: string; parent: string; tagValue: string; namespaced: string };

export function parseOrganization(raw: Json): RmNode {
  const name = str(raw.name);
  return {
    kind: "organization",
    name,
    id: shortName(name),
    label: str(raw.displayName) || shortName(name),
    parent: "",
    state: str(raw.state) || "ACTIVE",
    createTime: str(raw.createTime),
    deleteTime: str(raw.deleteTime),
    number: "",
    labels: {},
    etag: str(raw.etag),
    children: [],
  };
}

export function parseFolder(raw: Json): RmNode {
  const name = str(raw.name);
  return {
    kind: "folder",
    name,
    id: shortName(name),
    label: str(raw.displayName) || shortName(name),
    parent: str(raw.parent),
    state: str(raw.state) || "ACTIVE",
    createTime: str(raw.createTime),
    deleteTime: str(raw.deleteTime),
    number: "",
    labels: {},
    etag: str(raw.etag),
    children: [],
  };
}

export function parseProject(raw: Json): RmNode {
  const name = str(raw.name);
  const id = str(raw.projectId) || shortName(name);
  return {
    kind: "project",
    name,
    id,
    label: str(raw.displayName) || id,
    parent: str(raw.parent),
    state: str(raw.state) || "ACTIVE",
    createTime: str(raw.createTime),
    deleteTime: str(raw.deleteTime),
    number: shortName(name),
    labels: strMap(raw.labels),
    etag: str(raw.etag),
    children: [],
  };
}

const kindPath = (n: Pick<RmNode, "kind" | "id">): string =>
  `${n.kind === "organization" ? "organizations" : n.kind === "folder" ? "folders" : "projects"}/${enc(n.id)}`;

export const nodePath = kindPath;

export async function getEnforcement(fetchFn: FetchFn = fetch): Promise<RmEnforcement> {
  const body = await restCall(RM_PROXY_BASE, "GET", "/enforcement", undefined, fetchFn);
  return { projects: body.projects === true, serviceEnablement: body.serviceEnablement === true };
}

export async function listOrganizations(fetchFn: FetchFn = fetch): Promise<RmNode[]> {
  return (await listAll(RM_PROXY_BASE, "/organizations", "organizations", fetchFn)).map((o) =>
    parseOrganization(o),
  );
}

export async function listFolders(parent: string, fetchFn: FetchFn = fetch): Promise<RmNode[]> {
  return (
    await listAll(RM_PROXY_BASE, "/folders", "folders", fetchFn, `parent=${enc(parent)}`)
  ).map((f) => parseFolder(f));
}

export async function listProjects(parent: string, fetchFn: FetchFn = fetch): Promise<RmNode[]> {
  return (
    await listAll(RM_PROXY_BASE, "/projects", "projects", fetchFn, `parent=${enc(parent)}`)
  ).map((p) => parseProject(p));
}

/** Projects that have no organization or folder parent. */
export async function listOrphanProjects(fetchFn: FetchFn = fetch): Promise<RmNode[]> {
  return (await listAll(RM_PROXY_BASE, "/projects", "projects", fetchFn))
    .map((p) => parseProject(p))
    .filter((p) => !p.parent);
}

/** Pseudo-root that holds projects without a parent; its empty name keeps it unselectable. */
export function orphanRoot(projects: RmNode[]): RmNode {
  return {
    kind: "organization",
    name: "",
    id: "",
    label: "No organization",
    parent: "",
    state: "ACTIVE",
    createTime: "",
    deleteTime: "",
    number: "",
    labels: {},
    etag: "",
    children: projects,
  };
}

/** Loads the whole organization subtree: folders recursively, then each level's projects. */
export async function loadTree(org: RmNode, fetchFn: FetchFn = fetch): Promise<RmNode> {
  const root = { ...org, children: [] as RmNode[] };
  const walk = async (node: RmNode): Promise<void> => {
    const [folders, projects] = await Promise.all([
      listFolders(node.name, fetchFn),
      listProjects(node.name, fetchFn),
    ]);
    node.children = [...folders, ...projects];
    await Promise.all(folders.map((f) => walk(f)));
  };
  await walk(root);
  return root;
}

export function flatten(root: RmNode): RmNode[] {
  return [root, ...root.children.flatMap((c) => flatten(c))];
}

export type ParentOption = { name: string; label: string };

/** Org and active folders as "Org / Folder / Sub" choices, optionally skipping a subtree (for moves). */
export function parentOptions(root: RmNode, skip = ""): ParentOption[] {
  const out: ParentOption[] = [];
  const walk = (n: RmNode, path: string) => {
    if (n.kind === "project" || n.state !== "ACTIVE" || n.name === skip) return;
    const here = path ? `${path} / ${n.label}` : n.label;
    out.push({ name: n.name, label: here });
    for (const c of n.children) walk(c, here);
  };
  walk(root, "");
  return out;
}

export async function getProject(id: string, fetchFn: FetchFn = fetch): Promise<RmNode> {
  return parseProject(
    await restCall(RM_PROXY_BASE, "GET", `/projects/${enc(id)}`, undefined, fetchFn),
  );
}

export function parseService(raw: Json): RmService {
  const cfg = obj(raw.config);
  const name = str(cfg.name) || shortName(str(raw.name));
  return {
    name: str(raw.name),
    id: name,
    title: str(cfg.title) || name,
    summary: str(obj(cfg.documentation).summary),
    state: str(raw.state) || "DISABLED",
  };
}

export async function listServices(
  project: string,
  state: "" | "ENABLED" | "DISABLED" = "",
  fetchFn: FetchFn = fetch,
): Promise<RmService[]> {
  const out = await listAll(
    RM_PROXY_BASE,
    `/projects/${enc(project)}/services`,
    "services",
    fetchFn,
    `pageSize=200${state ? `&state=${state}` : ""}`,
  );
  return out.map((s) => parseService(s));
}

export function filterServices(all: RmService[], query: string, state: string): RmService[] {
  const q = query.trim().toLowerCase();
  return all.filter(
    (s) =>
      (!state || s.state === state) &&
      (!q || `${s.id} ${s.title} ${s.summary}`.toLowerCase().includes(q)),
  );
}

export async function enableService(
  project: string,
  service: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    RM_PROXY_BASE,
    "POST",
    `/projects/${enc(project)}/services/${enc(service)}:enable`,
    {},
    fetchFn,
  );
}

export async function disableService(
  project: string,
  service: string,
  disableDependentServices: boolean,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    RM_PROXY_BASE,
    "POST",
    `/projects/${enc(project)}/services/${enc(service)}:disable`,
    { disableDependentServices },
    fetchFn,
  );
}

export function parsePolicy(raw: Json): IamPolicy {
  return {
    etag: str(raw.etag),
    bindings: list(raw.bindings).map((b) => {
      const cond = obj(b.condition);
      return {
        role: str(b.role),
        members: Array.isArray(b.members) ? b.members.map(String) : [],
        condition: b.condition ? str(cond.title) || str(cond.expression) || "conditional" : "",
      };
    }),
  };
}

export async function getIamPolicy(node: RmNode, fetchFn: FetchFn = fetch): Promise<IamPolicy> {
  return parsePolicy(
    await restCall(RM_PROXY_BASE, "POST", `/${kindPath(node)}:getIamPolicy`, {}, fetchFn),
  );
}

export async function addMember(
  node: RmNode,
  role: string,
  member: string,
  etag: string,
  fetchFn: FetchFn = fetch,
): Promise<IamPolicy> {
  return parsePolicy(
    await restCall(
      RM_PROXY_BASE,
      "POST",
      `/${kindPath(node)}:addMember`,
      { role, member, etag },
      fetchFn,
    ),
  );
}

export async function removeMember(
  node: RmNode,
  role: string,
  member: string,
  etag: string,
  fetchFn: FetchFn = fetch,
): Promise<IamPolicy> {
  return parsePolicy(
    await restCall(
      RM_PROXY_BASE,
      "POST",
      `/${kindPath(node)}:removeMember`,
      { role, member, etag },
      fetchFn,
    ),
  );
}

export async function createFolder(
  parent: string,
  displayName: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(RM_PROXY_BASE, "POST", "/folders", { parent, displayName }, fetchFn);
}

export type NewProject = {
  projectId: string;
  displayName: string;
  parent: string;
  labels: { key: string; value: string }[];
};

export function buildProjectBody(p: NewProject): Json {
  const labels = Object.fromEntries(
    p.labels.filter((l) => l.key.trim()).map((l) => [l.key.trim(), l.value]),
  );
  const body: Json = { projectId: p.projectId.trim(), parent: p.parent };
  if (p.displayName.trim()) body.displayName = p.displayName.trim();
  if (Object.keys(labels).length > 0) body.labels = labels;
  return body;
}

export async function createProject(p: NewProject, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(RM_PROXY_BASE, "POST", "/projects", buildProjectBody(p), fetchFn);
}

export async function moveNode(
  node: RmNode,
  destinationParent: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(RM_PROXY_BASE, "POST", `/${kindPath(node)}:move`, { destinationParent }, fetchFn);
}

export async function deleteNode(node: RmNode, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(RM_PROXY_BASE, "DELETE", `/${kindPath(node)}`, undefined, fetchFn);
}

export async function undeleteNode(node: RmNode, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(RM_PROXY_BASE, "POST", `/${kindPath(node)}:undelete`, {}, fetchFn);
}

/** Full resource name tag bindings are keyed on. */
export const bindingParent = (kind: RmKind, id: string): string =>
  `//cloudresourcemanager.googleapis.com/${kind}/${id}`;

function parseTagValue(raw: Json): TagValue {
  const name = str(raw.name);
  return {
    name,
    shortName: str(raw.shortName) || shortName(name),
    namespacedName: str(raw.namespacedName),
    description: str(raw.description),
  };
}

export async function listTagKeys(parent: string, fetchFn: FetchFn = fetch): Promise<TagKey[]> {
  const keys = await listAll(
    RM_PROXY_BASE,
    "/tagKeys",
    "tagKeys",
    fetchFn,
    `parent=${enc(parent)}`,
  );
  return Promise.all(
    keys.map(async (k) => {
      const name = str(k.name);
      const values = await listAll(
        RM_PROXY_BASE,
        "/tagValues",
        "tagValues",
        fetchFn,
        `parent=${enc(name)}`,
      );
      return {
        name,
        shortName: str(k.shortName) || shortName(name),
        namespacedName: str(k.namespacedName),
        description: str(k.description),
        values: values.map((v) => parseTagValue(v)),
      };
    }),
  );
}

export async function listTagBindings(
  parents: string[],
  fetchFn: FetchFn = fetch,
): Promise<TagBinding[]> {
  const pages = await Promise.all(
    parents.map((p) =>
      listAll(RM_PROXY_BASE, "/tagBindings", "tagBindings", fetchFn, `parent=${enc(p)}`),
    ),
  );
  return pages.flat().map((b) => ({
    name: str(b.name),
    parent: str(b.parent),
    tagValue: str(b.tagValue),
    namespaced: str(b.tagValueNamespacedName),
  }));
}

/** Project id rules mirrored from the server: 6-30 chars, lowercase letters/digits/hyphens, starts with a letter. */
export function validProjectId(v: string): boolean {
  return /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/.test(v) && !/google|ssl/.test(v);
}

/** Display names of projects: 4-30 of letters, digits, hyphen, quotes, space and '!'. */
export function validProjectName(v: string): boolean {
  return v === "" || /^[\p{L}\p{N}\-'" !]{4,30}$/u.test(v);
}

/** Folder display names: 3-30 chars, letters/digits/underscore/hyphen/space, no space at either end. */
export function validFolderName(v: string): boolean {
  return /^[\p{L}\p{N}][\p{L}\p{N}_ -]{1,28}[\p{L}\p{N}]$/u.test(v);
}

export const isDeleted = (n: { state: string }): boolean => n.state === "DELETE_REQUESTED";

/** Tailwind classes for a lifecycle state badge. */
export function statePill(s: string): string {
  if (s === "ACTIVE" || s === "ENABLED")
    return "bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300";
  if (s === "DELETE_REQUESTED")
    return "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300";
  return "bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300";
}

export function memberKind(m: string): string {
  const i = m.indexOf(":");
  return i > 0 ? m.slice(0, i) : m;
}

/** Roles sorted, each with its members, unconditioned bindings first. */
export function groupBindings(policy: IamPolicy): IamBinding[] {
  return policy.bindings.toSorted(
    (a, b) =>
      Number(Boolean(a.condition)) - Number(Boolean(b.condition)) || a.role.localeCompare(b.role),
  );
}

export const validRole = (v: string): boolean =>
  /^(roles\/[A-Za-z0-9_.]{1,128}|(projects|organizations)\/[A-Za-z0-9_-]{1,64}\/roles\/[A-Za-z0-9_.]{1,128})$/.test(
    v,
  );

export const validMember = (v: string): boolean => /^\S{1,256}$/.test(v);
