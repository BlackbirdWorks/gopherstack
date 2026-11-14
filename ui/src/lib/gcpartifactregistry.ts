/** Client for the Artifact Registry emulator via the dashboard's in-process REST proxy. */

import { listAll, restCall, shortName, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const AR_PROXY_BASE = "/dashboard/api/v1/gcp/artifactregistry/v1";

const enc = encodeURIComponent;
const DEFAULT_PROJECT = "gopherstack-local";
const AR_PROJECT_KEY = "gopherstack_artifactregistry_project";
const AR_LOCATION_KEY = "gopherstack_artifactregistry_location";
const DEFAULT_AR_LOCATION = "us-central1";
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});

export type ArScope = { project: string; location: string };
export type RepoInfo = {
  name: string;
  id: string;
  format: string;
  mode: string;
  description: string;
  sizeBytes: number;
  createTime: string;
  labels: Record<string, string>;
  raw: Json;
};
export type PackageInfo = {
  name: string;
  id: string;
  displayName: string;
  updateTime: string;
  raw: Json;
};
export type VersionInfo = {
  name: string;
  id: string;
  createTime: string;
  relatedTags: string[];
  raw: Json;
};
export type TagInfo = { name: string; id: string; version: string; raw: Json };
export type FileInfo = {
  name: string;
  id: string;
  sizeBytes: number;
  hashes: string[];
  owner: string;
  createTime: string;
  raw: Json;
};

const base = (s: ArScope) => `/projects/${enc(s.project)}/locations/${enc(s.location)}`;

/** Resource-name segment as it appears in a URL path (package and file IDs contain slashes). */
export const seg = (id: string) => enc(id);

export function formatBytes(n: number): string {
  if (!Number.isFinite(n) || n <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(n) / Math.log(1024)));
  const v = n / 1024 ** i;
  return `${i === 0 ? v : v.toFixed(1)} ${units[i]}`;
}

export function parseRepo(raw: Json): RepoInfo {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    format: str(raw.format) || "FORMAT_UNSPECIFIED",
    mode: str(raw.mode) || "STANDARD_REPOSITORY",
    description: str(raw.description),
    sizeBytes: Number(raw.sizeBytes ?? 0),
    createTime: str(raw.createTime),
    labels: strMap(raw.labels),
    raw,
  };
}

/** Decodes a path-escaped resource-name tail ("a%2Fb" -> "a/b"); the input unchanged when malformed. */
export function unescapeName(s: string): string {
  try {
    return decodeURIComponent(s);
  } catch {
    return s;
  }
}

export function parsePackage(raw: Json): PackageInfo {
  const name = str(raw.name);
  return {
    name,
    id: unescapeName(shortName(name)),
    displayName: str(raw.displayName),
    updateTime: str(raw.updateTime),
    raw,
  };
}

export function parseVersion(raw: Json): VersionInfo {
  const name = str(raw.name);
  const tags = Array.isArray(raw.relatedTags) ? (raw.relatedTags as Json[]) : [];
  return {
    name,
    id: unescapeName(shortName(name)),
    createTime: str(raw.createTime),
    relatedTags: tags.map((t) => shortName(str(obj(t).name))).filter(Boolean),
    raw,
  };
}

export function parseTag(raw: Json): TagInfo {
  const name = str(raw.name);
  return { name, id: shortName(name), version: shortName(str(raw.version)), raw };
}

export function parseFile(raw: Json): FileInfo {
  const name = str(raw.name);
  const hashes = Array.isArray(raw.hashes) ? (raw.hashes as Json[]) : [];
  return {
    name,
    id: unescapeName(shortName(name)),
    sizeBytes: Number(raw.sizeBytes ?? 0),
    hashes: hashes.map((h) => `${str(obj(h).type) || "HASH"}:${str(obj(h).value)}`),
    owner: shortName(str(raw.owner)),
    createTime: str(raw.createTime),
    raw,
  };
}

export async function listRepositories(s: ArScope, fetchFn: FetchFn = fetch): Promise<RepoInfo[]> {
  return (await listAll(AR_PROXY_BASE, `${base(s)}/repositories`, "repositories", fetchFn)).map(
    (r) => parseRepo(r),
  );
}

export async function listPackages(repo: string, fetchFn: FetchFn = fetch): Promise<PackageInfo[]> {
  return (await listAll(AR_PROXY_BASE, `/${repo}/packages`, "packages", fetchFn)).map((r) =>
    parsePackage(r),
  );
}

export async function listVersions(pkg: string, fetchFn: FetchFn = fetch): Promise<VersionInfo[]> {
  return (await listAll(AR_PROXY_BASE, `/${pkg}/versions`, "versions", fetchFn, "view=BASIC")).map(
    (r) => parseVersion(r),
  );
}

export async function listTags(pkg: string, fetchFn: FetchFn = fetch): Promise<TagInfo[]> {
  return (await listAll(AR_PROXY_BASE, `/${pkg}/tags`, "tags", fetchFn)).map((r) => parseTag(r));
}

export async function listFiles(repo: string, fetchFn: FetchFn = fetch): Promise<FileInfo[]> {
  return (await listAll(AR_PROXY_BASE, `/${repo}/files`, "files", fetchFn)).map((r) =>
    parseFile(r),
  );
}

export type DockerImageInfo = {
  name: string;
  image: string;
  digest: string;
  tags: string[];
  sizeBytes: number;
  uploadTime: string;
  mediaType: string;
  raw: Json;
};
export type CleanupPolicyInfo = { id: string; action: string; summary: string };
export type UpstreamInfo = { kind: "remote" | "virtual" | "none"; lines: string[] };
export type CleanupPreview = { dryRun: boolean; policies: number; versions: string[] };

const list = (v: unknown): unknown[] => (Array.isArray(v) ? v : []);

/** STANDARD_REPOSITORY -> STANDARD; the label shown in the mode column. */
export function modeLabel(mode: string): string {
  return mode.replace(/_REPOSITORY$/, "") || "STANDARD";
}

export function parseDockerImage(raw: Json): DockerImageInfo {
  const name = str(raw.name);
  const [image, digest] = shortName(name).split("@");
  return {
    name,
    image: unescapeName(image ?? ""),
    digest: digest ?? "",
    tags: list(raw.tags).map(String),
    sizeBytes: Number(raw.imageSizeBytes ?? 0),
    uploadTime: str(raw.uploadTime),
    mediaType: str(raw.mediaType),
    raw,
  };
}

function describeCondition(c: Json): string {
  const parts: string[] = [];
  const state = str(c.tagState);
  if (state && state !== "TAG_STATE_UNSPECIFIED") parts.push(state.toLowerCase());
  for (const [key, label] of [
    ["tagPrefixes", "tag prefix"],
    ["versionNamePrefixes", "version prefix"],
    ["packageNamePrefixes", "package prefix"],
  ] as const) {
    const v = list(c[key]).map(String);
    if (v.length > 0) parts.push(`${label} ${v.join("|")}`);
  }
  if (c.olderThan) parts.push(`older than ${str(c.olderThan)}`);
  if (c.newerThan) parts.push(`newer than ${str(c.newerThan)}`);
  return parts.join(", ") || "any version";
}

/** Cleanup policies of a repository, sorted by id, each reduced to one line. */
export function parseCleanupPolicies(raw: Json): CleanupPolicyInfo[] {
  const policies = obj(raw.cleanupPolicies);
  return Object.keys(policies)
    .toSorted()
    .map((key) => {
      const p = obj(policies[key]);
      const mrv = obj(p.mostRecentVersions);
      const summary = p.mostRecentVersions
        ? `keep the ${String(mrv.keepCount ?? 0)} most recent versions${list(mrv.packageNamePrefixes).length > 0 ? ` of ${list(mrv.packageNamePrefixes).join("|")}` : ""}`
        : describeCondition(obj(p.condition));
      return { id: str(p.id) || key, action: str(p.action) || "ACTION_UNSPECIFIED", summary };
    });
}

/** "2 policies (1 delete, 1 keep), dry run" or an empty string when none are set. */
export function summarizeCleanup(raw: Json): string {
  const ps = parseCleanupPolicies(raw);
  if (ps.length === 0) return "";
  const del = ps.filter((p) => p.action === "DELETE").length;
  const keep = ps.filter((p) => p.action === "KEEP").length;
  const dry = raw.cleanupPolicyDryRun === true ? ", dry run" : "";
  return `${ps.length} ${ps.length === 1 ? "policy" : "policies"} (${del} delete, ${keep} keep)${dry}`;
}

/** Remote and virtual repository settings. Credentials are reduced to the Secret Manager resource name. */
export function parseUpstream(raw: Json): UpstreamInfo {
  const remote = obj(raw.remoteRepositoryConfig);
  if (raw.remoteRepositoryConfig) {
    const lines: string[] = [];
    if (remote.description) lines.push(`Description: ${str(remote.description)}`);
    for (const [key, v] of Object.entries(remote)) {
      if (!key.endsWith("Repository") || typeof v !== "object" || v === null) continue;
      const cfg = v as Json;
      const custom = str(obj(cfg.customRepository).uri);
      const pub = str(cfg.publicRepository);
      lines.push(
        `Upstream (${key.replace(/Repository$/, "")}): ${custom || pub || JSON.stringify(cfg)}`,
      );
    }
    const cred = obj(obj(remote.upstreamCredentials).usernamePasswordCredentials);
    if (obj(remote.upstreamCredentials).usernamePasswordCredentials) {
      lines.push(
        `Credentials: user ${str(cred.username) || "(none)"}, password secret ${str(cred.passwordSecretVersion) || "(none)"}`,
      );
    }
    return { kind: "remote", lines };
  }
  const virtual = obj(raw.virtualRepositoryConfig);
  if (raw.virtualRepositoryConfig) {
    const lines = list(virtual.upstreamPolicies).map((p) => {
      const o = obj(p);
      return `${str(o.id) || "(unnamed)"}: ${str(o.repository)} (priority ${String(o.priority ?? 0)})`;
    });
    return { kind: "virtual", lines };
  }
  return { kind: "none", lines: [] };
}

export async function listDockerImages(
  repo: string,
  fetchFn: FetchFn = fetch,
): Promise<DockerImageInfo[]> {
  return (await listAll(AR_PROXY_BASE, `/${repo}/dockerImages`, "dockerImages", fetchFn)).map((r) =>
    parseDockerImage(r),
  );
}

export async function previewCleanup(
  repo: string,
  fetchFn: FetchFn = fetch,
): Promise<CleanupPreview> {
  const body = await restCall(AR_PROXY_BASE, "GET", `/${repo}:cleanupPreview`, undefined, fetchFn);
  return {
    dryRun: body.dryRun === true,
    policies: Number(body.policies ?? 0),
    versions: list(body.versions).map(String),
  };
}

/** Same-origin URL that downloads a file through the dashboard proxy (size-capped, attachment). */
export function fileDownloadUrl(fileName: string): string {
  return `/dashboard/api/v1/gcp/artifactregistry/download?name=${encodeURIComponent(fileName)}`;
}

export async function deleteResource(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(AR_PROXY_BASE, "DELETE", `/${name}`, undefined, fetchFn);
}

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredArProject = () => readStored(AR_PROJECT_KEY, DEFAULT_PROJECT);
export const readStoredArLocation = () => readStored(AR_LOCATION_KEY, DEFAULT_AR_LOCATION);

export function writeStoredArScope(project: string, location: string): void {
  try {
    localStorage.setItem(AR_PROJECT_KEY, project);
    localStorage.setItem(AR_LOCATION_KEY, location);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
