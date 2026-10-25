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
