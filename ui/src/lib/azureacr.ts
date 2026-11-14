import { restCall, str, type FetchFn, type Json } from "./gcprest";

export const ACR_PROXY_BASE = "/dashboard/api/v1/azure/acr";

export type AcrRegistry = {
  name: string;
  loginServer: string;
  adminEnabled: boolean;
  createdAt: string;
  repositoryCount: number;
};
export type AcrRepository = {
  name: string;
  created: string;
  updated: string;
  manifestCount: number;
  tagCount: number;
};
export type AcrManifest = {
  digest: string;
  mediaType: string;
  architecture: string;
  os: string;
  sizeBytes: number;
  index: boolean;
  childManifests: number;
  created: string;
  tags: string[];
};
export type AcrTag = AcrManifest & { name: string; updated: string };
export type AcrCredentials = { username: string; password: string; password2: string };

const seg = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

function manifest(m: Json): AcrManifest {
  return {
    digest: str(m.digest),
    mediaType: str(m.mediaType),
    architecture: str(m.architecture),
    os: str(m.os),
    sizeBytes: num(m.sizeBytes),
    index: m.index === true,
    childManifests: num(m.childManifests),
    created: str(m.created),
    tags: Array.isArray(m.tags) ? (m.tags as string[]) : [],
  };
}

export function formatSize(bytes: number): string {
  if (bytes <= 0) return "-";
  const units = ["B", "KiB", "MiB", "GiB"];
  let v = bytes;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${i === 0 ? v : v.toFixed(1)} ${units[i]}`;
}

export function shortDigest(d: string): string {
  return d.startsWith("sha256:") ? d.slice(0, 19) : d;
}

export function platform(m: Pick<AcrManifest, "os" | "architecture" | "index">): string {
  if (m.index) return "multi-platform";
  return [m.os, m.architecture].filter(Boolean).join("/") || "-";
}

export async function listRegistries(fetchFn?: FetchFn): Promise<AcrRegistry[]> {
  const body = await restCall(ACR_PROXY_BASE, "GET", "/registries", undefined, fetchFn);
  return list(body.registries).map((r) => ({
    name: str(r.name),
    loginServer: str(r.loginServer),
    adminEnabled: r.adminEnabled === true,
    createdAt: str(r.createdAt),
    repositoryCount: num(r.repositoryCount),
  }));
}

export async function listRepositories(reg: string, fetchFn?: FetchFn): Promise<AcrRepository[]> {
  const body = await restCall(
    ACR_PROXY_BASE,
    "GET",
    `/registries/${seg(reg)}/repositories`,
    undefined,
    fetchFn,
  );
  return list(body.repositories).map((r) => ({
    name: str(r.name),
    created: str(r.created),
    updated: str(r.updated),
    manifestCount: num(r.manifestCount),
    tagCount: num(r.tagCount),
  }));
}

export async function listTags(reg: string, repo: string, fetchFn?: FetchFn): Promise<AcrTag[]> {
  const body = await restCall(
    ACR_PROXY_BASE,
    "GET",
    `/registries/${seg(reg)}/tags?repo=${seg(repo)}`,
    undefined,
    fetchFn,
  );
  return list(body.tags).map((t) => ({
    ...manifest(t),
    name: str(t.name),
    updated: str(t.updated),
  }));
}

export async function listManifests(
  reg: string,
  repo: string,
  fetchFn?: FetchFn,
): Promise<AcrManifest[]> {
  const body = await restCall(
    ACR_PROXY_BASE,
    "GET",
    `/registries/${seg(reg)}/manifests?repo=${seg(repo)}`,
    undefined,
    fetchFn,
  );
  return list(body.manifests).map((m) => manifest(m));
}

export async function deleteTag(
  reg: string,
  repo: string,
  tag: string,
  fetchFn?: FetchFn,
): Promise<void> {
  const q = new URLSearchParams({ repo, tag });
  await restCall(ACR_PROXY_BASE, "DELETE", `/registries/${seg(reg)}/tags?${q}`, undefined, fetchFn);
}

/** Fetches the admin passwords. Only call this from an explicit reveal action. */
export async function revealCredentials(reg: string, fetchFn?: FetchFn): Promise<AcrCredentials> {
  const b = await restCall(
    ACR_PROXY_BASE,
    "POST",
    `/registries/${seg(reg)}/credentials/reveal`,
    undefined,
    fetchFn,
  );
  return { username: str(b.username), password: str(b.password), password2: str(b.password2) };
}
