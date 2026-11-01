/** Client for the Oracle Cloud Artifacts and Container Registry emulator through the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const ART_PROXY_BASE = "/dashboard/api/v1/oci/artifacts";
export const ART_COMPARTMENT_KEY = "gopherstack_ociart_compartment";
export const ART_DOWNLOAD_LIMIT_MIB = 64;

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type ContainerRepo = {
  id: string;
  name: string;
  state: string;
  isPublic: boolean;
  isImmutable: boolean;
  imageCount: number;
  layerCount: number;
  sizeBytes: number;
  created: string;
  lastPushed: string;
  readme: string;
  readmeFormat: string;
};

export type ImageVersion = { version: string; created: string };
export type ImageLayer = { digest: string; sizeBytes: number };

export type ContainerImage = {
  id: string;
  digest: string;
  name: string;
  state: string;
  repositoryId: string;
  repositoryName: string;
  versions: ImageVersion[];
  layers: ImageLayer[];
  sizeBytes: number;
  created: string;
  pullCount: number;
  lastPulled: string;
};

export type ImageSignature = {
  id: string;
  keyId: string;
  keyVersionId: string;
  algorithm: string;
  description: string;
  created: string;
};

export type ContainerConfig = { namespace: string; createdOnFirstPush: boolean };

export type GenericRepo = {
  id: string;
  name: string;
  description: string;
  isImmutable: boolean;
  created: string;
};

export type GenericArtifact = {
  id: string;
  path: string;
  version: string;
  sizeBytes: number;
  sha256: string;
  state: string;
  created: string;
};

export function parseRepo(raw: Json): ContainerRepo {
  const readme = obj(raw.readme);
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    isPublic: raw.isPublic === true,
    isImmutable: raw.isImmutable === true,
    imageCount: num(raw.imageCount),
    layerCount: num(raw.layerCount),
    sizeBytes: num(raw.layersSizeInBytes),
    created: str(raw.timeCreated),
    lastPushed: str(raw.timeLastPushed),
    readme: str(readme.content),
    readmeFormat: str(readme.format),
  };
}

export function parseImage(raw: Json): ContainerImage {
  return {
    id: str(raw.id),
    digest: str(raw.digest),
    name: str(raw.displayName),
    state: str(raw.lifecycleState),
    repositoryId: str(raw.repositoryId),
    repositoryName: str(raw.repositoryName),
    versions: list(raw.versions).map((v) => ({
      version: str(v.version),
      created: str(v.timeCreated),
    })),
    layers: list(raw.layers).map((l) => ({
      digest: str(l.digest),
      sizeBytes: num(l.sizeInBytes),
    })),
    sizeBytes: num(raw.layersSizeInBytes),
    created: str(raw.timeCreated),
    pullCount: num(raw.pullCount),
    lastPulled: str(raw.timeLastPulled),
  };
}

/** Version tags of an image, in push order. */
export function imageTags(img: Pick<ContainerImage, "versions">): string[] {
  return img.versions.map((v) => v.version);
}

export function parseSignature(raw: Json): ImageSignature {
  return {
    id: str(raw.id),
    keyId: str(raw.kmsKeyId),
    keyVersionId: str(raw.kmsKeyVersionId),
    algorithm: str(raw.signingAlgorithm),
    description: str(raw.displayName),
    created: str(raw.timeCreated),
  };
}

export function parseGenericRepo(raw: Json): GenericRepo {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    description: str(raw.description),
    isImmutable: raw.isImmutable === true,
    created: str(raw.timeCreated),
  };
}

export function parseArtifact(raw: Json): GenericArtifact {
  return {
    id: str(raw.id),
    path: str(raw.artifactPath),
    version: str(raw.version),
    sizeBytes: num(raw.sizeInBytes),
    sha256: str(raw.sha256),
    state: str(raw.lifecycleState),
    created: str(raw.timeCreated),
  };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(ART_PROXY_BASE, fetchFn);

const compQuery = (compartment: string) => `compartmentId=${enc(compartment)}`;

/** Repositories of one compartment. The list answer omits isImmutable and the readme, so each is fetched. */
export async function listRepos(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<ContainerRepo[]> {
  const items = await ociListPages(
    ART_PROXY_BASE,
    "/api/containerRepositories",
    compQuery(compartment),
    "items",
    fetchFn,
  );
  return Promise.all(items.map((i) => getRepo(str(i.id), fetchFn)));
}

export async function getRepo(id: string, fetchFn: FetchFn = fetch): Promise<ContainerRepo> {
  return parseRepo(
    await ociGetOne(ART_PROXY_BASE, `/api/containerRepositories/${enc(id)}`, fetchFn),
  );
}

/** Images of one repository, optionally the soft-deleted ones. Full records are fetched for layers, versions and pulls. */
export async function listImages(
  compartment: string,
  repositoryId: string,
  deleted = false,
  fetchFn: FetchFn = fetch,
): Promise<ContainerImage[]> {
  const q = [
    compQuery(compartment),
    `repositoryId=${enc(repositoryId)}`,
    deleted ? "lifecycleState=DELETED" : "",
  ]
    .filter(Boolean)
    .join("&");
  const items = await ociListPages(ART_PROXY_BASE, "/api/containerImages", q, "items", fetchFn);
  return Promise.all(
    items.map(async (i) =>
      parseImage(
        await ociGetOne(ART_PROXY_BASE, `/api/containerImages/${enc(str(i.id))}`, fetchFn),
      ),
    ),
  );
}

export async function listSignatures(
  compartment: string,
  imageId: string,
  fetchFn: FetchFn = fetch,
): Promise<ImageSignature[]> {
  const items = await ociListPages(
    ART_PROXY_BASE,
    "/api/containerImageSignatures",
    `${compQuery(compartment)}&imageId=${enc(imageId)}`,
    "items",
    fetchFn,
  );
  return items.map((i) => parseSignature(i));
}

export async function getConfig(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<ContainerConfig> {
  const raw = await ociGetOne(
    ART_PROXY_BASE,
    `/api/containerConfiguration?${compQuery(compartment)}`,
    fetchFn,
  );
  return {
    namespace: str(raw.namespace),
    createdOnFirstPush: raw.isRepositoryCreatedOnFirstPush === true,
  };
}

export async function setCreatedOnFirstPush(
  compartment: string,
  value: boolean,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    ART_PROXY_BASE,
    "PUT",
    `/api/containerConfiguration?${compQuery(compartment)}`,
    { isRepositoryCreatedOnFirstPush: value },
    fetchFn,
  );
}

export type NewRepo = {
  compartmentId: string;
  displayName: string;
  isPublic: boolean;
  isImmutable: boolean;
  readme: string;
};

export async function createRepo(r: NewRepo, fetchFn: FetchFn = fetch): Promise<void> {
  const { readme, ...rest } = r;
  await restCall(
    ART_PROXY_BASE,
    "POST",
    "/api/containerRepositories",
    readme.trim() === "" ? rest : { ...rest, readme: { content: readme, format: "TEXT_MARKDOWN" } },
    fetchFn,
  );
}

export type RepoChange = { isPublic?: boolean; isImmutable?: boolean; readme?: string };

export async function updateRepo(
  id: string,
  change: RepoChange,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const { readme, ...rest } = change;
  await restCall(
    ART_PROXY_BASE,
    "PUT",
    `/api/containerRepositories/${enc(id)}`,
    readme === undefined ? rest : { ...rest, readme: { content: readme, format: "TEXT_MARKDOWN" } },
    fetchFn,
  );
}

export async function deleteRepo(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(
    ART_PROXY_BASE,
    "DELETE",
    `/api/containerRepositories/${enc(id)}`,
    undefined,
    fetchFn,
  );
}

export async function deleteImage(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(ART_PROXY_BASE, "DELETE", `/api/containerImages/${enc(id)}`, undefined, fetchFn);
}

export async function restoreImage(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(
    ART_PROXY_BASE,
    "POST",
    `/api/containerImages/${enc(id)}/restore`,
    undefined,
    fetchFn,
  );
}

export async function listGenericRepos(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<GenericRepo[]> {
  const items = await ociListPages(
    ART_PROXY_BASE,
    "/api/genericRepositories",
    compQuery(compartment),
    "items",
    fetchFn,
  );
  return items.map((i) => parseGenericRepo(i));
}

export type NewGenericRepo = {
  compartmentId: string;
  displayName: string;
  description: string;
  isImmutable: boolean;
};

export async function createGenericRepo(
  r: NewGenericRepo,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(ART_PROXY_BASE, "POST", "/api/genericRepositories", r, fetchFn);
}

export async function deleteGenericRepo(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(
    ART_PROXY_BASE,
    "DELETE",
    `/api/genericRepositories/${enc(id)}`,
    undefined,
    fetchFn,
  );
}

export async function listArtifacts(
  compartment: string,
  repositoryId: string,
  fetchFn: FetchFn = fetch,
): Promise<GenericArtifact[]> {
  const items = await ociListPages(
    ART_PROXY_BASE,
    "/api/genericArtifacts",
    `${compQuery(compartment)}&repositoryId=${enc(repositoryId)}`,
    "items",
    fetchFn,
  );
  return items.map((i) => parseArtifact(i));
}

export async function deleteArtifact(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(ART_PROXY_BASE, "DELETE", `/api/genericArtifacts/${enc(id)}`, undefined, fetchFn);
}

/** Download URL served by the proxy as an attachment (size-capped, never rendered inline). */
export const artifactDownloadUrl = (id: string): string =>
  `${ART_PROXY_BASE}/api/genericArtifacts/${enc(id)}/content`;

export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v >= 10 ? 0 : 1)} ${units[i]}`;
}

export const shortDigest = (digest: string): string =>
  digest.startsWith("sha256:") ? `sha256:${digest.slice(7, 19)}` : digest.slice(0, 19);

export type RegistryHints = {
  host: string;
  repo: string;
  login: string;
  push: string;
  pull: string;
};

/** `docker` commands for a repository. No credential value is ever included: the password is an OCI auth token the user holds. */
export function registryHints(
  hostname: string,
  port: number | undefined,
  namespace: string,
  repoName: string,
  tag: string,
): RegistryHints {
  const host = port ? `${hostname}:${port}` : hostname;
  const repo = `${host}/${namespace}/${repoName}`;
  const t = tag || "latest";
  return {
    host,
    repo,
    login: `docker login ${host} -u ${namespace}/<username>  # password: your OCI auth token`,
    push: `docker tag <local-image> ${repo}:${t} && docker push ${repo}:${t}`,
    pull: `docker pull ${repo}:${t}`,
  };
}

export const validName = (v: string): boolean =>
  /^[a-z0-9]+([._-][a-z0-9]+)*(\/[a-z0-9]+([._-][a-z0-9]+)*)*$/.test(v.trim()) &&
  v.trim().length <= 255;

export const readStoredArtCompartment = () => readStoredValue(ART_COMPARTMENT_KEY);
export const writeStoredArtCompartment = (id: string) => writeStoredValue(ART_COMPARTMENT_KEY, id);
