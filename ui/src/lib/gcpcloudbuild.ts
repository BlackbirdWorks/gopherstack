/** Client for the Cloud Build emulator via the dashboard's in-process REST proxy. */

import { listAll, restCall, shortName, str, type FetchFn, type Json } from "./gcprest.ts";

export const CLOUDBUILD_PROXY_BASE = "/dashboard/api/v1/gcp/cloudbuild/v1";
export const CLOUDBUILD_PROJECT_KEY = "gopherstack_cloudbuild_project";
export const CLOUDBUILD_LOCATION_KEY = "gopherstack_cloudbuild_location";
export const DEFAULT_CLOUDBUILD_LOCATION = "global";

const enc = encodeURIComponent;
const DEFAULT_PROJECT = "gopherstack-local";
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type CbScope = { project: string; location: string };
export type BuildStep = { id: string; name: string; args: string[]; status: string };
export type BuildInfo = {
  /** Full resource name, built from the scope when the server omits it. */
  name: string;
  id: string;
  status: string;
  createTime: string;
  startTime: string;
  finishTime: string;
  triggerId: string;
  logUrl: string;
  steps: BuildStep[];
  images: string[];
  statusDetail: string;
  raw: Json;
};
export type TriggerInfo = {
  name: string;
  id: string;
  displayName: string;
  description: string;
  disabled: boolean;
  event: string;
  createTime: string;
  raw: Json;
};
export type BuildLog = { available: boolean; log: string };

const base = (s: CbScope) => `/projects/${enc(s.project)}/locations/${enc(s.location)}`;

export function parseStep(raw: Json): BuildStep {
  return {
    id: str(raw.id),
    name: str(raw.name),
    args: Array.isArray(raw.args) ? (raw.args as unknown[]).map(String) : [],
    status: str(raw.status) || "PENDING",
  };
}

export function parseBuild(raw: Json, scope: CbScope): BuildInfo {
  const id = str(raw.id);
  return {
    name: str(raw.name) || `projects/${scope.project}/locations/${scope.location}/builds/${id}`,
    id,
    status: str(raw.status) || "STATUS_UNKNOWN",
    createTime: str(raw.createTime),
    startTime: str(raw.startTime),
    finishTime: str(raw.finishTime),
    triggerId: str(raw.buildTriggerId),
    logUrl: str(raw.logUrl),
    steps: list(raw.steps).map((s) => parseStep(s)),
    images: Array.isArray(raw.images) ? (raw.images as unknown[]).map(String) : [],
    statusDetail: str(raw.statusDetail),
    raw,
  };
}

/** Event summary of a trigger: the first populated event kind. */
export function describeTriggerEvent(raw: Json): string {
  if (raw.github) {
    const gh = obj(raw.github);
    const kind = gh.pullRequest ? "pull request" : "push";
    const ref =
      str(obj(gh.push).branch) || str(obj(gh.push).tag) || str(obj(gh.pullRequest).branch);
    return `GitHub ${kind}${ref ? ` ${ref}` : ""}`;
  }
  if (raw.pubsubConfig) return `Pub/Sub ${shortName(str(obj(raw.pubsubConfig).topic))}`;
  if (raw.webhookConfig) return "Webhook";
  if (raw.triggerTemplate) return `Repo ${str(obj(raw.triggerTemplate).repoName)}`;
  return "Manual";
}

export function parseTrigger(raw: Json): TriggerInfo {
  const id = str(raw.id);
  const displayName = str(raw.name);
  return {
    name: str(raw.resourceName) || id,
    id,
    displayName,
    description: str(raw.description),
    disabled: raw.disabled === true,
    event: describeTriggerEvent(raw),
    createTime: str(raw.createTime),
    raw,
  };
}

export async function listBuilds(s: CbScope, fetchFn: FetchFn = fetch): Promise<BuildInfo[]> {
  return (await listAll(CLOUDBUILD_PROXY_BASE, `${base(s)}/builds`, "builds", fetchFn)).map((r) =>
    parseBuild(r, s),
  );
}

export async function listTriggers(s: CbScope, fetchFn: FetchFn = fetch): Promise<TriggerInfo[]> {
  return (await listAll(CLOUDBUILD_PROXY_BASE, `${base(s)}/triggers`, "triggers", fetchFn)).map(
    (r) => parseTrigger(r),
  );
}

export async function getBuildLog(
  s: CbScope,
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<BuildLog> {
  const body = await restCall(
    CLOUDBUILD_PROXY_BASE,
    "GET",
    `${base(s)}/builds/${enc(id)}/log`,
    undefined,
    fetchFn,
  );
  return { available: body.available === true, log: str(body.log) };
}

export async function cancelBuild(s: CbScope, id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(CLOUDBUILD_PROXY_BASE, "POST", `${base(s)}/builds/${enc(id)}:cancel`, {}, fetchFn);
}

export async function retryBuild(s: CbScope, id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(CLOUDBUILD_PROXY_BASE, "POST", `${base(s)}/builds/${enc(id)}:retry`, {}, fetchFn);
}

export async function runTrigger(
  s: CbScope,
  triggerId: string,
  branch: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const body = branch ? { source: { branchName: branch } } : {};
  await restCall(
    CLOUDBUILD_PROXY_BASE,
    "POST",
    `${base(s)}/triggers/${enc(triggerId)}:run`,
    body,
    fetchFn,
  );
}

export async function deleteTrigger(
  s: CbScope,
  triggerId: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    CLOUDBUILD_PROXY_BASE,
    "DELETE",
    `${base(s)}/triggers/${enc(triggerId)}`,
    undefined,
    fetchFn,
  );
}

/** Statuses a build can still leave: cancel only applies to these. */
export const ACTIVE_BUILD_STATUSES: ReadonlySet<string> = new Set(["QUEUED", "PENDING", "WORKING"]);

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredCloudBuildProject = () =>
  readStored(CLOUDBUILD_PROJECT_KEY, DEFAULT_PROJECT);
export const readStoredCloudBuildLocation = () =>
  readStored(CLOUDBUILD_LOCATION_KEY, DEFAULT_CLOUDBUILD_LOCATION);

export function writeStoredCloudBuildScope(project: string, location: string): void {
  try {
    localStorage.setItem(CLOUDBUILD_PROJECT_KEY, project);
    localStorage.setItem(CLOUDBUILD_LOCATION_KEY, location);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
