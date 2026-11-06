/** Client for the Cloud Build emulator via the dashboard's in-process REST proxy. */

import {
  base64ToUtf8,
  listAll,
  restCall,
  shortName,
  str,
  type FetchFn,
  type Json,
} from "./gcprest.ts";

export { describeGcpError } from "./gcprest.ts";

export const CLOUDBUILD_PROXY_BASE = "/dashboard/api/v1/gcp/cloudbuild/v1";
export const CLOUDBUILD_PROJECT_KEY = "gopherstack_cloudbuild_project";
export const CLOUDBUILD_LOCATION_KEY = "gopherstack_cloudbuild_location";
export const DEFAULT_CLOUDBUILD_LOCATION = "global";

const enc = encodeURIComponent;
const DEFAULT_PROJECT = "gopherstack-local";
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type CbScope = { project: string; location: string };
export type BuildStep = {
  id: string;
  name: string;
  args: string[];
  status: string;
  startTime: string;
  endTime: string;
  exitCode: number | null;
};
export type BuildImage = { name: string; digest: string };
export type SecretRef = { name: string; kind: string };
export type PoolInfo = {
  name: string;
  id: string;
  state: string;
  machineType: string;
  diskSizeGb: string;
  createTime: string;
  raw: Json;
};
export type RunSource = {
  branch?: string;
  tag?: string;
  commit?: string;
  substitutions?: Record<string, string>;
};
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
  source: string;
  approval: string;
  approvalComment: string;
  resultImages: BuildImage[];
  stepOutputs: string[];
  artifacts: string[];
  secrets: SecretRef[];
  raw: Json;
};
export type TriggerInfo = {
  name: string;
  id: string;
  displayName: string;
  description: string;
  disabled: boolean;
  event: string;
  filter: string;
  includedFiles: string[];
  ignoredFiles: string[];
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
    startTime: str(obj(raw.timing).startTime),
    endTime: str(obj(raw.timing).endTime),
    exitCode: raw.exitCode === undefined ? null : Number(raw.exitCode),
  };
}

const strList = (v: unknown): string[] => (Array.isArray(v) ? (v as unknown[]).map(String) : []);

/** Where a build's source came from, in one short string. */
export function describeSource(raw: Json): string {
  const src = obj(raw.source);
  const storage = obj(src.storageSource);
  if (src.storageSource) return `gs://${str(storage.bucket)}/${str(storage.object)}`;
  const repo = obj(src.repoSource);
  if (src.repoSource) {
    const rev = str(repo.branchName) || str(repo.tagName) || str(repo.commitSha);
    return `${str(repo.repoName) || "repo"}${rev ? `@${rev}` : ""}`;
  }
  const git = obj(src.gitSource);
  if (src.gitSource) return `${str(git.url)}${git.revision ? `@${str(git.revision)}` : ""}`;
  if (src.connectedRepository) return str(obj(src.connectedRepository).repository);
  return "";
}

/** Milliseconds between two RFC 3339 timestamps; open-ended builds run until `now`. */
export function durationMs(start: string, end: string, now = Date.now()): number | null {
  if (!start) return null;
  const from = Date.parse(start);
  if (Number.isNaN(from)) return null;
  const to = end ? Date.parse(end) : now;
  return Number.isNaN(to) ? null : Math.max(0, to - from);
}

export function formatDuration(ms: number | null): string {
  if (ms === null) return "";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const sec = Math.round(ms / 1000);
  if (sec < 60) return `${sec}s`;
  return `${Math.floor(sec / 60)}m ${sec % 60}s`;
}

/** Secret names only: secretEnv keys, Secret Manager versions and inline KMS variables. */
export function listSecretRefs(raw: Json): SecretRef[] {
  const out: SecretRef[] = [];
  for (const s of Array.isArray(raw.secrets) ? (raw.secrets as Json[]) : []) {
    for (const k of Object.keys(obj(s.secretEnv))) out.push({ name: k, kind: "KMS-encrypted" });
  }
  const avail = obj(raw.availableSecrets);
  for (const s of Array.isArray(avail.secretManager) ? (avail.secretManager as Json[]) : []) {
    out.push({ name: str(s.env), kind: `Secret Manager ${str(s.versionName)}` });
  }
  for (const s of Array.isArray(avail.inline) ? (avail.inline as Json[]) : []) {
    for (const k of Object.keys(obj(s.envMap))) out.push({ name: k, kind: "KMS-encrypted" });
  }
  return out;
}

function listArtifacts(raw: Json): string[] {
  const a = obj(raw.artifacts);
  const out = strList(a.images).map((i) => `image ${i}`);
  const objs = obj(a.objects);
  if (a.objects) out.push(`${str(objs.location)} (${strList(objs.paths).join(", ")})`);
  for (const [key, label] of [
    ["mavenArtifacts", "maven"],
    ["pythonPackages", "python"],
    ["npmPackages", "npm"],
    ["goModules", "go"],
  ] as const) {
    const items = Array.isArray(a[key]) ? (a[key] as Json[]) : [];
    for (const m of items) out.push(`${label} ${str(m.repository)}`);
  }
  return out;
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
    source: describeSource(raw),
    approval: str(obj(raw.approval).state),
    approvalComment: str(obj(obj(raw.approval).result).comment),
    resultImages: (Array.isArray(obj(raw.results).images)
      ? (obj(raw.results).images as Json[])
      : []
    ).map((i) => ({ name: str(i.name), digest: str(i.digest) })),
    stepOutputs: strList(obj(raw.results).buildStepOutputs).map(
      (b) => base64ToUtf8(b) ?? "(binary)",
    ),
    artifacts: listArtifacts(raw),
    secrets: listSecretRefs(raw),
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
    filter: str(raw.filter),
    includedFiles: strList(raw.includedFiles),
    ignoredFiles: strList(raw.ignoredFiles),
    createTime: str(raw.createTime),
    raw,
  };
}

/** The newest build a trigger started, or null. */
export function lastRun(triggerId: string, builds: BuildInfo[]): BuildInfo | null {
  const mine = builds.filter((b) => b.triggerId === triggerId);
  return mine.toSorted((a, b) => b.createTime.localeCompare(a.createTime))[0] ?? null;
}

export function parsePool(raw: Json): PoolInfo {
  const name = str(raw.name);
  const cfg = obj(obj(raw.privatePoolV1Config).workerConfig);
  return {
    name,
    id: shortName(name) || str(raw.uid),
    state: str(raw.state) || "STATE_UNSPECIFIED",
    machineType: str(cfg.machineType),
    diskSizeGb: str(cfg.diskSizeGb),
    createTime: str(raw.createTime),
    raw,
  };
}

export async function listWorkerPools(s: CbScope, fetchFn: FetchFn = fetch): Promise<PoolInfo[]> {
  return (
    await listAll(CLOUDBUILD_PROXY_BASE, `${base(s)}/workerPools`, "workerPools", fetchFn)
  ).map((r) => parsePool(r));
}

export async function approveBuild(
  s: CbScope,
  id: string,
  approved: boolean,
  comment: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const approvalResult = { decision: approved ? "APPROVED" : "REJECTED", comment };
  await restCall(
    CLOUDBUILD_PROXY_BASE,
    "POST",
    `${base(s)}/builds/${enc(id)}:approve`,
    { approvalResult },
    fetchFn,
  );
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
  await runTriggerSource(s, triggerId, branch ? { branch } : {}, fetchFn);
}

/** Runs a trigger against an explicit branch, tag or commit with optional user substitutions. */
export async function runTriggerSource(
  s: CbScope,
  triggerId: string,
  src: RunSource,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const source: Json = {};
  if (src.branch) source.branchName = src.branch;
  else if (src.tag) source.tagName = src.tag;
  else if (src.commit) source.commitSha = src.commit;
  if (src.substitutions && Object.keys(src.substitutions).length > 0) {
    source.substitutions = src.substitutions;
  }
  const body = Object.keys(source).length > 0 ? { source } : {};
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

const SUB_KEY = /^_[A-Z0-9_]{1,64}$/;

/** Parses "_KEY=value" lines; error names the first bad line. */
export function parseSubstitutions(text: string): {
  values: Record<string, string>;
  error: string;
} {
  const values: Record<string, string> = {};
  for (const line of text.split("\n")) {
    if (!line.trim()) continue;
    const i = line.indexOf("=");
    const key = (i < 0 ? line : line.slice(0, i)).trim();
    if (i < 0 || !SUB_KEY.test(key)) {
      return { values: {}, error: `"${line.trim()}" must look like _KEY=value` };
    }
    values[key] = line.slice(i + 1);
  }
  return { values, error: "" };
}
