/** Client for the Cloud Run Admin API emulator via the dashboard's in-process REST proxy. */

import { restCall, shortName, str, type FetchFn, type Json } from "./gcprest.ts";

export const RUN_PROXY_BASE = "/dashboard/api/v1/gcp/run/v1";
export const RUN_PROJECT_KEY = "gopherstack_run_project";
export const RUN_LOCATION_KEY = "gopherstack_run_location";
export const DEFAULT_RUN_PROJECT = "gopherstack-local";
export const DEFAULT_RUN_LOCATION = "us-central1";
const PAGE = 100;
const MAX_RETRIES_LIMIT = 10;
const LATEST = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST";
const REVISION = "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION";
const NO_RUNTIME = /container runtime is unavailable/i;

export type RunScope = { project: string; location: string };
export type EnvVar = { name: string; value: string };
export type CondState = "ok" | "failed" | "pending";
export type Condition = { type: string; state: CondState; message: string };
export type TrafficShare = { revision: string; percent: number; latest: boolean; tag: string };

export type ServiceInfo = {
  name: string;
  id: string;
  uri: string;
  latestReady: string;
  latestCreated: string;
  image: string;
  port: number | null;
  env: EnvVar[];
  traffic: TrafficShare[];
  conditions: Condition[];
  state: CondState;
  template: Json;
};

export type RevisionInfo = {
  name: string;
  id: string;
  image: string;
  createTime: string;
  conditions: Condition[];
  state: CondState;
};

export type JobInfo = {
  name: string;
  id: string;
  image: string;
  taskCount: number;
  parallelism: number;
  maxRetries: number;
  executionCount: number;
  conditions: Condition[];
};

export type ExecutionState = "running" | "succeeded" | "failed" | "cancelled";
export type ExecutionInfo = {
  name: string;
  id: string;
  taskCount: number;
  running: number;
  succeeded: number;
  failed: number;
  cancelled: number;
  retried: number;
  state: ExecutionState;
  createTime: string;
  completionTime: string;
  message: string;
};

export type TaskState = "pending" | "running" | "succeeded" | "failed" | "cancelled";
export type TaskInfo = {
  name: string;
  id: string;
  index: number;
  state: TaskState;
  exitCode: number | null;
  message: string;
  retried: number;
  startTime: string;
  completionTime: string;
};

export type NewService = { id: string; image: string; port: string; env: EnvVar[] };
export type NewJob = {
  id: string;
  image: string;
  taskCount: string;
  parallelism: string;
  maxRetries: string;
  command: string;
  args: string;
  env: EnvVar[];
};
export type TrafficInput = { revision: string; latest: boolean; percent: number };

const enc = encodeURIComponent;
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const num = (v: unknown): number => (typeof v === "number" ? v : 0);
const locPath = (s: RunScope) => `/projects/${enc(s.project)}/locations/${enc(s.location)}`;
const servicePath = (s: RunScope, id: string) => `${locPath(s)}/services/${enc(id)}`;
const jobPath = (s: RunScope, id: string) => `${locPath(s)}/jobs/${enc(id)}`;

export function validateRunId(id: string): string | null {
  if (!id) return "name is required";
  if (!/^[a-z]([-a-z0-9]{0,47}[a-z0-9])?$/.test(id)) {
    return "lowercase letters, digits and -, starting with a letter (max 49)";
  }
  return null;
}

export function validateImage(image: string): string | null {
  if (!image.trim()) return "container image is required";
  return /\s/.test(image.trim()) ? "image must not contain spaces" : null;
}

export function validatePort(port: string): string | null {
  if (!port.trim()) return null;
  const n = Number(port);
  return Number.isInteger(n) && n >= 1 && n <= 65535 ? null : "port must be 1-65535";
}

function validateCount(v: string, label: string, min: number, max: number): string | null {
  if (!v.trim()) return null;
  const n = Number(v);
  if (!Number.isInteger(n) || n < min || n > max) return `${label} must be ${min}-${max}`;
  return null;
}

export const validateTaskCount = (v: string) => validateCount(v, "task count", 1, 10000);
export const validateParallelism = (v: string) => validateCount(v, "parallelism", 0, 10000);
export const validateMaxRetries = (v: string) =>
  validateCount(v, "max retries", 0, MAX_RETRIES_LIMIT);

export function validateEnvName(name: string): string | null {
  if (!name) return "name is required";
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? null : "not a valid variable name";
}

export function validateTraffic(rows: TrafficInput[]): string | null {
  const sum = rows.reduce((a, r) => a + r.percent, 0);
  if (rows.some((r) => !Number.isInteger(r.percent) || r.percent < 0 || r.percent > 100)) {
    return "each percentage must be a whole number from 0 to 100";
  }
  return sum === 100 ? null : `percentages sum to ${sum}, expected 100`;
}

function parseState(raw: unknown): CondState {
  if (raw === "CONDITION_SUCCEEDED") return "ok";
  return raw === "CONDITION_FAILED" ? "failed" : "pending";
}

export function parseConditions(raw: unknown): Condition[] {
  return list(raw).map((c) => ({
    type: str(c.type),
    state: parseState(c.state),
    message: str(c.message),
  }));
}

function overall(conds: Condition[]): CondState {
  const ready = conds.find((c) => c.type === "Ready") ?? conds[0];
  return ready ? ready.state : "pending";
}

function firstContainer(template: unknown): Json {
  return list(obj(template).containers)[0] ?? {};
}

function parseEnv(raw: unknown): EnvVar[] {
  return list(raw)
    .filter((e) => typeof e.name === "string")
    .map((e) => ({ name: str(e.name), value: str(e.value) }));
}

function parsePort(c: Json): number | null {
  const p = list(c.ports)[0];
  return p && typeof p.containerPort === "number" ? p.containerPort : null;
}

function parseTraffic(raw: unknown): TrafficShare[] {
  return list(raw).map((t) => ({
    revision: str(t.revision),
    percent: num(t.percent),
    latest: t.type === LATEST,
    tag: str(t.tag),
  }));
}

export function parseService(raw: Json): ServiceInfo {
  const name = str(raw.name);
  const template = obj(raw.template);
  const c = firstContainer(template);
  const conditions = parseConditions(raw.conditions);
  return {
    name,
    id: shortName(name),
    uri: str(raw.uri),
    latestReady: shortName(str(raw.latestReadyRevision)),
    latestCreated: shortName(str(raw.latestCreatedRevision)),
    image: str(c.image),
    port: parsePort(c),
    env: parseEnv(c.env),
    traffic: parseTraffic(raw.trafficStatuses),
    conditions,
    state: overall(conditions),
    template,
  };
}

export function parseRevision(raw: Json): RevisionInfo {
  const name = str(raw.name);
  const conditions = parseConditions(raw.conditions);
  return {
    name,
    id: shortName(name),
    image: str(firstContainer(raw).image),
    createTime: str(raw.createTime),
    conditions,
    state: overall(conditions),
  };
}

export function parseJob(raw: Json): JobInfo {
  const name = str(raw.name);
  const et = obj(raw.template);
  const tt = obj(et.template);
  return {
    name,
    id: shortName(name),
    image: str(firstContainer(tt).image),
    taskCount: num(et.taskCount),
    parallelism: num(et.parallelism),
    maxRetries: num(tt.maxRetries),
    executionCount: num(raw.executionCount),
    conditions: parseConditions(raw.conditions),
  };
}

function executionState(raw: Json, completed: Condition | undefined): ExecutionState {
  if (!str(raw.completionTime)) return "running";
  if (completed?.state !== "failed") return "succeeded";
  return num(raw.cancelledCount) > 0 && num(raw.failedCount) === 0 ? "cancelled" : "failed";
}

export function parseExecution(raw: Json): ExecutionInfo {
  const name = str(raw.name);
  const completed = parseConditions(raw.conditions).find((c) => c.type === "Completed");
  return {
    name,
    id: shortName(name),
    taskCount: num(raw.taskCount),
    running: num(raw.runningCount),
    succeeded: num(raw.succeededCount),
    failed: num(raw.failedCount),
    cancelled: num(raw.cancelledCount),
    retried: num(raw.retriedCount),
    state: executionState(raw, completed),
    createTime: str(raw.createTime),
    completionTime: str(raw.completionTime),
    message: completed?.message ?? "",
  };
}

function taskState(raw: Json, completed: Condition | undefined): TaskState {
  if (str(raw.completionTime)) {
    if (completed?.state !== "failed") return "succeeded";
    return /cancel/i.test(completed.message) ? "cancelled" : "failed";
  }
  return str(raw.startTime) ? "running" : "pending";
}

export function parseTask(raw: Json): TaskInfo {
  const name = str(raw.name);
  const completed = parseConditions(raw.conditions).find((c) => c.type === "Completed");
  const result = raw.lastAttemptResult ? obj(raw.lastAttemptResult) : null;
  const state = taskState(raw, completed);
  const done = state === "succeeded" || state === "failed" || state === "cancelled";
  return {
    name,
    id: shortName(name),
    index: num(raw.index),
    state,
    exitCode: done ? num(result?.exitCode) : null,
    message: str(obj(result?.status).message) || completed?.message || "",
    retried: num(raw.retried),
    startTime: str(raw.startTime),
    completionTime: str(raw.completionTime),
  };
}

/** True when any message reports that the container runtime is missing. */
export function runtimeUnavailable(texts: Iterable<string>): boolean {
  for (const t of texts) if (NO_RUNTIME.test(t)) return true;
  return false;
}

export const serviceMessages = (s: ServiceInfo): string[] => s.conditions.map((c) => c.message);
export const taskMessages = (t: TaskInfo): string[] => [t.message];

export const rowsToEnv = (rows: { key: string; value: string }[]): EnvVar[] =>
  rows.filter((r) => r.key).map((r) => ({ name: r.key, value: r.value }));
export const envToRows = (env: EnvVar[]): { key: string; value: string }[] =>
  env.map((e) => ({ key: e.name, value: e.value }));

/** Newest revision first, falling back to name order. */
export const newestFirst = (revs: RevisionInfo[]): RevisionInfo[] =>
  revs.toSorted((a, b) => b.createTime.localeCompare(a.createTime) || b.id.localeCompare(a.id));

/** One editable row per revision plus the "latest" row, seeded from the service's current split. */
export function trafficRows(
  svc: ServiceInfo,
  revs: RevisionInfo[],
): (TrafficInput & { label: string })[] {
  const pct = (match: (t: TrafficShare) => boolean) =>
    svc.traffic.filter(match).reduce((a, t) => a + t.percent, 0);
  const rows = newestFirst(revs).map((r) => ({
    revision: r.id,
    latest: false,
    percent: pct((t) => !t.latest && t.revision === r.id),
    label: r.id,
  }));
  return [
    { revision: "", latest: true, percent: pct((t) => t.latest), label: "Latest revision" },
    ...rows,
  ];
}

function splitLines(v: string): string[] {
  return v
    .split("\n")
    .map((l) => l.trim())
    .filter(Boolean);
}

function envBody(env: EnvVar[]): Json[] {
  return env.filter((e) => e.name).map((e) => ({ name: e.name, value: e.value }));
}

function containerBody(image: string, env: EnvVar[]): Json {
  const c: Json = { image: image.trim() };
  if (env.some((e) => e.name)) c.env = envBody(env);
  return c;
}

export function serviceBody(s: NewService): Json {
  const c = containerBody(s.image, s.env);
  if (s.port.trim()) c.ports = [{ containerPort: Number(s.port) }];
  return { template: { containers: [c] } };
}

/** Full template of svc with the first container's image and env replaced; revision name dropped so a new one is cut. */
export function redeployTemplate(svc: ServiceInfo, image: string, env: EnvVar[]): Json {
  const template = JSON.parse(JSON.stringify(svc.template)) as Json;
  delete template.revision;
  const containers = list(template.containers);
  const first: Json = { ...containers[0], image: image.trim() };
  delete first.env;
  if (env.some((e) => e.name)) first.env = envBody(env);
  template.containers = [first, ...containers.slice(1)];
  return template;
}

export function jobBody(j: NewJob): Json {
  const c = containerBody(j.image, j.env);
  const command = splitLines(j.command);
  const args = splitLines(j.args);
  if (command.length > 0) c.command = command;
  if (args.length > 0) c.args = args;
  const inner: Json = { containers: [c] };
  if (j.maxRetries.trim()) inner.maxRetries = Number(j.maxRetries);
  const body: Json = { template: { template: inner } };
  const tpl = body.template as Json;
  if (j.taskCount.trim()) tpl.taskCount = Number(j.taskCount);
  if (j.parallelism.trim()) tpl.parallelism = Number(j.parallelism);
  return body;
}

export function trafficBody(rows: TrafficInput[]): Json {
  const traffic = rows
    .filter((r) => r.percent > 0 || r.latest)
    .map((r) =>
      r.latest
        ? { type: LATEST, percent: r.percent }
        : { type: REVISION, revision: r.revision, percent: r.percent },
    );
  return { traffic };
}

async function listAll<T>(
  path: string,
  key: string,
  parse: (r: Json) => T,
  fetchFn: FetchFn,
): Promise<T[]> {
  const out: T[] = [];
  let token = "";
  do {
    const sep = path.includes("?") ? "&" : "?";
    const url = `${path}${sep}pageSize=${PAGE}${token ? `&pageToken=${enc(token)}` : ""}`;
    const body = await restCall<Json>(RUN_PROXY_BASE, "GET", url, undefined, fetchFn);
    out.push(...list(body[key]).map((r) => parse(r)));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export const listServices = (s: RunScope, f: FetchFn = fetch) =>
  listAll(`${locPath(s)}/services`, "services", parseService, f);
export const listRevisions = (s: RunScope, svc: string, f: FetchFn = fetch) =>
  listAll(`${servicePath(s, svc)}/revisions`, "revisions", parseRevision, f);
export const listJobs = (s: RunScope, f: FetchFn = fetch) =>
  listAll(`${locPath(s)}/jobs`, "jobs", parseJob, f);
export const listExecutions = (s: RunScope, job: string, f: FetchFn = fetch) =>
  listAll(`${jobPath(s, job)}/executions`, "executions", parseExecution, f);
export const listTasks = (s: RunScope, job: string, exec: string, f: FetchFn = fetch) =>
  listAll(`${jobPath(s, job)}/executions/${enc(exec)}/tasks`, "tasks", parseTask, f);

async function call(method: string, path: string, body?: unknown, f: FetchFn = fetch) {
  await restCall(RUN_PROXY_BASE, method, path, body, f);
}

export const createService = (s: RunScope, n: NewService, f: FetchFn = fetch) =>
  call("POST", `${locPath(s)}/services?serviceId=${enc(n.id)}`, serviceBody(n), f);

export const deployRevision = (
  s: RunScope,
  svc: ServiceInfo,
  image: string,
  env: EnvVar[],
  f: FetchFn = fetch,
) =>
  call(
    "PATCH",
    `${servicePath(s, svc.id)}?updateMask=template`,
    { template: redeployTemplate(svc, image, env) },
    f,
  );

export const setTraffic = (s: RunScope, svc: string, rows: TrafficInput[], f: FetchFn = fetch) =>
  call("PATCH", `${servicePath(s, svc)}?updateMask=traffic`, trafficBody(rows), f);

export const deleteService = (s: RunScope, id: string, f: FetchFn = fetch) =>
  call("DELETE", servicePath(s, id), undefined, f);

export const deleteRevision = (s: RunScope, svc: string, rev: string, f: FetchFn = fetch) =>
  call("DELETE", `${servicePath(s, svc)}/revisions/${enc(rev)}`, undefined, f);

export const createJob = (s: RunScope, n: NewJob, f: FetchFn = fetch) =>
  call("POST", `${locPath(s)}/jobs?jobId=${enc(n.id)}`, jobBody(n), f);

export const runJob = (s: RunScope, id: string, f: FetchFn = fetch) =>
  call("POST", `${jobPath(s, id)}:run`, {}, f);

export const deleteJob = (s: RunScope, id: string, f: FetchFn = fetch) =>
  call("DELETE", jobPath(s, id), undefined, f);

export const cancelExecution = (s: RunScope, job: string, exec: string, f: FetchFn = fetch) =>
  call("POST", `${jobPath(s, job)}/executions/${enc(exec)}:cancel`, {}, f);

export const deleteExecution = (s: RunScope, job: string, exec: string, f: FetchFn = fetch) =>
  call("DELETE", `${jobPath(s, job)}/executions/${enc(exec)}`, undefined, f);

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredRunProject = () => readStored(RUN_PROJECT_KEY, DEFAULT_RUN_PROJECT);
export const readStoredRunLocation = () => readStored(RUN_LOCATION_KEY, DEFAULT_RUN_LOCATION);

export function writeStoredRunScope(project: string, location: string): void {
  try {
    localStorage.setItem(RUN_PROJECT_KEY, project);
    localStorage.setItem(RUN_LOCATION_KEY, location);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
