/** Client for the Google Cloud Batch emulator (jobs, task groups, tasks) via the dashboard's in-process proxy. */

import { listAll, restCall, shortName, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const BATCH_PROXY_BASE = "/dashboard/api/v1/gcp/batch/v1";
export const BATCH_PROJECT_KEY = "gopherstack_batch_project";
export const BATCH_ALL_LOCATIONS = "-";
export const BATCH_LOG_ID = "batch_task_logs";
export const BATCH_LOCATIONS = [
  "us-central1",
  "us-east1",
  "us-east4",
  "us-west1",
  "us-west2",
  "europe-west1",
  "europe-west2",
  "europe-west4",
  "asia-northeast1",
  "asia-southeast1",
  "australia-southeast1",
];

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

export type BatchEvent = {
  type: string;
  description: string;
  time: string;
  exitCode: number | null;
};

export type BatchRunnable = {
  kind: "container" | "script" | "barrier";
  image: string;
  commands: string[];
  scriptText: string;
  scriptPath: string;
  background: boolean;
  alwaysRun: boolean;
  variables: Record<string, string>;
  secretNames: string[];
};

export type BatchTaskGroup = {
  name: string;
  taskCount: number;
  parallelism: number;
  maxRetryCount: number;
  maxRunDuration: string;
  runnables: BatchRunnable[];
  variables: Record<string, string>;
  secretNames: string[];
};

export type BatchJob = {
  name: string;
  id: string;
  project: string;
  location: string;
  state: string;
  createTime: string;
  runDuration: string;
  labels: Record<string, string>;
  taskCount: number;
  counts: Record<string, number>;
  events: BatchEvent[];
  groups: BatchTaskGroup[];
};

export type BatchTask = {
  name: string;
  index: string;
  state: string;
  exitCode: number | null;
  attempts: number;
  events: BatchEvent[];
};

export type NewJob = {
  id: string;
  image: string;
  commands: string;
  taskCount: number;
  parallelism: number;
  env: { name: string; value: string }[];
};

export function parseEvent(raw: Json): BatchEvent {
  const exec = obj(raw.taskExecution);
  return {
    type: str(raw.type),
    description: str(raw.description),
    time: str(raw.eventTime),
    // protojson omits a zero exit code but keeps the empty taskExecution
    exitCode: raw.taskExecution ? num(exec.exitCode) : null,
  };
}

function parseRunnable(raw: Json): BatchRunnable {
  const box = obj(raw.container);
  const script = obj(raw.script);
  const env = obj(raw.environment);
  const kind = raw.container ? "container" : raw.script ? "script" : "barrier";
  return {
    kind,
    image: str(box.imageUri),
    commands: Array.isArray(box.commands) ? box.commands.map(String) : [],
    scriptText: str(script.text),
    scriptPath: str(script.path),
    background: raw.background === true,
    alwaysRun: raw.alwaysRun === true,
    variables: strMap(env.variables),
    secretNames: secretNames(env.secretVariables),
  };
}

/** Variable names only: a secret variable's value never reaches the page. */
export function secretNames(v: unknown): string[] {
  return Object.keys(obj(v)).toSorted();
}

function parseGroup(raw: Json): BatchTaskGroup {
  const spec = obj(raw.taskSpec);
  const env = obj(spec.environment);
  return {
    name: str(raw.name),
    taskCount: num(raw.taskCount),
    parallelism: num(raw.parallelism),
    maxRetryCount: num(spec.maxRetryCount),
    maxRunDuration: str(spec.maxRunDuration),
    runnables: list(spec.runnables).map((r) => parseRunnable(r)),
    variables: strMap(env.variables),
    secretNames: secretNames(env.secretVariables),
  };
}

export function parseJob(raw: Json): BatchJob {
  const name = str(raw.name);
  const parts = name.split("/");
  const status = obj(raw.status);
  const groups = list(raw.taskGroups).map((g) => parseGroup(g));
  const counts: Record<string, number> = {};
  for (const g of Object.values(obj(status.taskGroups))) {
    for (const [k, v] of Object.entries(obj(obj(g).counts))) counts[k] = (counts[k] ?? 0) + num(v);
  }
  return {
    name,
    id: shortName(name),
    project: parts[1] ?? "",
    location: parts[3] ?? "",
    state: str(status.state),
    createTime: str(raw.createTime),
    runDuration: str(status.runDuration),
    labels: strMap(raw.labels),
    taskCount: groups.reduce((n, g) => n + g.taskCount, 0),
    counts,
    events: list(status.statusEvents).map((e) => parseEvent(e)),
    groups,
  };
}

export function parseTask(raw: Json): BatchTask {
  const name = str(raw.name);
  const status = obj(raw.status);
  const events = list(status.statusEvents).map((e) => parseEvent(e));
  const exit = events.findLast((e) => e.exitCode !== null)?.exitCode ?? null;
  const retries = events.filter((e) => e.type === "TASK_RETRY").length;
  const started = events.some((e) => e.description.endsWith("to RUNNING"));
  return {
    name,
    index: shortName(name),
    state: str(status.state),
    exitCode: exit,
    attempts: started ? retries + 1 : retries,
    events,
  };
}

const parent = (project: string, location: string) =>
  `/projects/${enc(project)}/locations/${enc(location)}`;

export async function listProjects(fetchFn: FetchFn = fetch): Promise<string[]> {
  const body = await restCall(BATCH_PROXY_BASE, "GET", "/projects", undefined, fetchFn);
  return Array.isArray(body.projects) ? body.projects.map(String) : [];
}

export async function listJobs(
  project: string,
  location: string,
  fetchFn: FetchFn = fetch,
): Promise<BatchJob[]> {
  return (
    await listAll(BATCH_PROXY_BASE, `${parent(project, location)}/jobs`, "jobs", fetchFn)
  ).map((j) => parseJob(j));
}

export async function getJob(name: string, fetchFn: FetchFn = fetch): Promise<BatchJob> {
  return parseJob(await restCall(BATCH_PROXY_BASE, "GET", `/${name}`, undefined, fetchFn));
}

export async function listTasks(jobName: string, fetchFn: FetchFn = fetch): Promise<BatchTask[]> {
  const tasks = await listAll(
    BATCH_PROXY_BASE,
    `/${jobName}/taskGroups/group0/tasks`,
    "tasks",
    fetchFn,
  );
  return tasks.map((t) => parseTask(t));
}

export function buildJobBody(job: NewJob): Json {
  const variables = Object.fromEntries(
    job.env.filter((e) => e.name.trim()).map((e) => [e.name.trim(), e.value]),
  );
  const commands = job.commands.split("\n").filter((c) => c.trim() !== "");
  return {
    taskGroups: [
      {
        taskCount: String(job.taskCount),
        parallelism: String(job.parallelism),
        taskSpec: {
          runnables: [{ container: { imageUri: job.image.trim(), commands } }],
          environment: { variables },
        },
      },
    ],
  };
}

export async function createJob(
  project: string,
  location: string,
  job: NewJob,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    BATCH_PROXY_BASE,
    "POST",
    `${parent(project, location)}/jobs?jobId=${enc(job.id)}`,
    buildJobBody(job),
    fetchFn,
  );
}

export async function cancelJob(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(BATCH_PROXY_BASE, "POST", `/${name}:cancel`, {}, fetchFn);
}

export async function deleteJob(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(BATCH_PROXY_BASE, "DELETE", `/${name}`, undefined, fetchFn);
}

/** Valid job ids: lowercase letters, digits and dashes, starting with a letter. */
export const validJobId = (v: string): boolean => /^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$/.test(v);

export const validEnvName = (v: string): boolean => /^[A-Za-z_][A-Za-z0-9_]*$/.test(v);

const ACTIVE = new Set(["QUEUED", "SCHEDULED", "RUNNING"]);
const BUSY = new Set([...ACTIVE, "CANCELLATION_IN_PROGRESS", "DELETION_IN_PROGRESS"]);

export const canCancel = (j: { state: string }): boolean => ACTIVE.has(j.state);
export const isSettled = (j: { state: string }): boolean => !BUSY.has(j.state);

/** Tailwind classes for a job or task state badge. */
export function statePill(s: string): string {
  if (s === "SUCCEEDED")
    return "bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300";
  if (s === "FAILED" || s === "CANCELLED")
    return "bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300";
  if (s === "RUNNING") return "bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300";
  return "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300";
}

/** Seconds of a protobuf Duration ("12.5s") as a short label. */
export function formatDuration(d: string): string {
  if (!d.endsWith("s")) return "—";
  const s = Number(d.slice(0, -1));
  if (!Number.isFinite(s)) return "—";
  if (s < 60) return `${Math.round(s * 10) / 10}s`;
  const m = Math.floor(s / 60);
  return `${m}m ${Math.round(s % 60)}s`;
}

/** Cloud Logging filter for one job's task logs. */
export const jobLogFilter = (project: string, job: string): string =>
  `logName="projects/${project}/logs/${BATCH_LOG_ID}" AND resource.labels.job="${job}"`;

export function readStoredProject(): string {
  try {
    return localStorage.getItem(BATCH_PROJECT_KEY) ?? "";
  } catch {
    return "";
  }
}

export function writeStoredProject(project: string): void {
  try {
    localStorage.setItem(BATCH_PROJECT_KEY, project);
  } catch {
    // storage unavailable: the choice just is not remembered
  }
}
