/** Client for the Cloud Tasks emulator via the dashboard's in-process REST proxy. */

import {
  base64ToUtf8,
  durationToSeconds,
  restCall,
  shortName,
  str,
  strMap,
  utf8ToBase64,
  type FetchFn,
  type Json,
} from "./gcprest.ts";

export const TASKS_PROXY_BASE = "/dashboard/api/v1/gcp/cloudtasks/v1";
export const TASKS_PROJECT_KEY = "gopherstack_cloudtasks_project";
export const TASKS_LOCATION_KEY = "gopherstack_cloudtasks_location";
export const DEFAULT_TASKS_PROJECT = "gopherstack-local";
export const DEFAULT_TASKS_LOCATION = "us-central1";
const PAGE = 100;

export const HTTP_METHODS = [
  "POST",
  "GET",
  "PUT",
  "PATCH",
  "DELETE",
  "HEAD",
  "OPTIONS",
] as const;
export type HttpMethod = (typeof HTTP_METHODS)[number];

export type QueueInfo = {
  name: string;
  id: string;
  state: string;
  maxDispatchesPerSecond: number | null;
  maxBurstSize: number | null;
  maxConcurrentDispatches: number | null;
  maxAttempts: number | null;
  maxRetryDuration: string;
  minBackoff: string;
  maxBackoff: string;
  maxDoublings: number | null;
  purgeTime: string;
};

export type AttemptInfo = {
  dispatchTime: string;
  responseTime: string;
  code: number | null;
  message: string;
};

export type TaskInfo = {
  name: string;
  id: string;
  url: string;
  method: string;
  headers: Record<string, string>;
  body: string | null;
  scheduleTime: string;
  createTime: string;
  dispatchCount: number;
  responseCount: number;
  lastAttempt: AttemptInfo | null;
};

export type QueueAction = "pause" | "resume" | "purge";

export type QueueScope = { project: string; location: string };
export type NewQueue = {
  id: string;
  maxDispatchesPerSecond: string;
  maxConcurrentDispatches: string;
  maxAttempts: string;
};
export type NewTask = {
  id: string;
  url: string;
  method: HttpMethod;
  headers: Record<string, string>;
  body: string;
  delaySeconds: string;
};

const enc = encodeURIComponent;
const locationPath = (p: string, l: string) =>
  `/projects/${enc(p)}/locations/${enc(l)}`;
const queuesPath = (p: string, l: string) => `${locationPath(p, l)}/queues`;
const queuePath = (p: string, l: string, q: string) =>
  `${queuesPath(p, l)}/${enc(q)}`;
const taskPath = (p: string, l: string, q: string, t: string) =>
  `${queuePath(p, l, q)}/tasks/${enc(t)}`;

export function validateQueueId(id: string): string | null {
  if (!id) return "name is required";
  if (!/^[A-Za-z0-9-]{1,100}$/.test(id))
    return "use letters, digits and - (max 100)";
  return null;
}

export function validateTaskId(id: string): string | null {
  if (!id) return null;
  if (!/^[A-Za-z0-9_-]{1,500}$/.test(id))
    return "use letters, digits, - and _ (max 500)";
  return null;
}

export function validateTaskUrl(url: string): string | null {
  if (!url) return "URL is required";
  try {
    const u = new URL(url);
    if (u.protocol !== "http:" && u.protocol !== "https:")
      return "URL must be http or https";
  } catch {
    return "enter an absolute URL such as http://localhost:9000/hook";
  }
  return null;
}

export function validateHeaderKey(key: string): string | null {
  if (!key) return "key is required";
  if (!/^[A-Za-z0-9!#$%&'*+.^_`|~-]+$/.test(key))
    return "not a valid header name";
  return null;
}

/** Optional non-negative number field; null when blank, error string when invalid. */
export function optionalNumber(
  input: string,
  opts: { integer?: boolean; min?: number } = {},
): { value: number | null } | { error: string } {
  const t = input.trim();
  if (!t) return { value: null };
  const n = Number(t);
  const min = opts.min ?? 0;
  if (!Number.isFinite(n) || n < min)
    return { error: `must be a number >= ${min}` };
  if (opts.integer && !Number.isInteger(n))
    return { error: "must be a whole number" };
  return { value: n };
}

const num = (v: unknown): number | null => (typeof v === "number" ? v : null);
const obj = (v: unknown): Json =>
  v && typeof v === "object" ? (v as Json) : {};
const dur = (v: unknown): string =>
  typeof v === "string" && v ? durationToSeconds(v) + "s" : "";

export function parseQueue(raw: Json): QueueInfo {
  const name = str(raw.name);
  const rate = obj(raw.rateLimits);
  const retry = obj(raw.retryConfig);
  return {
    name,
    id: shortName(name),
    state: str(raw.state) || "RUNNING",
    maxDispatchesPerSecond: num(rate.maxDispatchesPerSecond),
    maxBurstSize: num(rate.maxBurstSize),
    maxConcurrentDispatches: num(rate.maxConcurrentDispatches),
    maxAttempts: num(retry.maxAttempts),
    maxRetryDuration: dur(retry.maxRetryDuration),
    minBackoff: dur(retry.minBackoff),
    maxBackoff: dur(retry.maxBackoff),
    maxDoublings: num(retry.maxDoublings),
    purgeTime: str(raw.purgeTime),
  };
}

export function parseAttempt(raw: unknown): AttemptInfo | null {
  if (!raw || typeof raw !== "object") return null;
  const a = raw as Json;
  const status = a.responseStatus ? obj(a.responseStatus) : null;
  return {
    dispatchTime: str(a.dispatchTime),
    responseTime: str(a.responseTime),
    code: status ? (num(status.code) ?? 0) : null,
    message: status ? str(status.message) : "",
  };
}

export function parseTask(raw: Json): TaskInfo {
  const name = str(raw.name);
  const http = obj(raw.httpRequest);
  const body = str(http.body);
  return {
    name,
    id: shortName(name),
    url: str(http.url),
    method: str(http.httpMethod) || "POST",
    headers: strMap(http.headers),
    body: body ? base64ToUtf8(body) : "",
    scheduleTime: str(raw.scheduleTime),
    createTime: str(raw.createTime),
    dispatchCount: num(raw.dispatchCount) ?? 0,
    responseCount: num(raw.responseCount) ?? 0,
    lastAttempt: parseAttempt(raw.lastAttempt),
  };
}

const GRPC_CODES = [
  "OK",
  "CANCELLED",
  "UNKNOWN",
  "INVALID_ARGUMENT",
  "DEADLINE_EXCEEDED",
  "NOT_FOUND",
  "ALREADY_EXISTS",
  "PERMISSION_DENIED",
  "RESOURCE_EXHAUSTED",
  "FAILED_PRECONDITION",
  "ABORTED",
  "OUT_OF_RANGE",
  "UNIMPLEMENTED",
  "INTERNAL",
  "UNAVAILABLE",
  "DATA_LOSS",
  "UNAUTHENTICATED",
];

/** One-line summary of an attempt: "OK", "NOT_FOUND (HTTP status code Not Found)", or "pending". */
export function describeAttempt(a: AttemptInfo | null): string {
  if (!a) return "no attempts";
  if (a.code === null) return "in flight";
  const name = GRPC_CODES[a.code] ?? `code ${a.code}`;
  return a.message ? `${name} (${a.message})` : name;
}

export function attemptOk(a: AttemptInfo | null): boolean | null {
  return a && a.code !== null ? a.code === 0 : null;
}

export function queueBody(scope: QueueScope, q: NewQueue): Json {
  const rateLimits: Json = {};
  if (q.maxDispatchesPerSecond.trim())
    rateLimits.maxDispatchesPerSecond = Number(q.maxDispatchesPerSecond);
  if (q.maxConcurrentDispatches.trim())
    rateLimits.maxConcurrentDispatches = Number(q.maxConcurrentDispatches);
  const body: Json = {
    name: `projects/${scope.project}/locations/${scope.location}/queues/${q.id}`,
  };
  if (Object.keys(rateLimits).length > 0) body.rateLimits = rateLimits;
  if (q.maxAttempts.trim())
    body.retryConfig = { maxAttempts: Number(q.maxAttempts) };
  return body;
}

export function taskBody(t: NewTask, now: Date = new Date()): Json {
  const httpRequest: Json = { url: t.url.trim(), httpMethod: t.method };
  if (Object.keys(t.headers).length > 0) httpRequest.headers = t.headers;
  if (t.body) httpRequest.body = utf8ToBase64(t.body);
  const task: Json = { httpRequest };
  if (t.id.trim()) task.name = t.id.trim();
  const delay = Number(t.delaySeconds);
  if (t.delaySeconds.trim() && delay > 0) {
    task.scheduleTime = new Date(now.getTime() + delay * 1000).toISOString();
  }
  return { task };
}

async function listAll<T>(
  path: string,
  key: string,
  parse: (raw: Json) => T,
  fetchFn: FetchFn,
  extra = "",
): Promise<T[]> {
  const out: T[] = [];
  let token = "";
  do {
    const url = `${path}?pageSize=${PAGE}${extra}${token ? `&pageToken=${enc(token)}` : ""}`;
    const body = await restCall<Json>(
      TASKS_PROXY_BASE,
      "GET",
      url,
      undefined,
      fetchFn,
    );
    out.push(...((body[key] as Json[] | undefined) ?? []).map((r) => parse(r)));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export const listQueues = (s: QueueScope, fetchFn: FetchFn = fetch) =>
  listAll(queuesPath(s.project, s.location), "queues", parseQueue, fetchFn);

export async function createQueue(
  s: QueueScope,
  q: NewQueue,
  fetchFn: FetchFn = fetch,
): Promise<QueueInfo> {
  return parseQueue(
    await restCall(
      TASKS_PROXY_BASE,
      "POST",
      queuesPath(s.project, s.location),
      queueBody(s, q),
      fetchFn,
    ),
  );
}

export async function queueAction(
  s: QueueScope,
  queue: string,
  action: QueueAction,
  fetchFn: FetchFn = fetch,
): Promise<QueueInfo> {
  return parseQueue(
    await restCall(
      TASKS_PROXY_BASE,
      "POST",
      `${queuePath(s.project, s.location, queue)}:${action}`,
      {},
      fetchFn,
    ),
  );
}

export async function deleteQueue(
  s: QueueScope,
  queue: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    TASKS_PROXY_BASE,
    "DELETE",
    queuePath(s.project, s.location, queue),
    undefined,
    fetchFn,
  );
}

export const listTasks = (
  s: QueueScope,
  queue: string,
  fetchFn: FetchFn = fetch,
) =>
  listAll(
    `${queuePath(s.project, s.location, queue)}/tasks`,
    "tasks",
    parseTask,
    fetchFn,
    "&responseView=FULL",
  );

export async function createTask(
  s: QueueScope,
  queue: string,
  t: NewTask,
  fetchFn: FetchFn = fetch,
): Promise<TaskInfo> {
  const body = taskBody(t);
  const task = body.task as Json;
  if (task.name) {
    task.name = `projects/${s.project}/locations/${s.location}/queues/${queue}/tasks/${task.name}`;
  }
  return parseTask(
    await restCall(
      TASKS_PROXY_BASE,
      "POST",
      `${queuePath(s.project, s.location, queue)}/tasks`,
      body,
      fetchFn,
    ),
  );
}

export async function runTask(
  s: QueueScope,
  queue: string,
  task: string,
  fetchFn: FetchFn = fetch,
): Promise<TaskInfo> {
  return parseTask(
    await restCall(
      TASKS_PROXY_BASE,
      "POST",
      `${taskPath(s.project, s.location, queue, task)}:run`,
      {},
      fetchFn,
    ),
  );
}

export async function deleteTask(
  s: QueueScope,
  queue: string,
  task: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    TASKS_PROXY_BASE,
    "DELETE",
    taskPath(s.project, s.location, queue, task),
    undefined,
    fetchFn,
  );
}

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredTasksProject = () =>
  readStored(TASKS_PROJECT_KEY, DEFAULT_TASKS_PROJECT);
export const readStoredTasksLocation = () =>
  readStored(TASKS_LOCATION_KEY, DEFAULT_TASKS_LOCATION);

export function writeStoredTasksScope(project: string, location: string): void {
  try {
    localStorage.setItem(TASKS_PROJECT_KEY, project);
    localStorage.setItem(TASKS_LOCATION_KEY, location);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
