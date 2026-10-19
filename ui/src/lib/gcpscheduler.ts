/** Client for the Cloud Scheduler emulator via the dashboard's in-process REST proxy. */

import {
  base64ToUtf8,
  restCall,
  shortName,
  str,
  strMap,
  utf8ToBase64,
  type FetchFn,
  type Json,
} from "./gcprest.ts";

export const SCHEDULER_PROXY_BASE = "/dashboard/api/v1/gcp/scheduler/v1";
export const SCHEDULER_PROJECT_KEY = "gopherstack_scheduler_project";
export const SCHEDULER_LOCATION_KEY = "gopherstack_scheduler_location";
export const DEFAULT_SCHEDULER_PROJECT = "gopherstack-local";
export const DEFAULT_SCHEDULER_LOCATION = "us-central1";
export const DEFAULT_TIME_ZONE = "Etc/UTC";
const PAGE = 100;

export const HTTP_METHODS = ["POST", "GET", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"] as const;
export type HttpMethod = (typeof HTTP_METHODS)[number];
export type TargetKind = "http" | "pubsub";
export type JobAction = "pause" | "resume" | "run";

export type JobInfo = {
  name: string;
  id: string;
  description: string;
  schedule: string;
  timeZone: string;
  state: string;
  targetType: "HTTP" | "Pub/Sub" | "App Engine" | "none";
  target: string;
  method: string;
  headers: Record<string, string>;
  body: string | null;
  scheduleTime: string;
  lastAttemptTime: string;
  /** gRPC status code of the last attempt; null when the job never ran. */
  lastCode: number | null;
  lastMessage: string;
};

export type JobScope = { project: string; location: string };
export type NewJob = {
  id: string;
  description: string;
  schedule: string;
  timeZone: string;
  kind: TargetKind;
  url: string;
  method: HttpMethod;
  headers: Record<string, string>;
  body: string;
  topic: string;
  data: string;
};

const enc = encodeURIComponent;
const jobsPath = (p: string, l: string) => `/projects/${enc(p)}/locations/${enc(l)}/jobs`;
const jobPath = (p: string, l: string, j: string) => `${jobsPath(p, l)}/${enc(j)}`;
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});

export function validateJobId(id: string): string | null {
  if (!id) return "name is required";
  if (!/^[A-Za-z0-9_-]{1,500}$/.test(id)) return "use letters, digits, - and _ (max 500)";
  return null;
}

const FIELD_RANGES: readonly [number, number][] = [
  [0, 59],
  [0, 23],
  [1, 31],
  [1, 12],
  [0, 7],
];

function validCronField(field: string, min: number, max: number): boolean {
  return field.split(",").every((part) => {
    const [range, step, extra] = part.split("/");
    if (extra !== undefined) return false;
    if (step !== undefined && !/^\d+$/.test(step)) return false;
    if (range === "*" || range === "?") return true;
    const [lo, hi, more] = range.split("-");
    if (more !== undefined || !/^\d+$/.test(lo)) return false;
    if (hi !== undefined && !/^\d+$/.test(hi)) return false;
    return Number(lo) >= min && Number(hi ?? lo) <= max;
  });
}

/** Light client-side check of a five-field unix-cron schedule; the backend stays authoritative. */
export function validateSchedule(s: string): string | null {
  const t = s.trim();
  if (!t) return "schedule is required";
  const fields = t.split(/\s+/);
  if (fields.length !== FIELD_RANGES.length) return "use five cron fields, e.g. */5 * * * *";
  const ok = fields.every((f, i) => validCronField(f, FIELD_RANGES[i][0], FIELD_RANGES[i][1]));
  return ok ? null : "not a valid cron expression";
}

export function validateTimeZone(tz: string): string | null {
  if (!tz.trim()) return "time zone is required";
  try {
    Intl.DateTimeFormat("en", { timeZone: tz.trim() }).format();
  } catch {
    return "unknown time zone, e.g. America/New_York";
  }
  return null;
}

export function validateUrl(url: string): string | null {
  if (!url) return "URL is required";
  try {
    const u = new URL(url);
    if (u.protocol !== "http:" && u.protocol !== "https:") return "URL must be http or https";
  } catch {
    return "enter an absolute URL such as http://localhost:9000/hook";
  }
  return null;
}

export function validateTopic(topic: string, project: string): string | null {
  if (!topic) return "topic is required";
  if (/^projects\/[^/]+\/topics\/[^/]+$/.test(topic)) return null;
  if (/^[A-Za-z][A-Za-z0-9._~+%-]{2,254}$/.test(topic) && project) return null;
  return "enter a topic ID or projects/{project}/topics/{topic}";
}

export function validateHeaderKey(key: string): string | null {
  if (!key) return "key is required";
  if (!/^[A-Za-z0-9!#$%&'*+.^_`|~-]+$/.test(key)) return "not a valid header name";
  return null;
}

export const fullTopicName = (topic: string, project: string): string =>
  topic.startsWith("projects/") ? topic : `projects/${project}/topics/${topic}`;

export function parseJob(raw: Json): JobInfo {
  const name = str(raw.name);
  const http = obj(raw.httpTarget);
  const pubsub = obj(raw.pubsubTarget);
  const gae = obj(raw.appEngineHttpTarget);
  const status = raw.status && typeof raw.status === "object" ? obj(raw.status) : null;
  const lastAttemptTime = str(raw.lastAttemptTime);
  const base = {
    name,
    id: shortName(name),
    description: str(raw.description),
    schedule: str(raw.schedule),
    timeZone: str(raw.timeZone) || DEFAULT_TIME_ZONE,
    state: str(raw.state) || "ENABLED",
    scheduleTime: str(raw.scheduleTime),
    lastAttemptTime,
    lastCode:
      lastAttemptTime || status ? (typeof status?.code === "number" ? status.code : 0) : null,
    lastMessage: status ? str(status.message) : "",
  };
  if (raw.pubsubTarget) {
    const data = str(pubsub.data);
    return {
      ...base,
      targetType: "Pub/Sub",
      target: str(pubsub.topicName),
      method: "",
      headers: strMap(pubsub.attributes),
      body: data ? base64ToUtf8(data) : "",
    };
  }
  if (raw.httpTarget) {
    const body = str(http.body);
    return {
      ...base,
      targetType: "HTTP",
      target: str(http.uri),
      method: str(http.httpMethod) || "POST",
      headers: strMap(http.headers),
      body: body ? base64ToUtf8(body) : "",
    };
  }
  if (raw.appEngineHttpTarget) {
    return {
      ...base,
      targetType: "App Engine",
      target: str(gae.relativeUri),
      method: str(gae.httpMethod) || "POST",
      headers: strMap(gae.headers),
      body: "",
    };
  }
  return { ...base, targetType: "none", target: "", method: "", headers: {}, body: "" };
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

/** One-line last-attempt summary: "OK", "UNAVAILABLE (connection refused)" or "never run". */
export function describeLastAttempt(j: Pick<JobInfo, "lastCode" | "lastMessage">): string {
  if (j.lastCode === null) return "never run";
  const name = GRPC_CODES[j.lastCode] ?? `code ${j.lastCode}`;
  return j.lastMessage ? `${name} (${j.lastMessage})` : name;
}

export const lastAttemptOk = (j: Pick<JobInfo, "lastCode">): boolean | null =>
  j.lastCode === null ? null : j.lastCode === 0;

/** Friendly countdown to the next run: "in 42s", "in 3m 5s", "in 2h 4m", "due now". */
export function countdown(iso: string, now: number = Date.now()): string {
  if (!iso) return "—";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return "—";
  const secs = Math.ceil((t - now) / 1000);
  if (secs <= 0) return "due now";
  if (secs < 60) return `in ${secs}s`;
  const m = Math.floor(secs / 60);
  if (m < 60) return `in ${m}m ${secs % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `in ${h}h ${m % 60}m`;
  return `in ${Math.floor(h / 24)}d ${h % 24}h`;
}

export function jobBody(scope: JobScope, j: NewJob): Json {
  const body: Json = {
    name: `projects/${scope.project}/locations/${scope.location}/jobs/${j.id}`,
    schedule: j.schedule.trim(),
    timeZone: j.timeZone.trim() || DEFAULT_TIME_ZONE,
  };
  if (j.description.trim()) body.description = j.description.trim();
  if (j.kind === "pubsub") {
    const target: Json = { topicName: fullTopicName(j.topic.trim(), scope.project) };
    if (j.data) target.data = utf8ToBase64(j.data);
    if (Object.keys(j.headers).length > 0) target.attributes = j.headers;
    body.pubsubTarget = target;
  } else {
    const target: Json = { uri: j.url.trim(), httpMethod: j.method };
    if (Object.keys(j.headers).length > 0) target.headers = j.headers;
    if (j.body) target.body = utf8ToBase64(j.body);
    body.httpTarget = target;
  }
  return body;
}

export async function listJobs(s: JobScope, fetchFn: FetchFn = fetch): Promise<JobInfo[]> {
  const out: JobInfo[] = [];
  let token = "";
  do {
    const url = `${jobsPath(s.project, s.location)}?pageSize=${PAGE}${token ? `&pageToken=${enc(token)}` : ""}`;
    const body = await restCall<Json>(SCHEDULER_PROXY_BASE, "GET", url, undefined, fetchFn);
    out.push(...((body.jobs as Json[] | undefined) ?? []).map((r) => parseJob(r)));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export async function createJob(
  s: JobScope,
  j: NewJob,
  fetchFn: FetchFn = fetch,
): Promise<JobInfo> {
  return parseJob(
    await restCall(
      SCHEDULER_PROXY_BASE,
      "POST",
      jobsPath(s.project, s.location),
      jobBody(s, j),
      fetchFn,
    ),
  );
}

export async function jobAction(
  s: JobScope,
  job: string,
  action: JobAction,
  fetchFn: FetchFn = fetch,
): Promise<JobInfo> {
  return parseJob(
    await restCall(
      SCHEDULER_PROXY_BASE,
      "POST",
      `${jobPath(s.project, s.location, job)}:${action}`,
      {},
      fetchFn,
    ),
  );
}

/** Updates only schedule and time zone via an UpdateJob field mask. */
export async function updateSchedule(
  s: JobScope,
  job: string,
  schedule: string,
  timeZone: string,
  fetchFn: FetchFn = fetch,
): Promise<JobInfo> {
  return parseJob(
    await restCall(
      SCHEDULER_PROXY_BASE,
      "PATCH",
      `${jobPath(s.project, s.location, job)}?updateMask=schedule,timeZone`,
      { schedule: schedule.trim(), timeZone: timeZone.trim() },
      fetchFn,
    ),
  );
}

export async function deleteJob(s: JobScope, job: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(
    SCHEDULER_PROXY_BASE,
    "DELETE",
    jobPath(s.project, s.location, job),
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

export const readStoredSchedulerProject = () =>
  readStored(SCHEDULER_PROJECT_KEY, DEFAULT_SCHEDULER_PROJECT);
export const readStoredSchedulerLocation = () =>
  readStored(SCHEDULER_LOCATION_KEY, DEFAULT_SCHEDULER_LOCATION);

export function writeStoredSchedulerScope(project: string, location: string): void {
  try {
    localStorage.setItem(SCHEDULER_PROJECT_KEY, project);
    localStorage.setItem(SCHEDULER_LOCATION_KEY, location);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
