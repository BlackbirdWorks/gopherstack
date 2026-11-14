/** Client for Azure Logic Apps through the dashboard's in-process Resource Manager proxy (Microsoft.Logic). */

import { GcpRestError, str, type FetchFn, type Json } from "./gcprest.ts";

export const LOGIC_PROXY_BASE = "/dashboard/api/v1/azure/logic";
export const LOGIC_ACCESS_BASE = "/dashboard/api/v1/azure/logicaccess";
export const LOGIC_API_VERSION = "2019-05-01";
export const RUN_PAGE_SIZE = 50;
export const MAX_PAGES = 20;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const props = (raw: Json): Json => obj(raw.properties);
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type Workflow = {
  id: string;
  name: string;
  location: string;
  state: string;
  version: string;
  created: string;
  changed: string;
  accessEndpoint: string;
  definition: Json;
};

export type Trigger = {
  name: string;
  state: string;
  type: string;
  last: string;
  next: string;
  recurrence: string;
};

export type RunError = { code: string; message: string };

export type Run = {
  name: string;
  status: string;
  start: string;
  end: string;
  trigger: string;
  triggerStatus: string;
  error: RunError | undefined;
  hasTriggerInputs: boolean;
  hasTriggerOutputs: boolean;
};

export type ActionRun = {
  name: string;
  status: string;
  start: string;
  end: string;
  code: string;
  error: RunError | undefined;
  hasInputs: boolean;
  hasOutputs: boolean;
  inputsSize: number;
  outputsSize: number;
};

export type Callback = { url: string; method: string };
export type Invocation = { status: number; contentType: string; runId: string; body: string };

export type RunTarget = { subscription: string; group: string; workflow: string };

const runError = (v: unknown): RunError | undefined => {
  const e = obj(v);
  const code = str(e.code);
  const message = str(e.message);
  return code || message ? { code, message } : undefined;
};

export function parseWorkflow(raw: Json): Workflow {
  const p = props(raw);
  return {
    id: str(raw.id),
    name: str(raw.name),
    location: str(raw.location),
    state: str(p.state),
    version: str(p.version),
    created: str(p.createdTime),
    changed: str(p.changedTime),
    accessEndpoint: str(p.accessEndpoint),
    definition: obj(p.definition),
  };
}

/** Trigger name to type, read from the workflow definition (the trigger resource does not carry the type). */
export function triggerTypes(definition: Json): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [name, def] of Object.entries(obj(definition.triggers)))
    out[name] = str(obj(def).type);
  return out;
}

const recurrenceText = (r: Json): string => {
  const interval = num(r.interval) || 1;
  const freq = str(r.frequency);
  return freq ? `every ${interval} ${freq.toLowerCase()}${interval === 1 ? "" : "s"}` : "";
};

export function parseTrigger(raw: Json, types: Record<string, string>): Trigger {
  const p = props(raw);
  const name = str(raw.name);
  return {
    name,
    state: str(p.state),
    type: types[name] ?? "",
    last: str(p.lastExecutionTime),
    next: str(p.nextExecutionTime),
    recurrence: recurrenceText(obj(p.recurrence)),
  };
}

export function parseRun(raw: Json): Run {
  const p = props(raw);
  const trig = obj(p.trigger);
  return {
    name: str(raw.name),
    status: str(p.status),
    start: str(p.startTime),
    end: str(p.endTime),
    trigger: str(trig.name),
    triggerStatus: str(trig.status),
    error: runError(p.error),
    hasTriggerInputs: obj(trig.inputsLink).contentSize !== undefined,
    hasTriggerOutputs: obj(trig.outputsLink).contentSize !== undefined,
  };
}

export function parseAction(raw: Json): ActionRun {
  const p = props(raw);
  return {
    name: str(raw.name),
    status: str(p.status),
    start: str(p.startTime),
    end: str(p.endTime),
    code: str(p.code),
    error: runError(p.error),
    hasInputs: p.inputsLink !== undefined,
    hasOutputs: p.outputsLink !== undefined,
    inputsSize: num(obj(p.inputsLink).contentSize),
    outputsSize: num(obj(p.outputsLink).contentSize),
  };
}

export type Tone = "good" | "bad" | "busy" | "muted";

/** How a run or action status is colored. */
export function statusTone(status: string): Tone {
  switch (status) {
    case "Succeeded":
      return "good";
    case "Failed":
    case "TimedOut":
      return "bad";
    case "Running":
    case "Waiting":
      return "busy";
    default:
      return "muted";
  }
}

export const isRunning = (status: string): boolean => status === "Running" || status === "Waiting";

export function durationMs(start: string, end: string): number | undefined {
  const a = Date.parse(start);
  const b = Date.parse(end);
  return Number.isNaN(a) || Number.isNaN(b) ? undefined : Math.max(0, b - a);
}

export function formatDuration(ms: number | undefined): string {
  if (ms === undefined) return "—";
  if (ms < 1000) return `${ms} ms`;
  const s = ms / 1000;
  return s < 60 ? `${s.toFixed(1)} s` : `${Math.floor(s / 60)} min ${Math.round(s % 60)} s`;
}

export type TimelineEntry = ActionRun & {
  offsetPct: number;
  widthPct: number;
  duration: number | undefined;
};

const MIN_BAR_PCT = 1;

/** Orders actions by start time and places each one on a shared time axis for a timeline bar. */
export function buildTimeline(actions: readonly ActionRun[]): TimelineEntry[] {
  const sorted = actions.toSorted((a, b) => Date.parse(a.start) - Date.parse(b.start));
  const starts = sorted.map((a) => Date.parse(a.start)).filter((t) => !Number.isNaN(t));
  const origin = starts.length > 0 ? Math.min(...starts) : 0;
  const ends = sorted.map((a) => Date.parse(a.end || a.start)).filter((t) => !Number.isNaN(t));
  const span = Math.max(1, (ends.length > 0 ? Math.max(...ends) : origin) - origin);
  return sorted.map((a) => {
    const from = Date.parse(a.start);
    const to = Date.parse(a.end || a.start);
    const offset = Number.isNaN(from) ? 0 : ((from - origin) / span) * 100;
    const width = Number.isNaN(from) || Number.isNaN(to) ? MIN_BAR_PCT : ((to - from) / span) * 100;
    return {
      ...a,
      offsetPct: Math.min(100 - MIN_BAR_PCT, offset),
      widthPct: Math.max(MIN_BAR_PCT, Math.min(100 - offset, width)),
      duration: durationMs(a.start, a.end),
    };
  });
}

export type JsonToken = {
  kind: "key" | "string" | "number" | "literal" | "punct" | "space";
  text: string;
};

const TOKEN =
  /("(?:[^"\\]|\\.)*")(\s*:)?|(-?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)|\b(true|false|null)\b|([{}[\],:])|(\s+)/g;

function tokensOf(m: RegExpMatchArray): JsonToken[] {
  const [, quoted, colon, number, literal, punct, space] = m;
  if (quoted !== undefined) {
    return colon === undefined
      ? [{ kind: "string", text: quoted }]
      : [
          { kind: "key", text: quoted },
          { kind: "punct", text: colon },
        ];
  }
  if (number !== undefined) return [{ kind: "number", text: number }];
  if (literal !== undefined) return [{ kind: "literal", text: literal }];
  if (punct !== undefined) return [{ kind: "punct", text: punct }];
  return [{ kind: "space", text: space ?? "" }];
}

/** Splits JSON text into tokens for syntax coloring; anything unrecognised is kept as plain punctuation. */
export function tokenizeJson(text: string): JsonToken[] {
  const out: JsonToken[] = [];
  let last = 0;
  for (const m of text.matchAll(TOKEN)) {
    const at = m.index ?? 0;
    if (at > last) out.push({ kind: "punct", text: text.slice(last, at) });
    out.push(...tokensOf(m));
    last = at + m[0].length;
  }
  if (last < text.length) out.push({ kind: "punct", text: text.slice(last) });
  return out;
}

export function prettyJson(value: unknown): string {
  if (typeof value === "string") {
    try {
      return JSON.stringify(JSON.parse(value), null, 2);
    } catch {
      return value;
    }
  }
  return JSON.stringify(value, null, 2) ?? "";
}

/** Shows a callback URL with its signature hidden. The real value is only held after an explicit reveal. */
export function maskSignature(url: string): string {
  return url.replace(/([?&]sig=)[^&]*/, "$1••••••••");
}

async function failure(res: Response): Promise<GcpRestError> {
  const text = await res.text().catch(() => "");
  try {
    const e = (JSON.parse(text) as { error?: { code?: string; message?: string } }).error;
    if (e?.message || e?.code)
      return new GcpRestError(res.status, e.code ?? "", e.message ?? e.code ?? "");
  } catch {
    // fall through to the status text
  }
  return new GcpRestError(res.status, "", text.trim() || res.statusText || `HTTP ${res.status}`);
}

async function call(
  method: string,
  url: string,
  body: unknown,
  fetchFn: FetchFn,
): Promise<{ text: string; body: Json }> {
  let res: Response;
  try {
    res = await fetchFn(url, {
      method,
      ...(body === undefined
        ? {}
        : { body: JSON.stringify(body), headers: { "Content-Type": "application/json" } }),
    });
  } catch (e) {
    throw new GcpRestError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await failure(res);
  const text = await res.text();
  let parsed: Json = {};
  try {
    parsed = text ? (JSON.parse(text) as Json) : {};
  } catch {
    // content that is not an object: callers that need it read `text`
  }
  return { text, body: parsed };
}

const sub = (s: string) => `/subscriptions/${enc(s)}`;
const groupPath = (s: string, rg: string) =>
  `${sub(s)}/resourceGroups/${enc(rg)}/providers/Microsoft.Logic/workflows`;
const flowPath = (t: RunTarget) => `${groupPath(t.subscription, t.group)}/${enc(t.workflow)}`;
const withVersion = (path: string, extra = "") =>
  `${LOGIC_PROXY_BASE}${path}?api-version=${LOGIC_API_VERSION}${extra}`;

/** The `$skiptoken` of an ARM `nextLink`, or empty when there is no further page. */
export function nextToken(link: unknown): string {
  if (typeof link !== "string" || link === "") return "";
  try {
    return new URL(link, "http://local").searchParams.get("$skiptoken") ?? "";
  } catch {
    return "";
  }
}

async function listPages(path: string, extra: string, fetchFn: FetchFn): Promise<Json[]> {
  const out: Json[] = [];
  let token = "";
  for (let page = 0; page < MAX_PAGES; page++) {
    const more = `${extra}${token ? `&$skiptoken=${enc(token)}` : ""}`;
    const { body } = await call("GET", withVersion(path, more), undefined, fetchFn);
    out.push(...list(body.value));
    token = nextToken(body.nextLink);
    if (!token) break;
  }
  return out;
}

export async function listWorkflows(
  subscription: string,
  group: string,
  fetchFn: FetchFn = fetch,
): Promise<Workflow[]> {
  const items = await listPages(groupPath(subscription, group), "", fetchFn);
  return items.map((w) => parseWorkflow(w)).toSorted((a, b) => a.name.localeCompare(b.name));
}

export async function getWorkflow(t: RunTarget, fetchFn: FetchFn = fetch): Promise<Workflow> {
  return parseWorkflow((await call("GET", withVersion(flowPath(t)), undefined, fetchFn)).body);
}

export async function setWorkflowEnabled(
  t: RunTarget,
  enabled: boolean,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await call("POST", withVersion(`${flowPath(t)}/${enabled ? "enable" : "disable"}`), {}, fetchFn);
}

export async function listTriggers(
  t: RunTarget,
  definition: Json,
  fetchFn: FetchFn = fetch,
): Promise<Trigger[]> {
  const types = triggerTypes(definition);
  const items = await listPages(`${flowPath(t)}/triggers`, "", fetchFn);
  return items.map((r) => parseTrigger(r, types)).toSorted((a, b) => a.name.localeCompare(b.name));
}

export const isRequestTrigger = (type: string): boolean =>
  type === "Request" || type === "HttpRequest";

/** Request triggers are called through their signed URL (server side); recurrence triggers run through ARM. */
export async function runTrigger(
  t: RunTarget,
  trigger: Pick<Trigger, "name" | "type">,
  payload: string,
  fetchFn: FetchFn = fetch,
): Promise<Invocation | undefined> {
  if (!isRequestTrigger(trigger.type)) {
    await call(
      "POST",
      withVersion(`${flowPath(t)}/triggers/${enc(trigger.name)}/run`),
      {},
      fetchFn,
    );
    return undefined;
  }
  const text = payload.trim();
  const res = await fetchFn(
    `${LOGIC_ACCESS_BASE}/invoke/${enc(t.subscription)}/${enc(t.group)}/${enc(t.workflow)}/${enc(trigger.name)}`,
    { method: "POST", headers: { "Content-Type": "application/json" }, body: text },
  ).catch((e: unknown) => {
    throw new GcpRestError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  });
  if (!res.ok) throw await failure(res);
  const b = obj(await res.json());
  return {
    status: num(b.status),
    contentType: str(b.contentType),
    runId: str(b.runId),
    body: str(b.body),
  };
}

/** Validates the optional trigger payload: empty, or JSON. */
export function validatePayload(payload: string): string {
  if (payload.trim() === "") return "";
  try {
    JSON.parse(payload);
    return "";
  } catch {
    return "The payload must be valid JSON";
  }
}

/** Fetches the signed callback URL. Only call this from an explicit reveal action. */
export async function revealCallback(
  t: RunTarget,
  trigger: string,
  fetchFn: FetchFn = fetch,
): Promise<Callback> {
  const { body } = await call(
    "POST",
    `${LOGIC_ACCESS_BASE}/callback/${enc(t.subscription)}/${enc(t.group)}/${enc(t.workflow)}/${enc(trigger)}`,
    undefined,
    fetchFn,
  );
  return { url: str(body.value), method: str(body.method) };
}

export async function listRuns(t: RunTarget, fetchFn: FetchFn = fetch): Promise<Run[]> {
  const { body } = await call(
    "GET",
    withVersion(`${flowPath(t)}/runs`, `&$top=${RUN_PAGE_SIZE}`),
    undefined,
    fetchFn,
  );
  return list(body.value)
    .map((r) => parseRun(r))
    .toSorted((a, b) => Date.parse(b.start) - Date.parse(a.start));
}

export async function getRun(t: RunTarget, run: string, fetchFn: FetchFn = fetch): Promise<Run> {
  return parseRun(
    (await call("GET", withVersion(`${flowPath(t)}/runs/${enc(run)}`), undefined, fetchFn)).body,
  );
}

export async function listActions(
  t: RunTarget,
  run: string,
  fetchFn: FetchFn = fetch,
): Promise<ActionRun[]> {
  const items = await listPages(`${flowPath(t)}/runs/${enc(run)}/actions`, "", fetchFn);
  return items.map((a) => parseAction(a));
}

export async function cancelRun(
  t: RunTarget,
  run: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await call("POST", withVersion(`${flowPath(t)}/runs/${enc(run)}/cancel`), {}, fetchFn);
}

export type ContentScope = { trigger: true } | { trigger?: false; action: string };

/** Reads stored inputs or outputs of a trigger or action through the run's signed link, on the server. */
export async function getContent(
  t: RunTarget,
  run: string,
  scope: ContentScope,
  kind: "inputs" | "outputs",
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const where = scope.trigger ? "trigger" : `action/${enc(scope.action)}`;
  const { text } = await call(
    "GET",
    `${LOGIC_ACCESS_BASE}/content/${enc(t.subscription)}/${enc(t.group)}/${enc(t.workflow)}/${enc(run)}/${where}/${kind}`,
    undefined,
    fetchFn,
  );
  return text;
}
