/** Client for the Cloud Functions (v2) emulator via the dashboard's in-process REST proxy. */

import { zipStore } from "./gcpzip.ts";
import { restCall, shortName, str, type FetchFn, type Json } from "./gcprest.ts";

export const FN_PROXY_BASE = "/dashboard/api/v1/gcp/functions/v1";
export const FN_PROJECT_KEY = "gopherstack_functions_project";
export const FN_LOCATION_KEY = "gopherstack_functions_location";
export const DEFAULT_FN_PROJECT = "gopherstack-local";
export const DEFAULT_FN_LOCATION = "us-central1";
export const PUBSUB_EVENT = "google.cloud.pubsub.topic.v1.messagePublished";
export const MAX_ZIP_BYTES = 1 << 20;
const PAGE = 100;
const NO_RUNTIME = /container runtime is unavailable/i;

export type FnScope = { project: string; location: string };
export type FnState = "active" | "deploying" | "failed" | "deleting" | "unknown";
export type TriggerKind = "http" | "pubsub";
export type RuntimeInfo = { id: string; display: string };

export type FunctionInfo = {
  name: string;
  id: string;
  runtime: string;
  entryPoint: string;
  state: FnState;
  messages: string[];
  url: string;
  topic: string;
  trigger: TriggerKind;
  updateTime: string;
};

export type SourceInput =
  | { kind: "zip"; data: Uint8Array }
  | { kind: "inline"; filename: string; code: string };

export type NewFunction = {
  id: string;
  runtime: string;
  entryPoint: string;
  trigger: TriggerKind;
  topic: string;
  source: SourceInput;
};

export type InvokeInput = { method: string; path: string; body: string; headers: string };
export type InvokeOutput = { status: number; headers: Record<string, string>; body: string };

const enc = encodeURIComponent;
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const locPath = (s: FnScope) => `/projects/${enc(s.project)}/locations/${enc(s.location)}`;
const fnPath = (s: FnScope, id: string) => `${locPath(s)}/functions/${enc(id)}`;

/** Inline-editor starter file per runtime family; null when the runtime needs a zip. */
export const INLINE_TEMPLATES: Record<string, { filename: string; entry: string; code: string }> = {
  nodejs: {
    filename: "index.js",
    entry: "hello",
    code: "exports.hello = (req, res) => res.send('hi ' + (req.query.name || 'there'));\n",
  },
  python: {
    filename: "main.py",
    entry: "hello",
    code: "import functions_framework\n\n@functions_framework.http\ndef hello(request):\n    return 'hi ' + request.args.get('name', 'there')\n",
  },
};

export const runtimeFamily = (runtime: string): string => runtime.replace(/[0-9]+$/, "");
export const inlineTemplate = (runtime: string) => INLINE_TEMPLATES[runtimeFamily(runtime)] ?? null;

export function validateFunctionId(id: string): string | null {
  if (!id) return "name is required";
  if (!/^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$/.test(id)) {
    return "lowercase letters, digits and -, starting with a letter (max 63)";
  }
  return null;
}

export const validateEntryPoint = (v: string): string | null =>
  v.trim() ? null : "entry point is required";

export function validateTopic(trigger: TriggerKind, topic: string): string | null {
  return trigger === "pubsub" && !topic ? "pick a Pub/Sub topic" : null;
}

export function validateSource(src: SourceInput): string | null {
  if (src.kind === "inline") return src.code.trim() ? null : "source code is required";
  if (src.data.length === 0) return "choose a .zip file";
  return src.data.length > MAX_ZIP_BYTES ? "zip must be 1 MiB or smaller in the dashboard" : null;
}

export function parseHeaders(text: string): { headers: Record<string, string>; error: string } {
  const headers: Record<string, string> = {};
  for (const line of text.split("\n")) {
    if (!line.trim()) continue;
    const i = line.indexOf(":");
    if (i < 1) return { headers: {}, error: `bad header line "${line.trim()}"` };
    headers[line.slice(0, i).trim()] = line.slice(i + 1).trim();
  }
  return { headers, error: "" };
}

const STATES: Record<string, FnState> = {
  ACTIVE: "active",
  DEPLOYING: "deploying",
  FAILED: "failed",
  DELETING: "deleting",
};

export function parseFunction(raw: Json): FunctionInfo {
  const name = str(raw.name);
  const build = obj(raw.buildConfig);
  const topic = str(obj(raw.eventTrigger).pubsubTopic);
  return {
    name,
    id: shortName(name),
    runtime: str(build.runtime),
    entryPoint: str(build.entryPoint),
    state: STATES[str(raw.state)] ?? "unknown",
    messages: list(raw.stateMessages).map((m) => str(m.message)),
    url: str(obj(raw.serviceConfig).uri),
    topic,
    trigger: topic ? "pubsub" : "http",
    updateTime: str(raw.updateTime),
  };
}

export function parseRuntimes(raw: Json): RuntimeInfo[] {
  return list(raw.runtimes).map((r) => ({
    id: str(r.name),
    display: str(r.displayName) || str(r.name),
  }));
}

/** True when any message reports that the container runtime is missing. */
export function runtimeUnavailable(texts: Iterable<string>): boolean {
  for (const t of texts) if (NO_RUNTIME.test(t)) return true;
  return false;
}

export function sourceZip(src: SourceInput): Uint8Array {
  if (src.kind === "zip") return src.data;
  return zipStore([{ name: src.filename, data: new TextEncoder().encode(src.code) }]);
}

export function createBody(n: NewFunction, storage: Json): Json {
  const body: Json = {
    environment: "GEN_2",
    buildConfig: {
      runtime: n.runtime,
      entryPoint: n.entryPoint.trim(),
      source: { storageSource: storage },
    },
  };
  if (n.trigger === "pubsub") body.eventTrigger = { eventType: PUBSUB_EVENT, pubsubTopic: n.topic };
  return body;
}

export async function listFunctions(s: FnScope, fetchFn: FetchFn = fetch): Promise<FunctionInfo[]> {
  const out: FunctionInfo[] = [];
  let token = "";
  do {
    const q = `pageSize=${PAGE}${token ? `&pageToken=${enc(token)}` : ""}`;
    const body = await restCall(
      FN_PROXY_BASE,
      "GET",
      `${locPath(s)}/functions?${q}`,
      undefined,
      fetchFn,
    );
    out.push(...list(body.functions).map((r) => parseFunction(r)));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export async function listRuntimes(s: FnScope, fetchFn: FetchFn = fetch): Promise<RuntimeInfo[]> {
  return parseRuntimes(
    await restCall(FN_PROXY_BASE, "GET", `${locPath(s)}/runtimes`, undefined, fetchFn),
  );
}

/** GenerateUploadUrl, then PUT the archive through the proxy; resolves to the storage source. */
export async function uploadSource(
  s: FnScope,
  src: SourceInput,
  fetchFn: FetchFn = fetch,
): Promise<Json> {
  const slot = await restCall(
    FN_PROXY_BASE,
    "POST",
    `${locPath(s)}/functions:generateUploadUrl`,
    {},
    fetchFn,
  );
  const res = await fetchFn(
    `${FN_PROXY_BASE}${locPath(s)}/functions:upload?uploadUrl=${enc(str(slot.uploadUrl))}`,
    { method: "PUT", body: sourceZip(src) as BodyInit },
  );
  if (!res.ok) {
    const e = (await res.json().catch(() => ({}))) as { error?: { message?: string } };
    throw new Error(e.error?.message ?? `upload failed (HTTP ${res.status})`);
  }
  return obj(slot.storageSource);
}

export async function createFunction(s: FnScope, n: NewFunction, fetchFn: FetchFn = fetch) {
  const storage = await uploadSource(s, n.source, fetchFn);
  await restCall(
    FN_PROXY_BASE,
    "POST",
    `${locPath(s)}/functions?functionId=${enc(n.id)}`,
    createBody(n, storage),
    fetchFn,
  );
}

export async function updateSource(
  s: FnScope,
  id: string,
  src: SourceInput,
  fetchFn: FetchFn = fetch,
) {
  const storage = await uploadSource(s, src, fetchFn);
  await restCall(
    FN_PROXY_BASE,
    "PATCH",
    `${fnPath(s, id)}?updateMask=buildConfig.source`,
    { buildConfig: { source: { storageSource: storage } } },
    fetchFn,
  );
}

export async function deleteFunction(s: FnScope, id: string, fetchFn: FetchFn = fetch) {
  await restCall(FN_PROXY_BASE, "DELETE", fnPath(s, id), undefined, fetchFn);
}

export async function invokeFunction(
  s: FnScope,
  id: string,
  i: InvokeInput,
  fetchFn: FetchFn = fetch,
): Promise<InvokeOutput> {
  const { headers, error } = parseHeaders(i.headers);
  if (error) throw new Error(error);
  const res = await restCall(
    FN_PROXY_BASE,
    "POST",
    `${fnPath(s, id)}:invoke`,
    { method: i.method, path: i.path || "/", body: i.body, headers },
    fetchFn,
  );
  const hdrs = obj(res.headers);
  return {
    status: typeof res.status === "number" ? res.status : 0,
    headers: Object.fromEntries(Object.entries(hdrs).map(([k, v]) => [k, str(v)])),
    body: str(res.body),
  };
}

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredFnProject = () => readStored(FN_PROJECT_KEY, DEFAULT_FN_PROJECT);
export const readStoredFnLocation = () => readStored(FN_LOCATION_KEY, DEFAULT_FN_LOCATION);

export function writeStoredFnScope(project: string, location: string): void {
  try {
    localStorage.setItem(FN_PROJECT_KEY, project);
    localStorage.setItem(FN_LOCATION_KEY, location);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
