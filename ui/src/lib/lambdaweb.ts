/** Client for Lambda Web Functions through the dashboard's in-process proxy. */

import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const WEB_BASE = "/dashboard/api/v1/lambdaweb";
export const MAX_PAGES = 20;
export const NAMESPACE = "AWS/Lambda/Web";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number | undefined => (typeof v === "number" ? v : undefined);

export type WebFunction = {
  name: string;
  arn: string;
  state: string;
  stateReason: string;
  created: string;
  updated: string;
};

export function parseFunction(raw: Json): WebFunction {
  return {
    name: str(raw.functionName),
    arn: str(raw.functionArn),
    state: str(raw.state),
    stateReason: str(raw.stateReason),
    created: str(raw.createdAt),
    updated: str(raw.updatedAt),
  };
}

export type Revision = {
  id: string;
  arn: string;
  state: string;
  stateReason: string;
  created: string;
  description: string;
  runtime: string;
  bucket: string;
  key: string;
  versionId: string;
  timeout: number | undefined;
  concurrency: number | undefined;
  logGroup: string;
  role: string;
  envNames: string[];
  errors: string[];
};

export function parseRevision(raw: Json): Revision {
  const build = obj(raw.buildConfig);
  const s3 = obj(obj(build.codeConfig).s3Object);
  const svc = obj(raw.serviceConfig);
  return {
    id: str(raw.revisionId),
    arn: str(raw.revisionArn),
    state: str(raw.state),
    stateReason: str(raw.stateReason),
    created: str(raw.createdAt),
    description: str(raw.description),
    runtime: str(obj(build.runtimeConfig).runtime),
    bucket: str(s3.bucket),
    key: str(s3.key),
    versionId: str(s3.versionId),
    timeout: num(svc.timeoutSeconds),
    concurrency: num(svc.maxConcurrencyPerEnvironment),
    logGroup: str(obj(svc.loggingConfig).logGroup),
    role: str(svc.executionRoleArn),
    envNames: Object.keys(obj(svc.environmentVariables)).toSorted(),
    errors: list(raw.errors).map((e) => `${str(e.errorCode)}: ${str(e.errorMessage)}`),
  };
}

/** Where the code of a revision lives, as `s3://bucket/key`. */
export const codeLocation = (r: Pick<Revision, "bucket" | "key" | "versionId">): string =>
  r.bucket ? `s3://${r.bucket}/${r.key}${r.versionId ? ` (version ${r.versionId})` : ""}` : "—";

export type Weight = { revisionId: string; weight: number };

export type Endpoint = {
  name: string;
  type: string;
  auth: string;
  domain: string;
  state: string;
  stateReason: string;
  updateStatus: string;
  created: string;
  updated: string;
  weights: Weight[];
  maxEnvironments: number | undefined;
  rateLimit: number | undefined;
  regions: string[];
};

export function parseEndpoint(raw: Json): Endpoint {
  return {
    name: str(raw.endpointName),
    type: str(raw.endpointType),
    auth: str(raw.authType),
    domain: str(raw.domainName),
    state: str(raw.state),
    stateReason: str(raw.stateReason),
    updateStatus: str(raw.updateStatus),
    created: str(raw.createdAt),
    updated: str(raw.updatedAt),
    weights: list(raw.revisionWeights).map((w) => ({
      revisionId: str(w.revisionId),
      weight: num(w.weight) ?? 0,
    })),
    maxEnvironments: num(obj(raw.scalingConfig).maxEnvironments),
    rateLimit: num(obj(raw.throttleConfig).rateLimit),
    regions: list(raw.regions).map(String),
  };
}

export type WeightSegment = { revisionId: string; weight: number; percent: number };

/** Weights as shares of a bar. A zero total gives every revision an empty share. */
export function weightSegments(weights: readonly Weight[]): WeightSegment[] {
  const total = weights.reduce((sum, w) => sum + Math.max(0, w.weight), 0);
  return weights.map((w) => ({
    ...w,
    percent: total === 0 ? 0 : (Math.max(0, w.weight) / total) * 100,
  }));
}

const PALETTE = ["#0ea5e9", "#f59e0b", "#10b981", "#8b5cf6", "#ef4444", "#14b8a6"];
export const weightColor = (i: number): string => PALETTE[i % PALETTE.length]!;

const regionQuery = (region: string | undefined, extra = ""): string => {
  const parts = [region ? `region=${enc(region)}` : "", extra].filter(Boolean);
  return parts.length > 0 ? `?${parts.join("&")}` : "";
};

async function listPages(
  path: string,
  key: string,
  region: string | undefined,
  fetchFn: FetchFn,
): Promise<Json[]> {
  const out: Json[] = [];
  let token = "";
  for (let page = 0; page < MAX_PAGES; page++) {
    const body = await restCall(
      WEB_BASE,
      "GET",
      `${path}${regionQuery(region, token ? `nextToken=${enc(token)}` : "")}`,
      undefined,
      fetchFn,
    );
    out.push(...list(body[key]));
    token = str(body.nextToken);
    if (!token) break;
  }
  return out;
}

export async function listFunctions(
  region?: string,
  fetchFn: FetchFn = fetch,
): Promise<WebFunction[]> {
  const items = await listPages("/functions", "functions", region, fetchFn);
  return items.map((f) => parseFunction(f)).toSorted((a, b) => a.name.localeCompare(b.name));
}

export async function listRevisions(
  fn: string,
  region?: string,
  fetchFn: FetchFn = fetch,
): Promise<Revision[]> {
  const base = `/functions/${enc(fn)}/revisions`;
  const items = await listPages(base, "revisions", region, fetchFn);
  const full = await Promise.all(
    items.map((r) =>
      restCall(
        WEB_BASE,
        "GET",
        `${base}/${enc(str(r.revisionId))}${regionQuery(region)}`,
        undefined,
        fetchFn,
      ),
    ),
  );
  return full
    .map((r) => parseRevision(r))
    .toSorted((a, b) => Date.parse(b.created) - Date.parse(a.created));
}

export async function listEndpoints(
  fn: string,
  region?: string,
  fetchFn: FetchFn = fetch,
): Promise<Endpoint[]> {
  const items = await listPages(`/functions/${enc(fn)}/endpoints`, "endpoints", region, fetchFn);
  return items.map((e) => parseEndpoint(e)).toSorted((a, b) => a.name.localeCompare(b.name));
}

export type Header = { name: string; value: string };

export type InvokeRequest = {
  region: string;
  function: string;
  endpoint: string;
  method: string;
  path: string;
  headers: Header[];
  body: string;
};

export type InvokeReply = {
  status: number;
  domain: string;
  durationMs: number;
  headers: Record<string, string[]>;
  body: string;
  encoding: "text" | "base64";
  truncated: boolean;
};

export const INVOKE_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"] as const;
const HEADER_NAME = /^[A-Za-z0-9!#$%&'*+.^_`|~-]{1,64}$/;
const MAX_BODY = 1 << 17;

/** One header per line as `Name: value`; blank lines are skipped. Returns the first problem on failure. */
export function parseHeaders(text: string): { headers: Header[]; error: string } {
  const headers: Header[] = [];
  for (const line of text.split("\n")) {
    if (line.trim() === "") continue;
    const at = line.indexOf(":");
    const name = at < 0 ? "" : line.slice(0, at).trim();
    if (!HEADER_NAME.test(name))
      return { headers: [], error: `Invalid header line: ${line.trim()}` };
    headers.push({ name, value: line.slice(at + 1).trim() });
  }
  return { headers, error: "" };
}

/** The first problem with an invoke request, or empty when it can be sent. */
export function validateInvoke(path: string, headers: string, body: string): string {
  if (!path.startsWith("/") || path.startsWith("//") || /\s/.test(path)) {
    return "The path must start with / and contain no spaces";
  }
  if (path.length > 2048) return "The path is too long";
  if (body.length > MAX_BODY) return "The body is too large";
  return parseHeaders(headers).error;
}

export async function invokeEndpoint(
  req: InvokeRequest,
  fetchFn: FetchFn = fetch,
): Promise<InvokeReply> {
  const raw = await restCall(WEB_BASE, "POST", "/invoke", req, fetchFn);
  const headers: Record<string, string[]> = {};
  for (const [k, v] of Object.entries(obj(raw.headers))) headers[k] = list(v).map(String);
  return {
    status: num(raw.status) ?? 0,
    domain: str(raw.domain),
    durationMs: num(raw.durationMs) ?? 0,
    headers,
    body: str(raw.body),
    encoding: raw.encoding === "base64" ? "base64" : "text",
    truncated: raw.truncated === true,
  };
}

export function statusTone(status: number): "good" | "warn" | "bad" {
  if (status < 400) return "good";
  return status < 500 ? "warn" : "bad";
}

export const stateTone = (state: string): "good" | "busy" | "bad" | "muted" => {
  switch (state) {
    case "Active":
      return "good";
    case "Pending":
    case "InProgress":
    case "Deleting":
      return "busy";
    case "Failed":
      return "bad";
    default:
      return "muted";
  }
};

export type Point = { time: number; value: number };

/** Polyline points for a sparkline in a `width` x `height` box; one point is drawn as a flat line. */
export function sparkline(points: readonly Point[], width: number, height: number): string {
  if (points.length === 0) return "";
  const sorted = points.toSorted((a, b) => a.time - b.time);
  const t0 = sorted[0]!.time;
  const span = Math.max(1, sorted.at(-1)!.time - t0);
  const max = Math.max(1, ...sorted.map((p) => p.value));
  const x = (t: number) => (sorted.length === 1 ? 0 : ((t - t0) / span) * width);
  const y = (v: number) => height - (v / max) * height;
  const coords = sorted.map((p) => `${x(p.time).toFixed(1)},${y(p.value).toFixed(1)}`);
  return sorted.length === 1
    ? `0,${y(sorted[0]!.value).toFixed(1)} ${width},${y(sorted[0]!.value).toFixed(1)}`
    : coords.join(" ");
}

/** The log group a revision logs to when its logging config names none. */
export const defaultLogGroup = (fn: string): string => `/aws/lambda/web/${fn}`;

/** Link to the CloudWatch Logs page, filtered to a log group. */
export const logGroupLink = (group: string): string => `/dashboard/cloudwatchlogs?q=${enc(group)}`;
