/** Client for the Oracle Cloud Functions emulator (applications, functions, invoke, work requests) via the dashboard's in-process proxy. */

import { restCall, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const FUNCTIONS_PROXY_BASE = "/dashboard/api/v1/oci/functions";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type ConfigEntry = { key: string; value: string; masked: boolean };

export type AppInfo = {
  id: string;
  name: string;
  state: string;
  shape: string;
  compartmentId: string;
  subnetIds: string[];
  timeCreated: string;
  syslogUrl: string;
  config: ConfigEntry[];
  raw: Json;
};

export type FunctionInfo = {
  id: string;
  appId: string;
  name: string;
  state: string;
  image: string;
  imageDigest: string;
  memoryMb: number;
  timeoutSec: number;
  invokeEndpoint: string;
  timeCreated: string;
  config: ConfigEntry[];
  raw: Json;
};

export type WorkRequestInfo = {
  id: string;
  operation: string;
  status: string;
  percentComplete: number;
  resources: { entityType: string; actionType: string; identifier: string }[];
  timeAccepted: string;
  timeFinished: string;
};

export type WorkRequestLog = { timestamp: string; message: string };

export type InvokeResult = {
  status: number;
  callId: string;
  headers: [string, string][];
  text: string | null;
  base64: string;
};

function configEntries(raw: Json): ConfigEntry[] {
  const masked = new Set(
    (Array.isArray(raw.maskedConfigKeys) ? raw.maskedConfigKeys : []).map(String),
  );
  return Object.entries(strMap(raw.config))
    .toSorted(([a], [b]) => a.localeCompare(b))
    .map(([key, value]) => ({ key, value, masked: masked.has(key) }));
}

export function parseApp(raw: Json): AppInfo {
  return {
    id: str(raw.id),
    name: str(raw.displayName),
    state: str(raw.lifecycleState),
    shape: str(raw.shape),
    compartmentId: str(raw.compartmentId),
    subnetIds: (Array.isArray(raw.subnetIds) ? raw.subnetIds : []).map(String),
    timeCreated: str(raw.timeCreated),
    syslogUrl: str(raw.syslogUrl),
    config: configEntries(raw),
    raw,
  };
}

export function parseFunction(raw: Json): FunctionInfo {
  const src = (raw.sourceDetails ?? {}) as Json;
  return {
    id: str(raw.id),
    appId: str(raw.applicationId),
    name: str(raw.displayName),
    state: str(raw.lifecycleState),
    image: str(src.image),
    imageDigest: str(src.imageDigest),
    memoryMb: num(raw.memoryInMBs),
    timeoutSec: num(raw.timeoutInSeconds),
    invokeEndpoint: str(raw.invokeEndpoint),
    timeCreated: str(raw.timeCreated),
    config: configEntries(raw),
    raw,
  };
}

export function parseWorkRequest(raw: Json): WorkRequestInfo {
  return {
    id: str(raw.id),
    operation: str(raw.operationType),
    status: str(raw.status),
    percentComplete: num(raw.percentComplete),
    resources: list(raw.resources).map((r) => ({
      entityType: str(r.entityType),
      actionType: str(r.actionType),
      identifier: str(r.identifier),
    })),
    timeAccepted: str(raw.timeAccepted),
    timeFinished: str(raw.timeFinished),
  };
}

export async function listApplications(fetchFn: FetchFn = fetch): Promise<AppInfo[]> {
  const body = await restCall(FUNCTIONS_PROXY_BASE, "GET", "/applications", undefined, fetchFn);
  return list(body.applications).map((a) => parseApp(a));
}

export async function getApplication(id: string, fetchFn: FetchFn = fetch): Promise<AppInfo> {
  return parseApp(
    await restCall(FUNCTIONS_PROXY_BASE, "GET", `/applications/${enc(id)}`, undefined, fetchFn),
  );
}

export async function listFunctions(
  appId: string,
  fetchFn: FetchFn = fetch,
): Promise<FunctionInfo[]> {
  const body = await restCall(
    FUNCTIONS_PROXY_BASE,
    "GET",
    `/applications/${enc(appId)}/functions`,
    undefined,
    fetchFn,
  );
  return list(body.functions).map((f) => parseFunction(f));
}

export async function getFunction(id: string, fetchFn: FetchFn = fetch): Promise<FunctionInfo> {
  return parseFunction(
    await restCall(FUNCTIONS_PROXY_BASE, "GET", `/functions/${enc(id)}`, undefined, fetchFn),
  );
}

/** Synchronous invocation through the in-process backend. */
export async function invokeFunction(
  id: string,
  input: { body: string; contentType: string; dryRun?: boolean },
  fetchFn: FetchFn = fetch,
): Promise<InvokeResult> {
  const out = await restCall(
    FUNCTIONS_PROXY_BASE,
    "POST",
    `/functions/${enc(id)}/invoke`,
    input,
    fetchFn,
  );
  return {
    status: num(out.status),
    callId: str(out.callId),
    headers: Object.entries(strMap(out.headers)).toSorted(([a], [b]) => a.localeCompare(b)),
    text: out.isText === true ? str(out.text) : null,
    base64: str(out.base64),
  };
}

/** Work requests, newest first; optionally only those touching one resource. */
export async function listWorkRequests(
  resourceId = "",
  fetchFn: FetchFn = fetch,
): Promise<WorkRequestInfo[]> {
  const q = resourceId ? `?resourceId=${enc(resourceId)}` : "";
  const body = await restCall(FUNCTIONS_PROXY_BASE, "GET", `/workrequests${q}`, undefined, fetchFn);
  return list(body.workRequests)
    .map((w) => parseWorkRequest(w))
    .toSorted((a, b) => b.timeAccepted.localeCompare(a.timeAccepted));
}

export async function listWorkRequestLogs(
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<WorkRequestLog[]> {
  const body = await restCall(
    FUNCTIONS_PROXY_BASE,
    "GET",
    `/workrequests/${enc(id)}/logs`,
    undefined,
    fetchFn,
  );
  return list(body.logs).map((l) => ({ timestamp: str(l.timestamp), message: str(l.message) }));
}

/** Compact OCID for display: keeps the kind and the last characters. */
export function shortOcid(ocid: string): string {
  const parts = ocid.split(".");
  if (parts.length < 4) return ocid;
  const tail = parts.at(-1) ?? "";
  return `${parts[1]}…${tail.slice(-8)}`;
}
