/** Client for the Workflows emulator (workflows and executions) via the dashboard's in-process REST proxy. */

import { listAll, restCall, shortName, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const WORKFLOWS_PROXY_BASE = "/dashboard/api/v1/gcp/workflows/v1";
export const WF_PROJECT_KEY = "gopherstack_workflows_project";
export const WF_LOCATION_KEY = "gopherstack_workflows_location";
export const DEFAULT_WF_LOCATION = "us-central1";

const enc = encodeURIComponent;
const DEFAULT_PROJECT = "gopherstack-local";

export type WorkflowScope = { project: string; location: string };

export type WorkflowInfo = {
  name: string;
  id: string;
  state: string;
  description: string;
  revisionId: string;
  serviceAccount: string;
  createTime: string;
  updateTime: string;
  source: string;
  labels: Record<string, string>;
  raw: Json;
};

export type ExecutionInfo = {
  name: string;
  id: string;
  state: string;
  argument: string;
  result: string;
  errorPayload: string;
  errorContext: string;
  startTime: string;
  endTime: string;
  revisionId: string;
  raw: Json;
};

export function parseWorkflow(raw: Json): WorkflowInfo {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    state: str(raw.state) || "STATE_UNSPECIFIED",
    description: str(raw.description),
    revisionId: str(raw.revisionId),
    serviceAccount: str(raw.serviceAccount),
    createTime: str(raw.createTime),
    updateTime: str(raw.updateTime),
    source: str(raw.sourceContents),
    labels: strMap(raw.labels),
    raw,
  };
}

export function parseExecution(raw: Json): ExecutionInfo {
  const err = (raw.error ?? {}) as Json;
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    state: str(raw.state) || "STATE_UNSPECIFIED",
    argument: str(raw.argument),
    result: str(raw.result),
    errorPayload: str(err.payload),
    errorContext: str(err.context),
    startTime: str(raw.startTime),
    endTime: str(raw.endTime),
    revisionId: str(raw.workflowRevisionId),
    raw,
  };
}

/** Pretty-prints a JSON document held in a string field; falls back to the raw text. */
export function prettyJson(text: string): string {
  if (!text) return "";
  try {
    return JSON.stringify(JSON.parse(text), null, 2);
  } catch {
    return text;
  }
}

/** Validates an execution argument: empty (no argument) or a JSON document. */
export function validateArgument(text: string): string | null {
  if (!text.trim()) return null;
  try {
    JSON.parse(text);
    return null;
  } catch (e) {
    return e instanceof Error ? e.message : "invalid JSON";
  }
}

const parentPath = (s: WorkflowScope) =>
  `/projects/${enc(s.project)}/locations/${enc(s.location)}/workflows`;

export async function listWorkflows(
  s: WorkflowScope,
  fetchFn: FetchFn = fetch,
): Promise<WorkflowInfo[]> {
  return (await listAll(WORKFLOWS_PROXY_BASE, parentPath(s), "workflows", fetchFn)).map((w) =>
    parseWorkflow(w),
  );
}

export async function getWorkflow(
  name: string,
  revisionId = "",
  fetchFn: FetchFn = fetch,
): Promise<WorkflowInfo> {
  const q = revisionId ? `?revisionId=${enc(revisionId)}` : "";
  return parseWorkflow(
    await restCall(WORKFLOWS_PROXY_BASE, "GET", `/${name}${q}`, undefined, fetchFn),
  );
}

export async function listRevisions(
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<WorkflowInfo[]> {
  return (await listAll(WORKFLOWS_PROXY_BASE, `/${name}:listRevisions`, "workflows", fetchFn)).map(
    (w) => parseWorkflow(w),
  );
}

export async function listExecutions(
  workflowName: string,
  fetchFn: FetchFn = fetch,
): Promise<ExecutionInfo[]> {
  return (
    await listAll(WORKFLOWS_PROXY_BASE, `/${workflowName}/executions`, "executions", fetchFn)
  ).map((e) => parseExecution(e));
}

export async function runExecution(
  workflowName: string,
  argument: string,
  fetchFn: FetchFn = fetch,
): Promise<ExecutionInfo> {
  const body: Json = argument.trim() ? { argument: argument.trim() } : {};
  return parseExecution(
    await restCall(WORKFLOWS_PROXY_BASE, "POST", `/${workflowName}/executions`, body, fetchFn),
  );
}

export async function cancelExecution(
  executionName: string,
  fetchFn: FetchFn = fetch,
): Promise<ExecutionInfo> {
  return parseExecution(
    await restCall(WORKFLOWS_PROXY_BASE, "POST", `/${executionName}:cancel`, {}, fetchFn),
  );
}

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredWfProject = () => readStored(WF_PROJECT_KEY, DEFAULT_PROJECT);
export const readStoredWfLocation = () => readStored(WF_LOCATION_KEY, DEFAULT_WF_LOCATION);

export function writeStoredWfScope(project: string, location: string): void {
  try {
    localStorage.setItem(WF_PROJECT_KEY, project);
    localStorage.setItem(WF_LOCATION_KEY, location);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
