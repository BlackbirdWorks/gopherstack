/** Client for the Google Cloud API Gateway emulator (APIs, configs, gateways, operations, data-plane try-it) via the dashboard's in-process proxy. */

import {
  base64ToUtf8,
  listAll,
  restCall,
  shortName,
  str,
  strMap,
  type FetchFn,
  type Json,
} from "./gcprest.ts";
import type { TryResponse } from "./ociapigateway.ts";
import { cleanQuery, readTryResponse, type HeaderPair } from "./tryit.ts";

export const GW_PROXY_BASE = "/dashboard/api/v1/gcp/apigateway/v1";
export const GW_PROJECT_KEY = "gopherstack_apigateway_project";
export const GW_REGIONS = [
  "us-central1",
  "us-east1",
  "us-east4",
  "us-west2",
  "us-west3",
  "us-west4",
  "europe-west1",
  "europe-west2",
  "asia-northeast1",
  "australia-southeast1",
];

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});

export type GwApi = {
  name: string;
  id: string;
  displayName: string;
  state: string;
  managedService: string;
  createTime: string;
  labels: Record<string, string>;
};

export type GwConfig = {
  name: string;
  id: string;
  api: string;
  displayName: string;
  state: string;
  serviceConfigId: string;
  serviceAccount: string;
  createTime: string;
  documents: GwDocument[];
};

export type GwDocument = { path: string; contents: string; decoded: boolean };

export type GwGateway = {
  name: string;
  id: string;
  location: string;
  displayName: string;
  state: string;
  defaultHostname: string;
  apiConfig: string;
  createTime: string;
};

export type GwDiagnostic = { location: string; message: string };

export type GwOperation = {
  name: string;
  id: string;
  location: string;
  verb: string;
  target: string;
  done: boolean;
  status: "RUNNING" | "SUCCEEDED" | "FAILED";
  statusMessage: string;
  createTime: string;
  endTime: string;
  diagnostics: GwDiagnostic[];
};

export type GwTryRequest = {
  method: string;
  path: string;
  query: string;
  body: string;
  headers: HeaderPair[];
  apiKey: string;
};

export const parseApi = (raw: Json): GwApi => ({
  name: str(raw.name),
  id: shortName(str(raw.name)),
  displayName: str(raw.displayName),
  state: str(raw.state),
  managedService: str(raw.managedService),
  createTime: str(raw.createTime),
  labels: strMap(raw.labels),
});

/** Decodes an OpenAPI document's base64 contents; undecodable bytes are kept encoded. */
export function parseDocument(raw: Json): GwDocument {
  const doc = obj(raw.document);
  const contents = str(doc.contents);
  const text = base64ToUtf8(contents);
  return { path: str(doc.path), contents: text ?? contents, decoded: text !== null };
}

export function parseConfig(raw: Json): GwConfig {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    api: name.split("/configs/")[0] ?? "",
    displayName: str(raw.displayName),
    state: str(raw.state),
    serviceConfigId: str(raw.serviceConfigId),
    serviceAccount: str(raw.gatewayServiceAccount),
    createTime: str(raw.createTime),
    documents: list(raw.openapiDocuments).map((d) => parseDocument(d)),
  };
}

export const parseGateway = (raw: Json): GwGateway => {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    location: name.split("/")[3] ?? "",
    displayName: str(raw.displayName),
    state: str(raw.state),
    defaultHostname: str(raw.defaultHostname),
    apiConfig: str(raw.apiConfig),
    createTime: str(raw.createTime),
  };
};

export function parseOperation(raw: Json): GwOperation {
  const name = str(raw.name);
  const meta = obj(raw.metadata);
  const done = raw.done === true;
  const err = obj(raw.error);
  const failed = done && Object.keys(err).length > 0;
  return {
    name,
    id: shortName(name),
    location: name.split("/")[3] ?? "",
    verb: str(meta.verb),
    target: str(meta.target),
    done,
    status: done ? (failed ? "FAILED" : "SUCCEEDED") : "RUNNING",
    statusMessage: str(meta.statusMessage) || str(err.message),
    createTime: str(meta.createTime),
    endTime: str(meta.endTime),
    diagnostics: list(meta.diagnostics).map((d) => ({
      location: str(d.location),
      message: str(d.message),
    })),
  };
}

/** Diagnostics of the newest operation that targeted a resource, for a failed config. */
export function diagnosticsFor(target: string, ops: GwOperation[]): GwDiagnostic[] {
  const hits = ops
    .filter((o) => o.target === target && o.diagnostics.length > 0)
    .toSorted((a, b) => b.createTime.localeCompare(a.createTime));
  return hits[0]?.diagnostics ?? [];
}

const parent = (project: string, location: string) =>
  `/projects/${enc(project)}/locations/${enc(location)}`;

export async function listProjects(fetchFn: FetchFn = fetch): Promise<string[]> {
  const body = await restCall(GW_PROXY_BASE, "GET", "/projects", undefined, fetchFn);
  return Array.isArray(body.projects) ? body.projects.map(String) : [];
}

export async function listApis(project: string, fetchFn: FetchFn = fetch): Promise<GwApi[]> {
  return (await listAll(GW_PROXY_BASE, `${parent(project, "global")}/apis`, "apis", fetchFn)).map(
    (a) => parseApi(a),
  );
}

export async function listConfigs(apiName: string, fetchFn: FetchFn = fetch): Promise<GwConfig[]> {
  return (await listAll(GW_PROXY_BASE, `/${apiName}/configs`, "apiConfigs", fetchFn)).map((c) =>
    parseConfig(c),
  );
}

/** Full view of one config, including its decoded OpenAPI documents. */
export async function getConfig(name: string, fetchFn: FetchFn = fetch): Promise<GwConfig> {
  return parseConfig(await restCall(GW_PROXY_BASE, "GET", `/${name}`, undefined, fetchFn));
}

export async function listGateways(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<GwGateway[]> {
  return (
    await listAll(GW_PROXY_BASE, `${parent(project, "-")}/gateways`, "gateways", fetchFn)
  ).map((g) => parseGateway(g));
}

export async function createGateway(
  project: string,
  region: string,
  id: string,
  displayName: string,
  apiConfig: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    GW_PROXY_BASE,
    "POST",
    `${parent(project, region)}/gateways?gatewayId=${enc(id)}`,
    { displayName: displayName.trim() || undefined, apiConfig },
    fetchFn,
  );
}

export async function deleteGateway(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(GW_PROXY_BASE, "DELETE", `/${name}`, undefined, fetchFn);
}

/** Operations of the global location and of each given region, newest first. */
export async function listOperations(
  project: string,
  regions: string[],
  fetchFn: FetchFn = fetch,
): Promise<GwOperation[]> {
  const locations = ["global", ...new Set(regions)];
  const pages = await Promise.all(
    locations.map((l) =>
      listAll(GW_PROXY_BASE, `${parent(project, l)}/operations`, "operations", fetchFn),
    ),
  );
  return pages
    .flat()
    .map((o) => parseOperation(o))
    .toSorted((a, b) => b.createTime.localeCompare(a.createTime));
}

/** Sends a request through the selected gateway's data plane; the proxy fixes the destination. */
export async function tryGateway(
  gatewayName: string,
  req: GwTryRequest,
  fetchFn: FetchFn = fetch,
): Promise<TryResponse> {
  const body = await restCall(
    GW_PROXY_BASE,
    "POST",
    `/${gatewayName}:invoke`,
    {
      method: req.method,
      path: req.path,
      query: cleanQuery(req.query),
      body: req.body,
      headers: req.headers.filter((h) => h.name.trim()),
      apiKey: req.apiKey || undefined,
    },
    fetchFn,
  );
  return readTryResponse(body);
}

export const isActive = (s: { state: string }): boolean => s.state === "ACTIVE";

/** Valid gateway and config ids: lowercase letters, digits and dashes, starting with a letter. */
export const validGatewayId = (v: string): boolean => /^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$/.test(v);

export function readStoredProject(): string {
  try {
    return localStorage.getItem(GW_PROJECT_KEY) ?? "";
  } catch {
    return "";
  }
}

export function writeStoredProject(project: string): void {
  try {
    localStorage.setItem(GW_PROJECT_KEY, project);
  } catch {
    // storage unavailable: the choice just is not remembered
  }
}
