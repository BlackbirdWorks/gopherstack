/** Client for the Oracle Cloud Container Instances emulator through the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { GcpRestError, restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const CI_PROXY_BASE = "/dashboard/api/v1/oci/containerinstances";
export const CI_COMPARTMENT_KEY = "gopherstack_ocici_compartment";

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);

export type InstanceSummary = {
  id: string;
  name: string;
  state: string;
  details: string;
  shape: string;
  ocpus: number;
  memoryGBs: number;
  containerCount: number;
  volumeCount: number;
  restartPolicy: string;
  availabilityDomain: string;
  created: string;
};

export type ConfigFileInfo = { fileName: string; path: string; sizeBytes: number };
export type VolumeInfo = {
  name: string;
  type: string;
  backingStore: string;
  files: ConfigFileInfo[];
};
export type PullSecretInfo = { type: string; registry: string };

export type InstanceDetail = InstanceSummary & {
  gracefulShutdownSeconds: number | undefined;
  containers: { id: string; name: string }[];
  vnicIds: string[];
  volumes: VolumeInfo[];
  pullSecrets: PullSecretInfo[];
};

export type HealthCheckInfo = { name: string; type: string; status: string; details: string };

export type ContainerInfo = {
  id: string;
  name: string;
  image: string;
  state: string;
  details: string;
  exitCode: number | undefined;
  health: string;
  healthChecks: HealthCheckInfo[];
  restartCount: number;
  vcpusLimit: number | undefined;
  memoryLimitGBs: number | undefined;
  command: string[];
  args: string[];
  envNames: string[];
  mounts: { volume: string; path: string; readOnly: boolean }[];
};

export type WorkRequest = {
  id: string;
  type: string;
  status: string;
  percent: number;
  accepted: string;
  finished: string;
  resources: string;
};

export type WorkRequestError = { code: string; message: string; timestamp: string };

export type Shape = {
  name: string;
  processor: string;
  ocpuMin: number;
  ocpuMax: number;
  memoryMinGBs: number;
  memoryMaxGBs: number;
  defaultPerOcpu: number;
  maxPerOcpu: number;
};

export type Subnet = { id: string; name: string; cidr: string; vcnId: string };

export function parseSummary(raw: Json): InstanceSummary {
  const sc = obj(raw.shapeConfig);
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    details: str(raw.lifecycleDetails),
    shape: str(raw.shape),
    ocpus: num(sc.ocpus),
    memoryGBs: num(sc.memoryInGBs),
    containerCount: num(raw.containerCount),
    volumeCount: num(raw.volumeCount),
    restartPolicy: str(raw.containerRestartPolicy),
    availabilityDomain: str(raw.availabilityDomain),
    created: str(raw.timeCreated),
  };
}

export function parseDetail(raw: Json): InstanceDetail {
  return {
    ...parseSummary(raw),
    gracefulShutdownSeconds:
      raw.gracefulShutdownTimeoutInSeconds === undefined
        ? undefined
        : num(raw.gracefulShutdownTimeoutInSeconds),
    containers: list(raw.containers).map((c) => ({
      id: str(c.containerId),
      name: str(c.displayName),
    })),
    vnicIds: list(raw.vnics).map((v) => str(v.vnicId)),
    volumes: list(raw.volumes).map((v) => ({
      name: str(v.name),
      type: str(v.volumeType),
      backingStore: str(v.backingStore),
      files: list(v.configs).map((f) => ({
        fileName: str(f.fileName),
        path: str(f.path),
        sizeBytes: num(f.sizeInBytes),
      })),
    })),
    pullSecrets: list(raw.imagePullSecrets).map((s) => ({
      type: str(s.secretType),
      registry: str(s.registryEndpoint),
    })),
  };
}

/** Worst health across a container's checks: UNHEALTHY beats UNKNOWN beats HEALTHY; none means no checks. */
export function overallHealth(checks: readonly Pick<HealthCheckInfo, "status">[]): string {
  if (checks.length === 0) return "";
  if (checks.some((c) => c.status === "UNHEALTHY")) return "UNHEALTHY";
  if (checks.some((c) => c.status !== "HEALTHY")) return "UNKNOWN";
  return "HEALTHY";
}

export function parseContainer(raw: Json): ContainerInfo {
  const rc = obj(raw.resourceConfig);
  const checks = list(raw.healthChecks).map((h) => ({
    name: str(h.name),
    type: str(h.healthCheckType),
    status: str(h.status),
    details: str(h.statusDetails),
  }));
  return {
    id: str(raw.id),
    name: str(raw.displayName),
    image: str(raw.imageUrl),
    state: str(raw.lifecycleState),
    details: str(raw.lifecycleDetails),
    exitCode: raw.exitCode === undefined ? undefined : num(raw.exitCode),
    health: overallHealth(checks),
    healthChecks: checks,
    restartCount: num(raw.containerRestartAttemptCount),
    vcpusLimit: rc.vcpusLimit === undefined ? undefined : num(rc.vcpusLimit),
    memoryLimitGBs: rc.memoryLimitInGBs === undefined ? undefined : num(rc.memoryLimitInGBs),
    command: strs(raw.command),
    args: strs(raw.arguments),
    envNames: strs(raw.environmentVariableNames),
    mounts: list(raw.volumeMounts).map((m) => ({
      volume: str(m.volumeName),
      path: str(m.mountPath),
      readOnly: m.isReadOnly === true,
    })),
  };
}

export function parseWorkRequest(raw: Json): WorkRequest {
  return {
    id: str(raw.id),
    type: str(raw.operationType),
    status: str(raw.status),
    percent: num(raw.percentComplete),
    accepted: str(raw.timeAccepted),
    finished: str(raw.timeFinished),
    resources: list(raw.resources)
      .map((r) => `${str(r.actionType)} ${str(r.entityType)}`.trim())
      .join(", "),
  };
}

export function parseShape(raw: Json): Shape {
  const o = obj(raw.ocpuOptions);
  const m = obj(raw.memoryOptions);
  return {
    name: str(raw.name),
    processor: str(raw.processorDescription),
    ocpuMin: num(o.min),
    ocpuMax: num(o.max),
    memoryMinGBs: num(m.minInGBs),
    memoryMaxGBs: num(m.maxInGBs),
    defaultPerOcpu: num(m.defaultPerOcpuInGBs),
    maxPerOcpu: num(m.maxPerOcpuInGBs),
  };
}

/** Memory range allowed for a shape at an OCPU count: at least 1 GB per OCPU, at most the per-OCPU and shape maximum. */
export function memoryRange(shape: Shape, ocpus: number): { min: number; max: number } {
  return {
    min: Math.max(shape.memoryMinGBs, ocpus),
    max: Math.min(shape.memoryMaxGBs, shape.maxPerOcpu * ocpus),
  };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(CI_PROXY_BASE, fetchFn);

export async function getEngine(fetchFn: FetchFn = fetch): Promise<string> {
  return str((await restCall(CI_PROXY_BASE, "GET", "/engine", undefined, fetchFn)).engine);
}

export async function listAvailabilityDomains(fetchFn: FetchFn = fetch): Promise<string[]> {
  const body = await restCall(CI_PROXY_BASE, "GET", "/availabilitydomains", undefined, fetchFn);
  return list(body.items).map((i) => str(i.name));
}

export async function listSubnets(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<Subnet[]> {
  const body = await restCall(
    CI_PROXY_BASE,
    "GET",
    `/subnets?compartmentId=${enc(compartment)}`,
    undefined,
    fetchFn,
  );
  return list(body.items).map((s) => ({
    id: str(s.id),
    name: str(s.displayName) || str(s.id),
    cidr: str(s.cidrBlock),
    vcnId: str(s.vcnId),
  }));
}

const compQuery = (compartment: string) => `compartmentId=${enc(compartment)}`;

export async function listInstances(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<InstanceSummary[]> {
  const items = await ociListPages(
    CI_PROXY_BASE,
    "/api/containerInstances",
    compQuery(compartment),
    "items",
    fetchFn,
  );
  return items.map((i) => parseSummary(i)).filter((i) => i.state !== "DELETED");
}

export async function getInstance(id: string, fetchFn: FetchFn = fetch): Promise<InstanceDetail> {
  return parseDetail(await ociGetOne(CI_PROXY_BASE, `/api/containerInstances/${enc(id)}`, fetchFn));
}

export async function listContainers(
  compartment: string,
  instanceId: string,
  fetchFn: FetchFn = fetch,
): Promise<ContainerInfo[]> {
  const items = await ociListPages(
    CI_PROXY_BASE,
    "/api/containers",
    `${compQuery(compartment)}&containerInstanceId=${enc(instanceId)}`,
    "items",
    fetchFn,
  );
  return Promise.all(
    items.map(async (i) =>
      parseContainer(await ociGetOne(CI_PROXY_BASE, `/api/containers/${enc(str(i.id))}`, fetchFn)),
    ),
  );
}

export async function listWorkRequests(
  compartment: string,
  instanceId: string,
  fetchFn: FetchFn = fetch,
): Promise<WorkRequest[]> {
  const items = await ociListPages(
    CI_PROXY_BASE,
    "/api/workRequests",
    `${compQuery(compartment)}&resourceId=${enc(instanceId)}`,
    "items",
    fetchFn,
  );
  return items.map((w) => parseWorkRequest(w));
}

export async function listWorkRequestErrors(
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<WorkRequestError[]> {
  const items = await ociListPages(
    CI_PROXY_BASE,
    `/api/workRequests/${enc(id)}/errors`,
    "",
    "items",
    fetchFn,
  );
  return items.map((e) => ({
    code: str(e.code),
    message: str(e.message),
    timestamp: str(e.timestamp),
  }));
}

export async function listShapes(compartment: string, fetchFn: FetchFn = fetch): Promise<Shape[]> {
  const items = await ociListPages(
    CI_PROXY_BASE,
    "/api/containerInstanceShapes",
    compQuery(compartment),
    "items",
    fetchFn,
  );
  return items.map((s) => parseShape(s));
}

export type InstanceAction = "start" | "stop" | "restart";

/** Start, stop and restart are only offered from the states the service accepts them in. */
export function actionEnabled(
  action: InstanceAction,
  state: string,
  workRequestInProgress: boolean,
): boolean {
  if (workRequestInProgress) return false;
  return action === "start" ? state === "INACTIVE" : state === "ACTIVE";
}

export const isBusy = (state: string): boolean =>
  state === "CREATING" || state === "UPDATING" || state === "DELETING";

export const inProgress = (wrs: readonly Pick<WorkRequest, "status">[]): boolean =>
  wrs.some((w) => w.status === "IN_PROGRESS" || w.status === "ACCEPTED");

export async function instanceAction(
  id: string,
  action: InstanceAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    CI_PROXY_BASE,
    "POST",
    `/api/containerInstances/${enc(id)}/actions/${action}`,
    undefined,
    fetchFn,
  );
}

export async function deleteInstance(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(CI_PROXY_BASE, "DELETE", `/api/containerInstances/${enc(id)}`, undefined, fetchFn);
}

export async function renameInstance(
  id: string,
  displayName: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    CI_PROXY_BASE,
    "PUT",
    `/api/containerInstances/${enc(id)}`,
    { displayName },
    fetchFn,
  );
}

export type EnvPair = { name: string; value: string };

export type NewInstance = {
  compartmentId: string;
  availabilityDomain: string;
  displayName: string;
  shape: string;
  ocpus: number;
  memoryInGBs: number;
  restartPolicy: string;
  subnetId: string;
  image: string;
  containerName: string;
  command: string;
  args: string;
  env: EnvPair[];
};

/** Splits a command line on whitespace, honouring single and double quotes. */
export function splitArgs(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let quote = "";
  let has = false;
  for (const ch of line) {
    if (quote) {
      if (ch === quote) quote = "";
      else cur += ch;
    } else if (ch === '"' || ch === "'") {
      quote = ch;
      has = true;
    } else if (/\s/.test(ch)) {
      if (has || cur) out.push(cur);
      cur = "";
      has = false;
    } else {
      cur += ch;
    }
  }
  if (has || cur) out.push(cur);
  return out;
}

export const validEnvName = (n: string): boolean => /^[A-Za-z_][A-Za-z0-9_.-]{0,127}$/.test(n);

export function createBody(n: NewInstance): Json {
  const env: Record<string, string> = {};
  for (const p of n.env) if (p.name.trim() !== "") env[p.name.trim()] = p.value;
  const command = splitArgs(n.command);
  const args = splitArgs(n.args);
  const container: Json = { imageUrl: n.image.trim() };
  if (n.containerName.trim() !== "") container.displayName = n.containerName.trim();
  if (command.length > 0) container.command = command;
  if (args.length > 0) container.arguments = args;
  if (Object.keys(env).length > 0) container.environmentVariables = env;
  const body: Json = {
    compartmentId: n.compartmentId,
    availabilityDomain: n.availabilityDomain,
    shape: n.shape,
    shapeConfig: { ocpus: n.ocpus, memoryInGBs: n.memoryInGBs },
    containerRestartPolicy: n.restartPolicy,
    vnics: [{ subnetId: n.subnetId.trim() }],
    containers: [container],
  };
  if (n.displayName.trim() !== "") body.displayName = n.displayName.trim();
  return body;
}

export function validateNew(n: NewInstance, shape: Shape | undefined): string {
  if (!n.shape || !shape) return "Pick a shape";
  if (!n.availabilityDomain.trim()) return "Availability domain is required";
  if (!n.subnetId.trim()) return "Subnet is required";
  if (!n.image.trim()) return "Container image is required";
  if (!(n.ocpus >= shape.ocpuMin && n.ocpus <= shape.ocpuMax)) {
    return `OCPUs must be between ${shape.ocpuMin} and ${shape.ocpuMax}`;
  }
  const r = memoryRange(shape, n.ocpus);
  if (!(n.memoryInGBs >= r.min && n.memoryInGBs <= r.max)) {
    return `Memory must be between ${r.min} and ${r.max} GB`;
  }
  const bad = n.env.find((p) => p.name.trim() !== "" && !validEnvName(p.name.trim()));
  if (bad) return `Invalid environment variable name: ${bad.name}`;
  return "";
}

export async function createInstance(n: NewInstance, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(CI_PROXY_BASE, "POST", "/api/containerInstances", createBody(n), fetchFn);
}

export type LogsResult = { text: string; truncated: boolean; totalBytes: number };

/** Container logs as text. A 409 (no docker engine, or still creating) throws with the service's own message. */
export async function retrieveLogs(
  containerId: string,
  previous = false,
  fetchFn: FetchFn = fetch,
): Promise<LogsResult> {
  let res: Response;
  try {
    res = await fetchFn(
      `${CI_PROXY_BASE}/api/containers/${enc(containerId)}/actions/retrieveLogs${previous ? "?isPrevious=true" : ""}`,
      { method: "POST" },
    );
  } catch (e) {
    throw new GcpRestError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  const text = await res.text();
  if (!res.ok) {
    let body: Json = {};
    try {
      body = obj(JSON.parse(text));
    } catch {
      // non-JSON error body: fall back to the status text
    }
    const proxy = obj(body.error);
    throw new GcpRestError(
      res.status,
      str(proxy.status) || str(body.code),
      str(proxy.message) || str(body.message) || res.statusText || `HTTP ${res.status}`,
    );
  }
  const total = Number(res.headers.get("X-Logs-Total-Bytes"));
  return {
    text,
    truncated: res.headers.get("X-Logs-Truncated") === "true",
    totalBytes: total > 0 ? total : text.length,
  };
}

export const readStoredCiCompartment = () => readStoredValue(CI_COMPARTMENT_KEY);
export const writeStoredCiCompartment = (id: string) => writeStoredValue(CI_COMPARTMENT_KEY, id);
