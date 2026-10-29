/** Client for the Kubernetes Engine emulator (clusters, node pools, operations) via the dashboard's in-process REST proxy. */

import { restCall, shortName, str, type FetchFn, type Json } from "./gcprest.ts";

export const GKE_PROXY_BASE = "/dashboard/api/v1/gcp/container/v1";
export const GKE_PROJECT_KEY = "gopherstack_gke_project";
export const DEFAULT_GKE_PROJECT = "gopherstack-local";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type Autoscaling = { enabled: boolean; min: number; max: number };

export type NodePool = {
  name: string;
  status: string;
  version: string;
  machineType: string;
  diskSizeGb: number;
  initialNodeCount: number;
  autoscaling: Autoscaling;
  locations: string[];
};

export type Cluster = {
  name: string;
  /** Full resource name: projects/p/locations/l/clusters/c. */
  path: string;
  location: string;
  status: string;
  statusMessage: string;
  masterVersion: string;
  nodeVersion: string;
  endpoint: string;
  releaseChannel: string;
  network: string;
  subnetwork: string;
  currentNodeCount: number;
  created: string;
  pools: NodePool[];
};

export type GkeOperation = {
  name: string;
  type: string;
  status: string;
  target: string;
  location: string;
  start: string;
  end: string;
  detail: string;
};

export function parseAutoscaling(raw: unknown): Autoscaling {
  const a = obj(raw);
  return { enabled: a.enabled === true, min: num(a.minNodeCount), max: num(a.maxNodeCount) };
}

export function parseNodePool(raw: Json): NodePool {
  const cfg = obj(raw.config);
  return {
    name: str(raw.name),
    status: str(raw.status),
    version: str(raw.version),
    machineType: str(cfg.machineType),
    diskSizeGb: num(cfg.diskSizeGb),
    initialNodeCount: num(raw.initialNodeCount),
    autoscaling: parseAutoscaling(raw.autoscaling),
    locations: Array.isArray(raw.locations) ? raw.locations.map(String) : [],
  };
}

export function parseCluster(raw: Json, project: string): Cluster {
  const name = str(raw.name);
  const location = str(raw.location);
  return {
    name,
    path: `projects/${project}/locations/${location}/clusters/${name}`,
    location,
    status: str(raw.status),
    statusMessage: str(raw.statusMessage),
    masterVersion: str(raw.currentMasterVersion),
    nodeVersion: str(raw.currentNodeVersion),
    endpoint: str(raw.endpoint),
    releaseChannel: str(obj(raw.releaseChannel).channel),
    network: str(raw.network),
    subnetwork: str(raw.subnetwork),
    currentNodeCount: num(raw.currentNodeCount),
    created: str(raw.createTime),
    pools: list(raw.nodePools).map((r) => parseNodePool(r)),
  };
}

export function parseOperation(raw: Json): GkeOperation {
  return {
    name: str(raw.name),
    type: str(raw.operationType),
    status: str(raw.status),
    target: shortName(str(raw.targetLink)),
    location: str(raw.location),
    start: str(raw.startTime),
    end: str(raw.endTime),
    detail: str(raw.statusMessage) || str(obj(raw.error).message),
  };
}

/** Whether a cluster/pool is in a steady state in which actions make sense. */
export const isBusy = (status: string): boolean =>
  ["PROVISIONING", "RECONCILING", "STOPPING"].includes(status);

export function releaseChannelLabel(channel: string): string {
  if (!channel || channel === "UNSPECIFIED") return "none";
  return channel.toLowerCase();
}

export async function listClusters(project: string, fetchFn: FetchFn = fetch): Promise<Cluster[]> {
  const body = await restCall(
    GKE_PROXY_BASE,
    "GET",
    `/projects/${enc(project)}/locations/-/clusters`,
    undefined,
    fetchFn,
  );
  return list(body.clusters)
    .map((c) => parseCluster(c, project))
    .toSorted((a, b) => a.location.localeCompare(b.location) || a.name.localeCompare(b.name));
}

/** Current node count per zone of every pool, by pool name. */
export async function getPoolSizes(
  clusterPath: string,
  fetchFn: FetchFn = fetch,
): Promise<Record<string, number>> {
  const body = await restCall(
    GKE_PROXY_BASE,
    "GET",
    `/${clusterPath}:poolSizes`,
    undefined,
    fetchFn,
  );
  return Object.fromEntries(Object.entries(body).map(([k, v]) => [k, num(v)]));
}

export async function resizeNodePool(
  clusterPath: string,
  pool: string,
  nodeCount: number,
  fetchFn: FetchFn = fetch,
): Promise<GkeOperation> {
  return parseOperation(
    await restCall(
      GKE_PROXY_BASE,
      "POST",
      `/${clusterPath}/nodePools/${enc(pool)}:setSize`,
      { nodeCount },
      fetchFn,
    ),
  );
}

export async function deleteCluster(
  clusterPath: string,
  fetchFn: FetchFn = fetch,
): Promise<GkeOperation> {
  return parseOperation(
    await restCall(GKE_PROXY_BASE, "DELETE", `/${clusterPath}`, undefined, fetchFn),
  );
}

export async function listOperations(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<GkeOperation[]> {
  const body = await restCall(
    GKE_PROXY_BASE,
    "GET",
    `/projects/${enc(project)}/locations/-/operations`,
    undefined,
    fetchFn,
  );
  return list(body.operations)
    .map((r) => parseOperation(r))
    .toSorted((a, b) => b.start.localeCompare(a.start));
}

/** Whether `value` is a valid node count for SetNodePoolSize. */
export const validNodeCount = (value: string | number): boolean => {
  const text = String(value).trim();
  return /^\d+$/.test(text) && Number(text) <= 1000;
};

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredGkeProject = () => readStored(GKE_PROJECT_KEY, DEFAULT_GKE_PROJECT);

export function writeStoredGkeProject(project: string): void {
  try {
    localStorage.setItem(GKE_PROJECT_KEY, project);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
