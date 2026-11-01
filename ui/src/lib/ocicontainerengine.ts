/** Client for the Oracle Container Engine (OKE) emulator: clusters, node pools and work requests via the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const OKE_PROXY_BASE = "/dashboard/api/v1/oci/containerengine";
export const OKE_COMPARTMENT_KEY = "gopherstack_ociokes_compartment";
export const OKE_MAX_POOL_SIZE = 1000;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type OkeCluster = {
  id: string;
  name: string;
  state: string;
  details: string;
  version: string;
  type: string;
  vcnId: string;
  kubernetesEndpoint: string;
  publicEndpoint: string;
  privateEndpoint: string;
  created: string;
  upgrades: string[];
};

export type OkeNode = {
  id: string;
  name: string;
  state: string;
  version: string;
  availabilityDomain: string;
  privateIp: string;
};

export type OkePlacement = { availabilityDomain: string; subnetId: string };

export type OkeNodePool = {
  id: string;
  clusterId: string;
  name: string;
  state: string;
  version: string;
  shape: string;
  ocpus: number;
  memoryGbs: number;
  size: number;
  legacySize: boolean;
  placements: OkePlacement[];
  nodes: OkeNode[];
};

export type OkeWorkRequest = {
  id: string;
  operation: string;
  status: string;
  accepted: string;
  finished: string;
  resource: string;
};

export function parseCluster(raw: Json): OkeCluster {
  const ep = obj(raw.endpoints);
  return {
    id: str(raw.id),
    name: str(raw.name),
    state: str(raw.lifecycleState),
    details: str(raw.lifecycleDetails),
    version: str(raw.kubernetesVersion),
    type: str(raw.type),
    vcnId: str(raw.vcnId),
    kubernetesEndpoint: str(ep.kubernetes),
    publicEndpoint: str(ep.publicEndpoint),
    privateEndpoint: str(ep.privateEndpoint),
    created: str(obj(raw.metadata).timeCreated),
    upgrades: list(raw.availableKubernetesUpgrades).map(String),
  };
}

export function parseNode(raw: Json): OkeNode {
  return {
    id: str(raw.id),
    name: str(raw.name),
    state: str(raw.lifecycleState),
    version: str(raw.kubernetesVersion),
    availabilityDomain: str(raw.availabilityDomain),
    privateIp: str(raw.privateIp),
  };
}

export function parseNodePool(raw: Json): OkeNodePool {
  const cfg = obj(raw.nodeConfigDetails);
  const shape = obj(raw.nodeShapeConfig);
  const nodes = list(raw.nodes).map((n) => parseNode(n));
  const legacy = Object.keys(cfg).length === 0;
  const perSubnet = num(raw.quantityPerSubnet);
  return {
    id: str(raw.id),
    clusterId: str(raw.clusterId),
    name: str(raw.name),
    state: str(raw.lifecycleState),
    version: str(raw.kubernetesVersion),
    shape: str(raw.nodeShape),
    ocpus: num(shape.ocpus),
    memoryGbs: num(shape.memoryInGBs),
    size: legacy ? perSubnet : num(cfg.size),
    legacySize: legacy,
    placements: list(cfg.placementConfigs).map((p) => ({
      availabilityDomain: str(p.availabilityDomain),
      subnetId: str(p.subnetId),
    })),
    nodes,
  };
}

export function parseWorkRequest(raw: Json): OkeWorkRequest {
  const res = obj(list(raw.resources)[0]);
  return {
    id: str(raw.id),
    operation: str(raw.operationType),
    status: str(raw.status),
    accepted: str(raw.timeAccepted),
    finished: str(raw.timeFinished),
    resource: str(res.identifier),
  };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(OKE_PROXY_BASE, fetchFn);

export async function listClusters(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<OkeCluster[]> {
  const items = await ociListPages(
    OKE_PROXY_BASE,
    "/api/clusters",
    `compartmentId=${enc(compartment)}`,
    "",
    fetchFn,
  );
  return items.map((r) => parseCluster(r));
}

/** Pools of a compartment; each is read individually because only that body carries the nodes. */
export async function listNodePools(
  compartment: string,
  clusterId: string,
  fetchFn: FetchFn = fetch,
): Promise<OkeNodePool[]> {
  const summaries = await ociListPages(
    OKE_PROXY_BASE,
    "/api/nodePools",
    `compartmentId=${enc(compartment)}&clusterId=${enc(clusterId)}`,
    "",
    fetchFn,
  );
  const pools = await Promise.all(
    summaries.map(async (s) =>
      parseNodePool(await ociGetOne(OKE_PROXY_BASE, `/api/nodePools/${enc(str(s.id))}`, fetchFn)),
    ),
  );
  return pools.toSorted((a, b) => a.name.localeCompare(b.name));
}

export async function listWorkRequests(
  compartment: string,
  clusterId: string,
  fetchFn: FetchFn = fetch,
): Promise<OkeWorkRequest[]> {
  const items = await ociListPages(
    OKE_PROXY_BASE,
    "/api/workRequests",
    `compartmentId=${enc(compartment)}&clusterId=${enc(clusterId)}&sortBy=TIME_ACCEPTED&sortOrder=DESC`,
    "",
    fetchFn,
  );
  return items.map((r) => parseWorkRequest(r));
}

export async function deleteCluster(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(OKE_PROXY_BASE, "DELETE", `/api/clusters/${enc(id)}`, undefined, fetchFn);
}

/** Sets a pool's node count; pools created with quantityPerSubnet are resized per subnet. */
export async function scaleNodePool(
  pool: Pick<OkeNodePool, "id" | "legacySize">,
  size: number,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const body = pool.legacySize ? { quantityPerSubnet: size } : { nodeConfigDetails: { size } };
  await restCall(OKE_PROXY_BASE, "PUT", `/api/nodePools/${enc(pool.id)}`, body, fetchFn);
}

export const validPoolSize = (v: string): boolean => {
  if (!/^\d+$/.test(v.trim())) return false;
  return Number(v) <= OKE_MAX_POOL_SIZE;
};

/** The CLI command that fetches a kubeconfig; the dashboard never handles cluster credentials itself. */
export const kubeconfigCommand = (c: Pick<OkeCluster, "id">, endpoint: string): string =>
  `oci ce cluster create-kubeconfig --cluster-id ${c.id} --file $HOME/.kube/config --endpoint ${endpoint}`;

export const readStoredOkeCompartment = () => readStoredValue(OKE_COMPARTMENT_KEY);
export const writeStoredOkeCompartment = (id: string) => writeStoredValue(OKE_COMPARTMENT_KEY, id);
