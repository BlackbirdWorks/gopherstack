/** Client for Azure Kubernetes Service: managed clusters, agent pools and upgrade profiles via the dashboard's in-process ARM proxy. */

import { parseArmId } from "./azurearm.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const AKS_PROXY_BASE = "/dashboard/api/v1/azure/aks";
export const AKS_API_VERSION = "2025-05-01";
export const AKS_MAX_POOL_COUNT = 1000;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const strs = (v: unknown): string[] =>
  Array.isArray(v) ? v.filter((x) => typeof x === "string") : [];

export type PowerAction = "start" | "stop";

export type NetworkProfile = {
  plugin: string;
  pluginMode: string;
  policy: string;
  podCidr: string;
  serviceCidr: string;
  dnsServiceIp: string;
  loadBalancerSku: string;
  outboundType: string;
};

export type AgentPool = {
  name: string;
  mode: string;
  vmSize: string;
  count: number;
  autoscale: boolean;
  minCount: number;
  maxCount: number;
  zones: string[];
  osType: string;
  version: string;
  power: string;
  state: string;
};

export type AksCluster = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  version: string;
  currentVersion: string;
  power: string;
  state: string;
  fqdn: string;
  private: boolean;
  nodeResourceGroup: string;
  tier: string;
  network: NetworkProfile;
  pools: AgentPool[];
};

export type UpgradeTarget = { version: string; preview: boolean };

export type UpgradeProfile = {
  controlPlane: { version: string; upgrades: UpgradeTarget[] };
  pools: { name: string; version: string; upgrades: UpgradeTarget[] }[];
};

export function parsePool(raw: Json): AgentPool {
  const p = Object.keys(obj(raw.properties)).length > 0 ? obj(raw.properties) : raw;
  return {
    name: str(raw.name),
    mode: str(p.mode),
    vmSize: str(p.vmSize),
    count: num(p.count),
    autoscale: p.enableAutoScaling === true,
    minCount: num(p.minCount),
    maxCount: num(p.maxCount),
    zones: strs(p.availabilityZones),
    osType: str(p.osType),
    version: str(p.currentOrchestratorVersion) || str(p.orchestratorVersion),
    power: str(obj(p.powerState).code),
    state: str(p.provisioningState),
  };
}

export function parseNetwork(raw: unknown): NetworkProfile {
  const n = obj(raw);
  return {
    plugin: str(n.networkPlugin),
    pluginMode: str(n.networkPluginMode),
    policy: str(n.networkPolicy),
    podCidr: str(n.podCidr),
    serviceCidr: str(n.serviceCidr),
    dnsServiceIp: str(n.dnsServiceIP),
    loadBalancerSku: str(n.loadBalancerSku),
    outboundType: str(n.outboundType),
  };
}

export function parseCluster(raw: Json): AksCluster {
  const p = obj(raw.properties);
  const id = str(raw.id);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    version: str(p.kubernetesVersion),
    currentVersion: str(p.currentKubernetesVersion),
    power: str(obj(p.powerState).code),
    state: str(p.provisioningState),
    fqdn: str(p.fqdn) || str(p.privateFQDN),
    private: !str(p.fqdn) && !!str(p.privateFQDN),
    nodeResourceGroup: str(p.nodeResourceGroup),
    tier: str(obj(raw.sku).tier),
    network: parseNetwork(p.networkProfile),
    pools: list(p.agentPoolProfiles).map((r) => parsePool(r)),
  };
}

const targets = (v: unknown): UpgradeTarget[] =>
  list(v).map((u) => ({ version: str(u.kubernetesVersion), preview: u.isPreview === true }));

export function parseUpgradeProfile(raw: Json): UpgradeProfile {
  const p = obj(raw.properties);
  const cp = obj(p.controlPlaneProfile);
  return {
    controlPlane: { version: str(cp.kubernetesVersion), upgrades: targets(cp.upgrades) },
    pools: list(p.agentPoolProfiles).map((r) => ({
      name: str(r.name),
      version: str(r.kubernetesVersion),
      upgrades: targets(r.upgrades),
    })),
  };
}

const withVersion = (path: string) => `${path}?api-version=${AKS_API_VERSION}`;
const subPath = (s: string) => `/subscriptions/${enc(s)}`;
const clusterPath = (s: string, rg: string, name: string) =>
  `${subPath(s)}/resourceGroups/${enc(rg)}/providers/Microsoft.ContainerService/managedClusters/${enc(name)}`;

export async function listClusters(sub: string, fetchFn: FetchFn = fetch): Promise<AksCluster[]> {
  const body = await restCall(
    AKS_PROXY_BASE,
    "GET",
    withVersion(`${subPath(sub)}/providers/Microsoft.ContainerService/managedClusters`),
    undefined,
    fetchFn,
  );
  return list(body.value)
    .map((r) => parseCluster(r))
    .toSorted(
      (a, b) => a.resourceGroup.localeCompare(b.resourceGroup) || a.name.localeCompare(b.name),
    );
}

export async function getUpgradeProfile(
  sub: string,
  c: Pick<AksCluster, "resourceGroup" | "name">,
  fetchFn: FetchFn = fetch,
): Promise<UpgradeProfile> {
  return parseUpgradeProfile(
    await restCall(
      AKS_PROXY_BASE,
      "GET",
      withVersion(`${clusterPath(sub, c.resourceGroup, c.name)}/upgradeProfiles/default`),
      undefined,
      fetchFn,
    ),
  );
}

export async function powerAction(
  sub: string,
  c: Pick<AksCluster, "resourceGroup" | "name">,
  action: PowerAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    AKS_PROXY_BASE,
    "POST",
    withVersion(`${clusterPath(sub, c.resourceGroup, c.name)}/${action}`),
    undefined,
    fetchFn,
  );
}

export async function deleteCluster(
  sub: string,
  c: Pick<AksCluster, "resourceGroup" | "name">,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    AKS_PROXY_BASE,
    "DELETE",
    withVersion(clusterPath(sub, c.resourceGroup, c.name)),
    undefined,
    fetchFn,
  );
}

/** Sets a pool's node count: reads the pool's current properties and writes them back with the new count. */
export async function scalePool(
  sub: string,
  c: Pick<AksCluster, "resourceGroup" | "name">,
  pool: string,
  count: number,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const path = withVersion(`${clusterPath(sub, c.resourceGroup, c.name)}/agentPools/${enc(pool)}`);
  const current = await restCall(AKS_PROXY_BASE, "GET", path, undefined, fetchFn);
  const properties = { ...obj(current.properties), count };
  await restCall(AKS_PROXY_BASE, "PUT", path, { properties }, fetchFn);
}

export const validPoolCount = (v: string): boolean => {
  if (!/^\d+$/.test(v.trim())) return false;
  const n = Number(v);
  return n >= 0 && n <= AKS_MAX_POOL_COUNT;
};

/** The CLI command that fetches credentials for a cluster; the dashboard never handles them itself. */
export const credentialsCommand = (c: Pick<AksCluster, "resourceGroup" | "name">): string =>
  `az aks get-credentials --resource-group ${c.resourceGroup} --name ${c.name}`;

export const totalNodes = (c: Pick<AksCluster, "pools">): number =>
  c.pools.reduce((n, p) => n + p.count, 0);
