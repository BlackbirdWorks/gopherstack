/** Client for the Compute Engine emulator (instances, disks, networks, addresses, operations) via the dashboard proxy. */

import { restCall, shortName, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const COMPUTE_PROXY_BASE = "/dashboard/api/v1/gcp/compute";
export const COMPUTE_PROJECT_KEY = "gopherstack_compute_project";
export const COMPUTE_ZONE_KEY = "gopherstack_compute_zone";
export const ALL_ZONES = "";
const DEFAULT_PROJECT = "gopherstack-local";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type InstanceAction = "start" | "stop" | "reset";

export type ComputeDiskRef = { name: string; sizeGb: number; boot: boolean };

export type Instance = {
  name: string;
  zone: string;
  status: string;
  machineType: string;
  internalIps: string[];
  externalIps: string[];
  disks: ComputeDiskRef[];
  labels: Record<string, string>;
  metadataKeys: string[];
  created: string;
  raw: Json;
};

export type Disk = {
  name: string;
  zone: string;
  status: string;
  sizeGb: number;
  type: string;
  users: string[];
  labels: Record<string, string>;
  created: string;
  raw: Json;
};

export type Snapshot = {
  name: string;
  status: string;
  sourceDisk: string;
  sizeGb: number;
  created: string;
  raw: Json;
};

export type Network = {
  name: string;
  mode: string;
  routing: string;
  mtu: number;
  subnetworks: string[];
  created: string;
  raw: Json;
};

export type Subnetwork = {
  name: string;
  region: string;
  network: string;
  cidr: string;
  gateway: string;
  privateGoogleAccess: boolean;
  raw: Json;
};

export type Firewall = {
  name: string;
  network: string;
  direction: string;
  priority: number;
  disabled: boolean;
  ranges: string[];
  rules: string[];
  targets: string[];
  raw: Json;
};

export type Route = {
  name: string;
  network: string;
  destRange: string;
  priority: number;
  nextHop: string;
  tags: string[];
  raw: Json;
};

export type Router = {
  name: string;
  region: string;
  network: string;
  nats: string[];
  asn: number;
  raw: Json;
};

export type Address = {
  name: string;
  scope: string;
  address: string;
  type: string;
  status: string;
  purpose: string;
  users: string[];
  raw: Json;
};

export type ComputeOperation = {
  name: string;
  type: string;
  status: string;
  progress: number;
  target: string;
  scope: string;
  started: string;
  ended: string;
  error: string;
  raw: Json;
};

/** Resource scope of a URL such as .../zones/us-central1-a/instances/x -> us-central1-a. */
export function scopeOf(url: string, kind: "zones" | "regions"): string {
  const m = new RegExp(`/${kind}/([^/]+)`).exec(url);
  return m?.[1] ?? "";
}

/** The region a zone belongs to (us-central1-a -> us-central1). */
export function regionOfZone(zone: string): string {
  return zone.slice(0, Math.max(zone.lastIndexOf("-"), 0));
}

function instanceIps(raw: Json): { internal: string[]; external: string[] } {
  const internal: string[] = [];
  const external: string[] = [];
  for (const nic of list(raw.networkInterfaces)) {
    if (str(nic.networkIP)) internal.push(str(nic.networkIP));
    for (const ac of list(nic.accessConfigs)) if (str(ac.natIP)) external.push(str(ac.natIP));
  }
  return { internal, external };
}

export function parseInstance(raw: Json): Instance {
  const ips = instanceIps(raw);
  return {
    name: str(raw.name),
    zone: shortName(str(raw.zone)),
    status: str(raw.status),
    machineType: shortName(str(raw.machineType)),
    internalIps: ips.internal,
    externalIps: ips.external,
    disks: list(raw.disks).map((d) => ({
      name: shortName(str(d.source)) || str(d.deviceName),
      sizeGb: num(d.diskSizeGb),
      boot: d.boot === true,
    })),
    labels: strMap(raw.labels),
    metadataKeys: list(obj(raw.metadata).items).map((i) => str(i.key)),
    created: str(raw.creationTimestamp),
    raw,
  };
}

export function parseDisk(raw: Json): Disk {
  return {
    name: str(raw.name),
    zone: shortName(str(raw.zone)),
    status: str(raw.status),
    sizeGb: num(raw.sizeGb),
    type: shortName(str(raw.type)),
    users: strs(raw.users).map((x) => shortName(x)),
    labels: strMap(raw.labels),
    created: str(raw.creationTimestamp),
    raw,
  };
}

export function parseSnapshot(raw: Json): Snapshot {
  return {
    name: str(raw.name),
    status: str(raw.status),
    sourceDisk: shortName(str(raw.sourceDisk)),
    sizeGb: num(raw.diskSizeGb),
    created: str(raw.creationTimestamp),
    raw,
  };
}

export function parseNetwork(raw: Json): Network {
  const auto = raw.autoCreateSubnetworks === true;
  return {
    name: str(raw.name),
    mode: auto ? "auto" : "custom",
    routing: str(obj(raw.routingConfig).routingMode) || "REGIONAL",
    mtu: num(raw.mtu),
    subnetworks: strs(raw.subnetworks),
    created: str(raw.creationTimestamp),
    raw,
  };
}

export function parseSubnetwork(raw: Json): Subnetwork {
  return {
    name: str(raw.name),
    region: shortName(str(raw.region)),
    network: shortName(str(raw.network)),
    cidr: str(raw.ipCidrRange),
    gateway: str(raw.gatewayAddress),
    privateGoogleAccess: raw.privateIpGoogleAccess === true,
    raw,
  };
}

function ruleText(r: Json): string {
  const ports = strs(r.ports);
  const proto = str(r.IPProtocol);
  return ports.length > 0 ? `${proto}:${ports.join(",")}` : proto;
}

export function parseFirewall(raw: Json): Firewall {
  const direction = str(raw.direction) || "INGRESS";
  const allowed = list(raw.allowed).map((r) => `allow ${ruleText(r)}`);
  const denied = list(raw.denied).map((r) => `deny ${ruleText(r)}`);
  return {
    name: str(raw.name),
    network: shortName(str(raw.network)),
    direction,
    priority: num(raw.priority),
    disabled: raw.disabled === true,
    ranges: direction === "EGRESS" ? strs(raw.destinationRanges) : strs(raw.sourceRanges),
    rules: [...allowed, ...denied],
    targets: strs(raw.targetTags),
    raw,
  };
}

const NEXT_HOPS = [
  "nextHopGateway",
  "nextHopIp",
  "nextHopInstance",
  "nextHopNetwork",
  "nextHopIlb",
  "nextHopVpnTunnel",
] as const;

export function parseRoute(raw: Json): Route {
  const hop = NEXT_HOPS.find((k) => str(raw[k]));
  return {
    name: str(raw.name),
    network: shortName(str(raw.network)),
    destRange: str(raw.destRange),
    priority: num(raw.priority),
    nextHop: hop ? shortName(str(raw[hop])) : "",
    tags: strs(raw.tags),
    raw,
  };
}

export function parseRouter(raw: Json): Router {
  return {
    name: str(raw.name),
    region: shortName(str(raw.region)),
    network: shortName(str(raw.network)),
    nats: list(raw.nats).map(
      (n) =>
        `${str(n.name)} (${str(n.natIpAllocateOption) || "AUTO_ONLY"}, ${str(n.sourceSubnetworkIpRangesToNat)})`,
    ),
    asn: num(obj(raw.bgp).asn),
    raw,
  };
}

export function parseAddress(raw: Json): Address {
  const region = scopeOf(str(raw.region), "regions") || shortName(str(raw.region));
  return {
    name: str(raw.name),
    scope: region || "global",
    address: str(raw.address),
    type: str(raw.addressType) || "EXTERNAL",
    status: str(raw.status),
    purpose: str(raw.purpose),
    users: strs(raw.users).map((x) => shortName(x)),
    raw,
  };
}

export function parseOperation(raw: Json): ComputeOperation {
  const err = list(obj(raw.error).errors)
    .map((e) => str(e.message))
    .filter(Boolean)
    .join("; ");
  const scope = str(raw.zone) || str(raw.region);
  return {
    name: str(raw.name),
    type: str(raw.operationType),
    status: str(raw.status),
    progress: num(raw.progress),
    target: shortName(str(raw.targetLink)),
    scope: scope ? shortName(scope) : "global",
    started: str(raw.insertTime),
    ended: str(raw.endTime),
    error: err,
    raw,
  };
}

const projectPath = (project: string) => `/projects/${enc(project)}`;

/** Follows nextPageToken through a plain list endpoint. */
async function listPlain(path: string, fetchFn: FetchFn): Promise<Json[]> {
  const out: Json[] = [];
  let token = "";
  do {
    const url = token ? `${path}?pageToken=${enc(token)}` : path;
    const body = await restCall(COMPUTE_PROXY_BASE, "GET", url, undefined, fetchFn);
    out.push(...list(body.items));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

/** Flattens an aggregatedList: scopes that carry the collection array contribute their items. */
async function listAggregated(
  project: string,
  collection: string,
  fetchFn: FetchFn,
): Promise<Json[]> {
  const out: Json[] = [];
  let token = "";
  do {
    const path = `${projectPath(project)}/aggregated/${collection}`;
    const body = await restCall(
      COMPUTE_PROXY_BASE,
      "GET",
      token ? `${path}?pageToken=${enc(token)}` : path,
      undefined,
      fetchFn,
    );
    for (const scope of Object.values(obj(body.items))) out.push(...list(obj(scope)[collection]));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export async function listZoneNames(project: string, fetchFn: FetchFn = fetch): Promise<string[]> {
  const items = await listPlain(`${projectPath(project)}/zones`, fetchFn);
  return items.map((z) => str(z.name)).toSorted();
}

export async function listInstances(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<Instance[]> {
  return (await listAggregated(project, "instances", fetchFn)).map((x) => parseInstance(x));
}

export async function listDisks(project: string, fetchFn: FetchFn = fetch): Promise<Disk[]> {
  return (await listAggregated(project, "disks", fetchFn)).map((x) => parseDisk(x));
}

export async function listSnapshots(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<Snapshot[]> {
  return (await listPlain(`${projectPath(project)}/global/snapshots`, fetchFn)).map((x) =>
    parseSnapshot(x),
  );
}

export async function listNetworks(project: string, fetchFn: FetchFn = fetch): Promise<Network[]> {
  return (await listPlain(`${projectPath(project)}/global/networks`, fetchFn)).map((x) =>
    parseNetwork(x),
  );
}

export async function listSubnetworks(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<Subnetwork[]> {
  return (await listAggregated(project, "subnetworks", fetchFn)).map((x) => parseSubnetwork(x));
}

export async function listFirewalls(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<Firewall[]> {
  return (await listPlain(`${projectPath(project)}/global/firewalls`, fetchFn)).map((x) =>
    parseFirewall(x),
  );
}

export async function listRoutes(project: string, fetchFn: FetchFn = fetch): Promise<Route[]> {
  return (await listPlain(`${projectPath(project)}/global/routes`, fetchFn)).map((x) =>
    parseRoute(x),
  );
}

export async function listRouters(project: string, fetchFn: FetchFn = fetch): Promise<Router[]> {
  return (await listAggregated(project, "routers", fetchFn)).map((x) => parseRouter(x));
}

/** Aggregated lists carry the "global" scope too, so one call covers regional and global addresses. */
export async function listAddresses(project: string, fetchFn: FetchFn = fetch): Promise<Address[]> {
  return (await listAggregated(project, "addresses", fetchFn)).map((x) => parseAddress(x));
}

/** Newest first. */
export async function listOperations(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<ComputeOperation[]> {
  const ops = (await listAggregated(project, "operations", fetchFn)).map((x) => parseOperation(x));
  return ops.toSorted((a, b) => b.started.localeCompare(a.started));
}

const zonePath = (project: string, zone: string) => `${projectPath(project)}/zones/${enc(zone)}`;

export async function instanceAction(
  project: string,
  zone: string,
  name: string,
  action: InstanceAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    COMPUTE_PROXY_BASE,
    "POST",
    `${zonePath(project, zone)}/instances/${enc(name)}/${action}`,
    undefined,
    fetchFn,
  );
}

export async function deleteInstance(
  project: string,
  zone: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    COMPUTE_PROXY_BASE,
    "DELETE",
    `${zonePath(project, zone)}/instances/${enc(name)}`,
    undefined,
    fetchFn,
  );
}

export async function deleteDisk(
  project: string,
  zone: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    COMPUTE_PROXY_BASE,
    "DELETE",
    `${zonePath(project, zone)}/disks/${enc(name)}`,
    undefined,
    fetchFn,
  );
}

export async function deleteSnapshot(
  project: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    COMPUTE_PROXY_BASE,
    "DELETE",
    `${projectPath(project)}/global/snapshots/${enc(name)}`,
    undefined,
    fetchFn,
  );
}

/** Deletes a regional address, or a global one when scope is "global". */
export async function deleteAddress(
  project: string,
  scope: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const base =
    scope === "global"
      ? `${projectPath(project)}/global/addresses`
      : `${projectPath(project)}/regions/${enc(scope)}/addresses`;
  await restCall(COMPUTE_PROXY_BASE, "DELETE", `${base}/${enc(name)}`, undefined, fetchFn);
}

export function readStoredComputeScope(): { project: string; zone: string } {
  try {
    return {
      project: localStorage.getItem(COMPUTE_PROJECT_KEY)?.trim() || DEFAULT_PROJECT,
      zone: localStorage.getItem(COMPUTE_ZONE_KEY)?.trim() || ALL_ZONES,
    };
  } catch {
    return { project: DEFAULT_PROJECT, zone: ALL_ZONES };
  }
}

export function writeStoredComputeScope(project: string, zone: string): void {
  try {
    localStorage.setItem(COMPUTE_PROJECT_KEY, project);
    localStorage.setItem(COMPUTE_ZONE_KEY, zone);
  } catch {
    // storage unavailable: the scope applies for this session only
  }
}

/** "k=v" chips for a label map. */
export function labelText(labels: Record<string, string>): string {
  return Object.entries(labels)
    .map(([k, v]) => `${k}=${v}`)
    .join(", ");
}
