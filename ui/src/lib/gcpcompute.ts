/** Client for the Compute Engine emulator (instances, disks, networks, addresses, operations) via the dashboard proxy. */

import { restCall, shortName, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const COMPUTE_PROXY_BASE = "/dashboard/api/v1/gcp/compute";
export const COMPUTE_PROJECT_KEY = "gopherstack_compute_project";
export const COMPUTE_ZONE_KEY = "gopherstack_compute_zone";
export const COMPUTE_REGION_KEY = "gopherstack_compute_region";
export const ALL_ZONES = "";
export const ALL_REGIONS = "";
export const POLL_MS = 2000;
export const POLL_MAX = 45;
const DEFAULT_PROJECT = "gopherstack-local";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type InstanceAction = "start" | "stop" | "reset" | "suspend" | "resume";

export type ComputeDiskRef = {
  name: string;
  sizeGb: number;
  boot: boolean;
  mode: string;
  iface: string;
};

export type InstanceNic = {
  name: string;
  network: string;
  subnetwork: string;
  internalIp: string;
  externalIps: string[];
};

/** A metadata entry as the proxy returns it: the value is redacted, only its length is known. */
export type MetadataEntry = { key: string; length: number; revealable: boolean };

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
  metadata: MetadataEntry[];
  nics: InstanceNic[];
  tags: string[];
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
  peerings: Peering[];
  created: string;
  raw: Json;
};

export type Peering = {
  name: string;
  peer: string;
  state: string;
  detail: string;
  exportCustomRoutes: boolean;
  importCustomRoutes: boolean;
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
  sourceTags: string[];
  raw: Json;
};

export type Route = {
  name: string;
  routeType: string;
  description: string;
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

/** SSH-related keys are never revealed, in the UI or by the proxy. */
export function metadataRevealable(key: string): boolean {
  return key !== "" && !key.toLowerCase().includes("ssh");
}

function parseMetadataEntry(i: Json): MetadataEntry {
  const key = str(i.key);
  return {
    key,
    length: typeof i.valueLength === "number" ? i.valueLength : str(i.value).length,
    revealable: metadataRevealable(key),
  };
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
      mode: str(d.mode),
      iface: str(d.interface),
    })),
    labels: strMap(raw.labels),
    metadataKeys: list(obj(raw.metadata).items).map((i) => str(i.key)),
    metadata: list(obj(raw.metadata).items).map((i) => parseMetadataEntry(i)),
    nics: list(raw.networkInterfaces).map((n) => ({
      name: str(n.name),
      network: shortName(str(n.network)),
      subnetwork: shortName(str(n.subnetwork)),
      internalIp: str(n.networkIP),
      externalIps: list(n.accessConfigs)
        .map((a) => str(a.natIP))
        .filter(Boolean),
    })),
    tags: strs(obj(raw.tags).items),
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
    peerings: list(raw.peerings).map((p) => ({
      name: str(p.name),
      peer: shortName(str(p.network)),
      state: str(p.state),
      detail: str(p.stateDetails),
      exportCustomRoutes: p.exportCustomRoutes === true,
      importCustomRoutes: p.importCustomRoutes === true,
    })),
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
    sourceTags: strs(raw.sourceTags),
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
    routeType: str(raw.routeType) === "SUBNET" ? "system (subnet)" : "static",
    description: str(raw.description),
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

export function readStoredComputeScope(): { project: string; zone: string; region: string } {
  try {
    const zone = localStorage.getItem(COMPUTE_ZONE_KEY)?.trim() || ALL_ZONES;
    return {
      project: localStorage.getItem(COMPUTE_PROJECT_KEY)?.trim() || DEFAULT_PROJECT,
      zone,
      region: localStorage.getItem(COMPUTE_REGION_KEY)?.trim() || regionOfZone(zone),
    };
  } catch {
    return { project: DEFAULT_PROJECT, zone: ALL_ZONES, region: ALL_REGIONS };
  }
}

export function writeStoredComputeScope(project: string, zone: string, region = ""): void {
  try {
    localStorage.setItem(COMPUTE_PROJECT_KEY, project);
    localStorage.setItem(COMPUTE_ZONE_KEY, zone);
    localStorage.setItem(COMPUTE_REGION_KEY, region);
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

export type MachineType = { name: string; vcpus: number; memoryMb: number };
export type ImageFamily = { project: string; family: string; source: string; label: string };

export type NewInstance = {
  name: string;
  machineType: string;
  sourceImage: string;
  diskSizeGb: string;
  network: string;
  subnetwork: string;
  externalIp: boolean;
};

export type BackendService = {
  name: string;
  scope: string;
  protocol: string;
  scheme: string;
  backends: string[];
  healthChecks: string[];
  securityPolicy: string;
  edgeSecurityPolicy: string;
  raw: Json;
};

export type UrlMap = {
  name: string;
  scope: string;
  defaultService: string;
  hostRules: string[];
  pathMatchers: string[];
  raw: Json;
};

export type TargetProxy = {
  name: string;
  scope: string;
  kind: string;
  urlMap: string;
  certificates: string[];
  raw: Json;
};

export type ForwardingRule = {
  name: string;
  scope: string;
  ip: string;
  protocol: string;
  ports: string;
  target: string;
  scheme: string;
  raw: Json;
};

export type SecurityRule = {
  priority: number;
  action: string;
  match: string;
  description: string;
  preview: boolean;
};

export type SecurityPolicy = {
  name: string;
  type: string;
  description: string;
  rules: SecurityRule[];
  raw: Json;
};

export const IMAGE_PROJECTS = [
  "debian-cloud",
  "ubuntu-os-cloud",
  "rocky-linux-cloud",
  "centos-cloud",
];

const scopeName = (raw: Json): string => {
  const s = str(raw.zone) || str(raw.region);
  return s ? shortName(s) : "global";
};

export function parseMachineType(raw: Json): MachineType {
  return { name: str(raw.name), vcpus: num(raw.guestCpus), memoryMb: num(raw.memoryMb) };
}

export function parseBackendService(raw: Json): BackendService {
  return {
    name: str(raw.name),
    scope: scopeName(raw),
    protocol: str(raw.protocol),
    scheme: str(raw.loadBalancingScheme),
    backends: list(raw.backends).map((b) => shortName(str(b.group))),
    healthChecks: strs(raw.healthChecks).map((h) => shortName(h)),
    securityPolicy: shortName(str(raw.securityPolicy)),
    edgeSecurityPolicy: shortName(str(raw.edgeSecurityPolicy)),
    raw,
  };
}

export function parseUrlMap(raw: Json): UrlMap {
  return {
    name: str(raw.name),
    scope: scopeName(raw),
    defaultService: shortName(str(raw.defaultService)),
    hostRules: list(raw.hostRules).map(
      (h) => `${strs(h.hosts).join(",")} -> ${str(h.pathMatcher)}`,
    ),
    pathMatchers: list(raw.pathMatchers).map((m) => str(m.name)),
    raw,
  };
}

export function parseTargetProxy(raw: Json): TargetProxy {
  return {
    name: str(raw.name),
    scope: scopeName(raw),
    kind: Array.isArray(raw.sslCertificates) ? "HTTPS" : "HTTP",
    urlMap: shortName(str(raw.urlMap)),
    certificates: strs(raw.sslCertificates).map((c) => shortName(c)),
    raw,
  };
}

export function parseForwardingRule(raw: Json): ForwardingRule {
  const ports = strs(raw.ports);
  return {
    name: str(raw.name),
    scope: scopeName(raw),
    ip: str(raw.IPAddress),
    protocol: str(raw.IPProtocol),
    ports: str(raw.portRange) || ports.join(","),
    target: shortName(str(raw.target) || str(raw.backendService)),
    scheme: str(raw.loadBalancingScheme),
    raw,
  };
}

function ruleMatch(m: Json): string {
  const cfg = obj(m.config);
  const ranges = strs(cfg.srcIpRanges);
  if (ranges.length > 0) return ranges.join(", ");
  const expr = str(obj(m.expr).expression);
  return expr || str(m.versionedExpr);
}

export function parseSecurityPolicy(raw: Json): SecurityPolicy {
  return {
    name: str(raw.name),
    type: str(raw.type) || "CLOUD_ARMOR",
    description: str(raw.description),
    rules: list(raw.rules)
      .map((r) => ({
        priority: num(r.priority),
        action: str(r.action),
        match: ruleMatch(obj(r.match)),
        description: str(r.description),
        preview: r.preview === true,
      }))
      .toSorted((a, b) => a.priority - b.priority),
    raw,
  };
}

/** True while an operation has not reached DONE (drives bounded polling). */
export function operationRunning(o: Pick<ComputeOperation, "status">): boolean {
  return o.status === "PENDING" || o.status === "RUNNING";
}

export function instanceTransitioning(i: Pick<Instance, "status">): boolean {
  return ["PROVISIONING", "STAGING", "STOPPING", "SUSPENDING", "REPAIRING"].includes(i.status);
}

export function validResourceName(name: string): boolean {
  return /^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$/.test(name);
}

export async function listRegionNames(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<string[]> {
  const items = await listPlain(`${projectPath(project)}/regions`, fetchFn);
  return items.map((z) => str(z.name)).toSorted();
}

export async function listMachineTypes(
  project: string,
  zone: string,
  fetchFn: FetchFn = fetch,
): Promise<MachineType[]> {
  const items = await listPlain(`${zonePath(project, zone)}/machineTypes`, fetchFn);
  return items.map((x) => parseMachineType(x)).toSorted((a, b) => a.name.localeCompare(b.name));
}

export type ComputeImage = {
  name: string;
  project: string;
  family: string;
  status: string;
  sizeGb: number;
  created: string;
  raw: Json;
};

export function parseImage(raw: Json, project: string): ComputeImage {
  return {
    name: str(raw.name),
    project,
    family: str(raw.family),
    status: str(raw.status),
    sizeGb: num(raw.diskSizeGb),
    created: str(raw.creationTimestamp),
    raw,
  };
}

/** Images of the public image projects plus the project's own; a project that cannot be read is skipped. */
export async function listImages(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<ComputeImage[]> {
  const projects = [...new Set([...IMAGE_PROJECTS, project])];
  const lists = await Promise.all(
    projects.map((p) =>
      listPlain(`${projectPath(p)}/global/images`, fetchFn).then(
        (items) => items.map((x) => parseImage(x, p)),
        () => [] as ComputeImage[],
      ),
    ),
  );
  return lists.flat();
}

/** One entry per image family. */
export function imageFamilies(images: ComputeImage[]): ImageFamily[] {
  const seen = new Map<string, ImageFamily>();
  for (const img of images) {
    if (!img.family) continue;
    seen.set(`${img.project}/${img.family}`, {
      project: img.project,
      family: img.family,
      source: `projects/${img.project}/global/images/family/${img.family}`,
      label: `${img.family} (${img.project})`,
    });
  }
  return [...seen.values()].toSorted((a, b) => a.label.localeCompare(b.label));
}

export async function listBackendServices(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<BackendService[]> {
  return (await listAggregated(project, "backendServices", fetchFn)).map((x) =>
    parseBackendService(x),
  );
}

export async function listUrlMaps(project: string, fetchFn: FetchFn = fetch): Promise<UrlMap[]> {
  return (await listAggregated(project, "urlMaps", fetchFn)).map((x) => parseUrlMap(x));
}

export async function listTargetProxies(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<TargetProxy[]> {
  const [http, https] = await Promise.all([
    listAggregated(project, "targetHttpProxies", fetchFn),
    listAggregated(project, "targetHttpsProxies", fetchFn),
  ]);
  return [
    ...http.map((x) => ({ ...parseTargetProxy(x), kind: "HTTP" })),
    ...https.map((x) => ({ ...parseTargetProxy(x), kind: "HTTPS" })),
  ];
}

export async function listForwardingRules(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<ForwardingRule[]> {
  return (await listAggregated(project, "forwardingRules", fetchFn)).map((x) =>
    parseForwardingRule(x),
  );
}

export async function listSecurityPolicies(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<SecurityPolicy[]> {
  return (await listPlain(`${projectPath(project)}/global/securityPolicies`, fetchFn)).map((x) =>
    parseSecurityPolicy(x),
  );
}

export async function getInstance(
  project: string,
  zone: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<Instance> {
  const raw = await restCall(
    COMPUTE_PROXY_BASE,
    "GET",
    `${zonePath(project, zone)}/instances/${enc(name)}`,
    undefined,
    fetchFn,
  );
  return parseInstance(raw);
}

/** Fetches one metadata value; the proxy refuses SSH-related keys, which come back empty. */
export async function revealMetadata(
  project: string,
  zone: string,
  name: string,
  key: string,
  fetchFn: FetchFn = fetch,
): Promise<string> {
  if (!metadataRevealable(key)) return "";
  const raw = await restCall(
    COMPUTE_PROXY_BASE,
    "GET",
    `${zonePath(project, zone)}/instances/${enc(name)}?reveal=${enc(key)}`,
    undefined,
    fetchFn,
  );
  const item = list(obj(raw.metadata).items).find((i) => str(i.key) === key);
  return str(item?.value);
}

export async function createInstance(
  project: string,
  zone: string,
  req: NewInstance,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const nic: Json = {};
  if (req.network) nic.network = req.network;
  if (req.subnetwork) nic.subnetwork = req.subnetwork;
  if (req.externalIp) nic.accessConfigs = [{ type: "ONE_TO_ONE_NAT" }];
  const params: Json = { sourceImage: req.sourceImage };
  if (req.diskSizeGb.trim()) params.diskSizeGb = req.diskSizeGb.trim();
  await restCall(
    COMPUTE_PROXY_BASE,
    "POST",
    `${zonePath(project, zone)}/instances`,
    {
      name: req.name,
      machineType: req.machineType,
      disks: [{ boot: true, autoDelete: true, initializeParams: params }],
      networkInterfaces: [nic],
    },
    fetchFn,
  );
}

export async function createSnapshot(
  project: string,
  zone: string,
  disk: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    COMPUTE_PROXY_BASE,
    "POST",
    `${zonePath(project, zone)}/disks/${enc(disk)}/createSnapshot`,
    { name },
    fetchFn,
  );
}

/** Does a zonal resource belong to the selected zone, or to the selected region when no zone is picked? */
export function inZoneScope(itemZone: string, zone: string, region: string): boolean {
  if (zone) return itemZone === zone;
  return !region || regionOfZone(itemZone) === region;
}

/** Regional resources match their region; global ones are always shown. */
export function inRegionScope(itemRegion: string, region: string): boolean {
  return !region || itemRegion === region || itemRegion === "global";
}

/** Keep polling while something is unsettled, for at most POLL_MAX rounds. */
export function shouldPoll(unsettled: boolean, rounds: number): boolean {
  return unsettled && rounds < POLL_MAX;
}

/** Operations are zonal, regional or global; global ones always show. */
export function operationInScope(scope: string, zone: string, region: string): boolean {
  if (scope === "global" || (!zone && !region)) return true;
  if (zone) return scope === zone || scope === regionOfZone(zone);
  return scope === region || regionOfZone(scope) === region;
}
