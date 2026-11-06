/** Client for Azure networking through the dashboard's in-process Resource Manager proxy (Microsoft.Network). */

import { parseArmId } from "./azurearm.ts";
import { GcpRestError, str, type FetchFn, type Json } from "./gcprest.ts";

export const NETWORK_PROXY_BASE = "/dashboard/api/v1/azure/network";
export const NETWORK_API_VERSION = "2024-05-01";
export const MIN_RULE_PRIORITY = 100;
export const MAX_RULE_PRIORITY = 4096;
export const POLL_ATTEMPTS = 30;
export const POLL_INTERVAL_MS = 500;
export const RULE_PROTOCOLS: readonly string[] = ["Tcp", "Udp", "Icmp", "Esp", "Ah", "*"];
export const DELEGATION_SERVICES: readonly string[] = [
  "Microsoft.Web/serverFarms",
  "Microsoft.ContainerInstance/containerGroups",
  "Microsoft.App/environments",
  "Microsoft.DBforPostgreSQL/flexibleServers",
  "Microsoft.DBforMySQL/flexibleServers",
  "Microsoft.Sql/managedInstances",
];

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const strs = (v: unknown): string[] =>
  Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
const idOf = (v: unknown): string => str(obj(v).id);
const leaf = (id: string): string => id.split("/").pop() ?? "";
const props = (raw: Json): Json => obj(raw.properties);

export type Sleep = (ms: number) => Promise<void>;
const realSleep: Sleep = (ms) =>
  new Promise((resolve) => {
    setTimeout(resolve, ms);
  });

export type Delegation = { name: string; service: string };

export type Subnet = {
  name: string;
  addressPrefix: string;
  nsg: string;
  natGateway: string;
  delegations: Delegation[];
};

export type Vnet = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  addressSpace: string[];
  subnets: Subnet[];
  state: string;
};

export type SecurityRule = {
  name: string;
  priority: number;
  direction: string;
  access: string;
  protocol: string;
  sourcePorts: string[];
  destinationPorts: string[];
  sources: string[];
  destinations: string[];
  description: string;
  isDefault: boolean;
};

export type Nsg = {
  id: string;
  name: string;
  location: string;
  rules: SecurityRule[];
  defaultRules: SecurityRule[];
};

export type PublicIp = {
  id: string;
  name: string;
  location: string;
  sku: string;
  tier: string;
  allocation: string;
  version: string;
  address: string;
  state: string;
};

export type LoadBalancer = {
  id: string;
  name: string;
  location: string;
  sku: string;
  frontends: { name: string; privateIp: string; publicIp: string }[];
  pools: { name: string; members: number }[];
  probes: { name: string; protocol: string; port: number; path: string }[];
  rules: { name: string; protocol: string; frontendPort: number; backendPort: number }[];
};

export type AppGateway = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  sku: string;
  tier: string;
  capacity: number;
  operationalState: string;
  state: string;
  listeners: { name: string; protocol: string; host: string; port: string }[];
  pools: { name: string; addresses: string[] }[];
  settings: { name: string; protocol: string; port: number }[];
  rules: { name: string; type: string; priority: number; listener: string; pool: string }[];
  probes: { name: string; protocol: string; path: string; host: string }[];
};

export type BackendHealthServer = { address: string; health: string; log: string };
export type BackendHealthPool = { pool: string; settings: string; servers: BackendHealthServer[] };

export type Firewall = {
  id: string;
  name: string;
  location: string;
  sku: string;
  tier: string;
  threatIntel: string;
  policy: string;
  state: string;
  ipConfigs: { name: string; subnet: string; publicIp: string; privateIp: string }[];
};

export type FirewallPolicy = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  tier: string;
  threatIntel: string;
  basePolicy: string;
  state: string;
};

export type PolicyRule = { type: string; name: string; summary: string };
export type RuleCollection = {
  name: string;
  type: string;
  priority: number;
  action: string;
  rules: PolicyRule[];
};
export type RuleCollectionGroup = { name: string; priority: number; collections: RuleCollection[] };

export type IpGroup = {
  id: string;
  name: string;
  location: string;
  addresses: string[];
  firewalls: string[];
};

export type NatGateway = { id: string; name: string };

export function parseSubnet(raw: Json): Subnet {
  const p = props(raw);
  return {
    name: str(raw.name),
    addressPrefix: str(p.addressPrefix) || strs(p.addressPrefixes).join(", "),
    nsg: leaf(idOf(p.networkSecurityGroup)),
    natGateway: leaf(idOf(p.natGateway)),
    delegations: list(p.delegations).map((d) => ({
      name: str(d.name),
      service: str(props(d).serviceName),
    })),
  };
}

export function parseVnet(raw: Json): Vnet {
  const p = props(raw);
  const id = str(raw.id);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    addressSpace: strs(obj(p.addressSpace).addressPrefixes),
    subnets: list(p.subnets).map((s) => parseSubnet(s)),
    state: str(p.provisioningState),
  };
}

const many = (one: unknown, plural: unknown): string[] => {
  const all = strs(plural);
  return typeof one === "string" && one ? [one, ...all] : all;
};

export function parseRule(raw: Json, isDefault = false): SecurityRule {
  const p = props(raw);
  return {
    name: str(raw.name),
    priority: num(p.priority),
    direction: str(p.direction),
    access: str(p.access),
    protocol: str(p.protocol),
    sourcePorts: many(p.sourcePortRange, p.sourcePortRanges),
    destinationPorts: many(p.destinationPortRange, p.destinationPortRanges),
    sources: many(p.sourceAddressPrefix, p.sourceAddressPrefixes),
    destinations: many(p.destinationAddressPrefix, p.destinationAddressPrefixes),
    description: str(p.description),
    isDefault,
  };
}

export const sortRules = <T extends { priority: number; name: string }>(rules: T[]): T[] =>
  rules.toSorted((a, b) => a.priority - b.priority || a.name.localeCompare(b.name));

export function parseNsg(raw: Json): Nsg {
  const p = props(raw);
  return {
    id: str(raw.id),
    name: str(raw.name),
    location: str(raw.location),
    rules: sortRules(list(p.securityRules).map((r) => parseRule(r))),
    defaultRules: sortRules(list(p.defaultSecurityRules).map((r) => parseRule(r, true))),
  };
}

export function parsePublicIp(raw: Json): PublicIp {
  const p = props(raw);
  const sku = obj(raw.sku);
  return {
    id: str(raw.id),
    name: str(raw.name),
    location: str(raw.location),
    sku: str(sku.name),
    tier: str(sku.tier),
    allocation: str(p.publicIPAllocationMethod),
    version: str(p.publicIPAddressVersion),
    address: str(p.ipAddress),
    state: str(p.provisioningState),
  };
}

export function parseLoadBalancer(raw: Json): LoadBalancer {
  const p = props(raw);
  return {
    id: str(raw.id),
    name: str(raw.name),
    location: str(raw.location),
    sku: str(obj(raw.sku).name),
    frontends: list(p.frontendIPConfigurations).map((f) => ({
      name: str(f.name),
      privateIp: str(props(f).privateIPAddress),
      publicIp: leaf(idOf(props(f).publicIPAddress)),
    })),
    pools: list(p.backendAddressPools).map((b) => ({
      name: str(b.name),
      members:
        list(props(b).loadBalancerBackendAddresses).length +
        list(props(b).backendIPConfigurations).length,
    })),
    probes: list(p.probes).map((b) => ({
      name: str(b.name),
      protocol: str(props(b).protocol),
      port: num(props(b).port),
      path: str(props(b).requestPath),
    })),
    rules: list(p.loadBalancingRules).map((b) => ({
      name: str(b.name),
      protocol: str(props(b).protocol),
      frontendPort: num(props(b).frontendPort),
      backendPort: num(props(b).backendPort),
    })),
  };
}

export function parseAppGateway(raw: Json): AppGateway {
  const p = props(raw);
  const sku = obj(p.sku);
  const ports = new Map(
    list(p.frontendPorts).map((f) => [str(f.name), String(num(props(f).port))]),
  );
  return {
    id: str(raw.id),
    name: str(raw.name),
    resourceGroup: parseArmId(str(raw.id)).resourceGroup,
    location: str(raw.location),
    sku: str(sku.name),
    tier: str(sku.tier),
    capacity: num(sku.capacity) || num(obj(p.autoscaleConfiguration).minCapacity),
    operationalState: str(p.operationalState),
    state: str(p.provisioningState),
    listeners: list(p.httpListeners).map((l) => ({
      name: str(l.name),
      protocol: str(props(l).protocol),
      host: str(props(l).hostName) || strs(props(l).hostNames).join(", "),
      port: ports.get(leaf(idOf(props(l).frontendPort))) ?? leaf(idOf(props(l).frontendPort)),
    })),
    pools: list(p.backendAddressPools).map((b) => ({
      name: str(b.name),
      addresses: list(props(b).backendAddresses).map((a) => str(a.ipAddress) || str(a.fqdn)),
    })),
    settings: list(p.backendHttpSettingsCollection).map((s) => ({
      name: str(s.name),
      protocol: str(props(s).protocol),
      port: num(props(s).port),
    })),
    rules: sortRules(
      list(p.requestRoutingRules).map((r) => ({
        name: str(r.name),
        type: str(props(r).ruleType),
        priority: num(props(r).priority),
        listener: leaf(idOf(props(r).httpListener)),
        pool: leaf(idOf(props(r).backendAddressPool)) || leaf(idOf(props(r).urlPathMap)),
      })),
    ),
    probes: list(p.probes).map((b) => ({
      name: str(b.name),
      protocol: str(props(b).protocol),
      path: str(props(b).path),
      host: str(props(b).host),
    })),
  };
}

export function parseBackendHealth(raw: Json): BackendHealthPool[] {
  return list(raw.backendAddressPools).flatMap((pool) =>
    list(pool.backendHttpSettingsCollection).map((s) => ({
      pool: leaf(idOf(pool.backendAddressPool)),
      settings: leaf(idOf(s.backendHttpSettings)),
      servers: list(s.servers).map((v) => ({
        address: str(v.address),
        health: str(v.health),
        log: str(v.healthProbeLog),
      })),
    })),
  );
}

export function parseFirewall(raw: Json): Firewall {
  const p = props(raw);
  const sku = obj(p.sku);
  return {
    id: str(raw.id),
    name: str(raw.name),
    location: str(raw.location),
    sku: str(sku.name),
    tier: str(sku.tier),
    threatIntel: str(p.threatIntelMode),
    policy: leaf(idOf(p.firewallPolicy)),
    state: str(p.provisioningState),
    ipConfigs: list(p.ipConfigurations).map((c) => ({
      name: str(c.name),
      subnet: leaf(idOf(props(c).subnet)),
      publicIp: leaf(idOf(props(c).publicIPAddress)),
      privateIp: str(props(c).privateIPAddress),
    })),
  };
}

export function parseFirewallPolicy(raw: Json): FirewallPolicy {
  const p = props(raw);
  const id = str(raw.id);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    tier: str(obj(p.sku).tier),
    threatIntel: str(p.threatIntelMode),
    basePolicy: leaf(idOf(p.basePolicy)),
    state: str(p.provisioningState),
  };
}

const join = (v: unknown): string => strs(v).join(", ");
const firstSet = (...lists: unknown[]): string =>
  lists.map((l) => join(l)).find((j) => j !== "") ?? "—";

export function summarizeRule(rule: Json): string {
  const src = firstSet(rule.sourceAddresses, rule.sourceIpGroups);
  if (str(rule.ruleType) === "ApplicationRule") {
    const protocols = list(rule.protocols)
      .map((x) => `${str(x.protocolType)}:${num(x.port)}`)
      .join(", ");
    return `${src} -> ${firstSet(rule.targetFqdns, rule.fqdnTags)} (${protocols || "—"})`;
  }
  const dst = firstSet(rule.destinationAddresses, rule.destinationFqdns, rule.destinationIpGroups);
  const nat =
    str(rule.ruleType) === "NatRule"
      ? ` => ${str(rule.translatedAddress)}:${str(rule.translatedPort)}`
      : "";
  return `${firstSet(rule.ipProtocols)} ${src} -> ${dst}:${firstSet(rule.destinationPorts)}${nat}`;
}

export function parseRuleCollectionGroup(raw: Json): RuleCollectionGroup {
  const p = props(raw);
  return {
    name: str(raw.name),
    priority: num(p.priority),
    collections: sortRules(
      list(p.ruleCollections).map((c) => ({
        name: str(c.name),
        type: str(c.ruleCollectionType).replace("FirewallPolicy", "").replace("RuleCollection", ""),
        priority: num(c.priority),
        action: str(obj(c.action).type),
        rules: list(c.rules).map((r) => ({
          type: str(r.ruleType),
          name: str(r.name),
          summary: summarizeRule(r),
        })),
      })),
    ),
  };
}

export function parseIpGroup(raw: Json): IpGroup {
  const p = props(raw);
  return {
    id: str(raw.id),
    name: str(raw.name),
    location: str(raw.location),
    addresses: strs(p.ipAddresses),
    firewalls: list(p.firewalls).map((f) => leaf(str(f.id))),
  };
}

export const parseNatGateway = (raw: Json): NatGateway => ({
  id: str(raw.id),
  name: str(raw.name),
});

const byName = <T extends { name: string }>(items: T[]): T[] =>
  items.toSorted((a, b) => a.name.localeCompare(b.name));

// ---- validation and request bodies ----

const NAME_RE = /^[A-Za-z0-9]([A-Za-z0-9._-]{0,78}[A-Za-z0-9_])?$/;
const PORT_RE = /^(\*|\d{1,5}(-\d{1,5})?)$/;
const TAG_RE = /^[A-Za-z][A-Za-z0-9.]{0,63}$/;
const IPV4_CIDR_RE = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})(?:\/(\d{1,2}))?$/;

export const splitList = (input: string): string[] =>
  input
    .split(",")
    .map((x) => x.trim())
    .filter((x) => x !== "");

export function validName(name: string): string | null {
  if (!name) return "Name is required";
  return NAME_RE.test(name)
    ? null
    : "Use letters, numbers, . _ - and start and end with a letter or number";
}

export function validCidr(value: string): string | null {
  const m = IPV4_CIDR_RE.exec(value.trim());
  if (m && m[5] !== undefined) {
    const octets = m.slice(1, 5).map(Number);
    if (octets.every((o) => o <= 255) && Number(m[5]) <= 32) return null;
  }
  if (/^[0-9a-fA-F:]+\/\d{1,3}$/.test(value.trim()) && value.includes(":")) return null;
  return "Enter a CIDR block such as 10.0.0.0/16";
}

export function validPriority(value: string): string | null {
  if (!/^\d+$/.test(value.trim())) return "Priority must be a whole number";
  const n = Number(value);
  return n >= MIN_RULE_PRIORITY && n <= MAX_RULE_PRIORITY
    ? null
    : `Priority must be between ${MIN_RULE_PRIORITY} and ${MAX_RULE_PRIORITY}`;
}

export function validPorts(value: string): string | null {
  const parts = splitList(value);
  if (parts.length === 0) return "Enter a port, a range such as 1000-2000, or *";
  return parts.every(
    (p) => PORT_RE.test(p) && p.split("-").every((n) => n === "*" || Number(n) <= 65535),
  )
    ? null
    : "Use ports, ranges such as 1000-2000, or *";
}

export function validPrefixes(value: string): string | null {
  const parts = splitList(value);
  if (parts.length === 0) return "Enter an address prefix, a service tag or *";
  const ok = parts.every(
    (p) => p === "*" || TAG_RE.test(p) || validCidr(p.includes("/") ? p : `${p}/32`) === null,
  );
  return ok ? null : "Use CIDR blocks, IP addresses, service tags such as Internet, or *";
}

export type RuleInput = {
  name: string;
  priority: string;
  direction: string;
  access: string;
  protocol: string;
  sourcePorts: string;
  destinationPorts: string;
  sources: string;
  destinations: string;
  description: string;
};

export const emptyRule = (): RuleInput => ({
  name: "",
  priority: "100",
  direction: "Inbound",
  access: "Allow",
  protocol: "Tcp",
  sourcePorts: "*",
  destinationPorts: "",
  sources: "*",
  destinations: "*",
  description: "",
});

/** The first validation message for a rule draft, or null when it can be submitted. */
export function validateRule(r: RuleInput, existing: string[] = []): string | null {
  const name = validName(r.name);
  if (name) return name;
  if (existing.some((n) => n.toLowerCase() === r.name.toLowerCase()))
    return "A rule with this name exists";
  return (
    validPriority(r.priority) ??
    validPorts(r.sourcePorts) ??
    validPorts(r.destinationPorts) ??
    validPrefixes(r.sources) ??
    validPrefixes(r.destinations)
  );
}

const oneOrMany = (one: string, plural: string, values: string[]): Json =>
  values.length === 1 ? { [one]: values[0] } : { [plural]: values };

export function ruleBody(r: RuleInput): Json {
  const description = r.description.trim();
  return {
    properties: {
      priority: Number(r.priority),
      direction: r.direction,
      access: r.access,
      protocol: r.protocol,
      ...(description ? { description } : {}),
      ...oneOrMany("sourcePortRange", "sourcePortRanges", splitList(r.sourcePorts)),
      ...oneOrMany("destinationPortRange", "destinationPortRanges", splitList(r.destinationPorts)),
      ...oneOrMany("sourceAddressPrefix", "sourceAddressPrefixes", splitList(r.sources)),
      ...oneOrMany(
        "destinationAddressPrefix",
        "destinationAddressPrefixes",
        splitList(r.destinations),
      ),
    },
  };
}

export type VnetInput = {
  name: string;
  location: string;
  addressSpace: string;
  subnetName: string;
  subnetPrefix: string;
};

export function validateVnet(v: VnetInput): string | null {
  const name = validName(v.name);
  if (name) return name;
  const spaces = splitList(v.addressSpace);
  if (spaces.length === 0) return "Enter at least one address space";
  const bad = spaces.map((s) => validCidr(s)).find((e) => e !== null);
  if (bad) return bad;
  if (v.subnetName || v.subnetPrefix) return validName(v.subnetName) ?? validCidr(v.subnetPrefix);
  return null;
}

export function vnetBody(v: VnetInput): Json {
  return {
    location: v.location,
    properties: {
      addressSpace: { addressPrefixes: splitList(v.addressSpace) },
      ...(v.subnetName
        ? {
            subnets: [{ name: v.subnetName, properties: { addressPrefix: v.subnetPrefix.trim() } }],
          }
        : {}),
    },
  };
}

export type SubnetInput = {
  name: string;
  prefix: string;
  nsg: string;
  nat: string;
  delegation: string;
};

export function validateSubnet(s: SubnetInput): string | null {
  return validName(s.name) ?? validCidr(s.prefix);
}

export function subnetBody(s: SubnetInput): Json {
  return {
    properties: {
      addressPrefix: s.prefix.trim(),
      ...(s.nsg ? { networkSecurityGroup: { id: s.nsg } } : {}),
      ...(s.nat ? { natGateway: { id: s.nat } } : {}),
      ...(s.delegation
        ? { delegations: [{ name: "delegation", properties: { serviceName: s.delegation } }] }
        : {}),
    },
  };
}

// ---- transport ----

async function failure(res: Response): Promise<GcpRestError> {
  const text = await res.text().catch(() => "");
  try {
    const e = (JSON.parse(text) as { error?: { code?: string; message?: string } }).error;
    if (e?.message || e?.code)
      return new GcpRestError(res.status, e.code ?? "", e.message ?? e.code ?? "");
  } catch {
    // fall through to the status text
  }
  return new GcpRestError(res.status, "", text.trim() || res.statusText || `HTTP ${res.status}`);
}

type Reply = { body: Json; operation: string; result: string };

async function call(method: string, url: string, body: unknown, fetchFn: FetchFn): Promise<Reply> {
  let res: Response;
  try {
    res = await fetchFn(url, {
      method,
      ...(body === undefined
        ? {}
        : { body: JSON.stringify(body), headers: { "Content-Type": "application/json" } }),
    });
  } catch (e) {
    throw new GcpRestError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await failure(res);
  const text = await res.text();
  return {
    body: (text ? JSON.parse(text) : {}) as Json,
    operation: res.headers.get("Azure-AsyncOperation") ?? "",
    result: res.headers.get("Location") ?? "",
  };
}

const withVersion = (path: string) =>
  `${NETWORK_PROXY_BASE}${path}?api-version=${NETWORK_API_VERSION}`;

/** Polls an Azure-AsyncOperation URL until it succeeds; a failed or cancelled operation throws. */
export async function awaitOperation(
  operation: string,
  fetchFn: FetchFn = fetch,
  sleep: Sleep = realSleep,
): Promise<void> {
  if (!operation) return;
  for (let i = 0; i < POLL_ATTEMPTS; i++) {
    const status = str((await call("GET", operation, undefined, fetchFn)).body.status);
    if (status === "Succeeded") return;
    if (status === "Failed" || status === "Canceled") {
      throw new GcpRestError(0, status, `The operation ${status.toLowerCase()}`);
    }
    await sleep(POLL_INTERVAL_MS);
  }
  throw new GcpRestError(0, "Timeout", "The operation did not finish in time");
}

async function write(
  method: string,
  path: string,
  body: unknown,
  fetchFn: FetchFn,
  sleep: Sleep,
): Promise<Json> {
  const reply = await call(method, withVersion(path), body, fetchFn);
  await awaitOperation(reply.operation, fetchFn, sleep);
  return reply.body;
}

const sub = (s: string) => `/subscriptions/${enc(s)}`;
const rgPath = (s: string, rg: string, typ: string) =>
  `${sub(s)}/resourceGroups/${enc(rg)}/providers/Microsoft.Network/${typ}`;

async function listIn<T>(
  s: string,
  rg: string,
  typ: string,
  parse: (raw: Json) => T,
  fetchFn: FetchFn,
): Promise<T[]> {
  const reply = await call("GET", withVersion(rgPath(s, rg, typ)), undefined, fetchFn);
  return list(reply.body.value).map((r) => parse(r));
}

export const listVnets = async (s: string, rg: string, f: FetchFn = fetch) =>
  byName(await listIn(s, rg, "virtualNetworks", parseVnet, f));
export const listNsgs = async (s: string, rg: string, f: FetchFn = fetch) =>
  byName(await listIn(s, rg, "networkSecurityGroups", parseNsg, f));
export const listPublicIps = async (s: string, rg: string, f: FetchFn = fetch) =>
  byName(await listIn(s, rg, "publicIPAddresses", parsePublicIp, f));
export const listLoadBalancers = async (s: string, rg: string, f: FetchFn = fetch) =>
  byName(await listIn(s, rg, "loadBalancers", parseLoadBalancer, f));
export const listAppGateways = async (s: string, rg: string, f: FetchFn = fetch) =>
  byName(await listIn(s, rg, "applicationGateways", parseAppGateway, f));
export const listFirewalls = async (s: string, rg: string, f: FetchFn = fetch) =>
  byName(await listIn(s, rg, "azureFirewalls", parseFirewall, f));
export const listFirewallPolicies = async (s: string, rg: string, f: FetchFn = fetch) =>
  byName(await listIn(s, rg, "firewallPolicies", parseFirewallPolicy, f));
export const listIpGroups = async (s: string, rg: string, f: FetchFn = fetch) =>
  byName(await listIn(s, rg, "ipGroups", parseIpGroup, f));
export const listNatGateways = async (s: string, rg: string, f: FetchFn = fetch) =>
  byName(await listIn(s, rg, "natGateways", parseNatGateway, f));

export async function listRuleCollectionGroups(
  s: string,
  rg: string,
  policy: string,
  fetchFn: FetchFn = fetch,
): Promise<RuleCollectionGroup[]> {
  const path = `${rgPath(s, rg, "firewallPolicies")}/${enc(policy)}/ruleCollectionGroups`;
  const reply = await call("GET", withVersion(path), undefined, fetchFn);
  const groups = list(reply.body.value).map((r) => parseRuleCollectionGroup(r));
  return groups.toSorted((a, b) => a.priority - b.priority || a.name.localeCompare(b.name));
}

export const putVnet = (
  s: string,
  rg: string,
  v: VnetInput,
  f: FetchFn = fetch,
  sleep: Sleep = realSleep,
) => write("PUT", `${rgPath(s, rg, "virtualNetworks")}/${enc(v.name)}`, vnetBody(v), f, sleep);

export const deleteVnet = (
  s: string,
  rg: string,
  name: string,
  f: FetchFn = fetch,
  sleep: Sleep = realSleep,
) => write("DELETE", `${rgPath(s, rg, "virtualNetworks")}/${enc(name)}`, undefined, f, sleep);

export const putSubnet = (
  s: string,
  rg: string,
  vnet: string,
  sn: SubnetInput,
  f: FetchFn = fetch,
  sleep: Sleep = realSleep,
) =>
  write(
    "PUT",
    `${rgPath(s, rg, "virtualNetworks")}/${enc(vnet)}/subnets/${enc(sn.name)}`,
    subnetBody(sn),
    f,
    sleep,
  );

export const deleteSubnet = (
  s: string,
  rg: string,
  vnet: string,
  name: string,
  f: FetchFn = fetch,
  sleep: Sleep = realSleep,
) =>
  write(
    "DELETE",
    `${rgPath(s, rg, "virtualNetworks")}/${enc(vnet)}/subnets/${enc(name)}`,
    undefined,
    f,
    sleep,
  );

export const putRule = (
  s: string,
  rg: string,
  nsg: string,
  r: RuleInput,
  f: FetchFn = fetch,
  sleep: Sleep = realSleep,
) =>
  write(
    "PUT",
    `${rgPath(s, rg, "networkSecurityGroups")}/${enc(nsg)}/securityRules/${enc(r.name)}`,
    ruleBody(r),
    f,
    sleep,
  );

export const deleteRule = (
  s: string,
  rg: string,
  nsg: string,
  name: string,
  f: FetchFn = fetch,
  sleep: Sleep = realSleep,
) =>
  write(
    "DELETE",
    `${rgPath(s, rg, "networkSecurityGroups")}/${enc(nsg)}/securityRules/${enc(name)}`,
    undefined,
    f,
    sleep,
  );

export const deleteLoadBalancer = (
  s: string,
  rg: string,
  name: string,
  f: FetchFn = fetch,
  sleep: Sleep = realSleep,
) => write("DELETE", `${rgPath(s, rg, "loadBalancers")}/${enc(name)}`, undefined, f, sleep);

export type GatewayAction = "start" | "stop";

export const gatewayAction = (
  s: string,
  rg: string,
  name: string,
  action: GatewayAction,
  f: FetchFn = fetch,
  sleep: Sleep = realSleep,
) =>
  write(
    "POST",
    `${rgPath(s, rg, "applicationGateways")}/${enc(name)}/${action}`,
    undefined,
    f,
    sleep,
  );

/** Starts a backend health probe and reads the result once the operation has finished. */
export async function getBackendHealth(
  s: string,
  rg: string,
  name: string,
  fetchFn: FetchFn = fetch,
  sleep: Sleep = realSleep,
): Promise<BackendHealthPool[]> {
  const path = `${rgPath(s, rg, "applicationGateways")}/${enc(name)}/backendHealth`;
  const reply = await call("POST", withVersion(path), undefined, fetchFn);
  await awaitOperation(reply.operation, fetchFn, sleep);
  if (!reply.result) return [];
  return parseBackendHealth((await call("GET", reply.result, undefined, fetchFn)).body);
}

export const gatewayRunning = (g: Pick<AppGateway, "operationalState">): boolean =>
  g.operationalState !== "Stopped" && g.operationalState !== "Stopping";
