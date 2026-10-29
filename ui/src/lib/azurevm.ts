/** Client for Azure Virtual Machines and the networking they attach to: Microsoft.Compute/Network via the dashboard's in-process ARM proxy. */

import { parseArmId } from "./azurearm.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const VM_PROXY_BASE = "/dashboard/api/v1/azure/vm";
export const COMPUTE_API_VERSION = "2024-07-01";
export const NETWORK_API_VERSION = "2024-05-01";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type PowerAction = "start" | "powerOff" | "deallocate" | "restart";

export type VmDisk = { name: string; sizeGb: number; kind: "OS" | "Data"; type: string };

export type VirtualMachine = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  size: string;
  osType: string;
  power: string;
  provisioning: string;
  computerName: string;
  adminUsername: string;
  image: string;
  zones: string[];
  nicIds: string[];
  disks: VmDisk[];
};

export type Nic = {
  id: string;
  name: string;
  privateIps: string[];
  publicIpIds: string[];
  subnetId: string;
  nsgId: string;
  vmId: string;
};

export type PublicIp = { id: string; name: string; address: string; allocation: string };

export type Subnet = { name: string; prefix: string; nsg: string };

export type VirtualNetwork = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  prefixes: string[];
  state: string;
  subnets: Subnet[];
};

export type SecurityRule = {
  name: string;
  priority: number;
  direction: string;
  access: string;
  protocol: string;
  source: string;
  destination: string;
  ports: string;
};

export type SecurityGroup = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  rules: SecurityRule[];
};

export type Disk = {
  id: string;
  name: string;
  resourceGroup: string;
  sizeGb: number;
  sku: string;
  state: string;
  attachedTo: string;
};

/** The short name of the resource an ARM id ends with. */
export const lastSegment = (id: string): string => id.slice(id.lastIndexOf("/") + 1);

export function powerStateOf(vm: Json): string {
  const view = obj(obj(vm.properties).instanceView);
  for (const s of list(view.statuses)) {
    const code = str(s.code);
    if (code.startsWith("PowerState/")) return code.slice("PowerState/".length);
  }
  return "";
}

export function parseVm(raw: Json): VirtualMachine {
  const p = obj(raw.properties);
  const id = str(raw.id);
  const sp = obj(p.storageProfile);
  const os = obj(sp.osDisk);
  const image = obj(sp.imageReference);
  const disk = (d: Json, kind: VmDisk["kind"]): VmDisk => ({
    name: str(d.name),
    sizeGb: num(d.diskSizeGB),
    kind,
    type: str(obj(d.managedDisk).storageAccountType),
  });
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    size: str(obj(p.hardwareProfile).vmSize),
    osType: str(os.osType),
    power: powerStateOf(raw),
    provisioning: str(p.provisioningState),
    computerName: str(obj(p.osProfile).computerName),
    adminUsername: str(obj(p.osProfile).adminUsername),
    image: [str(image.publisher), str(image.offer), str(image.sku)].filter(Boolean).join(":"),
    zones: Array.isArray(raw.zones) ? raw.zones.map(String) : [],
    nicIds: list(obj(p.networkProfile).networkInterfaces).map((n) => str(n.id)),
    disks: [
      ...(os.name ? [disk(os, "OS")] : []),
      ...list(sp.dataDisks).map((d) => disk(d, "Data")),
    ],
  };
}

export function parseNic(raw: Json): Nic {
  const p = obj(raw.properties);
  const cfgs = list(p.ipConfigurations).map((c) => obj(c.properties));
  return {
    id: str(raw.id),
    name: str(raw.name),
    privateIps: cfgs.map((c) => str(c.privateIPAddress)).filter(Boolean),
    publicIpIds: cfgs.map((c) => str(obj(c.publicIPAddress).id)).filter(Boolean),
    subnetId: str(obj(cfgs[0]?.subnet).id),
    nsgId: str(obj(p.networkSecurityGroup).id),
    vmId: str(obj(p.virtualMachine).id),
  };
}

export function parsePublicIp(raw: Json): PublicIp {
  const p = obj(raw.properties);
  return {
    id: str(raw.id),
    name: str(raw.name),
    address: str(p.ipAddress),
    allocation: str(p.publicIPAllocationMethod),
  };
}

export function parseVnet(raw: Json): VirtualNetwork {
  const p = obj(raw.properties);
  const id = str(raw.id);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    prefixes: Array.isArray(obj(p.addressSpace).addressPrefixes)
      ? (obj(p.addressSpace).addressPrefixes as unknown[]).map(String)
      : [],
    state: str(p.provisioningState),
    subnets: list(p.subnets).map((s) => {
      const sp = obj(s.properties);
      return {
        name: str(s.name),
        prefix:
          str(sp.addressPrefix) ||
          (Array.isArray(sp.addressPrefixes) ? sp.addressPrefixes.map(String).join(", ") : ""),
        nsg: lastSegment(str(obj(sp.networkSecurityGroup).id)),
      };
    }),
  };
}

function portsOf(p: Json): string {
  if (Array.isArray(p.destinationPortRanges) && p.destinationPortRanges.length > 0)
    return p.destinationPortRanges.map(String).join(", ");
  return str(p.destinationPortRange) || "*";
}

export function parseNsg(raw: Json): SecurityGroup {
  const id = str(raw.id);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    rules: list(obj(raw.properties).securityRules)
      .map((r) => {
        const rp = obj(r.properties);
        return {
          name: str(r.name),
          priority: num(rp.priority),
          direction: str(rp.direction),
          access: str(rp.access),
          protocol: str(rp.protocol),
          source: str(rp.sourceAddressPrefix) || "*",
          destination: str(rp.destinationAddressPrefix) || "*",
          ports: portsOf(rp),
        };
      })
      .toSorted((a, b) => a.priority - b.priority),
  };
}

export function parseDisk(raw: Json): Disk {
  const p = obj(raw.properties);
  const id = str(raw.id);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    sizeGb: num(p.diskSizeGB),
    sku: str(obj(raw.sku).name),
    state: str(p.diskState),
    attachedTo: lastSegment(str(raw.managedBy)),
  };
}

/** Which power actions a power state allows. */
export function allowedActions(power: string): PowerAction[] {
  switch (power) {
    case "running":
      return ["restart", "powerOff", "deallocate"];
    case "stopped":
      return ["start", "deallocate"];
    case "deallocated":
      return ["start"];
    default:
      return [];
  }
}

const sub = (s: string) => `/subscriptions/${enc(s)}`;
const byGroupName = <T extends { resourceGroup: string; name: string }>(a: T, b: T) =>
  a.resourceGroup.localeCompare(b.resourceGroup) || a.name.localeCompare(b.name);

async function listType<T>(
  subscription: string,
  provider: "Compute" | "Network",
  type: string,
  parse: (r: Json) => T,
  query: string,
  fetchFn: FetchFn,
): Promise<T[]> {
  const version = provider === "Compute" ? COMPUTE_API_VERSION : NETWORK_API_VERSION;
  const body = await restCall(
    VM_PROXY_BASE,
    "GET",
    `${sub(subscription)}/providers/Microsoft.${provider}/${type}?api-version=${version}${query}`,
    undefined,
    fetchFn,
  );
  return list(body.value).map((r) => parse(r));
}

export async function listVms(s: string, fetchFn: FetchFn = fetch) {
  return (
    await listType(s, "Compute", "virtualMachines", parseVm, "&statusOnly=true", fetchFn)
  ).toSorted(byGroupName);
}

export async function listDisks(s: string, fetchFn: FetchFn = fetch) {
  return (await listType(s, "Compute", "disks", parseDisk, "", fetchFn)).toSorted(byGroupName);
}

export const listNics = (s: string, fetchFn: FetchFn = fetch) =>
  listType(s, "Network", "networkInterfaces", parseNic, "", fetchFn);

export const listPublicIps = (s: string, fetchFn: FetchFn = fetch) =>
  listType(s, "Network", "publicIPAddresses", parsePublicIp, "", fetchFn);

export async function listVnets(s: string, fetchFn: FetchFn = fetch) {
  return (await listType(s, "Network", "virtualNetworks", parseVnet, "", fetchFn)).toSorted(
    byGroupName,
  );
}

export async function listNsgs(s: string, fetchFn: FetchFn = fetch) {
  return (await listType(s, "Network", "networkSecurityGroups", parseNsg, "", fetchFn)).toSorted(
    byGroupName,
  );
}

export async function powerAction(
  s: string,
  vm: Pick<VirtualMachine, "resourceGroup" | "name">,
  action: PowerAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    VM_PROXY_BASE,
    "POST",
    `${sub(s)}/resourceGroups/${enc(vm.resourceGroup)}/providers/Microsoft.Compute/virtualMachines/${enc(vm.name)}/${action}?api-version=${COMPUTE_API_VERSION}`,
    undefined,
    fetchFn,
  );
}

/** The private and public addresses of a VM, resolved through its NICs. */
export function vmAddresses(
  vm: Pick<VirtualMachine, "nicIds">,
  nics: Nic[],
  ips: PublicIp[],
): { privateIps: string[]; publicIps: string[] } {
  const mine = nics.filter((n) => vm.nicIds.some((id) => id.toLowerCase() === n.id.toLowerCase()));
  const publicIps = mine
    .flatMap((n) => n.publicIpIds)
    .map((id) => ips.find((p) => p.id.toLowerCase() === id.toLowerCase())?.address ?? "")
    .filter(Boolean);
  return { privateIps: mine.flatMap((n) => n.privateIps), publicIps };
}

export function powerLabel(power: string): string {
  return power ? power.charAt(0).toUpperCase() + power.slice(1) : "Unknown";
}
