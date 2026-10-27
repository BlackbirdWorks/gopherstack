/** Client for the Oracle Cloud Core Services emulator (networking, compute, block storage) via the dashboard's in-process proxy. */

import { GcpRestError, restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const CORE_PROXY_BASE = "/dashboard/api/v1/oci/core";
export const CORE_COMPARTMENT_KEY = "gopherstack_ocicore_compartment";

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const arr = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const rec = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});

export type InstanceAction = "START" | "STOP" | "SOFTRESET";

export type CoreResource = {
  id: string;
  name: string;
  state: string;
  created: string;
  raw: Json;
};

export type CoreKind =
  | "vcns"
  | "subnets"
  | "routeTables"
  | "securityLists"
  | "networkSecurityGroups"
  | "instances"
  | "volumes"
  | "bootVolumes"
  | "volumeAttachments"
  | "bootVolumeAttachments"
  | "volumeBackups";

export type InstanceVnic = {
  attachmentId: string;
  vnicId: string;
  state: string;
  name: string;
  privateIp: string;
  publicIp: string;
  mac: string;
  subnetId: string;
  isPrimary: boolean;
};

export type RuleView = {
  id: string;
  direction: string;
  protocol: string;
  peer: string;
  stateless: boolean;
  ports: string;
  description: string;
};

export function parseResource(raw: Json): CoreResource {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    created: str(raw.timeCreated),
    raw,
  };
}

/** Last segment of an OCID, for compact display. */
export const shortId = (ocid: string): string => (ocid.length > 20 ? `…${ocid.slice(-12)}` : ocid);

/** OCI protocol numbers to names; "all" is protocol 'all'. */
export function protocolName(p: string): string {
  switch (p) {
    case "1":
      return "ICMP";
    case "6":
      return "TCP";
    case "17":
      return "UDP";
    case "58":
      return "ICMPv6";
    case "all":
      return "ALL";
    default:
      return p;
  }
}

function portRange(opts: unknown): string {
  const o = rec(opts);
  const r = rec(o.destinationPortRange);
  if (r.min === undefined) return "all";
  return num(r.min) === num(r.max) ? String(num(r.min)) : `${num(r.min)}-${num(r.max)}`;
}

function ruleOf(raw: Json, direction: string): RuleView {
  const tcp = raw.tcpOptions ?? raw.udpOptions;
  return {
    id: str(raw.id),
    direction,
    protocol: protocolName(str(raw.protocol)),
    peer: str(raw.source) || str(raw.destination) || "—",
    stateless: raw.isStateless === true,
    ports: tcp ? portRange(tcp) : "all",
    description: str(raw.description),
  };
}

/** Flattens a security list's ingress and egress rules. */
export function securityListRules(raw: Json): RuleView[] {
  return [
    ...arr(raw.ingressSecurityRules).map((r) => ruleOf(r, "INGRESS")),
    ...arr(raw.egressSecurityRules).map((r) => ruleOf(r, "EGRESS")),
  ];
}

/** Flattens a network security group's rules from the NSG rule listing. */
export function nsgRules(items: Json[]): RuleView[] {
  return items.map((r) => ruleOf(r, str(r.direction)));
}

export function routeRules(raw: Json): { destination: string; target: string }[] {
  return arr(raw.routeRules).map((r) => ({
    destination: str(r.destination) || str(r.cidrBlock),
    target: str(r.networkEntityId),
  }));
}

function failure(res: Response, body: Json): GcpRestError {
  return new GcpRestError(
    res.status,
    str(body.code),
    str(body.message) || res.statusText || `HTTP ${res.status}`,
  );
}

async function coreGet(path: string, fetchFn: FetchFn): Promise<{ body: unknown; next: string }> {
  let res: Response;
  try {
    res = await fetchFn(`${CORE_PROXY_BASE}${path}`);
  } catch (e) {
    throw new GcpRestError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  const text = await res.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    // non-JSON error body: keep the status text
  }
  if (!res.ok) throw failure(res, rec(body));
  return { body, next: res.headers.get("opc-next-page") ?? "" };
}

async function listPages(path: string, query: string, fetchFn: FetchFn): Promise<Json[]> {
  const out: Json[] = [];
  let page = "";
  do {
    const qs = [query, page ? `page=${enc(page)}` : ""].filter(Boolean).join("&");
    const { body, next } = await coreGet(`${path}?${qs}`, fetchFn);
    out.push(...arr(body));
    page = next;
  } while (page);
  return out;
}

export async function listCompartments(fetchFn: FetchFn = fetch): Promise<string[]> {
  const body = await restCall(CORE_PROXY_BASE, "GET", "/compartments", undefined, fetchFn);
  return (Array.isArray(body.compartments) ? body.compartments : []).map(String);
}

export async function listResources(
  kind: CoreKind,
  compartment: string,
  extra = "",
  fetchFn: FetchFn = fetch,
): Promise<CoreResource[]> {
  const query = [`compartmentId=${enc(compartment)}`, extra].filter(Boolean).join("&");
  return (await listPages(`/api/${kind}`, query, fetchFn)).map((r) => parseResource(r));
}

export async function listNsgRules(nsgId: string, fetchFn: FetchFn = fetch): Promise<RuleView[]> {
  return nsgRules(
    await listPages(`/api/networkSecurityGroups/${enc(nsgId)}/securityRules`, "", fetchFn),
  );
}

/** The VNICs attached to an instance with their addresses. */
export async function listInstanceVnics(
  compartment: string,
  instanceId: string,
  fetchFn: FetchFn = fetch,
): Promise<InstanceVnic[]> {
  const attachments = await listPages(
    "/api/vnicAttachments",
    `compartmentId=${enc(compartment)}&instanceId=${enc(instanceId)}`,
    fetchFn,
  );
  const out: InstanceVnic[] = [];
  for (const a of attachments) {
    const vnicId = str(a.vnicId);
    const vnic = vnicId ? rec((await coreGet(`/api/vnics/${enc(vnicId)}`, fetchFn)).body) : {};
    out.push({
      attachmentId: str(a.id),
      vnicId,
      state: str(a.lifecycleState),
      name: str(vnic.displayName),
      privateIp: str(vnic.privateIp),
      publicIp: str(vnic.publicIp),
      mac: str(vnic.macAddress),
      subnetId: str(a.subnetId),
      isPrimary: vnic.isPrimary === true,
    });
  }
  return out;
}

export async function instanceAction(
  id: string,
  action: InstanceAction,
  fetchFn: FetchFn = fetch,
): Promise<CoreResource> {
  return parseResource(
    await restCall(
      CORE_PROXY_BASE,
      "POST",
      `/api/instances/${enc(id)}?action=${action}`,
      undefined,
      fetchFn,
    ),
  );
}

/** Which power actions an instance state allows. */
export function allowedActions(state: string): InstanceAction[] {
  if (state === "RUNNING") return ["STOP", "SOFTRESET"];
  if (state === "STOPPED") return ["START"];
  return [];
}

export function instanceShape(i: CoreResource): string {
  const cfg = rec(i.raw.shapeConfig);
  const base = str(i.raw.shape);
  return cfg.ocpus === undefined
    ? base
    : `${base} (${num(cfg.ocpus)} OCPU, ${num(cfg.memoryInGBs)} GB)`;
}

function readStored(key: string): string {
  try {
    return localStorage.getItem(key)?.trim() ?? "";
  } catch {
    return "";
  }
}

export const readStoredCompartment = () => readStored(CORE_COMPARTMENT_KEY);

export function writeStoredCompartment(id: string): void {
  try {
    localStorage.setItem(CORE_COMPARTMENT_KEY, id);
  } catch {
    // storage unavailable: selection applies for this session only
  }
}
