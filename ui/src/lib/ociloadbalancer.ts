/** Client for the Oracle Cloud Load Balancer emulator: load balancers, backend health and a listener "try it" call via the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  ociPostOne,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const LB_PROXY_BASE = "/dashboard/api/v1/oci/loadbalancer";
export const LB_COMPARTMENT_KEY = "gopherstack_ocilb_compartment";
export const LB_MIN_MBPS = 10;
export const LB_MAX_MBPS = 8000;

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);
const sortedValues = (v: unknown): Json[] =>
  Object.entries(obj(v))
    .toSorted(([a], [b]) => a.localeCompare(b))
    .map(([, x]) => obj(x));

export type LbListener = {
  name: string;
  protocol: string;
  port: number;
  defaultBackendSet: string;
  hostnames: string[];
  pathRouteSet: string;
  routingPolicy: string;
  ruleSets: string[];
  tls: boolean;
  address: string;
};

export type LbBackend = {
  name: string;
  ipAddress: string;
  port: number;
  weight: number;
  backup: boolean;
  drain: boolean;
  offline: boolean;
  maxConnections: number | null;
};

export type LbHealthChecker = {
  protocol: string;
  port: number;
  urlPath: string;
  returnCode: number;
  retries: number;
  timeoutMillis: number;
  intervalMillis: number;
};

export type LbBackendSet = {
  name: string;
  policy: string;
  healthChecker: LbHealthChecker;
  backends: LbBackend[];
};

export type LbPathRoute = { path: string; matchType: string; backendSet: string };
export type LbPathRouteSet = { name: string; routes: LbPathRoute[] };
export type LbRoutingRule = { name: string; condition: string; backendSets: string[] };
export type LbRoutingPolicy = { name: string; rules: LbRoutingRule[] };
export type LbRuleSet = { name: string; items: string[] };
export type LbCertificate = {
  name: string;
  subjects: string[];
  issuer: string;
  notBefore: string;
  notAfter: string;
};

export type LbIp = { address: string; isPublic: boolean };

export type LoadBalancer = {
  id: string;
  name: string;
  state: string;
  shape: string;
  isPrivate: boolean;
  ips: LbIp[];
  subnetIds: string[];
  created: string;
  listeners: LbListener[];
  backendSets: LbBackendSet[];
  pathRouteSets: LbPathRouteSet[];
  routingPolicies: LbRoutingPolicy[];
  ruleSets: LbRuleSet[];
  certificates: LbCertificate[];
};

export type LbWorkRequest = {
  id: string;
  type: string;
  state: string;
  message: string;
  accepted: string;
  finished: string;
};

export type LbHealth = { status: string; detail: string };

export type TryRequest = {
  method: string;
  path: string;
  query: string;
  body: string;
  virtualHost: string;
  headers: { name: string; value: string }[];
};

export type TryResponse = {
  status: number;
  headers: Record<string, string>;
  body: string;
  encoding: string;
  truncated: boolean;
};

export const TRY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"];

export function parseListener(raw: Json): LbListener {
  return {
    name: str(raw.name),
    protocol: str(raw.protocol),
    port: num(raw.port),
    defaultBackendSet: str(raw.defaultBackendSetName),
    hostnames: strs(raw.hostnameNames),
    pathRouteSet: str(raw.pathRouteSetName),
    routingPolicy: str(raw.routingPolicyName),
    ruleSets: strs(raw.ruleSetNames),
    tls: Boolean(raw.sslConfiguration),
    address: str(raw.gopherstackAddress),
  };
}

export function parseBackend(raw: Json): LbBackend {
  return {
    name: str(raw.name),
    ipAddress: str(raw.ipAddress),
    port: num(raw.port),
    weight: num(raw.weight) || 1,
    backup: raw.backup === true,
    drain: raw.drain === true,
    offline: raw.offline === true,
    maxConnections: raw.maxConnections === undefined ? null : num(raw.maxConnections),
  };
}

export function parseBackendSet(raw: Json): LbBackendSet {
  const hc = obj(raw.healthChecker);
  return {
    name: str(raw.name),
    policy: str(raw.policy),
    healthChecker: {
      protocol: str(hc.protocol),
      port: num(hc.port),
      urlPath: str(hc.urlPath),
      returnCode: num(hc.returnCode),
      retries: num(hc.retries),
      timeoutMillis: num(hc.timeoutInMillis),
      intervalMillis: num(hc.intervalInMillis),
    },
    backends: list(raw.backends).map((b) => parseBackend(b)),
  };
}

function ruleSummary(r: Json): string {
  const a = str(r.action);
  const detail = str(r.header) || str(r.prefix) || str(r.suffix) || "";
  return detail ? `${a} ${detail}` : a;
}

export function parseCertificate(raw: Json): LbCertificate {
  return {
    name: str(raw.certificateName),
    subjects: strs(raw.subjectNames),
    issuer: str(raw.issuer),
    notBefore: str(raw.notBefore),
    notAfter: str(raw.notAfter),
  };
}

export function shapeLabel(raw: Json): string {
  const name = str(raw.shapeName);
  const d = obj(raw.shapeDetails);
  if (d.minimumBandwidthInMbps === undefined) return name;
  return `${name} ${num(d.minimumBandwidthInMbps)}-${num(d.maximumBandwidthInMbps)} Mbps`;
}

export function parseLoadBalancer(raw: Json): LoadBalancer {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    shape: shapeLabel(raw),
    isPrivate: raw.isPrivate === true,
    ips: list(raw.ipAddresses).map((i) => ({
      address: str(i.ipAddress),
      isPublic: i.isPublic === true,
    })),
    subnetIds: strs(raw.subnetIds),
    created: str(raw.timeCreated),
    listeners: sortedValues(raw.listeners).map((l) => parseListener(l)),
    backendSets: sortedValues(raw.backendSets).map((s) => parseBackendSet(s)),
    pathRouteSets: sortedValues(raw.pathRouteSets).map((s) => ({
      name: str(s.name),
      routes: list(s.pathRoutes).map((r) => ({
        path: str(r.path),
        matchType: str(obj(r.pathMatchType).matchType),
        backendSet: str(r.backendSetName),
      })),
    })),
    routingPolicies: sortedValues(raw.routingPolicies).map((p) => ({
      name: str(p.name),
      rules: list(p.rules).map((r) => ({
        name: str(r.name),
        condition: str(r.condition),
        backendSets: list(r.actions).map((a) => str(a.backendSetName)),
      })),
    })),
    ruleSets: sortedValues(raw.ruleSets).map((s) => ({
      name: str(s.name),
      items: list(s.items).map((i) => ruleSummary(i)),
    })),
    certificates: sortedValues(raw.certificates).map((c) => parseCertificate(c)),
  };
}

export function parseWorkRequest(raw: Json): LbWorkRequest {
  return {
    id: str(raw.id),
    type: str(raw.type),
    state: str(raw.lifecycleState),
    message: str(raw.message),
    accepted: str(raw.timeAccepted),
    finished: str(raw.timeFinished),
  };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(LB_PROXY_BASE, fetchFn);

export async function getLoadBalancer(id: string, fetchFn: FetchFn = fetch): Promise<LoadBalancer> {
  return parseLoadBalancer(await ociGetOne(LB_PROXY_BASE, `/api/loadBalancers/${enc(id)}`, fetchFn));
}

/** Load balancers of one compartment with listeners and backend sets (the list answer omits them, so each is fetched). */
export async function listLoadBalancers(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<LoadBalancer[]> {
  const items = await ociListPages(
    LB_PROXY_BASE,
    "/api/loadBalancers",
    `compartmentId=${enc(compartment)}`,
    "",
    fetchFn,
  );
  return Promise.all(items.map((i) => getLoadBalancer(str(i.id), fetchFn)));
}

export async function listWorkRequests(
  id: string,
  fetchFn: FetchFn = fetch,
): Promise<LbWorkRequest[]> {
  const items = await ociListPages(
    LB_PROXY_BASE,
    `/api/loadBalancers/${enc(id)}/workRequests`,
    "",
    "",
    fetchFn,
  );
  return items.map((r) => parseWorkRequest(r));
}

const setPath = (lbId: string, set: string) =>
  `/api/loadBalancers/${enc(lbId)}/backendSets/${enc(set)}`;
const backendPath = (lbId: string, set: string, backend: string) =>
  `${setPath(lbId, set)}/backends/${enc(backend)}`;

export async function getBackendSetHealth(
  lbId: string,
  set: string,
  fetchFn: FetchFn = fetch,
): Promise<LbHealth> {
  const h = await ociGetOne(LB_PROXY_BASE, `${setPath(lbId, set)}/health`, fetchFn);
  const bad = strs(h.criticalStateBackendNames).length + strs(h.warningStateBackendNames).length;
  return { status: str(h.status) || "UNKNOWN", detail: `${num(h.totalBackendCount)} backends, ${bad} unhealthy` };
}

export async function getBackendHealth(
  lbId: string,
  set: string,
  backend: string,
  fetchFn: FetchFn = fetch,
): Promise<LbHealth> {
  const h = await ociGetOne(LB_PROXY_BASE, `${backendPath(lbId, set, backend)}/health`, fetchFn);
  const first = obj(list(h.healthCheckResults)[0]);
  return { status: str(h.status) || "UNKNOWN", detail: str(first.healthCheckStatus) };
}

export async function getLoadBalancerHealth(
  lbId: string,
  fetchFn: FetchFn = fetch,
): Promise<LbHealth> {
  const h = await ociGetOne(LB_PROXY_BASE, `/api/loadBalancers/${enc(lbId)}/health`, fetchFn);
  return { status: str(h.status) || "UNKNOWN", detail: `${num(h.totalBackendSetCount)} backend sets` };
}

export const backendKey = (set: string, backend: string): string => `${set}\u0000${backend}`;

/** Health of every backend of the load balancer, keyed by `backendKey`, plus each set's aggregate under its name. */
export async function loadHealth(
  lb: Pick<LoadBalancer, "id" | "backendSets">,
  fetchFn: FetchFn = fetch,
): Promise<Record<string, LbHealth>> {
  const out: Record<string, LbHealth> = {};
  await Promise.all(
    lb.backendSets.map(async (s) => {
      out[s.name] = await getBackendSetHealth(lb.id, s.name, fetchFn);
      await Promise.all(
        s.backends.map(async (b) => {
          out[backendKey(s.name, b.name)] = await getBackendHealth(lb.id, s.name, b.name, fetchFn);
        }),
      );
    }),
  );
  return out;
}

export type NewBackend = { ipAddress: string; port: number; weight: number };

export async function addBackend(
  lbId: string,
  set: string,
  b: NewBackend,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(LB_PROXY_BASE, "POST", `${setPath(lbId, set)}/backends`, b, fetchFn);
}

/** UpdateBackend replaces weight, backup, drain and offline together, so the full current state is sent. */
export async function updateBackend(
  lbId: string,
  set: string,
  current: LbBackend,
  change: Partial<Pick<LbBackend, "weight" | "drain" | "offline" | "backup">>,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const next = { ...current, ...change };
  const body: Json = {
    weight: next.weight,
    backup: next.backup,
    drain: next.drain,
    offline: next.offline,
  };
  if (next.maxConnections !== null) body.maxConnections = next.maxConnections;
  await restCall(LB_PROXY_BASE, "PUT", backendPath(lbId, set, current.name), body, fetchFn);
}

export async function deleteBackend(
  lbId: string,
  set: string,
  backend: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(LB_PROXY_BASE, "DELETE", backendPath(lbId, set, backend), undefined, fetchFn);
}

export type NewLoadBalancer = {
  compartmentId: string;
  displayName: string;
  subnetId: string;
  isPrivate: boolean;
  minimumBandwidthInMbps: number;
  maximumBandwidthInMbps: number;
  listenerPort: number | null;
};

export async function createLoadBalancer(
  lb: NewLoadBalancer,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const { listenerPort, ...rest } = lb;
  await restCall(
    LB_PROXY_BASE,
    "POST",
    "/api/loadBalancers",
    listenerPort === null ? rest : { ...rest, listenerPort },
    fetchFn,
  );
}

export async function deleteLoadBalancer(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(LB_PROXY_BASE, "DELETE", `/api/loadBalancers/${enc(id)}`, undefined, fetchFn);
}

export const validPort = (v: string): boolean => /^\d+$/.test(v.trim()) && Number(v) >= 1 && Number(v) <= 65535;
export const validWeight = (v: string): boolean => /^\d+$/.test(v.trim()) && Number(v) >= 1;
export const validBandwidth = (v: string): boolean =>
  /^\d+$/.test(v.trim()) && Number(v) >= LB_MIN_MBPS && Number(v) <= LB_MAX_MBPS;
export const validIp = (v: string): boolean => {
  const s = v.trim();
  return /^(\d{1,3}\.){3}\d{1,3}$/.test(s)
    ? s.split(".").every((p) => Number(p) <= 255)
    : /^[0-9a-fA-F:]+$/.test(s) && s.includes(":");
};

/** Sends a request to the selected listener's in-process data plane; the proxy fixes the destination. */
export async function tryIt(
  lbId: string,
  listener: string,
  req: TryRequest,
  fetchFn: FetchFn = fetch,
): Promise<TryResponse> {
  const { body } = await ociPostOne(
    LB_PROXY_BASE,
    `/api/loadBalancers/${enc(lbId)}/listeners/${enc(listener)}/invoke`,
    {
      method: req.method,
      path: req.path,
      query: req.query.replace(/^\?/, ""),
      body: req.body,
      virtualHost: req.virtualHost.trim(),
      headers: req.headers.filter((h) => h.name.trim()),
    },
    fetchFn,
  );
  const b = obj(body);
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(obj(b.headers))) headers[k] = String(v);
  return {
    status: num(b.status),
    headers,
    body: str(b.body),
    encoding: str(b.encoding) || "utf8",
    truncated: b.truncated === true,
  };
}

/** Pretty-prints a response body when it is JSON. */
export function prettyBody(r: Pick<TryResponse, "body" | "encoding">): string {
  if (r.encoding === "base64") return `(binary, base64)\n${r.body}`;
  try {
    return JSON.stringify(JSON.parse(r.body), null, 2);
  } catch {
    return r.body;
  }
}

export const isServing = (lb: Pick<LoadBalancer, "state">): boolean => lb.state === "ACTIVE";

/** A listener the try-it panel can reach: serving, plain text. */
export const canTry = (l: Pick<LbListener, "address" | "tls">): boolean => l.address !== "" && !l.tls;

export type HealthTone = "ok" | "warn" | "bad" | "unknown";

export function healthTone(status: string): HealthTone {
  switch (status) {
    case "OK":
      return "ok";
    case "WARNING":
      return "warn";
    case "CRITICAL":
      return "bad";
    default:
      return "unknown";
  }
}

export const readStoredLbCompartment = () => readStoredValue(LB_COMPARTMENT_KEY);
export const writeStoredLbCompartment = (id: string) => writeStoredValue(LB_COMPARTMENT_KEY, id);
