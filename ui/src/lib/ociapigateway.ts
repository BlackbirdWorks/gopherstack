/** Client for the Oracle Cloud API Gateway emulator: gateways, deployments, certificates and a data-plane "try it" call via the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  ociPostOne,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { str, type FetchFn, type Json } from "./gcprest.ts";

export const APIGW_PROXY_BASE = "/dashboard/api/v1/oci/apigateway";
export const APIGW_COMPARTMENT_KEY = "gopherstack_ociapigateway_compartment";

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);

export type ApigwGateway = {
  id: string;
  name: string;
  state: string;
  hostname: string;
  endpointType: string;
  subnetId: string;
  certificateId: string;
  ipMode: string;
  created: string;
};

export type ApigwRoute = {
  path: string;
  methods: string[];
  backendType: string;
  backend: string;
  policies: string[];
};

export type ApigwDeployment = {
  id: string;
  name: string;
  state: string;
  gatewayId: string;
  pathPrefix: string;
  endpoint: string;
  policies: string[];
  routes: ApigwRoute[];
  created: string;
};

export type ApigwCertificate = {
  id: string;
  name: string;
  state: string;
  subjects: string[];
  notAfter: string;
  created: string;
};

export type TryRequest = {
  method: string;
  path: string;
  query: string;
  body: string;
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

export function parseGateway(raw: Json): ApigwGateway {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    hostname: str(raw.hostname),
    endpointType: str(raw.endpointType),
    subnetId: str(raw.subnetId),
    certificateId: str(raw.certificateId),
    ipMode: str(raw.ipMode),
    created: str(raw.timeCreated),
  };
}

const BACKEND_LABEL: Record<string, string> = {
  HTTP_BACKEND: "HTTP",
  ORACLE_FUNCTIONS_BACKEND: "Functions",
  STOCK_RESPONSE_BACKEND: "Stock response",
  DYNAMIC_ROUTING_BACKEND: "Dynamic routing",
};

/** Human label for a backend discriminator. */
export const backendLabel = (type: string): string => BACKEND_LABEL[type] ?? (type || "—");

function backendDetail(b: Json): string {
  switch (str(b.type)) {
    case "HTTP_BACKEND":
      return str(b.url);
    case "ORACLE_FUNCTIONS_BACKEND":
      return str(b.functionId);
    case "STOCK_RESPONSE_BACKEND":
      return `HTTP ${num(b.status)}`;
    case "DYNAMIC_ROUTING_BACKEND":
      return `${list(b.routingBackends).length} backends`;
    default:
      return "";
  }
}

const AUTH_LABEL: Record<string, string> = {
  JWT_AUTHENTICATION: "JWT auth",
  TOKEN_AUTHENTICATION: "Token auth",
  CUSTOM_AUTHENTICATION: "Custom auth",
};

function transformCount(t: Json): number {
  return Object.values(t).reduce<number>((n, v) => n + list(obj(v).items).length, 0);
}

/** One-line labels for the request/response policies in a policy block (deployment or route level). */
export function summarizePolicies(request: Json, response: Json = {}): string[] {
  const out: string[] = [];
  const auth = obj(request.authentication);
  if (str(auth.type)) out.push(AUTH_LABEL[str(auth.type)] ?? str(auth.type));
  const authz = obj(request.authorization);
  if (str(authz.type)) out.push(`authz ${str(authz.type).toLowerCase()}`);
  const rate = obj(request.rateLimiting);
  if (rate.rateInRequestsPerSecond !== undefined) {
    out.push(
      `rate ${num(rate.rateInRequestsPerSecond)}/s ${str(rate.rateKey).toLowerCase()}`.trim(),
    );
  }
  if (request.cors) out.push("CORS");
  const reqT = transformCount(obj(request.headerTransformations));
  const qT = transformCount(obj(request.queryParameterTransformations));
  const resT = transformCount(obj(response.headerTransformations));
  if (reqT) out.push(`${reqT} request header rule${reqT === 1 ? "" : "s"}`);
  if (qT) out.push(`${qT} query rule${qT === 1 ? "" : "s"}`);
  if (resT) out.push(`${resT} response header rule${resT === 1 ? "" : "s"}`);
  return out;
}

export function parseRoute(raw: Json): ApigwRoute {
  const backend = obj(raw.backend);
  return {
    path: str(raw.path),
    methods: strs(raw.methods),
    backendType: str(backend.type),
    backend: backendDetail(backend),
    policies: summarizePolicies(obj(raw.requestPolicies), obj(raw.responsePolicies)),
  };
}

export function parseDeployment(raw: Json): ApigwDeployment {
  const spec = obj(raw.specification);
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    gatewayId: str(raw.gatewayId),
    pathPrefix: str(raw.pathPrefix),
    endpoint: str(raw.endpoint),
    policies: summarizePolicies(obj(spec.requestPolicies)),
    routes: list(spec.routes).map((r) => parseRoute(r)),
    created: str(raw.timeCreated),
  };
}

/** Reads a certificate summary; the list answer carries subjects and expiry, never key material. */
export function parseCertificate(raw: Json): ApigwCertificate {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    subjects: strs(raw.subjectNames),
    notAfter: str(raw.timeNotValidAfter),
    created: str(raw.timeCreated),
  };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(APIGW_PROXY_BASE, fetchFn);

const cq = (compartment: string) => `compartmentId=${enc(compartment)}`;

export async function listGateways(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<ApigwGateway[]> {
  const items = await ociListPages(
    APIGW_PROXY_BASE,
    "/api/gateways",
    cq(compartment),
    "items",
    fetchFn,
  );
  return items.map((r) => parseGateway(r));
}

export async function getGateway(id: string, fetchFn: FetchFn = fetch): Promise<ApigwGateway> {
  return parseGateway(await ociGetOne(APIGW_PROXY_BASE, `/api/gateways/${enc(id)}`, fetchFn));
}

/** Deployments of one gateway with their routes (the list answer omits the specification, so each is fetched). */
export async function listDeployments(
  compartment: string,
  gatewayId: string,
  fetchFn: FetchFn = fetch,
): Promise<ApigwDeployment[]> {
  const items = await ociListPages(
    APIGW_PROXY_BASE,
    "/api/deployments",
    `${cq(compartment)}&gatewayId=${enc(gatewayId)}`,
    "items",
    fetchFn,
  );
  return Promise.all(
    items.map(async (d) =>
      parseDeployment(
        await ociGetOne(APIGW_PROXY_BASE, `/api/deployments/${enc(str(d.id))}`, fetchFn),
      ),
    ),
  );
}

export async function listCertificates(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<ApigwCertificate[]> {
  const items = await ociListPages(
    APIGW_PROXY_BASE,
    "/api/certificates",
    cq(compartment),
    "items",
    fetchFn,
  );
  return items.map((r) => parseCertificate(r));
}

/** Request path through a deployment: its prefix joined to a route path. */
export function joinPath(prefix: string, route: string): string {
  const p = prefix.replace(/\/+$/, "");
  const r = route.startsWith("/") ? route : `/${route}`;
  return `${p}${r}` || "/";
}

/** Sends a request to the selected gateway's in-process data plane; the proxy fixes the destination. */
export async function tryIt(
  gatewayId: string,
  req: TryRequest,
  fetchFn: FetchFn = fetch,
): Promise<TryResponse> {
  const { body } = await ociPostOne(
    APIGW_PROXY_BASE,
    `/api/gateways/${enc(gatewayId)}/invoke`,
    {
      method: req.method,
      path: req.path,
      query: req.query.replace(/^\?/, ""),
      body: req.body,
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

export const isServing = (g: Pick<ApigwGateway, "state">): boolean => g.state === "ACTIVE";

export const readStoredApigwCompartment = () => readStoredValue(APIGW_COMPARTMENT_KEY);
export const writeStoredApigwCompartment = (id: string) =>
  writeStoredValue(APIGW_COMPARTMENT_KEY, id);
