/** Client for Azure API Management (services, APIs, products, subscriptions, named values, policies, try-it) via the dashboard's in-process ARM proxy. */

import { parseArmId } from "./azurearm.ts";
import { GcpRestError, restCall, str, type FetchFn, type Json } from "./gcprest.ts";
import type { TryResponse } from "./ociapigateway.ts";
import { cleanQuery, readTryResponse, type HeaderPair } from "./tryit.ts";

export const APIM_PROXY_BASE = "/dashboard/api/v1/azure/apim";
export const APIM_API_VERSION = "2024-05-01";
const Q = `?api-version=${APIM_API_VERSION}`;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);

export type ApimService = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  sku: string;
  capacity: number;
  state: string;
  gatewayUrl: string;
  publisher: string;
};

export type ApimApi = {
  id: string;
  displayName: string;
  path: string;
  protocols: string[];
  serviceUrl: string;
  subscriptionRequired: boolean;
};

export type ApimOperation = {
  id: string;
  displayName: string;
  method: string;
  urlTemplate: string;
};

export type ApimProduct = {
  id: string;
  displayName: string;
  state: string;
  subscriptionRequired: boolean;
  approvalRequired: boolean;
};

export type ApimSubscription = {
  id: string;
  displayName: string;
  scope: string;
  state: string;
  createdDate: string;
};

export type ApimNamedValue = {
  id: string;
  displayName: string;
  secret: boolean;
  keyVault: boolean;
  value: string;
  tags: string[];
};

export type ApimTarget = Pick<ApimService, "resourceGroup" | "name">;

export type ApimTryRequest = {
  method: string;
  path: string;
  query: string;
  body: string;
  headers: HeaderPair[];
  subscription: string;
  api: string;
};

export function parseService(raw: Json): ApimService {
  const p = obj(raw.properties);
  const id = str(raw.id);
  const sku = obj(raw.sku);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    sku: str(sku.name),
    capacity: typeof sku.capacity === "number" ? sku.capacity : Number(sku.capacity) || 0,
    state: str(p.provisioningState),
    gatewayUrl: str(p.gatewayUrl),
    publisher: str(p.publisherName),
  };
}

export const parseApi = (raw: Json): ApimApi => {
  const p = obj(raw.properties);
  return {
    id: str(raw.name),
    displayName: str(p.displayName),
    path: str(p.path),
    protocols: strs(p.protocols),
    serviceUrl: str(p.serviceUrl),
    subscriptionRequired: p.subscriptionRequired === true,
  };
};

export const parseOperation = (raw: Json): ApimOperation => {
  const p = obj(raw.properties);
  return {
    id: str(raw.name),
    displayName: str(p.displayName),
    method: str(p.method).toUpperCase(),
    urlTemplate: str(p.urlTemplate),
  };
};

export const parseProduct = (raw: Json): ApimProduct => {
  const p = obj(raw.properties);
  return {
    id: str(raw.name),
    displayName: str(p.displayName),
    state: str(p.state),
    subscriptionRequired: p.subscriptionRequired === true,
    approvalRequired: p.approvalRequired === true,
  };
};

export const parseSubscription = (raw: Json): ApimSubscription => {
  const p = obj(raw.properties);
  return {
    id: str(raw.name),
    displayName: str(p.displayName),
    scope: str(p.scope),
    state: str(p.state),
    createdDate: str(p.createdDate),
  };
};

/** A secret named value never carries its value; a plain one shows it. */
export const parseNamedValue = (raw: Json): ApimNamedValue => {
  const p = obj(raw.properties);
  const secret = p.isSecret === true || p.secret === true;
  return {
    id: str(raw.name),
    displayName: str(p.displayName),
    secret,
    keyVault: Object.keys(obj(p.keyVault)).length > 0,
    value: secret ? "" : str(p.value),
    tags: strs(p.tags),
  };
};

const subPath = (s: string) => `/subscriptions/${enc(s)}`;
const svcPath = (sub: string, t: ApimTarget) =>
  `${subPath(sub)}/resourceGroups/${enc(t.resourceGroup)}/providers/Microsoft.ApiManagement/service/${enc(t.name)}`;

export async function listServices(sub: string, fetchFn: FetchFn = fetch): Promise<ApimService[]> {
  const body = await restCall(
    APIM_PROXY_BASE,
    "GET",
    `${subPath(sub)}/providers/Microsoft.ApiManagement/service${Q}`,
    undefined,
    fetchFn,
  );
  return list(body.value)
    .map((s) => parseService(s))
    .toSorted(
      (a, b) => a.resourceGroup.localeCompare(b.resourceGroup) || a.name.localeCompare(b.name),
    );
}

async function children<T>(
  sub: string,
  t: ApimTarget,
  rest: string,
  parse: (r: Json) => T,
  fetchFn: FetchFn,
): Promise<T[]> {
  const body = await restCall(
    APIM_PROXY_BASE,
    "GET",
    `${svcPath(sub, t)}/${rest}${Q}`,
    undefined,
    fetchFn,
  );
  return list(body.value).map((r) => parse(r));
}

export const listApis = (sub: string, t: ApimTarget, fetchFn: FetchFn = fetch) =>
  children(sub, t, "apis", parseApi, fetchFn);
export const listOperations = (sub: string, t: ApimTarget, api: string, fetchFn: FetchFn = fetch) =>
  children(sub, t, `apis/${enc(api)}/operations`, parseOperation, fetchFn);
export const listProducts = (sub: string, t: ApimTarget, fetchFn: FetchFn = fetch) =>
  children(sub, t, "products", parseProduct, fetchFn);
export const listSubscriptions = (sub: string, t: ApimTarget, fetchFn: FetchFn = fetch) =>
  children(sub, t, "subscriptions", parseSubscription, fetchFn);
export const listNamedValues = (sub: string, t: ApimTarget, fetchFn: FetchFn = fetch) =>
  children(sub, t, "namedValues", parseNamedValue, fetchFn);

export type PolicyScope = { api?: string; operation?: string };

/** The XML policy document at a scope; empty when none is set. */
export async function getPolicy(
  sub: string,
  t: ApimTarget,
  scope: PolicyScope = {},
  fetchFn: FetchFn = fetch,
): Promise<string> {
  let rest = "";
  if (scope.api) rest += `/apis/${enc(scope.api)}`;
  if (scope.api && scope.operation) rest += `/operations/${enc(scope.operation)}`;
  try {
    const body = await restCall(
      APIM_PROXY_BASE,
      "GET",
      `${svcPath(sub, t)}${rest}/policies/policy${Q}&format=rawxml`,
      undefined,
      fetchFn,
    );
    return str(obj(body.properties).value);
  } catch (e) {
    if (e instanceof GcpRestError && e.httpStatus === 404) return "";
    throw e;
  }
}

/** Sends a request through the service gateway; the proxy injects the chosen subscription's key server-side. */
export async function tryGateway(
  sub: string,
  t: ApimTarget,
  req: ApimTryRequest,
  fetchFn: FetchFn = fetch,
): Promise<TryResponse> {
  const body = await restCall(
    APIM_PROXY_BASE,
    "POST",
    `${svcPath(sub, t)}/invoke`,
    {
      method: req.method,
      path: req.path,
      query: cleanQuery(req.query),
      body: req.body,
      headers: req.headers.filter((h) => h.name.trim()),
      subscription: req.subscription || undefined,
      api: req.api || undefined,
    },
    fetchFn,
  );
  return readTryResponse(body);
}

/** Request path for an API operation: the API path joined to the URL template, template parameters left to edit. */
export function operationPath(
  api: Pick<ApimApi, "path">,
  op: Pick<ApimOperation, "urlTemplate">,
): string {
  const base = api.path.replaceAll(/^\/+|\/+$/g, "");
  const tpl = op.urlTemplate.startsWith("/") ? op.urlTemplate : `/${op.urlTemplate}`;
  return `/${base}${tpl}`.replaceAll(/\/{2,}/g, "/");
}

export const isActiveSubscription = (s: Pick<ApimSubscription, "state">): boolean =>
  s.state.toLowerCase() === "active";
