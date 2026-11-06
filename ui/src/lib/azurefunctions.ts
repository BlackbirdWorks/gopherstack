/** Client for Azure Functions: ARM (Microsoft.Web) for apps and plans, the dashboard proxy for the data plane. */

import { ARM_PROXY_BASE, getSubscriptionId } from "./azurearm.ts";
import { restCall, shortName, str, type FetchFn, type Json } from "./gcprest.ts";

export const FUNCTIONS_PROXY_BASE = "/dashboard/api/v1/azure/functions";
export const WEB_API_VERSION = "2023-12-01";

const enc = encodeURIComponent;
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type FunctionApp = {
  name: string;
  /** ARM resource id, the target of delete. */
  id: string;
  resourceGroup: string;
  location: string;
  kind: string;
  state: string;
  plan: string;
  planSku: string;
  runtimeStack: string;
  hostName: string;
  worker: string;
  /** Why the function host cannot start (for example no container runtime); empty when healthy. */
  failure: string;
  deployed: boolean;
  created: string;
  lastDeploy: string;
};

export type AppSetting = { name: string; value: string };

export type AppFunction = {
  name: string;
  language: string;
  triggerType: string;
  authLevel: string;
  invokeUrl: string;
  disabled: boolean;
};

export type Deployment = {
  id: string;
  deployer: string;
  message: string;
  succeeded: boolean;
  received: string;
  finished: string;
};

export type HostKey = { kind: string; name: string; value: string };

export type InvokeInput = {
  function: string;
  method: string;
  query: string;
  body: string;
  contentType: string;
};

export type InvokeOutput = { status: number; headers: Record<string, string>; body: string };

export type AppAction = "start" | "stop" | "restart";

const armPath = (sub: string, tail: string) =>
  `/subscriptions/${enc(sub)}/providers/Microsoft.Web/${tail}?api-version=${WEB_API_VERSION}`;

/** Plan label; the FC1 SKU is Flex Consumption. */
export function describePlan(plan: string, sku: string): string {
  if (!plan) return "";
  if (sku.toUpperCase() === "FC1") return `${plan} (Flex Consumption)`;
  return sku ? `${plan} (${sku})` : plan;
}

/** Display form of a linuxFxVersion such as "NODE|20" or "DOCKER|image:tag". */
export function describeRuntime(linuxFx: string): string {
  if (!linuxFx) return "";
  const [lang, ver] = linuxFx.split("|");
  if (!ver) return linuxFx;
  const name = lang === "DOCKER" ? "Docker" : lang.charAt(0) + lang.slice(1).toLowerCase();
  return `${name} ${ver}`;
}

export function parseApp(
  site: Json,
  plans: Map<string, string>,
  dp: Json | undefined,
): FunctionApp {
  const props = obj(site.properties);
  const farm = str(props.serverFarmId);
  const dataPlane = dp ?? {};
  return {
    name: str(site.name),
    id: str(site.id),
    resourceGroup: str(props.resourceGroup) || str(site.id).split("/")[4] || "",
    location: str(site.location),
    kind: str(site.kind),
    state: str(props.state) || str(dataPlane.state) || "Unknown",
    plan: shortName(farm),
    planSku: plans.get(farm.toLowerCase()) ?? "",
    runtimeStack: describeRuntime(str(obj(props.siteConfig).linuxFxVersion)),
    hostName: str(props.defaultHostName) || str(dataPlane.hostName),
    worker: str(dataPlane.worker),
    failure: str(dataPlane.failure),
    deployed: dataPlane.deployed === true,
    created: str(dataPlane.created),
    lastDeploy: str(dataPlane.lastDeploy),
  };
}

export async function listFunctionApps(fetchFn: FetchFn = fetch): Promise<FunctionApp[]> {
  const sub = await getSubscriptionId(fetchFn);
  const [sites, farms, dp] = await Promise.all([
    restCall(ARM_PROXY_BASE, "GET", armPath(sub, "sites"), undefined, fetchFn),
    restCall(ARM_PROXY_BASE, "GET", armPath(sub, "serverfarms"), undefined, fetchFn),
    restCall(FUNCTIONS_PROXY_BASE, "GET", "/apps", undefined, fetchFn),
  ]);
  const plans = new Map<string, string>();
  for (const f of list(farms.value)) {
    const sku = obj(f.sku);
    plans.set(str(f.id).toLowerCase(), str(sku.name) || str(sku.tier));
  }
  const dataPlane = new Map(list(dp.apps).map((a) => [str(a.name).toLowerCase(), a]));
  return list(sites.value)
    .filter((s) => str(s.kind).toLowerCase().includes("functionapp"))
    .map((s) => parseApp(s, plans, dataPlane.get(str(s.name).toLowerCase())))
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

const appPath = (name: string) => `/apps/${enc(name)}`;

/** Setting names only; every value arrives masked. */
export async function listSettings(name: string, fetchFn: FetchFn = fetch): Promise<AppSetting[]> {
  const body = await restCall(
    FUNCTIONS_PROXY_BASE,
    "GET",
    `${appPath(name)}/settings`,
    undefined,
    fetchFn,
  );
  return list(body.settings).map((s) => ({ name: str(s.name), value: str(s.value) }));
}

/** Fetches one setting's value; the server sends it only on this call. */
export async function revealSetting(
  name: string,
  setting: string,
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const body = await restCall(
    FUNCTIONS_PROXY_BASE,
    "POST",
    `${appPath(name)}/settings/reveal`,
    { name: setting },
    fetchFn,
  );
  return str(body.value);
}

export async function listAppFunctions(
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<AppFunction[]> {
  const body = await restCall(
    FUNCTIONS_PROXY_BASE,
    "GET",
    `${appPath(name)}/functions`,
    undefined,
    fetchFn,
  );
  return list(body.functions).map((f) => ({
    name: str(f.name),
    language: str(f.language),
    triggerType: str(f.triggerType),
    authLevel: str(f.authLevel),
    invokeUrl: str(f.invokeUrl),
    disabled: f.disabled === true,
  }));
}

export async function listDeployments(
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<Deployment[]> {
  const body = await restCall(
    FUNCTIONS_PROXY_BASE,
    "GET",
    `${appPath(name)}/deployments`,
    undefined,
    fetchFn,
  );
  return list(body.deployments).map((d) => ({
    id: str(d.id),
    deployer: str(d.deployer),
    message: str(d.message),
    succeeded: d.succeeded === true,
    received: str(d.received),
    finished: str(d.finished),
  }));
}

export async function appAction(
  name: string,
  action: AppAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(FUNCTIONS_PROXY_BASE, "POST", `${appPath(name)}/${action}`, undefined, fetchFn);
}

/** Deletes the site through ARM so the data-plane app goes with it. */
export async function deleteFunctionApp(app: FunctionApp, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(
    ARM_PROXY_BASE,
    "DELETE",
    `${app.id}?api-version=${WEB_API_VERSION}`,
    undefined,
    fetchFn,
  );
}

/** Host key names only; every value arrives masked. */
export async function listHostKeys(name: string, fetchFn: FetchFn = fetch): Promise<HostKey[]> {
  const body = await restCall(
    FUNCTIONS_PROXY_BASE,
    "GET",
    `${appPath(name)}/keys`,
    undefined,
    fetchFn,
  );
  return list(body.keys).map((k) => ({
    kind: str(k.kind),
    name: str(k.name),
    value: str(k.value),
  }));
}

/** Fetches one key's value; the server sends it only on this call. */
export async function revealHostKey(
  name: string,
  key: Pick<HostKey, "kind" | "name">,
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const body = await restCall(
    FUNCTIONS_PROXY_BASE,
    "POST",
    `${appPath(name)}/keys/reveal`,
    { kind: key.kind, name: key.name },
    fetchFn,
  );
  return str(body.value);
}

/** Calls an HTTP function through the app's own emulator host. */
export async function invokeAppFunction(
  name: string,
  input: InvokeInput,
  fetchFn: FetchFn = fetch,
): Promise<InvokeOutput> {
  const body = await restCall(
    FUNCTIONS_PROXY_BASE,
    "POST",
    `${appPath(name)}/invoke`,
    input,
    fetchFn,
  );
  const hdrs = obj(body.headers);
  return {
    status: typeof body.status === "number" ? body.status : 0,
    headers: Object.fromEntries(Object.entries(hdrs).map(([k, v]) => [k, str(v)])),
    body: str(body.body),
  };
}
