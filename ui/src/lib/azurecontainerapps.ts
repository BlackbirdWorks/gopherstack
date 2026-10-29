/** Client for Azure Container Apps: Microsoft.App environments, apps, revisions and jobs via the dashboard's in-process ARM proxy. */

import { parseArmId } from "./azurearm.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const ACA_PROXY_BASE = "/dashboard/api/v1/azure/containerapps";
export const ACA_API_VERSION = "2025-07-01";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type AppAction = "start" | "stop";
export type RevisionAction = "activate" | "deactivate" | "restart";

export type ContainerEnv = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  state: string;
  defaultDomain: string;
  staticIp: string;
  zoneRedundant: boolean;
};

export type TrafficWeight = { revision: string; weight: number; latest: boolean };

export type ContainerApp = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  state: string;
  runningStatus: string;
  environment: string;
  latestRevision: string;
  fqdn: string;
  external: boolean;
  targetPort: number;
  revisionMode: string;
  traffic: TrafficWeight[];
  minReplicas: number;
  maxReplicas: number;
  images: string[];
};

export type Revision = {
  name: string;
  active: boolean;
  state: string;
  running: string;
  health: string;
  replicas: number;
  traffic: number;
  created: string;
  fqdn: string;
  error: string;
  images: string[];
};

export type ContainerJob = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  state: string;
  trigger: string;
  environment: string;
  images: string[];
};

export type JobExecution = { name: string; status: string; start: string; end: string };

const imagesOf = (tmpl: unknown): string[] =>
  list(obj(tmpl).containers)
    .map((c) => str(c.image))
    .filter(Boolean);

export function parseEnv(raw: Json): ContainerEnv {
  const p = obj(raw.properties);
  const id = str(raw.id);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    state: str(p.provisioningState),
    defaultDomain: str(p.defaultDomain),
    staticIp: str(p.staticIp),
    zoneRedundant: p.zoneRedundant === true,
  };
}

export function parseApp(raw: Json): ContainerApp {
  const p = obj(raw.properties);
  const id = str(raw.id);
  const cfg = obj(p.configuration);
  const ingress = obj(cfg.ingress);
  const scale = obj(obj(p.template).scale);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    state: str(p.provisioningState),
    runningStatus: str(p.runningStatus),
    environment: str(p.managedEnvironmentId) || str(p.environmentId),
    latestRevision: str(p.latestRevisionName),
    fqdn: str(ingress.fqdn) || str(p.latestRevisionFqdn),
    external: ingress.external === true,
    targetPort: num(ingress.targetPort),
    revisionMode: str(cfg.activeRevisionsMode),
    traffic: list(ingress.traffic).map((t) => ({
      revision: str(t.revisionName),
      weight: num(t.weight),
      latest: t.latestRevision === true,
    })),
    minReplicas: num(scale.minReplicas),
    maxReplicas: num(scale.maxReplicas),
    images: imagesOf(p.template),
  };
}

export function parseRevision(raw: Json): Revision {
  const p = obj(raw.properties);
  return {
    name: str(raw.name),
    active: p.active === true,
    state: str(p.provisioningState),
    running: str(p.runningState),
    health: str(p.healthState),
    replicas: num(p.replicas),
    traffic: num(p.trafficWeight),
    created: str(p.createdTime),
    fqdn: str(p.fqdn),
    error: str(p.provisioningError),
    images: imagesOf(p.template),
  };
}

export function parseJob(raw: Json): ContainerJob {
  const p = obj(raw.properties);
  const id = str(raw.id);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    state: str(p.provisioningState),
    trigger: str(obj(p.configuration).triggerType),
    environment: str(p.environmentId),
    images: imagesOf(p.template),
  };
}

export function parseExecution(raw: Json): JobExecution {
  const p = obj(raw.properties);
  return {
    name: str(raw.name),
    status: str(p.status),
    start: str(p.startTime),
    end: str(p.endTime),
  };
}

/** The URL an app's ingress serves from; only apps with external ingress are linkable. */
export const appUrl = (a: Pick<ContainerApp, "fqdn" | "external">): string =>
  a.fqdn && a.external ? `https://${a.fqdn}` : "";

/** The last segment of an ARM id (the environment's name for a managedEnvironmentId). */
export const lastSegment = (id: string): string => id.slice(id.lastIndexOf("/") + 1);

const withVersion = (path: string) => `${path}?api-version=${ACA_API_VERSION}`;
const subPath = (s: string) => `/subscriptions/${enc(s)}`;
const rgPath = (s: string, rg: string, type: string, name: string) =>
  `${subPath(s)}/resourceGroups/${enc(rg)}/providers/Microsoft.App/${type}/${enc(name)}`;

async function listType<T>(
  subscription: string,
  type: string,
  parse: (r: Json) => T,
  fetchFn: FetchFn,
): Promise<T[]> {
  const body = await restCall(
    ACA_PROXY_BASE,
    "GET",
    withVersion(`${subPath(subscription)}/providers/Microsoft.App/${type}`),
    undefined,
    fetchFn,
  );
  return list(body.value).map((r) => parse(r));
}

const byGroupName = <T extends { resourceGroup: string; name: string }>(a: T, b: T) =>
  a.resourceGroup.localeCompare(b.resourceGroup) || a.name.localeCompare(b.name);

export async function listEnvironments(sub: string, fetchFn: FetchFn = fetch) {
  return (await listType(sub, "managedEnvironments", (r) => parseEnv(r), fetchFn)).toSorted(
    byGroupName,
  );
}

export async function listApps(sub: string, fetchFn: FetchFn = fetch) {
  return (await listType(sub, "containerApps", (r) => parseApp(r), fetchFn)).toSorted(byGroupName);
}

export async function listJobs(sub: string, fetchFn: FetchFn = fetch) {
  return (await listType(sub, "jobs", (r) => parseJob(r), fetchFn)).toSorted(byGroupName);
}

export async function listRevisions(
  sub: string,
  app: Pick<ContainerApp, "resourceGroup" | "name">,
  fetchFn: FetchFn = fetch,
): Promise<Revision[]> {
  const body = await restCall(
    ACA_PROXY_BASE,
    "GET",
    withVersion(`${rgPath(sub, app.resourceGroup, "containerApps", app.name)}/revisions`),
    undefined,
    fetchFn,
  );
  return list(body.value)
    .map((r) => parseRevision(r))
    .toSorted((a, b) => b.created.localeCompare(a.created));
}

export async function listExecutions(
  sub: string,
  job: Pick<ContainerJob, "resourceGroup" | "name">,
  fetchFn: FetchFn = fetch,
): Promise<JobExecution[]> {
  const body = await restCall(
    ACA_PROXY_BASE,
    "GET",
    withVersion(`${rgPath(sub, job.resourceGroup, "jobs", job.name)}/executions`),
    undefined,
    fetchFn,
  );
  return list(body.value)
    .map((r) => parseExecution(r))
    .toSorted((a, b) => b.start.localeCompare(a.start));
}

export async function appAction(
  sub: string,
  app: Pick<ContainerApp, "resourceGroup" | "name">,
  action: AppAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    ACA_PROXY_BASE,
    "POST",
    withVersion(`${rgPath(sub, app.resourceGroup, "containerApps", app.name)}/${action}`),
    undefined,
    fetchFn,
  );
}

export async function revisionAction(
  sub: string,
  app: Pick<ContainerApp, "resourceGroup" | "name">,
  revision: string,
  action: RevisionAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    ACA_PROXY_BASE,
    "POST",
    withVersion(
      `${rgPath(sub, app.resourceGroup, "containerApps", app.name)}/revisions/${enc(revision)}/${action}`,
    ),
    undefined,
    fetchFn,
  );
}

export async function startJob(
  sub: string,
  job: Pick<ContainerJob, "resourceGroup" | "name">,
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const body = await restCall(
    ACA_PROXY_BASE,
    "POST",
    withVersion(`${rgPath(sub, job.resourceGroup, "jobs", job.name)}/start`),
    {},
    fetchFn,
  );
  return str(body.name);
}

/** Which revision actions a revision allows: an inactive one can only be activated. */
export function revisionActions(r: Pick<Revision, "active">): RevisionAction[] {
  return r.active ? ["restart", "deactivate"] : ["activate"];
}
