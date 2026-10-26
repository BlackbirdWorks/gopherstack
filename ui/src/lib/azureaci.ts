/** Client for Azure Container Instances: ARM container groups via the dashboard's in-process proxy. */

import { parseArmId } from "./azurearm.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const ACI_PROXY_BASE = "/dashboard/api/v1/azure/aci";
export const ACI_API_VERSION = "2023-05-01";
export const ACI_DEFAULT_TAIL = 100;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type AciAction = "start" | "stop" | "restart";

export type AciEvent = {
  name: string;
  type: string;
  message: string;
  count: number;
  first: string;
  last: string;
};

export type AciState = {
  state: string;
  detail: string;
  exitCode: number | null;
  started: string;
  finished: string;
};

export type AciEnvVar = { name: string; value: string; secure: boolean };

export type AciContainer = {
  name: string;
  image: string;
  command: string[];
  ports: number[];
  cpu: number;
  memoryGb: number;
  env: AciEnvVar[];
  restartCount: number;
  current: AciState | null;
  previous: AciState | null;
  events: AciEvent[];
};

export type AciGroup = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  osType: string;
  provisioningState: string;
  instanceState: string;
  restartPolicy: string;
  ipType: string;
  ip: string;
  fqdn: string;
  ports: string[];
  registries: string[];
  tags: Record<string, string>;
  containers: AciContainer[];
  events: AciEvent[];
};

export function parseState(raw: unknown): AciState | null {
  const s = obj(raw);
  if (!str(s.state)) return null;
  return {
    state: str(s.state),
    detail: str(s.detailStatus),
    exitCode: typeof s.exitCode === "number" ? s.exitCode : null,
    started: str(s.startTime),
    finished: str(s.finishTime),
  };
}

export function parseEvent(raw: Json): AciEvent {
  return {
    name: str(raw.name),
    type: str(raw.type),
    message: str(raw.message),
    count: num(raw.count) || 1,
    first: str(raw.firstTimestamp),
    last: str(raw.lastTimestamp),
  };
}

export function parseContainer(raw: Json): AciContainer {
  const p = obj(raw.properties);
  const view = obj(p.instanceView);
  const requests = obj(obj(p.resources).requests);
  return {
    name: str(raw.name),
    image: str(p.image),
    command: Array.isArray(p.command) ? p.command.map(String) : [],
    ports: list(p.ports).map((x) => num(x.port)),
    cpu: num(requests.cpu),
    memoryGb: num(requests.memoryInGB),
    env: list(p.environmentVariables).map((e) => ({
      name: str(e.name),
      value: str(e.value),
      secure: e.value === undefined,
    })),
    restartCount: num(view.restartCount),
    current: parseState(view.currentState),
    previous: parseState(view.previousState),
    events: list(view.events).map((x) => parseEvent(x)),
  };
}

export function parseGroup(raw: Json): AciGroup {
  const p = obj(raw.properties);
  const ip = obj(p.ipAddress);
  const id = str(raw.id);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    osType: str(p.osType),
    provisioningState: str(p.provisioningState),
    instanceState: str(obj(p.instanceView).state),
    restartPolicy: str(p.restartPolicy),
    ipType: str(ip.type),
    ip: str(ip.ip),
    fqdn: str(ip.fqdn),
    ports: list(ip.ports).map((x) => `${num(x.port)}/${str(x.protocol) || "TCP"}`),
    registries: list(p.imageRegistryCredentials).map(
      (c) => `${str(c.server)} (${str(c.username)})`,
    ),
    tags: Object.fromEntries(
      Object.entries(obj(raw.tags)).filter((e): e is [string, string] => typeof e[1] === "string"),
    ),
    containers: list(p.containers).map((x) => parseContainer(x)),
    events: list(obj(p.instanceView).events).map((x) => parseEvent(x)),
  };
}

const groupsPath = (sub: string) =>
  `/subscriptions/${enc(sub)}/providers/Microsoft.ContainerInstance/containerGroups`;

const groupPath = (sub: string, g: Pick<AciGroup, "resourceGroup" | "name">) =>
  `/subscriptions/${enc(sub)}/resourceGroups/${enc(g.resourceGroup)}/providers/Microsoft.ContainerInstance/containerGroups/${enc(g.name)}`;

const withVersion = (path: string, extra = "") => `${path}?api-version=${ACI_API_VERSION}${extra}`;

export async function listGroups(sub: string, fetchFn: FetchFn = fetch): Promise<AciGroup[]> {
  const body = await restCall(
    ACI_PROXY_BASE,
    "GET",
    withVersion(groupsPath(sub)),
    undefined,
    fetchFn,
  );
  return list(body.value)
    .map((x) => parseGroup(x))
    .toSorted(
      (a, b) => a.resourceGroup.localeCompare(b.resourceGroup) || a.name.localeCompare(b.name),
    );
}

export async function getGroup(
  sub: string,
  g: Pick<AciGroup, "resourceGroup" | "name">,
  fetchFn: FetchFn = fetch,
): Promise<AciGroup> {
  return parseGroup(
    await restCall(ACI_PROXY_BASE, "GET", withVersion(groupPath(sub, g)), undefined, fetchFn),
  );
}

export async function groupAction(
  sub: string,
  g: Pick<AciGroup, "resourceGroup" | "name">,
  action: AciAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    ACI_PROXY_BASE,
    "POST",
    withVersion(`${groupPath(sub, g)}/${action}`),
    undefined,
    fetchFn,
  );
}

export async function deleteGroup(
  sub: string,
  g: Pick<AciGroup, "resourceGroup" | "name">,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(ACI_PROXY_BASE, "DELETE", withVersion(groupPath(sub, g)), undefined, fetchFn);
}

/** Last `tail` lines of a container's output. */
export async function containerLogs(
  sub: string,
  g: Pick<AciGroup, "resourceGroup" | "name">,
  container: string,
  tail: number = ACI_DEFAULT_TAIL,
  fetchFn: FetchFn = fetch,
): Promise<string> {
  const body = await restCall(
    ACI_PROXY_BASE,
    "GET",
    withVersion(
      `${groupPath(sub, g)}/containers/${enc(container)}/logs`,
      `&tail=${Math.max(1, Math.trunc(tail))}`,
    ),
    undefined,
    fetchFn,
  );
  return str(body.content);
}

/** Groups bucketed by resource group, in the order the resource groups first appear. */
export function byResourceGroup(
  groups: AciGroup[],
): { resourceGroup: string; groups: AciGroup[] }[] {
  const out = new Map<string, AciGroup[]>();
  for (const g of groups) out.set(g.resourceGroup, [...(out.get(g.resourceGroup) ?? []), g]);
  return [...out].map(([resourceGroup, gs]) => ({ resourceGroup, groups: gs }));
}
