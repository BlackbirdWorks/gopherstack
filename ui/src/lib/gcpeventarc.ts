/** Client for the Eventarc emulator via the dashboard's in-process REST proxy. */

import { listAll, restCall, shortName, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const EVENTARC_PROXY_BASE = "/dashboard/api/v1/gcp/eventarc/v1";
export const EVENTARC_PROJECT_KEY = "gopherstack_eventarc_project";
export const EVENTARC_LOCATION_KEY = "gopherstack_eventarc_location";
export const DEFAULT_EVENTARC_LOCATION = "us-central1";

const enc = encodeURIComponent;
const DEFAULT_PROJECT = "gopherstack-local";
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type EaScope = { project: string; location: string };
export type Filter = { attribute: string; value: string; operator: string };
export type TriggerInfo = {
  name: string;
  id: string;
  eventFilters: Filter[];
  destinationKind: string;
  destination: string;
  serviceAccount: string;
  transportTopic: string;
  channel: string;
  eventDataContentType: string;
  createTime: string;
  raw: Json;
};
export type ChannelInfo = {
  name: string;
  id: string;
  state: string;
  provider: string;
  pubsubTopic: string;
  createTime: string;
  raw: Json;
};
export type ProviderInfo = {
  name: string;
  id: string;
  displayName: string;
  eventTypes: string[];
  raw: Json;
};

const base = (s: EaScope) => `/projects/${enc(s.project)}/locations/${enc(s.location)}`;

export function parseFilter(raw: Json): Filter {
  return {
    attribute: str(raw.attribute),
    value: str(raw.value),
    operator: str(raw.operator) || "=",
  };
}

/** Destination summary: the first populated destination kind and its target. */
export function describeDestination(raw: unknown): { kind: string; target: string } {
  const d = obj(raw);
  const run = obj(d.cloudRun);
  if (d.cloudRun) {
    const path = str(run.path);
    return { kind: "Cloud Run", target: `${str(run.service)}${path ? ` ${path}` : ""}`.trim() };
  }
  if (d.cloudFunction) return { kind: "Cloud Function", target: str(d.cloudFunction) };
  if (d.gke) return { kind: "GKE", target: str(obj(d.gke).service) };
  if (d.workflow) return { kind: "Workflow", target: str(d.workflow) };
  if (d.httpEndpoint) return { kind: "HTTP", target: str(obj(d.httpEndpoint).uri) };
  return { kind: "none", target: "" };
}

export function parseTrigger(raw: Json): TriggerInfo {
  const name = str(raw.name);
  const dest = describeDestination(raw.destination);
  return {
    name,
    id: shortName(name),
    eventFilters: list(raw.eventFilters).map((r) => parseFilter(r)),
    destinationKind: dest.kind,
    destination: dest.target,
    serviceAccount: str(raw.serviceAccount),
    transportTopic: str(obj(obj(raw.transport).pubsub).topic),
    channel: str(raw.channel),
    eventDataContentType: str(raw.eventDataContentType),
    createTime: str(raw.createTime),
    raw,
  };
}

export function parseChannel(raw: Json): ChannelInfo {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    state: str(raw.state) || "STATE_UNSPECIFIED",
    provider: str(raw.provider),
    pubsubTopic: str(raw.pubsubTopic),
    createTime: str(raw.createTime),
    raw,
  };
}

export function parseProvider(raw: Json): ProviderInfo {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    displayName: str(raw.displayName),
    eventTypes: list(raw.eventTypes)
      .map((e) => str(e.type))
      .filter(Boolean),
    raw,
  };
}

export async function listTriggers(s: EaScope, fetchFn: FetchFn = fetch): Promise<TriggerInfo[]> {
  return (await listAll(EVENTARC_PROXY_BASE, `${base(s)}/triggers`, "triggers", fetchFn)).map((r) =>
    parseTrigger(r),
  );
}

export async function listChannels(s: EaScope, fetchFn: FetchFn = fetch): Promise<ChannelInfo[]> {
  return (await listAll(EVENTARC_PROXY_BASE, `${base(s)}/channels`, "channels", fetchFn)).map((r) =>
    parseChannel(r),
  );
}

export async function listProviders(s: EaScope, fetchFn: FetchFn = fetch): Promise<ProviderInfo[]> {
  return (await listAll(EVENTARC_PROXY_BASE, `${base(s)}/providers`, "providers", fetchFn)).map(
    (r) => parseProvider(r),
  );
}

export async function deleteTrigger(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(EVENTARC_PROXY_BASE, "DELETE", `/${name}`, undefined, fetchFn);
}

export async function deleteChannel(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(EVENTARC_PROXY_BASE, "DELETE", `/${name}`, undefined, fetchFn);
}

export const triggerLabels = (t: TriggerInfo): Record<string, string> => strMap(t.raw.labels);

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredEventarcProject = () => readStored(EVENTARC_PROJECT_KEY, DEFAULT_PROJECT);
export const readStoredEventarcLocation = () =>
  readStored(EVENTARC_LOCATION_KEY, DEFAULT_EVENTARC_LOCATION);

export function writeStoredEventarcScope(project: string, location: string): void {
  try {
    localStorage.setItem(EVENTARC_PROJECT_KEY, project);
    localStorage.setItem(EVENTARC_LOCATION_KEY, location);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
