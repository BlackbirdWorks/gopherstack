import { restCall, str, type FetchFn, type Json } from "./gcprest";

export const EH_PROXY_BASE = "/dashboard/api/v1/azure/eventhubs";
export const MAX_EVENT_BYTES = 1 << 20;
export const MAX_KEY_LEN = 128;

export type EhHub = {
  name: string;
  partitionIds: string[];
  consumerGroups: string[];
  retentionHours: number;
};
export type EhNamespace = { name: string; hubs: EhHub[] };
export type EhPartition = {
  id: string;
  beginSequence: number;
  lastSequence: number;
  lastEnqueuedTime: string;
  empty: boolean;
};
export type EhEvent = {
  sequence: number;
  offset: number;
  enqueued: string;
  partitionKey: string;
  contentType: string;
  body: string;
  properties: Record<string, unknown>;
};

const seg = (s: string) => encodeURIComponent(s);
const num = (v: unknown) => (typeof v === "number" ? v : 0);
const strs = (v: unknown) =>
  Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

function hubUrl(ns: string, hub: string): string {
  return `/namespaces/${seg(ns)}/hubs/${seg(hub)}`;
}

export function parseHub(raw: Json): EhHub {
  return {
    name: str(raw.name),
    partitionIds: strs(raw.partitionIds),
    consumerGroups: strs(raw.consumerGroups),
    retentionHours: num(raw.retentionHours),
  };
}

export function parsePartition(raw: Json): EhPartition {
  return {
    id: str(raw.id),
    beginSequence: num(raw.beginSequence),
    lastSequence: num(raw.lastSequence),
    lastEnqueuedTime: str(raw.lastEnqueuedTime),
    empty: raw.empty === true,
  };
}

export function parseEvent(raw: Json): EhEvent {
  return {
    sequence: num(raw.sequence),
    offset: num(raw.offset),
    enqueued: str(raw.enqueued),
    partitionKey: str(raw.partitionKey),
    contentType: str(raw.contentType),
    body: str(raw.body),
    properties: (raw.properties ?? {}) as Record<string, unknown>,
  };
}

export async function listNamespaces(fetchFn?: FetchFn): Promise<EhNamespace[]> {
  const body = await restCall(EH_PROXY_BASE, "GET", "/namespaces", undefined, fetchFn);
  return list(body.namespaces).map((n) => ({
    name: str(n.name),
    hubs: list(n.hubs).map((x) => parseHub(x)),
  }));
}

export async function listPartitions(
  ns: string,
  hub: string,
  fetchFn?: FetchFn,
): Promise<EhPartition[]> {
  const body = await restCall(
    EH_PROXY_BASE,
    "GET",
    `${hubUrl(ns, hub)}/partitions`,
    undefined,
    fetchFn,
  );
  return list(body.partitions).map((x) => parsePartition(x));
}

/** Newest-first events of one partition; nothing is consumed. */
export async function peekEvents(
  ns: string,
  hub: string,
  partition: string,
  limit = 20,
  fetchFn?: FetchFn,
): Promise<EhEvent[]> {
  const url = `${hubUrl(ns, hub)}/partitions/${seg(partition)}/events?limit=${limit}`;
  const body = await restCall(EH_PROXY_BASE, "GET", url, undefined, fetchFn);
  return list(body.events).map((x) => parseEvent(x));
}

export type SendInput = { body: string; partitionKey: string; partition: string };

/** Resolves to the partition the event landed on. */
export async function sendEvent(
  ns: string,
  hub: string,
  input: SendInput,
  fetchFn?: FetchFn,
): Promise<number> {
  const payload: Json = { body: input.body, partitionKey: input.partitionKey };
  if (input.partition !== "") payload.partition = Number(input.partition);
  const res = await restCall<{ partition?: number }>(
    EH_PROXY_BASE,
    "POST",
    `${hubUrl(ns, hub)}/events`,
    payload,
    fetchFn,
  );
  return num(res.partition);
}

export function validateSend(input: SendInput, partitionCount: number): string {
  if (!input.body) return "Event body is required";
  if (new TextEncoder().encode(input.body).length > MAX_EVENT_BYTES)
    return "Event body exceeds 1 MiB";
  if (input.partitionKey.length > MAX_KEY_LEN)
    return `Partition key is at most ${MAX_KEY_LEN} characters`;
  if (input.partition !== "") {
    const n = Number(input.partition);
    if (!Number.isInteger(n) || n < 0 || n >= partitionCount) {
      return `Partition must be 0-${partitionCount - 1}`;
    }
    if (input.partitionKey) return "Choose either a partition or a partition key, not both";
  }
  return "";
}
