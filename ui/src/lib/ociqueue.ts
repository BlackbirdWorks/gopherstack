/** Client for the Oracle Cloud Queue emulator (queues, stats, message peek, purge) via the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const QUEUE_PROXY_BASE = "/dashboard/api/v1/oci/queue";
export const QUEUE_COMPARTMENT_KEY = "gopherstack_ociqueue_compartment";

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const rec = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});

export type PurgeType = "NORMAL" | "DLQ" | "BOTH";

export type QueueInfo = {
  id: string;
  name: string;
  state: string;
  created: string;
  endpoint: string;
  retention: number;
  visibility: number;
  timeout: number;
  dlqDeliveryCount: number;
};

export type Counts = { visible: number; inFlight: number; sizeBytes: number };

export type QueueStats = { queue: Counts; dlq: Counts };

export type PeekedMessage = {
  id: number;
  content: string;
  channel: string;
  deliveryCount: number;
  inFlight: boolean;
  created: string;
  visibleAfter: string;
  expireAfter: string;
  properties: Record<string, string>;
};

export function parseQueue(raw: Json): QueueInfo {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    created: str(raw.timeCreated),
    endpoint: str(raw.messagesEndpoint),
    retention: num(raw.retentionInSeconds),
    visibility: num(raw.visibilityInSeconds),
    timeout: num(raw.timeoutInSeconds),
    dlqDeliveryCount: num(raw.deadLetterQueueDeliveryCount),
  };
}

const counts = (v: unknown): Counts => {
  const c = rec(v);
  return {
    visible: num(c.visibleMessages),
    inFlight: num(c.inFlightMessages),
    sizeBytes: num(c.sizeInBytes),
  };
};

export function parseStats(raw: Json): QueueStats {
  return { queue: counts(raw.queue), dlq: counts(raw.dlq) };
}

export function parsePeeked(raw: Json): PeekedMessage {
  const props = rec(raw.customProperties);
  return {
    id: num(raw.id),
    content: str(raw.content),
    channel: str(raw.channelId),
    deliveryCount: num(raw.deliveryCount),
    inFlight: raw.inFlight === true,
    created: str(raw.createdAt),
    visibleAfter: str(raw.visibleAfter),
    expireAfter: str(raw.expireAfter),
    properties: Object.fromEntries(
      Object.entries(props).filter((e): e is [string, string] => typeof e[1] === "string"),
    ),
  };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(QUEUE_PROXY_BASE, fetchFn);

export async function listQueues(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<QueueInfo[]> {
  const items = await ociListPages(
    QUEUE_PROXY_BASE,
    "/api/queues",
    `compartmentId=${enc(compartment)}`,
    "items",
    fetchFn,
  );
  return items.map((r) => parseQueue(r));
}

export async function getQueue(id: string, fetchFn: FetchFn = fetch): Promise<QueueInfo> {
  return parseQueue(await ociGetOne(QUEUE_PROXY_BASE, `/api/queues/${enc(id)}`, fetchFn));
}

export async function getStats(id: string, fetchFn: FetchFn = fetch): Promise<QueueStats> {
  return parseStats(await ociGetOne(QUEUE_PROXY_BASE, `/api/queues/${enc(id)}/stats`, fetchFn));
}

/** Messages of a queue (or its dead letter queue) without receiving them: visibility and delivery counts stay as they are. */
export async function peekMessages(
  id: string,
  dlq: boolean,
  limit = 20,
  fetchFn: FetchFn = fetch,
): Promise<PeekedMessage[]> {
  const body = await ociGetOne(
    QUEUE_PROXY_BASE,
    `/api/queues/${enc(id)}/peek?limit=${limit}&dlq=${dlq}`,
    fetchFn,
  );
  return (Array.isArray(body.messages) ? (body.messages as Json[]) : []).map((r) => parsePeeked(r));
}

export async function purgeQueue(
  id: string,
  purgeType: PurgeType,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    QUEUE_PROXY_BASE,
    "POST",
    `/api/queues/${enc(id)}/actions/purge`,
    { purgeType },
    fetchFn,
  );
}

export const readStoredQueueCompartment = () => readStoredValue(QUEUE_COMPARTMENT_KEY);
export const writeStoredQueueCompartment = (id: string) =>
  writeStoredValue(QUEUE_COMPARTMENT_KEY, id);

export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

/** Seconds as a compact duration (90 -> "1m 30s"). */
export function formatSeconds(s: number): string {
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m${s % 60 ? ` ${s % 60}s` : ""}`;
  if (s < 86400)
    return `${Math.floor(s / 3600)}h${Math.floor((s % 3600) / 60) ? ` ${Math.floor((s % 3600) / 60)}m` : ""}`;
  return `${Math.floor(s / 86400)}d`;
}
