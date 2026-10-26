/** Client for the Oracle Cloud Streaming emulator via the dashboard's in-process proxy. */

import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const STREAM_PROXY_BASE = "/dashboard/api/v1/oci/streaming";
export const DEFAULT_READ = 20;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type PoolInfo = {
  id: string;
  name: string;
  state: string;
  compartmentId: string;
  isPrivate: boolean;
  timeCreated: string;
  raw: Json;
};

export type StreamInfo = {
  id: string;
  name: string;
  state: string;
  poolId: string;
  partitions: number;
  compartmentId: string;
  messagesEndpoint: string;
  retentionInHours: number;
  timeCreated: string;
  raw: Json;
};

export type PartitionRange = { partition: string; base: number; end: number };

export type StreamBytes = { text: string; base64: string; isText: boolean };

export type StreamMessage = {
  partition: string;
  offset: number;
  timestamp: string;
  key: StreamBytes;
  value: StreamBytes;
};

export type MessageWindow = { messages: StreamMessage[]; base: number; end: number; start: number };

export function parsePool(raw: Json): PoolInfo {
  return {
    id: str(raw.id),
    name: str(raw.name),
    state: str(raw.lifecycleState),
    compartmentId: str(raw.compartmentId),
    isPrivate: raw.isPrivate === true,
    timeCreated: str(raw.timeCreated),
    raw,
  };
}

export function parseStream(raw: Json): StreamInfo {
  return {
    id: str(raw.id),
    name: str(raw.name),
    state: str(raw.lifecycleState),
    poolId: str(raw.streamPoolId),
    partitions: num(raw.partitions),
    compartmentId: str(raw.compartmentId),
    messagesEndpoint: str(raw.messagesEndpoint),
    retentionInHours: num(raw.retentionInHours),
    timeCreated: str(raw.timeCreated),
    raw,
  };
}

function parseBytes(raw: unknown): StreamBytes {
  const b = (raw ?? {}) as Json;
  return { text: str(b.text), base64: str(b.base64), isText: b.isText === true };
}

export function parseMessage(raw: Json): StreamMessage {
  return {
    partition: str(raw.partition),
    offset: num(raw.offset),
    timestamp: str(raw.timestamp),
    key: parseBytes(raw.key),
    value: parseBytes(raw.value),
  };
}

/** Display form of message bytes: the text when it is UTF-8, otherwise the base64 payload. */
export function showBytes(b: StreamBytes): string {
  if (b.isText) return b.text;
  return b.base64 ? `base64:${b.base64}` : "";
}

export async function listPools(fetchFn: FetchFn = fetch): Promise<PoolInfo[]> {
  const body = await restCall(STREAM_PROXY_BASE, "GET", "/pools", undefined, fetchFn);
  return list(body.pools).map((p) => parsePool(p));
}

export async function listStreams(poolId = "", fetchFn: FetchFn = fetch): Promise<StreamInfo[]> {
  const q = poolId ? `?poolId=${enc(poolId)}` : "";
  const body = await restCall(STREAM_PROXY_BASE, "GET", `/streams${q}`, undefined, fetchFn);
  return list(body.streams).map((s) => parseStream(s));
}

export async function listPartitions(
  streamId: string,
  fetchFn: FetchFn = fetch,
): Promise<PartitionRange[]> {
  const body = await restCall(
    STREAM_PROXY_BASE,
    "GET",
    `/streams/${enc(streamId)}/partitions`,
    undefined,
    fetchFn,
  );
  return list(body.partitions).map((p) => ({
    partition: str(p.partition),
    base: num(p.base),
    end: num(p.end),
  }));
}

/** Reads from a partition through a server-side cursor; without `offset` it returns the newest messages. */
export async function readMessages(
  streamId: string,
  opts: { partition: string; limit?: number; offset?: number },
  fetchFn: FetchFn = fetch,
): Promise<MessageWindow> {
  const q = new URLSearchParams({
    partition: opts.partition,
    limit: String(opts.limit ?? DEFAULT_READ),
  });
  if (opts.offset !== undefined) q.set("offset", String(opts.offset));
  const body = await restCall(
    STREAM_PROXY_BASE,
    "GET",
    `/streams/${enc(streamId)}/messages?${q.toString()}`,
    undefined,
    fetchFn,
  );
  return {
    messages: list(body.messages).map((m) => parseMessage(m)),
    base: num(body.base),
    end: num(body.end),
    start: num(body.start),
  };
}
