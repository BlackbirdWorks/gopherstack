import { restCall, str, type FetchFn, type Json } from "./gcprest";

export const REDIS_PROXY_BASE = "/dashboard/api/v1/azure/redis";
export const DATABASES = 16;
export const MAX_PATTERN_LEN = 256;

export type RedisCache = {
  id: string;
  name: string;
  sku: string;
  minTlsVersion: string;
  host: string;
  port: number;
  sslPort: number;
  keyCount: number;
  enableNonSslPort: boolean;
  persistence: boolean;
  authNotRequired: boolean;
};
export type RedisKey = { key: string; type: string; ttlSeconds: number };
export type RedisScan = { keys: RedisKey[]; next: string };
export type RedisValue = {
  key: string;
  type: string;
  text: string;
  elements: string[];
  total: number;
  truncated: boolean;
  ttlSeconds: number;
};
export type RedisAccessKeys = { primary: string; secondary: string };

const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export async function listCaches(fetchFn?: FetchFn): Promise<RedisCache[]> {
  const body = await restCall(REDIS_PROXY_BASE, "GET", "/caches", undefined, fetchFn);
  return list(body.caches).map((c) => ({
    id: str(c.id),
    name: str(c.name),
    sku: str(c.sku),
    minTlsVersion: str(c.minTlsVersion),
    host: str(c.host),
    port: num(c.port),
    sslPort: num(c.sslPort),
    keyCount: num(c.keyCount),
    enableNonSslPort: c.enableNonSslPort === true,
    persistence: c.persistence === true,
    authNotRequired: c.authNotRequired === true,
  }));
}

export function validatePattern(p: string): string {
  return p.length > MAX_PATTERN_LEN ? `Pattern is at most ${MAX_PATTERN_LEN} characters` : "";
}

export function ttlLabel(seconds: number): string {
  return seconds < 0 ? "no expiry" : `${seconds}s`;
}

/** One SCAN page: keys matching the glob pattern after the cursor (the last key of the previous page). */
export async function scanKeys(
  cache: string,
  db: number,
  match: string,
  cursor: string,
  fetchFn?: FetchFn,
): Promise<RedisScan> {
  const q = new URLSearchParams({ cache, db: String(db) });
  if (match) q.set("match", match);
  if (cursor) q.set("cursor", cursor);
  const body = await restCall(REDIS_PROXY_BASE, "GET", `/keys?${q}`, undefined, fetchFn);
  return {
    keys: list(body.keys).map((k) => ({
      key: str(k.key),
      type: str(k.type),
      ttlSeconds: num(k.ttlSeconds),
    })),
    next: str(body.next),
  };
}

export async function getValue(
  cache: string,
  db: number,
  key: string,
  fetchFn?: FetchFn,
): Promise<RedisValue> {
  const q = new URLSearchParams({ cache, db: String(db), key });
  const b = await restCall(REDIS_PROXY_BASE, "GET", `/value?${q}`, undefined, fetchFn);
  return {
    key: str(b.key),
    type: str(b.type),
    text: str(b.text),
    elements: Array.isArray(b.elements) ? (b.elements as string[]) : [],
    total: num(b.total),
    truncated: b.truncated === true,
    ttlSeconds: num(b.ttlSeconds),
  };
}

/** Fetches the key pair. Only call this from an explicit reveal action. */
export async function revealKeys(cache: string, fetchFn?: FetchFn): Promise<RedisAccessKeys> {
  const b = await restCall(REDIS_PROXY_BASE, "POST", "/access-keys/reveal", { cache }, fetchFn);
  return { primary: str(b.primary), secondary: str(b.secondary) };
}
