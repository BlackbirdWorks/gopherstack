/** Client for the Memorystore for Redis emulator via the dashboard's in-process REST proxy. */

import { listAll, restCall, shortName, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const REDIS_PROXY_BASE = "/dashboard/api/v1/gcp/redis/v1";
export const REDIS_PROJECT_KEY = "gopherstack_redis_project";
export const REDIS_LOCATION_KEY = "gopherstack_redis_location";
export const DEFAULT_REDIS_LOCATION = "us-central1";

const enc = encodeURIComponent;
const DEFAULT_PROJECT = "gopherstack-local";
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type RedisScope = { project: string; location: string };
export type RedisInstance = {
  name: string;
  id: string;
  displayName: string;
  state: string;
  tier: string;
  memorySizeGb: number;
  redisVersion: string;
  host: string;
  port: number;
  readEndpoint: string;
  readEndpointPort: number;
  authEnabled: boolean;
  transitEncryption: string;
  locationId: string;
  connectMode: string;
  authorizedNetwork: string;
  replicaCount: number;
  caCerts: number;
  createTime: string;
  labels: Record<string, string>;
  raw: Json;
};

const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export function parseInstance(raw: Json): RedisInstance {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    displayName: str(raw.displayName),
    state: str(raw.state) || "STATE_UNSPECIFIED",
    tier: str(raw.tier),
    memorySizeGb: num(raw.memorySizeGb),
    redisVersion: str(raw.redisVersion),
    host: str(raw.host),
    port: num(raw.port),
    readEndpoint: str(raw.readEndpoint),
    readEndpointPort: num(raw.readEndpointPort),
    authEnabled: raw.authEnabled === true,
    transitEncryption: str(raw.transitEncryptionMode) || "DISABLED",
    locationId: str(raw.locationId),
    connectMode: str(raw.connectMode),
    authorizedNetwork: str(raw.authorizedNetwork),
    replicaCount: num(raw.replicaCount),
    caCerts: list(raw.serverCaCerts).length,
    createTime: str(raw.createTime),
    labels: strMap(raw.labels),
    raw,
  };
}

/** host:port, or an empty string while the instance has no endpoint yet. */
export const endpointOf = (host: string, port: number): string => (host ? `${host}:${port}` : "");

export async function listInstances(
  s: RedisScope,
  fetchFn: FetchFn = fetch,
): Promise<RedisInstance[]> {
  const path = `/projects/${enc(s.project)}/locations/${enc(s.location)}/instances`;
  return (await listAll(REDIS_PROXY_BASE, path, "instances", fetchFn)).map((r) => parseInstance(r));
}

/** The instance's AUTH string; only fetched when the user asks for it. */
export async function getAuthString(name: string, fetchFn: FetchFn = fetch): Promise<string> {
  const body = await restCall(REDIS_PROXY_BASE, "GET", `/${name}/authString`, undefined, fetchFn);
  return str(body.authString);
}

export async function deleteInstance(name: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(REDIS_PROXY_BASE, "DELETE", `/${name}`, undefined, fetchFn);
}

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredRedisProject = () => readStored(REDIS_PROJECT_KEY, DEFAULT_PROJECT);
export const readStoredRedisLocation = () => readStored(REDIS_LOCATION_KEY, DEFAULT_REDIS_LOCATION);

export function writeStoredRedisScope(project: string, location: string): void {
  try {
    localStorage.setItem(REDIS_PROJECT_KEY, project);
    localStorage.setItem(REDIS_LOCATION_KEY, location);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
