/** Client for Azure Database for PostgreSQL / MySQL flexible servers via the dashboard's in-process ARM proxy. */

import { parseArmId } from "./azurearm.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const FLEX_PROXY_BASE = "/dashboard/api/v1/azure/flexdb";

export type FlexEngine = "postgresql" | "mysql";

export const FLEX_ENGINES: Record<
  FlexEngine,
  { namespace: string; label: string; apiVersion: string }
> = {
  postgresql: {
    namespace: "Microsoft.DBforPostgreSQL",
    label: "PostgreSQL",
    apiVersion: "2025-08-01",
  },
  mysql: { namespace: "Microsoft.DBforMySQL", label: "MySQL", apiVersion: "2024-12-30" },
};

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
/** ARM serialises some flags as booleans and some as "True"/"False" strings. */
const flag = (v: unknown): boolean =>
  v === true || (typeof v === "string" && v.toLowerCase() === "true");

export type PowerAction = "start" | "stop" | "restart";

export type FlexServer = {
  id: string;
  name: string;
  engine: FlexEngine;
  resourceGroup: string;
  location: string;
  state: string;
  version: string;
  sku: string;
  tier: string;
  storageGb: number;
  iops: number;
  haMode: string;
  haState: string;
  fqdn: string;
  endpoint: string;
  login: string;
};

export type FlexDatabase = { name: string; charset: string; collation: string };
export type FlexFirewallRule = { name: string; start: string; end: string };
export type FlexConfig = {
  name: string;
  value: string;
  defaultValue: string;
  source: string;
  pendingRestart: boolean;
  readOnly: boolean;
  dataType: string;
  description: string;
};

export type FlexTarget = Pick<FlexServer, "engine" | "resourceGroup" | "name">;

export function parseServer(raw: Json, engine: FlexEngine): FlexServer {
  const p = obj(raw.properties);
  const id = str(raw.id);
  const storage = obj(p.storage);
  const ha = obj(p.highAvailability);
  return {
    id,
    name: str(raw.name),
    engine,
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    state: str(p.state),
    version: str(p.version),
    sku: str(obj(raw.sku).name),
    tier: str(obj(raw.sku).tier),
    storageGb: num(storage.storageSizeGB),
    iops: num(storage.iops),
    haMode: str(ha.mode),
    haState: str(ha.state),
    fqdn: str(p.fullyQualifiedDomainName),
    endpoint: str(p.gopherstackEndpoint),
    login: str(p.administratorLogin),
  };
}

export const parseDatabase = (raw: Json): FlexDatabase => ({
  name: str(raw.name),
  charset: str(obj(raw.properties).charset),
  collation: str(obj(raw.properties).collation),
});

export const parseFirewallRule = (raw: Json): FlexFirewallRule => ({
  name: str(raw.name),
  start: str(obj(raw.properties).startIpAddress),
  end: str(obj(raw.properties).endIpAddress),
});

export const parseConfig = (raw: Json): FlexConfig => {
  const p = obj(raw.properties);
  return {
    name: str(raw.name),
    value: str(p.value),
    defaultValue: str(p.defaultValue),
    source: str(p.source),
    pendingRestart: flag(p.isConfigPendingRestart),
    readOnly: flag(p.isReadOnly),
    dataType: str(p.dataType),
    description: str(p.description),
  };
};

/** A parameter worth showing by default: changed from its default, or waiting on a restart. */
export const isNonDefault = (c: FlexConfig): boolean =>
  c.pendingRestart ||
  c.source === "user-override" ||
  (c.value !== c.defaultValue && c.source !== "system-default");

const version = (engine: FlexEngine) => `?api-version=${FLEX_ENGINES[engine].apiVersion}`;
const subPath = (s: string) => `/subscriptions/${enc(s)}`;
const typePath = (engine: FlexEngine) =>
  `providers/${FLEX_ENGINES[engine].namespace}/flexibleServers`;
const serverPath = (sub: string, t: FlexTarget) =>
  `${subPath(sub)}/resourceGroups/${enc(t.resourceGroup)}/${typePath(t.engine)}/${enc(t.name)}`;

/** Lists the servers of both engines; an engine that fails is skipped unless every engine fails. */
export async function listServers(sub: string, fetchFn: FetchFn = fetch): Promise<FlexServer[]> {
  const engines = Object.keys(FLEX_ENGINES) as FlexEngine[];
  const results = await Promise.allSettled(
    engines.map((engine) =>
      restCall(
        FLEX_PROXY_BASE,
        "GET",
        `${subPath(sub)}/${typePath(engine)}${version(engine)}`,
        undefined,
        fetchFn,
      ),
    ),
  );
  if (results.every((r) => r.status === "rejected"))
    throw (results[0] as PromiseRejectedResult).reason;
  const out: FlexServer[] = [];
  results.forEach((r, i) => {
    if (r.status === "fulfilled")
      out.push(...list(r.value.value).map((s) => parseServer(s, engines[i])));
  });
  return out.toSorted(
    (a, b) => a.resourceGroup.localeCompare(b.resourceGroup) || a.name.localeCompare(b.name),
  );
}

const children = async <T>(
  sub: string,
  t: FlexTarget,
  kind: string,
  parse: (r: Json) => T,
  fetchFn: FetchFn,
): Promise<T[]> => {
  const body = await restCall(
    FLEX_PROXY_BASE,
    "GET",
    `${serverPath(sub, t)}/${kind}${version(t.engine)}`,
    undefined,
    fetchFn,
  );
  return list(body.value).map((r) => parse(r));
};

export const listDatabases = (sub: string, t: FlexTarget, fetchFn: FetchFn = fetch) =>
  children(sub, t, "databases", parseDatabase, fetchFn);
export const listFirewallRules = (sub: string, t: FlexTarget, fetchFn: FetchFn = fetch) =>
  children(sub, t, "firewallRules", parseFirewallRule, fetchFn);
export const listConfigurations = (sub: string, t: FlexTarget, fetchFn: FetchFn = fetch) =>
  children(sub, t, "configurations", parseConfig, fetchFn);

export async function powerAction(
  sub: string,
  t: FlexTarget,
  action: PowerAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    FLEX_PROXY_BASE,
    "POST",
    `${serverPath(sub, t)}/${action}${version(t.engine)}`,
    undefined,
    fetchFn,
  );
}

export async function addDatabase(
  sub: string,
  t: FlexTarget,
  name: string,
  charset: string,
  collation: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const properties: Record<string, string> = {};
  if (charset.trim()) properties.charset = charset.trim();
  if (collation.trim()) properties.collation = collation.trim();
  await restCall(
    FLEX_PROXY_BASE,
    "PUT",
    `${serverPath(sub, t)}/databases/${enc(name)}${version(t.engine)}`,
    { properties },
    fetchFn,
  );
}

export async function deleteDatabase(
  sub: string,
  t: FlexTarget,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    FLEX_PROXY_BASE,
    "DELETE",
    `${serverPath(sub, t)}/databases/${enc(name)}${version(t.engine)}`,
    undefined,
    fetchFn,
  );
}

export async function addFirewallRule(
  sub: string,
  t: FlexTarget,
  name: string,
  start: string,
  end: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    FLEX_PROXY_BASE,
    "PUT",
    `${serverPath(sub, t)}/firewallRules/${enc(name)}${version(t.engine)}`,
    { properties: { startIpAddress: start.trim(), endIpAddress: end.trim() } },
    fetchFn,
  );
}

export async function deleteFirewallRule(
  sub: string,
  t: FlexTarget,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    FLEX_PROXY_BASE,
    "DELETE",
    `${serverPath(sub, t)}/firewallRules/${enc(name)}${version(t.engine)}`,
    undefined,
    fetchFn,
  );
}

export const validIpv4 = (v: string): boolean => {
  const parts = v.trim().split(".");
  return parts.length === 4 && parts.every((p) => /^\d{1,3}$/.test(p) && Number(p) <= 255);
};
export const validChildName = (v: string): boolean => /^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$/.test(v);
export const validCharset = (v: string): boolean =>
  v.trim() === "" || /^[A-Za-z0-9_.-]{1,64}$/.test(v.trim());

/** Whether a power action can run in the server's state. */
export function canAct(state: string, action: PowerAction): boolean {
  if (action === "start") return state === "Stopped";
  return state === "Ready";
}

export const engineLabel = (e: FlexEngine): string => FLEX_ENGINES[e].label;
