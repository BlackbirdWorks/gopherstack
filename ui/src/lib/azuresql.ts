import { parseArmId } from "./azurearm.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const SQL_PROXY_BASE = "/dashboard/api/v1/azure/sql";
export const SQL_QUERY_BASE = "/dashboard/api/v1/azure/sqlquery";
export const SQL_API_VERSION = "2023-08-01-preview";
export const MAX_QUERY_LEN = 4000;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type SqlServer = {
  id: string;
  name: string;
  resourceGroup: string;
  location: string;
  state: string;
  version: string;
  fqdn: string;
  endpoint: string;
  login: string;
  minTls: string;
  publicAccess: string;
};
export type SqlDatabase = {
  name: string;
  sku: string;
  tier: string;
  status: string;
  maxSizeBytes: number;
  collation: string;
};
export type SqlFirewallRule = { name: string; start: string; end: string };
export type SqlResult = {
  columns: string[];
  rows: string[][];
  truncated: boolean;
  maxRows: number;
  elapsedMs: number;
};

export function parseServer(raw: Json): SqlServer {
  const p = obj(raw.properties);
  const id = str(raw.id);
  return {
    id,
    name: str(raw.name),
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    state: str(p.state),
    version: str(p.version),
    fqdn: str(p.fullyQualifiedDomainName),
    endpoint: str(p.gopherstackEndpoint),
    login: str(p.administratorLogin),
    minTls: str(p.minimalTlsVersion),
    publicAccess: str(p.publicNetworkAccess),
  };
}

export function parseDatabase(raw: Json): SqlDatabase {
  const p = obj(raw.properties);
  const sku = obj(raw.sku);
  return {
    name: str(raw.name),
    sku: str(sku.name),
    tier: str(sku.tier),
    status: str(p.status),
    maxSizeBytes: num(p.maxSizeBytes),
    collation: str(p.collation),
  };
}

export const parseFirewallRule = (raw: Json): SqlFirewallRule => ({
  name: str(raw.name),
  start: str(obj(raw.properties).startIpAddress),
  end: str(obj(raw.properties).endIpAddress),
});

export function formatMaxSize(bytes: number): string {
  if (bytes <= 0) return "-";
  const gb = bytes / 2 ** 30;
  return gb >= 1 ? `${+gb.toFixed(1)} GB` : `${Math.round(bytes / 2 ** 20)} MB`;
}

const version = `?api-version=${SQL_API_VERSION}`;
const serverPath = (s: SqlServer, sub: string) =>
  `/subscriptions/${enc(sub)}/resourceGroups/${enc(s.resourceGroup)}/providers/Microsoft.Sql/servers/${enc(s.name)}`;

export async function listServers(sub: string, fetchFn?: FetchFn): Promise<SqlServer[]> {
  const body = await restCall(
    SQL_PROXY_BASE,
    "GET",
    `/subscriptions/${enc(sub)}/providers/Microsoft.Sql/servers${version}`,
    undefined,
    fetchFn,
  );
  return list(body.value)
    .map((s) => parseServer(s))
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

async function children<T>(
  s: SqlServer,
  sub: string,
  kind: string,
  parse: (r: Json) => T,
  fetchFn?: FetchFn,
): Promise<T[]> {
  const body = await restCall(
    SQL_PROXY_BASE,
    "GET",
    `${serverPath(s, sub)}/${kind}${version}`,
    undefined,
    fetchFn,
  );
  return list(body.value).map((r) => parse(r));
}

export const listDatabases = (s: SqlServer, sub: string, fetchFn?: FetchFn) =>
  children(s, sub, "databases", parseDatabase, fetchFn);
export const listFirewallRules = (s: SqlServer, sub: string, fetchFn?: FetchFn) =>
  children(s, sub, "firewallRules", parseFirewallRule, fetchFn);

const IPV4 = /^(25[0-5]|2[0-4]\d|1?\d?\d)(\.(25[0-5]|2[0-4]\d|1?\d?\d)){3}$/;

export function validateRule(name: string, start: string, end: string): string {
  if (!/^[A-Za-z0-9_.-]{1,128}$/.test(name)) return "Name: 1-128 letters, digits, . _ -";
  if (!IPV4.test(start.trim()) || !IPV4.test(end.trim()))
    return "Start and end must be IPv4 addresses";
  return "";
}

export async function putFirewallRule(
  s: SqlServer,
  sub: string,
  name: string,
  start: string,
  end: string,
  fetchFn?: FetchFn,
): Promise<void> {
  await restCall(
    SQL_PROXY_BASE,
    "PUT",
    `${serverPath(s, sub)}/firewallRules/${enc(name)}${version}`,
    { properties: { startIpAddress: start.trim(), endIpAddress: end.trim() } },
    fetchFn,
  );
}

export async function deleteFirewallRule(
  s: SqlServer,
  sub: string,
  name: string,
  fetchFn?: FetchFn,
): Promise<void> {
  await restCall(
    SQL_PROXY_BASE,
    "DELETE",
    `${serverPath(s, sub)}/firewallRules/${enc(name)}${version}`,
    undefined,
    fetchFn,
  );
}

/** Client-side check only; the server validates again and is authoritative. */
export function validateQuery(q: string): string {
  if (!q.trim()) return "Enter a SELECT statement";
  if (q.length > MAX_QUERY_LEN) return `Query is at most ${MAX_QUERY_LEN} characters`;
  if (!/^\s*select\b/i.test(q)) return "Only SELECT statements are allowed";
  return "";
}

export async function runQuery(
  server: SqlServer,
  database: string,
  query: string,
  fetchFn?: FetchFn,
): Promise<SqlResult> {
  const b = await restCall(
    SQL_QUERY_BASE,
    "POST",
    "",
    { server: server.id, database, query },
    fetchFn,
  );
  return {
    columns: Array.isArray(b.columns) ? (b.columns as string[]) : [],
    rows: Array.isArray(b.rows) ? (b.rows as string[][]) : [],
    truncated: b.truncated === true,
    maxRows: num(b.maxRows),
    elapsedMs: num(b.elapsedMs),
  };
}
