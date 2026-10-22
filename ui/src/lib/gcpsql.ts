import { restCall, str, type FetchFn, type Json } from "./gcprest";

export const SQL_PROXY_BASE = "/dashboard/api/v1/gcp/sql";
export const SQL_PROJECT_KEY = "gopherstack_sql_project";

export type SqlInstance = {
  name: string;
  state: string;
  databaseVersion: string;
  region: string;
  tier: string;
  instanceType: string;
  connectionName: string;
  createTime: string;
  ipAddresses: string[];
};

export type SqlDatabase = { name: string; charset: string; collation: string };
export type SqlUser = { name: string; host: string; type: string };

const seg = (s: string) => encodeURIComponent(s);

function items(body: Json): Json[] {
  return Array.isArray(body.items) ? (body.items as Json[]) : [];
}

export function parseInstance(raw: Json): SqlInstance {
  const settings = (raw.settings ?? {}) as Json;
  const ips = Array.isArray(raw.ipAddresses) ? (raw.ipAddresses as Json[]) : [];
  return {
    name: str(raw.name),
    state: str(raw.state),
    databaseVersion: str(raw.databaseVersion),
    region: str(raw.region),
    tier: str(settings.tier),
    instanceType: str(raw.instanceType),
    connectionName: str(raw.connectionName),
    createTime: str(raw.createTime),
    ipAddresses: ips.map((i) => str(i.ipAddress)).filter(Boolean),
  };
}

export function parseDatabase(raw: Json): SqlDatabase {
  return { name: str(raw.name), charset: str(raw.charset), collation: str(raw.collation) };
}

export function parseUser(raw: Json): SqlUser {
  return { name: str(raw.name), host: str(raw.host), type: str(raw.type) };
}

function instancesUrl(project: string): string {
  return `/projects/${seg(project)}/instances`;
}

export async function listInstances(project: string, fetchFn?: FetchFn): Promise<SqlInstance[]> {
  const body = await restCall(SQL_PROXY_BASE, "GET", instancesUrl(project), undefined, fetchFn);
  return items(body).map((x) => parseInstance(x));
}

export async function listDatabases(
  project: string,
  instance: string,
  fetchFn?: FetchFn,
): Promise<SqlDatabase[]> {
  const url = `${instancesUrl(project)}/${seg(instance)}/databases`;
  return items(await restCall(SQL_PROXY_BASE, "GET", url, undefined, fetchFn)).map((x) =>
    parseDatabase(x),
  );
}

export async function listUsers(
  project: string,
  instance: string,
  fetchFn?: FetchFn,
): Promise<SqlUser[]> {
  const url = `${instancesUrl(project)}/${seg(instance)}/users`;
  return items(await restCall(SQL_PROXY_BASE, "GET", url, undefined, fetchFn)).map((x) =>
    parseUser(x),
  );
}

/** Tailwind classes for an instance state badge. */
export function stateTone(state: string): string {
  if (state === "RUNNABLE")
    return "bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300";
  if (state === "PENDING_CREATE" || state === "MAINTENANCE") {
    return "bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300";
  }
  return "bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300";
}
