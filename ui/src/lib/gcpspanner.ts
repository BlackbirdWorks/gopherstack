/** Client for the Cloud Spanner emulator's admin plane via the dashboard's in-process REST proxy. */

import { listAll, restCall, shortName, str, type FetchFn, type Json } from "./gcprest.ts";

export const SPANNER_PROXY_BASE = "/dashboard/api/v1/gcp/spanner/v1";

const enc = encodeURIComponent;

export type SpannerInstance = {
  name: string;
  id: string;
  displayName: string;
  config: string;
  nodeCount: number;
  processingUnits: number;
  state: string;
  raw: Json;
};
export type SpannerDatabase = {
  name: string;
  id: string;
  state: string;
  dialect: string;
  createTime: string;
  raw: Json;
};

export function parseInstance(raw: Json): SpannerInstance {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    displayName: str(raw.displayName),
    config: shortName(str(raw.config)),
    nodeCount: Number(raw.nodeCount ?? 0),
    processingUnits: Number(raw.processingUnits ?? 0),
    state: str(raw.state) || "STATE_UNSPECIFIED",
    raw,
  };
}

export function parseDatabase(raw: Json): SpannerDatabase {
  const name = str(raw.name);
  return {
    name,
    id: shortName(name),
    state: str(raw.state) || "STATE_UNSPECIFIED",
    dialect: str(raw.databaseDialect) || "GOOGLE_STANDARD_SQL",
    createTime: str(raw.createTime),
    raw,
  };
}

export async function listInstances(
  project: string,
  fetchFn: FetchFn = fetch,
): Promise<SpannerInstance[]> {
  const items = await listAll(
    SPANNER_PROXY_BASE,
    `/projects/${enc(project)}/instances`,
    "instances",
    fetchFn,
  );
  return items.map((r) => parseInstance(r));
}

export async function listDatabases(
  instance: string,
  fetchFn: FetchFn = fetch,
): Promise<SpannerDatabase[]> {
  return (await listAll(SPANNER_PROXY_BASE, `/${instance}/databases`, "databases", fetchFn)).map(
    (r) => parseDatabase(r),
  );
}

export async function getDdl(database: string, fetchFn: FetchFn = fetch): Promise<string[]> {
  const body = await restCall<Json>(
    SPANNER_PROXY_BASE,
    "GET",
    `/${database}/ddl`,
    undefined,
    fetchFn,
  );
  return Array.isArray(body.statements) ? (body.statements as unknown[]).map((s) => str(s)) : [];
}

export async function deleteDatabase(database: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(SPANNER_PROXY_BASE, "DELETE", `/${database}`, undefined, fetchFn);
}

export async function deleteInstance(instance: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(SPANNER_PROXY_BASE, "DELETE", `/${instance}`, undefined, fetchFn);
}
