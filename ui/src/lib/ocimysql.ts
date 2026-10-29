/** Client for the Oracle Cloud MySQL HeatWave emulator: DB systems, backups and configurations via the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const MYSQL_PROXY_BASE = "/dashboard/api/v1/oci/mysql";
export const MYSQL_COMPARTMENT_KEY = "gopherstack_ocimysql_compartment";

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type MysqlSystem = {
  id: string;
  name: string;
  state: string;
  details: string;
  version: string;
  shape: string;
  ip: string;
  port: number;
  portX: number;
  storageGb: number;
  maxStorageGb: number;
  autoExpand: boolean;
  highlyAvailable: boolean;
  backupEnabled: boolean;
  backupRetentionDays: number;
  backupWindow: string;
  pitr: boolean;
  deleteProtected: boolean;
  finalBackup: string;
  availabilityDomain: string;
  faultDomain: string;
  databaseMode: string;
  accessMode: string;
  configurationId: string;
  subnetId: string;
  created: string;
};

export type MysqlBackup = {
  id: string;
  name: string;
  state: string;
  dbSystemId: string;
  type: string;
  creation: string;
  sizeGb: number;
  retentionDays: number;
  version: string;
  created: string;
};

export type MysqlConfiguration = {
  id: string;
  name: string;
  type: string;
  shape: string;
  state: string;
  description: string;
  created: string;
};

/** Reads a DB system summary or full body; the admin password never appears in either. */
export function parseSystem(raw: Json): MysqlSystem {
  const ep = obj(list(raw.endpoints)[0]);
  const storage = obj(raw.dataStorage);
  const backup = obj(raw.backupPolicy);
  const del = obj(raw.deletionPolicy);
  const place = obj(raw.currentPlacement);
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    details: str(raw.lifecycleDetails),
    version: str(raw.mysqlVersion),
    shape: str(raw.shapeName),
    ip: str(ep.ipAddress) || str(raw.ipAddress),
    port: num(ep.port) || num(raw.port),
    portX: num(ep.portX) || num(raw.portX),
    storageGb: num(storage.dataStorageSizeInGBs) || num(raw.dataStorageSizeInGBs),
    maxStorageGb: num(storage.maxStorageSizeInGBs),
    autoExpand: storage.isAutoExpandStorageEnabled === true,
    highlyAvailable: raw.isHighlyAvailable === true,
    backupEnabled: backup.isEnabled === true,
    backupRetentionDays: num(backup.retentionInDays),
    backupWindow: str(backup.windowStartTime),
    pitr: obj(backup.pitrPolicy).isEnabled === true,
    deleteProtected: del.isDeleteProtected === true,
    finalBackup: str(del.finalBackup),
    availabilityDomain: str(place.availabilityDomain) || str(raw.availabilityDomain),
    faultDomain: str(place.faultDomain) || str(raw.faultDomain),
    databaseMode: str(raw.databaseMode),
    accessMode: str(raw.accessMode),
    configurationId: str(raw.configurationId),
    subnetId: str(raw.subnetId),
    created: str(raw.timeCreated),
  };
}

export function parseBackup(raw: Json): MysqlBackup {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    state: str(raw.lifecycleState),
    dbSystemId: str(raw.dbSystemId),
    type: str(raw.backupType),
    creation: str(raw.creationType),
    sizeGb: num(raw.backupSizeInGBs),
    retentionDays: num(raw.retentionInDays),
    version: str(raw.mysqlVersion),
    created: str(raw.timeCreated),
  };
}

export function parseConfiguration(raw: Json): MysqlConfiguration {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.id),
    type: str(raw.type),
    shape: str(raw.shapeName),
    state: str(raw.lifecycleState),
    description: str(raw.description),
    created: str(raw.timeCreated),
  };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(MYSQL_PROXY_BASE, fetchFn);

export async function listSystems(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<MysqlSystem[]> {
  const items = await ociListPages(
    MYSQL_PROXY_BASE,
    "/api/dbSystems",
    `compartmentId=${enc(compartment)}`,
    "",
    fetchFn,
  );
  return items.map((r) => parseSystem(r));
}

export async function getSystem(id: string, fetchFn: FetchFn = fetch): Promise<MysqlSystem> {
  return parseSystem(await ociGetOne(MYSQL_PROXY_BASE, `/api/dbSystems/${enc(id)}`, fetchFn));
}

export async function listBackups(
  compartment: string,
  dbSystemId = "",
  fetchFn: FetchFn = fetch,
): Promise<MysqlBackup[]> {
  const items = await ociListPages(
    MYSQL_PROXY_BASE,
    "/api/backups",
    `compartmentId=${enc(compartment)}${dbSystemId ? `&dbSystemId=${enc(dbSystemId)}` : ""}`,
    "",
    fetchFn,
  );
  return items.map((r) => parseBackup(r));
}

export async function listConfigurations(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<MysqlConfiguration[]> {
  const items = await ociListPages(
    MYSQL_PROXY_BASE,
    "/api/configurations",
    `compartmentId=${enc(compartment)}`,
    "",
    fetchFn,
  );
  return items.map((r) => parseConfiguration(r));
}

export type SystemAction = "start" | "stop" | "restart";

export async function systemAction(
  id: string,
  action: SystemAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    MYSQL_PROXY_BASE,
    "POST",
    `/api/dbSystems/${enc(id)}/actions/${action}`,
    {},
    fetchFn,
  );
}

export async function deleteSystem(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(MYSQL_PROXY_BASE, "DELETE", `/api/dbSystems/${enc(id)}`, undefined, fetchFn);
}

export async function createBackup(
  dbSystemId: string,
  displayName: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const body: Json = { dbSystemId };
  if (displayName.trim()) body.displayName = displayName.trim();
  await restCall(MYSQL_PROXY_BASE, "POST", "/api/backups", body, fetchFn);
}

/** Which lifecycle actions a state allows. */
export function allowedActions(s: Pick<MysqlSystem, "state" | "deleteProtected">): {
  start: boolean;
  stop: boolean;
  restart: boolean;
  remove: boolean;
} {
  return {
    start: s.state === "INACTIVE",
    stop: s.state === "ACTIVE",
    restart: s.state === "ACTIVE",
    remove: !s.deleteProtected && s.state !== "DELETING" && s.state !== "DELETED",
  };
}

export const mysqlEndpoint = (s: Pick<MysqlSystem, "ip" | "port">): string =>
  s.ip ? `${s.ip}:${s.port}` : "";

export const readStoredMysqlCompartment = () => readStoredValue(MYSQL_COMPARTMENT_KEY);
export const writeStoredMysqlCompartment = (id: string) =>
  writeStoredValue(MYSQL_COMPARTMENT_KEY, id);
