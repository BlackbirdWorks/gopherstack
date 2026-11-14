/** Client for the Oracle Cloud Autonomous Database emulator through the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { GcpRestError, restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const DB_PROXY_BASE = "/dashboard/api/v1/oci/database";
export const DB_COMPARTMENT_KEY = "gopherstack_ocidb_compartment";

export const WORKLOADS = ["OLTP", "DW", "AJD", "APEX"] as const;

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);

export type DbSummary = {
  id: string;
  name: string;
  dbName: string;
  workload: string;
  state: string;
  details: string;
  computeModel: string;
  computeCount: number;
  storageTBs: number;
  freeTier: boolean;
  mtlsRequired: boolean;
  version: string;
  created: string;
};

export type ConnectionProfile = {
  name: string;
  consumerGroup: string;
  protocol: string;
  tlsAuthentication: string;
  sessionMode: string;
  value: string;
};

export type DbDetail = DbSummary & {
  profiles: ConnectionProfile[];
  connectionStrings: Record<string, string>;
  connectionUrls: Record<string, string>;
  acl: string[];
  aclEnabled: boolean;
  privateEndpoint: string;
  backupRetentionDays: number;
};

export type DbBackup = {
  id: string;
  name: string;
  type: string;
  state: string;
  automatic: boolean;
  started: string;
  ended: string;
  availableTill: string;
};

export function parseSummary(raw: Json): DbSummary {
  return {
    id: str(raw.id),
    name: str(raw.displayName) || str(raw.dbName),
    dbName: str(raw.dbName),
    workload: str(raw.dbWorkload),
    state: str(raw.lifecycleState),
    details: str(raw.lifecycleDetails),
    computeModel: str(raw.computeModel),
    computeCount: num(raw.computeCount),
    storageTBs: num(raw.dataStorageSizeInTBs),
    freeTier: raw.isFreeTier === true,
    mtlsRequired: raw.isMtlsConnectionRequired === true,
    version: str(raw.dbVersion),
    created: str(raw.timeCreated),
  };
}

function stringMap(v: unknown): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [k, val] of Object.entries(obj(v))) if (typeof val === "string") out[k] = val;
  return out;
}

export function parseDetail(raw: Json): DbDetail {
  const cs = obj(raw.connectionStrings);
  return {
    ...parseSummary(raw),
    profiles: list(cs.profiles).map((p) => ({
      name: str(p.displayName),
      consumerGroup: str(p.consumerGroup),
      protocol: str(p.protocol),
      tlsAuthentication: str(p.tlsAuthentication),
      sessionMode: str(p.sessionMode),
      value: str(p.value),
    })),
    connectionStrings: stringMap(cs.allConnectionStrings),
    connectionUrls: stringMap(raw.connectionUrls),
    acl: strs(raw.whitelistedIps),
    aclEnabled: raw.isAccessControlEnabled === true,
    privateEndpoint: str(raw.privateEndpoint),
    backupRetentionDays: num(raw.backupRetentionPeriodInDays),
  };
}

export function parseBackup(raw: Json): DbBackup {
  return {
    id: str(raw.id),
    name: str(raw.displayName),
    type: str(raw.type),
    state: str(raw.lifecycleState),
    automatic: raw.isAutomatic === true,
    started: str(raw.timeStarted),
    ended: str(raw.timeEnded),
    availableTill: str(raw.timeAvailableTill),
  };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(DB_PROXY_BASE, fetchFn);

export async function getEngine(fetchFn: FetchFn = fetch): Promise<string> {
  return str((await restCall(DB_PROXY_BASE, "GET", "/engine", undefined, fetchFn)).engine);
}

const compQuery = (compartment: string) => `compartmentId=${enc(compartment)}`;

export async function listDatabases(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<DbSummary[]> {
  const items = await ociListPages(
    DB_PROXY_BASE,
    "/api/autonomousDatabases",
    compQuery(compartment),
    "",
    fetchFn,
  );
  return items.map((d) => parseSummary(d)).filter((d) => d.state !== "TERMINATED");
}

export async function getDatabase(id: string, fetchFn: FetchFn = fetch): Promise<DbDetail> {
  return parseDetail(
    await ociGetOne(DB_PROXY_BASE, `/api/autonomousDatabases/${enc(id)}`, fetchFn),
  );
}

export async function listBackups(
  compartment: string,
  databaseId: string,
  fetchFn: FetchFn = fetch,
): Promise<DbBackup[]> {
  const items = await ociListPages(
    DB_PROXY_BASE,
    "/api/autonomousDatabaseBackups",
    `${compQuery(compartment)}&autonomousDatabaseId=${enc(databaseId)}`,
    "",
    fetchFn,
  );
  return items
    .map((b) => parseBackup(b))
    .filter((b) => b.state !== "DELETED")
    .toSorted((a, b) => b.started.localeCompare(a.started));
}

export type DbAction = "start" | "stop" | "restart";

/** Start, stop and restart are only offered from the states the service accepts them in. */
export function actionEnabled(action: DbAction, state: string): boolean {
  return action === "start" ? state === "STOPPED" : state === "AVAILABLE";
}

const BUSY = new Set([
  "PROVISIONING",
  "STARTING",
  "STOPPING",
  "RESTARTING",
  "UPDATING",
  "SCALE_IN_PROGRESS",
  "RESTORE_IN_PROGRESS",
  "BACKUP_IN_PROGRESS",
  "TERMINATING",
]);

export const isBusy = (state: string): boolean => BUSY.has(state);

export async function databaseAction(
  id: string,
  action: DbAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    DB_PROXY_BASE,
    "POST",
    `/api/autonomousDatabases/${enc(id)}/actions/${action}`,
    undefined,
    fetchFn,
  );
}

export async function deleteDatabase(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(
    DB_PROXY_BASE,
    "DELETE",
    `/api/autonomousDatabases/${enc(id)}`,
    undefined,
    fetchFn,
  );
}

export type DbUpdate = {
  displayName?: string;
  acl?: string[];
  aclEnabled?: boolean;
  computeCount?: number;
};

export function updateBody(u: DbUpdate): Json {
  const body: Json = {};
  if (u.displayName !== undefined) body.displayName = u.displayName.trim();
  if (u.acl !== undefined) body.whitelistedIps = u.acl;
  if (u.aclEnabled !== undefined) body.isAccessControlEnabled = u.aclEnabled;
  if (u.computeCount !== undefined) body.computeCount = u.computeCount;
  return body;
}

export async function updateDatabase(
  id: string,
  u: DbUpdate,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    DB_PROXY_BASE,
    "PUT",
    `/api/autonomousDatabases/${enc(id)}`,
    updateBody(u),
    fetchFn,
  );
}

/** Splits an access control list typed one entry per line or comma separated. */
export const parseAcl = (text: string): string[] =>
  text
    .split(/[\s,]+/)
    .map((s) => s.trim())
    .filter((s) => s !== "");

export const validAclEntry = (s: string): boolean => /^[A-Za-z0-9.:/_-]{1,255}$/.test(s);

export type NewDatabase = {
  compartmentId: string;
  displayName: string;
  dbName: string;
  workload: string;
  computeCount: number;
  storageTBs: number;
  freeTier: boolean;
  mtlsRequired: boolean;
  adminPassword: string;
};

export function validatePassword(pw: string): string {
  if (pw.length < 12 || pw.length > 30) return "Admin password must be 12 to 30 characters";
  if (!/[A-Z]/.test(pw) || !/[a-z]/.test(pw) || !/[0-9]/.test(pw)) {
    return "Admin password needs an uppercase letter, a lowercase letter and a digit";
  }
  if (pw.includes('"')) return 'Admin password cannot contain "';
  return "";
}

export function validateNew(n: NewDatabase): string {
  if (!/^[A-Za-z][A-Za-z0-9]{0,29}$/.test(n.dbName)) {
    return "Database name must start with a letter and use letters and digits only (30 max)";
  }
  if (!(WORKLOADS as readonly string[]).includes(n.workload)) return "Pick a workload";
  if (!n.freeTier) {
    if (!(n.computeCount >= 2)) return "Compute must be at least 2 ECPUs";
    if (!Number.isInteger(n.storageTBs) || n.storageTBs < 1) return "Storage must be at least 1 TB";
  }
  return validatePassword(n.adminPassword);
}

export function createBody(n: NewDatabase): Json {
  const body: Json = {
    compartmentId: n.compartmentId,
    dbName: n.dbName.trim(),
    dbWorkload: n.workload,
    adminPassword: n.adminPassword,
    isMtlsConnectionRequired: n.mtlsRequired,
    isFreeTier: n.freeTier,
  };
  if (n.displayName.trim() !== "") body.displayName = n.displayName.trim();
  if (!n.freeTier) {
    body.computeModel = "ECPU";
    body.computeCount = n.computeCount;
    body.dataStorageSizeInTBs = n.storageTBs;
  }
  return body;
}

export async function createDatabase(n: NewDatabase, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(DB_PROXY_BASE, "POST", "/api/autonomousDatabases", createBody(n), fetchFn);
}

export async function createBackup(
  databaseId: string,
  displayName: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const body: Json = { autonomousDatabaseId: databaseId };
  if (displayName.trim() !== "") body.displayName = displayName.trim();
  await restCall(DB_PROXY_BASE, "POST", "/api/autonomousDatabaseBackups", body, fetchFn);
}

export type WalletFile = { blob: Blob; filename: string };

/** Pulls the file name out of a Content-Disposition attachment header. */
export function attachmentName(header: string | null, fallback: string): string {
  const m = /filename="([^"\\/]+)"/.exec(header ?? "");
  return m?.[1] ?? fallback;
}

/** Wallet zip for a database. The password only travels in this request body. */
export async function downloadWallet(
  id: string,
  password: string,
  fetchFn: FetchFn = fetch,
): Promise<WalletFile> {
  let res: Response;
  try {
    res = await fetchFn(`${DB_PROXY_BASE}/api/autonomousDatabases/${enc(id)}/wallet`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ password }),
    });
  } catch (e) {
    throw new GcpRestError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) {
    let body: Json = {};
    try {
      body = obj(JSON.parse(await res.text()));
    } catch {
      // non-JSON error body: fall back to the status text
    }
    const proxy = obj(body.error);
    throw new GcpRestError(
      res.status,
      str(proxy.status) || str(body.code),
      str(proxy.message) || str(body.message) || res.statusText || `HTTP ${res.status}`,
    );
  }
  return {
    blob: await res.blob(),
    filename: attachmentName(res.headers.get("Content-Disposition"), "wallet.zip"),
  };
}

/** Hands a downloaded file to the browser through a temporary object URL. */
export function saveBlob(file: WalletFile): void {
  const url = URL.createObjectURL(file.blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = file.filename;
  a.click();
  URL.revokeObjectURL(url);
}

export const readStoredDbCompartment = () => readStoredValue(DB_COMPARTMENT_KEY);
export const writeStoredDbCompartment = (id: string) => writeStoredValue(DB_COMPARTMENT_KEY, id);
