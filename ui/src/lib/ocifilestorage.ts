/** Client for the Oracle Cloud File Storage emulator through the dashboard's in-process proxy. */

import {
  ociCompartments,
  ociGetOne,
  ociListPages,
  readStoredValue,
  writeStoredValue,
} from "./ocirest.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const FS_PROXY_BASE = "/dashboard/api/v1/oci/filestorage";
export const FS_COMPARTMENT_KEY = "gopherstack_ocifilestorage_compartment";
export const FS_AD_KEY = "gopherstack_ocifilestorage_ad";

export const ACCESS_MODES = ["READ_WRITE", "READ_ONLY"] as const;
export const SQUASH_MODES = ["ROOT", "NONE", "ALL"] as const;
export const MAX_EXPORT_OPTIONS = 32;

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const strs = (v: unknown): string[] =>
  Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];

export type FileSystem = {
  id: string;
  name: string;
  state: string;
  ad: string;
  created: string;
  meteredBytes: number;
  isCloneParent: boolean;
  parentId: string;
  sourceSnapshotId: string;
  cloneStatus: string;
  hydrated: boolean | undefined;
  policyId: string;
};

export type Snapshot = {
  id: string;
  name: string;
  state: string;
  fileSystemId: string;
  type: string;
  created: string;
  isCloneSource: boolean;
};

export type MountTarget = {
  id: string;
  name: string;
  state: string;
  ad: string;
  subnetId: string;
  exportSetId: string;
  privateIpIds: string[];
  created: string;
  throughput: number;
};

export type ExportOption = {
  source: string;
  access: string;
  squash: string;
  privileged: boolean;
};

export type ExportSummary = {
  id: string;
  exportSetId: string;
  fileSystemId: string;
  path: string;
  state: string;
  created: string;
};

export type ExportDetail = ExportSummary & { options: ExportOption[] };

export type Policy = {
  id: string;
  name: string;
  state: string;
  ad: string;
  prefix: string;
  schedules: string[];
  created: string;
};

export type Replication = {
  id: string;
  name: string;
  state: string;
  sourceId: string;
  targetId: string;
  intervalMinutes: number;
  lastSnapshotId: string;
  deltaStatus: string;
  created: string;
};

export type Subnet = { id: string; name: string; cidr: string; vcnId: string };
export type NfsInfo = { enabled: boolean; endpoints: Record<string, string> };
export type PrivateIp = { address: string; hostname: string };

export type NewFileSystem = { name: string; compartmentId: string; ad: string; snapshotId: string };
export type NewMountTarget = {
  name: string;
  compartmentId: string;
  ad: string;
  subnetId: string;
};
export type NewExport = {
  exportSetId: string;
  fileSystemId: string;
  path: string;
  options: ExportOption[];
};

export function parseFileSystem(raw: Json): FileSystem {
  const source = obj(raw.sourceDetails);
  return {
    id: str(raw.id),
    name: str(raw.displayName),
    state: str(raw.lifecycleState),
    ad: str(raw.availabilityDomain),
    created: str(raw.timeCreated),
    meteredBytes: num(raw.meteredBytes),
    isCloneParent: raw.isCloneParent === true,
    parentId: str(source.parentFileSystemId),
    sourceSnapshotId: str(source.sourceSnapshotId),
    cloneStatus: str(raw.cloneAttachStatus),
    hydrated: typeof raw.isHydrated === "boolean" ? raw.isHydrated : undefined,
    policyId: str(raw.filesystemSnapshotPolicyId),
  };
}

export function parseSnapshot(raw: Json): Snapshot {
  return {
    id: str(raw.id),
    name: str(raw.name),
    state: str(raw.lifecycleState),
    fileSystemId: str(raw.fileSystemId),
    type: str(raw.snapshotType),
    created: str(raw.timeCreated),
    isCloneSource: raw.isCloneSource === true,
  };
}

export function parseMountTarget(raw: Json): MountTarget {
  return {
    id: str(raw.id),
    name: str(raw.displayName),
    state: str(raw.lifecycleState),
    ad: str(raw.availabilityDomain),
    subnetId: str(raw.subnetId),
    exportSetId: str(raw.exportSetId),
    privateIpIds: strs(raw.privateIpIds),
    created: str(raw.timeCreated),
    throughput: num(raw.requestedThroughput),
  };
}

export function parseExport(raw: Json): ExportSummary {
  return {
    id: str(raw.id),
    exportSetId: str(raw.exportSetId),
    fileSystemId: str(raw.fileSystemId),
    path: str(raw.path),
    state: str(raw.lifecycleState),
    created: str(raw.timeCreated),
  };
}

export function parseExportDetail(raw: Json): ExportDetail {
  return {
    ...parseExport(raw),
    options: list(raw.exportOptions).map((o) => ({
      source: str(o.source),
      access: str(o.access),
      squash: str(o.identitySquash),
      privileged: o.requirePrivilegedSourcePort !== false,
    })),
  };
}

const scheduleText = (s: Json): string =>
  [str(s.period), s.hourOfDay === undefined ? "" : `hour ${num(s.hourOfDay)}`, str(s.timeZone)]
    .filter(Boolean)
    .join(" · ");

export function parsePolicy(raw: Json): Policy {
  return {
    id: str(raw.id),
    name: str(raw.displayName),
    state: str(raw.lifecycleState),
    ad: str(raw.availabilityDomain),
    prefix: str(raw.policyPrefix),
    schedules: list(raw.schedules).map((s) => scheduleText(s)),
    created: str(raw.timeCreated),
  };
}

export function parseReplication(raw: Json): Replication {
  return {
    id: str(raw.id),
    name: str(raw.displayName),
    state: str(raw.lifecycleState),
    sourceId: str(raw.sourceId),
    targetId: str(raw.targetId),
    intervalMinutes: num(raw.replicationInterval),
    lastSnapshotId: str(raw.lastSnapshotId),
    deltaStatus: str(raw.deltaStatus),
    created: str(raw.timeCreated),
  };
}

const KB = 1024;
const UNITS = ["B", "KB", "MB", "GB", "TB"];

/** Binary-prefix size with one decimal above bytes. */
export function formatBytes(bytes: number): string {
  let value = Math.max(0, bytes);
  let unit = 0;
  while (value >= KB && unit < UNITS.length - 1) {
    value /= KB;
    unit += 1;
  }
  return unit === 0 ? `${value} B` : `${value.toFixed(1)} ${UNITS[unit]}`;
}

const IPV4 = /^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$/;
const IPV6 = /^[0-9A-Fa-f:.]+$/;
const MAX_V4_BITS = 32;
const MAX_V6_BITS = 128;

function validAddress(addr: string): boolean {
  if (IPV4.test(addr)) return true;
  if (!addr.includes(":") || !IPV6.test(addr) || addr.split("::").length > 2) return false;
  const groups = addr.split(":");
  return (
    groups.length >= 3 &&
    groups.length <= 8 &&
    groups.every((g) => g.length <= 4 || g.includes("."))
  );
}

/** One IPv4 or IPv6 address or CIDR block, the only shapes an export option source may take. */
export function validateSource(source: string): string {
  const s = source.trim();
  if (s === "") return "Source is required";
  const [addr, bits, ...extra] = s.split("/");
  if (extra.length > 0 || !validAddress(addr)) return "Source must be an IP address or CIDR block";
  if (bits !== undefined) {
    const max = IPV4.test(addr) ? MAX_V4_BITS : MAX_V6_BITS;
    if (!/^\d{1,3}$/.test(bits) || Number(bits) > max) return `Prefix length must be 0 to ${max}`;
  }
  return "";
}

export function validateOption(o: ExportOption): string {
  const bad = validateSource(o.source);
  if (bad) return bad;
  if (!(ACCESS_MODES as readonly string[]).includes(o.access)) return "Pick an access mode";
  if (!(SQUASH_MODES as readonly string[]).includes(o.squash)) return "Pick an identity squash";
  return "";
}

export const emptyOption = (): ExportOption => ({
  source: "0.0.0.0/0",
  access: "READ_WRITE",
  squash: "ROOT",
  privileged: true,
});

export function validateExportPath(path: string): string {
  const p = path.trim();
  if (p === "") return "Path is required";
  if (!p.startsWith("/")) return "Path must start with a slash";
  if (p.length > 1 && p.endsWith("/")) return "Path must not end with a slash";
  if (p.length > 1024) return "Path is too long";
  if (
    p !== "/" &&
    p
      .slice(1)
      .split("/")
      .some((el) => el === "" || el === "." || el === "..")
  )
    return "Path elements must not be empty, . or ..";
  return "";
}

export function validateExport(e: NewExport): string {
  if (e.exportSetId === "") return "Pick a mount target";
  if (e.fileSystemId === "") return "Pick a file system";
  const bad = validateExportPath(e.path);
  if (bad) return bad;
  if (e.options.length > MAX_EXPORT_OPTIONS) return `At most ${MAX_EXPORT_OPTIONS} client options`;
  for (const [i, o] of e.options.entries()) {
    const why = validateOption(o);
    if (why) return `Option ${i + 1}: ${why}`;
  }
  return "";
}

export function exportBody(e: NewExport): Json {
  return {
    exportSetId: e.exportSetId,
    fileSystemId: e.fileSystemId,
    path: e.path.trim(),
    exportOptions: e.options.map((o) => ({
      source: o.source.trim(),
      access: o.access,
      identitySquash: o.squash,
      requirePrivilegedSourcePort: o.privileged,
    })),
  };
}

export function validateFileSystem(f: NewFileSystem): string {
  if (f.compartmentId === "") return "Pick a compartment";
  if (f.ad === "") return "Pick an availability domain";
  if (f.name.length > 255) return "Name is too long";
  return "";
}

export function fileSystemBody(f: NewFileSystem): Json {
  const body: Json = { compartmentId: f.compartmentId, availabilityDomain: f.ad };
  if (f.name.trim()) body.displayName = f.name.trim();
  if (f.snapshotId) body.sourceSnapshotId = f.snapshotId;
  return body;
}

export function validateMountTarget(m: NewMountTarget): string {
  if (m.compartmentId === "") return "Pick a compartment";
  if (m.ad === "") return "Pick an availability domain";
  if (m.subnetId === "") return "Pick a subnet";
  if (m.name.length > 255) return "Name is too long";
  return "";
}

export function mountTargetBody(m: NewMountTarget): Json {
  const body: Json = {
    compartmentId: m.compartmentId,
    availabilityDomain: m.ad,
    subnetId: m.subnetId,
  };
  if (m.name.trim()) body.displayName = m.name.trim();
  return body;
}

export function validateSnapshotName(name: string): string {
  const n = name.trim();
  if (n === "") return "Snapshot name is required";
  if (!/^[A-Za-z0-9._-]{1,255}$/.test(n)) return "Use letters, digits, dot, dash and underscore";
  return "";
}

const scope = (compartment: string, ad: string) =>
  `compartmentId=${enc(compartment)}&availabilityDomain=${enc(ad)}`;

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(FS_PROXY_BASE, fetchFn);

export async function listAvailabilityDomains(fetchFn: FetchFn = fetch): Promise<string[]> {
  const body = await restCall(FS_PROXY_BASE, "GET", "/availabilitydomains", undefined, fetchFn);
  return list(body.items)
    .map((a) => str(a.name))
    .filter(Boolean)
    .toSorted();
}

export async function listSubnets(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<Subnet[]> {
  const body = await restCall(
    FS_PROXY_BASE,
    "GET",
    `/subnets?compartmentId=${enc(compartment)}`,
    undefined,
    fetchFn,
  );
  return list(body.items).map((s) => ({
    id: str(s.id),
    name: str(s.displayName),
    cidr: str(s.cidrBlock),
    vcnId: str(s.vcnId),
  }));
}

export async function getNfs(fetchFn: FetchFn = fetch): Promise<NfsInfo> {
  const body = await restCall(FS_PROXY_BASE, "GET", "/nfs", undefined, fetchFn);
  const endpoints: Record<string, string> = {};
  for (const [id, addr] of Object.entries(obj(body.endpoints))) endpoints[id] = str(addr);
  return { enabled: body.enabled === true, endpoints };
}

export async function getPrivateIp(id: string, fetchFn: FetchFn = fetch): Promise<PrivateIp> {
  const body = await restCall(FS_PROXY_BASE, "GET", `/privateips/${enc(id)}`, undefined, fetchFn);
  return { address: str(body.ipAddress), hostname: str(body.hostnameLabel) };
}

export async function listFileSystems(
  compartment: string,
  ad: string,
  fetchFn: FetchFn = fetch,
): Promise<FileSystem[]> {
  const items = await ociListPages(
    FS_PROXY_BASE,
    "/api/fileSystems",
    scope(compartment, ad),
    "",
    fetchFn,
  );
  return items
    .map((f) => parseFileSystem(f))
    .filter((f) => f.state !== "DELETED")
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

export async function listMountTargets(
  compartment: string,
  ad: string,
  fetchFn: FetchFn = fetch,
): Promise<MountTarget[]> {
  const items = await ociListPages(
    FS_PROXY_BASE,
    "/api/mountTargets",
    scope(compartment, ad),
    "",
    fetchFn,
  );
  return items
    .map((m) => parseMountTarget(m))
    .filter((m) => m.state !== "DELETED")
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

export async function listExports(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<ExportSummary[]> {
  const items = await ociListPages(
    FS_PROXY_BASE,
    "/api/exports",
    `compartmentId=${enc(compartment)}`,
    "",
    fetchFn,
  );
  return items
    .map((e) => parseExport(e))
    .filter((e) => e.state !== "DELETED")
    .toSorted((a, b) => a.path.localeCompare(b.path));
}

export async function getExport(id: string, fetchFn: FetchFn = fetch): Promise<ExportDetail> {
  return parseExportDetail(await ociGetOne(FS_PROXY_BASE, `/api/exports/${enc(id)}`, fetchFn));
}

export async function listSnapshots(
  fileSystemId: string,
  fetchFn: FetchFn = fetch,
): Promise<Snapshot[]> {
  const items = await ociListPages(
    FS_PROXY_BASE,
    "/api/snapshots",
    `fileSystemId=${enc(fileSystemId)}`,
    "",
    fetchFn,
  );
  return items
    .map((s) => parseSnapshot(s))
    .filter((s) => s.state !== "DELETED")
    .toSorted((a, b) => b.created.localeCompare(a.created));
}

export async function listPolicies(
  compartment: string,
  ad: string,
  fetchFn: FetchFn = fetch,
): Promise<Policy[]> {
  const items = await ociListPages(
    FS_PROXY_BASE,
    "/api/filesystemSnapshotPolicies",
    scope(compartment, ad),
    "",
    fetchFn,
  );
  return items.map((p) => parsePolicy(p)).filter((p) => p.state !== "DELETED");
}

export async function listReplications(
  compartment: string,
  ad: string,
  fetchFn: FetchFn = fetch,
): Promise<Replication[]> {
  const items = await ociListPages(
    FS_PROXY_BASE,
    "/api/replications",
    scope(compartment, ad),
    "",
    fetchFn,
  );
  return items.map((r) => parseReplication(r)).filter((r) => r.state !== "DELETED");
}

export async function createFileSystem(f: NewFileSystem, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(FS_PROXY_BASE, "POST", "/api/fileSystems", fileSystemBody(f), fetchFn);
}

export async function deleteFileSystem(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(FS_PROXY_BASE, "DELETE", `/api/fileSystems/${enc(id)}`, undefined, fetchFn);
}

export async function createMountTarget(
  m: NewMountTarget,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(FS_PROXY_BASE, "POST", "/api/mountTargets", mountTargetBody(m), fetchFn);
}

export async function deleteMountTarget(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(FS_PROXY_BASE, "DELETE", `/api/mountTargets/${enc(id)}`, undefined, fetchFn);
}

export async function createExport(e: NewExport, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(FS_PROXY_BASE, "POST", "/api/exports", exportBody(e), fetchFn);
}

export async function deleteExport(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(FS_PROXY_BASE, "DELETE", `/api/exports/${enc(id)}`, undefined, fetchFn);
}

export async function createSnapshot(
  fileSystemId: string,
  name: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    FS_PROXY_BASE,
    "POST",
    "/api/snapshots",
    { fileSystemId, name: name.trim() },
    fetchFn,
  );
}

export async function deleteSnapshot(id: string, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(FS_PROXY_BASE, "DELETE", `/api/snapshots/${enc(id)}`, undefined, fetchFn);
}

/** A clone is a file system created from a snapshot of another one. */
export async function cloneFromSnapshot(
  snapshot: Pick<Snapshot, "id">,
  name: string,
  compartment: string,
  ad: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await createFileSystem(
    { name, compartmentId: compartment, ad, snapshotId: snapshot.id },
    fetchFn,
  );
}

export const readStoredCompartment = () => readStoredValue(FS_COMPARTMENT_KEY);
export const writeStoredCompartment = (id: string) => writeStoredValue(FS_COMPARTMENT_KEY, id);
export const readStoredAd = () => readStoredValue(FS_AD_KEY);
export const writeStoredAd = (ad: string) => writeStoredValue(FS_AD_KEY, ad);
