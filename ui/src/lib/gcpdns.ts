/** Client for the Cloud DNS emulator (managed zones, record sets, changes) via the dashboard's in-process proxy. */

import { restCall, str, strMap, type FetchFn, type Json } from "./gcprest.ts";

export const DNS_PROXY_BASE = "/dashboard/api/v1/gcp/dns/v1";
export const DNS_PROJECT_KEY = "gopherstack_dns_project";
const DEFAULT_PROJECT = "gopherstack-local";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const strs = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);

export type ZoneInfo = {
  name: string;
  dnsName: string;
  description: string;
  visibility: string;
  nameServers: string[];
  creationTime: string;
  labels: Record<string, string>;
  raw: Json;
};

export type RecordSet = { name: string; type: string; ttl: number; rrdatas: string[] };

export type ChangeInfo = {
  id: string;
  status: string;
  startTime: string;
  additions: RecordSet[];
  deletions: RecordSet[];
};

export function parseZone(raw: Json): ZoneInfo {
  return {
    name: str(raw.name),
    dnsName: str(raw.dnsName),
    description: str(raw.description),
    visibility: str(raw.visibility) || "public",
    nameServers: strs(raw.nameServers),
    creationTime: str(raw.creationTime),
    labels: strMap(raw.labels),
    raw,
  };
}

export function parseRecordSet(raw: Json): RecordSet {
  return {
    name: str(raw.name),
    type: str(raw.type),
    ttl: num(raw.ttl),
    rrdatas: strs(raw.rrdatas),
  };
}

export function parseChange(raw: Json): ChangeInfo {
  return {
    id: str(raw.id),
    status: str(raw.status),
    startTime: str(raw.startTime),
    additions: list(raw.additions).map((r) => parseRecordSet(r)),
    deletions: list(raw.deletions).map((r) => parseRecordSet(r)),
  };
}

/** A change as "+ name TYPE" / "- name TYPE" summary lines. */
export function summarizeChange(c: ChangeInfo): string[] {
  return [
    ...c.deletions.map((r) => `- ${r.name} ${r.type} ${r.rrdatas.join(", ")}`),
    ...c.additions.map((r) => `+ ${r.name} ${r.type} ${r.rrdatas.join(", ")}`),
  ];
}

const zonesPath = (project: string) => `/projects/${enc(project)}/managedZones`;
const zonePath = (project: string, zone: string) => `${zonesPath(project)}/${enc(zone)}`;

async function listPaged(path: string, key: string, fetchFn: FetchFn): Promise<Json[]> {
  const out: Json[] = [];
  let token = "";
  do {
    const sep = path.includes("?") ? "&" : "?";
    const url = token ? `${path}${sep}pageToken=${enc(token)}` : path;
    const body = await restCall(DNS_PROXY_BASE, "GET", url, undefined, fetchFn);
    out.push(...list(body[key]));
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export async function listZones(project: string, fetchFn: FetchFn = fetch): Promise<ZoneInfo[]> {
  return (await listPaged(zonesPath(project), "managedZones", fetchFn)).map((z) => parseZone(z));
}

export async function listRecordSets(
  project: string,
  zone: string,
  fetchFn: FetchFn = fetch,
): Promise<RecordSet[]> {
  return (await listPaged(`${zonePath(project, zone)}/rrsets`, "rrsets", fetchFn)).map((r) =>
    parseRecordSet(r),
  );
}

export async function listChanges(
  project: string,
  zone: string,
  fetchFn: FetchFn = fetch,
): Promise<ChangeInfo[]> {
  return (
    await listPaged(`${zonePath(project, zone)}/changes?sortOrder=descending`, "changes", fetchFn)
  ).map((c) => parseChange(c));
}

export async function deleteRecordSet(
  project: string,
  zone: string,
  rr: RecordSet,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    DNS_PROXY_BASE,
    "DELETE",
    `${zonePath(project, zone)}/rrsets/${enc(rr.name)}/${enc(rr.type)}`,
    undefined,
    fetchFn,
  );
}

export async function deleteZone(
  project: string,
  zone: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(DNS_PROXY_BASE, "DELETE", zonePath(project, zone), undefined, fetchFn);
}

export function readStoredDnsProject(): string {
  try {
    return localStorage.getItem(DNS_PROJECT_KEY)?.trim() || DEFAULT_PROJECT;
  } catch {
    return DEFAULT_PROJECT;
  }
}

export function writeStoredDnsProject(project: string): void {
  try {
    localStorage.setItem(DNS_PROJECT_KEY, project);
  } catch {
    // storage unavailable: the project applies for this session only
  }
}
