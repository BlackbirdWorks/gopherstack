/** Client for the Oracle Cloud DNS emulator through the dashboard's in-process proxy. */

import { ociCompartments, ociListPages, readStoredValue, writeStoredValue } from "./ocirest.ts";
import { GcpRestError, restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const DNS_PROXY_BASE = "/dashboard/api/v1/oci/dns";
export const DNS_COMPARTMENT_KEY = "gopherstack_ocidns_compartment";

export const RECORD_TYPES = ["A", "AAAA", "CNAME", "MX", "NS", "TXT", "SRV", "PTR", "CAA"] as const;
export const QUERY_TYPES = [...RECORD_TYPES, "SOA"] as const;
export const DEFAULT_TTL = 300;

const enc = encodeURIComponent;
const num = (v: unknown): number => (typeof v === "number" ? v : Number(v) || 0);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);

export type Scope = "GLOBAL" | "PRIVATE";

export type Zone = {
  id: string;
  name: string;
  type: string;
  scope: string;
  serial: number;
  created: string;
  state: string;
  viewId: string;
  nameservers: string[];
};

export type DnsRecord = {
  domain: string;
  rtype: string;
  ttl: number;
  rdata: string;
  protected: boolean;
};

export type RRSet = { domain: string; rtype: string; ttl: number; records: DnsRecord[] };

export type View = { id: string; name: string; created: string; protected: boolean; state: string };
export type Resolver = {
  id: string;
  name: string;
  vcnId: string;
  defaultViewId: string;
  state: string;
  created: string;
};
export type Endpoint = {
  name: string;
  subnetId: string;
  kind: string;
  address: string;
  state: string;
};
export type TsigKey = {
  id: string;
  name: string;
  algorithm: string;
  state: string;
  created: string;
};
export type SteeringPolicy = {
  id: string;
  name: string;
  template: string;
  ttl: number;
  state: string;
  created: string;
};

export type DnsAnswerRecord = { name: string; type: string; ttl: number; data: string };
export type DnsAnswer = {
  rcode: string;
  authoritative: boolean;
  name: string;
  type: string;
  server: string;
  answer: DnsAnswerRecord[];
  authority: DnsAnswerRecord[];
};

export type ZoneRecords = { records: DnsRecord[]; etag: string };

export type NewZone = {
  name: string;
  compartmentId: string;
  scope: Scope;
  viewId: string;
};

export type RecordDraft = { domain: string; rtype: string; ttl: number; rdata: string };
export type RecordOp = { operation: "ADD" | "REMOVE" } & RecordDraft;

export function parseZone(raw: Json): Zone {
  return {
    id: str(raw.id),
    name: str(raw.name),
    type: str(raw.zoneType),
    scope: str(raw.scope),
    serial: num(raw.serial),
    created: str(raw.timeCreated),
    state: str(raw.lifecycleState),
    viewId: str(raw.viewId),
    nameservers: list(raw.nameservers).map((n) => str(n.hostname)),
  };
}

export function parseRecord(raw: Json): DnsRecord {
  return {
    domain: str(raw.domain),
    rtype: str(raw.rtype),
    ttl: num(raw.ttl),
    rdata: str(raw.rdata),
    protected: raw.isProtected === true,
  };
}

/** Groups records by domain and type, the way OCI addresses an rrset. */
export function groupRecords(records: readonly DnsRecord[]): RRSet[] {
  const sets = new Map<string, RRSet>();
  for (const r of records) {
    const key = `${r.domain}|${r.rtype}`;
    const set = sets.get(key) ?? { domain: r.domain, rtype: r.rtype, ttl: r.ttl, records: [] };
    set.records.push(r);
    sets.set(key, set);
  }
  return [...sets.values()].toSorted(
    (a, b) => a.domain.localeCompare(b.domain) || a.rtype.localeCompare(b.rtype),
  );
}

export const isConflict = (e: unknown): boolean =>
  e instanceof GcpRestError && (e.httpStatus === 412 || e.httpStatus === 409);

export function validateRecord(zone: string, d: RecordDraft): string {
  const domain = d.domain.trim().replace(/\.$/, "");
  if (domain === "") return "Domain is required";
  if (!/^[A-Za-z0-9_*.-]{1,255}$/.test(domain)) return "Domain has invalid characters";
  if (domain !== zone && !domain.endsWith(`.${zone}`))
    return `Domain must be ${zone} or end in .${zone}`;
  if (!(RECORD_TYPES as readonly string[]).includes(d.rtype)) return "Pick a record type";
  if (d.rdata.trim() === "") return "Record data is required";
  if (!Number.isInteger(d.ttl) || d.ttl < 0 || d.ttl > 2147483647)
    return "TTL must be a whole number of seconds";
  return "";
}

const normalized = (d: RecordDraft): RecordDraft => ({
  domain: d.domain.trim().replace(/\.$/, ""),
  rtype: d.rtype,
  ttl: d.ttl,
  rdata: d.rdata.trim(),
});

export const addOp = (d: RecordDraft): RecordOp => ({ operation: "ADD", ...normalized(d) });
export const removeOp = (d: RecordDraft): RecordOp => ({ operation: "REMOVE", ...normalized(d) });

/** An edit is the old record removed and the new one added in one atomic patch. */
export const editOps = (old: RecordDraft, next: RecordDraft): RecordOp[] => [
  removeOp(old),
  addOp(next),
];

type Fetched = { body: unknown; etag: string; next: string };

async function dnsFetch(
  path: string,
  init: RequestInit | undefined,
  fetchFn: FetchFn,
): Promise<Fetched> {
  let res: Response;
  try {
    res = await fetchFn(`${DNS_PROXY_BASE}${path}`, init);
  } catch (e) {
    throw new GcpRestError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  const text = await res.text();
  let body: unknown = null;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    // non-JSON error body: keep the status text
  }
  if (!res.ok) {
    const b = obj(body);
    const proxy = obj(b.error);
    throw new GcpRestError(
      res.status,
      str(proxy.status) || str(b.code),
      str(proxy.message) || str(b.message) || res.statusText || `HTTP ${res.status}`,
    );
  }
  return {
    body,
    etag: res.headers.get("etag") ?? "",
    next: res.headers.get("opc-next-page") ?? "",
  };
}

export const listCompartments = (fetchFn: FetchFn = fetch) =>
  ociCompartments(DNS_PROXY_BASE, fetchFn);

export async function getDataPlane(
  fetchFn: FetchFn = fetch,
): Promise<{ enabled: boolean; port: number }> {
  const body = await restCall(DNS_PROXY_BASE, "GET", "/dataplane", undefined, fetchFn);
  return { enabled: body.enabled === true, port: num(body.port) };
}

const compQuery = (compartment: string) => `compartmentId=${enc(compartment)}`;

/** Zones of both scopes: GLOBAL zones answer on the data plane, PRIVATE zones belong to a view. */
export async function listZones(compartment: string, fetchFn: FetchFn = fetch): Promise<Zone[]> {
  const scopes: Scope[] = ["GLOBAL", "PRIVATE"];
  const lists = await Promise.all(
    scopes.map((scope) =>
      ociListPages(
        DNS_PROXY_BASE,
        "/api/zones",
        `${compQuery(compartment)}&scope=${scope}`,
        "",
        fetchFn,
      ),
    ),
  );
  return lists
    .flat()
    .map((z) => parseZone(z))
    .filter((z) => z.state !== "DELETED")
    .toSorted((a, b) => a.name.localeCompare(b.name));
}

const zoneQuery = (z: Pick<Zone, "scope" | "viewId">) =>
  z.scope === "PRIVATE" ? `scope=PRIVATE&viewId=${enc(z.viewId)}` : "";

export async function getZoneRecords(
  zone: Pick<Zone, "id" | "scope" | "viewId">,
  fetchFn: FetchFn = fetch,
): Promise<ZoneRecords> {
  const records: DnsRecord[] = [];
  let etag = "";
  let page = "";
  do {
    const qs = [zoneQuery(zone), page ? `page=${enc(page)}` : ""].filter(Boolean).join("&");
    const res = await dnsFetch(
      `/api/zones/${enc(zone.id)}/records${qs ? `?${qs}` : ""}`,
      undefined,
      fetchFn,
    );
    etag = etag || res.etag;
    records.push(...list(obj(res.body).items).map((r) => parseRecord(r)));
    page = res.next;
  } while (page);
  return { records, etag };
}

/** Applies ADD/REMOVE operations; the etag makes a concurrent change fail with a conflict. */
export async function patchRecords(
  zone: Pick<Zone, "id" | "scope" | "viewId">,
  ops: readonly RecordOp[],
  etag: string,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const qs = zoneQuery(zone);
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (etag) headers["If-Match"] = etag;
  await dnsFetch(
    `/api/zones/${enc(zone.id)}/records${qs ? `?${qs}` : ""}`,
    { method: "PATCH", headers, body: JSON.stringify({ items: ops }) },
    fetchFn,
  );
}

export function createZoneBody(z: NewZone): Json {
  const body: Json = {
    name: z.name.trim(),
    compartmentId: z.compartmentId,
    zoneType: "PRIMARY",
    scope: z.scope,
  };
  if (z.scope === "PRIVATE") body.viewId = z.viewId;
  return body;
}

export function validateZone(z: NewZone): string {
  const name = z.name.trim();
  if (name === "") return "Zone name is required";
  if (!/^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)+$/.test(name))
    return "Zone name must be a domain such as example.com";
  if (z.scope === "PRIVATE" && z.viewId === "") return "A private zone needs a view";
  return "";
}

export async function createZone(z: NewZone, fetchFn: FetchFn = fetch): Promise<void> {
  await restCall(DNS_PROXY_BASE, "POST", "/api/zones", createZoneBody(z), fetchFn);
}

export async function deleteZone(
  zone: Pick<Zone, "id" | "scope" | "viewId">,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const qs = zoneQuery(zone);
  await restCall(
    DNS_PROXY_BASE,
    "DELETE",
    `/api/zones/${enc(zone.id)}${qs ? `?${qs}` : ""}`,
    undefined,
    fetchFn,
  );
}

export async function listViews(compartment: string, fetchFn: FetchFn = fetch): Promise<View[]> {
  const items = await ociListPages(
    DNS_PROXY_BASE,
    "/api/views",
    compQuery(compartment),
    "",
    fetchFn,
  );
  return items.map((v) => ({
    id: str(v.id),
    name: str(v.displayName),
    created: str(v.timeCreated),
    protected: v.isProtected === true,
    state: str(v.lifecycleState),
  }));
}

export async function listResolvers(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<Resolver[]> {
  const items = await ociListPages(
    DNS_PROXY_BASE,
    "/api/resolvers",
    compQuery(compartment),
    "",
    fetchFn,
  );
  return items.map((r) => ({
    id: str(r.id),
    name: str(r.displayName),
    vcnId: str(r.attachedVcnId),
    defaultViewId: str(r.defaultViewId),
    state: str(r.lifecycleState),
    created: str(r.timeCreated),
  }));
}

export async function listEndpoints(
  resolverId: string,
  fetchFn: FetchFn = fetch,
): Promise<Endpoint[]> {
  const items = await ociListPages(
    DNS_PROXY_BASE,
    `/api/resolvers/${enc(resolverId)}/endpoints`,
    "",
    "",
    fetchFn,
  );
  return items.map((e) => ({
    name: str(e.name),
    subnetId: str(e.subnetId),
    kind: e.isForwarding === true ? "Forwarding" : "Listening",
    address: str(e.forwardingAddress) || str(e.listeningAddress),
    state: str(e.lifecycleState),
  }));
}

/** TSIG keys as the proxy answers them: the secret is never part of the answer. */
export async function listTsigKeys(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<TsigKey[]> {
  const items = await ociListPages(
    DNS_PROXY_BASE,
    "/api/tsigKeys",
    compQuery(compartment),
    "",
    fetchFn,
  );
  return items.map((k) => ({
    id: str(k.id),
    name: str(k.name),
    algorithm: str(k.algorithm),
    state: str(k.lifecycleState),
    created: str(k.timeCreated),
  }));
}

export async function listSteeringPolicies(
  compartment: string,
  fetchFn: FetchFn = fetch,
): Promise<SteeringPolicy[]> {
  const items = await ociListPages(
    DNS_PROXY_BASE,
    "/api/steeringPolicies",
    compQuery(compartment),
    "",
    fetchFn,
  );
  return items.map((p) => ({
    id: str(p.id),
    name: str(p.displayName),
    template: str(p.template),
    ttl: num(p.ttl),
    state: str(p.lifecycleState),
    created: str(p.timeCreated),
  }));
}

const parseRR = (r: Json): DnsAnswerRecord => ({
  name: str(r.name),
  type: str(r.type),
  ttl: num(r.ttl),
  data: str(r.data),
});

export function validateQuery(name: string, type: string): string {
  const n = name.trim().replace(/\.$/, "");
  if (n === "") return "Enter a name to look up";
  if (
    n.length > 253 ||
    !n.split(".").every((l) => /^[A-Za-z0-9_*]([A-Za-z0-9_-]{0,61}[A-Za-z0-9_])?$/.test(l))
  ) {
    return "Not a valid DNS name";
  }
  if (!(QUERY_TYPES as readonly string[]).includes(type)) return "Pick a record type";
  return "";
}

/** Runs one query against the emulator's DNS data plane (server side, loopback only). */
export async function tryQuery(
  name: string,
  type: string,
  fetchFn: FetchFn = fetch,
): Promise<DnsAnswer> {
  const b = await restCall(DNS_PROXY_BASE, "POST", "/query", { name: name.trim(), type }, fetchFn);
  return {
    rcode: str(b.rcode),
    authoritative: b.authoritative === true,
    name: str(b.name),
    type: str(b.type),
    server: str(b.server),
    answer: list(b.answer).map((r) => parseRR(r)),
    authority: list(b.authority).map((r) => parseRR(r)),
  };
}

export const readStoredDnsCompartment = () => readStoredValue(DNS_COMPARTMENT_KEY);
export const writeStoredDnsCompartment = (id: string) => writeStoredValue(DNS_COMPARTMENT_KEY, id);
