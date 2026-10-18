/** Client for the Azure Resource Manager emulator via the dashboard's in-process proxy. */

export const ARM_PROXY_BASE = "/dashboard/api/v1/azure/arm";
export const ARM_API_VERSION = "2021-04-01";
export const DEFAULT_LOCATION = "eastus";

export const LOCATIONS: readonly string[] = [
  "eastus",
  "eastus2",
  "westus",
  "westus2",
  "centralus",
  "northeurope",
  "westeurope",
  "uksouth",
  "southeastasia",
  "japaneast",
  "australiaeast",
];

export const STORAGE_SKUS: readonly string[] = [
  "Standard_LRS",
  "Standard_GRS",
  "Standard_RAGRS",
  "Standard_ZRS",
  "Premium_LRS",
];

export const SERVICE_BUS_SKUS: readonly string[] = ["Basic", "Standard", "Premium"];

export type ArmTags = Record<string, string>;

export type ArmResource = {
  id: string;
  name: string;
  type: string;
  location?: string;
  kind?: string;
  tags?: ArmTags;
  sku?: { name?: string; tier?: string };
  properties?: Record<string, unknown>;
};

export type ResourceGroup = ArmResource;
export type StorageAccount = ArmResource;
export type ServiceBusNamespace = ArmResource;

export type StorageKey = { keyName: string; value: string; permissions?: string };

export type ServiceBusKeys = {
  keyName?: string;
  primaryKey?: string;
  secondaryKey?: string;
  primaryConnectionString?: string;
  secondaryConnectionString?: string;
};

export class ArmError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ArmError";
    this.status = status;
    this.code = code;
  }
}

type FetchFn = typeof fetch;

async function toArmError(res: Response): Promise<ArmError> {
  const text = await res.text().catch(() => "");
  try {
    const parsed = JSON.parse(text) as { error?: { code?: string; message?: string } };
    if (parsed.error?.message || parsed.error?.code) {
      return new ArmError(
        res.status,
        parsed.error.code ?? `HTTP${res.status}`,
        parsed.error.message ?? parsed.error.code ?? `HTTP ${res.status}`,
      );
    }
  } catch {
    // fall through to the raw body
  }
  return new ArmError(res.status, `HTTP${res.status}`, text.trim() || `HTTP ${res.status}`);
}

export async function armRequest<T>(
  method: "GET" | "PUT" | "DELETE" | "POST",
  path: string,
  body?: unknown,
  fetchFn: FetchFn = fetch,
): Promise<T | undefined> {
  const url = `${ARM_PROXY_BASE}${path}${path.includes("?") ? "&" : "?"}api-version=${ARM_API_VERSION}`;
  let res: Response;
  try {
    res = await fetchFn(url, {
      method,
      headers: body === undefined ? undefined : { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    throw new ArmError(0, "NetworkError", e instanceof Error ? e.message : String(e));
  }
  if (!res.ok) throw await toArmError(res);
  const text = await res.text();
  if (!text) return undefined;
  try {
    return JSON.parse(text) as T;
  } catch {
    throw new ArmError(res.status, "InvalidResponse", "ARM returned a non-JSON response");
  }
}

async function listValue(path: string, fetchFn?: FetchFn): Promise<ArmResource[]> {
  const res = await armRequest<{ value?: ArmResource[] }>("GET", path, undefined, fetchFn);
  return res?.value ?? [];
}

export function sortByName<T extends { name: string }>(items: T[]): T[] {
  return items.toSorted((a, b) => a.name.localeCompare(b.name));
}

export async function getSubscriptionId(fetchFn?: FetchFn): Promise<string> {
  const res = await armRequest<{ value?: { subscriptionId?: string }[] }>(
    "GET",
    "/subscriptions",
    undefined,
    fetchFn,
  );
  const id = res?.value?.[0]?.subscriptionId;
  if (!id) throw new ArmError(404, "SubscriptionNotFound", "No subscription is available");
  return id;
}

export type ParsedArmId = {
  subscription: string;
  resourceGroup: string;
  provider: string;
  segments: string[];
};

/** Splits "/subscriptions/{s}/resourceGroups/{rg}/providers/{ns}/{type}/{name}/..." */
export function parseArmId(id: string): ParsedArmId {
  const parts = id.split("/").filter(Boolean);
  const out: ParsedArmId = { subscription: "", resourceGroup: "", provider: "", segments: [] };
  for (let i = 0; i < parts.length; i++) {
    const key = parts[i].toLowerCase();
    if (key === "subscriptions") out.subscription = parts[++i] ?? "";
    else if (key === "resourcegroups") out.resourceGroup = parts[++i] ?? "";
    else if (key === "providers") {
      out.provider = parts[++i] ?? "";
      out.segments = parts.slice(i + 1);
      break;
    }
  }
  return out;
}

export function inGroup(resource: ArmResource, group: string): boolean {
  return parseArmId(resource.id).resourceGroup.toLowerCase() === group.toLowerCase();
}

export function resourcesInGroup<T extends ArmResource>(items: T[], group: string): T[] {
  return items.filter((r) => inGroup(r, group));
}

export function shortType(type: string): string {
  return type.split("/").slice(1).join("/") || type;
}

const RG_NAME = /^[\w.()-]{1,90}$/;
const STORAGE_NAME = /^[a-z0-9]{3,24}$/;
const SB_NAME = /^[A-Za-z][A-Za-z0-9-]{4,48}[A-Za-z0-9]$/;

export function validateResourceGroupName(name: string): string | null {
  if (!name) return "Name is required";
  if (!RG_NAME.test(name) || name.endsWith("."))
    return "Use letters, numbers, . _ - ( ); up to 90 characters, not ending in a period";
  return null;
}

export function validateStorageAccountName(name: string): string | null {
  if (!name) return "Name is required";
  if (!STORAGE_NAME.test(name)) return "3-24 characters, lowercase letters and numbers only";
  return null;
}

export function validateNamespaceName(name: string): string | null {
  if (!name) return "Name is required";
  if (!SB_NAME.test(name)) {
    return "6-50 characters; letters, numbers and hyphens; start with a letter, end with a letter or number";
  }
  return null;
}

/** Parses "k=v, k2=v2" (or one pair per line) into tags; throws on a malformed pair. */
export function parseTags(input: string): ArmTags {
  const tags: ArmTags = {};
  for (const raw of input.split(/[\n,]/)) {
    const pair = raw.trim();
    if (!pair) continue;
    const eq = pair.indexOf("=");
    if (eq <= 0) throw new Error(`Invalid tag "${pair}"; use key=value`);
    tags[pair.slice(0, eq).trim()] = pair.slice(eq + 1).trim();
  }
  return tags;
}

export function formatTags(tags?: ArmTags): string {
  return Object.entries(tags ?? {})
    .map(([k, v]) => `${k}=${v}`)
    .join(", ");
}

const enc = encodeURIComponent;
const sub = (s: string) => `/subscriptions/${enc(s)}`;

export async function listResourceGroups(s: string, fetchFn?: FetchFn): Promise<ResourceGroup[]> {
  return sortByName(await listValue(`${sub(s)}/resourcegroups`, fetchFn));
}

export async function putResourceGroup(
  s: string,
  name: string,
  location: string,
  tags: ArmTags,
  fetchFn?: FetchFn,
): Promise<ResourceGroup> {
  const res = await armRequest<ResourceGroup>(
    "PUT",
    `${sub(s)}/resourcegroups/${enc(name)}`,
    { location, tags },
    fetchFn,
  );
  if (!res) throw new ArmError(500, "EmptyResponse", "ARM returned no body");
  return res;
}

export async function deleteResourceGroup(
  s: string,
  name: string,
  fetchFn?: FetchFn,
): Promise<void> {
  await armRequest("DELETE", `${sub(s)}/resourcegroups/${enc(name)}`, undefined, fetchFn);
}

export async function listStorageAccounts(s: string, fetchFn?: FetchFn): Promise<StorageAccount[]> {
  return sortByName(
    await listValue(`${sub(s)}/providers/Microsoft.Storage/storageAccounts`, fetchFn),
  );
}

export type StorageAccountInput = {
  name: string;
  resourceGroup: string;
  location: string;
  sku: string;
  kind: string;
  tags: ArmTags;
};

export async function putStorageAccount(
  s: string,
  input: StorageAccountInput,
  fetchFn?: FetchFn,
): Promise<StorageAccount> {
  const res = await armRequest<StorageAccount>(
    "PUT",
    `${sub(s)}/resourceGroups/${enc(input.resourceGroup)}/providers/Microsoft.Storage/storageAccounts/${enc(input.name)}`,
    { location: input.location, kind: input.kind, sku: { name: input.sku }, tags: input.tags },
    fetchFn,
  );
  if (!res) throw new ArmError(500, "EmptyResponse", "ARM returned no body");
  return res;
}

export async function deleteStorageAccount(
  s: string,
  acct: ArmResource,
  fetchFn?: FetchFn,
): Promise<void> {
  await armRequest("DELETE", resourcePath(s, acct), undefined, fetchFn);
}

export async function listStorageKeys(
  s: string,
  acct: ArmResource,
  fetchFn?: FetchFn,
): Promise<StorageKey[]> {
  const res = await armRequest<{ keys?: StorageKey[] }>(
    "POST",
    `${resourcePath(s, acct)}/listKeys`,
    undefined,
    fetchFn,
  );
  return res?.keys ?? [];
}

export async function listNamespaces(s: string, fetchFn?: FetchFn): Promise<ServiceBusNamespace[]> {
  return sortByName(
    await listValue(`${sub(s)}/providers/Microsoft.ServiceBus/namespaces`, fetchFn),
  );
}

export type NamespaceInput = {
  name: string;
  resourceGroup: string;
  location: string;
  sku: string;
  tags: ArmTags;
};

export async function putNamespace(
  s: string,
  input: NamespaceInput,
  fetchFn?: FetchFn,
): Promise<ServiceBusNamespace> {
  const res = await armRequest<ServiceBusNamespace>(
    "PUT",
    `${sub(s)}/resourceGroups/${enc(input.resourceGroup)}/providers/Microsoft.ServiceBus/namespaces/${enc(input.name)}`,
    { location: input.location, sku: { name: input.sku, tier: input.sku }, tags: input.tags },
    fetchFn,
  );
  if (!res) throw new ArmError(500, "EmptyResponse", "ARM returned no body");
  return res;
}

export async function deleteNamespace(
  s: string,
  ns: ArmResource,
  fetchFn?: FetchFn,
): Promise<void> {
  await armRequest("DELETE", resourcePath(s, ns), undefined, fetchFn);
}

export async function listNamespaceKeys(
  s: string,
  ns: ArmResource,
  rule = "RootManageSharedAccessKey",
  fetchFn?: FetchFn,
): Promise<ServiceBusKeys> {
  const res = await armRequest<ServiceBusKeys>(
    "POST",
    `${resourcePath(s, ns)}/authorizationRules/${enc(rule)}/listKeys`,
    undefined,
    fetchFn,
  );
  return res ?? {};
}

/** Builds the proxied ARM path for a resource from its group and the subscription. */
export function resourcePath(s: string, r: ArmResource): string {
  const parsed = parseArmId(r.id);
  return `${sub(s)}/resourceGroups/${enc(parsed.resourceGroup)}/providers/${parsed.provider}/${parsed.segments.map(enc).join("/")}`;
}
