/** Client for Azure App Service web apps and deployment slots: Microsoft.Web via the dashboard's in-process ARM proxy. */

import { parseArmId } from "./azurearm.ts";
import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const APPSERVICE_PROXY_BASE = "/dashboard/api/v1/azure/appservice";
export const APPSERVICE_API_VERSION = "2023-12-01";
export const PRODUCTION_SLOT = "production";

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});

export type SiteAction = "start" | "stop" | "restart";

/** How a swap proceeds: straight through, or in the two phases of a swap with preview. */
export type SwapMode = "swap" | "preview" | "complete" | "cancel";

export type WebSite = {
  id: string;
  name: string;
  app: string;
  slot: string;
  resourceGroup: string;
  location: string;
  kind: string;
  state: string;
  runtime: string;
  hostNames: string[];
  defaultHostName: string;
  httpsOnly: boolean;
  previewTarget: string;
  lastSwap: string;
};

export type StickySettings = {
  appSettings: string[];
  connectionStrings: string[];
  azureStorage: string[];
};

export function parseSite(raw: Json): WebSite {
  const p = obj(raw.properties);
  const id = str(raw.id);
  const name = str(raw.name);
  const [app = name, slot = ""] = name.split("/");
  const swap = obj(p.slotSwapStatus);
  const cfg = obj(p.siteConfig);
  const hosts = Array.isArray(p.hostNames) ? p.hostNames.map(String) : [];
  return {
    id,
    name,
    app,
    slot,
    resourceGroup: parseArmId(id).resourceGroup,
    location: str(raw.location),
    kind: str(raw.kind),
    state: str(p.state),
    runtime: str(cfg.linuxFxVersion) || str(cfg.windowsFxVersion),
    hostNames: hosts,
    defaultHostName: str(p.defaultHostName) || (hosts[0] ?? ""),
    httpsOnly: p.httpsOnly === true,
    previewTarget: str(p.targetSwapSlot),
    lastSwap: swap.sourceSlotName
      ? `${str(swap.sourceSlotName)} → ${str(swap.destinationSlotName)} at ${str(swap.timestampUtc)}`
      : "",
  };
}

/** Function apps share Microsoft.Web/sites with web apps; the web apps page shows only the latter. */
export const isWebApp = (s: Pick<WebSite, "kind">): boolean =>
  !s.kind.toLowerCase().includes("functionapp");

const names = (v: unknown): string[] => (Array.isArray(v) ? v.map(String) : []);

export function parseSticky(raw: Json): StickySettings {
  const p = obj(raw.properties);
  return {
    appSettings: names(p.appSettingNames),
    connectionStrings: names(p.connectionStringNames),
    azureStorage: names(p.azureStorageConfigNames),
  };
}

/** The URL the app serves from, for linking to its hostname. */
export const siteUrl = (s: Pick<WebSite, "defaultHostName" | "httpsOnly">): string =>
  s.defaultHostName ? `${s.httpsOnly ? "https" : "http"}://${s.defaultHostName}` : "";

const withVersion = (path: string) => `${path}?api-version=${APPSERVICE_API_VERSION}`;

const sitePath = (sub: string, rg: string, app: string) =>
  `/subscriptions/${enc(sub)}/resourceGroups/${enc(rg)}/providers/Microsoft.Web/sites/${enc(app)}`;

const targetPath = (sub: string, s: Pick<WebSite, "resourceGroup" | "app" | "slot">) =>
  `${sitePath(sub, s.resourceGroup, s.app)}${s.slot ? `/slots/${enc(s.slot)}` : ""}`;

export async function listWebApps(sub: string, fetchFn: FetchFn = fetch): Promise<WebSite[]> {
  const body = await restCall(
    APPSERVICE_PROXY_BASE,
    "GET",
    withVersion(`/subscriptions/${enc(sub)}/providers/Microsoft.Web/sites`),
    undefined,
    fetchFn,
  );
  return list(body.value)
    .map((r) => parseSite(r))
    .filter((s) => isWebApp(s) && !s.slot)
    .toSorted(
      (a, b) => a.resourceGroup.localeCompare(b.resourceGroup) || a.app.localeCompare(b.app),
    );
}

export async function listSlots(
  sub: string,
  app: Pick<WebSite, "resourceGroup" | "app">,
  fetchFn: FetchFn = fetch,
): Promise<WebSite[]> {
  const body = await restCall(
    APPSERVICE_PROXY_BASE,
    "GET",
    withVersion(`${sitePath(sub, app.resourceGroup, app.app)}/slots`),
    undefined,
    fetchFn,
  );
  return list(body.value)
    .map((r) => parseSite(r))
    .toSorted((a, b) => a.slot.localeCompare(b.slot));
}

export async function getStickySettings(
  sub: string,
  app: Pick<WebSite, "resourceGroup" | "app">,
  fetchFn: FetchFn = fetch,
): Promise<StickySettings> {
  return parseSticky(
    await restCall(
      APPSERVICE_PROXY_BASE,
      "GET",
      withVersion(`${sitePath(sub, app.resourceGroup, app.app)}/config/slotConfigNames`),
      undefined,
      fetchFn,
    ),
  );
}

export async function siteAction(
  sub: string,
  site: Pick<WebSite, "resourceGroup" | "app" | "slot">,
  action: SiteAction,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  await restCall(
    APPSERVICE_PROXY_BASE,
    "POST",
    withVersion(`${targetPath(sub, site)}/${action}`),
    undefined,
    fetchFn,
  );
}

const SWAP_VERB: Record<SwapMode, string> = {
  swap: "slotsswap",
  preview: "applySlotConfig",
  complete: "slotsswap",
  cancel: "resetSlotConfig",
};

/** Swaps `source` (a slot name, or "production") with `target`, in the given phase. */
export async function swapSlots(
  sub: string,
  app: Pick<WebSite, "resourceGroup" | "app">,
  source: string,
  target: string,
  mode: SwapMode,
  fetchFn: FetchFn = fetch,
): Promise<void> {
  const from = { ...app, slot: source === PRODUCTION_SLOT ? "" : source };
  await restCall(
    APPSERVICE_PROXY_BASE,
    "POST",
    withVersion(`${targetPath(sub, from)}/${SWAP_VERB[mode]}`),
    mode === "cancel" ? undefined : { targetSlot: target },
    fetchFn,
  );
}

/** The slots a given slot can swap with: every other slot, production included. */
export function swapTargets(slots: Pick<WebSite, "slot">[], source: string): string[] {
  return [PRODUCTION_SLOT, ...slots.map((s) => s.slot)].filter((n) => n !== source);
}

export const stickyCount = (s: StickySettings): number =>
  s.appSettings.length + s.connectionStrings.length + s.azureStorage.length;
