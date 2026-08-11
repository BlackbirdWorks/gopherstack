// Single source of truth for the "active AWS region" the dashboard talks to.
//
// This module must import ZERO AWS SDK code: `aws-client.ts` imports
// `regionProvider` from here, and AWS SDK clients pull region resolution in
// via that provider. If this file pulled in SDK code too, every route would
// end up bundling the full SDK just to read a region string.

export const DEFAULT_REGION = "us-east-1";

/**
 * Sentinel for "Region: All" mode. Stored under the same localStorage key
 * as a real region, so it round-trips exactly like one -- but it is never
 * handed to an AWS SDK client or the backend (there is no backend wildcard
 * region). Real AWS regions, and the made-up ones this emulator also
 * allows, are always plain lowercase/hyphen tokens like `us-east-1` or
 * `mars-north-1`; the double underscore here can't occur in one, so this
 * can never collide with a typed region even though arbitrary region names
 * are allowed everywhere else in this app.
 */
export const ALL_REGIONS = "__all__";

const REGION_STORAGE_KEY = "gopherstack_region";

function readStoredRegion(): string {
  if (typeof window === "undefined" || !window.localStorage) return ALL_REGIONS;
  try {
    return window.localStorage.getItem(REGION_STORAGE_KEY) ?? ALL_REGIONS;
  } catch {
    return ALL_REGIONS;
  }
}

let region = $state(readStoredRegion());

/** The raw selection -- a concrete region, or `ALL_REGIONS`. Drives the region picker UI. */
export function currentRegionSelection(): string {
  return region;
}

export function isAllRegions(): boolean {
  return region === ALL_REGIONS;
}

/**
 * Reads the current SINGLE region from the rune-backed store. Resolves
 * `ALL_REGIONS` down to `DEFAULT_REGION`, so every call site written
 * before "All" mode existed -- which is most of them -- keeps working
 * unchanged: they just see the default region's data instead of every
 * region's. Multi-region fan-out (`multi-region.ts`) reads
 * `isAllRegions()` directly instead of going through this fallback.
 */
export function currentRegion(): string {
  return region === ALL_REGIONS ? DEFAULT_REGION : region;
}

/** Alias kept for source compatibility with the old `$lib/aws/client` API. */
export function getStoredRegion(): string {
  return currentRegion();
}

/** Updates the active region (or ALL_REGIONS) in state and persists it to localStorage. */
export function setStoredRegion(newRegion: string): void {
  region = newRegion;

  if (typeof window === "undefined" || !window.localStorage) return;
  try {
    window.localStorage.setItem(REGION_STORAGE_KEY, newRegion);
  } catch {
    // ignore (e.g. private browsing / storage quota)
  }
}

/**
 * A Smithy `Provider<string>`. AWS SDK clients re-invoke this per request
 * (unmemoized), so passing it as a client's `region` makes the client
 * live-reactive to region changes with no need to re-create it. Resolves
 * through `currentRegion()`, so it never hands the SDK the `ALL_REGIONS`
 * sentinel.
 */
export const regionProvider = (): Promise<string> => Promise.resolve(currentRegion());

// Cross-tab sync: another tab changing the region updates this tab's state
// too, without needing every consumer to hand-roll a `storage` listener.
if (typeof window !== "undefined") {
  window.addEventListener("storage", (e: StorageEvent) => {
    if (e.key === REGION_STORAGE_KEY && e.newValue) {
      region = e.newValue;
    }
  });
}
