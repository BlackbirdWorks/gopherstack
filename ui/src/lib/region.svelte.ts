// Single source of truth for the "active AWS region" the dashboard talks to.
//
// This module must import ZERO AWS SDK code: `aws-client.ts` imports
// `regionProvider` from here, and AWS SDK clients pull region resolution in
// via that provider. If this file pulled in SDK code too, every route would
// end up bundling the full SDK just to read a region string.

export const DEFAULT_REGION = "us-east-1";

const REGION_STORAGE_KEY = "gopherstack_region";

function readStoredRegion(): string {
  if (typeof window === "undefined" || !window.localStorage) return DEFAULT_REGION;
  try {
    return window.localStorage.getItem(REGION_STORAGE_KEY) ?? DEFAULT_REGION;
  } catch {
    return DEFAULT_REGION;
  }
}

let region = $state(readStoredRegion());

/** Reads the current region from the rune-backed store. */
export function currentRegion(): string {
  return region;
}

/** Alias kept for source compatibility with the old `$lib/aws/client` API. */
export function getStoredRegion(): string {
  return currentRegion();
}

/** Updates the active region in state and persists it to localStorage. */
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
 * live-reactive to region changes with no need to re-create it.
 */
export const regionProvider = (): Promise<string> => Promise.resolve(region);

// Cross-tab sync: another tab changing the region updates this tab's state
// too, without needing every consumer to hand-roll a `storage` listener.
if (typeof window !== "undefined") {
  window.addEventListener("storage", (e: StorageEvent) => {
    if (e.key === REGION_STORAGE_KEY && e.newValue) {
      region = e.newValue;
    }
  });
}
