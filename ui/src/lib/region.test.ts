import { beforeEach, describe, expect, it, vi } from "vitest";

const REGION_STORAGE_KEY = "gopherstack_region";

describe("region store", () => {
  beforeEach(() => {
    vi.resetModules();
    window.localStorage.clear();
  });

  it("defaults to us-east-1 when nothing is stored", async () => {
    const { currentRegion, DEFAULT_REGION } = await import("./region.svelte");

    expect(DEFAULT_REGION).toBe("us-east-1");
    expect(currentRegion()).toBe("us-east-1");
  });

  it("defaults to All mode for a fresh user", async () => {
    const { currentRegionSelection, isAllRegions, ALL_REGIONS } = await import("./region.svelte");

    expect(currentRegionSelection()).toBe(ALL_REGIONS);
    expect(isAllRegions()).toBe(true);
  });

  it("All mode round-trips through localStorage", async () => {
    const { setStoredRegion, ALL_REGIONS } = await import("./region.svelte");
    setStoredRegion(ALL_REGIONS);

    expect(window.localStorage.getItem(REGION_STORAGE_KEY)).toBe(ALL_REGIONS);

    const { currentRegionSelection, isAllRegions } = await import("./region.svelte");
    expect(currentRegionSelection()).toBe(ALL_REGIONS);
    expect(isAllRegions()).toBe(true);
  });

  it("currentRegion() resolves ALL_REGIONS down to DEFAULT_REGION", async () => {
    const { currentRegion, setStoredRegion, ALL_REGIONS, DEFAULT_REGION } =
      await import("./region.svelte");
    setStoredRegion(ALL_REGIONS);

    expect(currentRegion()).toBe(DEFAULT_REGION);
  });

  it("selecting a concrete region leaves All mode", async () => {
    const { setStoredRegion, isAllRegions, currentRegion, currentRegionSelection } =
      await import("./region.svelte");
    setStoredRegion("eu-west-1");

    expect(isAllRegions()).toBe(false);
    expect(currentRegion()).toBe("eu-west-1");
    expect(currentRegionSelection()).toBe("eu-west-1");
  });

  it("regionProvider never resolves to the ALL_REGIONS sentinel", async () => {
    const { regionProvider, setStoredRegion, ALL_REGIONS, DEFAULT_REGION } =
      await import("./region.svelte");
    setStoredRegion(ALL_REGIONS);

    await expect(regionProvider()).resolves.toBe(DEFAULT_REGION);
  });

  it("initializes from a previously stored region", async () => {
    window.localStorage.setItem(REGION_STORAGE_KEY, "eu-west-1");

    const { currentRegion } = await import("./region.svelte");

    expect(currentRegion()).toBe("eu-west-1");
  });

  it("getStoredRegion is an alias for currentRegion", async () => {
    const { currentRegion, getStoredRegion, setStoredRegion } = await import("./region.svelte");

    setStoredRegion("ap-southeast-1");

    expect(getStoredRegion()).toBe("ap-southeast-1");
    expect(getStoredRegion()).toBe(currentRegion());
  });

  it("setStoredRegion updates state and persists to localStorage", async () => {
    const { currentRegion, setStoredRegion } = await import("./region.svelte");

    setStoredRegion("us-west-2");

    expect(currentRegion()).toBe("us-west-2");
    expect(window.localStorage.getItem(REGION_STORAGE_KEY)).toBe("us-west-2");
  });

  it("regionProvider resolves to the current region", async () => {
    const { setStoredRegion, regionProvider } = await import("./region.svelte");

    setStoredRegion("ca-central-1");

    await expect(regionProvider()).resolves.toBe("ca-central-1");
  });

  it("syncs across tabs via the storage event", async () => {
    const { currentRegion } = await import("./region.svelte");

    expect(currentRegion()).toBe("us-east-1");
    window.dispatchEvent(
      new StorageEvent("storage", { key: REGION_STORAGE_KEY, newValue: "sa-east-1" }),
    );

    expect(currentRegion()).toBe("sa-east-1");
  });

  it("ignores storage events for unrelated keys", async () => {
    const { currentRegion } = await import("./region.svelte");

    window.dispatchEvent(
      new StorageEvent("storage", { key: "some-other-key", newValue: "sa-east-1" }),
    );

    expect(currentRegion()).toBe("us-east-1");
  });

  it("falls back to the default region when window is unavailable (SSR)", async () => {
    const originalWindow = globalThis.window;
    // @ts-expect-error -- simulate an SSR/prerender environment with no window
    delete globalThis.window;

    try {
      const { currentRegion } = await import("./region.svelte");
      expect(currentRegion()).toBe("us-east-1");
    } finally {
      globalThis.window = originalWindow;
    }
  });
});
