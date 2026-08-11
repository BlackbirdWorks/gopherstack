import { flushSync } from "svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { onRegionChange, regionalClient } from "./region-effect.svelte";
import { withEffectRoot } from "./region-effect.harness.svelte";
import { ALL_REGIONS, DEFAULT_REGION, setStoredRegion } from "./region.svelte";

// Everything here is imported statically (not via vi.resetModules() +
// dynamic import, unlike region.test.ts) because $effect needs the SAME
// running copy of Svelte's internal runtime that created the enclosing
// $effect.root — vi.resetModules() would give the dynamically re-imported
// region-effect.svelte a fresh copy of the `svelte` package with its own
// separate "active effect" tracking, unrelated to the one `withEffectRoot`
// set up, and every $effect() call inside it would fail with
// `effect_orphan`. Instead, state is reset by hand between tests via
// setStoredRegion.
describe("onRegionChange", () => {
  beforeEach(() => {
    setStoredRegion(DEFAULT_REGION);
  });

  it("runs the callback immediately", () => {
    const callback = vi.fn();

    const { cleanup } = withEffectRoot(() => onRegionChange(callback));
    flushSync();

    expect(callback).toHaveBeenCalledTimes(1);
    cleanup();
  });

  it("re-runs the callback when the region changes", () => {
    const callback = vi.fn();

    const { cleanup } = withEffectRoot(() => onRegionChange(callback));
    flushSync();
    expect(callback).toHaveBeenCalledTimes(1);

    setStoredRegion("eu-west-2");
    flushSync();
    expect(callback).toHaveBeenCalledTimes(2);

    setStoredRegion("ap-southeast-1");
    flushSync();
    expect(callback).toHaveBeenCalledTimes(3);
    cleanup();
  });

  it("re-runs on toggling All mode even when the resolved single region doesn't change", () => {
    // Starting region is DEFAULT_REGION, so currentRegion() stays
    // DEFAULT_REGION on both sides of this transition -- only
    // currentRegionSelection() changes. A page relying on multiRegionList
    // needs this to still re-fire so it can fan out once All is selected.
    const callback = vi.fn();

    const { cleanup } = withEffectRoot(() => onRegionChange(callback));
    flushSync();
    expect(callback).toHaveBeenCalledTimes(1);

    setStoredRegion(ALL_REGIONS);
    flushSync();
    expect(callback).toHaveBeenCalledTimes(2);

    setStoredRegion(DEFAULT_REGION);
    flushSync();
    expect(callback).toHaveBeenCalledTimes(3);
    cleanup();
  });
});

describe("regionalClient", () => {
  beforeEach(() => {
    setStoredRegion(DEFAULT_REGION);
  });

  it("builds the client with the current region, passed as a concrete string", () => {
    const factory = vi.fn((region?: string) => ({ region }));

    const { result: client, cleanup } = withEffectRoot(() => regionalClient(factory));

    expect(client()).toEqual({ region: "us-east-1" });
    expect(factory).toHaveBeenCalledWith("us-east-1");
    cleanup();
  });

  it("rebuilds the client on the next read after the region changes (lazy, pull-based)", () => {
    const factory = vi.fn((region?: string) => ({ region, instance: Symbol("client") }));

    const { result: client, cleanup } = withEffectRoot(() => regionalClient(factory));

    const first = client();
    expect(first.region).toBe("us-east-1");
    expect(factory).toHaveBeenCalledTimes(1);

    setStoredRegion("eu-west-2");

    // No flushSync() here: reading the accessor itself must be enough to
    // observe the rebuilt client, with no intervening effect tick. This is
    // the whole point of using $derived (pull, recompute-on-read) instead
    // of an $effect that rebuilds as a side effect.
    const second = client();
    expect(second.region).toBe("eu-west-2");
    expect(second).not.toBe(first);
    expect(factory).toHaveBeenCalledTimes(2);
    cleanup();
  });

  it("does not rebuild on a read that isn't preceded by a region change", () => {
    const factory = vi.fn((region?: string) => ({ region }));

    const { result: client, cleanup } = withEffectRoot(() => regionalClient(factory));

    client();
    client();
    client();

    expect(factory).toHaveBeenCalledTimes(1);
    cleanup();
  });

  it("composes with onRegionChange so a refetch inside the callback always sees the rebuilt client", () => {
    // This is the ordering guarantee that matters: a page does
    //   const client = regionalClient(getXClient);
    //   onRegionChange(() => { client().send(...) });
    // and the send() must go out through a client already rebuilt for the
    // new region, not the one built for the region that was active when the
    // page mounted.
    const factory = vi.fn((region?: string) => ({ region }));
    const seenRegionsAtCallTime: (string | undefined)[] = [];

    const { cleanup } = withEffectRoot(() => {
      const client = regionalClient(factory);
      onRegionChange(() => {
        seenRegionsAtCallTime.push(client().region);
      });
    });
    flushSync();

    expect(seenRegionsAtCallTime).toEqual(["us-east-1"]);

    setStoredRegion("sa-east-1");
    flushSync();
    expect(seenRegionsAtCallTime).toEqual(["us-east-1", "sa-east-1"]);

    setStoredRegion("ca-central-1");
    flushSync();
    expect(seenRegionsAtCallTime).toEqual(["us-east-1", "sa-east-1", "ca-central-1"]);
    cleanup();
  });
});
