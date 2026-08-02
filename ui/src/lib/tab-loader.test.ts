import { render, screen, waitFor } from "@testing-library/svelte";
import { flushSync } from "svelte";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { onRegionChange } from "./region-effect.svelte";
import { withEffectRoot } from "./region-effect.harness.svelte";
import { DEFAULT_REGION, setStoredRegion } from "./region.svelte";
import { createTabLoader } from "./tab-loader.svelte";
import TabLoaderHarness from "./tab-loader.harness.svelte";

describe("createTabLoader", () => {
  it("calls the fetcher and marks the tab loaded", async () => {
    const fetcher = vi.fn(async () => {});
    const loader = createTabLoader({ a: fetcher });

    expect(loader.isLoaded("a")).toBe(false);
    await loader.load("a");

    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(loader.isLoaded("a")).toBe(true);
    expect(loader.isLoading("a")).toBe(false);
    expect(loader.getError("a")).toBeNull();
  });

  it("does not refetch a tab that is already loaded", async () => {
    const fetcher = vi.fn(async () => {});
    const loader = createTabLoader({ a: fetcher });

    await loader.load("a");
    await loader.load("a");
    await loader.load("a");

    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("dedupes concurrent loads of the same tab", async () => {
    let resolveFetch!: () => void;
    const fetcher = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          resolveFetch = resolve;
        }),
    );
    const loader = createTabLoader({ a: fetcher });

    const first = loader.load("a");
    const second = loader.load("a");

    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(loader.isLoading("a")).toBe(true);

    resolveFetch();
    await Promise.all([first, second]);

    expect(loader.isLoaded("a")).toBe(true);
  });

  it("force refreshes an already-loaded tab", async () => {
    const fetcher = vi.fn(async () => {});
    const loader = createTabLoader({ a: fetcher });

    await loader.load("a");
    await loader.refresh("a");
    await loader.load("a", true);

    expect(fetcher).toHaveBeenCalledTimes(3);
    expect(loader.isLoaded("a")).toBe(true);
  });

  it("captures errors into per-tab error state instead of throwing", async () => {
    const fetcher = vi.fn().mockRejectedValue(new Error("boom"));
    const loader = createTabLoader({ a: fetcher });

    await expect(loader.load("a")).resolves.toBeUndefined();

    expect(loader.getError("a")).toBe("boom");
    expect(loader.isLoaded("a")).toBe(false);
    expect(loader.isLoading("a")).toBe(false);
  });

  it("clears a previous error on a successful retry", async () => {
    const fetcher = vi
      .fn()
      .mockRejectedValueOnce(new Error("boom"))
      .mockImplementationOnce(async () => {});
    const loader = createTabLoader({ a: fetcher });

    await loader.load("a");
    expect(loader.getError("a")).toBe("boom");

    await loader.load("a", true);
    expect(loader.getError("a")).toBeNull();
    expect(loader.isLoaded("a")).toBe(true);
  });

  it("tracks state independently per tab", async () => {
    const fetcherA = vi.fn(async () => {});
    const fetcherB = vi.fn().mockRejectedValue(new Error("nope"));
    const loader = createTabLoader({ a: fetcherA, b: fetcherB });

    await Promise.all([loader.load("a"), loader.load("b")]);

    expect(loader.isLoaded("a")).toBe(true);
    expect(loader.getError("a")).toBeNull();

    expect(loader.isLoaded("b")).toBe(false);
    expect(loader.getError("b")).toBe("nope");
  });

  it("wraps non-Error throws with String()", async () => {
    const fetcher = vi.fn().mockRejectedValue("plain string failure");
    const loader = createTabLoader({ a: fetcher });

    await loader.load("a");

    expect(loader.getError("a")).toBe("plain string failure");
  });

  // Regression test for gopherstack-ks2s.7.
  //
  // A pure unit test on the `TabLoader` object can't reproduce this: the
  // defect is specifically about a template `{#if}` block being the
  // FIRST-EVER reader of a tab's `$state` object, at the moment the real
  // component mounts. `tab-loader.harness.svelte` reproduces the exact
  // shape every real page has: the fetch is kicked off from inside a plain
  // `$effect` (like `onRegionChange`), which runs after the component's
  // first render, and an inline error banner is gated behind
  // `{#if loader.getError(...)}`.
  //
  // Before the fix (lazy, on-first-access `$state` creation in `stateFor`),
  // this test fails: the error is written, but the `{#if}` block that was
  // the first-ever reader of that tab's (lazily-created) state never
  // re-renders, so the alert never appears and the test times out inside
  // `waitFor`. After eagerly creating every tab's `$state` object in
  // `createTabLoader` (so no template read is ever the first one), the
  // banner appears as soon as the error is written.
  it("shows an error banner written after this component's first render", async () => {
    const loader = createTabLoader({ a: vi.fn().mockRejectedValue(new Error("boom")) });

    render(TabLoaderHarness, { props: { loader } });

    await waitFor(() => {
      expect(screen.getByRole("alert")).toHaveTextContent("boom");
    });
  });
});

// Regression test for gopherstack-ks2s.9.
//
// Reuses `withEffectRoot` from region-effect.harness.svelte (the pattern
// established for testing runes that need an `$effect.root` owner) rather
// than mounting a full component, since this is about effect re-run counts,
// not template rendering.
describe("createTabLoader inside an $effect (double-fetch on write-back)", () => {
  beforeEach(() => {
    setStoredRegion(DEFAULT_REGION);
  });

  it("does not re-run a calling effect when the tab it refreshed finishes loading", async () => {
    let resolveFetch!: () => void;
    const fetcher = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          resolveFetch = resolve;
        }),
    );
    const loader = createTabLoader({ a: fetcher });

    // Touch the tab once, synchronously, before any effect runs — exactly
    // what every real page guarantees (this loader's own eager per-tab
    // `$state` creation now does it automatically; see the defect-1 fix
    // above). Doing it explicitly here isolates defect 2 from defect 1:
    // without this line, the effect below would be the very first-ever
    // reader of tab "a"'s lazily-created state on the *unfixed* primitive,
    // which trips defect 1 instead and masks defect 2 (a freshly
    // self-created object's reads don't track either, so the effect
    // wouldn't visibly double-run — for the wrong reason). With the state
    // already established up front, this test isolates and exercises only
    // defect 2's mechanism.
    loader.isLoading("a");

    let effectRuns = 0;
    // Mirrors detective/+page.svelte's real (now fixed) call site exactly:
    //   onRegionChange(() => { ...; tabLoader.refresh('graphs'); });
    // `onRegionChange`'s effect re-runs whenever a dependency it read
    // changes; the bug was that `refresh()` (via `load()`'s synchronous
    // `s.loaded` read) implicitly added the tab's own `loaded` flag as a
    // dependency of this effect, so the write to `loaded` once the fetch
    // resolved re-triggered it — double-fetching on every region change.
    const { cleanup } = withEffectRoot(() =>
      onRegionChange(() => {
        effectRuns++;
        void loader.refresh("a");
      }),
    );
    flushSync();

    expect(effectRuns).toBe(1);
    expect(fetcher).toHaveBeenCalledTimes(1);

    resolveFetch();
    await new Promise((r) => {
      setTimeout(r, 0);
    });
    flushSync();
    await new Promise((r) => {
      setTimeout(r, 0);
    });
    flushSync();

    expect(loader.isLoaded("a")).toBe(true);
    // Before the fix: writing `.loaded = true` re-triggers the effect above
    // (it implicitly depended on `.loaded`), calling `refresh()` again and
    // fetching a second time.
    expect(effectRuns).toBe(1);
    expect(fetcher).toHaveBeenCalledTimes(1);

    cleanup();
  });
});
