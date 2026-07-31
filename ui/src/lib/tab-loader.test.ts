import { describe, expect, it, vi } from "vitest";

import { createTabLoader } from "./tab-loader.svelte";

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
});
