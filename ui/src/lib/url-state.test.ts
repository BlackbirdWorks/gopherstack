import { flushSync, untrack } from "svelte";
import { describe, expect, it, vi, beforeEach } from "vitest";
import { goto, replaceState } from "$app/navigation";
import { withEffectRoot } from "./region-effect.harness.svelte";
import { getMockPage, setMockPageUrl, resetMockPage, fakeRegionEffect } from "./mock-page.svelte";

// $app/state and $app/navigation are already wired globally (vitest.setup.ts)
// to this same mock-page module, with a REAL reactive `page.url` (see
// mock-page.svelte.ts for why that matters). Only $app/environment needs a
// local override here, to flip `browser` for the SSR-safety test below —
// this vi.mock is hoisted above the `urlState` import by Vitest, same as
// every other mock in this file.
let browserFlag = true;
vi.mock("$app/environment", () => ({
  get browser() {
    return browserFlag;
  },
}));

// Imported statically, same reasoning as region-effect.test.ts: $effect
// needs the same running copy of svelte's internal runtime that created the
// enclosing $effect.root from withEffectRoot, so this can't go through
// vi.resetModules() + dynamic import.
import { urlState, setUrlParams } from "./url-state.svelte";

// The `goto` mock (vitest.setup.ts) defers its `page.url` write by one
// microtask, matching real `@sveltejs/kit` timing (see
// url-state.svelte.ts's "same-tick multi-set() hazard" section for why
// that deferral matters and is deliberate, not a mock bug). Any assertion
// against `page.url` after a `.set()`/`setUrlParams()` call must await
// this once first, or it observes the pre-write snapshot.
async function flushGoto(): Promise<void> {
  await Promise.resolve();
}

describe("urlState", () => {
  beforeEach(() => {
    resetMockPage();
    setMockPageUrl(new URL("http://localhost/s3"));
    browserFlag = true;
  });

  it("reads the initial value from the URL when the param is present", () => {
    setMockPageUrl(new URL("http://localhost/s3?tab=properties"));
    const tab = urlState<"objects" | "properties">("tab", "objects");
    expect(tab.get()).toBe("properties");
  });

  it("falls back to the supplied default when the param is absent", () => {
    const tab = urlState<"objects" | "properties">("tab", "objects");
    expect(tab.get()).toBe("objects");
  });

  it("writes the new value into the URL via goto({ replaceState: true }), not standalone replaceState", async () => {
    const tab = urlState<"objects" | "properties">("tab", "objects");
    tab.set("properties");
    await flushGoto();

    expect(getMockPage().url.searchParams.get("tab")).toBe("properties");
    // The value is reflected back on the next read.
    expect(tab.get()).toBe("properties");

    // Standalone `replaceState` from `$app/navigation` looks like the more
    // obvious tool for this job, but the real `@sveltejs/kit` client runtime
    // never reassigns `page.url` from it — only the navigation pipeline
    // `goto` drives does. Assert the call went through `goto`, configured so
    // it behaves like a replace (no new history entry, no scroll, no lost
    // focus), and that standalone `replaceState` was never touched.
    expect(vi.mocked(goto)).toHaveBeenCalledWith(
      expect.any(URL),
      expect.objectContaining({ replaceState: true, noScroll: true, keepFocus: true }),
    );
    expect(vi.mocked(replaceState)).not.toHaveBeenCalled();
  });

  it("removes the param when set back to the initial value, keeping shared links clean", async () => {
    setMockPageUrl(new URL("http://localhost/s3?tab=properties"));
    const tab = urlState<"objects" | "properties">("tab", "objects");
    tab.set("objects");
    await flushGoto();

    expect(tab.get()).toBe("objects");
    expect(getMockPage().url.searchParams.has("tab")).toBe(false);
  });

  it("does not clobber other query parameters already present", async () => {
    setMockPageUrl(new URL("http://localhost/s3?region=us-west-2&tab=objects"));
    const tab = urlState<"objects" | "properties">("tab", "objects");
    tab.set("properties");
    await flushGoto();

    expect(getMockPage().url.searchParams.get("region")).toBe("us-west-2");
    expect(getMockPage().url.searchParams.get("tab")).toBe("properties");
  });

  it("is a no-op during SSR/prerender, where there is no window/history to update", () => {
    browserFlag = false;
    const before = getMockPage().url.toString();
    const tab = urlState<"objects" | "properties">("tab", "objects");
    tab.set("properties");

    expect(getMockPage().url.toString()).toBe(before);
    // get() still works during SSR: page.url is populated from the request.
    expect(tab.get()).toBe("objects");
  });

  it("reading two different keys off the same URL does not interfere", async () => {
    setMockPageUrl(new URL("http://localhost/s3?tab=properties&q=alpha"));
    const tab = urlState<"objects" | "properties">("tab", "objects");
    const q = urlState<string>("q", "");

    expect(tab.get()).toBe("properties");
    expect(q.get()).toBe("alpha");

    q.set("beta");
    await flushGoto();
    expect(tab.get()).toBe("properties");
    expect(q.get()).toBe("beta");
  });
});

// setUrlParams() batches multiple urlState writes into ONE goto() call
// against ONE snapshot of page.url, so callers that need to change more
// than one param in a single handler don't hit the same-tick multi-set()
// clobber documented in this file's header (and reproduced end-to-end,
// against a page component, in routes/dynamodb/page.test.ts).
describe("setUrlParams", () => {
  beforeEach(() => {
    resetMockPage();
    setMockPageUrl(new URL("http://localhost/dynamodb?tab=pitr"));
    browserFlag = true;
    vi.mocked(goto).mockClear();
  });

  it("applies multiple writes via a single goto() call, composing rather than racing", async () => {
    const tab = urlState<string>("tab", "overview");
    const table = urlState<string>("table", "");

    setUrlParams(tab.write("overview"), table.write("Orders"));
    await flushGoto();

    // Exactly one navigation for both writes.
    expect(vi.mocked(goto)).toHaveBeenCalledTimes(1);
    // "overview" is tab's initial value, so the param is removed rather
    // than written -- proving both writes were computed against the SAME
    // starting URL, not two independent set()-style snapshots.
    expect(getMockPage().url.searchParams.get("tab")).toBeNull();
    expect(getMockPage().url.searchParams.get("table")).toBe("Orders");
  });

  it("is a no-op during SSR/prerender, same as set()", () => {
    browserFlag = false;
    const before = getMockPage().url.toString();
    const tab = urlState<string>("tab", "overview");
    const table = urlState<string>("table", "");

    setUrlParams(tab.write("overview"), table.write("Orders"));

    expect(getMockPage().url.toString()).toBe(before);
  });
});

// ---------------------------------------------------------------------
// The region-effect hazard (see the header comment in url-state.svelte.ts):
// an onRegionChange-shaped $effect that reads a urlState value WITHOUT
// untrack() re-fires when a *different* urlState key is written, because
// both reads go through the same shared, reactive `page.url` — a write to
// ANY key replaces it. Wrapping the read in untrack() must prevent that.
// This is a from-scratch repro of exactly that failure mode, using the
// same withEffectRoot harness region-effect.test.ts uses to exercise real
// $effects outside a component, against a page.url mock that is a real
// rune (mock-page.svelte.ts) so the reactivity is genuine, not simulated.
// ---------------------------------------------------------------------
describe("urlState read inside an onRegionChange-shaped effect", () => {
  beforeEach(() => {
    resetMockPage();
    setMockPageUrl(new URL("http://localhost/s3"));
    browserFlag = true;
  });

  it("a tracked read re-fires the effect when a sibling key is written", async () => {
    const tab = urlState<"objects" | "properties">("tab", "objects");
    const q = urlState<string>("q", "");
    let runs = 0;

    const { cleanup } = withEffectRoot(() =>
      fakeRegionEffect(() => {
        // Tracked: NOT wrapped in untrack().
        tab.get();
        runs++;
      }),
    );
    flushSync();
    expect(runs).toBe(1);

    // A different key, but the same shared page.url. set()'s goto() write
    // lands on a microtask (see flushGoto's doc comment), so the reactive
    // re-run it triggers doesn't happen until after that write lands —
    // await it before the second flushSync().
    q.set("something");
    await flushGoto();
    flushSync();
    // Re-fired — this is the hazard.
    expect(runs).toBe(2);

    cleanup();
  });

  it("untrack() prevents the same effect from re-firing on a sibling-key write", async () => {
    const tab = urlState<"objects" | "properties">("tab", "objects");
    const q = urlState<string>("q", "");
    let runs = 0;

    const { cleanup } = withEffectRoot(() =>
      fakeRegionEffect(() => {
        untrack(() => tab.get());
        runs++;
      }),
    );
    flushSync();
    expect(runs).toBe(1);

    q.set("something-else");
    await flushGoto();
    flushSync();
    // Untracked read: no re-fire, even once the write has actually landed.
    expect(runs).toBe(1);

    cleanup();
  });
});
