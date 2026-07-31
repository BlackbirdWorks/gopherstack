// Shared per-tab data loader for service pages.
//
// Every existing service page refetches unconditionally on every
// `switchTab()` call and caches nothing. `createTabLoader` gives pages a
// single object that tracks loaded/loading/error state per tab, dedupes
// concurrent loads of the same tab (e.g. a fast double-click), and skips
// refetching a tab that has already loaded successfully unless the caller
// forces it.

import { untrack } from "svelte";

type TabState = {
  loaded: boolean;
  loading: boolean;
  error: string | null;
};

export type TabLoader<K extends string> = {
  /** Load a tab's data. No-op if already loaded, unless `force` is true. */
  load(tab: K, force?: boolean): Promise<void>;
  /** Force a reload of a tab regardless of whether it is already loaded. */
  refresh(tab: K): Promise<void>;
  isLoading(tab: K): boolean;
  getError(tab: K): string | null;
  isLoaded(tab: K): boolean;
};

function errorMessage(e: unknown): string {
  return e instanceof Error ? e.message : String(e);
}

export function createTabLoader<K extends string>(
  fetchers: Record<K, () => Promise<void>>,
): TabLoader<K> {
  const state = new Map<K, TabState>();
  const inFlight = new Map<K, Promise<void>>();

  // Every tab's `$state` object is created eagerly, right here, instead of
  // lazily the first time `stateFor` is asked for it. `fetchers`'s keys are
  // the complete, known-up-front set of valid tabs (K is inferred from
  // them), so this covers every tab any caller can legally ask `stateFor`
  // for.
  //
  // This isn't just tidiness: a template `{#if}`/conditional block whose
  // FIRST-EVER evaluation is also the moment a lazily-created `$state`
  // object comes into existence never picks up later writes to that
  // object — even though an ordinary `$effect` or a non-conditional
  // reactive expression reading that very same object updates correctly.
  // (Confirmed with a from-scratch repro with no Map and no tab-loader
  // involved at all: an eagerly-created `$state` read by an `{#if}` updates
  // fine; the identical object, lazily created inside that `{#if}`'s own
  // condition on first read, never does. See tab-loader.test.ts.) Every
  // real caller loads a tab from inside an effect that runs after the
  // component's first render (e.g. `onRegionChange`), so the page's own
  // `{#if getError(...)}` / `{#if isLoading(...)}` blocks are exactly the
  // kind of first reader that trips this. Pre-populating `state` here
  // means no template read is ever the one that creates an entry.
  for (const tab of Object.keys(fetchers) as K[]) {
    const created = $state<TabState>({ loaded: false, loading: false, error: null });
    state.set(tab, created);
  }

  function stateFor(tab: K): TabState {
    const existing = state.get(tab);
    if (existing) {
      return existing;
    }

    // Unreachable for any tab key that was present in `fetchers` at
    // construction time (all pre-populated above) — kept as a safety net
    // in case a caller ever manages to pass a `K` that wasn't.
    //
    // $state(...) is only valid as a variable declaration initializer, so
    // the reactive object is built here and then stored, rather than
    // assigned into an already-declared variable.
    const created = $state<TabState>({ loaded: false, loading: false, error: null });
    state.set(tab, created);
    return created;
  }

  async function run(tab: K): Promise<void> {
    const s = stateFor(tab);
    s.loading = true;
    s.error = null;

    try {
      await fetchers[tab]();
      s.loaded = true;
    } catch (e) {
      s.error = errorMessage(e);
    } finally {
      s.loading = false;
      inFlight.delete(tab);
    }
  }

  function load(tab: K, force = false): Promise<void> {
    const s = stateFor(tab);

    // `s.loaded` is only relevant to decide whether THIS call can skip
    // fetching — it must not become a reactive dependency of whatever
    // effect happens to be calling `load`/`refresh`. Reordering the
    // operands short-circuits `!force` before ever touching `s.loaded`
    // when forcing (so `refresh()` performs no reactive read at all), and
    // `untrack` keeps the remaining non-forced read from being tracked.
    // Without this, a caller like `onRegionChange(() => { ...;
    // tabLoader.refresh(tab); })` would implicitly depend on `.loaded`
    // (read by the `load(tab, true)` this delegates to), and the write to
    // `.loaded` once the fetch settles would re-trigger that whole effect,
    // double-fetching on every region change.
    if (!force && untrack(() => s.loaded)) {
      return Promise.resolve();
    }

    const existing = inFlight.get(tab);
    if (existing) {
      return existing;
    }

    const promise = run(tab);
    inFlight.set(tab, promise);
    return promise;
  }

  function refresh(tab: K): Promise<void> {
    return load(tab, true);
  }

  function isLoading(tab: K): boolean {
    return stateFor(tab).loading;
  }

  function getError(tab: K): string | null {
    return stateFor(tab).error;
  }

  function isLoaded(tab: K): boolean {
    return stateFor(tab).loaded;
  }

  return { load, refresh, isLoading, getError, isLoaded };
}
