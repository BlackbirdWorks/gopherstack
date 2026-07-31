// Shared per-tab data loader for service pages.
//
// Every existing service page refetches unconditionally on every
// `switchTab()` call and caches nothing. `createTabLoader` gives pages a
// single object that tracks loaded/loading/error state per tab, dedupes
// concurrent loads of the same tab (e.g. a fast double-click), and skips
// refetching a tab that has already loaded successfully unless the caller
// forces it.

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

  function stateFor(tab: K): TabState {
    const existing = state.get(tab);
    if (existing) {
      return existing;
    }

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

    if (s.loaded && !force) {
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
