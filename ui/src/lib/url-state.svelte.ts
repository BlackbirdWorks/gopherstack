// Reusable primitive that binds a single query-string parameter to a
// rune-reactive get/set pair, so tab/filter/search state survives a
// refresh and is linkable — something none of the 161 routes did before
// this file existed.
//
// The URL (`page.url`, from `$app/state` — this repo is Svelte 5 runes
// mode, not the older `$app/stores` API) is the single source of truth.
// `get()` re-reads `page.url.searchParams` on every call, so reading it
// inside a `$derived`/`$effect` subscribes that derivation to `page.url`
// exactly like reading any other rune — no separate mirrored `$state` to
// keep in sync or drift from the address bar.
//
// `set()` writes through SvelteKit's `replaceState` (not `goto`/`pushState`),
// so choosing a tab or typing a filter never pushes a history entry or
// triggers a navigation/load — only the address bar and `page.url` change.
// Back/forward still works exactly as before: it wasn't touched.
//
// ## The region-effect hazard
//
// `region-effect.svelte.ts`'s `onRegionChange(callback)` is a bare
// `$effect` that tracks every reactive read `callback` makes while it runs.
// Upward of 25 pages already wrap tab/filter `$state` reads in `untrack()`
// inside that callback for exactly this reason: an untracked read doesn't
// make the region effect re-fire when that state changes, so switching tabs
// doesn't double-fetch.
//
// `urlState` doesn't change this hazard, but it does widen its blast
// radius: every key this primitive manages reads the SAME shared
// `page.url` object. If an `onRegionChange` callback reads any urlState
// value without `untrack()`, that effect subscribes to `page.url` as a
// whole — so calling `.set()` on a *different* key, anywhere else on the
// page, would also re-trigger it. The fix is identical to the existing
// pattern: wrap the read in `untrack()`. See `url-state.test.ts` for a
// from-scratch repro (a fake `onRegionChange`-shaped effect that reads a
// urlState value both with and without `untrack`) proving the untracked
// read does not re-fire when a sibling key is written, while the tracked
// read does.
import { page } from "$app/state";
import { replaceState } from "$app/navigation";
import { browser } from "$app/environment";

export type UrlState<T extends string> = {
  /** Current value: the URL's `key` param if present, else `initial`. */
  get(): T;
  /**
   * Writes `value` into the URL under `key` via `replaceState` (no
   * navigation, no scroll, no history entry). Setting `value === initial`
   * removes the param instead of writing it, so a page's default state
   * never clutters a shared link. Every other query parameter already
   * present is preserved untouched. No-op outside the browser (SSR /
   * prerender), where there is no address bar to update.
   */
  set(value: T): void;
};

export function urlState<T extends string>(key: string, initial: T): UrlState<T> {
  function get(): T {
    const raw = page.url.searchParams.get(key);
    return raw === null ? initial : (raw as T);
  }

  function set(value: T): void {
    if (!browser) return;

    const url = new URL(page.url);
    if (value === initial) {
      url.searchParams.delete(key);
    } else {
      url.searchParams.set(key, value);
    }

    replaceState(url, page.state);
  }

  return { get, set };
}
