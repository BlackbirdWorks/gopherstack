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
// `set()` writes through SvelteKit's `goto()` with `replaceState: true` (not
// `$app/navigation`'s standalone `replaceState`, despite that being the more
// obvious-looking name for this job). Reading the actual shipped
// `@sveltejs/kit` client runtime settles which one to use: standalone
// `replaceState(url, state)` calls `history.replaceState(...)` — updating
// the address bar and `page.state` — but does NOT reassign `page.url`; only
// the navigation pipeline that `goto` drives does that
// (`current.url = page.url = url` in `@sveltejs/kit`'s client.js). Every page
// on this branch was originally wired to standalone `replaceState`, which
// left the address bar looking right while every `$derived` reading
// `activeTabParam.get()`/`searchQueryParam.get()` kept rendering the OLD
// value forever — invisible in component tests because their `$app/navigation`
// mock's fake `replaceState` (incorrectly) wrote straight into the mock's
// reactive `page.url`, masking the exact gap the real implementation leaves.
// `goto(url, { replaceState: true, noScroll: true, keepFocus: true, state })`
// gets the address-bar/`page.state` behavior of standalone `replaceState`
// (no new history entry, no scroll, no lost focus while typing a filter)
// while actually updating `page.url` too. This repo has no `+page.ts`/
// `+page.server.ts` load functions anywhere, so the navigation `goto` triggers
// has nothing to re-fetch — it is exactly as cheap as the standalone call.
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
import { goto } from "$app/navigation";
import { browser } from "$app/environment";

export type UrlState<T extends string> = {
  /** Current value: the URL's `key` param if present, else `initial`. */
  get(): T;
  /**
   * Writes `value` into the URL under `key` via `goto(url, { replaceState:
   * true, ... })` (no new history entry, no scroll, no lost focus — see the
   * file header for why this has to be `goto` and not standalone
   * `replaceState`). Setting `value === initial` removes the param instead
   * of writing it, so a page's default state never clutters a shared link.
   * Every other query parameter already present is preserved untouched.
   * No-op outside the browser (SSR / prerender), where there is no address
   * bar to update.
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

    void goto(url, { replaceState: true, noScroll: true, keepFocus: true, state: page.state });
  }

  return { get, set };
}
