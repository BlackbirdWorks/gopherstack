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
//
// ## The same-tick multi-set() hazard
//
// `set()` computes its next URL as `new URL(page.url)` -- a snapshot of
// whatever `page.url` currently is -- and hands it to `goto()`. `goto()`
// is async and does NOT reassign `page.url` synchronously: reading
// `@sveltejs/kit`'s shipped client runtime, `goto()` awaits an internal
// `navigate()` call before it reaches the line that does
// `current.url = page.url = url`. That reassignment happens on a later
// microtask, well after `goto()`'s synchronous call returns.
//
// So calling `.set()` on two different urlState instances back to back in
// the same synchronous tick --
// `activeTabParam.set('overview'); selectedTableParam.set(name);` -- is
// broken: the second `set()`'s `new URL(page.url)` snapshot is taken
// BEFORE the first `set()`'s `goto()` has updated `page.url`, so it starts
// from the pre-first-write URL and clobbers the first write when its own
// `goto()` lands. `vitest.setup.ts`'s `goto` mock defers its
// `page.url` write by a microtask (`Promise.resolve().then(...)`) for
// exactly this reason: an earlier version of that mock applied the write
// synchronously, which accidentally serialized same-tick `.set()` calls
// (the first call's write landed before the second call's snapshot was
// taken) and made this bug invisible to every component test — see
// `routes/dynamodb/page.test.ts` for the end-to-end repro that the
// corrected, deferred mock now catches.
//
// The fix is `setUrlParams()` below: it builds ONE url from ONE snapshot
// of `page.url` and fires ONE `goto()`, so multiple param writes compose
// instead of racing. Any call site that needs to change more than one
// urlState value in the same handler MUST use `setUrlParams()` instead of
// multiple `.set()` calls -- see `+page.svelte`'s table-row click handler
// in `routes/dynamodb` for the first (and, as of this writing, only)
// caller.
import { page } from "$app/state";
import { goto } from "$app/navigation";
import { browser } from "$app/environment";

/**
 * A single param write, as produced by `UrlState.write()`. Opaque outside
 * this module — pass it straight into `setUrlParams()`.
 */
export type UrlWrite = { key: string; initial: string; value: string };

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
   *
   * Calling `set()` on more than one `UrlState` in the same synchronous
   * tick loses one of the writes — see the "same-tick multi-set() hazard"
   * section in this file's header. Use `setUrlParams()` instead whenever a
   * single handler needs to change more than one param.
   */
  set(value: T): void;
  /**
   * Builds a write-spec for `value` without navigating. Pass one or more
   * of these into `setUrlParams()` to apply them as a single `goto()`.
   */
  write(value: T): UrlWrite;
};

export function urlState<T extends string>(key: string, initial: T): UrlState<T> {
  function get(): T {
    const raw = page.url.searchParams.get(key);
    return raw === null ? initial : (raw as T);
  }

  function write(value: T): UrlWrite {
    return { key, initial, value };
  }

  function set(value: T): void {
    setUrlParams(write(value));
  }

  return { get, set, write };
}

/**
 * Applies one or more `UrlState.write()` results as a SINGLE `goto()`
 * navigation, all computed from the SAME snapshot of `page.url`. This is
 * the required way to change more than one urlState value from one event
 * handler — see the "same-tick multi-set() hazard" section in this file's
 * header for why chaining plain `.set()` calls silently drops all but the
 * last write.
 *
 * Example: a row click that both selects a table and resets the active
 * tab does `setUrlParams(selectedTableParam.write(name),
 * activeTabParam.write('overview'))` rather than two separate `.set()`
 * calls.
 *
 * No-op outside the browser (SSR / prerender), same as `set()`.
 */
export function setUrlParams(...writes: UrlWrite[]): void {
  if (!browser) return;

  const url = new URL(page.url);
  for (const { key, initial, value } of writes) {
    if (value === initial) {
      url.searchParams.delete(key);
    } else {
      url.searchParams.set(key, value);
    }
  }

  void goto(url, { replaceState: true, noScroll: true, keepFocus: true, state: page.state });
}
