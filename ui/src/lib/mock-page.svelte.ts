// Test-only. Backs the GLOBAL `$app/state` / `$app/navigation` mocks in
// vitest.setup.ts, and is reused directly by url-state.test.ts for
// finer-grained assertions.
//
// `page.url` has to be backed by a REAL Svelte rune, not a plain mutable
// object: `$lib/url-state.svelte.ts`'s `urlState()` reads `page.url` inside
// `$derived`s that page components create (e.g. `let searchQuery =
// $derived(searchQueryParam.get())`), and only a rune notifies Svelte's
// reactivity when it changes. A plain object mutated in place is invisible
// to that machinery — a component would render its value once and never
// update, exactly like `region.svelte.ts`'s `region` needing to be
// `$state`, not a plain variable, for `onRegionChange` to see it change.
// `$state` is compiled rune syntax, only available inside `.svelte` /
// `.svelte.js` / `.svelte.ts` files — hence this being its own file rather
// than living directly in vitest.setup.ts (a plain `.ts` file).
let mockUrl = $state<URL>(new URL("http://localhost/"));
let mockPageState = $state<Record<string, unknown>>({});

// Unchanged from the object literal vitest.setup.ts used before this file
// existed — kept as a plain (non-reactive) object since nothing needs
// `page.params` to update mid-test.
const mockParams: Record<string, string> = { tableName: "test-table", bucketName: "test-bucket" };

export function getMockPage(): {
  params: Record<string, string>;
  url: URL;
  state: Record<string, unknown>;
} {
  return { params: mockParams, url: mockUrl, state: mockPageState };
}

export function setMockPageUrl(url: URL): void {
  mockUrl = url;
}

export function setMockPageState(state: Record<string, unknown>): void {
  mockPageState = state;
}

export function resetMockPage(): void {
  mockUrl = new URL("http://localhost/");
  mockPageState = {};
}

/**
 * Shaped exactly like `onRegionChange`'s body
 * (`$effect(() => { ...; callback(); })`), so `url-state.test.ts` can prove
 * the region-effect hazard `url-state.svelte.ts` documents — and
 * untrack()'s fix for it — against a real reactive effect, without
 * standing up a whole page component.
 */
export function fakeRegionEffect(read: () => void): void {
  $effect(() => {
    read();
  });
}
