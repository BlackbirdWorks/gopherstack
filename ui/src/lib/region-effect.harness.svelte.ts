// Test-only harness. `$effect` (used by `onRegionChange`) needs an active
// reactive owner — a component instance, or an `$effect.root` — to run at
// all; a plain `.test.ts` file cannot create one directly, because rune
// syntax is only compiled inside `.svelte` / `.svelte.js` / `.svelte.ts`
// files. This wraps `fn` in an `$effect.root` so `region-effect.test.ts`
// can exercise `onRegionChange` / `regionalClient` the same way a real
// `+page.svelte` component's `<script>` would, without standing up a full
// component via `@testing-library/svelte` for what are plain reactive
// primitives.
export function withEffectRoot<T>(fn: () => T): { result: T; cleanup: () => void } {
  let result!: T;
  const cleanup = $effect.root(() => {
    result = fn();
  });
  return { result, cleanup };
}
