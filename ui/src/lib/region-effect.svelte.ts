import { currentRegion } from "$lib/region.svelte";

/**
 * Runs `callback` immediately and again every time the active region
 * changes.
 *
 * Because a Svelte `$effect` always runs once when it is created (in
 * addition to whenever its dependencies change), this is a one-line,
 * top-level replacement for `onMount(loadData)`: call
 * `onRegionChange(loadData)` directly in the component script instead of
 * wrapping `loadData` in `onMount`. Do not use both — combining them will
 * run `loadData` twice on mount.
 */
export function onRegionChange(callback: () => void): void {
  $effect(() => {
    currentRegion();
    callback();
  });
}
