<script lang="ts">
  // Test-only fixture for tab-loader.test.ts's defect-1 regression test.
  //
  // This mirrors the shape every real page using createTabLoader has: the
  // load is kicked off from inside a plain `$effect` (like
  // `onRegionChange`), which runs *after* the component's first render —
  // and the template reads `getError`/`isLoading` through `{#if}` blocks,
  // exactly like a page's inline error banner and loading indicator. A pure
  // unit test on the `TabLoader` object alone can't exercise this: the bug
  // is specifically about whether a template's conditional block is the
  // first-ever reader of a tab's (possibly lazily-created) `$state` object.
  import type { TabLoader } from "./tab-loader.svelte";

  type Props = {
    loader: TabLoader<"a">;
  };
  let { loader }: Props = $props();

  $effect(() => {
    void loader.load("a");
  });
</script>

{#if loader.getError("a")}
  <p role="alert">{loader.getError("a")}</p>
{/if}
{#if loader.isLoading("a")}
  <p>Loading…</p>
{/if}
