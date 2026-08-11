<script lang="ts">
	// Writes always target DEFAULT_REGION -- there is no backend wildcard
	// region a create call could target under "All". This hint makes that
	// implicit targeting visible next to a create action, but only when it's
	// non-obvious (i.e. "All" is selected); a specific-region selection
	// already makes the target obvious from the region picker itself, so
	// this renders nothing then.
	import { DEFAULT_REGION, isAllRegions } from '$lib/region.svelte';
</script>

{#if isAllRegions()}
	<span
		class="text-xs text-slate-500 dark:text-slate-400 font-mono"
		data-testid="write-region-hint"
	>
		using {DEFAULT_REGION}
	</span>
{/if}
