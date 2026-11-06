<script lang="ts">
	import AppConfigFeatures from '#lib/components/azure/AppConfigFeatures.svelte';
	import AppConfigSnapshots from '#lib/components/azure/AppConfigSnapshots.svelte';
	import AppConfigRevisions from '#lib/components/azure/AppConfigRevisions.svelte';
	import AppConfigKeys from '#lib/components/azure/AppConfigKeys.svelte';

	let { store, tab }: { store: string; tab: string } = $props();
</script>

{#if tab === 'features'}
	<AppConfigFeatures {store} />
{:else if tab === 'snapshots'}
	<AppConfigSnapshots {store} />
{:else if tab === 'revisions'}
	<AppConfigRevisions {store} />
{:else}
	<AppConfigKeys {store} />
{/if}
