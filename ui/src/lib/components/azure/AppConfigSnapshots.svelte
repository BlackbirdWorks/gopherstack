<script lang="ts">
	import { onMount } from 'svelte';
	import { GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listSnapshotItems, listSnapshots, type AcKeyValue, type AcSnapshot } from '#lib/azureappconfig.ts';

	let { store }: { store: string } = $props();

	let snaps = $state<AcSnapshot[]>([]);
	let open = $state('');
	let items = $state<AcKeyValue[]>([]);
	let loading = $state(true);
	let error = $state('');

	async function load() {
		try {
			snaps = await listSnapshots(store);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function view(name: string) {
		if (open === name) {
			open = '';
			return;
		}
		try {
			items = await listSnapshotItems(store, name);
			open = name;
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	onMount(() => void load());
</script>

<div class="space-y-3" data-testid="appconfig-snapshots">
	{#if error}
		<p class="text-sm text-red-600" role="alert">{error}</p>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Name</th><th class="px-4 py-2">Status</th><th class="px-4 py-2">Composition</th><th class="px-4 py-2">Items</th><th class="px-4 py-2">Filters</th><th class="px-4 py-2">Created</th><th class="px-4 py-2"></th></tr></thead>
				<tbody>
					{#each snaps as s (s.name)}
						<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="appconfig-snapshot">
							<td class="px-4 py-2 font-mono">{s.name}</td>
							<td class="px-4 py-2">{s.status}</td>
							<td class="px-4 py-2">{s.compositionType}</td>
							<td class="px-4 py-2">{s.itemCount}</td>
							<td class="px-4 py-2 font-mono text-xs">{s.filters.join('; ')}</td>
							<td class="px-4 py-2 text-xs">{s.created}</td>
							<td class="px-4 py-2 text-right"><button class={GCP_MINI} onclick={() => view(s.name)} data-testid="appconfig-snapshot-view">{open === s.name ? 'Hide' : 'View items'}</button></td>
						</tr>
					{:else}
						<tr><td class="px-4 py-6 text-center text-gray-500" colspan="7" data-testid="appconfig-snapshots-empty">No snapshots. Create them with the Azure CLI or SDK.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
		{#if open}
			<table class="w-full text-sm" data-testid="appconfig-snapshot-items">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Key</th><th class="px-4 py-2">Label</th><th class="px-4 py-2">Value</th></tr></thead>
				<tbody>
					{#each items as kv, i (i)}
						<tr class="border-t border-gray-100 dark:border-gray-800"><td class="px-4 py-2 font-mono">{kv.key}</td><td class="px-4 py-2">{kv.label || '(none)'}</td><td class="max-w-xs truncate px-4 py-2 font-mono" title={kv.value}>{kv.value}</td></tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{/if}
</div>
