<script lang="ts">
	import { onMount } from 'svelte';
	import { GCP_INPUT, GCP_GHOST } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listRevisions, type AcKeyValue } from '#lib/azureappconfig.ts';

	let { store }: { store: string } = $props();

	let keyFilter = $state('');
	let items = $state<AcKeyValue[]>([]);
	let loading = $state(true);
	let error = $state('');

	async function load() {
		try {
			items = await listRevisions(store, keyFilter.trim(), '');
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	onMount(() => void load());
</script>

<div class="space-y-3" data-testid="appconfig-revisions">
	<form class="flex items-end gap-3" onsubmit={(e) => { e.preventDefault(); void load(); }}>
		<label class="text-sm">Key filter<input class={GCP_INPUT} bind:value={keyFilter} placeholder="e.g. app/*" data-testid="appconfig-rev-filter" /></label>
		<button type="submit" class={GCP_GHOST}>Filter</button>
	</form>
	{#if error}
		<p class="text-sm text-red-600" role="alert">{error}</p>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Modified</th><th class="px-4 py-2">Key</th><th class="px-4 py-2">Label</th><th class="px-4 py-2">Value</th><th class="px-4 py-2">ETag</th></tr></thead>
				<tbody>
					{#each items as kv, i (i)}
						<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="appconfig-revision">
							<td class="px-4 py-2 text-xs">{kv.lastModified}</td>
							<td class="px-4 py-2 font-mono">{kv.key}</td>
							<td class="px-4 py-2">{kv.label || '(none)'}</td>
							<td class="max-w-xs truncate px-4 py-2 font-mono" title={kv.value}>{kv.value}</td>
							<td class="px-4 py-2 font-mono text-xs">{kv.etag}</td>
						</tr>
					{:else}
						<tr><td class="px-4 py-6 text-center text-gray-500" colspan="5">No revisions recorded.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
