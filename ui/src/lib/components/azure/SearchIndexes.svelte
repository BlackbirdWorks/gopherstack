<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw } from 'lucide-svelte';
	import JsonView from './JsonView.svelte';
	import { GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { fieldFlags, formatBytes, getIndexStats, listIndexes, type IndexDef, type IndexStats, type SearchTarget } from '#lib/azuresearch.ts';

	let { target }: { target: SearchTarget } = $props();

	let indexes = $state<IndexDef[]>([]);
	let stats = $state<Record<string, IndexStats>>({});
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let showRaw = $state(false);

	const index = $derived(indexes.find((i) => i.name === selected));

	async function load() {
		try {
			indexes = await listIndexes(target);
			const entries = await Promise.all(indexes.map(async (i) => [i.name, await getIndexStats(target, i.name)] as const));
			stats = Object.fromEntries(entries);
			error = '';
			if (!indexes.some((i) => i.name === selected)) selected = indexes[0]?.name ?? '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	onMount(load);

	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
</script>

<div class="space-y-4" data-testid="search-indexes">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="search-indexes-error">Could not load indexes: {error}</p>{/if}
	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="search-index-table">
				<thead class={thead}><tr><th class={cell}>Index</th><th class={cell}>Documents</th><th class={cell}>Storage</th><th class={cell}>Fields</th><th class={cell}>Features</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each indexes as i (i.name)}
						<tr class={selected === i.name ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="search-index">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => (selected = i.name)} data-testid="search-index-select">{i.name}</button></td>
							<td class="{cell} text-xs" data-testid="search-index-docs">{stats[i.name]?.documents ?? '—'}</td>
							<td class="{cell} text-xs" data-testid="search-index-storage">{stats[i.name] ? formatBytes(stats[i.name].storage) : '—'}</td>
							<td class="{cell} text-xs">{i.fields.length}</td>
							<td class="{cell} text-xs">{[i.suggesters.length > 0 ? 'suggesters' : '', i.scoringProfiles.length > 0 ? 'scoring profiles' : '', i.vector ? 'vector' : '', i.semantic ? 'semantic' : ''].filter(Boolean).join(', ') || '—'}</td>
						</tr>
					{:else}
						<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="search-indexes-empty">No indexes on this service.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if index}
			<section class="space-y-2" data-testid="search-schema">
				<div class="flex items-center justify-between">
					<h3 class="text-sm font-semibold">Fields of {index.name}</h3>
					<button class={GCP_MINI} onclick={() => (showRaw = !showRaw)} data-testid="search-schema-raw-toggle">{showRaw ? 'Show table' : 'Show JSON'}</button>
				</div>
				{#if showRaw}
					<JsonView value={index.raw} testid="search-schema-json" />
				{:else}
					<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
						<table class="w-full text-sm">
							<thead class={thead}><tr><th class={cell}>Field</th><th class={cell}>Type</th><th class={cell}>Attributes</th><th class={cell}>Analyzer</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each index.fields as f (f.name)}
									<tr data-testid="search-field">
										<td class="{cell} font-mono text-xs" data-testid="search-field-name">{f.name}</td>
										<td class="{cell} font-mono text-xs">{f.type}{f.dimensions > 0 ? ` (${f.dimensions})` : ''}</td>
										<td class="{cell} text-xs">{fieldFlags(f).join(', ') || '—'}</td>
										<td class="{cell} text-xs">{f.analyzer || '—'}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</section>
		{/if}
	{/if}
</div>
