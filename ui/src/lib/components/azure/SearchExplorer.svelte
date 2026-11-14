<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw } from 'lucide-svelte';
	import JsonView from './JsonView.svelte';
	import { GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		emptyQuery,
		listIndexes,
		runSearch,
		validateQuery,
		type IndexDef,
		type QueryForm,
		type SearchResult,
		type SearchTarget
	} from '#lib/azuresearch.ts';

	let { target }: { target: SearchTarget } = $props();

	let indexes = $state<IndexDef[]>([]);
	let index = $state('');
	let form = $state<QueryForm>({ ...emptyQuery });
	let result = $state<SearchResult>();
	let loading = $state(true);
	let running = $state(false);
	let error = $state('');
	let view = $state<'results' | 'json'>('results');

	const invalid = $derived(validateQuery(form));
	const def = $derived(indexes.find((i) => i.name === index));

	async function load() {
		try {
			indexes = await listIndexes(target);
			if (!indexes.some((i) => i.name === index)) index = indexes[0]?.name ?? '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function run() {
		if (!index || invalid) return;
		running = true;
		try {
			result = await runSearch(target, index, form);
			error = '';
		} catch (e) {
			result = undefined;
			error = describeGcpError(e);
		} finally {
			running = false;
		}
	}

	onMount(load);

	const label = 'block text-xs text-gray-500';
	const mono = `${GCP_INPUT} mt-1 !py-1 font-mono text-xs`;
</script>

<div class="space-y-4" data-testid="search-explorer">
	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if indexes.length === 0}
		<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="search-explorer-empty">Create an index to query it here.</p>
	{:else}
		<form class="grid gap-3 md:grid-cols-2" onsubmit={(e) => { e.preventDefault(); void run(); }}>
			<label class={label}>Index
				<select class={mono} bind:value={index} data-testid="search-explorer-index">
					{#each indexes as i (i.name)}<option value={i.name}>{i.name}</option>{/each}
				</select>
			</label>
			<label class={label}>Search text
				<input class={mono} bind:value={form.search} placeholder="*" data-testid="search-explorer-search" />
			</label>
			<label class={label}>$filter
				<input class={mono} bind:value={form.filter} placeholder="rating ge 4 and category eq 'hotel'" data-testid="search-explorer-filter" />
			</label>
			<label class={label}>$orderby
				<input class={mono} bind:value={form.orderby} placeholder="rating desc" data-testid="search-explorer-orderby" />
			</label>
			<label class={label}>$select
				<input class={mono} bind:value={form.select} placeholder={def ? def.fields.slice(0, 3).map((f) => f.name).join(',') : ''} data-testid="search-explorer-select" />
			</label>
			<div class="grid grid-cols-2 gap-3">
				<label class={label}>$top
					<input class={mono} bind:value={form.top} inputmode="numeric" data-testid="search-explorer-top" />
				</label>
				<label class={label}>$skip
					<input class={mono} bind:value={form.skip} inputmode="numeric" data-testid="search-explorer-skip" />
				</label>
			</div>
			<label class={label}>Facets (one per line)
				<textarea class={mono} rows="2" bind:value={form.facets} placeholder={'category\nrating,interval:1'} data-testid="search-explorer-facets"></textarea>
			</label>
			<div class="flex flex-wrap items-end gap-4 text-xs text-gray-600 dark:text-gray-300">
				<label>Query type
					<select class={mono} bind:value={form.queryType} data-testid="search-explorer-querytype"><option value="simple">simple</option><option value="full">full (Lucene)</option></select>
				</label>
				<label>Search mode
					<select class={mono} bind:value={form.searchMode}><option value="any">any</option><option value="all">all</option></select>
				</label>
				<label class="flex items-center gap-1"><input type="checkbox" bind:checked={form.count} /> Count</label>
			</div>
			<div class="flex items-center gap-3 md:col-span-2">
				<button type="submit" class={GCP_PRIMARY} disabled={running || invalid !== ''} data-testid="search-explorer-run">{running ? 'Searching…' : 'Search'}</button>
				{#if invalid}<span class="text-xs text-red-600" data-testid="search-explorer-invalid">{invalid}</span>{/if}
			</div>
		</form>

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="search-explorer-error">{error}</p>{/if}

		{#if result}
			<section class="space-y-3" data-testid="search-results">
				<div class="flex flex-wrap items-center justify-between gap-2 text-xs text-gray-500">
					<p><span data-testid="search-result-count">{result.count ?? result.rows.length}</span> result{(result.count ?? result.rows.length) === 1 ? '' : 's'} · {result.rows.length} shown · {result.elapsedMs} ms</p>
					<div class="flex gap-1">
						<button class={GCP_MINI} aria-pressed={view === 'results'} onclick={() => (view = 'results')}>Results</button>
						<button class={GCP_MINI} aria-pressed={view === 'json'} onclick={() => (view = 'json')} data-testid="search-view-json">JSON</button>
					</div>
				</div>

				{#if result.facets.length > 0}
					<div class="grid gap-3 md:grid-cols-3" data-testid="search-facets">
						{#each result.facets as g (g.field)}
							<div class="rounded-md border border-gray-200 p-3 dark:border-gray-700" data-testid="search-facet">
								<h4 class="text-xs font-semibold" data-testid="search-facet-field">{g.field}</h4>
								<ul class="mt-1 space-y-0.5 text-xs">
									{#each g.values as v (v.label)}
										<li class="flex justify-between gap-2" data-testid="search-facet-value"><span>{v.label}</span><span class="font-mono">{v.count}</span></li>
									{/each}
								</ul>
							</div>
						{/each}
					</div>
				{/if}

				{#if view === 'json'}
					<JsonView value={result.raw} testid="search-result-json" />
				{:else if result.rows.length === 0}
					<p class="py-4 text-center text-gray-500" data-testid="search-no-results">No documents matched.</p>
				{:else}
					<ul class="space-y-2" data-testid="search-rows">
						{#each result.rows as row, i (i)}
							<li class="rounded-md border border-gray-200 p-2 dark:border-gray-700" data-testid="search-row"><JsonView value={row} testid="search-row-json" /></li>
						{/each}
					</ul>
				{/if}
			</section>
		{/if}
	{/if}
</div>
