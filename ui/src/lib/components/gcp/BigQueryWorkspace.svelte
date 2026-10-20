<script lang="ts">
	import BigQueryQuery from '#lib/components/gcp/BigQueryQuery.svelte';
	import BigQueryLoad from '#lib/components/gcp/BigQueryLoad.svelte';
	import BigQueryTableDetail from '#lib/components/gcp/BigQueryTableDetail.svelte';

	export type Tab = 'query' | 'table' | 'load';

	let {
		project,
		datasetIds,
		selected,
		tab = $bindable<Tab>('query'),
		refresh,
		onchanged,
		onloaded
	}: {
		project: string;
		datasetIds: string[];
		selected: { dataset: string; table: string } | null;
		tab: Tab;
		refresh: number;
		onchanged: () => void;
		onloaded: (dataset: string, table: string) => void;
	} = $props();

	let sql = $state('SELECT 1 AS one');
	let defaultDataset = $state('');

	const tabs: { id: Tab; label: string }[] = [
		{ id: 'query', label: 'SQL query' },
		{ id: 'table', label: 'Table' },
		{ id: 'load', label: 'Load from GCS' }
	];

	const tabClass = (t: Tab) =>
		`px-4 py-2 text-sm font-medium border-b-2 ${tab === t ? 'border-blue-600 text-blue-700 dark:text-blue-400' : 'border-transparent text-gray-500 hover:text-gray-800 dark:hover:text-gray-200'}`;
</script>

<section aria-label="Workspace" class="min-w-0 space-y-4">
	<div class="flex border-b border-gray-200 dark:border-gray-700" role="tablist">
		{#each tabs as t (t.id)}
			<button role="tab" aria-selected={tab === t.id} class={tabClass(t.id)} onclick={() => (tab = t.id)} data-testid="bq-tab-{t.id}">{t.label}</button>
		{/each}
	</div>
	<div hidden={tab !== 'query'}><BigQueryQuery {project} datasets={datasetIds} bind:defaultDataset bind:sql {onchanged} /></div>
	{#if tab === 'table'}
		{#if selected}
			<BigQueryTableDetail {project} dataset={selected.dataset} table={selected.table} {refresh} />
		{:else}
			<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="bq-select-hint">Select a table in the explorer</p>
		{/if}
	{/if}
	{#if tab === 'load'}
		<BigQueryLoad {project} datasets={datasetIds} dataset={selected?.dataset ?? ''} {onloaded} />
	{/if}
</section>
