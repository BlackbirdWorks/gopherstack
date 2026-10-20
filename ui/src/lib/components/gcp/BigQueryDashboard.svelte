<script lang="ts">
	import { Database, Plus } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import BigQueryExplorer from '#lib/components/gcp/BigQueryExplorer.svelte';
	import BigQueryWorkspace from '#lib/components/gcp/BigQueryWorkspace.svelte';
	import { readStoredBqProject, writeStoredBqProject } from '#lib/gcpbigquery.ts';

	let project = $state(readStoredBqProject());
	let projectInput = $state(readStoredBqProject());
	let selected = $state<{ dataset: string; table: string } | null>(null);
	let tab = $state<'query' | 'table' | 'load'>('query');
	let datasetIds = $state<string[]>([]);
	let loadError = $state('');
	let refresh = $state(0);
	let explorer = $state<BigQueryExplorer | null>(null);

	function select(dataset: string, table: string) {
		selected = { dataset, table };
		tab = 'table';
		refresh += 1;
	}

	function removed(dataset: string, table: string) {
		if (selected?.dataset === dataset && (!table || selected.table === table)) selected = null;
	}

	function applyProject(event: Event) {
		event.preventDefault();
		const next = projectInput.trim();
		if (!next) return;
		project = next;
		writeStoredBqProject(next);
		selected = null;
		explorer?.reset();
	}

	async function afterQuery() {
		await explorer?.reload();
		refresh += 1;
	}
</script>

<div class="space-y-6 pb-20" data-testid="bq-dashboard">
	<PageHeader icon={Database} title="BigQuery" description="Google BigQuery: datasets, tables and SQL" color="blue" onRefresh={() => explorer?.reload()}>
		{#snippet actions()}
			<button class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" onclick={() => explorer?.openDataset()} disabled={Boolean(loadError)} data-testid="bq-new-dataset">
				<Plus class="h-4 w-4" /> New dataset
			</button>
		{/snippet}
	</PageHeader>

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyProject}>
		<label for="bq-project">Project</label>
		<input id="bq-project" class="w-56 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={projectInput} data-testid="bq-project" />
		<button type="submit" class="rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={!projectInput.trim() || projectInput.trim() === project}>Switch</button>
	</form>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="bq-error">
			<p class="font-medium">Could not reach BigQuery</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={() => explorer?.reload()}>Retry</button>
		</div>
	{/if}

	<div class="grid gap-6 lg:grid-cols-[19rem_1fr]" hidden={Boolean(loadError)}>
		<BigQueryExplorer bind:this={explorer} {project} {selected} bind:datasetIds bind:loadError onselect={select} onremoved={removed} />
		<BigQueryWorkspace {project} {datasetIds} {selected} bind:tab {refresh} onchanged={afterQuery} onloaded={(d, t) => explorer?.show(d, t)} />
	</div>
</div>
