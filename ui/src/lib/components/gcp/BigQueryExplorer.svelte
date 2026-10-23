<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import Modal from '#lib/components/Modal.svelte';
	import BigQueryTree from '#lib/components/gcp/BigQueryTree.svelte';
	import BigQueryTableForm from '#lib/components/gcp/BigQueryTableForm.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { GCP_GHOST, GCP_INPUT, GCP_PRIMARY, attempt } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		createDataset,
		createTable,
		deleteDataset,
		deleteTable,
		listDatasets,
		listTables,
		validateBqId,
		type DatasetInfo,
		type SchemaField,
		type TableInfo
	} from '#lib/gcpbigquery.ts';

	type Selected = { dataset: string; table: string } | null;

	let {
		project,
		selected,
		datasetIds = $bindable<string[]>([]),
		loadError = $bindable(''),
		onselect,
		onremoved
	}: {
		project: string;
		selected: Selected;
		datasetIds: string[];
		loadError: string;
		onselect: (dataset: string, table: string) => void;
		onremoved: (dataset: string, table: string) => void;
	} = $props();

	let datasets = $state<DatasetInfo[]>([]);
	let tables = $state<Record<string, TableInfo[]>>({});
	let expanded = $state(new Set<string>());
	let loading = $state(true);
	let busy = $state('');

	let dsModal = $state<Modal | null>(null);
	let tableForm = $state<BigQueryTableForm | null>(null);
	let tableDataset = $state('');
	let dsId = $state('');
	let dsLocation = $state('US');
	let dsTouched = $state(false);

	const dsError = $derived(validateBqId(dsId.trim(), 'Dataset ID'));

	async function loadTables(dataset: string) {
		try {
			tables = { ...tables, [dataset]: await listTables(project, dataset) };
		} catch (e) {
			toast.error(`Failed to list tables in ${dataset}: ${describeGcpError(e)}`);
		}
	}

	export async function reload() {
		loading = true;
		loadError = '';
		try {
			datasets = (await listDatasets(project)).toSorted((a, b) => a.id.localeCompare(b.id));
			datasetIds = datasets.map((d) => d.id);
			await Promise.all([...expanded].filter((d) => datasetIds.includes(d)).map((d) => loadTables(d)));
		} catch (e) {
			loadError = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	export async function show(dataset: string, table: string) {
		expanded = new Set(expanded).add(dataset);
		await loadTables(dataset);
		onselect(dataset, table);
	}

	export function reset() {
		tables = {};
		expanded = new Set();
		void reload();
	}

	async function toggle(dataset: string) {
		const next = new Set(expanded);
		if (next.has(dataset)) next.delete(dataset);
		else next.add(dataset);
		expanded = next;
		if (next.has(dataset)) await loadTables(dataset);
	}

	export function openDataset() {
		dsId = '';
		dsLocation = 'US';
		dsTouched = false;
		dsModal?.open();
	}

	async function submitDataset(event: Event) {
		event.preventDefault();
		dsTouched = true;
		if (dsError) return;
		const id = dsId.trim();
		busy = 'dataset';
		const ok = await attempt(() => createDataset(project, id, dsLocation), `Created dataset ${id}`, `Failed to create ${id}`);
		busy = '';
		if (!ok) return;
		dsModal?.close('created');
		await reload();
		await toggle(id);
	}

	function openTable(dataset: string) {
		tableDataset = dataset;
		tableForm?.open();
	}

	async function submitTable(id: string, schema: SchemaField[]) {
		busy = 'table';
		const ok = await attempt(() => createTable(project, tableDataset, id, schema), `Created table ${id}`, `Failed to create ${id}`);
		busy = '';
		if (!ok) return;
		tableForm?.close();
		await show(tableDataset, id);
	}

	async function removeDataset(dataset: string) {
		const message = `Delete dataset "${dataset}" and every table in it? This cannot be undone.`;
		if (!(await confirmDestructive({ title: 'Delete dataset', message }))) return;
		if (!(await attempt(() => deleteDataset(project, dataset), `Deleted dataset ${dataset}`, `Failed to delete ${dataset}`))) return;
		onremoved(dataset, '');
		await reload();
	}

	async function removeTable(dataset: string, table: string) {
		if (!(await confirmDestructive({ title: 'Delete table', message: `Delete table "${dataset}.${table}"? This cannot be undone.` }))) return;
		if (!(await attempt(() => deleteTable(project, dataset, table), `Deleted table ${table}`, `Failed to delete ${table}`))) return;
		onremoved(dataset, table);
		await loadTables(dataset);
	}

	onMount(reload);
</script>

<section aria-label="Explorer" class="space-y-2">
	<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Explorer</h2>
	{#if loading && datasets.length === 0}
		<p class="py-6 text-center text-sm text-gray-500 dark:text-gray-400">Loading…</p>
	{:else}
		<BigQueryTree {project} {datasets} {tables} {expanded} {selected} ontoggle={toggle} {onselect} onnewtable={openTable} ondeletedataset={removeDataset} ondeletetable={removeTable} />
	{/if}
</section>

<Modal bind:this={dsModal} title="New dataset" titleId="bq-dataset-title">
	{#snippet children()}
		<form id="bq-dataset-form" class="space-y-3" onsubmit={submitDataset} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Dataset ID</span>
				<input class={GCP_INPUT} bind:value={dsId} placeholder="analytics" autocomplete="off" aria-label="Dataset ID" data-testid="bq-dataset-id" />
				{#if dsTouched && dsError}<span class="block text-xs text-red-600 dark:text-red-400">{dsError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Location</span>
				<input class={GCP_INPUT} bind:value={dsLocation} aria-label="Location" />
			</label>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={GCP_GHOST} onclick={() => dsModal?.close()}>Cancel</button>
		<button type="submit" form="bq-dataset-form" class={GCP_PRIMARY} disabled={busy === 'dataset'} data-testid="bq-dataset-submit">{busy === 'dataset' ? 'Creating…' : 'Create dataset'}</button>
	{/snippet}
</Modal>

<BigQueryTableForm bind:this={tableForm} dataset={tableDataset} busy={busy === 'table'} onsubmit={submitTable} />
