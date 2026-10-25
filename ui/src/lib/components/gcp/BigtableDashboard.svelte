<script lang="ts">
	import { onMount } from 'svelte';
	import { Table2 } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import BigtableRows from '#lib/components/gcp/BigtableRows.svelte';
	import { confirmedAttempt } from '#lib/gcp-action.ts';
	import {
		deleteInstance,
		deleteTable,
		listClusters,
		listInstances,
		listTables,
		readStoredBigtableProject,
		writeStoredBigtableProject,
		type ClusterInfo,
		type InstanceInfo,
		type TableInfo
	} from '#lib/gcpbigtable.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'tables', label: 'Tables' },
		{ id: 'clusters', label: 'Clusters' }
	];

	let project = $state(readStoredBigtableProject());
	let instances = $state<InstanceInfo[]>([]);
	let clusters = $state<ClusterInfo[]>([]);
	let tables = $state<TableInfo[]>([]);
	let instance = $state('');
	let table = $state('');
	let tab = $state('tables');
	let loading = $state(true);
	let childLoading = $state(false);
	let error = $state('');
	let childError = $state('');

	async function loadInstances() {
		try {
			instances = (await listInstances(project)).toSorted((a, b) => a.id.localeCompare(b.id));
			error = '';
			if (!instances.some((i) => i.name === instance)) instance = instances[0]?.name ?? '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadChildren() {
		if (!instance) {
			clusters = [];
			tables = [];
			return;
		}
		childLoading = true;
		try {
			[clusters, tables] = await Promise.all([listClusters(instance), listTables(instance)]);
			tables = tables.toSorted((a, b) => a.id.localeCompare(b.id));
			childError = '';
			if (!tables.some((t) => t.name === table)) table = '';
		} catch (e) {
			childError = describeGcpError(e);
		} finally {
			childLoading = false;
		}
	}

	$effect(() => {
		void instance;
		void loadChildren();
	});

	function applyScope(p: string) {
		project = p;
		writeStoredBigtableProject(p);
		instance = '';
		table = '';
		loading = true;
		void loadInstances();
	}

	async function removeTable(row: ResourceRow) {
		const confirm = { title: 'Delete table', message: `Delete table "${row.cells[0]}" and all its rows?` };
		if (await confirmedAttempt(confirm, () => deleteTable(row.id), `Deleted table ${row.cells[0]}`, `Failed to delete ${row.cells[0]}`)) {
			if (table === row.id) table = '';
			await loadChildren();
		}
	}

	async function removeInstance(row: ResourceRow) {
		const confirm = { title: 'Delete instance', message: `Delete instance "${row.cells[0]}" with its clusters and tables?` };
		if (await confirmedAttempt(confirm, () => deleteInstance(row.id), `Deleted instance ${row.cells[0]}`, `Failed to delete ${row.cells[0]}`)) {
			if (instance === row.id) instance = '';
			await loadInstances();
		}
	}

	const instanceRows = $derived(instances.map((i) => ({ id: i.name, cells: [i.id, i.displayName, i.type, i.state], raw: i.raw })));
	const clusterRows = $derived(clusters.map((c) => ({ id: c.name, cells: [c.id, c.location, String(c.serveNodes), c.storageType, c.state], raw: c.raw })));
	const tableRows = $derived(tables.map((t) => ({ id: t.name, cells: [t.id, t.families.map((f) => f.name).join(', '), t.granularity], raw: t.raw })));
	const selectedTable = $derived(tables.find((t) => t.name === table));

	onMount(loadInstances);
</script>

<div class="space-y-6 pb-20" data-testid="bigtable-dashboard">
	<PageHeader icon={Table2} title="Cloud Bigtable" description="Google Cloud Bigtable: instances, clusters, tables and row data" color="blue" onRefresh={() => { void loadInstances(); void loadChildren(); }} />
	<GcpScopeBar prefix="bigtable" {project} hint="Instances are listed per project." onApply={applyScope} />

	<GcpResourceTable testId="bigtable-instance" noun="instances" columns={['Instance', 'Display name', 'Type', 'State']} rows={instanceRows} {loading} {error} selected={instance} onRetry={loadInstances} onSelect={(r) => { instance = r.id; table = ''; }} onDelete={removeInstance} />

	{#if instance}
		<section class="space-y-3" data-testid="bigtable-instance-panel">
			<h2 class="font-mono text-sm text-gray-700 dark:text-gray-300">{instance}</h2>
			<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />
			{#if tab === 'tables'}
				<GcpResourceTable testId="bigtable-table" noun="tables" columns={['Table', 'Column families', 'Granularity']} rows={tableRows} loading={childLoading} error={childError} selected={table} onRetry={loadChildren} onSelect={(r) => (table = r.id)} onDelete={removeTable}>
					{#snippet detail(row)}
						{@const t = tables.find((x) => x.name === row.id)}
						{#if t}
							<ul class="list-disc pl-5" data-testid="bigtable-families">
								{#each t.families as f (f.name)}<li><span class="font-mono">{f.name}</span>: {f.gcRule}</li>{:else}<li>No column families</li>{/each}
							</ul>
						{/if}
					{/snippet}
				</GcpResourceTable>
				{#if selectedTable}
					<h3 class="text-sm font-medium text-gray-700 dark:text-gray-300">Rows in <span class="font-mono">{selectedTable.id}</span></h3>
					<BigtableRows table={selectedTable.name} />
				{/if}
			{:else}
				<GcpResourceTable testId="bigtable-cluster" noun="clusters" columns={['Cluster', 'Location', 'Nodes', 'Storage', 'State']} rows={clusterRows} loading={childLoading} error={childError} onRetry={loadChildren} />
			{/if}
		</section>
	{/if}
</div>
