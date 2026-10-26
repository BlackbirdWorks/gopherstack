<script lang="ts">
	import { onMount } from 'svelte';
	import { Globe } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { attempt } from '#lib/gcp-action.ts';
	import { readStoredProject, writeStoredProject } from '#lib/gcpproject.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		deleteDatabase,
		deleteInstance,
		getDdl,
		listDatabases,
		listInstances,
		type SpannerDatabase,
		type SpannerInstance
	} from '#lib/gcpspanner.ts';

	const PROJECT_KEY = 'gopherstack_spanner_project';

	let project = $state(readStoredProject(PROJECT_KEY));
	let instances = $state<SpannerInstance[]>([]);
	let databases = $state<SpannerDatabase[]>([]);
	let instance = $state('');
	let ddl = $state<Record<string, string[] | string>>({});
	let loading = $state(true);
	let dbLoading = $state(false);
	let error = $state('');
	let dbError = $state('');

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

	async function loadDatabases() {
		if (!instance) {
			databases = [];
			return;
		}
		dbLoading = true;
		try {
			databases = (await listDatabases(instance)).toSorted((a, b) => a.id.localeCompare(b.id));
			dbError = '';
			await Promise.all(databases.map((d) => loadDdl(d.name)));
		} catch (e) {
			dbError = describeGcpError(e);
		} finally {
			dbLoading = false;
		}
	}

	$effect(() => {
		void instance;
		void loadDatabases();
	});

	async function loadDdl(name: string) {
		if (ddl[name]) return;
		try {
			ddl[name] = await getDdl(name);
		} catch (e) {
			ddl[name] = describeGcpError(e);
		}
	}

	function applyScope(p: string) {
		project = p;
		writeStoredProject(PROJECT_KEY, p);
		instance = '';
		loading = true;
		void loadInstances();
	}

	async function removeDatabase(row: ResourceRow) {
		if (!(await confirmDestructive({ title: 'Drop database', message: `Drop database "${row.cells[0]}"?` }))) return;
		if (await attempt(() => deleteDatabase(row.id), `Dropped database ${row.cells[0]}`, `Failed to drop ${row.cells[0]}`)) await loadDatabases();
	}

	async function removeInstance(row: ResourceRow) {
		if (!(await confirmDestructive({ title: 'Delete instance', message: `Delete instance "${row.cells[0]}" and its databases?` }))) return;
		if (await attempt(() => deleteInstance(row.id), `Deleted instance ${row.cells[0]}`, `Failed to delete ${row.cells[0]}`)) {
			if (instance === row.id) instance = '';
			await loadInstances();
		}
	}

	const instanceRows = $derived(instances.map((i) => ({ id: i.name, cells: [i.id, i.displayName, i.config, i.processingUnits ? `${i.processingUnits} PU` : `${i.nodeCount} nodes`, i.state], raw: i.raw })));
	const dbRows = $derived(databases.map((d) => ({ id: d.name, cells: [d.id, d.state, d.dialect], raw: d.raw })));

	onMount(loadInstances);
</script>

<div class="space-y-6 pb-20" data-testid="spanner-dashboard">
	<PageHeader icon={Globe} title="Cloud Spanner" description="Google Cloud Spanner: instances, databases and schema (admin plane)" color="blue" onRefresh={() => { void loadInstances(); void loadDatabases(); }} />
	<GcpScopeBar prefix="spanner" {project} hint="Instances are listed per project." onApply={applyScope} />

	<GcpResourceTable testId="spanner-instance" noun="instances" columns={['Instance', 'Display name', 'Config', 'Capacity', 'State']} rows={instanceRows} {loading} {error} selected={instance} onRetry={loadInstances} onSelect={(r) => (instance = r.id)} onDelete={removeInstance} />

	{#if instance}
		<section class="space-y-3" data-testid="spanner-instance-panel">
			<h2 class="font-mono text-sm text-gray-700 dark:text-gray-300">{instance}</h2>
			<GcpResourceTable testId="spanner-database" noun="databases" columns={['Database', 'State', 'Dialect']} rows={dbRows} loading={dbLoading} error={dbError} onRetry={loadDatabases} onDelete={removeDatabase}>
				{#snippet detail(row)}
					{@const stmts = ddl[row.id]}
					<p class="font-medium text-gray-500 dark:text-gray-400">Schema (DDL)</p>
					{#if Array.isArray(stmts)}
						<pre class="whitespace-pre-wrap break-all rounded-md bg-white dark:bg-slate-900 p-2 font-mono" data-testid="spanner-ddl">{stmts.length ? stmts.join(';\n\n') : '(no statements)'}</pre>
					{:else}
						<p>{stmts ?? 'Loading schema…'}</p>
					{/if}
				{/snippet}
			</GcpResourceTable>
			<p class="text-xs text-gray-500 dark:text-gray-400">Open Details to see a database's DDL. Row data is queried with a Spanner client against the emulator port.</p>
		</section>
	{/if}
</div>
