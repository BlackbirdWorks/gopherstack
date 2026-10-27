<script lang="ts">
	import { onMount } from 'svelte';
	import { Database, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import { GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import {
		DEFAULT_PAGE_SIZE,
		listEntities,
		listIndexes,
		listKinds,
		listNamespaces,
		nsLabel,
		readStoredDsProject,
		runGql,
		writeStoredDsProject,
		type DsEntity,
		type IndexInfo,
		type QueryPage
	} from '#lib/gcpdatastore.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'entities', label: 'Entities' },
		{ id: 'gql', label: 'GQL query' },
		{ id: 'indexes', label: 'Indexes' }
	];

	let project = $state(readStoredDsProject());
	let namespaces = $state<string[]>([]);
	let namespace = $state('');
	let kinds = $state<string[]>([]);
	let kind = $state('');
	let loading = $state(true);
	let error = $state('');
	let tab = $state('entities');
	let entities = $state<DsEntity[]>([]);
	let cursor = $state('');
	let more = $state(false);
	let childLoading = $state(false);
	let childError = $state('');
	let expanded = $state('');
	let gql = $state('SELECT * FROM `Kind` LIMIT 20');
	let gqlResult = $state<QueryPage | null>(null);
	let indexes = $state<IndexInfo[]>([]);

	async function load() {
		try {
			namespaces = await listNamespaces(project);
			if (!namespaces.includes(namespace)) namespace = namespaces[0] ?? '';
			kinds = await listKinds(project, namespace);
			if (!kinds.includes(kind)) kind = '';
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await loadChild();
	}

	async function loadEntities(append: boolean) {
		const page = await listEntities(project, namespace, kind, append ? cursor : '', DEFAULT_PAGE_SIZE);
		entities = append ? [...entities, ...page.entities] : page.entities;
		cursor = page.endCursor;
		more = page.moreResults === 'NOT_FINISHED' || page.moreResults === 'MORE_RESULTS_AFTER_LIMIT';
	}

	async function loadChild(append = false) {
		childError = '';
		if (tab === 'gql') return;
		if (tab === 'entities' && !kind) {
			entities = [];
			more = false;
			return;
		}
		childLoading = true;
		try {
			if (tab === 'entities') await loadEntities(append);
			else indexes = await listIndexes(project);
		} catch (e) {
			childError = describeGcpError(e);
		} finally {
			childLoading = false;
		}
	}

	async function execute() {
		childError = '';
		childLoading = true;
		try {
			gqlResult = await runGql(project, namespace, gql);
		} catch (e) {
			gqlResult = null;
			childError = describeGcpError(e);
		} finally {
			childLoading = false;
		}
	}

	function applyProject(p: string) {
		project = p;
		writeStoredDsProject(p);
		namespace = '';
		kind = '';
		loading = true;
		void load();
	}

	async function pickNamespace() {
		kind = '';
		kinds = await listKinds(project, namespace).catch(() => []);
		await loadChild();
	}

	function pickKind(k: string) {
		kind = kind === k ? '' : k;
		if (kind) gql = `SELECT * FROM \`${kind}\` LIMIT 20`;
		tab = 'entities';
		expanded = '';
		void loadChild();
	}

	function selectTab(id: string) {
		tab = id;
		void loadChild();
	}

	const cell = 'px-4 py-2';
	const entityId = (e: DsEntity) => e.label;

	onMount(load);
</script>

{#snippet entityTable(items: DsEntity[], testId: string)}
	<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700" data-testid="{testId}-list">
		<table class="w-full text-sm">
			<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Key</th><th class={cell}>Properties</th><th class="{cell} text-right">Actions</th></tr></thead>
			<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
				{#each items as e (entityId(e))}
					<tr data-testid="{testId}-row">
						<td class="{cell} font-mono text-xs break-all" data-testid="{testId}-key">{e.label}</td>
						<td class="{cell} text-xs break-all">{e.properties.map((p) => p.name).join(', ') || '—'}</td>
						<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => (expanded = expanded === entityId(e) ? '' : entityId(e))} aria-expanded={expanded === entityId(e)} data-testid="{testId}-details">Properties</button></td>
					</tr>
					{#if expanded === entityId(e)}
						<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="{testId}-detail">
							<td colspan="3" class="px-4 py-3 text-xs">
								{#if e.properties.length === 0}
									<p class="text-gray-500">No properties</p>
								{:else}
									<table class="w-full" data-testid="{testId}-props">
										<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="py-1 pr-4">Property</th><th class="py-1 pr-4">Type</th><th class="py-1 pr-4">Value</th><th class="py-1">Indexed</th></tr></thead>
										<tbody>
											{#each e.properties as p (p.name)}
												<tr data-testid="{testId}-prop">
													<td class="py-1 pr-4 font-mono">{p.name}</td><td class="py-1 pr-4">{p.type}</td>
													<td class="py-1 pr-4 font-mono break-all">{p.display}</td>
													<td class="py-1" data-testid="{testId}-indexed">{p.excludeFromIndexes ? 'excluded' : 'indexed'}</td>
												</tr>
											{/each}
										</tbody>
									</table>
								{/if}
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	</div>
{/snippet}

<div class="space-y-6 pb-20" data-testid="datastore-dashboard">
	<PageHeader icon={Database} title="Datastore" description="Google Cloud Datastore: namespaces, kinds, entities, GQL queries and composite indexes" color="blue" onRefresh={load} />
	<GcpScopeBar prefix="datastore" {project} hint="Entities are listed per project and namespace." onApply={(p) => applyProject(p)} />

	{#if error}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="datastore-error">
			<p class="font-medium">Could not load Datastore</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="flex flex-wrap items-center gap-3 text-xs text-gray-500 dark:text-gray-400">
			<label for="datastore-namespace">Namespace</label>
			<select id="datastore-namespace" class="{GCP_INPUT} !w-56 !py-1 text-xs" bind:value={namespace} onchange={pickNamespace} data-testid="datastore-namespace">
				{#each namespaces as ns (ns)}<option value={ns}>{nsLabel(ns)}</option>{/each}
			</select>
		</div>

		<section class="space-y-2" data-testid="datastore-kinds">
			<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Kinds</h2>
			{#if kinds.length === 0}
				<p class="py-4 text-gray-500 dark:text-gray-400" data-testid="datastore-kinds-empty">No kinds in this namespace</p>
			{:else}
				<div class="flex flex-wrap gap-2">
					{#each kinds as k (k)}
						<button class="{GCP_MINI} font-mono {kind === k ? 'bg-blue-50 dark:bg-blue-950/30' : ''}" onclick={() => pickKind(k)} aria-pressed={kind === k} data-testid="datastore-kind">{k}</button>
					{/each}
				</div>
			{/if}
		</section>

		<div class="space-y-3" data-testid="datastore-detail">
			<Tabs tabs={TABS} active={tab} onSelect={selectTab} color="blue" />
			{#if childError}<p class="text-sm text-red-600" role="alert" data-testid="datastore-detail-error">{childError}</p>{/if}

			{#if tab === 'entities'}
				{#if !kind}
					<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="datastore-pick-kind">Select a kind to browse its entities</p>
				{:else if childLoading && entities.length === 0}
					<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
				{:else if entities.length === 0}
					<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="datastore-entities-empty">No entities of kind {kind}</p>
				{:else}
					{@render entityTable(entities, 'datastore-entity')}
					{#if more}<button class={GCP_MINI} onclick={() => loadChild(true)} disabled={childLoading} data-testid="datastore-more">Load more</button>{/if}
				{/if}
			{:else if tab === 'gql'}
				<div class="space-y-2 rounded-lg border border-gray-200 p-3 dark:border-gray-700">
					<label class="block text-xs font-medium" for="datastore-gql">GQL query (namespace {nsLabel(namespace)})</label>
					<textarea id="datastore-gql" class="{GCP_INPUT} h-24 font-mono text-xs" bind:value={gql} data-testid="datastore-gql"></textarea>
					<button class={GCP_PRIMARY} onclick={execute} disabled={childLoading || !gql.trim()} data-testid="datastore-run">Run query</button>
				</div>
				{#if gqlResult}
					{#if gqlResult.entities.length === 0}
						<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="datastore-gql-empty">The query returned no entities</p>
					{:else}
						{@render entityTable(gqlResult.entities, 'datastore-result')}
					{/if}
				{/if}
			{:else if childLoading}
				<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
			{:else if indexes.length === 0}
				<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="datastore-indexes-empty">No composite indexes</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="datastore-indexes">
						<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Index</th><th class={cell}>Kind</th><th class={cell}>Ancestor</th><th class={cell}>Properties</th><th class={cell}>State</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each indexes as i (i.id)}
								<tr data-testid="datastore-index">
									<td class="{cell} font-mono text-xs">{i.id}</td><td class="{cell} font-mono text-xs">{i.kind}</td>
									<td class="{cell} text-xs">{i.ancestor}</td>
									<td class="{cell} font-mono text-xs">{i.properties.map((p) => `${p.name} ${p.direction === 'DESCENDING' ? 'desc' : 'asc'}`).join(', ')}</td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {i.state === 'READY' ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-700'}">{i.state}</span></td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}
</div>
