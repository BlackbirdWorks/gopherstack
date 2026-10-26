<script lang="ts">
	import { onMount } from 'svelte';
	import { Activity, Play, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		EXAMPLE_QUERIES,
		QueryError,
		TIMESPANS,
		addToHistory,
		formatCell,
		listEndpoints,
		listRules,
		listTables,
		listWorkspaces,
		readHistory,
		runQuery,
		writeHistory,
		type EndpointInfo,
		type HistoryEntry,
		type QueryResult,
		type RuleInfo,
		type TableInfo,
		type WorkspaceInfo
	} from '#lib/azuremonitor.ts';

	const TABS = [
		{ id: 'workspaces', label: 'Workspaces' },
		{ id: 'collection', label: 'Data collection' },
		{ id: 'query', label: 'Query console' }
	];

	let workspaces = $state<WorkspaceInfo[]>([]);
	let endpoints = $state<EndpointInfo[]>([]);
	let rules = $state<RuleInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let tab = $state('workspaces');
	let workspaceId = $state('');
	let tables = $state<TableInfo[]>([]);
	let tablesLoading = $state(false);
	let tablesError = $state('');

	let queryWorkspace = $state('');
	let timespan = $state('PT24H');
	let query = $state(EXAMPLE_QUERIES[0].query);
	let running = $state(false);
	let result = $state<QueryResult | null>(null);
	let queryError = $state<QueryError | null>(null);
	let history = $state<HistoryEntry[]>([]);

	async function load() {
		try {
			[workspaces, endpoints, rules] = await Promise.all([listWorkspaces(), listEndpoints(), listRules()]);
			error = '';
			if (!workspaces.some((w) => w.customerId === workspaceId)) workspaceId = '';
			if (!workspaces.some((w) => w.customerId === queryWorkspace)) queryWorkspace = workspaces[0]?.customerId ?? '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	function selectWorkspace(row: ResourceRow) {
		workspaceId = workspaceId === row.id ? '' : row.id;
		tables = [];
		if (!workspaceId) return;
		queryWorkspace = workspaceId;
		void loadTables();
	}

	async function loadTables() {
		tablesLoading = true;
		tablesError = '';
		try {
			tables = await listTables(workspaceId);
		} catch (e) {
			tablesError = describeGcpError(e);
		} finally {
			tablesLoading = false;
		}
	}

	async function run() {
		if (!queryWorkspace || running) return;
		running = true;
		queryError = null;
		result = null;
		try {
			result = await runQuery(queryWorkspace, query, timespan);
		} catch (e) {
			queryError = e instanceof QueryError ? e : new QueryError(0, 'Error', describeGcpError(e), []);
		} finally {
			running = false;
		}
		history = addToHistory(history, { query, timespan, at: new Date().toISOString() });
		writeHistory(history);
	}

	function onKey(e: KeyboardEvent) {
		if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
			e.preventDefault();
			void run();
		}
	}

	function useHistory(h: HistoryEntry) {
		query = h.query;
		timespan = h.timespan;
	}

	function clearHistory() {
		history = [];
		writeHistory(history);
	}

	const workspaceRows = $derived(
		workspaces.map((w) => ({ id: w.customerId, cells: [w.name, w.customerId, w.sku, `${w.retentionDays} days`, w.location], raw: w.raw }))
	);
	const tableRows = $derived(
		tables.map((t) => ({
			id: t.name,
			cells: [t.name, t.custom ? 'Custom' : 'Built-in', t.plan, `${t.retentionDays} / ${t.totalRetentionDays} days`, String(t.columns.length)],
			raw: t
		}))
	);
	const endpointRows = $derived(endpoints.map((e) => ({ id: e.id, cells: [e.name, e.kind, e.location, e.immutableId, e.logsIngestion], raw: e })));
	const ruleRows = $derived(
		rules.map((r) => ({ id: r.id, cells: [r.name, r.kind, r.location, r.immutableId, `${r.dataFlows.length} flow(s)`], raw: r }))
	);
	const cell = 'px-3 py-2';

	onMount(() => {
		history = readHistory();
		void load();
	});
</script>

<div class="space-y-6 pb-20" data-testid="monitor-dashboard">
	<PageHeader icon={Activity} title="Monitor Logs" description="Azure Monitor Logs: Log Analytics workspaces, tables, data collection endpoints and rules, and a KQL query console" color="blue" onRefresh={load} />
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />

	{#if tab === 'workspaces'}
		<GcpResourceTable testId="monitor-workspace" noun="workspaces" columns={['Workspace', 'Customer ID', 'SKU', 'Retention', 'Location']} rows={workspaceRows} {loading} {error} selected={workspaceId} onRetry={load} onSelect={selectWorkspace} />
		{#if workspaceId}
			<div class="space-y-3" data-testid="monitor-tables">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Tables</h2>
				{#if tablesLoading}
					<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
				{:else}
					<GcpResourceTable testId="monitor-table" noun="tables" columns={['Table', 'Kind', 'Plan', 'Retention / total', 'Columns']} rows={tableRows} loading={false} error={tablesError} onRetry={loadTables}>
						{#snippet detail(row)}
							{@const t = tables.find((x) => x.name === row.id)}
							{#if t}
								<table class="w-full text-xs" data-testid="monitor-schema">
									<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Column</th><th class={cell}>Type</th><th class={cell}>Description</th></tr></thead>
									<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
										{#each t.columns as c (c.name)}
											<tr data-testid="monitor-schema-column"><td class="{cell} font-mono">{c.name}</td><td class={cell}>{c.type}</td><td class={cell}>{c.description || '—'}</td></tr>
										{/each}
									</tbody>
								</table>
							{/if}
						{/snippet}
					</GcpResourceTable>
				{/if}
			</div>
		{/if}
	{:else if tab === 'collection'}
		<div class="space-y-6">
			<div class="space-y-2">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Data collection endpoints</h2>
				<GcpResourceTable testId="monitor-dce" noun="data collection endpoints" columns={['Endpoint', 'Kind', 'Location', 'Immutable ID', 'Logs ingestion']} rows={endpointRows} {loading} {error} onRetry={load} />
			</div>
			<div class="space-y-2">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Data collection rules</h2>
				<GcpResourceTable testId="monitor-dcr" noun="data collection rules" columns={['Rule', 'Kind', 'Location', 'Immutable ID', 'Flows']} rows={ruleRows} {loading} {error} onRetry={load}>
					{#snippet detail(row)}
						{@const r = rules.find((x) => x.id === row.id)}
						{#if r}
							<div class="space-y-3" data-testid="monitor-dcr-detail">
								<div>
									<p class="font-semibold">Stream declarations</p>
									{#each r.streams as s (s.name)}
										<p class="mt-1 font-mono" data-testid="monitor-dcr-stream">{s.name}</p>
										<p class="text-gray-600 dark:text-gray-400">{s.columns.map((c) => `${c.name}: ${c.type}`).join(', ') || 'no columns'}</p>
									{:else}
										<p class="text-gray-500">None</p>
									{/each}
								</div>
								<div>
									<p class="font-semibold">Data flows</p>
									{#each r.dataFlows as f, i (i)}
										<div class="mt-1 rounded-md border border-gray-200 dark:border-gray-700 p-2" data-testid="monitor-dcr-flow">
											<p>{f.streams.join(', ')} → {f.destinations.join(', ')} <span class="text-gray-500">(output {f.outputStream || 'default'})</span></p>
											<pre class="mt-1 whitespace-pre-wrap font-mono" data-testid="monitor-dcr-transform">{f.transformKql || 'source'}</pre>
										</div>
									{:else}
										<p class="text-gray-500">None</p>
									{/each}
								</div>
							</div>
						{/if}
					{/snippet}
				</GcpResourceTable>
			</div>
		</div>
	{:else}
		<div class="space-y-4" data-testid="monitor-console">
			{#if !loading && workspaces.length === 0}
				<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="monitor-console-empty">No workspaces to query. Create one through Resource Manager (Microsoft.OperationalInsights/workspaces).</p>
			{:else}
				<div class="grid gap-3 sm:grid-cols-3">
					<label class="text-sm"><span class="mb-1 block font-medium">Workspace</span>
						<select class={GCP_INPUT} bind:value={queryWorkspace} data-testid="monitor-query-workspace">
							{#each workspaces as w (w.customerId)}<option value={w.customerId}>{w.name}</option>{/each}
						</select>
					</label>
					<label class="text-sm"><span class="mb-1 block font-medium">Time range</span>
						<select class={GCP_INPUT} bind:value={timespan} data-testid="monitor-timespan">
							{#each TIMESPANS as t (t.value)}<option value={t.value}>{t.label}</option>{/each}
						</select>
					</label>
					<label class="text-sm"><span class="mb-1 block font-medium">Examples</span>
						<select class={GCP_INPUT} onchange={(e) => { const i = e.currentTarget.selectedIndex - 1; if (i >= 0) query = EXAMPLE_QUERIES[i].query; e.currentTarget.selectedIndex = 0; }} data-testid="monitor-examples">
							<option value="">Insert an example…</option>
							{#each EXAMPLE_QUERIES as ex (ex.label)}<option value={ex.label}>{ex.label}</option>{/each}
						</select>
					</label>
				</div>
				<textarea class="{GCP_INPUT} h-40 font-mono" spellcheck="false" aria-label="KQL query" bind:value={query} onkeydown={onKey} data-testid="monitor-query"></textarea>
				<div class="flex items-center gap-3">
					<button class={GCP_PRIMARY} onclick={run} disabled={running || !queryWorkspace || !query.trim()} data-testid="monitor-run"><Play class="inline h-4 w-4" /> {running ? 'Running…' : 'Run'}</button>
					<span class="text-xs text-gray-500 dark:text-gray-400">Ctrl+Enter to run. Queries run against the in-process workspace data only.</span>
				</div>

				{#if queryError}
					<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="monitor-query-error">
						<p class="font-medium" data-testid="monitor-error-code">{queryError.code}: {queryError.message}</p>
						<ul class="mt-1 list-disc pl-5 font-mono text-xs">
							{#each queryError.details as d, i (i)}<li data-testid="monitor-error-detail">{d}</li>{/each}
						</ul>
					</div>
				{/if}

				{#if result}
					{#each result.tables as t (t.name)}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto" data-testid="monitor-result">
							<p class="px-3 py-2 text-xs text-gray-500 dark:text-gray-400" data-testid="monitor-result-count">{t.name}: {t.rows.length} row(s)</p>
							<table class="w-full text-xs">
								<thead class="bg-gray-50 dark:bg-slate-800/60 text-left">
									<tr>
										{#each t.columns as c (c.name)}
											<th class={cell} data-testid="monitor-result-column"><span class="font-mono">{c.name}</span> <span class="font-normal text-gray-500" data-testid="monitor-result-type">{c.type}</span></th>
										{/each}
									</tr>
								</thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
									{#each t.rows as row, i (i)}
										<tr data-testid="monitor-result-row">
											{#each row as v, j (j)}<td class="{cell} font-mono break-all">{formatCell(v)}</td>{/each}
										</tr>
									{:else}
										<tr><td class="{cell} text-gray-500" colspan={Math.max(t.columns.length, 1)} data-testid="monitor-result-empty">No rows</td></tr>
									{/each}
								</tbody>
							</table>
						</div>
					{/each}
				{/if}

				<div class="space-y-2" data-testid="monitor-history">
					<div class="flex items-center justify-between">
						<h2 class="text-sm font-semibold text-gray-900 dark:text-white">Query history <span class="font-normal text-gray-500">(stored in this browser only)</span></h2>
						{#if history.length > 0}<button class={GCP_MINI} onclick={clearHistory} data-testid="monitor-history-clear">Clear</button>{/if}
					</div>
					{#each history as h (h.at + h.query)}
						<button class="block w-full truncate rounded-md border border-gray-200 dark:border-gray-700 px-3 py-1.5 text-left font-mono text-xs hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => useHistory(h)} data-testid="monitor-history-item">{h.query.replace(/\s+/g, ' ')}</button>
					{:else}
						<p class="text-xs text-gray-500" data-testid="monitor-history-empty">No queries yet</p>
					{/each}
				</div>
			{/if}
		</div>
	{/if}
</div>
