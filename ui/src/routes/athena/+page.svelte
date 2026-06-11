<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { getAthenaClient } from '$lib/aws-client';
	import {
		ListWorkGroupsCommand,
		ListDataCatalogsCommand,
		StartQueryExecutionCommand,
		GetQueryExecutionCommand,
		GetQueryResultsCommand,
		StopQueryExecutionCommand,
		ListQueryExecutionsCommand,
		ListSessionsCommand,
		StartSessionCommand,
		TerminateSessionCommand,
		ListNotebookMetadataCommand,
		CreateNotebookCommand,
		ListPreparedStatementsCommand,
		type WorkGroupSummary,
		type DataCatalogSummary,
		type QueryExecution,
		type ResultSet,
		type SessionSummary,
		type NotebookMetadata,
		type PreparedStatementSummary
	} from '@aws-sdk/client-athena';
	import { toast } from 'svelte-sonner';
	import { Search, RefreshCw, Play, XCircle, Database, Clock, ChevronRight, Table, BookOpen, Terminal } from 'lucide-svelte';

	const athena = getAthenaClient();

	let activeTab = $state<'query' | 'workgroups' | 'catalogs' | 'history' | 'sessions' | 'notebooks' | 'prepared'>('query');

	// Query Editor
	let queryText = $state('SELECT * FROM my_table LIMIT 10;');
	let queryWorkgroup = $state('primary');
	let queryDatabase = $state('default');
	let queryOutputLocation = $state('s3://my-athena-results/');
	let queryExecuting = $state(false);
	let currentQueryId = $state<string | null>(null);
	let queryResults = $state<ResultSet | null>(null);
	let queryStatus = $state<QueryExecution | null>(null);
	let pollingInterval: ReturnType<typeof setInterval> | undefined;

	// Work Groups
	let workgroups = $state<WorkGroupSummary[]>([]);
	let loadingWorkgroups = $state(false);
	let selectedWorkgroup = $state<string | null>(null);

	// Data Catalogs
	let catalogs = $state<DataCatalogSummary[]>([]);
	let loadingCatalogs = $state(false);

	// History
	let queryHistory = $state<string[]>([]);
	let historyDetail = $state<QueryExecution[]>([]);
	let loadingHistory = $state(false);

	// Sessions
	let sessions = $state<SessionSummary[]>([]);
	let loadingSessions = $state(false);
	let newSessionWorkgroup = $state('primary');

	// Notebooks
	let notebooks = $state<NotebookMetadata[]>([]);
	let loadingNotebooks = $state(false);
	let newNotebookName = $state('');
	let newNotebookWorkgroup = $state('primary');

	// Prepared Statements
	let preparedStatements = $state<PreparedStatementSummary[]>([]);
	let loadingPrepared = $state(false);

	const statusColor = (state: string | undefined) => {
		if (!state) return 'gray';
		if (state === 'SUCCEEDED') return 'green';
		if (state === 'FAILED' || state === 'CANCELLED') return 'red';
		if (state === 'RUNNING') return 'blue';
		return 'yellow';
	};

	async function executeQuery() {
		if (!queryText.trim()) return;
		queryExecuting = true;
		queryResults = null;
		queryStatus = null;
		currentQueryId = null;
		try {
			const resp = await athena.send(new StartQueryExecutionCommand({
				QueryString: queryText.trim(),
				WorkGroup: queryWorkgroup || 'primary',
				QueryExecutionContext: { Database: queryDatabase || undefined },
				ResultConfiguration: {
					OutputLocation: queryOutputLocation || undefined
				}
			}));
			currentQueryId = resp.QueryExecutionId ?? null;
			toast.success('Query started: ' + currentQueryId);
			await pollQueryStatus(currentQueryId!);
		} catch (e) {
			toast.error('Query failed: ' + String(e));
			queryExecuting = false;
		}
	}

	async function pollQueryStatus(queryId: string) {
		let attempts = 0;
		const poll = async () => {
			attempts++;
			try {
				const resp = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
				queryStatus = resp.QueryExecution ?? null;
				const state = queryStatus?.Status?.State;
				if (state === 'SUCCEEDED') {
					queryExecuting = false;
					clearInterval(pollingInterval);
					await loadResults(queryId);
				} else if (state === 'FAILED' || state === 'CANCELLED') {
					queryExecuting = false;
					clearInterval(pollingInterval);
					toast.error('Query ' + state.toLowerCase() + ': ' + (queryStatus?.Status?.StateChangeReason ?? ''));
				} else if (attempts > 60) {
					queryExecuting = false;
					clearInterval(pollingInterval);
					toast.warning('Query is taking long. Check history.');
				}
			} catch (e) {
				clearInterval(pollingInterval);
				queryExecuting = false;
			}
		};
		await poll();
		if (queryExecuting) {
			pollingInterval = setInterval(poll, 2000);
		}
	}

	async function loadResults(queryId: string) {
		try {
			const resp = await athena.send(new GetQueryResultsCommand({ QueryExecutionId: queryId, MaxResults: 100 }));
			queryResults = resp.ResultSet ?? null;
		} catch (e) {
			toast.error('Failed to load results: ' + String(e));
		}
	}

	async function stopQuery() {
		if (!currentQueryId) return;
		try {
			await athena.send(new StopQueryExecutionCommand({ QueryExecutionId: currentQueryId }));
			toast.success('Query stop requested');
			clearInterval(pollingInterval);
			queryExecuting = false;
		} catch (e) {
			toast.error('Failed to stop query: ' + String(e));
		}
	}

	async function loadWorkgroups() {
		loadingWorkgroups = true;
		try {
			const resp = await athena.send(new ListWorkGroupsCommand({}));
			workgroups = resp.WorkGroups ?? [];
		} catch (e) {
			toast.error('Failed to load workgroups: ' + String(e));
		} finally {
			loadingWorkgroups = false;
		}
	}

	async function loadCatalogs() {
		loadingCatalogs = true;
		try {
			const resp = await athena.send(new ListDataCatalogsCommand({}));
			catalogs = resp.DataCatalogsSummary ?? [];
		} catch (e) {
			toast.error('Failed to load catalogs: ' + String(e));
		} finally {
			loadingCatalogs = false;
		}
	}

	async function loadHistory() {
		loadingHistory = true;
		try {
			const resp = await athena.send(new ListQueryExecutionsCommand({ MaxResults: 20 }));
			queryHistory = resp.QueryExecutionIds ?? [];
			// Load details for each
			const details = await Promise.allSettled(
				queryHistory.map((id) => athena.send(new GetQueryExecutionCommand({ QueryExecutionId: id })).then((r) => r.QueryExecution))
			);
			historyDetail = details.filter((r) => r.status === 'fulfilled').map((r) => (r as PromiseFulfilledResult<QueryExecution | undefined>).value!).filter(Boolean);
		} catch (e) {
			toast.error('Failed to load history: ' + String(e));
		} finally {
			loadingHistory = false;
		}
	}

	async function loadSessions() {
		loadingSessions = true;
		try {
			const resp = await athena.send(new ListSessionsCommand({ WorkGroup: newSessionWorkgroup }));
			sessions = resp.Sessions ?? [];
		} catch (e) {
			toast.error('Failed to load sessions: ' + String(e));
		} finally {
			loadingSessions = false;
		}
	}

	async function startSession() {
		try {
			await athena.send(new StartSessionCommand({
				WorkGroup: newSessionWorkgroup,
				EngineConfiguration: { MaxConcurrentDpus: 2 }
			}));
			toast.success('Session started');
			await loadSessions();
		} catch (e) {
			toast.error('Failed to start session: ' + String(e));
		}
	}

	async function terminateSession(sessionId: string) {
		try {
			await athena.send(new TerminateSessionCommand({ SessionId: sessionId }));
			toast.success('Session terminating');
			await loadSessions();
		} catch (e) {
			toast.error('Failed to terminate session: ' + String(e));
		}
	}

	async function loadNotebooks() {
		loadingNotebooks = true;
		try {
			const resp = await athena.send(new ListNotebookMetadataCommand({ WorkGroup: newNotebookWorkgroup }));
			notebooks = resp.NotebookMetadataList ?? [];
		} catch (e) {
			toast.error('Failed to load notebooks: ' + String(e));
		} finally {
			loadingNotebooks = false;
		}
	}

	async function createNotebook() {
		if (!newNotebookName.trim()) return;
		try {
			await athena.send(new CreateNotebookCommand({
				WorkGroup: newNotebookWorkgroup,
				Name: newNotebookName.trim()
			}));
			toast.success(`Notebook "${newNotebookName}" created`);
			newNotebookName = '';
			await loadNotebooks();
		} catch (e) {
			toast.error('Failed to create notebook: ' + String(e));
		}
	}

	async function loadPreparedStatements() {
		loadingPrepared = true;
		try {
			const resp = await athena.send(new ListPreparedStatementsCommand({ WorkGroup: queryWorkgroup || 'primary' }));
			preparedStatements = resp.PreparedStatements ?? [];
		} catch (e) {
			toast.error('Failed to load prepared statements: ' + String(e));
		} finally {
			loadingPrepared = false;
		}
	}

	async function handleTabChange(tab: 'query' | 'workgroups' | 'catalogs' | 'history' | 'sessions' | 'notebooks' | 'prepared') {
		activeTab = tab;
		if (tab === 'workgroups' && workgroups.length === 0) await loadWorkgroups();
		if (tab === 'catalogs' && catalogs.length === 0) await loadCatalogs();
		if (tab === 'history') await loadHistory();
		if (tab === 'sessions') await loadSessions();
		if (tab === 'notebooks') await loadNotebooks();
		if (tab === 'prepared') await loadPreparedStatements();
	}

	function formatDate(d: Date | undefined): string {
		if (!d) return '-';
		return d.toLocaleString();
	}

	function formatBytes(bytes: number | bigint | undefined): string {
		if (!bytes) return '0 B';
		const n = typeof bytes === 'bigint' ? Number(bytes) : bytes;
		if (n < 1024) return `${n} B`;
		if (n < 1048576) return `${(n / 1024).toFixed(1)} KB`;
		if (n < 1073741824) return `${(n / 1048576).toFixed(1)} MB`;
		return `${(n / 1073741824).toFixed(1)} GB`;
	}

	const columnHeaders = $derived(
		(queryResults?.ResultSetMetadata?.ColumnInfo ?? []).map((c) => c.Name ?? '')
	);
	const resultRows = $derived(
		(queryResults?.Rows ?? []).slice(1).map((r) => (r.Data ?? []).map((d) => d.VarCharValue ?? ''))
	);

	function triggerDownload(content: string, filename: string, mime: string) {
		const blob = new Blob([content], { type: mime });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = filename;
		a.click();
		URL.revokeObjectURL(url);
	}

	function csvEscape(v: string): string {
		return /[",\n]/.test(v) ? `"${v.replaceAll('"', '""')}"` : v;
	}

	function exportResultsCsv() {
		const header = columnHeaders.map((c) => csvEscape(c)).join(',');
		const body = resultRows.map((row) => row.map((v) => csvEscape(v)).join(',')).join('\n');
		triggerDownload([header, body].filter(Boolean).join('\n'), 'athena-results.csv', 'text/csv');
	}

	function exportResultsJson() {
		const objects = resultRows.map((row) =>
			Object.fromEntries(columnHeaders.map((col, i) => [col, row[i] ?? '']))
		);
		triggerDownload(JSON.stringify(objects, null, 2), 'athena-results.json', 'application/json');
	}

	onMount(loadWorkgroups);

	onDestroy(() => {
		if (pollingInterval !== undefined) {
			clearInterval(pollingInterval);
			pollingInterval = undefined;
		}
	});
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Search class="w-7 h-7 text-teal-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Athena</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Interactive SQL query service</p>
			</div>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700 flex-wrap">
		{#each [['query', 'Query Editor'], ['workgroups', 'Workgroups'], ['catalogs', 'Data Catalogs'], ['history', 'Query History'], ['sessions', 'Sessions'], ['notebooks', 'Notebooks'], ['prepared', 'Prepared Statements']] as [tab, label]}
			<button
				onclick={() => handleTabChange(tab as 'query' | 'workgroups' | 'catalogs' | 'history' | 'sessions' | 'notebooks' | 'prepared')}
				class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-teal-500 text-teal-600 dark:text-teal-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
			>
				{label}
			</button>
		{/each}
	</div>

	<!-- QUERY EDITOR -->
	{#if activeTab === 'query'}
		<div class="grid grid-cols-3 gap-4">
			<div class="col-span-3 md:col-span-1">
				<label for="query-workgroup" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Workgroup</label>
				<select id="query-workgroup" bind:value={queryWorkgroup} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm">
					<option value="primary">primary</option>
					{#each workgroups as wg}
						<option value={wg.Name}>{wg.Name}</option>
					{/each}
				</select>
			</div>
			<div class="col-span-3 md:col-span-1">
				<label for="query-database" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Database</label>
				<input id="query-database" bind:value={queryDatabase} type="text" placeholder="default" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
			</div>
			<div class="col-span-3 md:col-span-1">
				<label for="output-location" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Output Location</label>
				<input id="output-location" bind:value={queryOutputLocation} type="text" placeholder="s3://my-bucket/results/" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
			</div>
		</div>

		<div>
			<div class="flex items-center justify-between mb-2">
				<label for="query-text" class="block text-sm font-semibold text-gray-700 dark:text-gray-300">SQL Query</label>
				<div class="flex gap-2">
					{#if queryExecuting}
						<button onclick={stopQuery} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm font-medium">
							<XCircle class="w-4 h-4" /> Stop
						</button>
					{:else}
						<button onclick={executeQuery} disabled={!queryText.trim()} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm font-medium disabled:opacity-50">
							<Play class="w-4 h-4" /> Run Query
						</button>
					{/if}
				</div>
			</div>
			<textarea
				id="query-text"
				bind:value={queryText}
				rows={8}
				class="w-full px-4 py-3 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono text-gray-900 dark:text-white resize-y focus:ring-2 focus:ring-teal-500 focus:border-transparent"
			></textarea>
		</div>

		<!-- Query Status -->
		{#if queryStatus}
			<div class={`flex items-center gap-3 p-4 rounded-xl border ${queryStatus.Status?.State === 'SUCCEEDED' ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800' : queryStatus.Status?.State === 'FAILED' ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800' : 'bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800'}`}>
				{#if queryExecuting}
					<div class="animate-spin w-5 h-5 border-2 border-blue-600 border-t-transparent rounded-full"></div>
				{/if}
				<div>
					<div class="font-medium text-sm">{queryStatus.Status?.State} {queryExecuting ? '- Running...' : ''}</div>
					<div class="text-xs text-gray-500 mt-0.5">
						Scanned: {formatBytes(queryStatus.Statistics?.DataScannedInBytes)} | Runtime: {((queryStatus.Statistics?.TotalExecutionTimeInMillis ?? 0) / 1000).toFixed(2)}s
					</div>
				</div>
			</div>
		{/if}

		<!-- Results -->
		{#if queryResults && (queryResults.Rows ?? []).length > 0}
			<div>
				<div class="flex items-center justify-between mb-2">
					<div class="text-sm font-semibold text-gray-700 dark:text-gray-300">Results ({resultRows.length} rows)</div>
					<div class="flex items-center gap-2">
						<button onclick={exportResultsCsv} class="px-2.5 py-1 text-xs rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700">Export CSV</button>
						<button onclick={exportResultsJson} class="px-2.5 py-1 text-xs rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700">Export JSON</button>
					</div>
				</div>
				<div class="overflow-auto max-h-96 rounded-xl border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase sticky top-0">
							<tr>
								{#each columnHeaders as col}
									<th class="px-4 py-3 text-left whitespace-nowrap">{col}</th>
								{/each}
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800 bg-white dark:bg-gray-900">
							{#each resultRows as row}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
									{#each row as cell}
										<td class="px-4 py-2 whitespace-nowrap">{cell}</td>
									{/each}
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</div>
		{/if}
	{/if}

	<!-- WORKGROUPS -->
	{#if activeTab === 'workgroups'}
		{#if loadingWorkgroups}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-teal-600 border-t-transparent rounded-full"></div></div>
		{:else if workgroups.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Database class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No workgroups found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Name</th>
							<th class="px-4 py-3 text-left">State</th>
							<th class="px-4 py-3 text-left">Description</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each workgroups as wg}
							<tr>
								<td class="px-4 py-3 font-medium text-teal-600 dark:text-teal-400">{wg.Name}</td>
								<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${wg.State === 'ENABLED' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{wg.State}</span></td>
								<td class="px-4 py-3 text-xs text-gray-500">{wg.Description ?? '-'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- DATA CATALOGS -->
	{#if activeTab === 'catalogs'}
		{#if loadingCatalogs}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-teal-600 border-t-transparent rounded-full"></div></div>
		{:else if catalogs.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Table class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No data catalogs found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Catalog Name</th>
							<th class="px-4 py-3 text-left">Type</th>
							<th class="px-4 py-3 text-left">Description</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each catalogs as cat}
							<tr>
								<td class="px-4 py-3 font-medium">{cat.CatalogName}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{cat.Type}</td>
								<td class="px-4 py-3 text-xs text-gray-500">-</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- SESSIONS -->
	{#if activeTab === 'sessions'}
		<div class="flex items-center justify-between">
			<div class="flex items-center gap-3">
				<label for="session-wg" class="text-xs font-medium text-gray-600 dark:text-gray-400">Workgroup:</label>
				<select id="session-wg" bind:value={newSessionWorkgroup} class="px-3 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm">
					<option value="primary">primary</option>
					{#each workgroups as wg}
						<option value={wg.Name}>{wg.Name}</option>
					{/each}
				</select>
			</div>
			<div class="flex gap-2">
				<button onclick={loadSessions} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">
					<RefreshCw class="w-4 h-4" /> Refresh
				</button>
				<button onclick={startSession} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm font-medium">
					<Play class="w-4 h-4" /> Start Session
				</button>
			</div>
		</div>
		{#if loadingSessions}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-teal-600 border-t-transparent rounded-full"></div></div>
		{:else if sessions.length === 0}
			<div class="text-center py-12 text-gray-500"><Terminal class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No active sessions in workgroup "{newSessionWorkgroup}"</p></div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Session ID</th>
							<th class="px-4 py-3 text-left">Status</th>
							<th class="px-4 py-3 text-left">Description</th>
							<th class="px-4 py-3 text-left">Created</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each sessions as session}
							<tr>
								<td class="px-4 py-3 font-mono text-xs">{session.SessionId ?? '-'}</td>
								<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${session.Status?.State === 'IDLE' || session.Status?.State === 'BUSY' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{session.Status?.State ?? '-'}</span></td>
								<td class="px-4 py-3 text-xs text-gray-500">{session.Description ?? '-'}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{formatDate(session.Status?.StartDateTime)}</td>
								<td class="px-4 py-3">
									{#if session.Status?.State !== 'TERMINATED' && session.Status?.State !== 'TERMINATING'}
										<button onclick={() => terminateSession(session.SessionId ?? '')} class="text-red-500 hover:text-red-700 p-1"><XCircle class="w-4 h-4" /></button>
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- NOTEBOOKS -->
	{#if activeTab === 'notebooks'}
		<div class="flex items-center justify-between mb-4">
			<div class="flex items-center gap-3">
				<label for="nb-wg" class="text-xs font-medium text-gray-600 dark:text-gray-400">Workgroup:</label>
				<select id="nb-wg" bind:value={newNotebookWorkgroup} onchange={loadNotebooks} class="px-3 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm">
					<option value="primary">primary</option>
					{#each workgroups as wg}
						<option value={wg.Name}>{wg.Name}</option>
					{/each}
				</select>
			</div>
			<div class="flex gap-2 items-center">
				<input bind:value={newNotebookName} type="text" placeholder="Notebook name..." class="px-3 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
				<button onclick={createNotebook} disabled={!newNotebookName.trim()} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm font-medium disabled:opacity-50">
					<BookOpen class="w-4 h-4" /> Create
				</button>
			</div>
		</div>
		{#if loadingNotebooks}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-teal-600 border-t-transparent rounded-full"></div></div>
		{:else if notebooks.length === 0}
			<div class="text-center py-12 text-gray-500"><BookOpen class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No notebooks found in workgroup "{newNotebookWorkgroup}"</p></div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Notebook ID</th>
							<th class="px-4 py-3 text-left">Name</th>
							<th class="px-4 py-3 text-left">Type</th>
							<th class="px-4 py-3 text-left">Created</th>
							<th class="px-4 py-3 text-left">Last Modified</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each notebooks as nb}
							<tr>
								<td class="px-4 py-3 font-mono text-xs">{nb.NotebookId ?? '-'}</td>
								<td class="px-4 py-3 font-medium">{nb.Name ?? '-'}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{nb.Type ?? '-'}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{formatDate(nb.CreationTime)}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{formatDate(nb.LastModifiedTime)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- PREPARED STATEMENTS -->
	{#if activeTab === 'prepared'}
		<div class="flex justify-end mb-4">
			<button onclick={loadPreparedStatements} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
		{#if loadingPrepared}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-teal-600 border-t-transparent rounded-full"></div></div>
		{:else if preparedStatements.length === 0}
			<div class="text-center py-12 text-gray-500"><Database class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No prepared statements in workgroup "{queryWorkgroup || 'primary'}"</p></div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Statement Name</th>
							<th class="px-4 py-3 text-left">Last Modified</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each preparedStatements as stmt}
							<tr>
								<td class="px-4 py-3 font-medium">{stmt.StatementName ?? '-'}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{formatDate(stmt.LastModifiedTime)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- QUERY HISTORY -->
	{#if activeTab === 'history'}
		<div class="flex justify-end">
			<button onclick={loadHistory} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
		{#if loadingHistory}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-teal-600 border-t-transparent rounded-full"></div></div>
		{:else if historyDetail.length === 0}
			<div class="text-center py-12 text-gray-500"><Clock class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No recent queries</p></div>
		{:else}
			<div class="space-y-3">
				{#each historyDetail as qe}
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
						<div class="flex items-start justify-between gap-4">
							<div class="flex-1 min-w-0">
								<code class="text-xs text-gray-700 dark:text-gray-300 truncate block">{qe.Query}</code>
								<div class="flex gap-3 mt-2 text-xs text-gray-500">
									<span>{formatDate(qe.Status?.SubmissionDateTime)}</span>
									<span>Scanned: {formatBytes(qe.Statistics?.DataScannedInBytes)}</span>
									<span>{((qe.Statistics?.TotalExecutionTimeInMillis ?? 0) / 1000).toFixed(2)}s</span>
								</div>
							</div>
							<div class="flex items-center gap-2">
								<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(qe.Status?.State)}-100 text-${statusColor(qe.Status?.State)}-700 whitespace-nowrap`}>
									{qe.Status?.State}
								</span>
								<button onclick={() => { queryText = qe.Query ?? ''; activeTab = 'query'; }} class="text-teal-600 text-xs hover:underline whitespace-nowrap">Reuse</button>
							</div>
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{/if}
</div>
