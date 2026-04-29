<script lang="ts">
	import { onMount } from 'svelte';
	import { getRedshiftDataClient } from '$lib/aws-client';
	import {
		ExecuteStatementCommand,
		DescribeStatementCommand,
		ListStatementsCommand,
		CancelStatementCommand,
		ListDatabasesCommand,
		ListSchemasCommand,
		ListTablesCommand,
		type StatementData
	} from '@aws-sdk/client-redshift-data';
	import { toast } from 'svelte-sonner';
	import { Database, Play, XCircle, RefreshCw, Clock, Table, ChevronRight, Search } from 'lucide-svelte';

	const client = getRedshiftDataClient();

	let activeTab = $state<'query' | 'history' | 'schema'>('query');

	// --- Connection settings ---
	let database = $state('dev');
	let workgroupName = $state('');
	let clusterIdentifier = $state('');
	let dbUser = $state('');
	let secretArn = $state('');
	let statementName = $state('');

	// --- SQL editor ---
	let sqlText = $state('SELECT 1;');
	let executing = $state(false);
	let currentStatementId = $state<string | null>(null);
	let statementStatus = $state<string | null>(null);
	let statementError = $state<string | null>(null);

	// --- Results ---
	let resultColumns = $state<string[]>([]);
	let resultRows = $state<string[][]>([]);

	// --- History ---
	let historyStatements = $state<StatementData[]>([]);
	let historyLoading = $state(false);

	// --- Schema browser ---
	let databases = $state<string[]>([]);
	let schemas = $state<string[]>([]);
	let tables = $state<{ name: string; schema: string; type: string }[]>([]);
	let schemaLoading = $state(false);
	let selectedSchema = $state<string | null>(null);
	let schemaSearch = $state('');

	// Derived
	const filteredTables = $derived(
		tables.filter(
			(t) =>
				!schemaSearch ||
				t.name.toLowerCase().includes(schemaSearch.toLowerCase()) ||
				t.schema.toLowerCase().includes(schemaSearch.toLowerCase())
		)
	);

	function statusBadgeClass(status: string | undefined): string {
		switch (status) {
			case 'FINISHED':
				return 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300';
			case 'FAILED':
			case 'ABORTED':
				return 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300';
			case 'STARTED':
			case 'SUBMITTED':
			case 'PICKED':
				return 'bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300';
			default:
				return 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300';
		}
	}

	function formatDate(epoch: number | undefined): string {
		if (!epoch) return '-';
		return new Date(epoch * 1000).toLocaleString();
	}

	function formatStatementDate(createdAt: Date | undefined): string {
		if (!createdAt) return '-';
		return createdAt.toLocaleString();
	}

	function handleReuseQuery(queryString: string) {
		sqlText = queryString;
		activeTab = 'query';
	}

	function handleTableQuery(schema: string, tableName: string) {
		sqlText = `SELECT * FROM "${schema}"."${tableName}" LIMIT 10;`;
		activeTab = 'query';
	}

	// Execute statement
	async function executeStatement() {
		if (!sqlText.trim()) return;
		executing = true;
		statementStatus = null;
		statementError = null;
		resultColumns = [];
		resultRows = [];
		currentStatementId = null;

		try {
			const resp = await client.send(
				new ExecuteStatementCommand({
					Sql: sqlText.trim(),
					Database: database || undefined,
					WorkgroupName: workgroupName || undefined,
					ClusterIdentifier: clusterIdentifier || undefined,
					DbUser: dbUser || undefined,
					SecretArn: secretArn || undefined,
					StatementName: statementName || undefined
				})
			);
			currentStatementId = resp.Id ?? null;
			toast.success('Statement submitted: ' + currentStatementId);
			if (currentStatementId) {
				await pollStatement(currentStatementId);
			}
		} catch (e) {
			statementError = String(e);
			toast.error('Failed to execute: ' + String(e));
			executing = false;
		}
	}

	async function pollStatement(id: string) {
		let attempts = 0;
		const poll = async () => {
			attempts++;
			try {
				const resp = await client.send(new DescribeStatementCommand({ Id: id }));
				statementStatus = resp.Status ?? null;
				if (resp.Status === 'FINISHED') {
					executing = false;
					if (resp.HasResultSet) {
						await loadResults(id);
					}
				} else if (resp.Status === 'FAILED' || resp.Status === 'ABORTED') {
					executing = false;
					statementError = resp.Error ?? resp.Status ?? 'Statement failed';
					toast.error('Statement ' + resp.Status?.toLowerCase() + (resp.Error ? ': ' + resp.Error : ''));
				} else if (attempts > 30) {
					executing = false;
					toast.warning('Statement is taking long. Check history for status.');
				} else {
					setTimeout(poll, 1000);
				}
			} catch (e) {
				executing = false;
				statementError = String(e);
			}
		};
		await poll();
	}

	async function loadResults(id: string) {
		try {
			// The mock backend returns empty records/columns; we reflect that
			const { GetStatementResultCommand } = await import('@aws-sdk/client-redshift-data');
			const resp = await client.send(new GetStatementResultCommand({ Id: id }));
			const colInfo = resp.ColumnMetadata ?? [];
			resultColumns = colInfo.map((c) => c.name ?? c.label ?? '');
			resultRows = (resp.Records ?? []).map((row) =>
				row.map((field) => {
					if (field.isNull) return 'NULL';
					if (field.stringValue !== undefined) return field.stringValue;
					if (field.longValue !== undefined) return String(field.longValue);
					if (field.doubleValue !== undefined) return String(field.doubleValue);
					if (field.booleanValue !== undefined) return String(field.booleanValue);
					if (field.blobValue !== undefined) return '<blob>';
					return '';
				})
			);
		} catch (e) {
			toast.error('Failed to load results: ' + String(e));
		}
	}

	async function cancelStatement() {
		if (!currentStatementId) return;
		try {
			await client.send(new CancelStatementCommand({ Id: currentStatementId }));
			toast.success('Cancel requested');
			executing = false;
			statementStatus = 'ABORTED';
		} catch (e) {
			toast.error('Failed to cancel: ' + String(e));
		}
	}

	async function loadHistory() {
		historyLoading = true;
		try {
			const resp = await client.send(
				new ListStatementsCommand({
					WorkgroupName: workgroupName || undefined
				})
			);
			historyStatements = resp.Statements ?? [];
		} catch (e) {
			toast.error('Failed to load history: ' + String(e));
		} finally {
			historyLoading = false;
		}
	}

	async function loadSchema() {
		schemaLoading = true;
		databases = [];
		schemas = [];
		tables = [];
		selectedSchema = null;
		try {
			const dbResp = await client.send(
				new ListDatabasesCommand({
					Database: database || undefined,
					WorkgroupName: workgroupName || undefined,
					ClusterIdentifier: clusterIdentifier || undefined
				})
			);
			databases = dbResp.Databases ?? [];

			const schemaResp = await client.send(
				new ListSchemasCommand({
					Database: database || undefined,
					WorkgroupName: workgroupName || undefined,
					ClusterIdentifier: clusterIdentifier || undefined,
					SchemaPattern: '%'
				})
			);
			schemas = schemaResp.Schemas ?? [];

			const tableResp = await client.send(
				new ListTablesCommand({
					Database: database || undefined,
					WorkgroupName: workgroupName || undefined,
					ClusterIdentifier: clusterIdentifier || undefined,
					SchemaPattern: '%',
					TablePattern: '%'
				})
			);
			tables = (tableResp.Tables ?? []).map((t) => ({
				name: t.name ?? '',
				schema: t.schema ?? '',
				type: t.type ?? ''
			}));
		} catch (e) {
			toast.error('Failed to load schema: ' + String(e));
		} finally {
			schemaLoading = false;
		}
	}

	async function handleTabChange(tab: 'query' | 'history' | 'schema') {
		activeTab = tab;
		if (tab === 'history') await loadHistory();
		if (tab === 'schema') await loadSchema();
	}

	onMount(async () => {
		// pre-load history on mount
		await loadHistory();
	});
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center gap-3">
		<Database class="w-7 h-7 text-blue-500" />
		<div>
			<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Redshift Data API</h1>
			<p class="text-sm text-gray-500 dark:text-gray-400">Run SQL statements against Amazon Redshift</p>
		</div>
	</div>

	<!-- Connection settings -->
	<details class="rounded-xl border border-gray-200 dark:border-gray-700">
		<summary class="px-4 py-3 text-sm font-medium cursor-pointer select-none text-gray-700 dark:text-gray-300 flex items-center gap-2">
			<ChevronRight class="w-4 h-4 transition-transform [[open]_&]:rotate-90" /> Connection Settings
		</summary>
		<div class="p-4 grid grid-cols-2 gap-3 text-sm border-t border-gray-200 dark:border-gray-700">
			<div>
				<label for="rd-database" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Database</label>
				<input id="rd-database" bind:value={database} type="text" placeholder="dev" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
			</div>
			<div>
				<label for="rd-workgroup" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Workgroup Name</label>
				<input id="rd-workgroup" bind:value={workgroupName} type="text" placeholder="default (serverless)" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
			</div>
			<div>
				<label for="rd-cluster" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Cluster Identifier</label>
				<input id="rd-cluster" bind:value={clusterIdentifier} type="text" placeholder="my-cluster (provisioned)" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
			</div>
			<div>
				<label for="rd-dbuser" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">DB User</label>
				<input id="rd-dbuser" bind:value={dbUser} type="text" placeholder="admin" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
			</div>
			<div>
				<label for="rd-secret" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Secret ARN</label>
				<input id="rd-secret" bind:value={secretArn} type="text" placeholder="arn:aws:secretsmanager:..." class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
			</div>
			<div>
				<label for="rd-stmtname" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Statement Name</label>
				<input id="rd-stmtname" bind:value={statementName} type="text" placeholder="optional label" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
			</div>
		</div>
	</details>

	<!-- Tabs -->
	<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
		{#each [['query', 'SQL Editor'], ['history', 'Query History'], ['schema', 'Schema Browser']] as [tab, label]}
			<button
				onclick={() => handleTabChange(tab as 'query' | 'history' | 'schema')}
				class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-blue-500 text-blue-600 dark:text-blue-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'}`}
			>
				{label}
			</button>
		{/each}
	</div>

	<!-- SQL EDITOR -->
	{#if activeTab === 'query'}
		<div>
			<div class="flex items-center justify-between mb-2">
				<label for="rd-sql" class="block text-sm font-semibold text-gray-700 dark:text-gray-300">SQL Statement</label>
				<div class="flex gap-2">
					{#if executing}
						<button onclick={cancelStatement} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm font-medium">
							<XCircle class="w-4 h-4" /> Cancel
						</button>
					{:else}
						<button onclick={executeStatement} disabled={!sqlText.trim() || !database.trim()} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm font-medium disabled:opacity-50">
							<Play class="w-4 h-4" /> Run
						</button>
					{/if}
				</div>
			</div>
			<textarea
				id="rd-sql"
				bind:value={sqlText}
				rows={8}
				class="w-full px-4 py-3 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono text-gray-900 dark:text-white resize-y focus:ring-2 focus:ring-blue-500 focus:border-transparent"
				placeholder="SELECT * FROM my_table LIMIT 10;"
			></textarea>
		</div>

		<!-- Status banner -->
		{#if statementStatus}
			<div class={`flex items-center gap-3 p-4 rounded-xl border ${statementStatus === 'FINISHED' ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800' : statementStatus === 'FAILED' || statementStatus === 'ABORTED' ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800' : 'bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800'}`}>
				{#if executing}
					<div class="animate-spin w-4 h-4 border-2 border-blue-600 border-t-transparent rounded-full"></div>
				{/if}
				<div>
					<span class={`text-sm font-medium px-2 py-0.5 rounded ${statusBadgeClass(statementStatus)}`}>{statementStatus}</span>
					{#if currentStatementId}
						<span class="ml-2 text-xs text-gray-500 font-mono">{currentStatementId}</span>
					{/if}
					{#if statementError}
						<p class="mt-1 text-sm text-red-600 dark:text-red-400">{statementError}</p>
					{/if}
				</div>
			</div>
		{/if}

		<!-- Results table -->
		{#if resultColumns.length > 0 || resultRows.length > 0}
			<div>
				<div class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">
					Results ({resultRows.length} row{resultRows.length === 1 ? '' : 's'})
				</div>
				<div class="overflow-auto max-h-96 rounded-xl border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase sticky top-0">
							<tr>
								{#each resultColumns as col}
									<th class="px-4 py-3 text-left whitespace-nowrap">{col}</th>
								{/each}
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800 bg-white dark:bg-gray-900">
							{#each resultRows as row}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
									{#each row as cell}
										<td class="px-4 py-2 whitespace-nowrap font-mono text-xs">{cell}</td>
									{/each}
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</div>
		{:else if statementStatus === 'FINISHED'}
			<div class="text-center py-8 text-gray-500 dark:text-gray-400 text-sm">
				<Table class="w-10 h-10 mx-auto mb-2 opacity-40" />
				<p>Statement finished with no result set.</p>
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
		{#if historyLoading}
			<div class="flex justify-center py-12">
				<div class="animate-spin w-8 h-8 border-4 border-blue-600 border-t-transparent rounded-full"></div>
			</div>
		{:else if historyStatements.length === 0}
			<div class="text-center py-12 text-gray-500 dark:text-gray-400">
				<Clock class="w-10 h-10 mx-auto mb-2 opacity-40" />
				<p>No recent statements</p>
			</div>
		{:else}
			<div class="space-y-2">
				{#each historyStatements as stmt}
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
						<div class="flex items-start justify-between gap-4">
							<div class="flex-1 min-w-0">
								<code class="text-xs text-gray-700 dark:text-gray-300 break-all block">{stmt.QueryString ?? (stmt.IsBatchStatement ? '(batch statement)' : '-')}</code>
								{#if stmt.StatementName}
									<p class="text-xs text-gray-400 mt-1">Name: {stmt.StatementName}</p>
								{/if}
								<div class="flex flex-wrap gap-3 mt-2 text-xs text-gray-500">
									<span>Created: {formatStatementDate(stmt.CreatedAt)}</span>
									{#if stmt.IsBatchStatement}
										<span class="text-xs bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300 px-1.5 py-0.5 rounded">batch</span>
									{/if}
								</div>
							</div>
							<div class="flex flex-col items-end gap-2 shrink-0">
								<span class={`px-2 py-0.5 rounded text-xs font-medium ${statusBadgeClass(stmt.Status)}`}>
									{stmt.Status}
								</span>
								{#if stmt.QueryString}
									<button
										onclick={() => handleReuseQuery(stmt.QueryString ?? '')}
										class="text-blue-600 dark:text-blue-400 text-xs hover:underline"
									>
										Reuse
									</button>
								{/if}
							</div>
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{/if}

	<!-- SCHEMA BROWSER -->
	{#if activeTab === 'schema'}
		<div class="flex justify-between items-center">
			<div class="relative flex-1 max-w-xs">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input
					bind:value={schemaSearch}
					type="text"
					placeholder="Filter tables..."
					class="pl-9 pr-4 py-2 w-full text-sm rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900"
				/>
			</div>
			<button onclick={loadSchema} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 ml-3">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>

		{#if schemaLoading}
			<div class="flex justify-center py-12">
				<div class="animate-spin w-8 h-8 border-4 border-blue-600 border-t-transparent rounded-full"></div>
			</div>
		{:else}
			<!-- Databases -->
			{#if databases.length > 0}
				<div>
					<h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Databases</h3>
					<div class="flex flex-wrap gap-2">
						{#each databases as db}
							<span class="px-3 py-1 rounded-full bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300 text-xs font-medium">{db}</span>
						{/each}
					</div>
				</div>
			{/if}

			<!-- Schemas -->
			{#if schemas.length > 0}
				<div>
					<h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Schemas</h3>
					<div class="flex flex-wrap gap-2">
						{#each schemas as schema}
							<button
								onclick={() => selectedSchema = selectedSchema === schema ? null : schema}
								class={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${selectedSchema === schema ? 'bg-blue-500 text-white' : 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
							>
								{schema}
							</button>
						{/each}
					</div>
				</div>
			{/if}

			<!-- Tables -->
			{#if filteredTables.length > 0}
				<div>
					<h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Tables</h3>
					<div class="overflow-auto max-h-96 rounded-xl border border-gray-200 dark:border-gray-700">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase sticky top-0">
								<tr>
									<th class="px-4 py-3 text-left">Schema</th>
									<th class="px-4 py-3 text-left">Table</th>
									<th class="px-4 py-3 text-left">Type</th>
									<th class="px-4 py-3 text-left">Action</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800 bg-white dark:bg-gray-900">
								{#each filteredTables as t}
									<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
										<td class="px-4 py-2 text-gray-500">{t.schema}</td>
										<td class="px-4 py-2 font-medium text-blue-600 dark:text-blue-400">{t.name}</td>
										<td class="px-4 py-2 text-xs text-gray-400">{t.type}</td>
										<td class="px-4 py-2">
											<button
												onclick={() => handleTableQuery(t.schema, t.name)}
												class="text-xs text-blue-600 dark:text-blue-400 hover:underline"
											>
												Query
											</button>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{:else if !schemaLoading}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Database class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No tables found</p>
					<p class="text-sm mt-1">The mock backend returns an empty schema. Connect to a real Redshift endpoint to browse tables.</p>
				</div>
			{/if}
		{/if}
	{/if}
</div>
