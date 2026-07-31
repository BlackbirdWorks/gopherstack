<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getTimestreamQueryClient, getTimestreamWriteClient } from '$lib/aws-client';
	import {
		ListDatabasesCommand,
		ListTablesCommand as ListWriteTablesCommand,
		type Database,
		type Table
	} from '@aws-sdk/client-timestream-write';
	import {
		ListScheduledQueriesCommand,
		type ScheduledQuery
	} from '@aws-sdk/client-timestream-query';
	import { toast } from 'svelte-sonner';
	import { Clock, RefreshCw, Search, Database as DatabaseIcon, Table2, Activity } from 'lucide-svelte';

	const tsWrite = regionalClient(getTimestreamWriteClient);
	const tsQuery = regionalClient(getTimestreamQueryClient);

	let loading = $state(false);
	let activeTab = $state<'databases' | 'tables' | 'queries'>('databases');
	let searchQuery = $state('');
	let databases = $state<Database[]>([]);
	let tables = $state<Table[]>([]);
	let scheduledQueries = $state<ScheduledQuery[]>([]);
	let selectedDbName = $state<string | null>(null);

	const filteredDatabases = $derived(databases.filter((d) => (d.DatabaseName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredTables = $derived(tables.filter((t) => (t.TableName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredQueries = $derived(scheduledQueries.filter((q) => (q.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const [dbResp, qResp] = await Promise.all([
				tsWrite().send(new ListDatabasesCommand({})),
				tsQuery().send(new ListScheduledQueriesCommand({}))
			]);
			databases = dbResp.Databases ?? [];
			scheduledQueries = qResp.ScheduledQueries ?? [];
		} catch (e) {
			toast.error('Failed to load Timestream data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadTables(dbName: string) {
		selectedDbName = dbName;
		try {
			const resp = await tsWrite().send(new ListWriteTablesCommand({ DatabaseName: dbName }));
			tables = resp.Tables ?? [];
			activeTab = 'tables';
		} catch (e) {
			toast.error('Failed to load tables: ' + String(e));
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Clock class="w-7 h-7 text-cyan-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Timestream</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Fast, scalable, serverless time series database</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-cyan-100 dark:bg-cyan-900/30 rounded-lg"><DatabaseIcon class="w-5 h-5 text-cyan-600 dark:text-cyan-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{databases.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Databases</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Table2 class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{tables.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Tables (selected)</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Activity class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{scheduledQueries.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Scheduled Queries</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['databases', 'Databases'], ['tables', 'Tables'], ['queries', 'Scheduled Queries']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-cyan-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'databases'}
				{#if filteredDatabases.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No databases found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredDatabases as db}
							<button onclick={() => loadTables(db.DatabaseName ?? '')}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left">
								<div class="flex items-center gap-3">
									<DatabaseIcon class="w-5 h-5 text-cyan-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{db.DatabaseName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{db.TableCount} tables</p>
									</div>
								</div>
								<span class="text-xs text-gray-400">View tables →</span>
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'tables'}
				{#if filteredTables.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No tables found. Select a database to view its tables.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTables as table}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Table2 class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{table.TableName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{table.DatabaseName}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">{table.TableStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'queries'}
				{#if filteredQueries.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No scheduled queries found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredQueries as query}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-green-500" />
									<p class="font-medium text-gray-900 dark:text-white">{query.Name}</p>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{query.State}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
