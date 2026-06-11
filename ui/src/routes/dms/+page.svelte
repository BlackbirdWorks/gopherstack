<script lang="ts">
	import { onMount } from 'svelte';
	import { getDMSClient } from '$lib/aws-client';
	import {
		DescribeReplicationInstancesCommand,
		DescribeReplicationTasksCommand,
		DescribeEndpointsCommand,
		DescribeTableStatisticsCommand,
		ModifyEndpointCommand,
		TestConnectionCommand,
		DescribeConnectionsCommand,
		type ReplicationInstance,
		type ReplicationTask,
		type Endpoint,
		type TableStatistics,
		type Connection
	} from '@aws-sdk/client-database-migration-service';
	import { toast } from 'svelte-sonner';
	import { Server, Search, RefreshCw, ChevronRight, GitBranch, Plug, Table2, Edit2, X, Check, Plug2 } from 'lucide-svelte';

	const client = getDMSClient();

	let loading = $state(false);
	let instances = $state<ReplicationInstance[]>([]);
	let tasks = $state<ReplicationTask[]>([]);
	let endpoints = $state<Endpoint[]>([]);
	let selectedInstance = $state<ReplicationInstance | null>(null);
	let selectedTask = $state<ReplicationTask | null>(null);
	let tableStats = $state<TableStatistics[]>([]);
	let tableStatsLoading = $state(false);
	let activeTab = $state<'instances' | 'tasks' | 'endpoints'>('instances');
	let searchQuery = $state('');

	// Endpoint editor modal state
	let editingEndpoint = $state<Endpoint | null>(null);
	let editForm = $state({ serverName: '', databaseName: '', username: '', port: '' });
	let editSaving = $state(false);

	// Connection test modal state
	let testingEndpoint = $state<Endpoint | null>(null);
	let testInstanceArn = $state('');
	let testRunning = $state(false);
	let testResult = $state<Connection | null>(null);

	const filteredInstances = $derived(
		instances.filter(
			(i) => !searchQuery || (i.ReplicationInstanceIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredTasks = $derived(
		tasks.filter(
			(t) => !searchQuery || (t.ReplicationTaskIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredEndpoints = $derived(
		endpoints.filter(
			(e) => !searchQuery || (e.EndpointIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const runningCount = $derived(tasks.filter((t) => t.Status === 'running').length);
	const failedCount = $derived(tasks.filter((t) => t.Status === 'failed').length);

	async function loadData() {
		loading = true;
		try {
			const [instancesResp, tasksResp, endpointsResp] = await Promise.all([
				client.send(new DescribeReplicationInstancesCommand({})),
				client.send(new DescribeReplicationTasksCommand({})),
				client.send(new DescribeEndpointsCommand({}))
			]);
			instances = instancesResp.ReplicationInstances ?? [];
			tasks = tasksResp.ReplicationTasks ?? [];
			endpoints = endpointsResp.Endpoints ?? [];
		} catch (e) {
			toast.error('Failed to load DMS data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadTableStats(task: ReplicationTask) {
		if (!task.ReplicationTaskArn) return;
		selectedTask = task;
		tableStatsLoading = true;
		tableStats = [];
		try {
			const resp = await client.send(new DescribeTableStatisticsCommand({
				ReplicationTaskArn: task.ReplicationTaskArn
			}));
			tableStats = resp.TableStatistics ?? [];
		} catch (e) {
			toast.error('Failed to load table statistics: ' + String(e));
		} finally {
			tableStatsLoading = false;
		}
	}

	function openEndpointEditor(ep: Endpoint) {
		editingEndpoint = ep;
		editForm = {
			serverName: ep.ServerName ?? '',
			databaseName: ep.DatabaseName ?? '',
			username: ep.Username ?? '',
			port: ep.Port ? String(ep.Port) : ''
		};
	}

	function closeEndpointEditor() {
		editingEndpoint = null;
		editSaving = false;
	}

	async function saveEndpointChanges() {
		if (!editingEndpoint?.EndpointArn) return;
		editSaving = true;
		try {
			await client.send(new ModifyEndpointCommand({
				EndpointArn: editingEndpoint.EndpointArn,
				ServerName: editForm.serverName || undefined,
				DatabaseName: editForm.databaseName || undefined,
				Username: editForm.username || undefined,
				Port: editForm.port ? parseInt(editForm.port, 10) : undefined
			}));
			toast.success('Endpoint updated');
			closeEndpointEditor();
			await loadData();
		} catch (e) {
			toast.error('Failed to update endpoint: ' + String(e));
		} finally {
			editSaving = false;
		}
	}

	function openConnectionTest(ep: Endpoint) {
		testingEndpoint = ep;
		testResult = null;
		testRunning = false;
		testInstanceArn = instances[0]?.ReplicationInstanceArn ?? '';
	}

	function closeConnectionTest() {
		testingEndpoint = null;
		testInstanceArn = '';
		testResult = null;
		testRunning = false;
	}

	async function runConnectionTest() {
		if (!testingEndpoint?.EndpointArn || !testInstanceArn) return;
		testRunning = true;
		testResult = null;
		try {
			const resp = await client.send(
				new TestConnectionCommand({
					ReplicationInstanceArn: testInstanceArn,
					EndpointArn: testingEndpoint.EndpointArn
				})
			);
			testResult = resp.Connection ?? null;
			const endpointArn = testingEndpoint.EndpointArn;
			const instanceArn = testInstanceArn;
			// Poll DescribeConnections a few times until the test settles.
			for (let i = 0; i < 6; i++) {
				if (testResult?.Status && testResult.Status !== 'testing') break;
				await new Promise((resolve) => {
					setTimeout(resolve, 1500);
				});
				const dc = await client.send(
					new DescribeConnectionsCommand({
						Filters: [{ Name: 'endpoint-arn', Values: [endpointArn] }]
					})
				);
				const match = (dc.Connections ?? []).find(
					(c) => c.ReplicationInstanceArn === instanceArn
				);
				if (match) testResult = match;
			}
			if (testResult?.Status === 'successful') {
				toast.success('Connection test successful');
			} else if (testResult?.Status === 'failed') {
				toast.error('Connection test failed');
			}
		} catch (e) {
			toast.error('Failed to test connection: ' + String(e));
		} finally {
			testRunning = false;
		}
	}

	function parseTableMappings(tableMappings: string | undefined): { schema: string; table: string }[] {
		if (!tableMappings) return [];
		try {
			const parsed = JSON.parse(tableMappings);
			const rules: { schema: string; table: string }[] = [];
			for (const rule of parsed.rules ?? []) {
				if (rule['rule-type'] === 'selection') {
					rules.push({
						schema: rule['object-locator']?.['schema-name'] ?? '*',
						table: rule['object-locator']?.['table-name'] ?? '*'
					});
				}
			}
			return rules;
		} catch {
			return [];
		}
	}

	onMount(loadData);
</script>

<!-- Endpoint config editor modal -->
{#if editingEndpoint}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
		<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 shadow-xl w-full max-w-lg p-6">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Edit Endpoint: {editingEndpoint.EndpointIdentifier}</h2>
				<button onclick={closeEndpointEditor} class="text-gray-400 hover:text-gray-600"><X class="w-5 h-5" /></button>
			</div>
			<div class="space-y-4">
				<div>
					<label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Server Name</label>
					<input bind:value={editForm.serverName} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="e.g. db.example.com" />
				</div>
				<div>
					<label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Database Name</label>
					<input bind:value={editForm.databaseName} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="e.g. mydb" />
				</div>
				<div>
					<label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Username</label>
					<input bind:value={editForm.username} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="e.g. admin" />
				</div>
				<div>
					<label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Port</label>
					<input bind:value={editForm.port} type="number" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="e.g. 5432" />
				</div>
			</div>
			<div class="flex justify-end gap-2 mt-6">
				<button onclick={closeEndpointEditor} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={saveEndpointChanges} disabled={editSaving} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2">
					{#if editSaving}<div class="animate-spin w-4 h-4 border-2 border-white border-t-transparent rounded-full"></div>{:else}<Check class="w-4 h-4" />{/if}
					Save
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Connection test modal -->
{#if testingEndpoint}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
		<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 shadow-xl w-full max-w-lg p-6">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2"><Plug2 class="w-5 h-5 text-blue-500" /> Test Connection: {testingEndpoint.EndpointIdentifier}</h2>
				<button onclick={closeConnectionTest} class="text-gray-400 hover:text-gray-600"><X class="w-5 h-5" /></button>
			</div>
			<div class="space-y-4">
				<div>
					<label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Replication Instance</label>
					{#if instances.length === 0}
						<p class="text-sm text-gray-500">No replication instances available to test from.</p>
					{:else}
						<select bind:value={testInstanceArn} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
							{#each instances as inst}
								<option value={inst.ReplicationInstanceArn}>{inst.ReplicationInstanceIdentifier} ({inst.ReplicationInstanceClass})</option>
							{/each}
						</select>
					{/if}
				</div>
				{#if testResult}
					<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-3 space-y-2">
						<div class="flex items-center gap-2">
							<span class="text-xs text-gray-500">Status:</span>
							<span class={`px-2 py-0.5 rounded text-xs font-medium ${testResult.Status === 'successful' ? 'bg-green-100 text-green-700' : testResult.Status === 'failed' ? 'bg-red-100 text-red-700' : 'bg-yellow-100 text-yellow-700'}`}>{testResult.Status ?? '-'}</span>
						</div>
						{#if testResult.LastFailureMessage}
							<div class="text-xs text-red-600 dark:text-red-400 font-mono break-all">{testResult.LastFailureMessage}</div>
						{/if}
					</div>
				{/if}
			</div>
			<div class="flex justify-end gap-2 mt-6">
				<button onclick={closeConnectionTest} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
				<button onclick={runConnectionTest} disabled={testRunning || !testInstanceArn} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 flex items-center gap-2">
					{#if testRunning}<div class="animate-spin w-4 h-4 border-2 border-white border-t-transparent rounded-full"></div>{:else}<Plug2 class="w-4 h-4" />{/if}
					Run Test
				</button>
			</div>
		</div>
	</div>
{/if}

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<GitBranch class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Database Migration Service</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Migrate databases to AWS quickly and securely</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<!-- Stat Cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Server class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{instances.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Replication Instances</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<GitBranch class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{tasks.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Tasks</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<Server class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{runningCount}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Running</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Plug class="w-5 h-5 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{endpoints.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Endpoints</p>
			</div>
		</div>
	</div>

	{#if selectedInstance}
		<!-- Instance detail breadcrumb view -->
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedInstance = null; }} class="text-blue-600 hover:underline">Replication Instances</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="font-medium text-gray-700 dark:text-gray-300">{selectedInstance.ReplicationInstanceIdentifier}</span>
		</div>
		<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
			{#each [
				{ label: 'Instance ARN', value: selectedInstance.ReplicationInstanceArn ?? '-' },
				{ label: 'Instance Class', value: selectedInstance.ReplicationInstanceClass ?? '-' },
				{ label: 'Engine Version', value: selectedInstance.EngineVersion ?? '-' },
				{ label: 'Status', value: selectedInstance.ReplicationInstanceStatus ?? '-' },
				{ label: 'Multi-AZ', value: selectedInstance.MultiAZ ? 'Yes' : 'No' },
				{ label: 'Publicly Accessible', value: selectedInstance.PubliclyAccessible ? 'Yes' : 'No' }
			] as row}
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500 font-medium">{row.label}</div>
					<div class="text-sm text-gray-900 dark:text-white mt-1 font-mono truncate">{row.value}</div>
				</div>
			{/each}
		</div>
	{:else if selectedTask}
		<!-- Task detail with table mapping visualization -->
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedTask = null; tableStats = []; }} class="text-blue-600 hover:underline">Tasks</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="font-medium text-gray-700 dark:text-gray-300">{selectedTask.ReplicationTaskIdentifier}</span>
		</div>

		<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
			{#each [
				{ label: 'Task ARN', value: selectedTask.ReplicationTaskArn ?? '-' },
				{ label: 'Migration Type', value: selectedTask.MigrationType ?? '-' },
				{ label: 'Status', value: selectedTask.Status ?? '-' }
			] as row}
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500 font-medium">{row.label}</div>
					<div class="text-sm text-gray-900 dark:text-white mt-1 font-mono truncate">{row.value}</div>
				</div>
			{/each}
		</div>

		<!-- Table Mapping Visualization -->
		<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700">
			<div class="flex items-center gap-2 px-4 py-3 border-b border-gray-100 dark:border-gray-800">
				<Table2 class="w-4 h-4 text-gray-500" />
				<h3 class="text-sm font-semibold text-gray-900 dark:text-white">Table Mapping Rules</h3>
			</div>
			{#if parseTableMappings(selectedTask.TableMappings).length > 0}
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Schema</th>
								<th class="px-4 py-3 text-left">Table</th>
								<th class="px-4 py-3 text-left">Rule Type</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each parseTableMappings(selectedTask.TableMappings) as rule}
								<tr>
									<td class="px-4 py-3 font-mono text-blue-600 dark:text-blue-400">{rule.schema}</td>
									<td class="px-4 py-3 font-mono text-gray-800 dark:text-gray-200">{rule.table}</td>
									<td class="px-4 py-3"><span class="px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400">Include</span></td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else}
				<div class="px-4 py-3 text-sm text-gray-500">No table mapping rules configured.</div>
			{/if}
		</div>

		<!-- Live Table Statistics -->
		<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700">
			<div class="flex items-center justify-between px-4 py-3 border-b border-gray-100 dark:border-gray-800">
				<div class="flex items-center gap-2">
					<Table2 class="w-4 h-4 text-gray-500" />
					<h3 class="text-sm font-semibold text-gray-900 dark:text-white">Table Statistics</h3>
				</div>
				{#if tableStatsLoading}
					<div class="animate-spin w-4 h-4 border-2 border-blue-600 border-t-transparent rounded-full"></div>
				{/if}
			</div>
			{#if tableStats.length > 0}
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Schema</th>
								<th class="px-4 py-3 text-left">Table</th>
								<th class="px-4 py-3 text-right">Rows Loaded</th>
								<th class="px-4 py-3 text-right">Errors</th>
								<th class="px-4 py-3 text-left">State</th>
								<th class="px-4 py-3 text-left">Validation</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each tableStats as stat}
								<tr>
									<td class="px-4 py-3 font-mono text-xs text-blue-600 dark:text-blue-400">{stat.SchemaName ?? '-'}</td>
									<td class="px-4 py-3 font-mono text-xs">{stat.TableName ?? '-'}</td>
									<td class="px-4 py-3 text-right text-gray-700 dark:text-gray-300">{stat.FullLoadRows ?? 0}</td>
									<td class="px-4 py-3 text-right text-red-600 dark:text-red-400">{stat.FullLoadErrorRows ?? 0}</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${stat.TableState === 'Table completed' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-600'}`}>{stat.TableState ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-xs text-gray-500">{stat.ValidationState ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else if !tableStatsLoading}
				<div class="px-4 py-3 text-sm text-gray-500">No table statistics available.</div>
			{/if}
		</div>
	{:else}
		<!-- Main tab view -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['instances', 'Replication Instances'], ['tasks', 'Tasks'], ['endpoints', 'Endpoints']] as [tab, label]}
				<button
					onclick={() => { activeTab = tab as 'instances' | 'tasks' | 'endpoints'; searchQuery = ''; }}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-blue-500 text-blue-600 dark:text-blue-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
				>{label}</button>
			{/each}
		</div>

		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-blue-600 border-t-transparent rounded-full"></div></div>
		{:else if activeTab === 'instances'}
			{#if filteredInstances.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Server class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No replication instances found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Identifier</th>
								<th class="px-4 py-3 text-left">Class</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Engine Version</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredInstances as inst}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3">
										<button onclick={() => { selectedInstance = inst; }} class="text-blue-600 dark:text-blue-400 hover:underline font-medium">{inst.ReplicationInstanceIdentifier ?? '-'}</button>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{inst.ReplicationInstanceClass ?? '-'}</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${inst.ReplicationInstanceStatus === 'available' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{inst.ReplicationInstanceStatus ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{inst.EngineVersion ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if activeTab === 'tasks'}
			{#if filteredTasks.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<GitBranch class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No replication tasks found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Identifier</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Migration Type</th>
								<th class="px-4 py-3 text-left">Table Mappings</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredTasks as task}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3">
										<button onclick={() => loadTableStats(task)} class="text-blue-600 dark:text-blue-400 hover:underline font-medium">{task.ReplicationTaskIdentifier ?? '-'}</button>
									</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${task.Status === 'running' ? 'bg-green-100 text-green-700' : task.Status === 'failed' ? 'bg-red-100 text-red-700' : 'bg-gray-100 text-gray-700'}`}>{task.Status ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{task.MigrationType ?? '-'}</td>
									<td class="px-4 py-3 text-gray-500 text-xs">
										{#if parseTableMappings(task.TableMappings).length > 0}
											<span class="inline-flex items-center gap-1"><Table2 class="w-3 h-3" />{parseTableMappings(task.TableMappings).length} rule(s)</span>
										{:else}
											<span class="text-gray-400">None</span>
										{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else}
			<!-- Endpoints tab -->
			{#if filteredEndpoints.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Plug class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No endpoints found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Identifier</th>
								<th class="px-4 py-3 text-left">Type</th>
								<th class="px-4 py-3 text-left">Engine</th>
								<th class="px-4 py-3 text-left">Server</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left"></th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredEndpoints as ep}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{ep.EndpointIdentifier ?? '-'}</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${ep.EndpointType === 'source' ? 'bg-blue-100 text-blue-700' : 'bg-purple-100 text-purple-700'}`}>{ep.EndpointType ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{ep.EngineName ?? '-'}</td>
									<td class="px-4 py-3 text-gray-500 font-mono text-xs">{ep.ServerName ?? '-'}{ep.Port ? ':' + ep.Port : ''}</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${ep.Status === 'active' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{ep.Status ?? '-'}</span>
									</td>
									<td class="px-4 py-3">
										<div class="flex items-center gap-3">
											<button onclick={() => openConnectionTest(ep)} class="text-gray-400 hover:text-blue-600 transition-colors" title="Test connection">
												<Plug2 class="w-4 h-4" />
											</button>
											<button onclick={() => openEndpointEditor(ep)} class="text-gray-400 hover:text-blue-600 transition-colors" title="Edit endpoint">
												<Edit2 class="w-4 h-4" />
											</button>
										</div>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>
