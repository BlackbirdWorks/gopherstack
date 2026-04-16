<script lang="ts">
	import { onMount } from 'svelte';
	import { getDMSClient } from '$lib/aws-client';
	import {
		DescribeReplicationInstancesCommand,
		DescribeReplicationTasksCommand,
		type ReplicationInstance,
		type ReplicationTask
	} from '@aws-sdk/client-database-migration-service';
	import { toast } from 'svelte-sonner';
	import { Server, Search, RefreshCw, ChevronRight, GitBranch } from 'lucide-svelte';

	const client = getDMSClient();

	let loading = $state(false);
	let instances = $state<ReplicationInstance[]>([]);
	let tasks = $state<ReplicationTask[]>([]);
	let selectedInstance = $state<ReplicationInstance | null>(null);
	let activeTab = $state<'instances' | 'tasks'>('instances');
	let searchQuery = $state('');

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

	const runningCount = $derived(tasks.filter((t) => t.Status === 'running').length);
	const failedCount = $derived(tasks.filter((t) => t.Status === 'failed').length);

	async function loadData() {
		loading = true;
		try {
			const [instancesResp, tasksResp] = await Promise.all([
				client.send(new DescribeReplicationInstancesCommand({})),
				client.send(new DescribeReplicationTasksCommand({}))
			]);
			instances = instancesResp.ReplicationInstances ?? [];
			tasks = tasksResp.ReplicationTasks ?? [];
		} catch (e) {
			toast.error('Failed to load DMS data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

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
			<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg">
				<Server class="w-5 h-5 text-red-600 dark:text-red-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{failedCount}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Failed</p>
			</div>
		</div>
	</div>

	{#if selectedInstance}
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
	{:else}
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['instances', 'Replication Instances'], ['tasks', 'Tasks']] as [tab, label]}
				<button
					onclick={() => { activeTab = tab as 'instances' | 'tasks'; searchQuery = ''; }}
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
		{:else}
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
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredTasks as task}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{task.ReplicationTaskIdentifier ?? '-'}</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${task.Status === 'running' ? 'bg-green-100 text-green-700' : task.Status === 'failed' ? 'bg-red-100 text-red-700' : 'bg-gray-100 text-gray-700'}`}>{task.Status ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{task.MigrationType ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>
