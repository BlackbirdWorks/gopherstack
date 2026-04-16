<script lang="ts">
	import { onMount } from 'svelte';
	import { getDocDBClient } from '$lib/aws-client';
	import {
		DescribeDBClustersCommand,
		DescribeDBInstancesCommand,
		type DBCluster,
		type DBInstance
	} from '@aws-sdk/client-docdb';
	import { toast } from 'svelte-sonner';
	import { Database, Search, RefreshCw, ChevronRight, Server } from 'lucide-svelte';

	const client = getDocDBClient();

	let loading = $state(false);
	let dbClusters = $state<DBCluster[]>([]);
	let dbInstances = $state<DBInstance[]>([]);
	let selectedCluster = $state<DBCluster | null>(null);
	let activeTab = $state<'clusters' | 'instances'>('clusters');
	let searchQuery = $state('');

	const filteredClusters = $derived(
		dbClusters.filter(
			(c) => !searchQuery || (c.DBClusterIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredInstances = $derived(
		dbInstances.filter(
			(i) => !searchQuery || (i.DBInstanceIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const availableCount = $derived(dbClusters.filter((c) => c.Status === 'available').length);
	const creatingCount = $derived(dbClusters.filter((c) => c.Status === 'creating').length);

	async function loadData() {
		loading = true;
		try {
			const [clustersResp, instancesResp] = await Promise.all([
				client.send(new DescribeDBClustersCommand({})),
				client.send(new DescribeDBInstancesCommand({}))
			]);
			dbClusters = clustersResp.DBClusters ?? [];
			dbInstances = instancesResp.DBInstances ?? [];
		} catch (e) {
			toast.error('Failed to load DocumentDB data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Database class="w-7 h-7 text-green-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon DocumentDB</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">MongoDB-compatible document database service</p>
			</div>
		</div>
		<button onclick={loadData} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<!-- Stat Cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<Database class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{dbClusters.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Clusters</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Server class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{dbInstances.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Instances</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-emerald-100 dark:bg-emerald-900/30 rounded-lg">
				<Database class="w-5 h-5 text-emerald-600 dark:text-emerald-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{availableCount}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Available</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-yellow-100 dark:bg-yellow-900/30 rounded-lg">
				<Database class="w-5 h-5 text-yellow-600 dark:text-yellow-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{creatingCount}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Creating</p>
			</div>
		</div>
	</div>

	{#if selectedCluster}
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedCluster = null; }} class="text-green-600 hover:underline">Clusters</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="font-medium text-gray-700 dark:text-gray-300">{selectedCluster.DBClusterIdentifier}</span>
		</div>
		<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
			{#each [
				{ label: 'Cluster ARN', value: selectedCluster.DBClusterArn ?? '-' },
				{ label: 'Engine Version', value: selectedCluster.EngineVersion ?? '-' },
				{ label: 'Status', value: selectedCluster.Status ?? '-' },
				{ label: 'VPC ID', value: selectedCluster.DbClusterResourceId ?? '-' },
				{ label: 'Multi-AZ', value: selectedCluster.MultiAZ ? 'Yes' : 'No' },
				{ label: 'Endpoint', value: selectedCluster.Endpoint ?? '-' }
			] as row}
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500 font-medium">{row.label}</div>
					<div class="text-sm text-gray-900 dark:text-white mt-1 font-mono truncate">{row.value}</div>
				</div>
			{/each}
		</div>
	{:else}
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['clusters', 'Clusters'], ['instances', 'Instances']] as [tab, label]}
				<button
					onclick={() => { activeTab = tab as 'clusters' | 'instances'; searchQuery = ''; }}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-green-500 text-green-600 dark:text-green-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
				>{label}</button>
			{/each}
		</div>

		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
		{:else if activeTab === 'clusters'}
			{#if filteredClusters.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Database class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No clusters found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Identifier</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Engine Version</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredClusters as cluster}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3">
										<button onclick={() => { selectedCluster = cluster; }} class="text-green-600 dark:text-green-400 hover:underline font-medium">{cluster.DBClusterIdentifier ?? '-'}</button>
									</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${cluster.Status === 'available' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{cluster.Status ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{cluster.EngineVersion ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else}
			{#if filteredInstances.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Server class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No instances found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Identifier</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Class</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredInstances as inst}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{inst.DBInstanceIdentifier ?? '-'}</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${inst.DBInstanceStatus === 'available' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{inst.DBInstanceStatus ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{inst.DBInstanceClass ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>
