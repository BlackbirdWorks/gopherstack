<script lang="ts">
	import { onMount } from 'svelte';
	import { getMemoryDBClient } from '$lib/aws-client';
	import {
		DescribeClustersCommand,
		DescribeSnapshotsCommand,
		type Cluster,
		type Snapshot
	} from '@aws-sdk/client-memorydb';
	import { toast } from 'svelte-sonner';
	import { Database, Search, RefreshCw, ChevronRight, Layers } from 'lucide-svelte';

	const client = getMemoryDBClient();

	let loading = $state(false);
	let clusters = $state<Cluster[]>([]);
	let snapshots = $state<Snapshot[]>([]);
	let selectedCluster = $state<Cluster | null>(null);
	let activeTab = $state<'clusters' | 'snapshots'>('clusters');
	let searchQuery = $state('');

	const filteredClusters = $derived(
		clusters.filter(
			(c) => !searchQuery || (c.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredSnapshots = $derived(
		snapshots.filter(
			(s) => !searchQuery || (s.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const availableCount = $derived(clusters.filter((c) => c.Status === 'available').length);
	const totalShards = $derived(clusters.reduce((acc, c) => acc + (c.NumberOfShards ?? 0), 0));

	async function loadData() {
		loading = true;
		try {
			const [clustersResp, snapshotsResp] = await Promise.all([
				client.send(new DescribeClustersCommand({})),
				client.send(new DescribeSnapshotsCommand({}))
			]);
			clusters = clustersResp.Clusters ?? [];
			snapshots = snapshotsResp.Snapshots ?? [];
		} catch (e) {
			toast.error('Failed to load MemoryDB data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Database class="w-7 h-7 text-purple-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon MemoryDB</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Redis-compatible, durable, in-memory database service</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<!-- Stat Cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Database class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{clusters.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Total Clusters</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<Database class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{availableCount}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Available</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Layers class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{snapshots.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Snapshots</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Layers class="w-5 h-5 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{totalShards}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Shards</p>
			</div>
		</div>
	</div>

	{#if selectedCluster}
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedCluster = null; }} class="text-purple-600 hover:underline">Clusters</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="font-medium text-gray-700 dark:text-gray-300">{selectedCluster.Name}</span>
		</div>
		<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
			{#each [
				{ label: 'Cluster ARN', value: selectedCluster.ARN ?? '-' },
				{ label: 'Status', value: selectedCluster.Status ?? '-' },
				{ label: 'Node Type', value: selectedCluster.NodeType ?? '-' },
				{ label: 'Engine Version', value: selectedCluster.EngineVersion ?? '-' },
				{ label: 'Number of Shards', value: String(selectedCluster.NumberOfShards ?? 0) },
				{ label: 'Availability Mode', value: selectedCluster.AvailabilityMode ?? '-' }
			] as row}
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500 font-medium">{row.label}</div>
					<div class="text-sm text-gray-900 dark:text-white mt-1 font-mono truncate">{row.value}</div>
				</div>
			{/each}
		</div>
	{:else}
		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['clusters', 'Clusters'], ['snapshots', 'Snapshots']] as [tab, label]}
				<button
					onclick={() => { activeTab = tab as 'clusters' | 'snapshots'; searchQuery = ''; }}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
				>{label}</button>
			{/each}
		</div>

		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-purple-600 border-t-transparent rounded-full"></div></div>
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
								<th class="px-4 py-3 text-left">Name</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Node Type</th>
								<th class="px-4 py-3 text-left">Shards</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredClusters as cluster}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3">
										<button onclick={() => { selectedCluster = cluster; }} class="text-purple-600 dark:text-purple-400 hover:underline font-medium">{cluster.Name ?? '-'}</button>
									</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${cluster.Status === 'available' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{cluster.Status ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{cluster.NodeType ?? '-'}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{cluster.NumberOfShards ?? 0}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else}
			{#if filteredSnapshots.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Layers class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No snapshots found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Name</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Cluster</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredSnapshots as snap}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{snap.Name ?? '-'}</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${snap.Status === 'available' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{snap.Status ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{snap.ClusterConfiguration?.Name ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>
