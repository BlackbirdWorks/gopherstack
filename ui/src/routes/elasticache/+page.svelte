<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getElastiCacheClient } from '$lib/aws-client';
	import {
		DescribeCacheClustersCommand,
		CreateCacheClusterCommand,
		DeleteCacheClusterCommand,
		RebootCacheClusterCommand,
		type CacheCluster
	} from '@aws-sdk/client-elasticache';
	import { toast } from 'svelte-sonner';
	import { 
		Database, Search, RefreshCw, Plus, Trash2, 
		Activity, Server, Clock, HardDrive, Cpu, 
		ChevronRight, MoreVertical, Power
	} from 'lucide-svelte';

	const ec = getElastiCacheClient();

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let clusters = $state<CacheCluster[]>([]);
	let selectedCluster = $state<CacheCluster | null>(null);

	// Modal State
	let showCreateModal = $state(false);
	let newClusterId = $state('');
	let engine = $state('redis');
	let nodeType = $state('cache.t3.micro');
	let numNodes = $state(1);
	let creating = $state(false);

	// Derived
	const filteredClusters = $derived(
		clusters.filter(c => c.CacheClusterId?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function loadClusters() {
		loading = true;
		try {
			const res = await ec.send(new DescribeCacheClustersCommand({}));
			clusters = res.CacheClusters ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load clusters: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createCluster() {
		if (!newClusterId.trim()) return;
		creating = true;
		try {
			await ec.send(new CreateCacheClusterCommand({
				CacheClusterId: newClusterId.trim(),
				Engine: engine,
				CacheNodeType: nodeType,
				NumCacheNodes: numNodes
			}));
			toast.success(`Cluster "${newClusterId}" creation initiated`);
			showCreateModal = false;
			newClusterId = '';
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteCluster(id: string) {
		if (!await confirmDestructive(`Delete cluster "${id}"?`)) return;
		try {
			await ec.send(new DeleteCacheClusterCommand({ CacheClusterId: id }));
			toast.success(`Cluster "${id}" deletion initiated`);
			if (selectedCluster?.CacheClusterId === id) selectedCluster = null;
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function rebootCluster(id: string) {
		if (!await confirmDestructive(`Reboot cluster "${id}"?`)) return;
		try {
			await ec.send(new RebootCacheClusterCommand({ 
				CacheClusterId: id,
				// Default node
				CacheNodeIdsToReboot: ['0001']
			}));
			toast.success(`Cluster "${id}" reboot initiated`);
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Reboot failed: ${(err as Error).message}`);
		}
	}

	function getStatusColor(status: string | undefined): string {
		if (!status) return 'bg-slate-500';
		const s = status.toLowerCase();
		if (s === 'available') return 'bg-emerald-500';
		if (s === 'creating' || s === 'modifying' || s === 'rebooting') return 'bg-amber-500';
		if (s === 'deleting' || s === 'incompatible-network') return 'bg-rose-500';
		return 'bg-slate-500';
	}

	onMount(() => {
		loadClusters();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-cyan-500/20 rounded-xl">
				<Database class="w-8 h-8 text-cyan-500" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-cyan-600 to-blue-600 dark:from-cyan-400 dark:to-blue-400 bg-clip-text text-transparent">ElastiCache Clusters</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Managed Redis and Memcached in-memory data stores.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={loadClusters}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95"
				title="Refresh data"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button 
				onclick={() => showCreateModal = true}
				class="flex items-center gap-2 px-5 py-2.5 bg-cyan-600 hover:bg-cyan-700 text-white rounded-xl font-medium shadow-lg shadow-cyan-600/20 transition-all active:scale-95"
			>
				<Plus class="w-5 h-5" />
				Create Cluster
			</button>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Cluster List -->
		<div class="lg:col-span-8 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Search clusters..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-cyan-500 outline-none transition-all"
						/>
					</div>
				</div>

				<div class="overflow-x-auto">
					<table class="w-full text-left border-collapse">
						<thead>
							<tr class="bg-slate-50/50 dark:bg-slate-900/20">
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Cluster Details</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center">Engine</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center">Status</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
							{#if loading && !clusters.length}
								{#each Array(3) as _}
									<tr class="animate-pulse">
										<td colspan="4" class="px-6 py-8"><div class="h-10 bg-slate-200/50 dark:bg-slate-700/30 rounded-xl w-full"></div></td>
									</tr>
								{/each}
							{:else}
								{#each filteredClusters as cluster}
									<tr 
										class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all cursor-pointer {selectedCluster?.CacheClusterId === cluster.CacheClusterId ? 'bg-cyan-500/5 dark:bg-cyan-500/10' : ''}"
										onclick={() => selectedCluster = cluster}
									>
										<td class="px-6 py-4">
											<div class="flex items-center gap-4">
												<div class="p-2 bg-cyan-500/10 rounded-lg">
													<Database class="w-5 h-5 text-cyan-600 dark:text-cyan-400" />
												</div>
												<div>
													<div class="font-bold text-slate-900 dark:text-white uppercase tracking-tight">{cluster.CacheClusterId}</div>
													<div class="text-[10px] text-slate-400 font-mono italic">{cluster.CacheNodeType}</div>
												</div>
											</div>
										</td>
										<td class="px-6 py-4 text-center">
											<span class="px-2 py-0.5 rounded text-[10px] font-black uppercase {cluster.Engine === 'redis' ? 'bg-red-500/10 text-red-600' : 'bg-blue-500/10 text-blue-600'}">
												{cluster.Engine}
											</span>
										</td>
										<td class="px-6 py-4 text-center">
											<div class="flex items-center justify-center gap-1.5">
												<div class="w-2 h-2 rounded-full {getStatusColor(cluster.CacheClusterStatus)}"></div>
												<span class="text-xs font-medium text-slate-600 dark:text-slate-300">{cluster.CacheClusterStatus}</span>
											</div>
										</td>
										<td class="px-6 py-4 text-right">
											<div class="flex items-center justify-end gap-2">
												<button 
													onclick={(e) => { e.stopPropagation(); rebootCluster(cluster.CacheClusterId!); }}
													class="p-2 text-slate-400 hover:text-amber-500 rounded-lg transition-colors" 
													title="Reboot Cluster"
												>
													<Power class="w-4 h-4" />
												</button>
												<button 
													onclick={(e) => { e.stopPropagation(); deleteCluster(cluster.CacheClusterId!); }}
													class="p-2 text-slate-400 hover:text-red-500 rounded-lg transition-colors" 
													title="Delete Cluster"
												>
													<Trash2 class="w-4 h-4" />
												</button>
												<ChevronRight class="w-4 h-4 text-slate-300" />
											</div>
										</td>
									</tr>
								{/each}

								{#if !clusters.length}
									<tr>
										<td colspan="4" class="px-6 py-20 text-center">
											<div class="flex flex-col items-center gap-4">
												<div class="p-4 bg-slate-50 dark:bg-slate-900/40 rounded-full">
													<Database class="w-12 h-12 text-slate-300 dark:text-slate-700" />
												</div>
												<div>
													<p class="text-lg font-medium text-slate-900 dark:text-white">No clusters found</p>
													<p class="text-sm text-slate-500 dark:text-slate-400">Created ElastiCache clusters will appear here.</p>
												</div>
											</div>
										</td>
									</tr>
								{/if}
							{/if}
						</tbody>
					</table>
				</div>
			</div>
		</div>

		<!-- Details Panel -->
		<div class="lg:col-span-4 space-y-6">
			{#if selectedCluster}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
					<div class="p-6 border-b border-slate-200 dark:border-slate-700/50 bg-gradient-to-br from-cyan-500/5 to-blue-500/5">
						<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-1 uppercase tracking-wider">{selectedCluster.CacheClusterId}</h2>
						<div class="flex items-center gap-2 mt-2">
							<span class="px-2 py-0.5 rounded text-[10px] font-bold bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300">
								{selectedCluster.Engine} {selectedCluster.EngineVersion}
							</span>
							<span class="text-xs text-slate-400">•</span>
							<span class="text-xs text-slate-500 dark:text-slate-400">{selectedCluster.CacheNodeType}</span>
						</div>
					</div>

					<div class="p-6 space-y-6">
						<!-- Endpoint Info -->
						<div class="p-4 bg-slate-900 dark:bg-black rounded-xl border border-slate-800 shadow-inner">
							<div class="flex items-center gap-2 text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2">
								<Activity class="w-3 h-3 text-cyan-500" />
								Primary Endpoint
							</div>
							<div class="font-mono text-sm text-cyan-400 break-all select-all">
								{selectedCluster.ConfigurationEndpoint?.Address ?? 'pending-endpoint.local'}:{selectedCluster.ConfigurationEndpoint?.Port ?? (selectedCluster.Engine === 'redis' ? 6379 : 11211)}
							</div>
						</div>

						<!-- Metric Cards style details -->
						<div class="grid grid-cols-2 gap-4">
							<div class="p-3 bg-white/50 dark:bg-slate-900/40 rounded-xl border border-slate-200 dark:border-slate-700/50 shadow-sm">
								<div class="flex items-center gap-2 text-slate-500 dark:text-slate-400 text-[10px] uppercase font-bold tracking-wider mb-1">
									<Server class="w-3 h-3 text-blue-500" />
									Nodes
								</div>
								<div class="text-lg font-black text-slate-900 dark:text-white tabular-nums">{selectedCluster.NumCacheNodes}</div>
							</div>
							<div class="p-3 bg-white/50 dark:bg-slate-900/40 rounded-xl border border-slate-200 dark:border-slate-700/50 shadow-sm">
								<div class="flex items-center gap-2 text-slate-500 dark:text-slate-400 text-[10px] uppercase font-bold tracking-wider mb-1">
									<Clock class="w-3 h-3 text-indigo-500" />
									Created
								</div>
								<div class="text-xs font-bold text-slate-900 dark:text-white mt-1">
									{selectedCluster.CacheClusterCreateTime?.toLocaleDateString() ?? 'N/A'}
								</div>
							</div>
						</div>

						<div class="space-y-4">
							<h3 class="flex items-center gap-2 text-xs font-bold text-slate-500 dark:text-slate-400 uppercase tracking-widest">
								<Activity class="w-3 h-3" />
								Configuration
							</h3>
							
							<div class="space-y-3">
								<div class="flex justify-between items-center text-sm p-2.5 bg-slate-50/50 dark:bg-slate-900/40 rounded-lg">
									<span class="text-slate-500 dark:text-slate-400">Parameter Group</span>
									<span class="font-bold text-slate-700 dark:text-slate-200 text-xs">{selectedCluster.CacheParameterGroup?.CacheParameterGroupName}</span>
								</div>
								<div class="flex justify-between items-center text-sm p-2.5 bg-slate-50/50 dark:bg-slate-900/40 rounded-lg">
									<span class="text-slate-500 dark:text-slate-400">Preferred AZ</span>
									<span class="font-bold text-slate-700 dark:text-slate-200 text-xs">{selectedCluster.PreferredAvailabilityZone}</span>
								</div>
							</div>
						</div>

						<div class="pt-4 border-t border-slate-100 dark:border-slate-700/50 flex flex-col gap-3">
							<button 
								onclick={() => selectedCluster && rebootCluster(selectedCluster.CacheClusterId!)}
								class="w-full flex items-center justify-center gap-2 py-3 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 text-slate-700 dark:text-slate-200 rounded-xl font-bold hover:bg-slate-50 dark:hover:bg-slate-600 transition-all active:scale-[0.98]"
							>
								<RefreshCw class="w-4 h-4" />
								Reboot Nodes
							</button>
							<button 
								onclick={() => selectedCluster && deleteCluster(selectedCluster.CacheClusterId!)}
								class="w-full flex items-center justify-center gap-2 py-3 bg-rose-600 text-white rounded-xl font-bold hover:bg-rose-700 transition-all shadow-lg shadow-rose-600/20 active:scale-[0.98]"
							>
								<Trash2 class="w-4 h-4" />
								Delete Cluster
							</button>
						</div>
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-2xl p-12 text-center flex flex-col items-center gap-4">
					<div class="p-4 bg-slate-50 dark:bg-slate-800 rounded-2xl">
						<Database class="w-10 h-10 text-slate-300 dark:text-slate-600" />
					</div>
					<p class="text-slate-500 dark:text-slate-400 text-sm font-medium">Select a cluster to view connection endpoints and operational status.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div role="none" onclick={() => showCreateModal = false} onkeydown={(e) => e.key === 'Escape' && (showCreateModal = false)} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm shadow-inner"></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700 overflow-hidden">
			<div class="p-6">
				<h3 class="text-xl font-bold text-slate-900 dark:text-white mb-6">Create Cache Cluster</h3>
				
				<form onsubmit={(e) => { e.preventDefault(); createCluster(); }} class="space-y-4">
					<div>
						<label for="clusterId" class="block text-xs font-bold text-slate-500 dark:text-slate-400 uppercase tracking-widest mb-1.5 px-1">Cluster ID</label>
						<input 
							id="clusterId"
							type="text" 
							bind:value={newClusterId}
							placeholder="e.g. session-cache"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 transition-all font-mono"
							required
						/>
					</div>

					<div class="grid grid-cols-2 gap-4">
						<div>
							<label for="ec-engine" class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1">Engine</label>
							<select id="ec-engine" bind:value={engine} class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 transition-all">
								<option value="redis">Redis</option>
								<option value="memcached">Memcached</option>
							</select>
						</div>
						<div>
							<label for="ec-nodes" class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1">Nodes</label>
							<input id="ec-nodes" type="number" bind:value={numNodes} min="1" max="10" class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 transition-all" />
						</div>
					</div>

					<div>
						<label for="ec-node-type" class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1">Node Type</label>
						<select id="ec-node-type" bind:value={nodeType} class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 transition-all font-mono text-xs">
							<option value="cache.t3.micro">cache.t3.micro (0.5 GiB)</option>
							<option value="cache.t3.small">cache.t3.small (1.37 GiB)</option>
							<option value="cache.m5.large">cache.m5.large (6.38 GiB)</option>
						</select>
					</div>

					<div class="flex gap-3 pt-4">
						<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-bold">Cancel</button>
						<button type="submit" disabled={creating} class="flex-1 px-4 py-3 bg-cyan-600 text-white rounded-xl font-bold disabled:opacity-50">
							{creating ? 'Creating...' : 'Create'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<style>
	/* Custom scrollbar */
	::-webkit-scrollbar {
		width: 8px;
		height: 8px;
	}
	::-webkit-scrollbar-track {
		background: transparent;
	}
	::-webkit-scrollbar-thumb {
		background: rgba(148, 163, 184, 0.1);
		border-radius: 10px;
	}
	::-webkit-scrollbar-thumb:hover {
		background: rgba(148, 163, 184, 0.2);
	}
</style>
