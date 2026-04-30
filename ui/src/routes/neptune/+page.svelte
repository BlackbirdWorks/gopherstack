<script lang="ts">
	import { onMount } from 'svelte';
	import { getNeptuneClient } from '$lib/aws-client';
	import {
		DescribeDBClustersCommand,
		DescribeDBInstancesCommand,
		DescribeDBClusterSnapshotsCommand,
		CreateDBClusterCommand,
		DeleteDBClusterCommand,
		CreateDBInstanceCommand,
		DeleteDBInstanceCommand,
		type DBCluster,
		type DBInstance,
		type DBClusterSnapshot
	} from '@aws-sdk/client-neptune';
	import { toast } from 'svelte-sonner';
	import {
		Database,
		Search,
		RefreshCw,
		Plus,
		Layers,
		Camera,
		Server,
		CheckCircle,
		Clock,
		XCircle,
		ChevronRight,
		Trash2
	} from 'lucide-svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';

	const neptune = getNeptuneClient();

	let loading = $state(false);
	let activeTab = $state<'clusters' | 'instances' | 'snapshots'>('clusters');
	let searchQuery = $state('');
	let clusters = $state<DBCluster[]>([]);
	let instances = $state<DBInstance[]>([]);
	let snapshots = $state<DBClusterSnapshot[]>([]);
	let selectedCluster = $state<DBCluster | null>(null);
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newClusterId = $state('');
	let newEngine = $state('neptune');
	let newEngineVersion = $state('1.3.1.0');

	// Instance creation state
	let showCreateInstanceModal = $state(false);
	let creatingInstance = $state(false);
	let newInstanceId = $state('');
	let newInstanceClusterId = $state('');
	let newInstanceClass = $state('db.r5.large');

	const filteredClusters = $derived(
		clusters.filter(
			(c) =>
				(c.DBClusterIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(c.Status ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredInstances = $derived(
		instances.filter(
			(i) =>
				(i.DBInstanceIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(i.DBInstanceStatus ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredSnapshots = $derived(
		snapshots.filter(
			(s) =>
				(s.DBClusterSnapshotIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(s.Status ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const availableCount = $derived(clusters.filter((c) => c.Status === 'available').length);

	function statusColor(status: string | undefined): string {
		if (status === 'available') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		if (status === 'creating') return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
		if (status === 'deleting' || status === 'failed')
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
		return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	}

	function statusIcon(status: string | undefined) {
		if (status === 'available') return CheckCircle;
		if (status === 'creating') return Clock;
		return XCircle;
	}

	async function loadClusters() {
		loading = true;
		try {
			const res = await neptune.send(new DescribeDBClustersCommand({ Filters: [{ Name: 'engine', Values: ['neptune'] }] }));
			clusters = res.DBClusters ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load clusters: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadInstances() {
		loading = true;
		try {
			const res = await neptune.send(new DescribeDBInstancesCommand({ Filters: [{ Name: 'engine', Values: ['neptune'] }] }));
			instances = res.DBInstances ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load instances: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadSnapshots() {
		loading = true;
		try {
			const res = await neptune.send(new DescribeDBClusterSnapshotsCommand({}));
			snapshots = res.DBClusterSnapshots ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load snapshots: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		selectedCluster = null;
		if (tab === 'clusters' && clusters.length === 0) await loadClusters();
		else if (tab === 'instances' && instances.length === 0) await loadInstances();
		else if (tab === 'snapshots' && snapshots.length === 0) await loadSnapshots();
	}

	async function refresh() {
		if (activeTab === 'clusters') { clusters = []; await loadClusters(); }
		else if (activeTab === 'instances') { instances = []; await loadInstances(); }
		else { snapshots = []; await loadSnapshots(); }
	}

	async function createCluster() {
		if (!newClusterId.trim()) return;
		creating = true;
		try {
			await neptune.send(
				new CreateDBClusterCommand({
					DBClusterIdentifier: newClusterId.trim(),
					Engine: newEngine,
					EngineVersion: newEngineVersion
				})
			);
			toast.success(`Cluster "${newClusterId.trim()}" creating`);
			showCreateModal = false;
			newClusterId = '';
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteCluster(cluster: DBCluster) {
		const id = cluster.DBClusterIdentifier ?? '';
		if (!await confirmDestructive({ title: 'Delete Cluster', message: `Delete cluster "${id}"? This action cannot be undone.` })) return;
		try {
			await neptune.send(new DeleteDBClusterCommand({ DBClusterIdentifier: id, SkipFinalSnapshot: true }));
			toast.success(`Cluster "${id}" deletion initiated`);
			if (selectedCluster?.DBClusterIdentifier === id) selectedCluster = null;
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function createInstance() {
		if (!newInstanceId.trim() || !newInstanceClusterId.trim()) return;
		creatingInstance = true;
		try {
			await neptune.send(
				new CreateDBInstanceCommand({
					DBInstanceIdentifier: newInstanceId.trim(),
					DBClusterIdentifier: newInstanceClusterId.trim(),
					DBInstanceClass: newInstanceClass,
					Engine: 'neptune'
				})
			);
			toast.success(`Instance "${newInstanceId.trim()}" creating`);
			showCreateInstanceModal = false;
			newInstanceId = '';
			newInstanceClusterId = '';
			if (activeTab === 'instances') await loadInstances();
		} catch (err: unknown) {
			toast.error(`Create instance failed: ${(err as Error).message}`);
		} finally {
			creatingInstance = false;
		}
	}

	async function deleteInstance(instance: DBInstance) {
		const id = instance.DBInstanceIdentifier ?? '';
		if (!await confirmDestructive({ title: 'Delete Instance', message: `Delete instance "${id}"? This action cannot be undone.` })) return;
		try {
			await neptune.send(new DeleteDBInstanceCommand({ DBInstanceIdentifier: id }));
			toast.success(`Instance "${id}" deletion initiated`);
			await loadInstances();
		} catch (err: unknown) {
			toast.error(`Delete instance failed: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		loadClusters();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Database class="w-6 h-6 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Amazon Neptune</h1>
				<p class="text-slate-600 dark:text-slate-300">Managed graph database service</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={() => refresh()}
				class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white"
				title="Refresh"
			>
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
			<button
				onclick={() => { showCreateInstanceModal = true; newInstanceClusterId = selectedCluster?.DBClusterIdentifier ?? ''; }}
				class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2"
			>
				<Plus class="w-4 h-4" />Create Instance
			</button>
			<button
				onclick={() => { showCreateModal = true; }}
				class="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 flex items-center gap-2"
			>
				<Plus class="w-4 h-4" />Create Cluster
			</button>
		</div>
	</div>

	<!-- Stat cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Layers class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{clusters.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Clusters</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Server class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{instances.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Instances</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-lg">
				<Camera class="w-5 h-5 text-amber-600 dark:text-amber-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{snapshots.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Snapshots</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{availableCount}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Available</p>
			</div>
		</div>
	</div>

	<!-- Tab navigation -->
	<div class="flex items-center gap-1 border-b border-slate-200 dark:border-slate-700">
		<button
			onclick={() => selectTab('clusters')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'clusters' ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><Layers class="w-4 h-4" />Clusters</span>
		</button>
		<button
			onclick={() => selectTab('instances')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'instances' ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><Server class="w-4 h-4" />Instances</span>
		</button>
		<button
			onclick={() => selectTab('snapshots')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'snapshots' ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><Camera class="w-4 h-4" />Snapshots</span>
		</button>
	</div>

	<!-- Search -->
	<div class="relative">
		<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
		<input
			type="text"
			bind:value={searchQuery}
			placeholder="Search {activeTab}..."
			class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-purple-500"
		/>
	</div>

	<!-- Content -->
	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- List -->
		<div class="lg:col-span-1 space-y-2">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-purple-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading {activeTab}...</p>
				</div>
			{:else if activeTab === 'clusters'}
				{#if filteredClusters.length === 0}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
						<Database class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">No clusters found</p>
					</div>
				{:else}
					{#each filteredClusters as cluster}
						<div
							role="button"
							tabindex="0"
							onclick={() => { selectedCluster = cluster; }}
							onkeypress={(e) => { if (e.key === 'Enter') selectedCluster = cluster; }}
							class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-purple-400 transition-colors cursor-pointer {selectedCluster?.DBClusterIdentifier === cluster.DBClusterIdentifier ? 'border-purple-500 ring-1 ring-purple-500' : 'border-slate-200 dark:border-slate-700'}"
						>
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white truncate">{cluster.DBClusterIdentifier}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{cluster.Engine ?? 'neptune'} {cluster.EngineVersion ?? ''}</p>
								</div>
								<div class="flex items-center gap-1.5 ml-2 flex-shrink-0">
									<span class="px-2 py-0.5 text-xs rounded-full {statusColor(cluster.Status)}">{cluster.Status}</span>
									<button
										onclick={(e) => { e.stopPropagation(); deleteCluster(cluster); }}
										class="p-1 text-slate-400 hover:text-red-500"
										title="Delete cluster"
									>
										<Trash2 class="w-4 h-4" />
									</button>
									<ChevronRight class="w-4 h-4 text-slate-400" />
								</div>
							</div>
						</div>
					{/each}
				{/if}
			{:else if activeTab === 'instances'}
				{#if filteredInstances.length === 0}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
						<Server class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">No instances found</p>
					</div>
				{:else}
					{#each filteredInstances as instance}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white truncate">{instance.DBInstanceIdentifier}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{instance.DBInstanceClass ?? 'unknown'}</p>
								</div>
								<div class="flex items-center gap-1.5 ml-2 flex-shrink-0">
									<span class="px-2 py-0.5 text-xs rounded-full {statusColor(instance.DBInstanceStatus)}">{instance.DBInstanceStatus}</span>
									<button
										onclick={() => deleteInstance(instance)}
										class="p-1 text-slate-400 hover:text-red-500"
										title="Delete instance"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>
						</div>
					{/each}
				{/if}
			{:else}
				{#if filteredSnapshots.length === 0}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
						<Camera class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">No snapshots found</p>
					</div>
				{:else}
					{#each filteredSnapshots as snapshot}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white truncate">{snapshot.DBClusterSnapshotIdentifier}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{snapshot.SnapshotType ?? 'manual'}</p>
								</div>
								<span class="px-2 py-0.5 text-xs rounded-full {statusColor(snapshot.Status)} flex-shrink-0 ml-2">{snapshot.Status}</span>
							</div>
						</div>
					{/each}
				{/if}
			{/if}
		</div>

		<!-- Detail panel -->
		<div class="lg:col-span-2">
			{#if selectedCluster}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
					<div class="flex items-start justify-between">
						<div>
							<h2 class="text-xl font-bold text-slate-900 dark:text-white">
								{selectedCluster.DBClusterIdentifier}
							</h2>
							<span class="mt-1 inline-block px-2 py-0.5 text-xs rounded-full {statusColor(selectedCluster.Status)}">
								{selectedCluster.Status}
							</span>
						</div>
						<div class="flex items-center gap-2">
							<button
								onclick={() => { showCreateInstanceModal = true; newInstanceClusterId = selectedCluster?.DBClusterIdentifier ?? ''; }}
								class="px-3 py-1.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 flex items-center gap-1.5"
								title="Add instance to cluster"
							>
								<Plus class="w-3.5 h-3.5" />Add Instance
							</button>
							<button
								onclick={() => deleteCluster(selectedCluster!)}
								class="px-3 py-1.5 bg-red-600 text-white text-sm rounded-lg hover:bg-red-700 flex items-center gap-1.5"
								title="Delete cluster"
							>
								<Trash2 class="w-3.5 h-3.5" />Delete
							</button>
							<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
								<Database class="w-5 h-5 text-purple-600 dark:text-purple-400" />
							</div>
						</div>
					</div>

					<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
						{#each [
							['Cluster ARN', selectedCluster.DBClusterArn ?? 'N/A'],
							['Engine', selectedCluster.Engine ?? 'neptune'],
							['Engine Version', selectedCluster.EngineVersion ?? 'N/A'],
							['Reader Endpoint', selectedCluster.ReaderEndpoint ?? 'N/A'],
							['Writer Endpoint', selectedCluster.Endpoint ?? 'N/A'],
							['Multi-AZ', String(selectedCluster.MultiAZ ?? false)],
							['Storage Encrypted', String(selectedCluster.StorageEncrypted ?? false)],
							['Deletion Protection', String(selectedCluster.DeletionProtection ?? false)],
							['Availability Zones', (selectedCluster.AvailabilityZones ?? []).join(', ') || 'N/A']
						] as [label, value]}
							<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
								<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
								<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate" title={value}>{value}</p>
							</div>
						{/each}
					</div>

					{#if (selectedCluster.DBClusterMembers ?? []).length > 0}
						<div>
							<h3 class="font-semibold text-slate-900 dark:text-white mb-2">Cluster Members</h3>
							<div class="space-y-1">
								{#each selectedCluster.DBClusterMembers ?? [] as member}
									<div class="flex items-center justify-between bg-slate-50 dark:bg-slate-700/30 rounded-lg px-3 py-2">
										<span class="text-sm text-slate-900 dark:text-white font-medium">{member.DBInstanceIdentifier}</span>
										<span class="text-xs px-2 py-0.5 rounded-full {member.IsClusterWriter ? 'bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400'}">
											{member.IsClusterWriter ? 'Writer' : 'Reader'}
										</span>
									</div>
								{/each}
							</div>
						</div>
					{/if}
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Database class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a cluster to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Cluster Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Neptune Cluster</h2>
			<form onsubmit={(e) => { e.preventDefault(); createCluster(); }} class="space-y-4">
				<div>
					<label for="neptune-cluster-id" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Cluster Identifier</label>
					<input
						id="neptune-cluster-id"
						type="text"
						bind:value={newClusterId}
						placeholder="e.g. my-neptune-cluster"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-purple-500"
						required
					/>
				</div>
				<div>
					<label for="neptune-engine-version" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Engine Version</label>
					<select
						id="neptune-engine-version"
						bind:value={newEngineVersion}
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-purple-500"
					>
						<option value="1.3.1.0">1.3.1.0</option>
						<option value="1.2.1.0">1.2.1.0</option>
						<option value="1.1.1.0">1.1.1.0</option>
					</select>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button
						type="button"
						onclick={() => { showCreateModal = false; }}
						class="px-4 py-2 text-slate-600 dark:text-slate-400"
					>Cancel</button>
					<button
						type="submit"
						disabled={creating}
						class="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 disabled:opacity-50"
					>
						{creating ? 'Creating...' : 'Create Cluster'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Instance Modal -->
{#if showCreateInstanceModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Neptune Instance</h2>
			<form onsubmit={(e) => { e.preventDefault(); createInstance(); }} class="space-y-4">
				<div>
					<label for="neptune-instance-id" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Instance Identifier</label>
					<input
						id="neptune-instance-id"
						type="text"
						bind:value={newInstanceId}
						placeholder="e.g. my-neptune-instance"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
						required
					/>
				</div>
				<div>
					<label for="neptune-instance-cluster" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Cluster Identifier</label>
					<input
						id="neptune-instance-cluster"
						type="text"
						bind:value={newInstanceClusterId}
						placeholder="e.g. my-neptune-cluster"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
						required
					/>
				</div>
				<div>
					<label for="neptune-instance-class" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Instance Class</label>
					<select
						id="neptune-instance-class"
						bind:value={newInstanceClass}
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
					>
						<option value="db.r5.large">db.r5.large</option>
						<option value="db.r5.xlarge">db.r5.xlarge</option>
						<option value="db.r5.2xlarge">db.r5.2xlarge</option>
						<option value="db.r5.4xlarge">db.r5.4xlarge</option>
						<option value="db.r6g.large">db.r6g.large</option>
						<option value="db.r6g.xlarge">db.r6g.xlarge</option>
					</select>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button
						type="button"
						onclick={() => { showCreateInstanceModal = false; }}
						class="px-4 py-2 text-slate-600 dark:text-slate-400"
					>Cancel</button>
					<button
						type="submit"
						disabled={creatingInstance}
						class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
					>
						{creatingInstance ? 'Creating...' : 'Create Instance'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
