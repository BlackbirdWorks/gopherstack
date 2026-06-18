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
		CreateDBClusterSnapshotCommand,
		DeleteDBClusterSnapshotCommand,
		StopDBClusterCommand,
		StartDBClusterCommand,
		FailoverDBClusterCommand,
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
		Trash2,
		Copy,
		StopCircle,
		PlayCircle,
		Globe
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
	let newEngineVersion = $state('1.3.0.0');

	// Instance creation state
	let showCreateInstanceModal = $state(false);
	let creatingInstance = $state(false);
	let newInstanceId = $state('');
	let newInstanceClusterId = $state('');
	let newInstanceClass = $state('db.r5.large');

	// Snapshot creation state
	let showCreateSnapshotModal = $state(false);
	let creatingSnapshot = $state(false);
	let newSnapshotId = $state('');
	let newSnapshotClusterId = $state('');

	// Restore from snapshot state
	let showRestoreModal = $state(false);
	let restoring = $state(false);
	let restoreClusterId = $state('');
	let selectedSnapshot = $state<DBClusterSnapshot | null>(null);

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
				(i.DBInstanceStatus ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(i.DBClusterIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredSnapshots = $derived(
		snapshots.filter(
			(s) =>
				(s.DBClusterSnapshotIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(s.Status ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(s.DBClusterIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const availableCount = $derived(clusters.filter((c) => c.Status === 'available').length);
	const stoppedCount = $derived(clusters.filter((c) => c.Status === 'stopped').length);
	const clusterInstanceCounts = $derived(
		Object.fromEntries(
			clusters.map((c) => [c.DBClusterIdentifier ?? '', (c.DBClusterMembers ?? []).length])
		)
	);

	function statusColor(status: string | undefined): string {
		if (status === 'available') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		if (status === 'creating') return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
		if (status === 'stopped') return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
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
			const res = await neptune.send(new DescribeDBClustersCommand({}));
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
			const res = await neptune.send(new DescribeDBInstancesCommand({}));
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
		const confirmed = await confirmDestructive({ title: 'Delete Cluster', message: `Delete cluster "${id}"? This action cannot be undone.` });
		if (!confirmed) return;
		try {
			await neptune.send(new DeleteDBClusterCommand({ DBClusterIdentifier: id, SkipFinalSnapshot: true }));
			toast.success(`Cluster "${id}" deletion initiated`);
			if (selectedCluster?.DBClusterIdentifier === id) selectedCluster = null;
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function stopCluster(cluster: DBCluster) {
		const id = cluster.DBClusterIdentifier ?? '';
		try {
			await neptune.send(new StopDBClusterCommand({ DBClusterIdentifier: id }));
			toast.success(`Cluster "${id}" stopping`);
			await loadClusters();
			selectedCluster = clusters.find(c => c.DBClusterIdentifier === id) ?? null;
		} catch (err: unknown) {
			toast.error(`Stop failed: ${(err as Error).message}`);
		}
	}

	async function startCluster(cluster: DBCluster) {
		const id = cluster.DBClusterIdentifier ?? '';
		try {
			await neptune.send(new StartDBClusterCommand({ DBClusterIdentifier: id }));
			toast.success(`Cluster "${id}" starting`);
			await loadClusters();
			selectedCluster = clusters.find(c => c.DBClusterIdentifier === id) ?? null;
		} catch (err: unknown) {
			toast.error(`Start failed: ${(err as Error).message}`);
		}
	}

	async function failoverCluster(cluster: DBCluster) {
		const id = cluster.DBClusterIdentifier ?? '';
		const reader = (cluster.DBClusterMembers ?? []).find((m) => !m.IsClusterWriter)?.DBInstanceIdentifier;
		if (!await confirmDestructive({ title: 'Failover Cluster', message: `Promote a reader to writer for "${id}"? The current writer will become a reader. This causes a brief interruption.`, confirmLabel: 'Failover', dangerous: false })) return;
		try {
			await neptune.send(new FailoverDBClusterCommand({
				DBClusterIdentifier: id,
				TargetDBInstanceIdentifier: reader
			}));
			toast.success(`Failover initiated for "${id}"`);
			await loadClusters();
			selectedCluster = clusters.find(c => c.DBClusterIdentifier === id) ?? null;
		} catch (err: unknown) {
			toast.error(`Failover failed: ${(err as Error).message}`);
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
			await loadClusters();
			if (selectedCluster) {
				selectedCluster = clusters.find(c => c.DBClusterIdentifier === selectedCluster?.DBClusterIdentifier) ?? null;
			}
		} catch (err: unknown) {
			toast.error(`Create instance failed: ${(err as Error).message}`);
		} finally {
			creatingInstance = false;
		}
	}

	async function deleteInstance(instance: DBInstance) {
		const id = instance.DBInstanceIdentifier ?? '';
		const confirmed = await confirmDestructive({ title: 'Delete Instance', message: `Delete instance "${id}"? This action cannot be undone.` });
		if (!confirmed) return;
		try {
			await neptune.send(new DeleteDBInstanceCommand({ DBInstanceIdentifier: id }));
			toast.success(`Instance "${id}" deletion initiated`);
			await loadInstances();
		} catch (err: unknown) {
			toast.error(`Delete instance failed: ${(err as Error).message}`);
		}
	}

	async function createSnapshot() {
		if (!newSnapshotId.trim() || !newSnapshotClusterId.trim()) return;
		creatingSnapshot = true;
		try {
			await neptune.send(
				new CreateDBClusterSnapshotCommand({
					DBClusterSnapshotIdentifier: newSnapshotId.trim(),
					DBClusterIdentifier: newSnapshotClusterId.trim()
				})
			);
			toast.success(`Snapshot "${newSnapshotId.trim()}" creating`);
			showCreateSnapshotModal = false;
			newSnapshotId = '';
			newSnapshotClusterId = '';
			if (activeTab === 'snapshots') await loadSnapshots();
		} catch (err: unknown) {
			toast.error(`Create snapshot failed: ${(err as Error).message}`);
		} finally {
			creatingSnapshot = false;
		}
	}

	async function deleteSnapshot(snapshot: DBClusterSnapshot) {
		const id = snapshot.DBClusterSnapshotIdentifier ?? '';
		const confirmed = await confirmDestructive({ title: 'Delete Snapshot', message: `Delete snapshot "${id}"? This action cannot be undone.` });
		if (!confirmed) return;
		try {
			await neptune.send(new DeleteDBClusterSnapshotCommand({ DBClusterSnapshotIdentifier: id }));
			toast.success(`Snapshot "${id}" deleted`);
			if (selectedSnapshot?.DBClusterSnapshotIdentifier === id) selectedSnapshot = null;
			await loadSnapshots();
		} catch (err: unknown) {
			toast.error(`Delete snapshot failed: ${(err as Error).message}`);
		}
	}

	function openRestoreModal(snapshot: DBClusterSnapshot) {
		selectedSnapshot = snapshot;
		restoreClusterId = '';
		showRestoreModal = true;
	}

	async function restoreFromSnapshot() {
		if (!restoreClusterId.trim() || !selectedSnapshot) return;
		restoring = true;
		try {
			const { RestoreDBClusterFromSnapshotCommand } = await import('@aws-sdk/client-neptune');
			await neptune.send(
				new RestoreDBClusterFromSnapshotCommand({
					DBClusterIdentifier: restoreClusterId.trim(),
					SnapshotIdentifier: selectedSnapshot.DBClusterSnapshotIdentifier!,
					Engine: 'neptune'
				})
			);
			toast.success(`Restoring cluster "${restoreClusterId.trim()}" from snapshot`);
			showRestoreModal = false;
			restoreClusterId = '';
			selectedSnapshot = null;
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Restore failed: ${(err as Error).message}`);
		} finally {
			restoring = false;
		}
	}

	function copyToClipboard(text: string, label: string) {
		navigator.clipboard.writeText(text).then(() => {
			toast.success(`${label} copied to clipboard`);
		}).catch(() => {
			toast.error('Failed to copy to clipboard');
		});
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
			{#if activeTab === 'snapshots'}
				<button
					onclick={() => { showCreateSnapshotModal = true; newSnapshotClusterId = selectedCluster?.DBClusterIdentifier ?? ''; }}
					class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 flex items-center gap-2"
				>
					<Plus class="w-4 h-4" />Create Snapshot
				</button>
			{:else if activeTab === 'instances'}
				<button
					onclick={() => { showCreateInstanceModal = true; newInstanceClusterId = selectedCluster?.DBClusterIdentifier ?? ''; }}
					class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2"
				>
					<Plus class="w-4 h-4" />Create Instance
				</button>
			{:else}
				<button
					onclick={() => { showCreateInstanceModal = true; newInstanceClusterId = selectedCluster?.DBClusterIdentifier ?? ''; }}
					class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 flex items-center gap-2"
				>
					<Plus class="w-4 h-4" />Create Instance
				</button>
			{/if}
			<button
				onclick={() => { showCreateModal = true; }}
				class="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 flex items-center gap-2"
			>
				<Plus class="w-4 h-4" />Create Cluster
			</button>
		</div>
	</div>

	<!-- Stat cards -->
	<div class="grid grid-cols-2 sm:grid-cols-5 gap-4">
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
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-slate-100 dark:bg-slate-700 rounded-lg">
				<StopCircle class="w-5 h-5 text-slate-600 dark:text-slate-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{stoppedCount}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Stopped</p>
			</div>
		</div>
	</div>

	<!-- Tab navigation -->
	<div class="flex items-center gap-1 border-b border-slate-200 dark:border-slate-700">
		<button
			onclick={() => selectTab('clusters')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'clusters' ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><Layers class="w-4 h-4" />Clusters
				{#if clusters.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{clusters.length}</span>{/if}
			</span>
		</button>
		<button
			onclick={() => selectTab('instances')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'instances' ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><Server class="w-4 h-4" />Instances
				{#if instances.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{instances.length}</span>{/if}
			</span>
		</button>
		<button
			onclick={() => selectTab('snapshots')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'snapshots' ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><Camera class="w-4 h-4" />Snapshots
				{#if snapshots.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{snapshots.length}</span>{/if}
			</span>
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
						<button onclick={() => { showCreateModal = true; }} class="mt-3 px-3 py-1.5 text-sm text-purple-600 dark:text-purple-400 hover:underline">Create your first cluster</button>
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
									<div class="flex items-center gap-2 mt-0.5">
										<p class="text-xs text-slate-500 dark:text-slate-400">{cluster.Engine ?? 'neptune'} {cluster.EngineVersion ?? ''}</p>
										{#if (clusterInstanceCounts[cluster.DBClusterIdentifier ?? ''] ?? 0) > 0}
											<span class="text-xs text-blue-600 dark:text-blue-400">
												{clusterInstanceCounts[cluster.DBClusterIdentifier ?? '']} instance{(clusterInstanceCounts[cluster.DBClusterIdentifier ?? ''] ?? 0) !== 1 ? 's' : ''}
											</span>
										{/if}
									</div>
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
						<button onclick={() => { showCreateInstanceModal = true; }} class="mt-3 px-3 py-1.5 text-sm text-blue-600 dark:text-blue-400 hover:underline">Create your first instance</button>
					</div>
				{:else}
					{#each filteredInstances as instance}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white truncate">{instance.DBInstanceIdentifier}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{instance.DBInstanceClass ?? 'unknown'}</p>
									{#if instance.DBClusterIdentifier}
										<p class="text-xs text-purple-600 dark:text-purple-400 mt-0.5 flex items-center gap-1">
											<Layers class="w-3 h-3" />{instance.DBClusterIdentifier}
										</p>
									{/if}
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
						<button onclick={() => { showCreateSnapshotModal = true; }} class="mt-3 px-3 py-1.5 text-sm text-amber-600 dark:text-amber-400 hover:underline">Create your first snapshot</button>
					</div>
				{:else}
					{#each filteredSnapshots as snapshot}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white truncate">{snapshot.DBClusterSnapshotIdentifier}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{snapshot.SnapshotType ?? 'manual'} · {snapshot.DBClusterIdentifier}</p>
									{#if snapshot.EngineVersion}
										<p class="text-xs text-slate-400 dark:text-slate-500 mt-0.5">v{snapshot.EngineVersion}</p>
									{/if}
								</div>
								<div class="flex items-center gap-1.5 ml-2 flex-shrink-0">
									<span class="px-2 py-0.5 text-xs rounded-full {statusColor(snapshot.Status)}">{snapshot.Status}</span>
									<button
										onclick={() => openRestoreModal(snapshot)}
										class="p-1 text-slate-400 hover:text-purple-500"
										title="Restore from snapshot"
									>
										<PlayCircle class="w-4 h-4" />
									</button>
									<button
										onclick={() => deleteSnapshot(snapshot)}
										class="p-1 text-slate-400 hover:text-red-500"
										title="Delete snapshot"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>
						</div>
					{/each}
				{/if}
			{/if}
		</div>

		<!-- Detail panel -->
		<div class="lg:col-span-2">
			{#if selectedCluster && activeTab === 'clusters'}
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
						<div class="flex items-center gap-2 flex-wrap justify-end">
							<button
								onclick={() => { showCreateSnapshotModal = true; newSnapshotClusterId = selectedCluster?.DBClusterIdentifier ?? ''; }}
								class="px-3 py-1.5 bg-amber-600 text-white text-sm rounded-lg hover:bg-amber-700 flex items-center gap-1.5"
								title="Create snapshot"
							>
								<Camera class="w-3.5 h-3.5" />Snapshot
							</button>
							<button
								onclick={() => { showCreateInstanceModal = true; newInstanceClusterId = selectedCluster?.DBClusterIdentifier ?? ''; }}
								class="px-3 py-1.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 flex items-center gap-1.5"
								title="Add instance to cluster"
							>
								<Plus class="w-3.5 h-3.5" />Add Instance
							</button>
							{#if selectedCluster.Status === 'available' && (selectedCluster.DBClusterMembers ?? []).length > 1}
								<button
									onclick={() => failoverCluster(selectedCluster!)}
									class="px-3 py-1.5 bg-purple-600 text-white text-sm rounded-lg hover:bg-purple-700 flex items-center gap-1.5"
									title="Failover cluster (promote a reader)"
								>
									<RefreshCw class="w-3.5 h-3.5" />Failover
								</button>
							{/if}
							{#if selectedCluster.Status === 'available'}
								<button
									onclick={() => stopCluster(selectedCluster!)}
									class="px-3 py-1.5 bg-orange-600 text-white text-sm rounded-lg hover:bg-orange-700 flex items-center gap-1.5"
									title="Stop cluster"
								>
									<StopCircle class="w-3.5 h-3.5" />Stop
								</button>
							{:else if selectedCluster.Status === 'stopped'}
								<button
									onclick={() => startCluster(selectedCluster!)}
									class="px-3 py-1.5 bg-green-600 text-white text-sm rounded-lg hover:bg-green-700 flex items-center gap-1.5"
									title="Start cluster"
								>
									<PlayCircle class="w-3.5 h-3.5" />Start
								</button>
							{/if}
							<button
								onclick={() => deleteCluster(selectedCluster!)}
								class="px-3 py-1.5 bg-red-600 text-white text-sm rounded-lg hover:bg-red-700 flex items-center gap-1.5"
								title="Delete cluster"
							>
								<Trash2 class="w-3.5 h-3.5" />Delete
							</button>
						</div>
					</div>

					<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
						{#each [
							['Engine', selectedCluster.Engine ?? 'neptune'],
							['Engine Version', selectedCluster.EngineVersion ?? ''],
							['Status', selectedCluster.Status ?? 'N/A'],
							['Port', String(selectedCluster.Port ?? 8182)],
							['Multi-AZ', String(selectedCluster.MultiAZ ?? false)],
							['Storage Encrypted', String(selectedCluster.StorageEncrypted ?? false)],
							['Backup Retention', `${selectedCluster.BackupRetentionPeriod ?? 1} day(s)`],
						] as [label, value]}
							<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
								<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
								<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate" title={value}>{value}</p>
							</div>
						{/each}
					</div>

					<!-- Serverless v2 Scaling -->
					{#if selectedCluster.ServerlessV2ScalingConfiguration}
						<div class="space-y-2">
							<h3 class="font-semibold text-slate-900 dark:text-white text-sm">Serverless v2 Scaling</h3>
							<div class="grid grid-cols-2 gap-3">
								<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
									<p class="text-xs text-slate-500 dark:text-slate-400">Min Capacity (ACU)</p>
									<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5">{selectedCluster.ServerlessV2ScalingConfiguration.MinCapacity ?? '—'}</p>
								</div>
								<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
									<p class="text-xs text-slate-500 dark:text-slate-400">Max Capacity (ACU)</p>
									<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5">{selectedCluster.ServerlessV2ScalingConfiguration.MaxCapacity ?? '—'}</p>
								</div>
							</div>
						</div>
					{/if}

					<!-- Endpoints -->
					<div class="space-y-2">
						<h3 class="font-semibold text-slate-900 dark:text-white text-sm">Endpoints</h3>
						{#each [
							['Writer Endpoint', selectedCluster.Endpoint ?? ''],
							['Reader Endpoint', selectedCluster.ReaderEndpoint ?? ''],
						] as [label, value]}
							{#if value}
								<div class="flex items-center justify-between bg-slate-50 dark:bg-slate-700/30 rounded-lg px-3 py-2">
									<div class="min-w-0">
										<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
										<p class="text-sm font-mono text-slate-900 dark:text-white truncate" title={value}>{value}</p>
									</div>
									<button
										onclick={() => copyToClipboard(value, label)}
										class="ml-2 p-1.5 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 flex-shrink-0"
										title="Copy {label}"
									>
										<Copy class="w-3.5 h-3.5" />
									</button>
								</div>
							{/if}
						{/each}
						{#if selectedCluster.DBClusterArn}
							<div class="flex items-center justify-between bg-slate-50 dark:bg-slate-700/30 rounded-lg px-3 py-2">
								<div class="min-w-0">
									<p class="text-xs text-slate-500 dark:text-slate-400">Cluster ARN</p>
									<p class="text-xs font-mono text-slate-700 dark:text-slate-300 truncate" title={selectedCluster.DBClusterArn}>{selectedCluster.DBClusterArn}</p>
								</div>
								<button
									onclick={() => copyToClipboard(selectedCluster!.DBClusterArn!, 'ARN')}
									class="ml-2 p-1.5 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 flex-shrink-0"
									title="Copy ARN"
								>
									<Copy class="w-3.5 h-3.5" />
								</button>
							</div>
						{/if}
					</div>

					{#if (selectedCluster.DBClusterMembers ?? []).length > 0}
						<div>
							<h3 class="font-semibold text-slate-900 dark:text-white mb-2 text-sm">
								Cluster Members
								<span class="ml-1 text-xs text-slate-500 font-normal">({(selectedCluster.DBClusterMembers ?? []).length})</span>
							</h3>
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
					{:else}
						<div class="bg-blue-50 dark:bg-blue-900/20 rounded-lg px-4 py-3 text-sm text-blue-700 dark:text-blue-300">
							No instances in this cluster.
							<button onclick={() => { showCreateInstanceModal = true; newInstanceClusterId = selectedCluster?.DBClusterIdentifier ?? ''; }} class="underline ml-1">Add an instance</button>
						</div>
					{/if}
				</div>
			{:else if activeTab === 'clusters'}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Database class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a cluster to view details</p>
				</div>
			{:else if activeTab === 'instances'}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Server class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select an instance to view details</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Globe class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a snapshot to restore or delete it</p>
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
						<option value="1.4.0.0">1.4.0.0 (latest)</option>
						<option value="1.3.2.0">1.3.2.0</option>
						<option value="1.3.1.0">1.3.1.0</option>
						<option value="1.3.0.0">1.3.0.0</option>
						<option value="1.2.1.0">1.2.1.0</option>
						<option value="1.2.0.2">1.2.0.2</option>
						<option value="1.2.0.0">1.2.0.0</option>
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
					{#if clusters.length > 0}
						<select
							id="neptune-instance-cluster"
							bind:value={newInstanceClusterId}
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
							required
						>
							<option value="">Select a cluster...</option>
							{#each clusters as cluster}
								<option value={cluster.DBClusterIdentifier}>{cluster.DBClusterIdentifier} ({cluster.Status})</option>
							{/each}
						</select>
					{:else}
						<input
							id="neptune-instance-cluster"
							type="text"
							bind:value={newInstanceClusterId}
							placeholder="e.g. my-neptune-cluster"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
							required
						/>
					{/if}
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
						<option value="db.r5.8xlarge">db.r5.8xlarge</option>
						<option value="db.r6g.large">db.r6g.large</option>
						<option value="db.r6g.xlarge">db.r6g.xlarge</option>
						<option value="db.r6g.2xlarge">db.r6g.2xlarge</option>
						<option value="db.r6g.4xlarge">db.r6g.4xlarge</option>
						<option value="db.t3.medium">db.t3.medium</option>
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

<!-- Create Snapshot Modal -->
{#if showCreateSnapshotModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Cluster Snapshot</h2>
			<form onsubmit={(e) => { e.preventDefault(); createSnapshot(); }} class="space-y-4">
				<div>
					<label for="neptune-snapshot-id" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Snapshot Identifier</label>
					<input
						id="neptune-snapshot-id"
						type="text"
						bind:value={newSnapshotId}
						placeholder="e.g. my-cluster-snapshot-2024"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-amber-500"
						required
					/>
				</div>
				<div>
					<label for="neptune-snapshot-cluster" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Source Cluster</label>
					{#if clusters.length > 0}
						<select
							id="neptune-snapshot-cluster"
							bind:value={newSnapshotClusterId}
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-amber-500"
							required
						>
							<option value="">Select a cluster...</option>
							{#each clusters as cluster}
								<option value={cluster.DBClusterIdentifier}>{cluster.DBClusterIdentifier} ({cluster.Status})</option>
							{/each}
						</select>
					{:else}
						<input
							id="neptune-snapshot-cluster"
							type="text"
							bind:value={newSnapshotClusterId}
							placeholder="e.g. my-neptune-cluster"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-amber-500"
							required
						/>
					{/if}
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button
						type="button"
						onclick={() => { showCreateSnapshotModal = false; }}
						class="px-4 py-2 text-slate-600 dark:text-slate-400"
					>Cancel</button>
					<button
						type="submit"
						disabled={creatingSnapshot}
						class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 disabled:opacity-50"
					>
						{creatingSnapshot ? 'Creating...' : 'Create Snapshot'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Restore from Snapshot Modal -->
{#if showRestoreModal && selectedSnapshot}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-2">Restore from Snapshot</h2>
			<p class="text-sm text-slate-500 dark:text-slate-400 mb-4">
				Source snapshot: <span class="font-mono text-slate-700 dark:text-slate-300">{selectedSnapshot.DBClusterSnapshotIdentifier}</span>
			</p>
			<form onsubmit={(e) => { e.preventDefault(); restoreFromSnapshot(); }} class="space-y-4">
				<div>
					<label for="neptune-restore-cluster-id" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">New Cluster Identifier</label>
					<input
						id="neptune-restore-cluster-id"
						type="text"
						bind:value={restoreClusterId}
						placeholder="e.g. my-restored-cluster"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-purple-500"
						required
					/>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button
						type="button"
						onclick={() => { showRestoreModal = false; selectedSnapshot = null; }}
						class="px-4 py-2 text-slate-600 dark:text-slate-400"
					>Cancel</button>
					<button
						type="submit"
						disabled={restoring}
						class="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 disabled:opacity-50"
					>
						{restoring ? 'Restoring...' : 'Restore Cluster'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
