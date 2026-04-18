<script lang="ts">
import { onMount } from 'svelte';
import { getRDSClient } from '$lib/aws-client';
import { DescribeDBInstancesCommand, DeleteDBInstanceCommand, DescribeDBSnapshotsCommand, DescribeDBClustersCommand, type DBInstance, type DBSnapshot, type DBCluster } from '@aws-sdk/client-rds';
import { toast } from 'svelte-sonner';
import {
	Database,
	Plus,
	RefreshCw,
	Search,
	Trash2,
	CheckCircle,
	XCircle,
	Clock,
	Shield,
	HardDrive,
	MapPin,
	ChevronDown,
	ChevronUp,
	Camera,
	Layers
} from 'lucide-svelte';

const rds = getRDSClient();

let instances = $state<DBInstance[]>([]);
let snapshots = $state<DBSnapshot[]>([]);
let clusters = $state<DBCluster[]>([]);
let loading = $state(true);
let search = $state('');
let snapshotSearch = $state('');
let engineFilter = $state('all');
let showCreateModal = $state(false);
let expandedInstance = $state<string | null>(null);
let activeTab = $state<'instances' | 'snapshots' | 'clusters'>('instances');

onMount(async () => {
	await loadInstances();
});

async function loadInstances() {
	try {
		loading = true;
		const data = await rds.send(new DescribeDBInstancesCommand({}));
		instances = data.DBInstances || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load RDS instances');
	} finally {
		loading = false;
	}
}

async function loadSnapshots() {
	try {
		loading = true;
		const data = await rds.send(new DescribeDBSnapshotsCommand({ MaxRecords: 100 }));
		snapshots = data.DBSnapshots || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load snapshots');
	} finally {
		loading = false;
	}
}

async function loadClusters() {
	try {
		loading = true;
		const data = await rds.send(new DescribeDBClustersCommand({}));
		clusters = data.DBClusters || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load clusters');
	} finally {
		loading = false;
	}
}

async function selectTab(t: typeof activeTab) {
	activeTab = t;
	if (t === 'instances' && instances.length === 0) await loadInstances();
	else if (t === 'snapshots') await loadSnapshots();
	else if (t === 'clusters') await loadClusters();
}

async function refresh() {
	if (activeTab === 'instances') { instances = []; await loadInstances(); }
	else if (activeTab === 'snapshots') { snapshots = []; await loadSnapshots(); }
	else { clusters = []; await loadClusters(); }
}

async function deleteInstance(id: string) {
	if (!confirm(`Delete RDS instance ${id}? This cannot be undone.`)) return;
	try {
		await rds.send(new DeleteDBInstanceCommand({ DBInstanceIdentifier: id, SkipFinalSnapshot: true }));
		toast.success(`Instance ${id} deleted`);
		await loadInstances();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to delete');
	}
}

function getStatusColor(status: string) {
	if (status === 'available') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
	if (status === 'stopped') return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	if (status === 'deleting' || status === 'failed') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
	return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
}

function formatDate(d: string | Date | undefined): string {
	if (!d) return '—';
	return new Date(d).toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
}

function toggleExpand(id: string) {
	expandedInstance = expandedInstance === id ? null : id;
}

let engines = $derived([...new Set(instances.map(i => i.Engine).filter((e): e is string => e !== null))]);

let filtered = $derived(instances.filter(i => {
	const matchSearch = !search || i.DBInstanceIdentifier?.toLowerCase().includes(search.toLowerCase())
		|| i.Engine?.toLowerCase().includes(search.toLowerCase());
	const matchEngine = engineFilter === 'all' || i.Engine === engineFilter;
	return matchSearch && matchEngine;
}));

let availableCount = $derived(instances.filter(i => i.DBInstanceStatus === 'available').length);
let stoppedCount = $derived(instances.filter(i => i.DBInstanceStatus === 'stopped').length);
let multiAZCount = $derived(instances.filter(i => i.MultiAZ).length);
let filteredSnapshots = $derived(snapshots.filter(s =>
	!snapshotSearch || s.DBSnapshotIdentifier?.toLowerCase().includes(snapshotSearch.toLowerCase())
		|| s.DBInstanceIdentifier?.toLowerCase().includes(snapshotSearch.toLowerCase())
));
let automatedSnapshotCount = $derived(snapshots.filter(s => s.SnapshotType === 'automated').length);
let manualSnapshotCount = $derived(snapshots.filter(s => s.SnapshotType === 'manual').length);
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Database class="w-6 h-6 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">RDS Instances</h1>
				<p class="text-slate-600 dark:text-slate-300">Relational Database Service</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button
				onclick={refresh}
				class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700"
				title="Refresh"
			>
				<RefreshCw class="w-4 h-4" />
			</button>
			<button
				onclick={() => showCreateModal = true}
				class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"
			>
				<Plus class="w-4 h-4" />
				Create Instance
			</button>
		</div>
	</div>

	<!-- Stats cards -->
	{#if !loading && instances.length > 0}
		<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
				<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
					<Database class="w-5 h-5 text-blue-600 dark:text-blue-400" />
				</div>
				<div>
					<p class="text-2xl font-bold text-slate-900 dark:text-white">{instances.length}</p>
					<p class="text-xs text-slate-500 dark:text-slate-400">Total Instances</p>
				</div>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
				<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
					<CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" />
				</div>
				<div>
					<p class="text-2xl font-bold text-green-600 dark:text-green-400">{availableCount}</p>
					<p class="text-xs text-slate-500 dark:text-slate-400">Available</p>
				</div>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
				<div class="p-2 bg-slate-100 dark:bg-slate-700 rounded-lg">
					<XCircle class="w-5 h-5 text-slate-500 dark:text-slate-400" />
				</div>
				<div>
					<p class="text-2xl font-bold text-slate-600 dark:text-slate-300">{stoppedCount}</p>
					<p class="text-xs text-slate-500 dark:text-slate-400">Stopped</p>
				</div>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
				<div class="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg">
					<Shield class="w-5 h-5 text-indigo-600 dark:text-indigo-400" />
				</div>
				<div>
					<p class="text-2xl font-bold text-indigo-600 dark:text-indigo-400">{multiAZCount}</p>
					<p class="text-xs text-slate-500 dark:text-slate-400">Multi-AZ</p>
				</div>
			</div>
		</div>
	{/if}

	<!-- Tabs -->
	<div class="border-b border-slate-200 dark:border-slate-700">
		<nav class="flex gap-1 -mb-px">
			<button onclick={() => selectTab('instances')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'instances' ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Database class="w-4 h-4" /> Instances {#if instances.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{instances.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('snapshots')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'snapshots' ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Camera class="w-4 h-4" /> Snapshots {#if snapshots.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{snapshots.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('clusters')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'clusters' ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Layers class="w-4 h-4" /> Aurora Clusters {#if clusters.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{clusters.length}</span>{/if}
			</button>
		</nav>
	</div>

	<!-- Instances Tab -->
	{#if activeTab === 'instances'}
	<!-- Search and filter bar -->
	<div class="flex gap-3 flex-wrap">
		<div class="relative flex-1 min-w-48">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input
				type="text"
				placeholder="Search instances..."
				bind:value={search}
				class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
			/>
		</div>
		<select
			bind:value={engineFilter}
			class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white"
		>
			<option value="all">All Engines</option>
			{#each engines as engine}<option value={engine}>{engine}</option>{/each}
		</select>
	</div>

	<!-- Count summary -->
	{#if !loading && instances.length > 0}
		<p class="text-sm text-slate-500 dark:text-slate-400">
			Showing {filtered.length} of {instances.length} {instances.length === 1 ? 'RDS instance' : 'RDS instances'}
		</p>
	{/if}

	<!-- Content -->
	{#if loading}
		<div class="text-center py-12 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
			<p>Loading instances...</p>
		</div>
	{:else if filtered.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
			<Database class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
			<p class="text-slate-600 dark:text-slate-300 text-lg mb-1">No RDS instances found</p>
			<p class="text-slate-400 dark:text-slate-500 text-sm mb-4">
				{search || engineFilter !== 'all' ? 'Try adjusting your filters.' : 'Create your first RDS instance to get started.'}
			</p>
			{#if !search && engineFilter === 'all'}
				<button
					onclick={() => showCreateModal = true}
					class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700"
				>
					Create Instance
				</button>
			{/if}
		</div>
	{:else}
		<div class="grid gap-4">
			{#each filtered as instance}
				{@const expanded = expandedInstance === instance.DBInstanceIdentifier}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
					<!-- Main row -->
					<div class="p-5">
						<div class="flex items-start justify-between mb-4">
							<div class="flex-1 min-w-0">
								<h3 class="font-semibold text-slate-900 dark:text-white text-lg">{instance.DBInstanceIdentifier}</h3>
								<p class="text-sm text-slate-500 dark:text-slate-400">{instance.Engine}{instance.EngineVersion ? ` ${instance.EngineVersion}` : ''}</p>
							</div>
							<span class={`ml-3 shrink-0 px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(instance.DBInstanceStatus ?? '')}`}>
								{instance.DBInstanceStatus}
							</span>
						</div>

						<div class="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm mb-4">
							<div>
								<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Class</p>
								<p class="font-medium text-slate-900 dark:text-white">{instance.DBInstanceClass}</p>
							</div>
							<div>
								<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Storage</p>
								<p class="font-medium text-slate-900 dark:text-white flex items-center gap-1">
									<HardDrive class="w-3 h-3 text-slate-400" />
									{instance.AllocatedStorage} GB
								</p>
							</div>
							<div>
								<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Endpoint</p>
								<p class="font-mono text-slate-900 dark:text-white text-xs truncate">{instance.Endpoint?.Address || '—'}</p>
							</div>
							<div>
								<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Port</p>
								<p class="font-medium text-slate-900 dark:text-white">{instance.Endpoint?.Port || '—'}</p>
							</div>
						</div>

						<!-- Tags row: Multi-AZ, backup, AZ -->
						<div class="flex flex-wrap gap-2 mb-4">
							{#if instance.MultiAZ}
								<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300">
									<Shield class="w-3 h-3" /> Multi-AZ
								</span>
							{/if}
							{#if instance.BackupRetentionPeriod != null}
								<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300">
									<Clock class="w-3 h-3" /> Backup: {instance.BackupRetentionPeriod}d
								</span>
							{/if}
							{#if instance.AvailabilityZone}
								<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300">
									<MapPin class="w-3 h-3" /> {instance.AvailabilityZone}
								</span>
							{/if}
							{#if instance.InstanceCreateTime}
								<span class="inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300">
									Created: {formatDate(instance.InstanceCreateTime)}
								</span>
							{/if}
						</div>

						<!-- Actions -->
						<div class="flex items-center justify-between">
							<div class="flex gap-2">
								<button
									onclick={() => deleteInstance(instance.DBInstanceIdentifier ?? '')}
									class="px-3 py-1.5 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center gap-1"
								>
									<Trash2 class="w-3 h-3" /> Delete
								</button>
							</div>
							<button
							onclick={() => toggleExpand(instance.DBInstanceIdentifier ?? '')}
								class="text-sm text-indigo-600 dark:text-indigo-400 hover:underline flex items-center gap-1"
							>
								{expanded ? 'Hide details' : 'Show details'}
								{#if expanded}
									<ChevronUp class="w-4 h-4" />
								{:else}
									<ChevronDown class="w-4 h-4" />
								{/if}
							</button>
						</div>
					</div>

					<!-- Expanded detail section -->
					{#if expanded}
						<div class="border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900/50 p-5">
							<h4 class="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-3">Instance Details</h4>
							<div class="grid grid-cols-2 md:grid-cols-3 gap-4 text-sm">
								<div>
									<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">DB Name</p>
									<p class="font-medium text-slate-900 dark:text-white">{instance.DBName || '—'}</p>
								</div>
								<div>
									<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">VPC ID</p>
									<p class="font-mono text-slate-900 dark:text-white text-xs">{instance.DBSubnetGroup?.VpcId || '—'}</p>
								</div>
								<div>
									<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">License Model</p>
									<p class="font-medium text-slate-900 dark:text-white">{instance.LicenseModel || '—'}</p>
								</div>
								<div>
									<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Storage Type</p>
									<p class="font-medium text-slate-900 dark:text-white">{instance.StorageType || '—'}</p>
								</div>
								<div>
									<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Storage Encrypted</p>
									<p class="font-medium text-slate-900 dark:text-white">{instance.StorageEncrypted ? 'Yes' : 'No'}</p>
								</div>
								<div>
									<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">IAM Auth</p>
									<p class="font-medium text-slate-900 dark:text-white">{instance.IAMDatabaseAuthenticationEnabled ? 'Enabled' : 'Disabled'}</p>
								</div>
							</div>
						</div>
					{/if}
				</div>
			{/each}
		</div>
	{/if}

	<!-- End Instances Tab -->
	{/if}

	<!-- Snapshots Tab -->
	{#if activeTab === 'snapshots'}
		<div>
			<div class="flex items-center gap-3 mb-4">
				<div class="relative flex-1">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
					<input type="text" placeholder="Search snapshots..." bind:value={snapshotSearch}
						class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm" />
				</div>
				<div class="flex gap-2 shrink-0 text-sm">
					<span class="px-2.5 py-1 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">{manualSnapshotCount} manual</span>
					<span class="px-2.5 py-1 rounded-full bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300">{automatedSnapshotCount} automated</span>
				</div>
			</div>
			{#if loading}
				<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div></div>
			{:else if filteredSnapshots.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Camera class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
					<p class="text-slate-500 dark:text-slate-400">No snapshots found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-slate-50 dark:bg-slate-700">
							<tr>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Snapshot ID</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Instance</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Engine</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Status</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Type</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Size</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Created</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
							{#each filteredSnapshots as snap}
								<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
									<td class="px-4 py-3 font-mono text-xs text-slate-900 dark:text-white">{snap.DBSnapshotIdentifier}</td>
									<td class="px-4 py-3 text-sm text-slate-600 dark:text-slate-300">{snap.DBInstanceIdentifier || '—'}</td>
									<td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">{snap.Engine || '—'}</td>
									<td class="px-4 py-3">
										<span class="px-2 py-0.5 rounded-full text-xs {snap.Status === 'available' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-300'}">{snap.Status || '—'}</span>
									</td>
									<td class="px-4 py-3 text-xs text-slate-500">{snap.SnapshotType || '—'}</td>
									<td class="px-4 py-3 text-xs text-slate-500">{snap.AllocatedStorage ? snap.AllocatedStorage + ' GiB' : '—'}</td>
									<td class="px-4 py-3 text-xs text-slate-500">{formatDate(snap.SnapshotCreateTime)}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Clusters Tab -->
	{#if activeTab === 'clusters'}
		{#if loading}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div></div>
		{:else if clusters.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Layers class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
				<p class="text-slate-500 dark:text-slate-400">No Aurora clusters found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-slate-50 dark:bg-slate-700">
						<tr>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Cluster ID</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Engine</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Status</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Members</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Endpoint</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
						{#each clusters as cluster}
							<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
								<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{cluster.DBClusterIdentifier}</td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{cluster.Engine} {cluster.EngineVersion || ''}</td>
								<td class="px-4 py-3">
									<span class="px-2 py-0.5 rounded-full text-xs {cluster.Status === 'available' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' : 'bg-yellow-100 text-yellow-700'}">{cluster.Status || '—'}</span>
								</td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{cluster.DBClusterMembers?.length ?? 0}</td>
								<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400 max-w-xs truncate">{cluster.Endpoint || '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

{#if showCreateModal}
	<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
		<div class="bg-white dark:bg-slate-800 rounded-xl w-full max-w-md p-6 shadow-xl">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-2">Create RDS Instance</h2>
			<p class="text-slate-500 dark:text-slate-400 text-sm mb-4">
				Use the AWS console or CLI to create RDS instances with full configuration options.
			</p>
			<div class="flex justify-end">
				<button
					onclick={() => showCreateModal = false}
					class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700"
				>
					Close
				</button>
			</div>
		</div>
	</div>
{/if}
