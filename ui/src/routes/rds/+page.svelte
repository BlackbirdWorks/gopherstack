<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
import { untrack } from 'svelte';
import { onRegionChange } from '$lib/region-effect.svelte';
import { multiRegionList } from '$lib/multi-region';
import RegionChip from '$lib/components/RegionChip.svelte';
import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
import { getRDSClient } from '$lib/aws-client';
import {
	DescribeDBInstancesCommand,
	DeleteDBInstanceCommand,
	DescribeBlueGreenDeploymentsCommand,
	DescribeDBShardGroupsCommand,
	DescribeIntegrationsCommand,
	DescribeTenantDatabasesCommand,
	type BlueGreenDeployment,
	type DBShardGroup,
	type Integration,
	type TenantDatabase,
	DescribeDBSnapshotsCommand,
	DescribeDBClustersCommand,
	DescribeDBParameterGroupsCommand,
	DescribeDBSubnetGroupsCommand,
	DescribeDBParametersCommand,
	ModifyDBParameterGroupCommand,
	RestoreDBInstanceFromDBSnapshotCommand,
	type DBInstance,
	type DBSnapshot,
	type DBCluster,
	type DBParameterGroup,
	type DBSubnetGroup,
	type Parameter
} from '@aws-sdk/client-rds';
import { toast } from 'svelte-sonner';
import {
	Database,
	Plus,
	BarChart2,
	RefreshCw,
	GitMerge,
	Box,
	Link,
	Users,
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
	Layers,
	Sliders,
	Network
} from 'lucide-svelte';

// Every row carries the region its Describe*Command call was made against.
// Row actions (delete/restore/edit) must use THIS region, not the page's
// shared `rds()` client -- in All mode the same identifier can legitimately
// exist in two different regions.
type Regioned<T> = T & { region: string };

let instances = $state<Regioned<DBInstance>[]>([]);
let snapshots = $state<Regioned<DBSnapshot>[]>([]);
let clusters = $state<Regioned<DBCluster>[]>([]);
let loading = $state(true);
let search = $state('');
let snapshotSearch = $state('');
let engineFilter = $state('all');
let showCreateModal = $state(false);
let expandedInstance = $state<string | null>(null);
let activeTab = $state<'instances' | 'snapshots' | 'clusters' | 'paramgroups' | 'subnetgroups' | 'pi' | 'bluegreen' | 'shardgroups' | 'integrations' | 'tenantdbs'>('instances');
let blueGreenDeployments = $state<Regioned<BlueGreenDeployment>[]>([]);
let shardGroups = $state<Regioned<DBShardGroup>[]>([]);
let integrations = $state<Regioned<Integration>[]>([]);
let tenantDatabases = $state<Regioned<TenantDatabase>[]>([]);
let piInstances = $derived(instances.filter(i => i.PerformanceInsightsEnabled));
let paramGroups = $state<Regioned<DBParameterGroup>[]>([]);
let subnetGroups = $state<Regioned<DBSubnetGroup>[]>([]);
// Parameter-group editor state
let expandedParamGroup = $state<string | null>(null);
let pgParameters = $state<Parameter[]>([]);
let pgParamsLoading = $state(false);
let pgParamSearch = $state('');
let pgEdits = $state<Record<string, string>>({});
let savingPgParams = $state(false);
// Snapshot restore state
let restoreSnapshotId = $state<string | null>(null);
let restoreSnapshotRegion = $state<string>('');
let restoreInstanceId = $state('');
let restoring = $state(false);

// Expanded rows and in-progress editors reference resources (instance ids,
// parameter group names, snapshot ids) that are only unique within the
// region they were fetched from, and every non-instances tab is lazily
// loaded per-tab (see selectTab/refresh below) rather than up front — so a
// region change must clear that selection state and reload whichever tab
// is currently active, not just the instances list. `activeTab` is read
// via untrack() because selectTab() also writes it: without untrack, every
// tab switch would re-trigger this region effect and double-fetch.
onRegionChange(() => {
	expandedInstance = null;
	expandedParamGroup = null;
	pgParameters = [];
	pgEdits = {};
	restoreSnapshotId = null;
	restoreInstanceId = '';
	instances = [];
	const tab = untrack(() => activeTab);
	if (tab === 'snapshots') void loadSnapshots();
	else if (tab === 'clusters') void loadClusters();
	else if (tab === 'paramgroups') void loadParamGroups();
	else if (tab === 'subnetgroups') void loadSubnetGroups();
	else if (tab === 'bluegreen') void loadBlueGreenDeployments();
	else if (tab === 'shardgroups') void loadShardGroups();
	else if (tab === 'integrations') void loadIntegrations();
	else if (tab === 'tenantdbs') void loadTenantDatabases();
	// 'instances' and 'pi' (derived from instances) fall through to here.
	else void loadInstances();
});

// expandedParamGroup is keyed by "region::name" -- a bare name would collide
// across regions under All mode.
async function toggleParamGroup(name: string, region: string) {
	const key = `${region}::${name}`;
	if (expandedParamGroup === key) {
		expandedParamGroup = null;
		return;
	}
	expandedParamGroup = key;
	pgParameters = [];
	pgEdits = {};
	pgParamSearch = '';
	pgParamsLoading = true;
	try {
		const data = await getRDSClient(region).send(new DescribeDBParametersCommand({ DBParameterGroupName: name, MaxRecords: 100 }));
		pgParameters = data.Parameters || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load parameters');
	} finally {
		pgParamsLoading = false;
	}
}

async function savePgParams(name: string, region: string) {
	const changed = Object.entries(pgEdits).filter(([k, v]) => {
		const orig = pgParameters.find((p) => p.ParameterName === k);
		return orig && v !== (orig.ParameterValue ?? '');
	});
	if (changed.length === 0) {
		toast.error('No parameter changes to save');
		return;
	}
	savingPgParams = true;
	try {
		await getRDSClient(region).send(new ModifyDBParameterGroupCommand({
			DBParameterGroupName: name,
			Parameters: changed.map(([k, v]) => {
				const orig = pgParameters.find((p) => p.ParameterName === k);
				return { ParameterName: k, ParameterValue: v, ApplyMethod: orig?.ApplyType === 'static' ? 'pending-reboot' : 'immediate' };
			})
		}));
		toast.success(`Updated ${changed.length} parameter(s)`);
		await toggleParamGroup(name, region);
		expandedParamGroup = `${region}::${name}`;
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to update parameters');
	} finally {
		savingPgParams = false;
	}
}

async function restoreSnapshot() {
	if (!restoreSnapshotId || !restoreInstanceId.trim()) {
		toast.error('New instance identifier is required');
		return;
	}
	restoring = true;
	try {
		await getRDSClient(restoreSnapshotRegion).send(new RestoreDBInstanceFromDBSnapshotCommand({
			DBSnapshotIdentifier: restoreSnapshotId,
			DBInstanceIdentifier: restoreInstanceId.trim()
		}));
		toast.success(`Restoring snapshot to "${restoreInstanceId.trim()}"`);
		restoreSnapshotId = null;
		restoreInstanceId = '';
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to restore snapshot');
	} finally {
		restoring = false;
	}
}


// Every load* function goes through multiRegionList: in single-region mode
// that's exactly one Describe*Command call (unchanged behavior), and in All
// mode it fans out across every region with data, tagging each row.
async function loadRegioned<TResponse, TItem>(
	label: string,
	regionCall: (region: string) => Promise<TResponse>,
	extractItems: (r: TResponse) => TItem[],
	assign: (items: Regioned<TItem>[]) => void
) {
	try {
		loading = true;
		const result = await multiRegionList(regionCall, extractItems);
		assign(result.items.map(({ region, item }) => ({ ...item, region }) as Regioned<TItem>));
		if (result.errors.length > 0) toast.error(`Failed to load ${label} from ${result.errors.length} region(s)`);
	} catch (e) {
		toast.error(e instanceof Error ? e.message : `Failed to load ${label}`);
	} finally {
		loading = false;
	}
}

async function loadBlueGreenDeployments() {
	await loadRegioned('blue/green deployments',
		(region) => getRDSClient(region).send(new DescribeBlueGreenDeploymentsCommand({})),
		(r) => r.BlueGreenDeployments ?? [],
		(items) => blueGreenDeployments = items);
}

async function loadShardGroups() {
	await loadRegioned('DB shard groups',
		(region) => getRDSClient(region).send(new DescribeDBShardGroupsCommand({})),
		(r) => r.DBShardGroups ?? [],
		(items) => shardGroups = items);
}

async function loadIntegrations() {
	await loadRegioned('integrations',
		(region) => getRDSClient(region).send(new DescribeIntegrationsCommand({})),
		(r) => r.Integrations ?? [],
		(items) => integrations = items);
}

async function loadTenantDatabases() {
	await loadRegioned('tenant databases',
		(region) => getRDSClient(region).send(new DescribeTenantDatabasesCommand({})),
		(r) => r.TenantDatabases ?? [],
		(items) => tenantDatabases = items);
}

async function loadInstances() {
	await loadRegioned('RDS instances',
		(region) => getRDSClient(region).send(new DescribeDBInstancesCommand({})),
		(r) => r.DBInstances ?? [],
		(items) => instances = items);
}

async function loadSnapshots() {
	await loadRegioned('snapshots',
		(region) => getRDSClient(region).send(new DescribeDBSnapshotsCommand({ MaxRecords: 100 })),
		(r) => r.DBSnapshots ?? [],
		(items) => snapshots = items);
}

async function loadClusters() {
	await loadRegioned('clusters',
		(region) => getRDSClient(region).send(new DescribeDBClustersCommand({})),
		(r) => r.DBClusters ?? [],
		(items) => clusters = items);
}

async function loadParamGroups() {
	await loadRegioned('parameter groups',
		(region) => getRDSClient(region).send(new DescribeDBParameterGroupsCommand({})),
		(r) => r.DBParameterGroups ?? [],
		(items) => paramGroups = items);
}

async function loadSubnetGroups() {
	await loadRegioned('subnet groups',
		(region) => getRDSClient(region).send(new DescribeDBSubnetGroupsCommand({})),
		(r) => r.DBSubnetGroups ?? [],
		(items) => subnetGroups = items);
}

async function selectTab(t: typeof activeTab) {
	activeTab = t;
	if (t === 'instances' && instances.length === 0) await loadInstances();
	else if (t === 'snapshots') await loadSnapshots();
	else if (t === 'clusters') await loadClusters();
	else if (t === 'paramgroups') await loadParamGroups();
	else if (t === 'subnetgroups') await loadSubnetGroups();
}

async function refresh() {
	if (activeTab === 'instances') { instances = []; await loadInstances(); }
	else if (activeTab === 'snapshots') { snapshots = []; await loadSnapshots(); }
	else if (activeTab === 'clusters') { clusters = []; await loadClusters(); }
	else if (activeTab === 'paramgroups') { paramGroups = []; await loadParamGroups(); }
	else { subnetGroups = []; await loadSubnetGroups(); }
}

async function deleteInstance(id: string, region: string) {
	if (!await confirmDestructive({ title: 'Delete RDS Instance', message: `Delete instance ${id}? All data will be permanently lost unless a final snapshot was taken.` })) return;
	try {
		await getRDSClient(region).send(new DeleteDBInstanceCommand({ DBInstanceIdentifier: id, SkipFinalSnapshot: true }));
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

function toggleExpand(id: string, region: string) {
	const key = `${region}::${id}`;
	expandedInstance = expandedInstance === key ? null : key;
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
			<button onclick={() => selectTab('paramgroups')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'paramgroups' ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Sliders class="w-4 h-4" /> Parameter Groups {#if paramGroups.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{paramGroups.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('subnetgroups')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'subnetgroups' ? 'border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<HardDrive class="w-4 h-4" /> Subnet Groups {#if subnetGroups.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{subnetGroups.length}</span>{/if}
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
				{@const expanded = expandedInstance === `${instance.region}::${instance.DBInstanceIdentifier}`}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
					<!-- Main row -->
					<div class="p-5">
						<div class="flex items-start justify-between mb-4">
							<div class="flex-1 min-w-0">
								<div class="flex items-center gap-2">
									<h3 class="font-semibold text-slate-900 dark:text-white text-lg">{instance.DBInstanceIdentifier}</h3>
									<RegionChip region={instance.region} />
								</div>
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
									onclick={() => deleteInstance(instance.DBInstanceIdentifier ?? '', instance.region)}
									class="px-3 py-1.5 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center gap-1"
								>
									<Trash2 class="w-3 h-3" /> Delete
								</button>
							</div>
							<button
							onclick={() => toggleExpand(instance.DBInstanceIdentifier ?? '', instance.region)}
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
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Region</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Instance</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Engine</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Status</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Type</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Size</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Created</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
							{#each filteredSnapshots as snap}
								<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
									<td class="px-4 py-3 font-mono text-xs text-slate-900 dark:text-white">{snap.DBSnapshotIdentifier}</td>
									<td class="px-4 py-3"><RegionChip region={snap.region} /></td>
									<td class="px-4 py-3 text-sm text-slate-600 dark:text-slate-300">{snap.DBInstanceIdentifier || '—'}</td>
									<td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400">{snap.Engine || '—'}</td>
									<td class="px-4 py-3">
										<span class="px-2 py-0.5 rounded-full text-xs {snap.Status === 'available' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-300'}">{snap.Status || '—'}</span>
									</td>
									<td class="px-4 py-3 text-xs text-slate-500">{snap.SnapshotType || '—'}</td>
									<td class="px-4 py-3 text-xs text-slate-500">{snap.AllocatedStorage ? snap.AllocatedStorage + ' GiB' : '—'}</td>
									<td class="px-4 py-3 text-xs text-slate-500">{formatDate(snap.SnapshotCreateTime)}</td>
									<td class="px-4 py-3">
										{#if restoreSnapshotId === snap.DBSnapshotIdentifier && restoreSnapshotRegion === snap.region}
											<div class="flex items-center gap-1">
												<input type="text" bind:value={restoreInstanceId} placeholder="new-instance-id" class="w-32 px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
												<button disabled={restoring} onclick={restoreSnapshot} class="px-2 py-1 text-xs bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50">{restoring ? '...' : 'Go'}</button>
												<button onclick={() => { restoreSnapshotId = null; restoreInstanceId = ''; }} class="px-2 py-1 text-xs text-slate-500">Cancel</button>
											</div>
										{:else}
											<button onclick={() => { restoreSnapshotId = snap.DBSnapshotIdentifier ?? null; restoreSnapshotRegion = snap.region; restoreInstanceId = `${snap.DBInstanceIdentifier ?? 'restored'}-restore`; }} class="px-2 py-1 text-xs bg-blue-600 text-white rounded hover:bg-blue-700">Restore</button>
										{/if}
									</td>
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
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Region</th>
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
								<td class="px-4 py-3"><RegionChip region={cluster.region} /></td>
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

	<!-- Parameter Groups Tab -->
	{#if activeTab === 'paramgroups'}
		{#if loading}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div></div>
		{:else if paramGroups.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Sliders class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
				<p class="text-slate-500 dark:text-slate-400">No parameter groups found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-slate-50 dark:bg-slate-700">
						<tr>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Name</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Region</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Family</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Description</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Parameters</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
						{#each paramGroups as pg}
							<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50 cursor-pointer" onclick={() => toggleParamGroup(pg.DBParameterGroupName ?? '', pg.region)}>
								<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{pg.DBParameterGroupName || '—'}</td>
								<td class="px-4 py-3"><RegionChip region={pg.region} /></td>
								<td class="px-4 py-3 text-sm text-slate-600 dark:text-slate-300">{pg.DBParameterGroupFamily || '—'}</td>
								<td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400 max-w-xs truncate">{pg.Description || '—'}</td>
								<td class="px-4 py-3 text-xs text-blue-500">{expandedParamGroup === `${pg.region}::${pg.DBParameterGroupName}` ? 'Hide ▲' : 'Edit ▼'}</td>
							</tr>
							{#if expandedParamGroup === `${pg.region}::${pg.DBParameterGroupName}`}
								<tr class="bg-slate-50 dark:bg-slate-900/40">
									<td colspan="5" class="px-4 py-4">
										{#if pgParamsLoading}
											<p class="text-sm text-slate-500">Loading parameters...</p>
										{:else}
											<div class="flex items-center justify-between mb-2 gap-2">
												<input type="text" bind:value={pgParamSearch} placeholder="Filter parameters..." class="flex-1 px-3 py-1.5 text-sm border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
												<button disabled={savingPgParams} onclick={() => savePgParams(pg.DBParameterGroupName ?? '', pg.region)} class="px-3 py-1.5 text-sm bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 whitespace-nowrap">{savingPgParams ? 'Saving...' : 'Save Changes'}</button>
											</div>
											<div class="max-h-72 overflow-y-auto">
												<table class="w-full text-xs">
													<thead class="text-slate-500 dark:text-slate-400">
														<tr><th class="text-left py-1">Parameter</th><th class="text-left py-1">Value</th><th class="text-left py-1">Apply</th></tr>
													</thead>
													<tbody>
														{#each pgParameters.filter((p) => !pgParamSearch || (p.ParameterName ?? '').toLowerCase().includes(pgParamSearch.toLowerCase())) as param}
															<tr class="border-t border-slate-200 dark:border-slate-700">
																<td class="py-1.5 font-mono text-slate-700 dark:text-slate-300 pr-2">{param.ParameterName}</td>
																<td class="py-1.5 pr-2">
																	{#if param.IsModifiable}
																		<input type="text" value={pgEdits[param.ParameterName ?? ''] ?? param.ParameterValue ?? ''} oninput={(e) => { pgEdits[param.ParameterName ?? ''] = e.currentTarget.value; }} class="w-40 px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
																	{:else}
																		<span class="text-slate-500 font-mono">{param.ParameterValue || '—'} <span class="text-[10px] italic">(read-only)</span></span>
																	{/if}
																</td>
																<td class="py-1.5 text-slate-400">{param.ApplyType ?? '—'}</td>
															</tr>
														{/each}
													</tbody>
												</table>
											</div>
										{/if}
									</td>
								</tr>
							{/if}
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Subnet Groups Tab -->
	{#if activeTab === 'subnetgroups'}
		{#if loading}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div></div>
		{:else if subnetGroups.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Network class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
				<p class="text-slate-500 dark:text-slate-400">No subnet groups found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-slate-50 dark:bg-slate-700">
						<tr>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Name</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Region</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">VPC</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Status</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Subnets</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Description</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
						{#each subnetGroups as sg}
							<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
								<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{sg.DBSubnetGroupName || '—'}</td>
								<td class="px-4 py-3"><RegionChip region={sg.region} /></td>
								<td class="px-4 py-3 font-mono text-xs text-slate-500">{sg.VpcId || '—'}</td>
								<td class="px-4 py-3">
									<span class="px-2 py-0.5 rounded-full text-xs {sg.SubnetGroupStatus === 'Complete' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-300'}">{sg.SubnetGroupStatus || '—'}</span>
								</td>
								<td class="px-4 py-3 text-xs text-slate-500">{(sg.Subnets || []).length} subnet{(sg.Subnets || []).length !== 1 ? 's' : ''}</td>
								<td class="px-4 py-3 text-sm text-slate-500 dark:text-slate-400 max-w-xs truncate">{sg.DBSubnetGroupDescription || '—'}</td>
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
			<div class="flex items-center justify-between mb-2">
				<h2 class="text-xl font-bold text-slate-900 dark:text-white">Create RDS Instance</h2>
				<WriteRegionHint />
			</div>
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


	<!-- Performance Insights Tab -->
	{#if activeTab === 'pi'}
		{#if loading}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div></div>
		{:else if piInstances.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
				<BarChart2 class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
				<p class="text-slate-500 dark:text-slate-400">No instances with Performance Insights enabled</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-slate-50 dark:bg-slate-700">
						<tr>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Instance ID</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Region</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Engine</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Retention Period</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">KMS Key</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
						{#each piInstances as instance}
							<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors">
								<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{instance.DBInstanceIdentifier}</td>
								<td class="px-4 py-3"><RegionChip region={instance.region} /></td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{instance.Engine}</td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{instance.PerformanceInsightsRetentionPeriod ?? 7} days</td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400 font-mono text-xs max-w-xs truncate">{instance.PerformanceInsightsKMSKeyId || 'default'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Blue/Green Deployments Tab -->
	{#if activeTab === 'bluegreen'}
		{#if loading}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div></div>
		{:else if blueGreenDeployments.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
				<GitMerge class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
				<p class="text-slate-500 dark:text-slate-400">No blue/green deployments found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-slate-50 dark:bg-slate-700">
						<tr>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Deployment Name</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Region</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Status</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Source</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Target</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
						{#each blueGreenDeployments as bgd}
							<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors">
								<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{bgd.BlueGreenDeploymentName}</td>
								<td class="px-4 py-3"><RegionChip region={bgd.region} /></td>
								<td class="px-4 py-3">
									<span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-blue-50 text-blue-700 dark:bg-blue-500/10 dark:text-blue-400">
										{bgd.Status}
									</span>
								</td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{bgd.Source || '—'}</td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{bgd.Target || '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Shard Groups Tab -->
	{#if activeTab === 'shardgroups'}
		{#if loading}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div></div>
		{:else if shardGroups.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Box class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
				<p class="text-slate-500 dark:text-slate-400">No DB shard groups found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-slate-50 dark:bg-slate-700">
						<tr>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Shard Group ID</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Region</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Status</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Max ACU</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
						{#each shardGroups as sg}
							<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors">
								<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{sg.DBShardGroupIdentifier}</td>
								<td class="px-4 py-3"><RegionChip region={sg.region} /></td>
								<td class="px-4 py-3">
									<span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300">
										{sg.Status || '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{sg.MaxACU || '—'} ACU</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Integrations Tab -->
	{#if activeTab === 'integrations'}
		{#if loading}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div></div>
		{:else if integrations.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Link class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
				<p class="text-slate-500 dark:text-slate-400">No zero-ETL integrations found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-slate-50 dark:bg-slate-700">
						<tr>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Integration Name</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Region</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Status</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Source</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Target</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
						{#each integrations as intg}
							<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors">
								<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{intg.IntegrationName}</td>
								<td class="px-4 py-3"><RegionChip region={intg.region} /></td>
								<td class="px-4 py-3">
									<span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300">
										{intg.Status}
									</span>
								</td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400 max-w-xs truncate font-mono text-xs">{intg.SourceArn || '—'}</td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400 max-w-xs truncate font-mono text-xs">{intg.TargetArn || '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Tenant Databases Tab -->
	{#if activeTab === 'tenantdbs'}
		{#if loading}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div></div>
		{:else if tenantDatabases.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Users class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
				<p class="text-slate-500 dark:text-slate-400">No tenant databases found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-slate-50 dark:bg-slate-700">
						<tr>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Tenant DB Name</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Region</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Instance</th>
							<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Status</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
						{#each tenantDatabases as tdb}
							<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50 transition-colors">
								<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{tdb.TenantDBName}</td>
								<td class="px-4 py-3"><RegionChip region={tdb.region} /></td>
								<td class="px-4 py-3 text-slate-500 dark:text-slate-400 font-mono text-xs">{tdb.DBInstanceIdentifier || '—'}</td>
								<td class="px-4 py-3">
									<span class="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300">
										{tdb.Status || '—'}
									</span>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
