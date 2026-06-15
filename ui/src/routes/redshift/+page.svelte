<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getRedshiftClient } from '$lib/aws-client';
	import {
		DescribeClustersCommand,
		CreateClusterCommand,
		DeleteClusterCommand,
		RebootClusterCommand,
		CreateClusterSnapshotCommand,
		DescribeClusterSnapshotsCommand,
		ResizeClusterCommand,
		type Cluster,
		type Snapshot
	} from '@aws-sdk/client-redshift';
	import { toast } from 'svelte-sonner';
	import { Database, Search, RefreshCw, Plus, Trash2, RotateCcw, ChevronRight, Archive, Settings } from 'lucide-svelte';

	const redshift = getRedshiftClient();

	let loading = $state(false);
	let clusters = $state<Cluster[]>([]);
	let selectedCluster = $state<Cluster | null>(null);
	let activeTab = $state<'overview' | 'snapshots' | 'resize'>('overview');
	let searchQuery = $state('');

	// Snapshots
	let snapshots = $state<Snapshot[]>([]);
	let loadingSnapshots = $state(false);

	// Create Cluster
	let showCreateCluster = $state(false);
	let creatingCluster = $state(false);
	let newClusterId = $state('');
	let newNodeType = $state('dc2.large');
	let newClusterType = $state<'single-node' | 'multi-node'>('single-node');
	let newNumNodes = $state(2);
	let newMasterUser = $state('admin');
	let newMasterPassword = $state('');
	let newDbName = $state('dev');
	let newPort = $state(5439);

	// Create Snapshot
	let showCreateSnapshot = $state(false);
	let creatingSnapshot = $state(false);
	let newSnapshotId = $state('');

	// Resize
	let resizingCluster = $state(false);
	let resizeNodeType = $state('');
	let resizeNumNodes = $state(1);

	const filteredClusters = $derived(
		clusters.filter((c) => !searchQuery || (c.ClusterIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const statusColor = (status: string | undefined) => {
		if (!status) return 'gray';
		if (status === 'available') return 'green';
		if (status === 'creating' || status === 'modifying') return 'yellow';
		if (status === 'deleting' || status === 'unavailable') return 'red';
		return 'blue';
	};

	const nodeTypes = [
		'dc2.large', 'dc2.8xlarge',
		'ra3.xlplus', 'ra3.4xlarge', 'ra3.16xlarge',
		'ds2.xlarge', 'ds2.8xlarge'
	];

	async function loadClusters() {
		loading = true;
		try {
			const resp = await redshift.send(new DescribeClustersCommand({ MaxRecords: 50 }));
			clusters = resp.Clusters ?? [];
		} catch (e) {
			toast.error('Failed to load clusters: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function selectCluster(cluster: Cluster) {
		selectedCluster = cluster;
		activeTab = 'overview';
		resizeNodeType = cluster.NodeType ?? '';
		resizeNumNodes = cluster.NumberOfNodes ?? 1;
	}

	async function handleTabChange(tab: 'overview' | 'snapshots' | 'resize') {
		activeTab = tab;
		if (tab === 'snapshots' && selectedCluster && snapshots.length === 0) {
			await loadSnapshots(selectedCluster.ClusterIdentifier ?? '');
		}
	}

	async function loadSnapshots(clusterId: string) {
		loadingSnapshots = true;
		try {
			const resp = await redshift.send(new DescribeClusterSnapshotsCommand({
				ClusterIdentifier: clusterId,
				MaxRecords: 20
			}));
			snapshots = resp.Snapshots ?? [];
		} catch (e) {
			toast.error('Failed to load snapshots: ' + String(e));
		} finally {
			loadingSnapshots = false;
		}
	}

	async function createCluster() {
		if (!newClusterId.trim() || !newMasterPassword.trim()) return;
		creatingCluster = true;
		try {
			await redshift.send(new CreateClusterCommand({
				ClusterIdentifier: newClusterId.trim(),
				NodeType: newNodeType,
				ClusterType: newClusterType,
				NumberOfNodes: newClusterType === 'multi-node' ? newNumNodes : undefined,
				MasterUsername: newMasterUser.trim(),
				MasterUserPassword: newMasterPassword,
				DBName: newDbName.trim() || 'dev',
				Port: newPort
			}));
			toast.success(`Cluster "${newClusterId}" creation initiated`);
			showCreateCluster = false;
			resetCreateForm();
			await loadClusters();
		} catch (e) {
			toast.error('Failed to create cluster: ' + String(e));
		} finally {
			creatingCluster = false;
		}
	}

	function resetCreateForm() {
		newClusterId = '';
		newNodeType = 'dc2.large';
		newClusterType = 'single-node';
		newNumNodes = 2;
		newMasterUser = 'admin';
		newMasterPassword = '';
		newDbName = 'dev';
		newPort = 5439;
	}

	async function deleteCluster(id: string) {
		if (!await confirmDestructive({ title: 'Delete Redshift Cluster', message: `Delete cluster "${id}"? All data will be permanently lost unless a final snapshot was created.` })) return;
		try {
			await redshift.send(new DeleteClusterCommand({
				ClusterIdentifier: id,
				SkipFinalClusterSnapshot: true
			}));
			toast.success(`Cluster "${id}" deletion initiated`);
			if (selectedCluster?.ClusterIdentifier === id) selectedCluster = null;
			await loadClusters();
		} catch (e) {
			toast.error('Failed to delete cluster: ' + String(e));
		}
	}

	async function rebootCluster(id: string) {
		try {
			await redshift.send(new RebootClusterCommand({ ClusterIdentifier: id }));
			toast.success(`Cluster "${id}" rebooting`);
			await loadClusters();
		} catch (e) {
			toast.error('Failed to reboot: ' + String(e));
		}
	}

	async function createSnapshot() {
		if (!selectedCluster || !newSnapshotId.trim()) return;
		creatingSnapshot = true;
		try {
			await redshift.send(new CreateClusterSnapshotCommand({
				ClusterIdentifier: selectedCluster.ClusterIdentifier ?? '',
				SnapshotIdentifier: newSnapshotId.trim()
			}));
			toast.success(`Snapshot "${newSnapshotId}" creation initiated`);
			showCreateSnapshot = false;
			newSnapshotId = '';
			snapshots = [];
			await loadSnapshots(selectedCluster.ClusterIdentifier ?? '');
		} catch (e) {
			toast.error('Failed to create snapshot: ' + String(e));
		} finally {
			creatingSnapshot = false;
		}
	}

	async function resizeCluster() {
		if (!selectedCluster) return;
		resizingCluster = true;
		try {
			await redshift.send(new ResizeClusterCommand({
				ClusterIdentifier: selectedCluster.ClusterIdentifier ?? '',
				NodeType: resizeNodeType,
				NumberOfNodes: resizeNumNodes,
				Classic: false
			}));
			toast.success('Cluster resize initiated');
			await selectCluster(selectedCluster);
		} catch (e) {
			toast.error('Failed to resize: ' + String(e));
		} finally {
			resizingCluster = false;
		}
	}

	function formatDate(d: Date | undefined): string {
		if (!d) return '-';
		return d.toLocaleString();
	}

	onMount(loadClusters);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Database class="w-7 h-7 text-rose-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Redshift</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Cloud data warehouse</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={loadClusters} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			<button onclick={() => (showCreateCluster = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 text-sm font-medium">
				<Plus class="w-4 h-4" /> Create Cluster
			</button>
		</div>
	</div>

	{#if selectedCluster}
		<!-- Cluster Detail -->
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedCluster = null; snapshots = []; }} class="text-rose-600 hover:underline">Clusters</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="text-gray-600 dark:text-gray-300 font-medium">{selectedCluster.ClusterIdentifier}</span>
			<span class={`ml-2 px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(selectedCluster.ClusterStatus)}-100 text-${statusColor(selectedCluster.ClusterStatus)}-700`}>
				{selectedCluster.ClusterStatus}
			</span>
		</div>

		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['overview', 'Overview'], ['snapshots', 'Snapshots'], ['resize', 'Resize']] as [tab, label]}
				<button
					onclick={() => handleTabChange(tab as 'overview' | 'snapshots' | 'resize')}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-rose-500 text-rose-600 dark:text-rose-400' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
				>
					{label}
				</button>
			{/each}
			<button onclick={() => rebootCluster(selectedCluster?.ClusterIdentifier ?? '')} class="ml-auto px-4 py-2 text-sm text-amber-600 hover:underline flex items-center gap-1">
				<RotateCcw class="w-3.5 h-3.5" /> Reboot
			</button>
			<button onclick={() => deleteCluster(selectedCluster?.ClusterIdentifier ?? '')} class="px-4 py-2 text-sm text-red-500 hover:underline">Delete</button>
		</div>

		{#if activeTab === 'overview'}
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#each [
					{ label: 'Status', value: selectedCluster.ClusterStatus ?? '-' },
					{ label: 'Endpoint', value: selectedCluster.Endpoint ? `${selectedCluster.Endpoint.Address}:${selectedCluster.Endpoint.Port}` : '-' },
					{ label: 'Database', value: selectedCluster.DBName ?? '-' },
					{ label: 'Master Username', value: selectedCluster.MasterUsername ?? '-' },
					{ label: 'Node Type', value: selectedCluster.NodeType ?? '-' },
					{ label: 'Number of Nodes', value: String(selectedCluster.NumberOfNodes ?? 1) },
					{ label: 'Cluster Type', value: (selectedCluster.NumberOfNodes ?? 1) > 1 ? 'multi-node' : 'single-node' },
					{ label: 'Availability Zone', value: selectedCluster.AvailabilityZone ?? '-' },
					{ label: 'Created', value: formatDate(selectedCluster.ClusterCreateTime) },
					{ label: 'VPC ID', value: selectedCluster.VpcId ?? '-' },
					{ label: 'Encryption', value: selectedCluster.Encrypted ? 'Encrypted' : 'Not Encrypted' }
				] as row}
					<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
						<div class="text-xs text-gray-500 font-medium">{row.label}</div>
						<div class="text-sm text-gray-900 dark:text-white mt-1 truncate font-mono">{row.value}</div>
					</div>
				{/each}
			</div>

			{#if selectedCluster.ClusterNodes && selectedCluster.ClusterNodes.length > 0}
				<div>
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Nodes</h3>
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
								<tr>
									<th class="px-4 py-3 text-left">Role</th>
									<th class="px-4 py-3 text-left">Private IP</th>
									<th class="px-4 py-3 text-left">Public IP</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each selectedCluster.ClusterNodes as node}
									<tr>
										<td class="px-4 py-3 text-sm">{node.NodeRole}</td>
										<td class="px-4 py-3 font-mono text-xs">{node.PrivateIPAddress ?? '-'}</td>
										<td class="px-4 py-3 font-mono text-xs">{node.PublicIPAddress ?? '-'}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'snapshots'}
			<div class="flex justify-end mb-2">
				<button onclick={() => (showCreateSnapshot = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 text-sm font-medium">
					<Archive class="w-4 h-4" /> Create Snapshot
				</button>
			</div>
			{#if loadingSnapshots}
				<div class="flex justify-center py-8"><div class="animate-spin w-8 h-8 border-4 border-rose-600 border-t-transparent rounded-full"></div></div>
			{:else if snapshots.length === 0}
				<div class="text-center py-12 text-gray-500"><Archive class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No snapshots</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Snapshot ID</th>
								<th class="px-4 py-3 text-left">Type</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Created</th>
								<th class="px-4 py-3 text-left">Size (GB)</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each snapshots as snap}
								<tr>
									<td class="px-4 py-3 font-medium text-rose-600 dark:text-rose-400">{snap.SnapshotIdentifier}</td>
									<td class="px-4 py-3 text-xs text-gray-500">{snap.SnapshotType}</td>
									<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(snap.Status)}-100 text-${statusColor(snap.Status)}-700`}>{snap.Status}</span></td>
									<td class="px-4 py-3 text-xs text-gray-500">{formatDate(snap.SnapshotCreateTime)}</td>
									<td class="px-4 py-3 text-gray-600">{snap.TotalBackupSizeInMegaBytes ? (snap.TotalBackupSizeInMegaBytes / 1024).toFixed(1) : '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'resize'}
			<div class="max-w-md space-y-4">
				<p class="text-sm text-gray-600 dark:text-gray-400">Current: {selectedCluster.NodeType} × {selectedCluster.NumberOfNodes ?? 1} nodes</p>
				<div>
					<label for="resize-node-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">New Node Type</label>
					<select id="resize-node-type" bind:value={resizeNodeType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm">
						{#each nodeTypes as t}<option value={t}>{t}</option>{/each}
					</select>
				</div>
				<div>
					<label for="resize-num-nodes" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Number of Nodes</label>
					<input id="resize-num-nodes" bind:value={resizeNumNodes} type="number" min="1" max="100" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
				</div>
				<button onclick={resizeCluster} disabled={resizingCluster} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 text-sm font-medium disabled:opacity-50">
					<Settings class="w-4 h-4" /> {resizingCluster ? 'Resizing...' : 'Apply Resize'}
				</button>
			</div>
		{/if}

	{:else}
		<!-- Cluster List -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search clusters..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-rose-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredClusters.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Database class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No clusters found</p>
				<p class="text-sm mt-1">Create a Redshift cluster to start analyzing your data</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Cluster ID</th>
							<th class="px-4 py-3 text-left">Status</th>
							<th class="px-4 py-3 text-left">Node Type</th>
							<th class="px-4 py-3 text-left">Nodes</th>
							<th class="px-4 py-3 text-left">Database</th>
							<th class="px-4 py-3 text-left">Endpoint</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredClusters as cluster}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button onclick={() => selectCluster(cluster)} class="text-rose-600 dark:text-rose-400 hover:underline font-medium">{cluster.ClusterIdentifier}</button>
								</td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(cluster.ClusterStatus)}-100 text-${statusColor(cluster.ClusterStatus)}-700`}>
										{cluster.ClusterStatus}
									</span>
								</td>
								<td class="px-4 py-3 text-xs text-gray-500">{cluster.NodeType}</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{cluster.NumberOfNodes ?? 1}</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{cluster.DBName}</td>
								<td class="px-4 py-3 font-mono text-xs text-gray-500 truncate max-w-xs">{cluster.Endpoint?.Address ?? '-'}</td>
								<td class="px-4 py-3 flex gap-1">
									<button onclick={() => rebootCluster(cluster.ClusterIdentifier ?? '')} class="text-amber-500 hover:text-amber-700 p-1" title="Reboot">
										<RotateCcw class="w-4 h-4" />
									</button>
									<button onclick={() => deleteCluster(cluster.ClusterIdentifier ?? '')} class="text-red-500 hover:text-red-700 p-1">
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Cluster Modal -->
{#if showCreateCluster}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-lg p-6 space-y-4 max-h-[90vh] overflow-y-auto">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Redshift Cluster</h2>
			<div>
				<label for="cluster-id" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Cluster Identifier</label>
				<input id="cluster-id" bind:value={newClusterId} type="text" placeholder="my-redshift-cluster" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="node-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Node Type</label>
					<select id="node-type" bind:value={newNodeType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						{#each nodeTypes as t}<option value={t}>{t}</option>{/each}
					</select>
				</div>
				<div>
					<label for="cluster-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Cluster Type</label>
					<select id="cluster-type" bind:value={newClusterType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="single-node">Single Node</option>
						<option value="multi-node">Multi Node</option>
					</select>
				</div>
			</div>
			{#if newClusterType === 'multi-node'}
				<div>
					<label for="num-nodes" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Number of Nodes</label>
					<input id="num-nodes" bind:value={newNumNodes} type="number" min="2" max="100" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			{/if}
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="master-user" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Master Username</label>
					<input id="master-user" bind:value={newMasterUser} type="text" placeholder="admin" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="db-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Database Name</label>
					<input id="db-name" bind:value={newDbName} type="text" placeholder="dev" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div>
				<label for="master-pw" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Master Password</label>
				<input id="master-pw" bind:value={newMasterPassword} type="password" placeholder="At least 8 chars, uppercase, lowercase, number" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => { showCreateCluster = false; resetCreateForm(); }} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createCluster} disabled={creatingCluster || !newClusterId.trim() || !newMasterPassword.trim()} class="flex-1 px-4 py-2 rounded-lg bg-rose-600 text-white text-sm font-medium hover:bg-rose-700 disabled:opacity-50">
					{creatingCluster ? 'Creating...' : 'Create Cluster'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Snapshot Modal -->
{#if showCreateSnapshot && selectedCluster}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-sm p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Snapshot</h2>
			<p class="text-sm text-gray-500">Cluster: {selectedCluster.ClusterIdentifier}</p>
			<div>
				<label for="snapshot-id" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Snapshot Identifier</label>
				<input id="snapshot-id" bind:value={newSnapshotId} type="text" placeholder="my-snapshot-2024" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateSnapshot = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={createSnapshot} disabled={creatingSnapshot || !newSnapshotId.trim()} class="flex-1 px-4 py-2 rounded-lg bg-rose-600 text-white text-sm font-medium hover:bg-rose-700 disabled:opacity-50">
					{creatingSnapshot ? 'Creating...' : 'Create Snapshot'}
				</button>
			</div>
		</div>
	</div>
{/if}
