<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getMemoryDBClient } from '$lib/aws-client';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import {
		DescribeClustersCommand,
		CreateClusterCommand,
		DeleteClusterCommand,
		DescribeSnapshotsCommand,
		CreateSnapshotCommand,
		DeleteSnapshotCommand,
		DescribeUsersCommand,
		CreateUserCommand,
		DeleteUserCommand,
		UpdateUserCommand,
		DescribeACLsCommand,
		CreateACLCommand,
		DeleteACLCommand,
		UpdateACLCommand,
		InputAuthenticationType,
		type Cluster,
		type Snapshot,
		type User,
		type ACL
	} from '@aws-sdk/client-memorydb';
	import { toast } from 'svelte-sonner';
	import { Database, Search, RefreshCw, ChevronRight, Layers, Plus, Trash2, Users, Shield, Camera, Pencil, X } from 'lucide-svelte';

	const client = regionalClient(getMemoryDBClient);

	type ActiveTab = 'clusters' | 'snapshots' | 'users' | 'acls';

	let loading = $state(false);
	let clusters = $state<Cluster[]>([]);
	let snapshots = $state<Snapshot[]>([]);
	let users = $state<User[]>([]);
	let acls = $state<ACL[]>([]);
	let selectedCluster = $state<Cluster | null>(null);
	let activeTab = $state<ActiveTab>('clusters');
	let searchQuery = $state('');

	// Create cluster modal state
	let showCreateCluster = $state(false);
	let newClusterName = $state('');
	let newClusterNodeType = $state('db.r6g.large');
	let newClusterACLName = $state('open-access');
	let newClusterEngineVersion = $state('7.0');
	let newClusterNumShards = $state(1);
	let newClusterNumReplicas = $state(1);
	let creatingCluster = $state(false);

	// Create user modal state
	let showCreateUser = $state(false);
	let newUserName = $state('');
	let newUserAccessString = $state('on ~* &* +@all');
	let creatingUser = $state(false);

	// Create ACL modal state
	let showCreateACL = $state(false);
	let newACLName = $state('');
	let creatingACL = $state(false);

	// Create snapshot modal state
	let showCreateSnapshot = $state(false);
	let newSnapshotName = $state('');
	let newSnapshotCluster = $state('');
	let creatingSnapshot = $state(false);

	// Update user modal state
	let showUpdateUser = $state(false);
	let updateUserTarget = $state<User | null>(null);
	let updateUserAccessString = $state('');
	let updatingUser = $state(false);

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

	const filteredUsers = $derived(
		users.filter(
			(u) => !searchQuery || (u.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredACLs = $derived(
		acls.filter(
			(a) => !searchQuery || (a.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const availableCount = $derived(clusters.filter((c) => c.Status === 'available').length);
	const totalShards = $derived(clusters.reduce((acc, c) => acc + (c.NumberOfShards ?? 0), 0));

	async function loadData() {
		loading = true;
		try {
			const [clustersResp, snapshotsResp, usersResp, aclsResp] = await Promise.all([
				client().send(new DescribeClustersCommand({})),
				client().send(new DescribeSnapshotsCommand({})),
				client().send(new DescribeUsersCommand({})),
				client().send(new DescribeACLsCommand({}))
			]);
			clusters = clustersResp.Clusters ?? [];
			snapshots = snapshotsResp.Snapshots ?? [];
			users = usersResp.Users ?? [];
			acls = aclsResp.ACLs ?? [];
		} catch (e) {
			toast.error('Failed to load MemoryDB data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function createCluster() {
		if (!newClusterName.trim()) {
			toast.error('Cluster name is required');
			return;
		}
		creatingCluster = true;
		try {
			await client().send(new CreateClusterCommand({
				ClusterName: newClusterName.trim(),
				NodeType: newClusterNodeType,
				ACLName: newClusterACLName,
				EngineVersion: newClusterEngineVersion,
				NumShards: newClusterNumShards,
				NumReplicasPerShard: newClusterNumReplicas,
			}));
			toast.success(`Cluster "${newClusterName}" created`);
			showCreateCluster = false;
			newClusterName = '';
			await loadData();
		} catch (e) {
			toast.error('Failed to create cluster: ' + String(e));
		} finally {
			creatingCluster = false;
		}
	}

	async function deleteCluster(cluster: Cluster) {
		if (!await confirmDestructive({ title: 'Delete Cluster', message: `Delete cluster "${cluster.Name}"? This action cannot be undone.` })) return;
		try {
			await client().send(new DeleteClusterCommand({ ClusterName: cluster.Name! }));
			toast.success(`Cluster "${cluster.Name}" deleted`);
			if (selectedCluster?.Name === cluster.Name) selectedCluster = null;
			await loadData();
		} catch (e) {
			toast.error('Failed to delete cluster: ' + String(e));
		}
	}

	async function createUser() {
		if (!newUserName.trim()) {
			toast.error('User name is required');
			return;
		}
		creatingUser = true;
		try {
			await client().send(new CreateUserCommand({
				UserName: newUserName.trim(),
				AccessString: newUserAccessString,
				AuthenticationMode: { Type: 'no-password' as InputAuthenticationType },
			}));
			toast.success(`User "${newUserName}" created`);
			showCreateUser = false;
			newUserName = '';
			await loadData();
		} catch (e) {
			toast.error('Failed to create user: ' + String(e));
		} finally {
			creatingUser = false;
		}
	}

	async function deleteUser(user: User) {
		if (!await confirmDestructive({ title: 'Delete User', message: `Delete user "${user.Name}"?` })) return;
		try {
			await client().send(new DeleteUserCommand({ UserName: user.Name! }));
			toast.success(`User "${user.Name}" deleted`);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete user: ' + String(e));
		}
	}

	async function createACL() {
		if (!newACLName.trim()) {
			toast.error('ACL name is required');
			return;
		}
		creatingACL = true;
		try {
			await client().send(new CreateACLCommand({ ACLName: newACLName.trim() }));
			toast.success(`ACL "${newACLName}" created`);
			showCreateACL = false;
			newACLName = '';
			await loadData();
		} catch (e) {
			toast.error('Failed to create ACL: ' + String(e));
		} finally {
			creatingACL = false;
		}
	}

	async function deleteACL(acl: ACL) {
		if (!await confirmDestructive({ title: 'Delete ACL', message: `Delete ACL "${acl.Name}"?` })) return;
		try {
			await client().send(new DeleteACLCommand({ ACLName: acl.Name! }));
			toast.success(`ACL "${acl.Name}" deleted`);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete ACL: ' + String(e));
		}
	}

	async function createSnapshot() {
		if (!newSnapshotName.trim()) {
			toast.error('Snapshot name is required');
			return;
		}
		if (!newSnapshotCluster.trim()) {
			toast.error('Cluster name is required');
			return;
		}
		creatingSnapshot = true;
		try {
			await client().send(new CreateSnapshotCommand({
				ClusterName: newSnapshotCluster.trim(),
				SnapshotName: newSnapshotName.trim(),
			}));
			toast.success(`Snapshot "${newSnapshotName}" created`);
			showCreateSnapshot = false;
			newSnapshotName = '';
			newSnapshotCluster = '';
			await loadData();
		} catch (e) {
			toast.error('Failed to create snapshot: ' + String(e));
		} finally {
			creatingSnapshot = false;
		}
	}

	async function deleteSnapshot(snap: Snapshot) {
		if (!await confirmDestructive({ title: 'Delete Snapshot', message: `Delete snapshot "${snap.Name}"? This action cannot be undone.` })) return;
		try {
			await client().send(new DeleteSnapshotCommand({ SnapshotName: snap.Name! }));
			toast.success(`Snapshot "${snap.Name}" deleted`);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete snapshot: ' + String(e));
		}
	}

	function openUpdateUser(user: User) {
		updateUserTarget = user;
		updateUserAccessString = user.AccessString ?? '';
		showUpdateUser = true;
	}

	async function updateUser() {
		if (!updateUserTarget) return;
		updatingUser = true;
		try {
			await client().send(new UpdateUserCommand({
				UserName: updateUserTarget.Name!,
				AccessString: updateUserAccessString,
			}));
			toast.success(`User "${updateUserTarget.Name}" updated`);
			showUpdateUser = false;
			updateUserTarget = null;
			await loadData();
		} catch (e) {
			toast.error('Failed to update user: ' + String(e));
		} finally {
			updatingUser = false;
		}
	}

	async function removeUserFromACL(aclName: string, userName: string) {
		try {
			await client().send(new UpdateACLCommand({
				ACLName: aclName,
				UserNamesToRemove: [userName],
			}));
			toast.success(`Removed "${userName}" from ACL "${aclName}"`);
			await loadData();
		} catch (e) {
			toast.error('Failed to remove user from ACL: ' + String(e));
		}
	}

	function takeClusterSnapshot(cluster: Cluster) {
		newSnapshotCluster = cluster.Name ?? '';
		showCreateSnapshot = true;
	}

	onRegionChange(loadData);
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
		<div class="flex items-center gap-2">
			<button onclick={() => { showCreateCluster = true; }} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white text-sm hover:bg-purple-700 transition-colors">
				<Plus class="w-4 h-4" /> Create Cluster
			</button>
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
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
		<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{selectedCluster.Name}</h2>
				<div class="flex items-center gap-2">
					<button
						onclick={() => takeClusterSnapshot(selectedCluster!)}
						class="flex items-center gap-1 px-3 py-1.5 text-sm text-blue-600 border border-blue-200 dark:border-blue-800 rounded-lg hover:bg-blue-50 dark:hover:bg-blue-950 transition-colors"
					>
						<Camera class="w-4 h-4" /> Snapshot
					</button>
					<button
						onclick={() => deleteCluster(selectedCluster!)}
						class="flex items-center gap-1 px-3 py-1.5 text-sm text-red-600 border border-red-200 dark:border-red-800 rounded-lg hover:bg-red-50 dark:hover:bg-red-950 transition-colors"
					>
						<Trash2 class="w-4 h-4" /> Delete
					</button>
				</div>
			</div>
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#each [
					{ label: 'Cluster ARN', value: selectedCluster.ARN ?? '-' },
					{ label: 'Status', value: selectedCluster.Status ?? '-' },
					{ label: 'Node Type', value: selectedCluster.NodeType ?? '-' },
					{ label: 'Engine Version', value: selectedCluster.EngineVersion ?? '-' },
					{ label: 'Number of Shards', value: String(selectedCluster.NumberOfShards ?? 0) },
					{ label: 'Replicas / Shard', value: String((selectedCluster as any).NumberOfReplicasPerShard ?? 0) },
					{ label: 'Availability Mode', value: (selectedCluster as any).AvailabilityMode ?? '-' },
					{ label: 'TLS Enabled', value: selectedCluster.TLSEnabled ? 'Yes' : 'No' },
					{ label: 'Endpoint', value: selectedCluster.ClusterEndpoint ? `${selectedCluster.ClusterEndpoint.Address}:${selectedCluster.ClusterEndpoint.Port}` : '-' },
				] as row}
					<div class="bg-gray-50 dark:bg-gray-800 rounded-lg p-4">
						<div class="text-xs text-gray-500 font-medium">{row.label}</div>
						<div class="text-sm text-gray-900 dark:text-white mt-1 font-mono truncate">{row.value}</div>
					</div>
				{/each}
			</div>
		</div>
	{:else}
		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['clusters', 'Clusters'], ['snapshots', 'Snapshots'], ['users', 'Users'], ['acls', 'ACLs']] as [tab, label]}
				<button
					onclick={() => { activeTab = tab as ActiveTab; searchQuery = ''; }}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
				>{label}</button>
			{/each}
		</div>

		<div class="flex items-center gap-3">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} type="text" placeholder="Search..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
			</div>
			{#if activeTab === 'users'}
				<button onclick={() => { showCreateUser = true; }} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white text-sm hover:bg-purple-700 transition-colors whitespace-nowrap">
					<Plus class="w-4 h-4" /> Create User
				</button>
			{:else if activeTab === 'acls'}
				<button onclick={() => { showCreateACL = true; }} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white text-sm hover:bg-purple-700 transition-colors whitespace-nowrap">
					<Plus class="w-4 h-4" /> Create ACL
				</button>
			{:else if activeTab === 'snapshots'}
				<button onclick={() => { newSnapshotCluster = ''; showCreateSnapshot = true; }} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white text-sm hover:bg-purple-700 transition-colors whitespace-nowrap">
					<Camera class="w-4 h-4" /> Create Snapshot
				</button>
			{/if}
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-purple-600 border-t-transparent rounded-full"></div></div>
		{:else if activeTab === 'clusters'}
			{#if filteredClusters.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Database class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No clusters found</p>
					<button onclick={() => { showCreateCluster = true; }} class="mt-4 inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-purple-600 text-white text-sm hover:bg-purple-700">
						<Plus class="w-4 h-4" /> Create Cluster
					</button>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Name</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Engine</th>
								<th class="px-4 py-3 text-left">Node Type</th>
								<th class="px-4 py-3 text-left">Shards</th>
								<th class="px-4 py-3 text-right">Actions</th>
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
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{cluster.EngineVersion ?? '-'}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{cluster.NodeType ?? '-'}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{cluster.NumberOfShards ?? 0}</td>
									<td class="px-4 py-3 text-right flex items-center justify-end gap-1">
										<button
											onclick={(e) => { e.stopPropagation(); takeClusterSnapshot(cluster); }}
											class="p-1.5 text-gray-400 hover:text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950 rounded transition-colors"
											title="Create snapshot"
										>
											<Camera class="w-4 h-4" />
										</button>
										<button
											onclick={(e) => { e.stopPropagation(); deleteCluster(cluster); }}
											class="p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-950 rounded transition-colors"
											title="Delete cluster"
										>
											<Trash2 class="w-4 h-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if activeTab === 'snapshots'}
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
								<th class="px-4 py-3 text-left">Engine</th>
								<th class="px-4 py-3 text-right">Actions</th>
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
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{snap.ClusterConfiguration?.EngineVersion ?? '-'}</td>
									<td class="px-4 py-3 text-right">
										<button
											onclick={() => deleteSnapshot(snap)}
											class="p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-950 rounded transition-colors"
											title="Delete snapshot"
										>
											<Trash2 class="w-4 h-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if activeTab === 'users'}
			{#if filteredUsers.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Users class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No users found</p>
					<button onclick={() => { showCreateUser = true; }} class="mt-4 inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-purple-600 text-white text-sm hover:bg-purple-700">
						<Plus class="w-4 h-4" /> Create User
					</button>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Name</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Access String</th>
								<th class="px-4 py-3 text-left">ACL Memberships</th>
								<th class="px-4 py-3 text-right">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredUsers as user}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{user.Name ?? '-'}</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${user.Status === 'active' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{user.Status ?? '-'}</span>
									</td>
									<td class="px-4 py-3 font-mono text-xs text-gray-600 dark:text-gray-300 truncate max-w-xs">{user.AccessString ?? '-'}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300 text-xs">
										{acls.filter(a => (a.UserNames ?? []).includes(user.Name ?? '')).map(a => a.Name).join(', ') || '-'}
									</td>
									<td class="px-4 py-3 text-right flex items-center justify-end gap-1">
										<button
											onclick={() => openUpdateUser(user)}
											class="p-1.5 text-gray-400 hover:text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950 rounded transition-colors"
											title="Edit user"
										>
											<Pencil class="w-4 h-4" />
										</button>
										<button
											onclick={() => deleteUser(user)}
											class="p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-950 rounded transition-colors"
											title="Delete user"
										>
											<Trash2 class="w-4 h-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if activeTab === 'acls'}
			{#if filteredACLs.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Shield class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No ACLs found</p>
					<button onclick={() => { showCreateACL = true; }} class="mt-4 inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-purple-600 text-white text-sm hover:bg-purple-700">
						<Plus class="w-4 h-4" /> Create ACL
					</button>
				</div>
			{:else}
				<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
					{#each filteredACLs as acl}
						<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4 space-y-3">
							<div class="flex items-center justify-between">
								<div class="flex items-center gap-2">
									<Shield class="w-4 h-4 text-purple-500" />
									<span class="font-semibold text-gray-900 dark:text-white text-sm">{acl.Name ?? '-'}</span>
									<span class={`px-2 py-0.5 rounded text-xs font-medium ${acl.Status === 'active' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{acl.Status ?? '-'}</span>
								</div>
								<button
									onclick={() => deleteACL(acl)}
									class="p-1.5 text-gray-400 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-950 rounded transition-colors"
									title="Delete ACL"
								>
									<Trash2 class="w-4 h-4" />
								</button>
							</div>
							<div>
								<p class="text-xs text-gray-500 dark:text-gray-400 mb-2">Members</p>
								{#if (acl.UserNames ?? []).length === 0}
									<p class="text-xs text-gray-400 italic">No members</p>
								{:else}
									<div class="flex flex-wrap gap-1">
										{#each (acl.UserNames ?? []) as userName}
											<span class="inline-flex items-center gap-1 px-2 py-0.5 bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 rounded-full text-xs">
												{userName}
												<button
													onclick={() => removeUserFromACL(acl.Name!, userName)}
													class="hover:text-red-500 transition-colors"
													title="Remove from ACL"
												>
													<X class="w-3 h-3" />
												</button>
											</span>
										{/each}
									</div>
								{/if}
							</div>
						</div>
					{/each}
				</div>
			{/if}
		{/if}
	{/if}
</div>

<!-- Create Cluster Modal -->
{#if showCreateCluster}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" role="dialog" aria-modal="true" aria-labelledby="modal-create-cluster-title">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 id="modal-create-cluster-title" class="text-lg font-semibold text-gray-900 dark:text-white">Create MemoryDB Cluster</h2>
			<div>
				<label for="cluster-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Cluster Name</label>
				<input id="cluster-name" bind:value={newClusterName} type="text" placeholder="my-cluster" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="node-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Node Type</label>
				<select id="node-type" bind:value={newClusterNodeType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="db.t4g.small">db.t4g.small</option>
					<option value="db.t4g.medium">db.t4g.medium</option>
					<option value="db.r6g.large">db.r6g.large</option>
					<option value="db.r6g.xlarge">db.r6g.xlarge</option>
					<option value="db.r6g.2xlarge">db.r6g.2xlarge</option>
				</select>
			</div>
			<div>
				<label for="engine-version" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Engine Version</label>
				<select id="engine-version" bind:value={newClusterEngineVersion} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="7.1">7.1</option>
					<option value="7.0">7.0</option>
					<option value="6.2">6.2</option>
				</select>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="num-shards" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Num Shards</label>
					<input id="num-shards" bind:value={newClusterNumShards} type="number" min="1" max="500" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="num-replicas" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Replicas / Shard</label>
					<input id="num-replicas" bind:value={newClusterNumReplicas} type="number" min="0" max="5" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div>
				<label for="acl-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">ACL</label>
				<select id="acl-name" bind:value={newClusterACLName} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					{#each acls as acl}
						<option value={acl.Name}>{acl.Name}</option>
					{/each}
					{#if acls.length === 0}
						<option value="open-access">open-access</option>
					{/if}
				</select>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={createCluster} disabled={creatingCluster} class="flex-1 py-2 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50">
					{creatingCluster ? 'Creating…' : 'Create Cluster'}
				</button>
				<button onclick={() => { showCreateCluster = false; }} class="flex-1 py-2 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">
					Cancel
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create User Modal -->
{#if showCreateUser}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" role="dialog" aria-modal="true" aria-labelledby="modal-create-user-title">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 id="modal-create-user-title" class="text-lg font-semibold text-gray-900 dark:text-white">Create User</h2>
			<div>
				<label for="user-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">User Name</label>
				<input id="user-name" bind:value={newUserName} type="text" placeholder="my-user" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="access-string" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Access String</label>
				<input id="access-string" bind:value={newUserAccessString} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={createUser} disabled={creatingUser} class="flex-1 py-2 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50">
					{creatingUser ? 'Creating…' : 'Create User'}
				</button>
				<button onclick={() => { showCreateUser = false; }} class="flex-1 py-2 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">
					Cancel
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create ACL Modal -->
{#if showCreateACL}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" role="dialog" aria-modal="true" aria-labelledby="modal-create-acl-title">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 id="modal-create-acl-title" class="text-lg font-semibold text-gray-900 dark:text-white">Create ACL</h2>
			<div>
				<label for="acl-name-new" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">ACL Name</label>
				<input id="acl-name-new" bind:value={newACLName} type="text" placeholder="my-acl" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={createACL} disabled={creatingACL} class="flex-1 py-2 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50">
					{creatingACL ? 'Creating…' : 'Create ACL'}
				</button>
				<button onclick={() => { showCreateACL = false; }} class="flex-1 py-2 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">
					Cancel
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Snapshot Modal -->
{#if showCreateSnapshot}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" role="dialog" aria-modal="true" aria-labelledby="modal-create-snapshot-title">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 id="modal-create-snapshot-title" class="text-lg font-semibold text-gray-900 dark:text-white">Create Snapshot</h2>
			<div>
				<label for="snapshot-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Snapshot Name</label>
				<input id="snapshot-name" bind:value={newSnapshotName} type="text" placeholder="my-snapshot" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="snapshot-cluster" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Cluster</label>
				<select id="snapshot-cluster" bind:value={newSnapshotCluster} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					{#each clusters as cluster}
						<option value={cluster.Name}>{cluster.Name}</option>
					{/each}
				</select>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={createSnapshot} disabled={creatingSnapshot} class="flex-1 py-2 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50">
					{creatingSnapshot ? 'Creating…' : 'Create Snapshot'}
				</button>
				<button onclick={() => { showCreateSnapshot = false; }} class="flex-1 py-2 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">
					Cancel
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Update User Modal -->
{#if showUpdateUser && updateUserTarget}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" role="dialog" aria-modal="true" aria-labelledby="modal-update-user-title">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 id="modal-update-user-title" class="text-lg font-semibold text-gray-900 dark:text-white">Update User: {updateUserTarget.Name}</h2>
			<div>
				<label for="update-access-string" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Access String</label>
				<input id="update-access-string" bind:value={updateUserAccessString} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={updateUser} disabled={updatingUser} class="flex-1 py-2 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50">
					{updatingUser ? 'Saving…' : 'Save Changes'}
				</button>
				<button onclick={() => { showUpdateUser = false; updateUserTarget = null; }} class="flex-1 py-2 border border-gray-200 dark:border-gray-700 rounded-lg text-sm text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">
					Cancel
				</button>
			</div>
		</div>
	</div>
{/if}
