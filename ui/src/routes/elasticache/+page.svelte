<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getElastiCacheClient } from '$lib/aws-client';
	import {
		DescribeCacheClustersCommand,
		CreateCacheClusterCommand,
		DeleteCacheClusterCommand,
		RebootCacheClusterCommand,
		DescribeReplicationGroupsCommand,
		CreateReplicationGroupCommand,
		DeleteReplicationGroupCommand,
		DescribeCacheParameterGroupsCommand,
		CreateCacheParameterGroupCommand,
		DeleteCacheParameterGroupCommand,
		DescribeCacheSubnetGroupsCommand,
		CreateCacheSubnetGroupCommand,
		DeleteCacheSubnetGroupCommand,
		DescribeSnapshotsCommand,
		CreateSnapshotCommand,
		DeleteSnapshotCommand,
		DescribeUsersCommand,
		CreateUserCommand,
		DeleteUserCommand,
		DescribeUserGroupsCommand,
		CreateUserGroupCommand,
		DeleteUserGroupCommand,
		DescribeEventsCommand,
		DescribeReservedCacheNodesCommand,
		DescribeServerlessCachesCommand,
		CreateServerlessCacheCommand,
		DeleteServerlessCacheCommand,
		type CacheCluster,
		type ReplicationGroup,
		type CacheParameterGroup,
		type CacheSubnetGroup,
		type Snapshot,
		type User,
		type UserGroup,
		type ServerlessCache,
		type ReservedCacheNode,
		type Event as ElastiCacheEvent
	} from '@aws-sdk/client-elasticache';
	import { toast } from 'svelte-sonner';
	import {
		Database,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Activity,
		Server,
		Clock,
		ChevronRight,
		Power,
		Copy,
		BookOpen,
		BarChart2,
		Layers,
		Settings,
		HardDrive,
		Users,
		Shield,
		Zap,
		Tag,
		X,
		UsersRound,
		Bell,
		Bookmark
	} from 'lucide-svelte';

	const ec = getElastiCacheClient();

	// ─── Top-level tab ────────────────────────────────────────────────────────
	type TopTab =
		| 'clusters'
		| 'replication-groups'
		| 'parameter-groups'
		| 'subnet-groups'
		| 'snapshots'
		| 'users'
		| 'user-groups'
		| 'serverless'
		| 'events'
		| 'reserved'
		| 'metrics'
		| 'docs';
	let activeTab = $state<TopTab>('clusters');

	// ─── Shared ───────────────────────────────────────────────────────────────
	let loading = $state(false);
	let searchQuery = $state('');
	let seedingDemo = $state(false);

	// ─── Clusters ─────────────────────────────────────────────────────────────
	let clusters = $state<CacheCluster[]>([]);
	let selectedCluster = $state<CacheCluster | null>(null);
	let showCreateClusterModal = $state(false);
	let newClusterId = $state('');
	let clusterEngine = $state('redis');
	let clusterNodeType = $state('cache.t3.micro');
	let clusterNumNodes = $state(1);
	let clusterParamGroup = $state('');
	let clusterMaintWindow = $state('sun:05:00-sun:06:00');
	let creatingCluster = $state(false);
	let clusterStatusFilter = $state<'all' | 'available' | 'creating' | 'deleting'>('all');

	// ─── Replication Groups ───────────────────────────────────────────────────
	let replicationGroups = $state<ReplicationGroup[]>([]);
	let showCreateRGModal = $state(false);
	let newRGId = $state('');
	let newRGDesc = $state('');
	let creatingRG = $state(false);

	// ─── Parameter Groups ─────────────────────────────────────────────────────
	let parameterGroups = $state<CacheParameterGroup[]>([]);
	let showCreatePGModal = $state(false);
	let newPGName = $state('');
	let newPGFamily = $state('redis7');
	let newPGDesc = $state('');
	let creatingPG = $state(false);

	// ─── Subnet Groups ────────────────────────────────────────────────────────
	let subnetGroups = $state<CacheSubnetGroup[]>([]);
	let showCreateSGModal = $state(false);
	let newSGName = $state('');
	let newSGDesc = $state('');
	let newSGSubnets = $state('subnet-12345678');
	let creatingSG = $state(false);

	// ─── Snapshots ────────────────────────────────────────────────────────────
	let snapshots = $state<Snapshot[]>([]);
	let showCreateSnapModal = $state(false);
	let newSnapName = $state('');
	let newSnapClusterId = $state('');
	let creatingSnap = $state(false);

	// ─── Users ────────────────────────────────────────────────────────────────
	let users = $state<User[]>([]);
	let showCreateUserModal = $state(false);
	let newUserId = $state('');
	let newUserName = $state('');
	let newUserAccess = $state('on ~* +@all');
	let newUserNoPassword = $state(true);
	let creatingUser = $state(false);

	// ─── User Groups ──────────────────────────────────────────────────────────
	let userGroups = $state<UserGroup[]>([]);
	let showCreateUGModal = $state(false);
	let newUGId = $state('');
	let newUGDesc = $state('');
	let creatingUG = $state(false);

	// ─── Events ───────────────────────────────────────────────────────────────
	let events = $state<ElastiCacheEvent[]>([]);
	let eventSourceType = $state('');

	// ─── Reserved Cache Nodes ─────────────────────────────────────────────────
	let reservedCacheNodes = $state<ReservedCacheNode[]>([]);

	// ─── Serverless ───────────────────────────────────────────────────────────
	let serverlessCaches = $state<ServerlessCache[]>([]);
	let showCreateSLModal = $state(false);
	let newSLName = $state('');
	let newSLEngine = $state('redis');
	let creatingSL = $state(false);

	// ─── Derived ──────────────────────────────────────────────────────────────
	const filteredClusters = $derived(
		clusters.filter((c) => {
			const nameMatch = (c.CacheClusterId ?? '')
				.toLowerCase()
				.includes(searchQuery.toLowerCase());
			const statusMatch =
				clusterStatusFilter === 'all' ||
				(c.CacheClusterStatus ?? '') === clusterStatusFilter;
			return nameMatch && statusMatch;
		})
	);
	const filteredRGs = $derived(
		replicationGroups.filter((rg) =>
			(rg.ReplicationGroupId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredPGs = $derived(
		parameterGroups.filter((pg) =>
			(pg.CacheParameterGroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredSGs = $derived(
		subnetGroups.filter((sg) =>
			(sg.CacheSubnetGroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredSnapshots = $derived(
		snapshots.filter((s) =>
			(s.SnapshotName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredUsers = $derived(
		users.filter(
			(u) =>
				(u.UserId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(u.UserName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredSL = $derived(
		serverlessCaches.filter((s) =>
			(s.ServerlessCacheName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredUserGroups = $derived(
		userGroups.filter((ug) =>
			(ug.UserGroupId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredEvents = $derived(
		events.filter(
			(e) =>
				(eventSourceType === '' || e.SourceType === eventSourceType) &&
				((e.SourceIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
					(e.Message ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
		)
	);
	const filteredReserved = $derived(
		reservedCacheNodes.filter(
			(r) =>
				(r.ReservedCacheNodeId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(r.CacheNodeType ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	// ─── Status colour helper ─────────────────────────────────────────────────
	function getStatusColor(status: string | undefined): string {
		if (!status) return 'bg-slate-500';
		const s = status.toLowerCase();
		if (s === 'available' || s === 'active') return 'bg-emerald-500';
		if (s === 'creating' || s === 'modifying' || s === 'rebooting') return 'bg-amber-500';
		if (s === 'deleting' || s === 'deleted') return 'bg-rose-500';
		return 'bg-slate-500';
	}

	function getStatusBadge(status: string | undefined): string {
		if (!status) return 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
		const s = status.toLowerCase();
		if (s === 'available' || s === 'active')
			return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-400';
		if (s === 'creating' || s === 'modifying')
			return 'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-400';
		if (s === 'deleting' || s === 'deleted')
			return 'bg-rose-100 text-rose-700 dark:bg-rose-900/40 dark:text-rose-400';
		return 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
	}

	async function copyToClipboard(text: string) {
		await navigator.clipboard
			.writeText(text)
			.then(() => toast.success('Copied to clipboard'))
			.catch(() => toast.error('Failed to copy to clipboard'));
	}

	// ─── Cluster actions ──────────────────────────────────────────────────────
	async function loadClusters() {
		loading = true;
		try {
			const res = await ec.send(new DescribeCacheClustersCommand({ ShowCacheNodeInfo: true }));
			clusters = res.CacheClusters ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load clusters: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createCluster() {
		if (!newClusterId.trim()) return;
		creatingCluster = true;
		try {
			await ec.send(
				new CreateCacheClusterCommand({
					CacheClusterId: newClusterId.trim(),
					Engine: clusterEngine,
					CacheNodeType: clusterNodeType,
					NumCacheNodes: clusterNumNodes,
					CacheParameterGroupName: clusterParamGroup || undefined,
					PreferredMaintenanceWindow: clusterMaintWindow || undefined
				})
			);
			toast.success(`Cluster "${newClusterId}" creation initiated`);
			showCreateClusterModal = false;
			resetClusterForm();
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creatingCluster = false;
		}
	}

	function resetClusterForm() {
		newClusterId = '';
		clusterEngine = 'redis';
		clusterNodeType = 'cache.t3.micro';
		clusterNumNodes = 1;
		clusterParamGroup = '';
		clusterMaintWindow = 'sun:05:00-sun:06:00';
	}

	async function deleteCluster(id: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete Cluster',
				message: `Delete ElastiCache cluster "${id}"? All cached data will be lost.`
			}))
		)
			return;
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
		if (
			!(await confirmDestructive({
				title: 'Reboot Cluster',
				message: `Reboot cluster "${id}"? All connections will be dropped.`,
				confirmLabel: 'Reboot',
				dangerous: false
			}))
		)
			return;
		try {
			await ec.send(
				new RebootCacheClusterCommand({ CacheClusterId: id, CacheNodeIdsToReboot: ['0001'] })
			);
			toast.success(`Cluster "${id}" reboot initiated`);
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Reboot failed: ${(err as Error).message}`);
		}
	}

	// ─── Replication Group actions ────────────────────────────────────────────
	async function loadReplicationGroups() {
		loading = true;
		try {
			const res = await ec.send(new DescribeReplicationGroupsCommand({}));
			replicationGroups = res.ReplicationGroups ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load replication groups: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createReplicationGroup() {
		if (!newRGId.trim()) return;
		creatingRG = true;
		try {
			await ec.send(
				new CreateReplicationGroupCommand({
					ReplicationGroupId: newRGId.trim(),
					ReplicationGroupDescription: newRGDesc || newRGId.trim()
				})
			);
			toast.success(`Replication group "${newRGId}" created`);
			showCreateRGModal = false;
			newRGId = '';
			newRGDesc = '';
			await loadReplicationGroups();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creatingRG = false;
		}
	}

	async function deleteReplicationGroup(id: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete Replication Group',
				message: `Delete replication group "${id}"?`
			}))
		)
			return;
		try {
			await ec.send(new DeleteReplicationGroupCommand({ ReplicationGroupId: id }));
			toast.success(`Replication group "${id}" deletion initiated`);
			await loadReplicationGroups();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ─── Parameter Group actions ──────────────────────────────────────────────
	async function loadParameterGroups() {
		loading = true;
		try {
			const res = await ec.send(new DescribeCacheParameterGroupsCommand({}));
			parameterGroups = res.CacheParameterGroups ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load parameter groups: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createParameterGroup() {
		if (!newPGName.trim()) return;
		creatingPG = true;
		try {
			await ec.send(
				new CreateCacheParameterGroupCommand({
					CacheParameterGroupName: newPGName.trim(),
					CacheParameterGroupFamily: newPGFamily,
					Description: newPGDesc || newPGName.trim()
				})
			);
			toast.success(`Parameter group "${newPGName}" created`);
			showCreatePGModal = false;
			newPGName = '';
			newPGDesc = '';
			await loadParameterGroups();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creatingPG = false;
		}
	}

	async function deleteParameterGroup(name: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete Parameter Group',
				message: `Delete parameter group "${name}"?`
			}))
		)
			return;
		try {
			await ec.send(new DeleteCacheParameterGroupCommand({ CacheParameterGroupName: name }));
			toast.success(`Parameter group "${name}" deleted`);
			await loadParameterGroups();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ─── Subnet Group actions ─────────────────────────────────────────────────
	async function loadSubnetGroups() {
		loading = true;
		try {
			const res = await ec.send(new DescribeCacheSubnetGroupsCommand({}));
			subnetGroups = res.CacheSubnetGroups ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load subnet groups: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createSubnetGroup() {
		if (!newSGName.trim()) return;
		creatingSG = true;
		try {
			const subnetIds = newSGSubnets
				.split(',')
				.map((s) => s.trim())
				.filter(Boolean);
			await ec.send(
				new CreateCacheSubnetGroupCommand({
					CacheSubnetGroupName: newSGName.trim(),
					CacheSubnetGroupDescription: newSGDesc || newSGName.trim(),
					SubnetIds: subnetIds
				})
			);
			toast.success(`Subnet group "${newSGName}" created`);
			showCreateSGModal = false;
			newSGName = '';
			newSGDesc = '';
			newSGSubnets = 'subnet-12345678';
			await loadSubnetGroups();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creatingSG = false;
		}
	}

	async function deleteSubnetGroup(name: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete Subnet Group',
				message: `Delete subnet group "${name}"?`
			}))
		)
			return;
		try {
			await ec.send(new DeleteCacheSubnetGroupCommand({ CacheSubnetGroupName: name }));
			toast.success(`Subnet group "${name}" deleted`);
			await loadSubnetGroups();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ─── Snapshot actions ─────────────────────────────────────────────────────
	async function loadSnapshots() {
		loading = true;
		try {
			const res = await ec.send(new DescribeSnapshotsCommand({}));
			snapshots = res.Snapshots ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load snapshots: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createSnapshot() {
		if (!newSnapName.trim() || !newSnapClusterId.trim()) return;
		creatingSnap = true;
		try {
			await ec.send(
				new CreateSnapshotCommand({
					SnapshotName: newSnapName.trim(),
					CacheClusterId: newSnapClusterId.trim()
				})
			);
			toast.success(`Snapshot "${newSnapName}" creation initiated`);
			showCreateSnapModal = false;
			newSnapName = '';
			newSnapClusterId = '';
			await loadSnapshots();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creatingSnap = false;
		}
	}

	async function deleteSnapshot(name: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete Snapshot',
				message: `Delete snapshot "${name}"?`
			}))
		)
			return;
		try {
			await ec.send(new DeleteSnapshotCommand({ SnapshotName: name }));
			toast.success(`Snapshot "${name}" deleted`);
			await loadSnapshots();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ─── User actions ─────────────────────────────────────────────────────────
	async function loadUsers() {
		loading = true;
		try {
			const res = await ec.send(new DescribeUsersCommand({}));
			users = res.Users ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load users: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createUser() {
		if (!newUserId.trim() || !newUserName.trim()) return;
		creatingUser = true;
		try {
			await ec.send(
				new CreateUserCommand({
					UserId: newUserId.trim(),
					UserName: newUserName.trim(),
					Engine: 'Redis',
					AccessString: newUserAccess,
					NoPasswordRequired: newUserNoPassword
				})
			);
			toast.success(`User "${newUserId}" created`);
			showCreateUserModal = false;
			newUserId = '';
			newUserName = '';
			newUserAccess = 'on ~* +@all';
			newUserNoPassword = true;
			await loadUsers();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creatingUser = false;
		}
	}

	async function deleteUser(id: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete User',
				message: `Delete user "${id}"?`
			}))
		)
			return;
		try {
			await ec.send(new DeleteUserCommand({ UserId: id }));
			toast.success(`User "${id}" deleted`);
			await loadUsers();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ─── Serverless Cache actions ─────────────────────────────────────────────
	async function loadServerlessCaches() {		loading = true;
		try {
			const res = await ec.send(new DescribeServerlessCachesCommand({}));
			serverlessCaches = res.ServerlessCaches ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load serverless caches: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createServerlessCache() {
		if (!newSLName.trim()) return;
		creatingSL = true;
		try {
			await ec.send(
				new CreateServerlessCacheCommand({
					ServerlessCacheName: newSLName.trim(),
					Engine: newSLEngine
				})
			);
			toast.success(`Serverless cache "${newSLName}" created`);
			showCreateSLModal = false;
			newSLName = '';
			newSLEngine = 'redis';
			await loadServerlessCaches();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creatingSL = false;
		}
	}

	async function deleteServerlessCache(name: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete Serverless Cache',
				message: `Delete serverless cache "${name}"?`
			}))
		)
			return;
		try {
			await ec.send(new DeleteServerlessCacheCommand({ ServerlessCacheName: name }));
			toast.success(`Serverless cache "${name}" deleted`);
			await loadServerlessCaches();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ─── User Group actions ───────────────────────────────────────────────────
	async function loadUserGroups() {
		loading = true;
		try {
			const res = await ec.send(new DescribeUserGroupsCommand({}));
			userGroups = res.UserGroups ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load user groups: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createUserGroup() {
		if (!newUGId.trim()) return;
		creatingUG = true;
		try {
			await ec.send(
				new CreateUserGroupCommand({
					UserGroupId: newUGId.trim(),
					Engine: 'Redis'
				})
			);
			toast.success(`User group "${newUGId}" created`);
			showCreateUGModal = false;
			newUGId = '';
			newUGDesc = '';
			await loadUserGroups();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creatingUG = false;
		}
	}

	async function deleteUserGroup(id: string) {
		if (
			!(await confirmDestructive({
				title: 'Delete User Group',
				message: `Delete user group "${id}"?`
			}))
		)
			return;
		try {
			await ec.send(new DeleteUserGroupCommand({ UserGroupId: id }));
			toast.success(`User group "${id}" deleted`);
			await loadUserGroups();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ─── Events actions ───────────────────────────────────────────────────────
	async function loadEvents() {
		loading = true;
		try {
			const res = await ec.send(new DescribeEventsCommand({}));
			events = res.Events ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load events: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	// ─── Reserved Cache Nodes actions ─────────────────────────────────────────
	async function loadReservedCacheNodes() {
		loading = true;
		try {
			const res = await ec.send(new DescribeReservedCacheNodesCommand({}));
			reservedCacheNodes = res.ReservedCacheNodes ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load reserved cache nodes: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	// ─── Tab change handler ───────────────────────────────────────────────────
	async function onTabChange(tab: TopTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'clusters') await loadClusters();
		else if (tab === 'replication-groups') await loadReplicationGroups();
		else if (tab === 'parameter-groups') await loadParameterGroups();
		else if (tab === 'subnet-groups') await loadSubnetGroups();
		else if (tab === 'snapshots') await loadSnapshots();
		else if (tab === 'users') await loadUsers();
		else if (tab === 'user-groups') await loadUserGroups();
		else if (tab === 'serverless') await loadServerlessCaches();
		else if (tab === 'events') await loadEvents();
		else if (tab === 'reserved') await loadReservedCacheNodes();
		else if (tab === 'metrics') await loadAllMetrics();
	}

	// ─── Metrics ─────────────────────────────────────────────────────────────
	async function loadAllMetrics() {
		loading = true;
		try {
			await Promise.all([
				loadClusters(),
				loadReplicationGroups(),
				loadParameterGroups(),
				loadSubnetGroups(),
				loadSnapshots(),
				loadUsers(),
				loadUserGroups(),
				loadServerlessCaches(),
				loadReservedCacheNodes()
			]);
		} finally {
			loading = false;
		}
	}

	// ─── Demo data ────────────────────────────────────────────────────────────
	async function seedDemoData() {
		seedingDemo = true;
		try {
			await Promise.allSettled([
				ec.send(
					new CreateCacheClusterCommand({
						CacheClusterId: 'demo-session-cache',
						Engine: 'redis',
						CacheNodeType: 'cache.t3.micro',
						NumCacheNodes: 1
					})
				),
				ec.send(
					new CreateCacheClusterCommand({
						CacheClusterId: 'demo-api-cache',
						Engine: 'memcached',
						CacheNodeType: 'cache.t3.small',
						NumCacheNodes: 3
					})
				),
				ec.send(
					new CreateCacheClusterCommand({
						CacheClusterId: 'demo-rate-limiter',
						Engine: 'redis',
						CacheNodeType: 'cache.m5.large',
						NumCacheNodes: 1
					})
				),
				ec.send(
					new CreateReplicationGroupCommand({
						ReplicationGroupId: 'demo-primary-rg',
						ReplicationGroupDescription: 'Demo primary replication group'
					})
				),
				ec.send(
					new CreateReplicationGroupCommand({
						ReplicationGroupId: 'demo-readonly-rg',
						ReplicationGroupDescription: 'Demo read-only replication group'
					})
				),
				ec.send(
					new CreateCacheParameterGroupCommand({
						CacheParameterGroupName: 'demo-redis-params',
						CacheParameterGroupFamily: 'redis7',
						Description: 'Demo Redis 7 parameter group'
					})
				),
				ec.send(
					new CreateCacheParameterGroupCommand({
						CacheParameterGroupName: 'demo-memcached-params',
						CacheParameterGroupFamily: 'memcached1.6',
						Description: 'Demo Memcached 1.6 parameter group'
					})
				),
				ec.send(
					new CreateCacheSubnetGroupCommand({
						CacheSubnetGroupName: 'demo-subnet-group',
						CacheSubnetGroupDescription: 'Demo VPC subnet group',
						SubnetIds: ['subnet-demo1234']
					})
				),
				ec.send(
					new CreateUserCommand({
						UserId: 'demo-admin',
						UserName: 'demo-admin',
						Engine: 'Redis',
						AccessString: 'on ~* +@all',
						NoPasswordRequired: true
					})
				),
				ec.send(
					new CreateUserCommand({
						UserId: 'demo-readonly',
						UserName: 'demo-readonly',
						Engine: 'Redis',
						AccessString: 'on ~* -@write',
						NoPasswordRequired: false
					})
				),
				ec.send(
					new CreateUserGroupCommand({
						UserGroupId: 'demo-admin-group',
						Engine: 'Redis'
					})
				),
				ec.send(
					new CreateServerlessCacheCommand({
						ServerlessCacheName: 'demo-serverless',
						Engine: 'redis'
					})
				)
			]);
			toast.success('Demo data seeded successfully');
			await onTabChange(activeTab);
		} catch (err: unknown) {
			toast.error(`Seed failed: ${(err as Error).message}`);
		} finally {
			seedingDemo = false;
		}
	}

	onMount(() => {
		loadClusters();
	});

	// ─── Tab button classes ───────────────────────────────────────────────────
	function tabClass(tab: TopTab): string {
		return `flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${activeTab === tab ? 'border-cyan-500 text-cyan-600 dark:text-cyan-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}`;
	}
</script>

<div class="space-y-4">
	<!-- ── Header ──────────────────────────────────────────────────────────── -->
	<div
		class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl"
	>
		<div class="flex items-center gap-4">
			<div class="p-3 bg-cyan-500/20 rounded-xl">
				<Database class="w-8 h-8 text-cyan-500" />
			</div>
			<div>
				<h1
					class="text-3xl font-bold bg-gradient-to-r from-cyan-600 to-blue-600 dark:from-cyan-400 dark:to-blue-400 bg-clip-text text-transparent"
				>
					ElastiCache
				</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">
					Managed in-memory data stores — Redis and Memcached
				</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button
				onclick={() => void seedDemoData()}
				disabled={seedingDemo}
				class="flex items-center gap-2 px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-xl text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700 transition-all text-sm disabled:opacity-50"
				title="Seed demo data"
			>
				<Zap class="w-4 h-4" />
				{seedingDemo ? 'Seeding…' : 'Seed Demo'}
			</button>
			<button
				onclick={() => void onTabChange(activeTab)}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95"
				title="Refresh"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
		</div>
	</div>

	<!-- ── Tab bar ─────────────────────────────────────────────────────────── -->
	<div
		class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden"
	>
		<div class="flex overflow-x-auto border-b border-slate-200 dark:border-slate-700/50 px-4">
			<button onclick={() => void onTabChange('clusters')} class={tabClass('clusters')}>
				<Database class="w-4 h-4" /> Clusters
			</button>
			<button
				onclick={() => void onTabChange('replication-groups')}
				class={tabClass('replication-groups')}
			>
				<Layers class="w-4 h-4" /> Replication Groups
			</button>
			<button
				onclick={() => void onTabChange('parameter-groups')}
				class={tabClass('parameter-groups')}
			>
				<Settings class="w-4 h-4" /> Parameter Groups
			</button>
			<button onclick={() => void onTabChange('subnet-groups')} class={tabClass('subnet-groups')}>
				<Server class="w-4 h-4" /> Subnet Groups
			</button>
			<button onclick={() => void onTabChange('snapshots')} class={tabClass('snapshots')}>
				<HardDrive class="w-4 h-4" /> Snapshots
			</button>
			<button onclick={() => void onTabChange('users')} class={tabClass('users')}>
				<Users class="w-4 h-4" /> Users
			</button>
			<button onclick={() => void onTabChange('user-groups')} class={tabClass('user-groups')}>
				<UsersRound class="w-4 h-4" /> User Groups
			</button>
			<button onclick={() => void onTabChange('serverless')} class={tabClass('serverless')}>
				<Shield class="w-4 h-4" /> Serverless
			</button>
			<button onclick={() => void onTabChange('events')} class={tabClass('events')}>
				<Bell class="w-4 h-4" /> Events
			</button>
			<button onclick={() => void onTabChange('reserved')} class={tabClass('reserved')}>
				<Bookmark class="w-4 h-4" /> Reserved
			</button>
			<button onclick={() => void onTabChange('metrics')} class={tabClass('metrics')}>
				<BarChart2 class="w-4 h-4" /> Metrics
			</button>
			<button onclick={() => void onTabChange('docs')} class={tabClass('docs')}>
				<BookOpen class="w-4 h-4" /> Docs
			</button>
		</div>

		<!-- Search bar (hidden on metrics/docs) -->
		{#if activeTab !== 'metrics' && activeTab !== 'docs'}
			<div class="p-4 border-b border-slate-200 dark:border-slate-700/50 flex items-center gap-3">
				<div class="relative flex-1">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
					<input
						type="text"
						bind:value={searchQuery}
						placeholder="Search…"
						class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-cyan-500 outline-none transition-all"
					/>
				</div>
				{#if activeTab === 'clusters'}
					<select
						bind:value={clusterStatusFilter}
						class="px-3 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-cyan-500 outline-none"
					>
						<option value="all">All Statuses</option>
						<option value="available">Available</option>
						<option value="creating">Creating</option>
						<option value="deleting">Deleting</option>
					</select>
					<button
						onclick={() => (showCreateClusterModal = true)}
						class="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded-xl font-medium text-sm shadow-lg shadow-cyan-600/20 transition-all active:scale-95 whitespace-nowrap"
					>
						<Plus class="w-4 h-4" />
						Create Cluster
					</button>
				{:else if activeTab === 'replication-groups'}
					<button
						onclick={() => (showCreateRGModal = true)}
						class="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded-xl font-medium text-sm shadow-lg shadow-cyan-600/20 transition-all active:scale-95 whitespace-nowrap"
					>
						<Plus class="w-4 h-4" />
						Create Group
					</button>
				{:else if activeTab === 'parameter-groups'}
					<button
						onclick={() => (showCreatePGModal = true)}
						class="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded-xl font-medium text-sm shadow-lg shadow-cyan-600/20 transition-all active:scale-95 whitespace-nowrap"
					>
						<Plus class="w-4 h-4" />
						Create Group
					</button>
				{:else if activeTab === 'subnet-groups'}
					<button
						onclick={() => (showCreateSGModal = true)}
						class="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded-xl font-medium text-sm shadow-lg shadow-cyan-600/20 transition-all active:scale-95 whitespace-nowrap"
					>
						<Plus class="w-4 h-4" />
						Create Group
					</button>
				{:else if activeTab === 'snapshots'}
					<button
						onclick={() => (showCreateSnapModal = true)}
						class="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded-xl font-medium text-sm shadow-lg shadow-cyan-600/20 transition-all active:scale-95 whitespace-nowrap"
					>
						<Plus class="w-4 h-4" />
						Create Snapshot
					</button>
				{:else if activeTab === 'users'}
					<button
						onclick={() => (showCreateUserModal = true)}
						class="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded-xl font-medium text-sm shadow-lg shadow-cyan-600/20 transition-all active:scale-95 whitespace-nowrap"
					>
						<Plus class="w-4 h-4" />
						Create User
					</button>
				{:else if activeTab === 'user-groups'}
					<button
						onclick={() => (showCreateUGModal = true)}
						class="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded-xl font-medium text-sm shadow-lg shadow-cyan-600/20 transition-all active:scale-95 whitespace-nowrap"
					>
						<Plus class="w-4 h-4" />
						Create Group
					</button>
				{:else if activeTab === 'events'}
					<select
						bind:value={eventSourceType}
						class="px-3 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-cyan-500 outline-none"
					>
						<option value="">All Source Types</option>
						<option value="cache-cluster">cache-cluster</option>
						<option value="replication-group">replication-group</option>
						<option value="snapshot">snapshot</option>
						<option value="cache-parameter-group">cache-parameter-group</option>
						<option value="user">user</option>
						<option value="user-group">user-group</option>
					</select>
				{:else if activeTab === 'serverless'}
					<button
						onclick={() => (showCreateSLModal = true)}
						class="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded-xl font-medium text-sm shadow-lg shadow-cyan-600/20 transition-all active:scale-95 whitespace-nowrap"
					>
						<Plus class="w-4 h-4" />
						Create Cache
					</button>
				{/if}
			</div>
		{/if}

		<!-- ── CLUSTERS TAB ─────────────────────────────────────────────────── -->
		{#if activeTab === 'clusters'}
			<div class="grid grid-cols-1 lg:grid-cols-12 gap-0">
				<!-- List -->
				<div class="lg:col-span-8 overflow-x-auto border-r border-slate-100 dark:border-slate-700/50">
					<table class="w-full text-left border-collapse">
						<thead>
							<tr class="bg-slate-50/50 dark:bg-slate-900/20">
								<th
									class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
									>Cluster</th
								>
								<th
									class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
									>Engine</th
								>
								<th
									class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
									>Status</th
								>
								<th
									class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
									>Nodes</th
								>
								<th
									class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
									>Actions</th
								>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
							{#if loading && !clusters.length}
								{#each Array(3) as _}
									<tr class="animate-pulse">
										<td colspan="5" class="px-6 py-8"
											><div
												class="h-10 bg-slate-200/50 dark:bg-slate-700/30 rounded-xl w-full"
											></div></td
										>
									</tr>
								{/each}
							{:else}
								{#each filteredClusters as cluster}
									<tr
										class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all cursor-pointer {selectedCluster?.CacheClusterId === cluster.CacheClusterId ? 'bg-cyan-500/5 dark:bg-cyan-500/10' : ''}"
										onclick={() => (selectedCluster = cluster)}
									>
										<td class="px-6 py-4">
											<div class="flex items-center gap-3">
												<div class="p-2 bg-cyan-500/10 rounded-lg">
													<Database class="w-4 h-4 text-cyan-600 dark:text-cyan-400" />
												</div>
												<div>
													<div class="font-bold text-slate-900 dark:text-white">
														{cluster.CacheClusterId}
													</div>
													<div class="text-[10px] text-slate-400 font-mono">
														{cluster.CacheNodeType} · v{cluster.EngineVersion}
													</div>
												</div>
											</div>
										</td>
										<td class="px-6 py-4 text-center">
											<span
												class="px-2 py-0.5 rounded text-[10px] font-black uppercase {cluster.Engine === 'redis' ? 'bg-red-500/10 text-red-600 dark:text-red-400' : 'bg-blue-500/10 text-blue-600 dark:text-blue-400'}"
											>
												{cluster.Engine}
											</span>
										</td>
										<td class="px-6 py-4 text-center">
											<span
												class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium {getStatusBadge(cluster.CacheClusterStatus)}"
											>
												<span
													class="w-1.5 h-1.5 rounded-full {getStatusColor(cluster.CacheClusterStatus)}"
												></span>
												{cluster.CacheClusterStatus}
											</span>
										</td>
										<td class="px-6 py-4 text-center text-sm font-bold text-slate-700 dark:text-slate-200">
											{cluster.NumCacheNodes}
										</td>
										<td class="px-6 py-4 text-right">
											<div class="flex items-center justify-end gap-2">
												<button
													onclick={(e) => {
														e.stopPropagation();
														rebootCluster(cluster.CacheClusterId!);
													}}
													class="p-2 text-slate-400 hover:text-amber-500 rounded-lg transition-colors"
													title="Reboot"
												>
													<Power class="w-4 h-4" />
												</button>
												<button
													onclick={(e) => {
														e.stopPropagation();
														deleteCluster(cluster.CacheClusterId!);
													}}
													class="p-2 text-slate-400 hover:text-red-500 rounded-lg transition-colors"
													title="Delete"
												>
													<Trash2 class="w-4 h-4" />
												</button>
												<ChevronRight class="w-4 h-4 text-slate-300" />
											</div>
										</td>
									</tr>
								{/each}
								{#if !filteredClusters.length}
									<tr>
										<td colspan="5" class="px-6 py-16 text-center">
											<div class="flex flex-col items-center gap-3">
												<Database class="w-10 h-10 text-slate-300 dark:text-slate-700" />
												<p class="text-slate-500 dark:text-slate-400">No clusters found</p>
											</div>
										</td>
									</tr>
								{/if}
							{/if}
						</tbody>
					</table>
				</div>

				<!-- Details Panel -->
				<div class="lg:col-span-4 p-6">
					{#if selectedCluster}
						<div class="space-y-5">
							<div class="flex items-center justify-between">
								<h2 class="text-lg font-bold text-slate-900 dark:text-white">
									{selectedCluster.CacheClusterId}
								</h2>
								<span
									class="text-xs px-2 py-0.5 rounded-full font-medium {getStatusBadge(selectedCluster.CacheClusterStatus)}"
									>{selectedCluster.CacheClusterStatus}</span
								>
							</div>

							<!-- Endpoint -->
							<div class="p-3 bg-slate-900 dark:bg-black rounded-xl border border-slate-800">
								<div
									class="flex items-center justify-between text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-2"
								>
									<span class="flex items-center gap-1">
										<Activity class="w-3 h-3 text-cyan-500" />
										Endpoint
									</span>
									<button
										onclick={() =>
											copyToClipboard(
												`${selectedCluster?.ConfigurationEndpoint?.Address ?? selectedCluster?.CacheNodes?.[0]?.Endpoint?.Address ?? 'pending'}:${selectedCluster?.ConfigurationEndpoint?.Port ?? (selectedCluster?.Engine === 'redis' ? 6379 : 11211)}`
											)}
										class="hover:text-slate-300 transition-colors"
										title="Copy endpoint"
									>
										<Copy class="w-3 h-3" />
									</button>
								</div>
								<div class="font-mono text-sm text-cyan-400 break-all">
									{selectedCluster.ConfigurationEndpoint?.Address ??
										selectedCluster.CacheNodes?.[0]?.Endpoint?.Address ??
										'pending-endpoint.local'}:{selectedCluster.ConfigurationEndpoint?.Port ??
										(selectedCluster.Engine === 'redis' ? 6379 : 11211)}
								</div>
							</div>

							<!-- Stats grid -->
							<div class="grid grid-cols-2 gap-3">
								<div
									class="p-3 bg-white/50 dark:bg-slate-900/40 rounded-xl border border-slate-200 dark:border-slate-700/50"
								>
									<div
										class="flex items-center gap-1 text-slate-500 dark:text-slate-400 text-[10px] uppercase font-bold tracking-wider mb-1"
									>
										<Server class="w-3 h-3 text-blue-500" /> Nodes
									</div>
									<div class="text-lg font-black text-slate-900 dark:text-white">
										{selectedCluster.NumCacheNodes}
									</div>
								</div>
								<div
									class="p-3 bg-white/50 dark:bg-slate-900/40 rounded-xl border border-slate-200 dark:border-slate-700/50"
								>
									<div
										class="flex items-center gap-1 text-slate-500 dark:text-slate-400 text-[10px] uppercase font-bold tracking-wider mb-1"
									>
										<Clock class="w-3 h-3 text-indigo-500" /> Created
									</div>
									<div class="text-xs font-bold text-slate-900 dark:text-white">
										{selectedCluster.CacheClusterCreateTime?.toLocaleDateString() ?? '—'}
									</div>
								</div>
							</div>

							<!-- Config details -->
							<div class="space-y-2">
								{#each [
									['Node Type', selectedCluster.CacheNodeType],
									['Engine', `${selectedCluster.Engine} ${selectedCluster.EngineVersion}`],
									['Param Group', selectedCluster.CacheParameterGroup?.CacheParameterGroupName ?? '—'],
									['AZ', selectedCluster.PreferredAvailabilityZone ?? '—'],
									['Maintenance', selectedCluster.PreferredMaintenanceWindow ?? '—'],
									...(selectedCluster.ReplicationGroupId ? [['Replication Group', selectedCluster.ReplicationGroupId]] : []),
									['Transit Encrypt', selectedCluster.TransitEncryptionEnabled ? 'Yes' : 'No'],
									['At-Rest Encrypt', selectedCluster.AtRestEncryptionEnabled ? 'Yes' : 'No']
								] as [label, value]}
									<div
										class="flex justify-between items-center text-sm p-2.5 bg-slate-50/50 dark:bg-slate-900/40 rounded-lg"
									>
										<span class="text-slate-500 dark:text-slate-400 text-xs">{label}</span>
										<span class="font-bold text-slate-700 dark:text-slate-200 text-xs">{value}</span>
									</div>
								{/each}
							</div>
							<!-- Cache Nodes -->
							{#if selectedCluster.CacheNodes && selectedCluster.CacheNodes.length > 0}
								<div class="space-y-1">
									<div class="text-[10px] font-bold text-slate-500 dark:text-slate-400 uppercase tracking-wider mb-2 flex items-center gap-1">
										<Server class="w-3 h-3 text-blue-500" /> Cache Nodes
									</div>
									{#each selectedCluster.CacheNodes as node}
										<div class="p-2.5 bg-slate-50/80 dark:bg-slate-900/40 rounded-lg border border-slate-100 dark:border-slate-700/50 space-y-1">
											<div class="flex items-center justify-between">
												<span class="text-xs font-bold text-slate-700 dark:text-slate-200">{node.CacheNodeId ?? '—'}</span>
												<span class="text-[10px] px-1.5 py-0.5 rounded font-medium {getStatusBadge(node.CacheNodeStatus)}">{node.CacheNodeStatus ?? '—'}</span>
											</div>
											{#if node.Endpoint?.Address}
												<div class="flex items-center justify-between">
													<span class="font-mono text-[10px] text-cyan-600 dark:text-cyan-400 truncate">{node.Endpoint.Address}:{node.Endpoint.Port}</span>
													<button onclick={() => copyToClipboard(`${node.Endpoint?.Address}:${node.Endpoint?.Port}`)} class="ml-1 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 flex-shrink-0" title="Copy">
														<Copy class="w-3 h-3" />
													</button>
												</div>
											{/if}
											{#if node.PreferredAvailabilityZone}
												<div class="text-[10px] text-slate-400">{node.PreferredAvailabilityZone}</div>
											{/if}
										</div>
									{/each}
								</div>
							{/if}

							<!-- ARN -->
							<div class="p-2.5 bg-slate-50 dark:bg-slate-900/40 rounded-lg">
								<div class="flex items-center justify-between mb-1">
									<span class="text-[10px] text-slate-400 uppercase font-bold">ARN</span>
									<button
										onclick={() => copyToClipboard(selectedCluster?.ARN ?? '')}
										class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
									>
										<Copy class="w-3 h-3" />
									</button>
								</div>
								<div class="font-mono text-[10px] text-slate-500 dark:text-slate-400 break-all">
									{selectedCluster.ARN ?? '—'}
								</div>
							</div>

							<!-- Actions -->
							<div class="flex flex-col gap-2 pt-2 border-t border-slate-100 dark:border-slate-700/50">
								<button
									onclick={() => selectedCluster && rebootCluster(selectedCluster.CacheClusterId!)}
									class="w-full flex items-center justify-center gap-2 py-2.5 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 text-slate-700 dark:text-slate-200 rounded-xl font-bold text-sm hover:bg-slate-50 dark:hover:bg-slate-600 transition-all active:scale-[0.98]"
								>
									<RefreshCw class="w-4 h-4" /> Reboot Nodes
								</button>
								<button
									onclick={() => selectedCluster && deleteCluster(selectedCluster.CacheClusterId!)}
									class="w-full flex items-center justify-center gap-2 py-2.5 bg-rose-600 text-white rounded-xl font-bold text-sm hover:bg-rose-700 transition-all shadow-lg shadow-rose-600/20 active:scale-[0.98]"
								>
									<Trash2 class="w-4 h-4" /> Delete Cluster
								</button>
							</div>
						</div>
					{:else}
						<div class="flex flex-col items-center gap-4 py-12 text-center">
							<div class="p-4 bg-slate-50 dark:bg-slate-800 rounded-2xl">
								<Database class="w-10 h-10 text-slate-300 dark:text-slate-600" />
							</div>
							<p class="text-slate-500 dark:text-slate-400 text-sm">
								Select a cluster to view details
							</p>
						</div>
					{/if}
				</div>
			</div>
		{/if}

		<!-- ── REPLICATION GROUPS TAB ──────────────────────────────────────── -->
		{#if activeTab === 'replication-groups'}
			<div class="overflow-x-auto">
				<table class="w-full text-left border-collapse">
					<thead>
						<tr class="bg-slate-50/50 dark:bg-slate-900/20">
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>ID</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Description</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Status</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Auto Failover</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
								>Actions</th
							>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
						{#each filteredRGs as rg}
							<tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all">
								<td class="px-6 py-4">
									<div class="flex items-center gap-3">
										<div class="p-2 bg-indigo-500/10 rounded-lg">
											<Layers class="w-4 h-4 text-indigo-600 dark:text-indigo-400" />
										</div>
										<div class="font-bold text-slate-900 dark:text-white">
											{rg.ReplicationGroupId}
										</div>
									</div>
								</td>
								<td class="px-6 py-4 text-sm text-slate-500 dark:text-slate-400">
									{rg.Description ?? '—'}
								</td>
								<td class="px-6 py-4 text-center">
									<span
										class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium {getStatusBadge(rg.Status)}"
									>
										<span class="w-1.5 h-1.5 rounded-full {getStatusColor(rg.Status)}"></span>
										{rg.Status ?? '—'}
									</span>
								</td>
								<td class="px-6 py-4 text-center text-sm text-slate-600 dark:text-slate-300">
									{rg.AutomaticFailover ?? '—'}
								</td>
								<td class="px-6 py-4 text-right">
									<button
										onclick={() => deleteReplicationGroup(rg.ReplicationGroupId!)}
										class="p-2 text-slate-400 hover:text-red-500 rounded-lg transition-colors"
										title="Delete"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
						{#if !filteredRGs.length}
							<tr>
								<td colspan="5" class="px-6 py-16 text-center">
									<div class="flex flex-col items-center gap-3">
										<Layers class="w-10 h-10 text-slate-300 dark:text-slate-700" />
										<p class="text-slate-500 dark:text-slate-400">No replication groups found</p>
									</div>
								</td>
							</tr>
						{/if}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- ── PARAMETER GROUPS TAB ───────────────────────────────────────── -->
		{#if activeTab === 'parameter-groups'}
			<div class="overflow-x-auto">
				<table class="w-full text-left border-collapse">
					<thead>
						<tr class="bg-slate-50/50 dark:bg-slate-900/20">
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Name</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Family</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Description</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
								>Actions</th
							>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
						{#each filteredPGs as pg}
							<tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all">
								<td class="px-6 py-4">
									<div class="flex items-center gap-3">
										<div class="p-2 bg-purple-500/10 rounded-lg">
											<Settings class="w-4 h-4 text-purple-600 dark:text-purple-400" />
										</div>
										<div class="font-bold text-slate-900 dark:text-white">
											{pg.CacheParameterGroupName}
										</div>
									</div>
								</td>
								<td class="px-6 py-4">
									<span
										class="px-2 py-0.5 rounded text-[10px] font-bold bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300"
									>
										{pg.CacheParameterGroupFamily ?? '—'}
									</span>
								</td>
								<td class="px-6 py-4 text-sm text-slate-500 dark:text-slate-400">
									{pg.Description ?? '—'}
								</td>
								<td class="px-6 py-4 text-right">
									<button
										onclick={() => deleteParameterGroup(pg.CacheParameterGroupName!)}
										class="p-2 text-slate-400 hover:text-red-500 rounded-lg transition-colors"
										title="Delete"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
						{#if !filteredPGs.length}
							<tr>
								<td colspan="4" class="px-6 py-16 text-center">
									<div class="flex flex-col items-center gap-3">
										<Settings class="w-10 h-10 text-slate-300 dark:text-slate-700" />
										<p class="text-slate-500 dark:text-slate-400">No parameter groups found</p>
									</div>
								</td>
							</tr>
						{/if}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- ── SUBNET GROUPS TAB ──────────────────────────────────────────── -->
		{#if activeTab === 'subnet-groups'}
			<div class="overflow-x-auto">
				<table class="w-full text-left border-collapse">
					<thead>
						<tr class="bg-slate-50/50 dark:bg-slate-900/20">
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Name</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>VPC</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Description</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
								>Actions</th
							>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
						{#each filteredSGs as sg}
							<tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all">
								<td class="px-6 py-4">
									<div class="flex items-center gap-3">
										<div class="p-2 bg-teal-500/10 rounded-lg">
											<Server class="w-4 h-4 text-teal-600 dark:text-teal-400" />
										</div>
										<div class="font-bold text-slate-900 dark:text-white">
											{sg.CacheSubnetGroupName}
										</div>
									</div>
								</td>
								<td class="px-6 py-4 text-sm font-mono text-slate-500 dark:text-slate-400">
									{sg.VpcId ?? '—'}
								</td>
								<td class="px-6 py-4 text-sm text-slate-500 dark:text-slate-400">
									{sg.CacheSubnetGroupDescription ?? '—'}
								</td>
								<td class="px-6 py-4 text-right">
									<button
										onclick={() => deleteSubnetGroup(sg.CacheSubnetGroupName!)}
										class="p-2 text-slate-400 hover:text-red-500 rounded-lg transition-colors"
										title="Delete"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
						{#if !filteredSGs.length}
							<tr>
								<td colspan="4" class="px-6 py-16 text-center">
									<div class="flex flex-col items-center gap-3">
										<Server class="w-10 h-10 text-slate-300 dark:text-slate-700" />
										<p class="text-slate-500 dark:text-slate-400">No subnet groups found</p>
									</div>
								</td>
							</tr>
						{/if}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- ── SNAPSHOTS TAB ──────────────────────────────────────────────── -->
		{#if activeTab === 'snapshots'}
			<div class="overflow-x-auto">
				<table class="w-full text-left border-collapse">
					<thead>
						<tr class="bg-slate-50/50 dark:bg-slate-900/20">
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Name</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Cluster</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Status</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Source</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
								>Actions</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
								>Created</th
							>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
						{#each filteredSnapshots as snap}
							<tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all">
								<td class="px-6 py-4">
									<div class="flex items-center gap-3">
										<div class="p-2 bg-amber-500/10 rounded-lg">
											<HardDrive class="w-4 h-4 text-amber-600 dark:text-amber-400" />
										</div>
										<div class="font-bold text-slate-900 dark:text-white">{snap.SnapshotName}</div>
									</div>
								</td>
								<td class="px-6 py-4 text-sm text-slate-500 dark:text-slate-400">
									{snap.CacheClusterId ?? snap.ReplicationGroupId ?? '—'}
								</td>
								<td class="px-6 py-4 text-center">
									<span
										class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium {getStatusBadge(snap.SnapshotStatus)}"
									>
										<span class="w-1.5 h-1.5 rounded-full {getStatusColor(snap.SnapshotStatus)}"
										></span>
										{snap.SnapshotStatus ?? '—'}
									</span>
								</td>
								<td class="px-6 py-4 text-center text-xs text-slate-500 dark:text-slate-400">
									{snap.SnapshotSource ?? '—'}
								</td>
							<td class="px-6 py-4 text-right text-xs text-slate-400 font-mono whitespace-nowrap">
								{snap.NodeSnapshots?.[0]?.SnapshotCreateTime?.toLocaleDateString() ?? '—'}
							</td>
								<td class="px-6 py-4 text-right">
									<button
										onclick={() => deleteSnapshot(snap.SnapshotName!)}
										class="p-2 text-slate-400 hover:text-red-500 rounded-lg transition-colors"
										title="Delete"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
						{#if !filteredSnapshots.length}
							<tr>
								<td colspan="5" class="px-6 py-16 text-center">
									<div class="flex flex-col items-center gap-3">
										<HardDrive class="w-10 h-10 text-slate-300 dark:text-slate-700" />
										<p class="text-slate-500 dark:text-slate-400">No snapshots found</p>
									</div>
								</td>
							</tr>
						{/if}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- ── USERS TAB ──────────────────────────────────────────────────── -->
		{#if activeTab === 'users'}
			<div class="overflow-x-auto">
				<table class="w-full text-left border-collapse">
					<thead>
						<tr class="bg-slate-50/50 dark:bg-slate-900/20">
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>User</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Status</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Access String</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
								>Actions</th
							>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
						{#each filteredUsers as user}
							<tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all">
								<td class="px-6 py-4">
									<div class="flex items-center gap-3">
										<div class="p-2 bg-green-500/10 rounded-lg">
											<Users class="w-4 h-4 text-green-600 dark:text-green-400" />
										</div>
										<div>
											<div class="font-bold text-slate-900 dark:text-white">{user.UserId}</div>
											<div class="text-[10px] text-slate-400">{user.UserName}</div>
										</div>
									</div>
								</td>
								<td class="px-6 py-4 text-center">
									<span
										class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium {getStatusBadge(user.Status)}"
									>
										<span class="w-1.5 h-1.5 rounded-full {getStatusColor(user.Status)}"></span>
										{user.Status ?? '—'}
									</span>
								</td>
								<td class="px-6 py-4 text-sm font-mono text-slate-500 dark:text-slate-400">
									{user.AccessString ?? '—'}
								</td>
								<td class="px-6 py-4 text-right">
									<button
										onclick={() => deleteUser(user.UserId!)}
										class="p-2 text-slate-400 hover:text-red-500 rounded-lg transition-colors"
										title="Delete"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
						{#if !filteredUsers.length}
							<tr>
								<td colspan="4" class="px-6 py-16 text-center">
									<div class="flex flex-col items-center gap-3">
										<Users class="w-10 h-10 text-slate-300 dark:text-slate-700" />
										<p class="text-slate-500 dark:text-slate-400">No users found</p>
									</div>
								</td>
							</tr>
						{/if}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- ── SERVERLESS TAB ─────────────────────────────────────────────── -->
		{#if activeTab === 'serverless'}
			<div class="overflow-x-auto">
				<table class="w-full text-left border-collapse">
					<thead>
						<tr class="bg-slate-50/50 dark:bg-slate-900/20">
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Name</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Engine</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Status</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
								>Actions</th
							>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
						{#each filteredSL as sc}
							<tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all">
								<td class="px-6 py-4">
									<div class="flex items-center gap-3">
										<div class="p-2 bg-violet-500/10 rounded-lg">
											<Shield class="w-4 h-4 text-violet-600 dark:text-violet-400" />
										</div>
										<div class="font-bold text-slate-900 dark:text-white">
											{sc.ServerlessCacheName}
										</div>
									</div>
								</td>
								<td class="px-6 py-4 text-center">
									<span
										class="px-2 py-0.5 rounded text-[10px] font-black uppercase {(sc.Engine ?? '') === 'redis' ? 'bg-red-500/10 text-red-600 dark:text-red-400' : 'bg-blue-500/10 text-blue-600 dark:text-blue-400'}"
									>
										{sc.Engine ?? '—'}
									</span>
								</td>
								<td class="px-6 py-4 text-center">
									<span
										class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium {getStatusBadge(sc.Status)}"
									>
										<span class="w-1.5 h-1.5 rounded-full {getStatusColor(sc.Status)}"></span>
										{sc.Status ?? '—'}
									</span>
								</td>
								<td class="px-6 py-4 text-right">
									<button
										onclick={() => deleteServerlessCache(sc.ServerlessCacheName!)}
										class="p-2 text-slate-400 hover:text-red-500 rounded-lg transition-colors"
										title="Delete"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
						{#if !filteredSL.length}
							<tr>
								<td colspan="4" class="px-6 py-16 text-center">
									<div class="flex flex-col items-center gap-3">
										<Shield class="w-10 h-10 text-slate-300 dark:text-slate-700" />
										<p class="text-slate-500 dark:text-slate-400">No serverless caches found</p>
									</div>
								</td>
							</tr>
						{/if}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- ── USER GROUPS TAB ────────────────────────────────────────────── -->
		{#if activeTab === 'user-groups'}
			<div class="overflow-x-auto">
				<table class="w-full text-left border-collapse">
					<thead>
						<tr class="bg-slate-50/50 dark:bg-slate-900/20">
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Group ID</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Status</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Engine</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Users</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
								>Actions</th
							>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
						{#each filteredUserGroups as ug}
							<tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all">
								<td class="px-6 py-4">
									<div class="flex items-center gap-3">
										<div class="p-2 bg-green-500/10 rounded-lg">
											<UsersRound class="w-4 h-4 text-green-600 dark:text-green-400" />
										</div>
										<div>
											<div class="font-bold text-slate-900 dark:text-white">{ug.UserGroupId}</div>
											{#if ug.ARN}
												<div class="text-[10px] text-slate-400 font-mono">{ug.ARN}</div>
											{/if}
										</div>
									</div>
								</td>
								<td class="px-6 py-4 text-center">
									<span
										class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium {getStatusBadge(ug.Status)}"
									>
										<span class="w-1.5 h-1.5 rounded-full {getStatusColor(ug.Status)}"></span>
										{ug.Status ?? '—'}
									</span>
								</td>
								<td class="px-6 py-4 text-center">
									<span
										class="px-2 py-0.5 rounded text-[10px] font-black uppercase bg-red-500/10 text-red-600 dark:text-red-400"
									>
										{ug.Engine ?? 'Redis'}
									</span>
								</td>
								<td class="px-6 py-4 text-center text-sm text-slate-600 dark:text-slate-300">
									{ug.UserIds?.length ?? 0}
								</td>
								<td class="px-6 py-4 text-right">
									<button
										onclick={() => deleteUserGroup(ug.UserGroupId!)}
										class="p-2 text-slate-400 hover:text-red-500 rounded-lg transition-colors"
										title="Delete"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
						{#if !filteredUserGroups.length}
							<tr>
								<td colspan="5" class="px-6 py-16 text-center">
									<div class="flex flex-col items-center gap-3">
										<UsersRound class="w-10 h-10 text-slate-300 dark:text-slate-700" />
										<p class="text-slate-500 dark:text-slate-400">No user groups found</p>
									</div>
								</td>
							</tr>
						{/if}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- ── EVENTS TAB ─────────────────────────────────────────────────── -->
		{#if activeTab === 'events'}
			<div class="overflow-x-auto">
				<table class="w-full text-left border-collapse">
					<thead>
						<tr class="bg-slate-50/50 dark:bg-slate-900/20">
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Source</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Type</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Message</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
								>Date</th
							>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
						{#each filteredEvents as ev}
							<tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all">
								<td class="px-6 py-4">
									<div class="flex items-center gap-3">
										<div class="p-2 bg-cyan-500/10 rounded-lg">
											<Bell class="w-4 h-4 text-cyan-600 dark:text-cyan-400" />
										</div>
										<div class="font-bold text-slate-900 dark:text-white text-sm">
											{ev.SourceIdentifier ?? '—'}
										</div>
									</div>
								</td>
								<td class="px-6 py-4 text-center">
									<span
										class="px-2 py-0.5 rounded text-[10px] font-bold bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300"
									>
										{ev.SourceType ?? '—'}
									</span>
								</td>
								<td class="px-6 py-4 text-sm text-slate-500 dark:text-slate-400">
									{ev.Message ?? '—'}
								</td>
								<td class="px-6 py-4 text-right text-xs text-slate-400 font-mono whitespace-nowrap">
									{ev.Date?.toLocaleString() ?? '—'}
								</td>
							</tr>
						{/each}
						{#if !filteredEvents.length}
							<tr>
								<td colspan="4" class="px-6 py-16 text-center">
									<div class="flex flex-col items-center gap-3">
										<Bell class="w-10 h-10 text-slate-300 dark:text-slate-700" />
										<p class="text-slate-500 dark:text-slate-400">No events found</p>
										<p class="text-xs text-slate-400">Events are recorded on cluster operations</p>
									</div>
								</td>
							</tr>
						{/if}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- ── RESERVED CACHE NODES TAB ───────────────────────────────────── -->
		{#if activeTab === 'reserved'}
			<div class="overflow-x-auto">
				<table class="w-full text-left border-collapse">
					<thead>
						<tr class="bg-slate-50/50 dark:bg-slate-900/20">
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Reservation ID</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider"
								>Node Type</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>State</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Count</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-center"
								>Offering Type</th
							>
							<th
								class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right"
								>Start Time</th
							>
						</tr>
					</thead>
					<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
						{#each filteredReserved as rcn}
							<tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all">
								<td class="px-6 py-4">
									<div class="flex items-center gap-3">
										<div class="p-2 bg-amber-500/10 rounded-lg">
											<Bookmark class="w-4 h-4 text-amber-600 dark:text-amber-400" />
										</div>
										<div class="font-bold text-slate-900 dark:text-white text-sm">
											{rcn.ReservedCacheNodeId ?? '—'}
										</div>
									</div>
								</td>
								<td class="px-6 py-4 font-mono text-xs text-slate-600 dark:text-slate-300">
									{rcn.CacheNodeType ?? '—'}
								</td>
								<td class="px-6 py-4 text-center">
									<span
										class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-medium {getStatusBadge(rcn.State)}"
									>
										<span class="w-1.5 h-1.5 rounded-full {getStatusColor(rcn.State)}"></span>
										{rcn.State ?? '—'}
									</span>
								</td>
								<td class="px-6 py-4 text-center text-sm font-bold text-slate-700 dark:text-slate-200">
									{rcn.CacheNodeCount ?? 1}
								</td>
								<td class="px-6 py-4 text-center">
									<span
										class="px-2 py-0.5 rounded text-[10px] font-bold bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300"
									>
										{rcn.OfferingType ?? '—'}
									</span>
								</td>
								<td class="px-6 py-4 text-right text-xs text-slate-400 font-mono whitespace-nowrap">
									{rcn.StartTime?.toLocaleDateString() ?? '—'}
								</td>
							</tr>
						{/each}
						{#if !filteredReserved.length}
							<tr>
								<td colspan="6" class="px-6 py-16 text-center">
									<div class="flex flex-col items-center gap-3">
										<Bookmark class="w-10 h-10 text-slate-300 dark:text-slate-700" />
										<p class="text-slate-500 dark:text-slate-400">No reserved cache nodes</p>
										<p class="text-xs text-slate-400">
											Purchase reservations with PurchaseReservedCacheNodesOffering
										</p>
									</div>
								</td>
							</tr>
						{/if}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- ── METRICS TAB ────────────────────────────────────────────────── -->
		{#if activeTab === 'metrics'}
			<div class="p-6 space-y-6">
				<h3 class="text-lg font-semibold text-slate-900 dark:text-white">Resource Overview</h3>
				<div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4">
					{#each [['Clusters', clusters.length, 'text-cyan-600 dark:text-cyan-400'], ['Replication Groups', replicationGroups.length, 'text-indigo-600 dark:text-indigo-400'], ['Parameter Groups', parameterGroups.length, 'text-purple-600 dark:text-purple-400'], ['Subnet Groups', subnetGroups.length, 'text-teal-600 dark:text-teal-400'], ['Snapshots', snapshots.length, 'text-amber-600 dark:text-amber-400'], ['Users', users.length, 'text-green-600 dark:text-green-400'], ['User Groups', userGroups.length, 'text-emerald-600 dark:text-emerald-400'], ['Serverless Caches', serverlessCaches.length, 'text-violet-600 dark:text-violet-400'], ['Reserved Nodes', reservedCacheNodes.length, 'text-orange-600 dark:text-orange-400'], ['Available Clusters', clusters.filter((c) => c.CacheClusterStatus === 'available').length, 'text-cyan-600 dark:text-cyan-400'], ['Active Reservations', reservedCacheNodes.filter((r) => r.State === 'active').length, 'text-emerald-600 dark:text-emerald-400']] as [label, value, color]}
						<div
							class="p-5 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm"
						>
							<div
								class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider"
							>
								{label}
							</div>
							<div class="text-3xl font-bold {color} mt-2">{value}</div>
						</div>
					{/each}
				</div>

				{#if clusters.length > 0}
					<div>
						<h4
							class="text-sm font-semibold text-slate-700 dark:text-slate-200 mb-3 uppercase tracking-wider"
						>
							Clusters by Engine
						</h4>
						<div class="grid grid-cols-2 gap-4">
							{#each [['Redis', 'redis'], ['Memcached', 'memcached']] as [label, engine]}
								<div
									class="p-4 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600"
								>
									<div class="text-xs text-slate-500 dark:text-slate-400 uppercase font-bold mb-1">
										{label}
									</div>
									<div
										class="text-2xl font-bold {engine === 'redis' ? 'text-red-600 dark:text-red-400' : 'text-blue-600 dark:text-blue-400'}"
									>
										{clusters.filter((c) => c.Engine === engine).length}
									</div>
								</div>
							{/each}
						</div>
					</div>

					<div>
						<h4
							class="text-sm font-semibold text-slate-700 dark:text-slate-200 mb-3 uppercase tracking-wider"
						>
							Engine Version Distribution
						</h4>
						<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
							{#each [...new Set(clusters.map((c) => `${c.Engine ?? ''} ${c.EngineVersion ?? ''}}`.trim()).filter(Boolean))] as evLabel}
								<div
									class="p-3 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600"
								>
									<div class="text-xs text-slate-500 dark:text-slate-400 font-mono mb-1">{evLabel}</div>
									<div class="text-xl font-bold text-slate-700 dark:text-slate-200">
										{clusters.filter((c) => `${c.Engine ?? ''} ${c.EngineVersion ?? ''}}`.trim() === evLabel).length}
									</div>
								</div>
							{/each}
						</div>
					</div>
				{/if}

				<div class="pt-4">
					<button
						onclick={() => void loadAllMetrics()}
						class="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-700 text-white rounded-xl font-medium text-sm transition-all active:scale-95"
					>
						<RefreshCw class="w-4 h-4 {loading ? 'animate-spin' : ''}" />
						Refresh Metrics
					</button>
				</div>
			</div>
		{/if}

		<!-- ── DOCS TAB ───────────────────────────────────────────────────── -->
		{#if activeTab === 'docs'}
			<div class="p-6 space-y-6 max-w-4xl">
				<div class="flex items-center gap-3 mb-2">
					<BookOpen class="w-6 h-6 text-cyan-500" />
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">ElastiCache Operations</h3>
				</div>
				<p class="text-slate-500 dark:text-slate-400 text-sm">
					This emulator supports the ElastiCache 2015-02-02 query protocol. All requests use
					<code class="text-xs bg-slate-100 dark:bg-slate-700 px-1.5 py-0.5 rounded font-mono"
						>POST application/x-www-form-urlencoded</code
					> with an <code
						class="text-xs bg-slate-100 dark:bg-slate-700 px-1.5 py-0.5 rounded font-mono"
						>Action</code
					> parameter.
				</p>
				<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
					{#each [['Clusters', ['CreateCacheCluster', 'DeleteCacheCluster', 'DescribeCacheClusters', 'ModifyCacheCluster', 'RebootCacheCluster', 'DescribeCacheEngineVersions']], ['Replication Groups', ['CreateReplicationGroup', 'DeleteReplicationGroup', 'DescribeReplicationGroups', 'ModifyReplicationGroup', 'TestFailover', 'IncreaseReplicaCount', 'DecreaseReplicaCount', 'ModifyReplicationGroupShardConfiguration']], ['Parameter Groups', ['CreateCacheParameterGroup', 'DeleteCacheParameterGroup', 'DescribeCacheParameterGroups', 'ModifyCacheParameterGroup', 'ResetCacheParameterGroup', 'DescribeCacheParameters', 'DescribeEngineDefaultParameters']], ['Subnet Groups', ['CreateCacheSubnetGroup', 'DeleteCacheSubnetGroup', 'DescribeCacheSubnetGroups', 'ModifyCacheSubnetGroup']], ['Snapshots', ['CreateSnapshot', 'DeleteSnapshot', 'DescribeSnapshots', 'CopySnapshot']], ['Users & Groups', ['CreateUser', 'DeleteUser', 'DescribeUsers', 'ModifyUser', 'CreateUserGroup', 'DeleteUserGroup', 'DescribeUserGroups', 'ModifyUserGroup']], ['Serverless', ['CreateServerlessCache', 'DeleteServerlessCache', 'DescribeServerlessCaches', 'ModifyServerlessCache', 'CreateServerlessCacheSnapshot', 'DeleteServerlessCacheSnapshot', 'DescribeServerlessCacheSnapshots', 'CopyServerlessCacheSnapshot', 'ExportServerlessCacheSnapshot']], ['Global Replication', ['CreateGlobalReplicationGroup', 'DeleteGlobalReplicationGroup', 'DescribeGlobalReplicationGroups', 'ModifyGlobalReplicationGroup', 'FailoverGlobalReplicationGroup', 'DisassociateGlobalReplicationGroup', 'IncreaseNodeGroupsInGlobalReplicationGroup', 'DecreaseNodeGroupsInGlobalReplicationGroup', 'RebalanceSlotsInGlobalReplicationGroup']], ['Tags', ['AddTagsToResource', 'RemoveTagsFromResource', 'ListTagsForResource']], ['Reserved Nodes', ['DescribeReservedCacheNodes', 'DescribeReservedCacheNodesOfferings', 'PurchaseReservedCacheNodesOffering']], ['Events', ['DescribeEvents']], ['Misc', ['BatchApplyUpdateAction', 'BatchStopUpdateAction', 'CompleteMigration', 'StartMigration', 'TestMigration', 'DescribeServiceUpdates', 'DescribeUpdateActions', 'ListAllowedNodeTypeModifications', 'CreateCacheSecurityGroup', 'AuthorizeCacheSecurityGroupIngress', 'DeleteCacheSecurityGroup', 'DescribeCacheSecurityGroups', 'RevokeCacheSecurityGroupIngress']]] as [category, ops]}
						<div
							class="p-4 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600"
						>
							<h4 class="font-bold text-slate-900 dark:text-white text-sm mb-3 flex items-center gap-2">
								<Tag class="w-4 h-4 text-cyan-500" />
								{category}
							</h4>
							<div class="space-y-1">
								{#each ops as op}
									<div
										class="flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400 py-0.5"
									>
										<span class="w-1.5 h-1.5 rounded-full bg-emerald-500 flex-shrink-0"></span>
										<code
											class="font-mono text-slate-700 dark:text-slate-200 text-[11px] bg-slate-50 dark:bg-slate-800 px-1.5 py-0.5 rounded"
											>{op}</code
										>
									</div>
								{/each}
							</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}
	</div>
</div>

<!-- ── MODALS ─────────────────────────────────────────────────────────────── -->

<!-- Create Cluster Modal -->
{#if showCreateClusterModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div
			role="none"
			onclick={() => (showCreateClusterModal = false)}
			onkeydown={(e) => e.key === 'Escape' && (showCreateClusterModal = false)}
			class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
		></div>
		<div
			class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700 overflow-hidden"
		>
			<div class="p-6">
				<div class="flex items-center justify-between mb-6">
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">Create Cache Cluster</h3>
					<button
						onclick={() => (showCreateClusterModal = false)}
						class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						createCluster();
					}}
					class="space-y-4"
				>
					<div>
						<label
							for="clusterId"
							class="block text-xs font-bold text-slate-500 dark:text-slate-400 uppercase tracking-widest mb-1.5 px-1"
							>Cluster ID</label
						>
						<input
							id="clusterId"
							type="text"
							bind:value={newClusterId}
							placeholder="e.g. session-cache"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono"
							required
						/>
					</div>
					<div class="grid grid-cols-2 gap-4">
						<div>
							<label
								for="ec-engine"
								class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
								>Engine</label
							>
							<select
								id="ec-engine"
								bind:value={clusterEngine}
								class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500"
							>
								<option value="redis">Redis</option>
								<option value="memcached">Memcached</option>
							</select>
						</div>
						<div>
							<label
								for="ec-nodes"
								class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
								>Nodes</label
							>
							<input
								id="ec-nodes"
								type="number"
								bind:value={clusterNumNodes}
								min="1"
								max="20"
								class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500"
							/>
						</div>
					</div>
					<div>
						<label
							for="ec-node-type"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Node Type</label
						>
						<select
							id="ec-node-type"
							bind:value={clusterNodeType}
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono text-xs"
						>
							<option value="cache.t3.micro">cache.t3.micro (0.5 GiB)</option>
							<option value="cache.t3.small">cache.t3.small (1.37 GiB)</option>
							<option value="cache.t3.medium">cache.t3.medium (3.09 GiB)</option>
							<option value="cache.m5.large">cache.m5.large (6.38 GiB)</option>
							<option value="cache.m5.xlarge">cache.m5.xlarge (12.93 GiB)</option>
							<option value="cache.r6g.large">cache.r6g.large (13.07 GiB)</option>
						</select>
					</div>
					<div>
						<label
							for="ec-maint-window"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Maintenance Window</label
						>
						<input
							id="ec-maint-window"
							type="text"
							bind:value={clusterMaintWindow}
							placeholder="sun:05:00-sun:06:00"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono text-sm"
						/>
					</div>
					<div class="flex gap-3 pt-2">
						<button
							type="button"
							onclick={() => (showCreateClusterModal = false)}
							class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-bold"
							>Cancel</button
						>
						<button
							type="submit"
							disabled={creatingCluster}
							class="flex-1 px-4 py-3 bg-cyan-600 text-white rounded-xl font-bold disabled:opacity-50"
						>
							{creatingCluster ? 'Creating…' : 'Create'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<!-- Create Replication Group Modal -->
{#if showCreateRGModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div
			role="none"
			onclick={() => (showCreateRGModal = false)}
			onkeydown={(e) => e.key === 'Escape' && (showCreateRGModal = false)}
			class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
		></div>
		<div
			class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700"
		>
			<div class="p-6">
				<div class="flex items-center justify-between mb-6">
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">
						Create Replication Group
					</h3>
					<button
						onclick={() => (showCreateRGModal = false)}
						class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						createReplicationGroup();
					}}
					class="space-y-4"
				>
					<div>
						<label
							for="rg-id"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Replication Group ID</label
						>
						<input
							id="rg-id"
							type="text"
							bind:value={newRGId}
							placeholder="e.g. my-redis-rg"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono"
							required
						/>
					</div>
					<div>
						<label
							for="rg-desc"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Description</label
						>
						<input
							id="rg-desc"
							type="text"
							bind:value={newRGDesc}
							placeholder="Optional description"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500"
						/>
					</div>
					<div class="flex gap-3 pt-2">
						<button
							type="button"
							onclick={() => (showCreateRGModal = false)}
							class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-bold"
							>Cancel</button
						>
						<button
							type="submit"
							disabled={creatingRG}
							class="flex-1 px-4 py-3 bg-cyan-600 text-white rounded-xl font-bold disabled:opacity-50"
						>
							{creatingRG ? 'Creating…' : 'Create'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<!-- Create Parameter Group Modal -->
{#if showCreatePGModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div
			role="none"
			onclick={() => (showCreatePGModal = false)}
			onkeydown={(e) => e.key === 'Escape' && (showCreatePGModal = false)}
			class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
		></div>
		<div
			class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700"
		>
			<div class="p-6">
				<div class="flex items-center justify-between mb-6">
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">Create Parameter Group</h3>
					<button
						onclick={() => (showCreatePGModal = false)}
						class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						createParameterGroup();
					}}
					class="space-y-4"
				>
					<div>
						<label
							for="pg-name"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Name</label
						>
						<input
							id="pg-name"
							type="text"
							bind:value={newPGName}
							placeholder="e.g. my-redis-params"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono"
							required
						/>
					</div>
					<div>
						<label
							for="pg-family"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Family</label
						>
						<select
							id="pg-family"
							bind:value={newPGFamily}
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500"
						>
							<option value="redis7">redis7</option>
							<option value="redis6.x">redis6.x</option>
							<option value="redis5.0">redis5.0</option>
							<option value="memcached1.6">memcached1.6</option>
							<option value="memcached1.5">memcached1.5</option>
						</select>
					</div>
					<div>
						<label
							for="pg-desc"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Description</label
						>
						<input
							id="pg-desc"
							type="text"
							bind:value={newPGDesc}
							placeholder="Optional description"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500"
						/>
					</div>
					<div class="flex gap-3 pt-2">
						<button
							type="button"
							onclick={() => (showCreatePGModal = false)}
							class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-bold"
							>Cancel</button
						>
						<button
							type="submit"
							disabled={creatingPG}
							class="flex-1 px-4 py-3 bg-cyan-600 text-white rounded-xl font-bold disabled:opacity-50"
						>
							{creatingPG ? 'Creating…' : 'Create'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<!-- Create Subnet Group Modal -->
{#if showCreateSGModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div
			role="none"
			onclick={() => (showCreateSGModal = false)}
			onkeydown={(e) => e.key === 'Escape' && (showCreateSGModal = false)}
			class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
		></div>
		<div
			class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700"
		>
			<div class="p-6">
				<div class="flex items-center justify-between mb-6">
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">Create Subnet Group</h3>
					<button
						onclick={() => (showCreateSGModal = false)}
						class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						createSubnetGroup();
					}}
					class="space-y-4"
				>
					<div>
						<label
							for="sg-name"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Name</label
						>
						<input
							id="sg-name"
							type="text"
							bind:value={newSGName}
							placeholder="e.g. my-subnet-group"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono"
							required
						/>
					</div>
					<div>
						<label
							for="sg-desc"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Description</label
						>
						<input
							id="sg-desc"
							type="text"
							bind:value={newSGDesc}
							placeholder="Optional description"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500"
						/>
					</div>
					<div>
						<label
							for="sg-subnets"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Subnet IDs (comma-separated)</label
						>
						<input
							id="sg-subnets"
							type="text"
							bind:value={newSGSubnets}
							placeholder="subnet-12345678, subnet-87654321"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono text-sm"
							required
						/>
					</div>
					<div class="flex gap-3 pt-2">
						<button
							type="button"
							onclick={() => (showCreateSGModal = false)}
							class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-bold"
							>Cancel</button
						>
						<button
							type="submit"
							disabled={creatingSG}
							class="flex-1 px-4 py-3 bg-cyan-600 text-white rounded-xl font-bold disabled:opacity-50"
						>
							{creatingSG ? 'Creating…' : 'Create'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<!-- Create Snapshot Modal -->
{#if showCreateSnapModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div
			role="none"
			onclick={() => (showCreateSnapModal = false)}
			onkeydown={(e) => e.key === 'Escape' && (showCreateSnapModal = false)}
			class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
		></div>
		<div
			class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700"
		>
			<div class="p-6">
				<div class="flex items-center justify-between mb-6">
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">Create Snapshot</h3>
					<button
						onclick={() => (showCreateSnapModal = false)}
						class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						createSnapshot();
					}}
					class="space-y-4"
				>
					<div>
						<label
							for="snap-name"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Snapshot Name</label
						>
						<input
							id="snap-name"
							type="text"
							bind:value={newSnapName}
							placeholder="e.g. my-snapshot-2024"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono"
							required
						/>
					</div>
					<div>
						<label
							for="snap-cluster"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Source Cluster ID</label
						>
						<input
							id="snap-cluster"
							type="text"
							bind:value={newSnapClusterId}
							list="cluster-list"
							placeholder="e.g. my-cluster"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono"
							required
						/>
						<datalist id="cluster-list">
							{#each clusters as c}
								<option value={c.CacheClusterId}>{c.CacheClusterId}</option>
							{/each}
						</datalist>
					</div>
					<div class="flex gap-3 pt-2">
						<button
							type="button"
							onclick={() => (showCreateSnapModal = false)}
							class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-bold"
							>Cancel</button
						>
						<button
							type="submit"
							disabled={creatingSnap}
							class="flex-1 px-4 py-3 bg-cyan-600 text-white rounded-xl font-bold disabled:opacity-50"
						>
							{creatingSnap ? 'Creating…' : 'Create'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<!-- Create User Modal -->
{#if showCreateUserModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div
			role="none"
			onclick={() => (showCreateUserModal = false)}
			onkeydown={(e) => e.key === 'Escape' && (showCreateUserModal = false)}
			class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
		></div>
		<div
			class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700"
		>
			<div class="p-6">
				<div class="flex items-center justify-between mb-6">
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">Create User</h3>
					<button
						onclick={() => (showCreateUserModal = false)}
						class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						createUser();
					}}
					class="space-y-4"
				>
					<div class="grid grid-cols-2 gap-4">
						<div>
							<label
								for="user-id"
								class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
								>User ID</label
							>
							<input
								id="user-id"
								type="text"
								bind:value={newUserId}
								placeholder="e.g. my-user"
								class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono text-sm"
								required
							/>
						</div>
						<div>
							<label
								for="user-name"
								class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
								>Username</label
							>
							<input
								id="user-name"
								type="text"
								bind:value={newUserName}
								placeholder="e.g. alice"
								class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono text-sm"
								required
							/>
						</div>
					</div>
					<div>
						<label
							for="user-access"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Access String</label
						>
						<input
							id="user-access"
							type="text"
							bind:value={newUserAccess}
							placeholder="on ~* +@all"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono text-sm"
						/>
					</div>
					<label class="flex items-center gap-3 cursor-pointer">
						<input
							type="checkbox"
							bind:checked={newUserNoPassword}
							class="w-4 h-4 rounded accent-cyan-600"
						/>
						<span class="text-sm text-slate-700 dark:text-slate-200">No password required</span>
					</label>
					<div class="flex gap-3 pt-2">
						<button
							type="button"
							onclick={() => (showCreateUserModal = false)}
							class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-bold"
							>Cancel</button
						>
						<button
							type="submit"
							disabled={creatingUser}
							class="flex-1 px-4 py-3 bg-cyan-600 text-white rounded-xl font-bold disabled:opacity-50"
						>
							{creatingUser ? 'Creating…' : 'Create'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<!-- Create Serverless Cache Modal -->
{#if showCreateSLModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div
			role="none"
			onclick={() => (showCreateSLModal = false)}
			onkeydown={(e) => e.key === 'Escape' && (showCreateSLModal = false)}
			class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
		></div>
		<div
			class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700"
		>
			<div class="p-6">
				<div class="flex items-center justify-between mb-6">
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">Create Serverless Cache</h3>
					<button
						onclick={() => (showCreateSLModal = false)}
						class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						createServerlessCache();
					}}
					class="space-y-4"
				>
					<div>
						<label
							for="sl-name"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Name</label
						>
						<input
							id="sl-name"
							type="text"
							bind:value={newSLName}
							placeholder="e.g. my-serverless-cache"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono"
							required
						/>
					</div>
					<div>
						<label
							for="sl-engine"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Engine</label
						>
						<select
							id="sl-engine"
							bind:value={newSLEngine}
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500"
						>
							<option value="redis">Redis</option>
							<option value="memcached">Memcached</option>
						</select>
					</div>
					<div class="flex gap-3 pt-2">
						<button
							type="button"
							onclick={() => (showCreateSLModal = false)}
							class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-bold"
							>Cancel</button
						>
						<button
							type="submit"
							disabled={creatingSL}
							class="flex-1 px-4 py-3 bg-cyan-600 text-white rounded-xl font-bold disabled:opacity-50"
						>
							{creatingSL ? 'Creating…' : 'Create'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<!-- Create User Group Modal -->
{#if showCreateUGModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div
			role="none"
			onclick={() => (showCreateUGModal = false)}
			onkeydown={(e) => e.key === 'Escape' && (showCreateUGModal = false)}
			class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"
		></div>
		<div
			class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700"
		>
			<div class="p-6">
				<div class="flex items-center justify-between mb-6">
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">Create User Group</h3>
					<button
						onclick={() => (showCreateUGModal = false)}
						class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
				<form
					onsubmit={(e) => {
						e.preventDefault();
						createUserGroup();
					}}
					class="space-y-4"
				>
					<div>
						<label
							for="ug-id"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Group ID</label
						>
						<input
							id="ug-id"
							type="text"
							bind:value={newUGId}
							placeholder="e.g. admin-group"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500 font-mono"
							required
						/>
					</div>
					<div>
						<label
							for="ug-desc"
							class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1.5 px-1"
							>Description (optional)</label
						>
						<input
							id="ug-desc"
							type="text"
							bind:value={newUGDesc}
							placeholder="Optional description"
							class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-cyan-500"
						/>
					</div>
					<div class="flex gap-3 pt-2">
						<button
							type="button"
							onclick={() => (showCreateUGModal = false)}
							class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-bold"
							>Cancel</button
						>
						<button
							type="submit"
							disabled={creatingUG}
							class="flex-1 px-4 py-3 bg-cyan-600 text-white rounded-xl font-bold disabled:opacity-50"
						>
							{creatingUG ? 'Creating…' : 'Create'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<style>
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
