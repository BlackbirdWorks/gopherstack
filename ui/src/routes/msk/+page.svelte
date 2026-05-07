<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getMSKClient } from '$lib/aws-client';
	import {
		ListClustersV2Command,
		DescribeClusterV2Command,
		CreateClusterV2Command,
		DeleteClusterCommand,
		ListNodesCommand,
		ListKafkaVersionsCommand,
		ListConfigurationsCommand,
		CreateConfigurationCommand,
		DeleteConfigurationCommand,
		ListReplicatorsCommand,
		CreateReplicatorCommand,
		DeleteReplicatorCommand,
		ListVpcConnectionsCommand,
		DeleteVpcConnectionCommand,
		GetBootstrapBrokersCommand,
		ListClusterOperationsV2Command,
		type ClusterInfo,
		type NodeInfo,
		type Configuration,
		type ReplicatorSummary,
		type VpcConnection,
		type ClusterOperationV2
	} from '@aws-sdk/client-kafka';
	import { toast } from 'svelte-sonner';
	import {
		Database,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Eye,
		Server,
		Layers,
		GitBranch,
		Network,
		Activity,
		ChevronRight
	} from 'lucide-svelte';

	const msk = getMSKClient();

	type Tab = 'clusters' | 'configs' | 'replicators' | 'vpc' | 'versions';
	let activeTab = $state<Tab>('clusters');
	let searchQuery = $state('');
	let loading = $state(false);

	// ─── Clusters ───────────────────────────────────────────────
	let clusters = $state<ClusterInfo[]>([]);
	let selectedCluster = $state<ClusterInfo | null>(null);
	let clusterNodes = $state<NodeInfo[]>([]);
	let clusterOps = $state<ClusterOperationV2[]>([]);
	let bootstrapBrokers = $state('');
	let loadingDetail = $state(false);
	let showCreateClusterModal = $state(false);
	let creatingCluster = $state(false);
	let newClusterName = $state('');
	let newBrokerCount = $state(3);
	let newBrokerType = $state('kafka.m5.large');
	let newKafkaVersion = $state('2.8.1');
	let newStorageSize = $state(100);

	const filteredClusters = $derived(
		clusters.filter((c) => (c.ClusterName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ─── Configurations ─────────────────────────────────────────
	let configurations = $state<Configuration[]>([]);
	let showCreateConfigModal = $state(false);
	let creatingConfig = $state(false);
	let newConfigName = $state('');
	let newConfigProperties = $state('auto.create.topics.enable=false\ndefault.replication.factor=3');

	// ─── Replicators ────────────────────────────────────────────
	let replicators = $state<ReplicatorSummary[]>([]);
	let showCreateReplicatorModal = $state(false);
	let creatingReplicator = $state(false);
	let newReplicatorName = $state('');
	let newReplicatorRoleArn = $state('');

	// ─── VPC Connections ────────────────────────────────────────
	let vpcConnections = $state<VpcConnection[]>([]);

	// ─── Kafka Versions ─────────────────────────────────────────
	let kafkaVersions = $state<Array<{ Version?: string; Status?: string }>>([]);

	// ─── Helpers ────────────────────────────────────────────────
	function stateBadge(state?: string) {
		switch (state) {
			case 'ACTIVE':
				return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
			case 'CREATING':
				return 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-900';
			case 'DELETING':
				return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
			case 'FAILED':
				return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
			case 'RUNNING':
				return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
			default:
				return 'text-muted-foreground bg-muted';
		}
	}

	// ─── Cluster actions ────────────────────────────────────────
	async function loadClusters() {
		loading = true;
		try {
			const res = await msk.send(new ListClustersV2Command({ MaxResults: 100 }));
			clusters = res.ClusterInfoList ?? [];
		} catch (e) {
			toast.error(`Failed to load clusters: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewCluster(cluster: ClusterInfo) {
		if (!cluster.ClusterArn) return;
		selectedCluster = cluster;
		loadingDetail = true;
		clusterNodes = [];
		clusterOps = [];
		bootstrapBrokers = '';
		try {
			const [nodesRes, opsRes, brokerRes] = await Promise.allSettled([
				msk.send(new ListNodesCommand({ ClusterArn: cluster.ClusterArn, MaxResults: 100 })),
				msk.send(
					new ListClusterOperationsV2Command({ ClusterArn: cluster.ClusterArn, MaxResults: 20 })
				),
				msk.send(new GetBootstrapBrokersCommand({ ClusterArn: cluster.ClusterArn }))
			]);
			if (nodesRes.status === 'fulfilled') clusterNodes = nodesRes.value.NodeInfoList ?? [];
			if (opsRes.status === 'fulfilled')
				clusterOps = opsRes.value.ClusterOperationInfoList ?? [];
			if (brokerRes.status === 'fulfilled')
				bootstrapBrokers =
					brokerRes.value.BootstrapBrokerString ??
					brokerRes.value.BootstrapBrokerStringTls ??
					'';
		} catch (e) {
			toast.error(`Failed to load cluster detail: ${e}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function deleteCluster(cluster: ClusterInfo) {
		if (
			!cluster.ClusterArn ||
			!(await confirmDestructive({
				title: 'Delete MSK Cluster',
				message: `Delete cluster "${cluster.ClusterName}"? All broker data will be permanently removed.`
			}))
		)
			return;
		try {
			await msk.send(
				new DeleteClusterCommand({
					ClusterArn: cluster.ClusterArn,
					CurrentVersion: cluster.CurrentVersion
				})
			);
			toast.success(`Cluster "${cluster.ClusterName}" deletion initiated`);
			if (selectedCluster?.ClusterArn === cluster.ClusterArn) selectedCluster = null;
			await loadClusters();
		} catch (e) {
			toast.error(`Failed to delete cluster: ${e}`);
		}
	}

	async function createCluster() {
		if (!newClusterName.trim()) return;
		creatingCluster = true;
		try {
			await msk.send(
				new CreateClusterV2Command({
					ClusterName: newClusterName.trim(),
					Provisioned: {
						BrokerNodeGroupInfo: {
							InstanceType: newBrokerType,
							ClientSubnets: [],
							BrokerAZDistribution: 'DEFAULT',
							StorageInfo: { EbsStorageInfo: { VolumeSize: newStorageSize } }
						},
						NumberOfBrokerNodes: newBrokerCount,
						KafkaVersion: newKafkaVersion
					}
				})
			);
			toast.success(`Cluster "${newClusterName}" creation initiated`);
			showCreateClusterModal = false;
			newClusterName = '';
			await loadClusters();
		} catch (e) {
			toast.error(`Failed to create cluster: ${e}`);
		} finally {
			creatingCluster = false;
		}
	}

	// ─── Configuration actions ──────────────────────────────────
	async function loadConfigurations() {
		loading = true;
		try {
			const res = await msk.send(new ListConfigurationsCommand({ MaxResults: 100 }));
			configurations = res.Configurations ?? [];
		} catch (e) {
			toast.error(`Failed to load configurations: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function createConfiguration() {
		if (!newConfigName.trim()) return;
		creatingConfig = true;
		try {
			await msk.send(
				new CreateConfigurationCommand({
					Name: newConfigName.trim(),
					ServerProperties: new TextEncoder().encode(newConfigProperties)
				})
			);
			toast.success(`Configuration "${newConfigName}" created`);
			showCreateConfigModal = false;
			newConfigName = '';
			await loadConfigurations();
		} catch (e) {
			toast.error(`Failed to create configuration: ${e}`);
		} finally {
			creatingConfig = false;
		}
	}

	async function deleteConfiguration(config: Configuration) {
		if (
			!config.Arn ||
			!(await confirmDestructive({
				title: 'Delete Configuration',
				message: `Delete configuration "${config.Name}"?`
			}))
		)
			return;
		try {
			await msk.send(new DeleteConfigurationCommand({ Arn: config.Arn }));
			toast.success(`Configuration "${config.Name}" deleted`);
			await loadConfigurations();
		} catch (e) {
			toast.error(`Failed to delete configuration: ${e}`);
		}
	}

	// ─── Replicator actions ──────────────────────────────────────
	async function loadReplicators() {
		loading = true;
		try {
			const res = await msk.send(new ListReplicatorsCommand({ MaxResults: 100 }));
			replicators = res.Replicators ?? [];
		} catch (e) {
			toast.error(`Failed to load replicators: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function createReplicator() {
		if (!newReplicatorName.trim()) return;
		creatingReplicator = true;
		try {
			await msk.send(
				new CreateReplicatorCommand({
					ReplicatorName: newReplicatorName.trim(),
					ServiceExecutionRoleArn: newReplicatorRoleArn.trim() || 'arn:aws:iam::000000000000:role/MSKRole',
					KafkaClusters: [],
					ReplicationInfoList: []
				})
			);
			toast.success(`Replicator "${newReplicatorName}" created`);
			showCreateReplicatorModal = false;
			newReplicatorName = '';
			newReplicatorRoleArn = '';
			await loadReplicators();
		} catch (e) {
			toast.error(`Failed to create replicator: ${e}`);
		} finally {
			creatingReplicator = false;
		}
	}

	async function deleteReplicator(r: ReplicatorSummary) {
		if (
			!r.ReplicatorArn ||
			!(await confirmDestructive({
				title: 'Delete Replicator',
				message: `Delete replicator "${r.ReplicatorName}"?`
			}))
		)
			return;
		try {
			await msk.send(
				new DeleteReplicatorCommand({
					ReplicatorArn: r.ReplicatorArn,
					CurrentVersion: r.CurrentVersion
				})
			);
			toast.success(`Replicator "${r.ReplicatorName}" deleted`);
			await loadReplicators();
		} catch (e) {
			toast.error(`Failed to delete replicator: ${e}`);
		}
	}

	// ─── VPC connection actions ──────────────────────────────────
	async function loadVpcConnections() {
		loading = true;
		try {
			const res = await msk.send(new ListVpcConnectionsCommand({ MaxResults: 100 }));
			vpcConnections = res.VpcConnections ?? [];
		} catch (e) {
			toast.error(`Failed to load VPC connections: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function deleteVpcConnection(v: VpcConnection) {
		if (
			!v.VpcConnectionArn ||
			!(await confirmDestructive({
				title: 'Delete VPC Connection',
				message: `Delete VPC connection for VPC "${v.VpcId}"?`
			}))
		)
			return;
		try {
			await msk.send(new DeleteVpcConnectionCommand({ Arn: v.VpcConnectionArn }));
			toast.success('VPC connection deleted');
			await loadVpcConnections();
		} catch (e) {
			toast.error(`Failed to delete VPC connection: ${e}`);
		}
	}

	// ─── Kafka versions ──────────────────────────────────────────
	async function loadVersions() {
		loading = true;
		try {
			const res = await msk.send(new ListKafkaVersionsCommand({ MaxResults: 100 }));
			kafkaVersions = (res.KafkaVersions ?? []).map((v) => ({
				Version: v.Version,
				Status: v.Status
			}));
		} catch (e) {
			toast.error(`Failed to load Kafka versions: ${e}`);
		} finally {
			loading = false;
		}
	}

	// ─── Tab switching ───────────────────────────────────────────
	async function onTabChange(tab: Tab) {
		activeTab = tab;
		searchQuery = '';
		selectedCluster = null;
		switch (tab) {
			case 'clusters':
				await loadClusters();
				break;
			case 'configs':
				await loadConfigurations();
				break;
			case 'replicators':
				await loadReplicators();
				break;
			case 'vpc':
				await loadVpcConnections();
				break;
			case 'versions':
				await loadVersions();
				break;
		}
	}

	onMount(() => loadClusters());
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Database class="h-8 w-8 text-orange-600" />
			<div>
				<h1 class="text-2xl font-bold">Amazon MSK</h1>
				<p class="text-sm text-muted-foreground">Managed Streaming for Apache Kafka</p>
			</div>
		</div>
		<button
			onclick={() => onTabChange(activeTab)}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [
			{ id: 'clusters', label: 'Clusters', icon: Server },
			{ id: 'configs', label: 'Configurations', icon: Layers },
			{ id: 'replicators', label: 'Replicators', icon: GitBranch },
			{ id: 'vpc', label: 'VPC Connections', icon: Network },
			{ id: 'versions', label: 'Kafka Versions', icon: Activity }
		] as tab}
			<button
				onclick={() => onTabChange(tab.id as Tab)}
				class="flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				<tab.icon class="h-4 w-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- ─── Clusters Tab ─────────────────────────────────────── -->
	{#if activeTab === 'clusters'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search clusters..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateClusterModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Cluster
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if filteredClusters.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Database class="h-12 w-12 mb-3 opacity-30" />
				<p>No MSK clusters found</p>
				<p class="text-sm">Create a cluster to start streaming with Kafka</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-left font-medium">State</th>
							<th class="px-4 py-3 text-left font-medium">Version</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredClusters as cluster}
							<tr
								class="hover:bg-muted/30 cursor-pointer {selectedCluster?.ClusterArn === cluster.ClusterArn ? 'bg-muted/50' : ''}"
								onclick={() => viewCluster(cluster)}
							>
								<td class="px-4 py-3 font-medium flex items-center gap-1">
									{cluster.ClusterName}
									{#if selectedCluster?.ClusterArn === cluster.ClusterArn}
										<ChevronRight class="h-3 w-3 text-primary" />
									{/if}
								</td>
								<td class="px-4 py-3 text-muted-foreground capitalize">{cluster.CurrentVersion ?? '—'}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {stateBadge(cluster.State)}">
										{cluster.State ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{cluster.CurrentVersion ?? '—'}</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={(e) => { e.stopPropagation(); deleteCluster(cluster); }}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete cluster"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- Cluster Detail Panel -->
		{#if selectedCluster}
			<div class="rounded-lg border p-4 space-y-4">
				<div class="flex items-center justify-between">
					<h3 class="font-semibold flex items-center gap-2">
						<Server class="h-5 w-5" />
						{selectedCluster.ClusterName}
					</h3>
					<button onclick={() => (selectedCluster = null)} class="text-xs text-muted-foreground hover:text-foreground">Close</button>
				</div>

				{#if bootstrapBrokers}
					<div class="rounded bg-muted/50 px-3 py-2 text-xs font-mono text-muted-foreground break-all">
						<span class="font-semibold text-foreground">Bootstrap Brokers:</span> {bootstrapBrokers}
					</div>
				{/if}

				{#if loadingDetail}
					<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
				{:else}
					<!-- Broker Nodes -->
					<div>
						<h4 class="text-sm font-medium mb-2">Broker Nodes</h4>
						{#if clusterNodes.length === 0}
							<p class="text-sm text-muted-foreground">No nodes found.</p>
						{:else}
							<div class="rounded border overflow-hidden">
								<table class="w-full text-sm">
									<thead class="bg-muted/50">
										<tr>
											<th class="px-3 py-2 text-left font-medium">Broker ID</th>
											<th class="px-3 py-2 text-left font-medium">Instance Type</th>
											<th class="px-3 py-2 text-left font-medium">Endpoint</th>
										</tr>
									</thead>
									<tbody class="divide-y">
										{#each clusterNodes as node}
											<tr>
												<td class="px-3 py-2">{node.BrokerNodeInfo?.BrokerId ?? '—'}</td>
												<td class="px-3 py-2">{node.InstanceType ?? '—'}</td>
												<td class="px-3 py-2 text-xs text-muted-foreground truncate max-w-[250px]">
													{node.BrokerNodeInfo?.Endpoints?.[0] ?? '—'}
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</div>

					<!-- Recent Operations -->
					{#if clusterOps.length > 0}
						<div>
							<h4 class="text-sm font-medium mb-2">Recent Operations</h4>
							<div class="rounded border overflow-hidden">
								<table class="w-full text-sm">
									<thead class="bg-muted/50">
										<tr>
											<th class="px-3 py-2 text-left font-medium">Type</th>
											<th class="px-3 py-2 text-left font-medium">State</th>
										</tr>
									</thead>
									<tbody class="divide-y">
										{#each clusterOps as op}
											<tr>
												<td class="px-3 py-2">{op.OperationType ?? '—'}</td>
												<td class="px-3 py-2">
													<span class="rounded-full px-2 py-0.5 text-xs font-medium {stateBadge(op.OperationState)}">
														{op.OperationState ?? '—'}
													</span>
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						</div>
					{/if}
				{/if}
			</div>
		{/if}
	{/if}

	<!-- ─── Configurations Tab ───────────────────────────────── -->
	{#if activeTab === 'configs'}
		<div class="flex justify-end">
			<button
				onclick={() => (showCreateConfigModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Configuration
			</button>
		</div>
		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if configurations.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Layers class="h-12 w-12 mb-3 opacity-30" />
				<p>No configurations found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Description</th>
							<th class="px-4 py-3 text-left font-medium">Kafka Versions</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each configurations as config}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{config.Name ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground">{config.Description ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">{(config.KafkaVersions ?? []).join(', ') || '—'}</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteConfiguration(config)}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- ─── Replicators Tab ─────────────────────────────────── -->
	{#if activeTab === 'replicators'}
		<div class="flex justify-end">
			<button
				onclick={() => (showCreateReplicatorModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Replicator
			</button>
		</div>
		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if replicators.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<GitBranch class="h-12 w-12 mb-3 opacity-30" />
				<p>No replicators found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">State</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each replicators as r}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{r.ReplicatorName ?? '—'}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {stateBadge(r.ReplicatorState)}">
										{r.ReplicatorState ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteReplicator(r)}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- ─── VPC Connections Tab ─────────────────────────────── -->
	{#if activeTab === 'vpc'}
		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if vpcConnections.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Network class="h-12 w-12 mb-3 opacity-30" />
				<p>No VPC connections found</p>
				<p class="text-sm">VPC connections are created via CreateVpcConnection API</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">VPC ID</th>
							<th class="px-4 py-3 text-left font-medium">State</th>
							<th class="px-4 py-3 text-left font-medium">Authentication</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each vpcConnections as v}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-mono font-medium">{v.VpcId ?? '—'}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {stateBadge(v.State)}">
										{v.State ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{v.Authentication ?? '—'}</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteVpcConnection(v)}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- ─── Kafka Versions Tab ──────────────────────────────── -->
	{#if activeTab === 'versions'}
		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Kafka Version</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each kafkaVersions as version}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-mono font-medium">{version.Version}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {stateBadge(version.Status)}">
										{version.Status ?? '—'}
									</span>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- ─── Create Cluster Modal ──────────────────────────────────── -->
{#if showCreateClusterModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create MSK Cluster</h2>
			<div class="space-y-3">
				<div>
					<label for="cluster-name" class="block text-sm font-medium mb-1">Cluster Name *</label>
					<input id="cluster-name" type="text" bind:value={newClusterName} placeholder="my-kafka-cluster"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="kafka-version" class="block text-sm font-medium mb-1">Kafka Version</label>
					<input id="kafka-version" type="text" bind:value={newKafkaVersion}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="broker-type" class="block text-sm font-medium mb-1">Broker Instance Type</label>
					<select id="broker-type" bind:value={newBrokerType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						{#each ['kafka.t3.small', 'kafka.m5.large', 'kafka.m5.xlarge', 'kafka.m5.2xlarge', 'kafka.m5.4xlarge'] as t}
							<option value={t}>{t}</option>
						{/each}
					</select>
				</div>
				<div>
					<label for="broker-count" class="block text-sm font-medium mb-1">Number of Brokers</label>
					<input id="broker-count" type="number" min={1} max={15} bind:value={newBrokerCount}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="storage-size" class="block text-sm font-medium mb-1">EBS Storage (GB)</label>
					<input id="storage-size" type="number" min={1} max={16384} bind:value={newStorageSize}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateClusterModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createCluster} disabled={creatingCluster || !newClusterName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creatingCluster ? 'Creating...' : 'Create Cluster'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- ─── Create Configuration Modal ────────────────────────────── -->
{#if showCreateConfigModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create MSK Configuration</h2>
			<div class="space-y-3">
				<div>
					<label for="config-name" class="block text-sm font-medium mb-1">Configuration Name *</label>
					<input id="config-name" type="text" bind:value={newConfigName} placeholder="my-config"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="config-props" class="block text-sm font-medium mb-1">Server Properties</label>
					<textarea id="config-props" bind:value={newConfigProperties} rows={5}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"></textarea>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateConfigModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createConfiguration} disabled={creatingConfig || !newConfigName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creatingConfig ? 'Creating...' : 'Create Configuration'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- ─── Create Replicator Modal ────────────────────────────────── -->
{#if showCreateReplicatorModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create MSK Replicator</h2>
			<div class="space-y-3">
				<div>
					<label for="replicator-name" class="block text-sm font-medium mb-1">Replicator Name *</label>
					<input id="replicator-name" type="text" bind:value={newReplicatorName} placeholder="my-replicator"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="replicator-role" class="block text-sm font-medium mb-1">Service Execution Role ARN</label>
					<input id="replicator-role" type="text" bind:value={newReplicatorRoleArn}
						placeholder="arn:aws:iam::000000000000:role/MSKRole"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateReplicatorModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createReplicator} disabled={creatingReplicator || !newReplicatorName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creatingReplicator ? 'Creating...' : 'Create Replicator'}
				</button>
			</div>
		</div>
	</div>
{/if}
