<script lang="ts">
	import { onMount } from 'svelte';
	import { getMSKClient } from '$lib/aws-client';
	import {
		ListClustersV2Command,
		DescribeClusterV2Command,
		CreateClusterV2Command,
		DeleteClusterCommand,
		ListNodesCommand,
		ListKafkaVersionsCommand,
		type Cluster,
		type NodeInfo
	} from '@aws-sdk/client-kafka';
	import { toast } from 'svelte-sonner';
	import {
		Database,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Eye,
		Server
	} from 'lucide-svelte';

	const msk = getMSKClient();

	let loading = $state(false);
	let activeTab = $state<'clusters' | 'versions'>('clusters');
	let searchQuery = $state('');

	// Clusters
	let clusters = $state<Cluster[]>([]);
	let selectedCluster = $state<Cluster | null>(null);
	let clusterNodes = $state<NodeInfo[]>([]);
	let loadingNodes = $state(false);
	let showCreateModal = $state(false);
	let creating = $state(false);

	// Form
	let newClusterName = $state('');
	let newBrokerCount = $state(3);
	let newBrokerType = $state('kafka.m5.large');
	let newKafkaVersion = $state('2.8.1');
	let newStorageSize = $state(100);

	// Versions
	let kafkaVersions = $state<Array<{ Version?: string; Status?: string }>>([]);

	const filteredClusters = $derived(
		clusters.filter(
			(c) =>
				(c.ActiveOperationArn?.toLowerCase().includes(searchQuery.toLowerCase())) ||
				(c.ClusterName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function clusterStateBadge(state?: string) {
		if (state === 'ACTIVE') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (state === 'CREATING') return 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-900';
		if (state === 'DELETING') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		if (state === 'FAILED') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		return 'text-muted-foreground bg-muted';
	}

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

	async function viewCluster(cluster: Cluster) {
		if (!cluster.ClusterArn) return;
		selectedCluster = cluster;
		loadingNodes = true;
		clusterNodes = [];
		try {
			const nodesRes = await msk.send(
				new ListNodesCommand({ ClusterArn: cluster.ClusterArn, MaxResults: 100 })
			);
			clusterNodes = nodesRes.NodeInfoList ?? [];
		} catch (e) {
			toast.error(`Failed to load nodes: ${e}`);
		} finally {
			loadingNodes = false;
		}
	}

	async function deleteCluster(cluster: Cluster) {
		if (!cluster.ClusterArn || !confirm(`Delete cluster "${cluster.ClusterName}"?`)) return;
		try {
			await msk.send(
				new DeleteClusterCommand({
					ClusterArn: cluster.ClusterArn,
					CurrentVersion: cluster.CurrentVersion
				})
			);
			toast.success(`Cluster "${cluster.ClusterName}" deletion initiated`);
			await loadClusters();
		} catch (e) {
			toast.error(`Failed to delete cluster: ${e}`);
		}
	}

	async function createCluster() {
		if (!newClusterName.trim()) return;
		creating = true;
		try {
			await msk.send(
				new CreateClusterV2Command({
					ClusterName: newClusterName.trim(),
					Provisioned: {
						BrokerNodeGroupInfo: {
							InstanceType: newBrokerType,
							ClientSubnets: [],
							BrokerAZDistribution: 'DEFAULT',
							StorageInfo: {
								EbsStorageInfo: { VolumeSize: newStorageSize }
							}
						},
						NumberOfBrokerNodes: newBrokerCount,
						KafkaVersion: newKafkaVersion
					}
				})
			);
			toast.success(`Cluster "${newClusterName}" creation initiated`);
			showCreateModal = false;
			newClusterName = '';
			await loadClusters();
		} catch (e) {
			toast.error(`Failed to create cluster: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'clusters') await loadClusters();
		else await loadVersions();
	}

	onMount(() => loadClusters());
</script>

<div class="space-y-6">
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
		{#each [{ id: 'clusters', label: 'Clusters' }, { id: 'versions', label: 'Kafka Versions' }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Clusters Tab -->
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
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Cluster
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
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
							<tr class="hover:bg-muted/30 cursor-pointer" onclick={() => viewCluster(cluster)}>
								<td class="px-4 py-3 font-medium">{cluster.ClusterName}</td>
								<td class="px-4 py-3 text-muted-foreground capitalize">
									{cluster.ClusterType ?? '—'}
								</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {clusterStateBadge(cluster.State)}">
										{cluster.State ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground">
									{cluster.CurrentVersion ?? '—'}
								</td>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={(e) => { e.stopPropagation(); viewCluster(cluster); }}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View nodes"
									>
										<Eye class="h-4 w-4" />
									</button>
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

			<!-- Cluster Nodes Panel -->
			{#if selectedCluster}
				<div class="rounded-lg border p-4 space-y-3">
					<div class="flex items-center justify-between">
						<h3 class="font-semibold flex items-center gap-2">
							<Server class="h-5 w-5" />
							{selectedCluster.ClusterName} — Broker Nodes
						</h3>
						<button onclick={() => (selectedCluster = null)} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>
					{#if loadingNodes}
						<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
					{:else if clusterNodes.length === 0}
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
			{/if}
		{/if}
	{/if}

	<!-- Versions Tab -->
	{#if activeTab === 'versions'}
		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
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
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {version.Status === 'ACTIVE' ? 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900' : 'text-muted-foreground bg-muted'}">
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

<!-- Create Cluster Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create MSK Cluster</h2>
			<div class="space-y-3">
				<div>
					<label for="cluster-name" class="block text-sm font-medium mb-1">Cluster Name *</label>
					<input
						id="cluster-name"
						type="text"
						bind:value={newClusterName}
						placeholder="my-kafka-cluster"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="kafka-version" class="block text-sm font-medium mb-1">Kafka Version</label>
					<input
						id="kafka-version"
						type="text"
						bind:value={newKafkaVersion}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="broker-type" class="block text-sm font-medium mb-1">Broker Instance Type</label>
					<select
						id="broker-type"
						bind:value={newBrokerType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						{#each ['kafka.t3.small', 'kafka.m5.large', 'kafka.m5.xlarge', 'kafka.m5.2xlarge', 'kafka.m5.4xlarge'] as t}
							<option value={t}>{t}</option>
						{/each}
					</select>
				</div>
				<div>
					<label for="broker-count" class="block text-sm font-medium mb-1">Number of Brokers</label>
					<input
						id="broker-count"
						type="number"
						min={1}
						max={15}
						bind:value={newBrokerCount}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="storage-size" class="block text-sm font-medium mb-1">EBS Storage (GB)</label>
					<input
						id="storage-size"
						type="number"
						min={1}
						max={16384}
						bind:value={newStorageSize}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createCluster}
					disabled={creating || !newClusterName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create Cluster'}
				</button>
			</div>
		</div>
	</div>
{/if}
