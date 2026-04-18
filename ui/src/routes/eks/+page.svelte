<script lang="ts">
	import { onMount } from 'svelte';
	import { getEKSClient } from '$lib/aws-client';
	import {
		ListClustersCommand,
		DescribeClusterCommand,
		CreateClusterCommand,
		DeleteClusterCommand,
		ListNodegroupsCommand,
		DescribeNodegroupCommand,
		CreateNodegroupCommand,
		DeleteNodegroupCommand,
		type Cluster,
		type Nodegroup,
		AMITypes
	} from '@aws-sdk/client-eks';
	import { toast } from 'svelte-sonner';
	import { Box, Search, RefreshCw, Plus, Trash2, Server, Layers } from 'lucide-svelte';

	const eks = getEKSClient();

	let loading = $state(false);
	let clusters = $state<string[]>([]);
	let searchQuery = $state('');
	let selectedCluster = $state<Cluster | null>(null);
	let loadingCluster = $state(false);
	let nodeGroups = $state<string[]>([]);
	let nodeGroupDetails = $state<Nodegroup[]>([]);
	let loadingNodeGroups = $state(false);
	let detailTab = $state<'overview' | 'nodegroups'>('overview');

	// Create cluster modal
	let showCreateCluster = $state(false);
	let creatingCluster = $state(false);
	let newClusterName = $state('');
	let newK8sVersion = $state('1.31');
	let newRoleArn = $state('');

	// Create nodegroup modal
	let showCreateNG = $state(false);
	let creatingNG = $state(false);
	let newNGName = $state('');
	let newNGInstanceType = $state('t3.medium');
	let newNGAmiType = $state('AL2_x86_64');
	let newNGMin = $state(1);
	let newNGMax = $state(3);
	let newNGDesired = $state(2);

	const filteredClusters = $derived(
		clusters.filter((c) => c.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	function statusColor(status: string | undefined): string {
		if (status === 'ACTIVE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		if (status === 'CREATING') return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
		if (status === 'DELETING' || status === 'FAILED') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
		return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	}

	async function loadClusters() {
		loading = true;
		try {
			const res = await eks.send(new ListClustersCommand({}));
			clusters = res.clusters ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load clusters: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectCluster(name: string) {
		loadingCluster = true;
		detailTab = 'overview';
		nodeGroups = [];
		nodeGroupDetails = [];
		try {
			const res = await eks.send(new DescribeClusterCommand({ name }));
			selectedCluster = res.cluster ?? null;
			await loadNodeGroups(name);
		} catch (err: unknown) {
			toast.error(`Failed to describe cluster: ${(err as Error).message}`);
		} finally {
			loadingCluster = false;
		}
	}

	async function loadNodeGroups(clusterName: string) {
		loadingNodeGroups = true;
		try {
			const res = await eks.send(new ListNodegroupsCommand({ clusterName }));
			nodeGroups = res.nodegroups ?? [];
			const details = await Promise.all(
				nodeGroups.slice(0, 10).map(async (ng) => {
					const d = await eks.send(new DescribeNodegroupCommand({ clusterName, nodegroupName: ng }));
					return d.nodegroup!;
				})
			);
			nodeGroupDetails = details.filter(Boolean);
		} catch (err: unknown) {
			toast.error(`Failed to load node groups: ${(err as Error).message}`);
		} finally {
			loadingNodeGroups = false;
		}
	}

	async function createCluster() {
		if (!newClusterName.trim() || !newRoleArn.trim()) return;
		creatingCluster = true;
		try {
			await eks.send(new CreateClusterCommand({
				name: newClusterName.trim(),
				version: newK8sVersion,
				roleArn: newRoleArn.trim(),
				resourcesVpcConfig: { subnetIds: [], securityGroupIds: [] }
			}));
			toast.success(`Cluster "${newClusterName.trim()}" creating`);
			showCreateCluster = false;
			newClusterName = '';
			newRoleArn = '';
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creatingCluster = false;
		}
	}

	async function deleteCluster(name: string) {
		if (!confirm(`Delete cluster "${name}"?`)) return;
		try {
			await eks.send(new DeleteClusterCommand({ name }));
			toast.success(`Cluster "${name}" deleting`);
			if (selectedCluster?.name === name) selectedCluster = null;
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function createNodeGroup() {
		if (!selectedCluster?.name || !newNGName.trim()) return;
		creatingNG = true;
		try {
			await eks.send(new CreateNodegroupCommand({
				clusterName: selectedCluster.name,
				nodegroupName: newNGName.trim(),
				instanceTypes: [newNGInstanceType],
				amiType: newNGAmiType as AMITypes,
				scalingConfig: { minSize: newNGMin, maxSize: newNGMax, desiredSize: newNGDesired },
				nodeRole: 'arn:aws:iam::123456789012:role/EKSNodeRole',
				subnets: []
			}));
			toast.success(`Node group "${newNGName.trim()}" creating`);
			showCreateNG = false;
			newNGName = '';
			await loadNodeGroups(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Create node group failed: ${(err as Error).message}`);
		} finally {
			creatingNG = false;
		}
	}

	async function deleteNodeGroup(ngName: string) {
		if (!selectedCluster?.name || !confirm(`Delete node group "${ngName}"?`)) return;
		try {
			await eks.send(new DeleteNodegroupCommand({ clusterName: selectedCluster.name, nodegroupName: ngName }));
			toast.success(`Node group "${ngName}" deleting`);
			await loadNodeGroups(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	onMount(() => { loadClusters(); });
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-cyan-100 dark:bg-cyan-900/30 rounded-lg">
				<Box class="w-6 h-6 text-cyan-600 dark:text-cyan-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">EKS Clusters</h1>
				<p class="text-slate-600 dark:text-slate-300">Elastic Kubernetes Service</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={() => loadClusters()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
			<button onclick={() => { showCreateCluster = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />Create Cluster
			</button>
		</div>
	</div>

	<!-- Search -->
	<div class="relative">
		<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
		<input type="text" bind:value={searchQuery} placeholder="Search clusters..." class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Cluster List -->
		<div class="lg:col-span-1 space-y-2">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading clusters...</p>
				</div>
			{:else if filteredClusters.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
					<Box class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">No clusters found</p>
				</div>
			{:else}
				{#each filteredClusters as cluster}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectCluster(cluster)}
						onkeypress={(e) => { if (e.key === 'Enter') selectCluster(cluster); }}
						class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-indigo-400 transition-colors cursor-pointer {selectedCluster?.name === cluster ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
					>
						<div class="flex items-center justify-between">
							<p class="font-medium text-slate-900 dark:text-white truncate">{cluster}</p>
							<button onclick={(e) => { e.stopPropagation(); deleteCluster(cluster); }} class="p-1 text-slate-400 hover:text-red-500 ml-2">
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
					</div>
				{/each}
			{/if}
		</div>

		<!-- Cluster Detail -->
		<div class="lg:col-span-2">
			{#if selectedCluster}
				<div class="space-y-4">
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
						<div class="flex items-start justify-between mb-4">
							<div>
								<h2 class="text-xl font-bold text-slate-900 dark:text-white">{selectedCluster.name}</h2>
								<span class="mt-1 inline-block px-2 py-0.5 text-xs rounded-full {statusColor(selectedCluster.status)}">{selectedCluster.status}</span>
							</div>
						</div>
						<!-- Tabs -->
						<div class="flex border-b border-slate-200 dark:border-slate-700 mb-4">
							{#each [{ id: 'overview', label: 'Overview', icon: Server }, { id: 'nodegroups', label: 'Node Groups', icon: Layers }] as tab}
								<button
									onclick={() => { detailTab = tab.id as 'overview' | 'nodegroups'; }}
									class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors {detailTab === tab.id ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400'}"
								>
									<tab.icon class="w-4 h-4" />{tab.label}
								</button>
							{/each}
						</div>

						{#if detailTab === 'overview'}
							{#if loadingCluster}
								<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
							{:else}
								<div class="grid grid-cols-2 gap-3">
									{#each [
										['K8s Version', selectedCluster.version ?? 'N/A'],
										['Endpoint', selectedCluster.endpoint ? selectedCluster.endpoint.substring(0, 30) + '...' : 'N/A'],
										['Role ARN', selectedCluster.roleArn?.split('/').pop() ?? 'N/A'],
										['Created', selectedCluster.createdAt ? new Date(selectedCluster.createdAt).toLocaleDateString() : 'N/A']
									] as [label, value]}
										<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
											<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
											<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate">{value}</p>
										</div>
									{/each}
								</div>
							{/if}
						{:else}
							<div class="space-y-3">
								<div class="flex justify-end">
									<button onclick={() => { showCreateNG = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm">
										<Plus class="w-4 h-4" />Create Node Group
									</button>
								</div>
								{#if loadingNodeGroups}
									<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
								{:else if nodeGroupDetails.length === 0}
									<p class="text-slate-500 dark:text-slate-400 text-sm text-center py-4">No node groups</p>
								{:else}
									{#each nodeGroupDetails as ng}
										<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-4 flex items-start justify-between">
											<div>
												<div class="flex items-center gap-2 mb-1">
													<p class="font-medium text-slate-900 dark:text-white">{ng.nodegroupName}</p>
													<span class="px-2 py-0.5 text-xs rounded-full {statusColor(ng.status)}">{ng.status}</span>
												</div>
												<p class="text-xs text-slate-500 dark:text-slate-400">
													{ng.instanceTypes?.join(', ')} · min {ng.scalingConfig?.minSize ?? 0} / desired {ng.scalingConfig?.desiredSize ?? 0} / max {ng.scalingConfig?.maxSize ?? 0}
												</p>
											</div>
											<button onclick={() => deleteNodeGroup(ng.nodegroupName ?? '')} class="p-1.5 text-slate-400 hover:text-red-500">
												<Trash2 class="w-4 h-4" />
											</button>
										</div>
									{/each}
								{/if}
							</div>
						{/if}
					</div>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Box class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a cluster to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Cluster Modal -->
{#if showCreateCluster}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create EKS Cluster</h2>
			<form onsubmit={(e) => { e.preventDefault(); createCluster(); }} class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Cluster Name</label>
					<input type="text" bind:value={newClusterName} placeholder="e.g. production-cluster" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Kubernetes Version</label>
					<select bind:value={newK8sVersion} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
						{#each ['1.31', '1.30', '1.29', '1.28'] as v}
							<option value={v}>{v}</option>
						{/each}
					</select>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Cluster Role ARN</label>
					<input type="text" bind:value={newRoleArn} placeholder="arn:aws:iam::123:role/EKSClusterRole" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm" required />
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateCluster = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingCluster} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingCluster ? 'Creating...' : 'Create Cluster'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Node Group Modal -->
{#if showCreateNG}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Node Group</h2>
			<form onsubmit={(e) => { e.preventDefault(); createNodeGroup(); }} class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Node Group Name</label>
					<input type="text" bind:value={newNGName} placeholder="e.g. general-workers" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Instance Type</label>
						<select bind:value={newNGInstanceType} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
							{#each ['t3.small', 't3.medium', 't3.large', 'm5.large', 'm5.xlarge', 'c5.large'] as it}
								<option value={it}>{it}</option>
							{/each}
						</select>
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">AMI Type</label>
						<select bind:value={newNGAmiType} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
							{#each ['AL2_x86_64', 'AL2_x86_64_GPU', 'AL2_ARM_64', 'BOTTLEROCKET_x86_64'] as at}
								<option value={at}>{at}</option>
							{/each}
						</select>
					</div>
				</div>
				<div class="grid grid-cols-3 gap-3">
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Min</label>
						<input type="number" bind:value={newNGMin} min="0" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Desired</label>
						<input type="number" bind:value={newNGDesired} min="0" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Max</label>
						<input type="number" bind:value={newNGMax} min="1" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateNG = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingNG} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingNG ? 'Creating...' : 'Create Node Group'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
