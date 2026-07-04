<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { getDAXClient } from '$lib/aws-client';
	import { getStoredRegion } from '$lib/aws/client';
	import {
		DescribeClustersCommand,
		DescribeParameterGroupsCommand,
		DescribeSubnetGroupsCommand,
		CreateClusterCommand,
		DeleteClusterCommand,
		IncreaseReplicationFactorCommand,
		DecreaseReplicationFactorCommand,
		UpdateClusterCommand,
		CreateParameterGroupCommand,
		UpdateParameterGroupCommand,
		DeleteParameterGroupCommand,
		CreateSubnetGroupCommand,
		UpdateSubnetGroupCommand,
		DeleteSubnetGroupCommand,
		type Cluster,
		type ParameterGroup,
		type SubnetGroup
	} from '@aws-sdk/client-dax';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { RefreshCw, Search, Zap, Plus, Trash2 } from 'lucide-svelte';

	let currentRegion = $state(getStoredRegion());
	let client = $state(getDAXClient(currentRegion));

	const activeStatuses = new Set<string>(['ACTIVE', 'AVAILABLE', 'ENABLED', 'RUNNING', 'COMPLETE', 'COMPLETED', 'IDLE', 'Active', 'opt-in-not-required', 'ENABLED_BY_DEFAULT']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s)) ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let loading = $state(false);
	let activeTab = $state<'clusters' | 'paramgroups' | 'subnetgroups'>('clusters');
	let searchQuery = $state('');
	let clustersData = $state<Cluster[]>([]);
	let paramgroupsData = $state<ParameterGroup[]>([]);
	let subnetgroupsData = $state<SubnetGroup[]>([]);

	const filteredClusters = $derived(clustersData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredParamgroups = $derived(paramgroupsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredSubnetgroups = $derived(subnetgroupsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			if (activeTab === 'clusters') {
				const resp = await client.send(new DescribeClustersCommand({}));
				clustersData = resp.Clusters ?? [];
			}
			if (activeTab === 'paramgroups') {
				const resp = await client.send(new DescribeParameterGroupsCommand({}));
				paramgroupsData = resp.ParameterGroups ?? [];
			}
			if (activeTab === 'subnetgroups') {
				const resp = await client.send(new DescribeSubnetGroupsCommand({}));
				subnetgroupsData = resp.SubnetGroups ?? [];
			}
		} catch (e) {
			toast.error('Failed to load DAX data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		loadData();
	}

	function handleRegionChange(e: Event) {
		const ce = e as CustomEvent<string>;
		currentRegion = ce.detail;
		client = getDAXClient(currentRegion);
		loadData();
	}

	onMount(() => {
		loadData();
		window.addEventListener('gopherstack:region-change', handleRegionChange);
	});

	onDestroy(() => {
		if (typeof window !== 'undefined') {
			window.removeEventListener('gopherstack:region-change', handleRegionChange);
		}
	});

	async function createCluster() {
		// eslint-disable-next-line no-alert
		const name = prompt("Cluster Name:");
		if (!name) return;
		try {
			await client.send(new CreateClusterCommand({
				ClusterName: name,
				NodeType: "dax.r4.large",
				ReplicationFactor: 1,
				IamRoleArn: "arn:aws:iam::000000000000:role/dax-role"
			}));
			toast.success("Cluster created");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}
	async function deleteCluster(name: string) {
		if (!await confirmDestructive({ title: 'Delete Cluster', message: `Delete ${name}?` })) return;
		try {
			await client.send(new DeleteClusterCommand({ ClusterName: name }));
			toast.success("Cluster deleted");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}
	async function updateCluster(name: string) {
		// eslint-disable-next-line no-alert
		const desc = prompt("New Description:");
		if (desc === null) return;
		try {
			await client.send(new UpdateClusterCommand({ ClusterName: name, Description: desc }));
			toast.success("Cluster updated");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}
	async function increaseRep(name: string) {
		try {
			const c = clustersData.find(x => x.ClusterName === name);
			if (!c) return;
			await client.send(new IncreaseReplicationFactorCommand({ ClusterName: name, NewReplicationFactor: (c.TotalNodes ?? 1) + 1 }));
			toast.success("Replication factor increased");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}
	async function decreaseRep(name: string) {
		try {
			const c = clustersData.find(x => x.ClusterName === name);
			if (!c) return;
			const n = (c.TotalNodes ?? 1) - 1;
			if (n < 1) return toast.error("Cannot decrease below 1");
			await client.send(new DecreaseReplicationFactorCommand({ ClusterName: name, NewReplicationFactor: n }));
			toast.success("Replication factor decreased");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}

	async function createParamGroup() {
		// eslint-disable-next-line no-alert
		const name = prompt("Parameter Group Name:");
		if (!name) return;
		try {
			await client.send(new CreateParameterGroupCommand({ ParameterGroupName: name }));
			toast.success("Parameter Group created");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}
	async function deleteParamGroup(name: string) {
		if (!await confirmDestructive({ title: 'Delete Parameter Group', message: `Delete ${name}?` })) return;
		try {
			await client.send(new DeleteParameterGroupCommand({ ParameterGroupName: name }));
			toast.success("Parameter Group deleted");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}
	async function updateParamGroup(name: string) {
		// eslint-disable-next-line no-alert
		const pName = prompt("Parameter Name to update:");
		if (!pName) return;
		// eslint-disable-next-line no-alert
		const pValue = prompt("Parameter Value:");
		if (!pValue) return;
		try {
			await client.send(new UpdateParameterGroupCommand({
				ParameterGroupName: name,
				ParameterNameValues: [{ ParameterName: pName, ParameterValue: pValue }]
			}));
			toast.success("Parameter Group updated");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}

	async function createSubnetGroup() {
		// eslint-disable-next-line no-alert
		const name = prompt("Subnet Group Name:");
		if (!name) return;
		try {
			await client.send(new CreateSubnetGroupCommand({ SubnetGroupName: name, SubnetIds: ["subnet-12345"] }));
			toast.success("Subnet Group created");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}
	async function deleteSubnetGroup(name: string) {
		if (!await confirmDestructive({ title: 'Delete Subnet Group', message: `Delete ${name}?` })) return;
		try {
			await client.send(new DeleteSubnetGroupCommand({ SubnetGroupName: name }));
			toast.success("Subnet Group deleted");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}
	async function updateSubnetGroup(name: string) {
		// eslint-disable-next-line no-alert
		const desc = prompt("New Description:");
		if (desc === null) return;
		try {
			await client.send(new UpdateSubnetGroupCommand({ SubnetGroupName: name, Description: desc }));
			toast.success("Subnet Group updated");
			loadData();
		} catch (e) {
			toast.error(String(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between flex-wrap gap-3">
		<div class="flex items-center gap-3">
			<Zap class="w-7 h-7 text-indigo-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon DynamoDB Accelerator (DAX)</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">In-memory cache for DynamoDB</p>
			</div>
		</div>
		<div class="flex items-center gap-2 flex-wrap">
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			{#if activeTab === 'clusters'}
				<button onclick={createCluster} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"><Plus class="w-4 h-4"/> Create cluster</button>
			{:else if activeTab === 'paramgroups'}
				<button onclick={createParamGroup} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"><Plus class="w-4 h-4"/> Create param group</button>
			{:else if activeTab === 'subnetgroups'}
				<button onclick={createSubnetGroup} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"><Plus class="w-4 h-4"/> Create subnet group</button>
			{/if}
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['clusters', 'Clusters'], ['paramgroups', 'Parameter Groups'], ['subnetgroups', 'Subnet Groups']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-indigo-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'clusters'}
				{#if filteredClusters.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No clusters found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredClusters as a}
							<div class="flex flex-col sm:flex-row items-start sm:items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 gap-4">
								<div class="flex items-center gap-3 min-w-0">
									<Zap class="w-5 h-5 text-indigo-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.ClusterName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.NodeType ?? '-'} · ${a.TotalNodes ?? 0} nodes`}</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									{#if a.Status}
										<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.Status)}">{a.Status}</span>
									{/if}
									<button onclick={() => decreaseRep(a.ClusterName!)} class="text-xs px-2 py-1 rounded border hover:bg-gray-200 dark:hover:bg-slate-600">- Rep</button>
									<button onclick={() => increaseRep(a.ClusterName!)} class="text-xs px-2 py-1 rounded border hover:bg-gray-200 dark:hover:bg-slate-600">+ Rep</button>
									<button onclick={() => updateCluster(a.ClusterName!)} class="text-xs px-2 py-1 rounded border hover:bg-gray-200 dark:hover:bg-slate-600">Edit</button>
									<button onclick={() => deleteCluster(a.ClusterName!)} class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4"/></button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'paramgroups'}
				{#if filteredParamgroups.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No parameter groups found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredParamgroups as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Zap class="w-5 h-5 text-indigo-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.ParameterGroupName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.Description ?? ''}`}</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									<button onclick={() => updateParamGroup(a.ParameterGroupName!)} class="text-xs px-2 py-1 rounded border hover:bg-gray-200 dark:hover:bg-slate-600">Edit Params</button>
									<button onclick={() => deleteParamGroup(a.ParameterGroupName!)} class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4"/></button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'subnetgroups'}
				{#if filteredSubnetgroups.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No subnet groups found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredSubnetgroups as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Zap class="w-5 h-5 text-indigo-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.SubnetGroupName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`VPC: ${a.VpcId ?? '-'}`}</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									<button onclick={() => updateSubnetGroup(a.SubnetGroupName!)} class="text-xs px-2 py-1 rounded border hover:bg-gray-200 dark:hover:bg-slate-600">Edit</button>
									<button onclick={() => deleteSubnetGroup(a.SubnetGroupName!)} class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4"/></button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
