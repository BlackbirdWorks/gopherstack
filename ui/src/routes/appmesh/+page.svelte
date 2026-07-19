<script lang="ts">
	import { onMount } from 'svelte';
	import { getAppMeshClient } from '$lib/aws-client';
	import {
		ListMeshesCommand,
		ListVirtualGatewaysCommand,
		ListVirtualNodesCommand,
		ListVirtualRoutersCommand,
		ListVirtualServicesCommand,
		type MeshRef,
		type VirtualGatewayRef,
		type VirtualNodeRef,
		type VirtualRouterRef,
		type VirtualServiceRef
	} from '@aws-sdk/client-app-mesh';
	import { toast } from 'svelte-sonner';
	import { RefreshCw, Search, Share2 } from 'lucide-svelte';

	const client = getAppMeshClient();

	type TabId = 'meshes' | 'nodes' | 'services' | 'routers' | 'gateways';

	let loading = $state(false);
	let activeTab = $state<TabId>('meshes');
	let searchQuery = $state('');
	let meshesData = $state<MeshRef[]>([]);
	let nodesData = $state<VirtualNodeRef[]>([]);
	let servicesData = $state<VirtualServiceRef[]>([]);
	let routersData = $state<VirtualRouterRef[]>([]);
	let gatewaysData = $state<VirtualGatewayRef[]>([]);
	let meshNameFilter = $state('');

	const filteredMeshes = $derived(meshesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredNodes = $derived(nodesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredServices = $derived(servicesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredRouters = $derived(routersData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredGateways = $derived(gatewaysData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadMeshesForTab() {
		const resp = await client.send(new ListMeshesCommand({}));
		meshesData = resp.meshes ?? [];
	}

	async function loadNodesForTab() {
		if (!meshNameFilter) {
			nodesData = [];
			return;
		}
		const resp = await client.send(new ListVirtualNodesCommand({ meshName: meshNameFilter }));
		nodesData = resp.virtualNodes ?? [];
	}

	async function loadServicesForTab() {
		if (!meshNameFilter) {
			servicesData = [];
			return;
		}
		const resp = await client.send(new ListVirtualServicesCommand({ meshName: meshNameFilter }));
		servicesData = resp.virtualServices ?? [];
	}

	async function loadRoutersForTab() {
		if (!meshNameFilter) {
			routersData = [];
			return;
		}
		const resp = await client.send(new ListVirtualRoutersCommand({ meshName: meshNameFilter }));
		routersData = resp.virtualRouters ?? [];
	}

	async function loadGatewaysForTab() {
		if (!meshNameFilter) {
			gatewaysData = [];
			return;
		}
		const resp = await client.send(new ListVirtualGatewaysCommand({ meshName: meshNameFilter }));
		gatewaysData = resp.virtualGateways ?? [];
	}

	const tabDataLoaders: Record<TabId, () => Promise<void>> = {
		meshes: loadMeshesForTab,
		nodes: loadNodesForTab,
		services: loadServicesForTab,
		routers: loadRoutersForTab,
		gateways: loadGatewaysForTab
	};

	async function loadData() {
		loading = true;
		try {
			await tabDataLoaders[activeTab]();
		} catch (e) {
			toast.error('Failed to load AWS App Mesh data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		loadData();
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between flex-wrap gap-3">
		<div class="flex items-center gap-3">
			<Share2 class="w-7 h-7 text-purple-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS App Mesh</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Service mesh for microservice networking</p>
			</div>
		</div>
		<div class="flex items-center gap-2 flex-wrap">
			<input bind:value={meshNameFilter} onchange={loadData} placeholder="Mesh name" class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-40" />
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<p class="text-xs text-gray-500 dark:text-gray-400">Enter a mesh name above to list its virtual nodes, services, routers and gateways.</p>
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['meshes', 'Meshes'], ['nodes', 'Virtual Nodes'], ['services', 'Virtual Services'], ['routers', 'Virtual Routers'], ['gateways', 'Virtual Gateways']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-purple-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'meshes'}
				{#if filteredMeshes.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No meshes found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredMeshes as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Share2 class="w-5 h-5 text-purple-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.meshName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Owner: ${a.resourceOwner ?? '-'} · v${a.version ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'nodes'}
				{#if filteredNodes.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No virtual nodes found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredNodes as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Share2 class="w-5 h-5 text-purple-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.virtualNodeName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Mesh: ${a.meshName ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'services'}
				{#if filteredServices.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No virtual services found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredServices as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Share2 class="w-5 h-5 text-purple-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.virtualServiceName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Mesh: ${a.meshName ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'routers'}
				{#if filteredRouters.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No virtual routers found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredRouters as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Share2 class="w-5 h-5 text-purple-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.virtualRouterName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Mesh: ${a.meshName ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'gateways'}
				{#if filteredGateways.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No virtual gateways found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredGateways as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Share2 class="w-5 h-5 text-purple-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.virtualGatewayName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Mesh: ${a.meshName ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
