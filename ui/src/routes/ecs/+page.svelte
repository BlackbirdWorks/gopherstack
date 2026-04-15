<script lang="ts">
import { onMount } from 'svelte';
import { getECSClient } from '$lib/aws-client';
import { ListClustersCommand, ListServicesCommand } from '@aws-sdk/client-ecs';
import { toast } from 'svelte-sonner';
import { Container, Plus, RefreshCw, Search, Layers, Activity, Server, ChevronRight } from 'lucide-svelte';

const ecs = getECSClient();

let clusters = $state<string[]>([]);
let loading = $state(true);
let selectedCluster = $state<string | null>(null);
let services = $state<string[]>([]);
let servicesLoading = $state(false);
let search = $state('');
let servicesByCluster = $state<Record<string, string[]>>({});

onMount(async () => {
	await loadClusters();
});

async function loadClusters() {
	try {
		loading = true;
		const data = await ecs.send(new ListClustersCommand({}));
		clusters = data.clusterArns || [];
		if (clusters.length > 0) {
			selectedCluster = clusters[0];
			await loadServices(clusters[0]);
		}
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load clusters');
	} finally {
		loading = false;
	}
}

async function loadServices(clusterArn: string) {
	try {
		servicesLoading = true;
		const data = await ecs.send(new ListServicesCommand({ cluster: clusterArn }));
		const arns = data.serviceArns || [];
		services = arns;
		servicesByCluster = { ...servicesByCluster, [clusterArn]: arns };
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load services');
	} finally {
		servicesLoading = false;
	}
}

async function selectCluster(arn: string) {
	selectedCluster = arn;
	await loadServices(arn);
}

function clusterName(arn: string) {
	return arn.split('/').pop() || arn;
}

function serviceName(arn: string) {
	return arn.split('/').pop() || arn;
}

function clusterRegion(arn: string) {
	const parts = arn.split(':');
	return parts[3] || '—';
}

function clusterAccount(arn: string) {
	const parts = arn.split(':');
	return parts[4] || '—';
}

let filteredServices = $derived(services.filter(s =>
	!search || serviceName(s).toLowerCase().includes(search.toLowerCase())
));

let totalServices = $derived(Object.values(servicesByCluster).reduce((acc, s) => acc + s.length, 0));
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Container class="w-6 h-6 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">ECS Clusters</h1>
				<p class="text-slate-600 dark:text-slate-300">Elastic Container Service</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button
				onclick={loadClusters}
				class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300"
				title="Refresh"
			>
				<RefreshCw class="w-4 h-4" />
			</button>
			<button
				onclick={() => toast.info('Use the AWS console or CLI to create ECS clusters.')}
				class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"
			>
				<Plus class="w-4 h-4" />
				Create Cluster
			</button>
		</div>
	</div>

	<!-- Stats cards (shown when clusters are loaded) -->
	{#if !loading && clusters.length > 0}
		<div class="grid grid-cols-1 sm:grid-cols-3 gap-4">
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
				<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
					<Layers class="w-5 h-5 text-purple-600 dark:text-purple-400" />
				</div>
				<div>
					<p class="text-2xl font-bold text-slate-900 dark:text-white">{clusters.length}</p>
					<p class="text-sm text-slate-500 dark:text-slate-400">{clusters.length === 1 ? 'Cluster' : 'Clusters'}</p>
				</div>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
				<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
					<Activity class="w-5 h-5 text-green-600 dark:text-green-400" />
				</div>
				<div>
					<p class="text-2xl font-bold text-slate-900 dark:text-white">{services.length}</p>
					<p class="text-sm text-slate-500 dark:text-slate-400">Services (selected cluster)</p>
				</div>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
				<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
					<Server class="w-5 h-5 text-blue-600 dark:text-blue-400" />
				</div>
				<div>
					<p class="text-2xl font-bold text-slate-900 dark:text-white">{totalServices}</p>
					<p class="text-sm text-slate-500 dark:text-slate-400">Services (all clusters)</p>
				</div>
			</div>
		</div>
	{/if}

	{#if loading}
		<div class="text-center py-12 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
			<p>Loading clusters...</p>
		</div>
	{:else if clusters.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
			<Container class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
			<p class="text-slate-600 dark:text-slate-300 text-lg mb-1">No ECS clusters found</p>
			<p class="text-slate-400 dark:text-slate-500 text-sm mb-4">Create a cluster to start deploying containers.</p>
			<button
				onclick={() => toast.info('Use the AWS console or CLI to create ECS clusters.')}
				class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700"
			>
				Create Cluster
			</button>
		</div>
	{:else}
		<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
			<!-- Cluster list panel -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<h2 class="font-semibold text-slate-900 dark:text-white mb-3 flex items-center gap-2">
					<Layers class="w-4 h-4 text-purple-500" />
					Clusters ({clusters.length})
				</h2>
				<div class="space-y-1">
					{#each clusters as cluster}
						{@const name = clusterName(cluster)}
						{@const region = clusterRegion(cluster)}
						{@const account = clusterAccount(cluster)}
						{@const clusterServices = servicesByCluster[cluster] ?? []}
						<button
							onclick={() => selectCluster(cluster)}
							class="w-full text-left px-3 py-3 rounded-lg transition-colors {selectedCluster === cluster
								? 'bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 ring-1 ring-indigo-300 dark:ring-indigo-700'
								: 'hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300'}"
						>
							<div class="flex items-center justify-between">
								<p class="font-semibold truncate">{name}</p>
								<ChevronRight class="w-4 h-4 shrink-0 opacity-50" />
							</div>
							<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{region} · {account}</p>
							{#if clusterServices.length > 0}
								<p class="text-xs text-green-600 dark:text-green-400 mt-0.5">
									{clusterServices.length} {clusterServices.length === 1 ? 'service' : 'services'}
								</p>
							{/if}
							<p class="text-xs text-slate-400 dark:text-slate-500 font-mono mt-1 truncate">{cluster}</p>
						</button>
					{/each}
				</div>
			</div>

			<!-- Services panel -->
			<div class="lg:col-span-2 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<div class="flex items-center justify-between mb-3">
					<h2 class="font-semibold text-slate-900 dark:text-white flex items-center gap-2">
						<Activity class="w-4 h-4 text-green-500" />
						Services{selectedCluster ? ` — ${clusterName(selectedCluster)}` : ''}
					</h2>
					<div class="relative">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input
							type="text"
							placeholder="Search services..."
							bind:value={search}
							class="pl-9 pr-4 py-1.5 text-sm border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
						/>
					</div>
				</div>

				{#if servicesLoading}
					<div class="text-center py-8 text-slate-500 dark:text-slate-400 text-sm">
						<div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500 mb-2"></div>
						<p>Loading services...</p>
					</div>
				{:else if filteredServices.length === 0}
					<div class="text-center py-10">
						<Activity class="w-10 h-10 mx-auto text-slate-300 dark:text-slate-600 mb-2 opacity-50" />
						<p class="text-slate-500 dark:text-slate-400 text-sm">No services found in this cluster</p>
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredServices as service}
							{@const name = serviceName(service)}
							{@const region = clusterRegion(service)}
							<div class="p-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg border border-slate-200 dark:border-slate-600 hover:border-indigo-300 dark:hover:border-indigo-700 transition-colors">
								<div class="flex items-center justify-between">
									<p class="font-semibold text-slate-900 dark:text-white text-sm">{name}</p>
									<span class="text-xs px-2 py-0.5 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300 font-medium">
										Active
									</span>
								</div>
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{region}</p>
								<p class="text-xs text-slate-400 dark:text-slate-500 font-mono mt-1 truncate">{service}</p>
							</div>
						{/each}
					</div>
					<p class="text-xs text-slate-400 dark:text-slate-500 mt-3 text-right">
						{filteredServices.length} {filteredServices.length === 1 ? 'service' : 'services'}
					</p>
				{/if}
			</div>
		</div>
	{/if}
</div>
