<script lang="ts">
	import { onMount } from 'svelte';
	import { ECSClient, ListClustersCommand, DescribeServicesCommand, ListServicesCommand } from '@aws-sdk/client-ecs';
	import { toast } from 'svelte-sonner';
	import { Container, Plus } from 'lucide-svelte';

	let clusters = $state<any[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let selectedCluster = $state<string | null>(null);
	let services = $state<any[]>([]);

	const ecs = new ECSClient({});

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
			error = e instanceof Error ? e.message : 'Failed to load clusters';
			toast.error(error);
		} finally {
			loading = false;
		}
	}

	async function loadServices(clusterArn: string) {
		try {
			const data = await ecs.send(new ListServicesCommand({ cluster: clusterArn }));
			services = data.serviceArns || [];
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load services';
			toast.error(error);
		}
	}

	async function selectCluster(clusterArn: string) {
		selectedCluster = clusterArn;
		await loadServices(clusterArn);
	}
</script>

<div class="space-y-6">
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
		<button onclick={() => toast.info('Create cluster feature coming soon')} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
			<Plus class="w-4 h-4" />
			Create Cluster
		</button>
	</div>

	{#if loading}
		<div class="text-center py-8 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
			<p>Loading clusters...</p>
		</div>
	{:else if error}
		<div class="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg text-red-700 dark:text-red-300">
			{error}
		</div>
	{:else if clusters.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
			<Container class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
			<p class="text-slate-600 dark:text-slate-300">No ECS clusters found</p>
		</div>
	{:else}
		<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
			<div class="lg:col-span-1 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<h2 class="font-semibold text-slate-900 dark:text-white mb-3">Clusters</h2>
				<div class="space-y-2">
					{#each clusters as cluster}
						<button onclick={() => selectCluster(cluster)} class="w-full text-left px-3 py-2 rounded transition-colors {selectedCluster === cluster ? 'bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 font-semibold' : 'hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-400'}">
							{cluster.split('/').pop() || cluster}
						</button>
					{/each}
				</div>
			</div>

			<div class="lg:col-span-2 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<h2 class="font-semibold text-slate-900 dark:text-white mb-3">Services</h2>
				{#if services.length === 0}
					<p class="text-slate-500 dark:text-slate-500 text-sm">No services in this cluster</p>
				{:else}
					<div class="space-y-2">
						{#each services as service}
							<div class="p-3 bg-slate-50 dark:bg-slate-700 rounded text-sm">
								<p class="font-semibold text-slate-900 dark:text-white">{service.split('/').pop() || service}</p>
								<p class="text-xs text-slate-500 dark:text-slate-500 font-mono">{service}</p>
							</div>
						{/each}
					</div>
				{/if}
			</div>
		</div>
	{/if}
</div>
