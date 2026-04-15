<script lang="ts">
import { onMount } from 'svelte';
import { getECSClient } from '$lib/aws-client';
import { ListClustersCommand, ListServicesCommand } from '@aws-sdk/client-ecs';
import { toast } from 'svelte-sonner';
import { Container, Plus, RefreshCw, Search } from 'lucide-svelte';

const ecs = getECSClient();

let clusters = $state<string[]>([]);
let loading = $state(true);
let selectedCluster = $state<string | null>(null);
let services = $state<string[]>([]);
let servicesLoading = $state(false);
let search = $state('');

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
services = data.serviceArns || [];
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

let filteredServices = $derived(services.filter(s =>
!search || serviceName(s).toLowerCase().includes(search.toLowerCase())
));
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
<div class="flex gap-2">
<button onclick={loadClusters} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300">
<RefreshCw class="w-4 h-4" />
</button>
<button onclick={() => toast.info('Use AWS console to create clusters')} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
<Plus class="w-4 h-4" />
Create Cluster
</button>
</div>
</div>

{#if loading}
<div class="text-center py-12 text-slate-500 dark:text-slate-400">
<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
<p>Loading clusters...</p>
</div>
{:else if clusters.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Container class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
<p class="text-slate-600 dark:text-slate-300">No ECS clusters found</p>
</div>
{:else}
<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
<h2 class="font-semibold text-slate-900 dark:text-white mb-3">Clusters ({clusters.length})</h2>
<div class="space-y-1">
{#each clusters as cluster}
<button
onclick={() => selectCluster(cluster)}
class="w-full text-left px-3 py-2 rounded transition-colors {selectedCluster === cluster
? 'bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 font-semibold'
: 'hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300'}"
>
<p class="font-medium">{clusterName(cluster)}</p>
<p class="text-xs text-slate-500 dark:text-slate-400 truncate">{cluster}</p>
</button>
{/each}
</div>
</div>

<div class="lg:col-span-2 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
<div class="flex items-center justify-between mb-3">
<h2 class="font-semibold text-slate-900 dark:text-white">
Services {selectedCluster ? `— ${clusterName(selectedCluster)}` : ''}
</h2>
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input
type="text"
placeholder="Search services..."
bind:value={search}
class="pl-9 pr-4 py-1.5 text-sm border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white"
/>
</div>
</div>
{#if servicesLoading}
<div class="text-center py-8 text-slate-500 dark:text-slate-400 text-sm">Loading services...</div>
{:else if filteredServices.length === 0}
<p class="text-slate-500 dark:text-slate-500 text-sm py-4 text-center">No services found in this cluster</p>
{:else}
<div class="space-y-2">
{#each filteredServices as service}
<div class="p-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg border border-slate-200 dark:border-slate-600">
<p class="font-semibold text-slate-900 dark:text-white text-sm">{serviceName(service)}</p>
<p class="text-xs text-slate-500 dark:text-slate-400 font-mono mt-0.5 truncate">{service}</p>
</div>
{/each}
</div>
{/if}
</div>
</div>
{/if}
</div>
