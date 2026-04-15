<script lang="ts">
import { onMount } from 'svelte';
import { getRDSClient } from '$lib/aws-client';
import { DescribeDBInstancesCommand, DeleteDBInstanceCommand } from '@aws-sdk/client-rds';
import { toast } from 'svelte-sonner';
import { Database, Plus, RefreshCw, Search, Trash2 } from 'lucide-svelte';

const rds = getRDSClient();

let instances = $state<any[]>([]);
let loading = $state(true);
let search = $state('');
let engineFilter = $state('all');
let showCreateModal = $state(false);

onMount(async () => { await loadInstances(); });

async function loadInstances() {
try {
loading = true;
const data = await rds.send(new DescribeDBInstancesCommand({}));
instances = data.DBInstances || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load RDS instances');
} finally {
loading = false;
}
}

async function deleteInstance(id: string) {
if (!confirm(`Delete RDS instance ${id}? This cannot be undone.`)) return;
try {
await rds.send(new DeleteDBInstanceCommand({ DBInstanceIdentifier: id, SkipFinalSnapshot: true }));
toast.success(`Instance ${id} deleted`);
await loadInstances();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete');
}
}

function getStatusColor(status: string) {
if (status === 'available') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
if (status === 'stopped') return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
}

let engines = $derived([...new Set(instances.map((i: any) => i.Engine).filter(Boolean))]);
let filtered = $derived(instances.filter((i: any) => {
const matchSearch = !search || i.DBInstanceIdentifier?.toLowerCase().includes(search.toLowerCase());
const matchEngine = engineFilter === 'all' || i.Engine === engineFilter;
return matchSearch && matchEngine;
}));
</script>

<div class="space-y-6">
<div class="flex items-center justify-between">
<div class="flex items-center gap-3">
<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
<Database class="w-6 h-6 text-blue-600 dark:text-blue-400" />
</div>
<div>
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">RDS Instances</h1>
<p class="text-slate-600 dark:text-slate-300">Relational Database Service</p>
</div>
</div>
<div class="flex gap-2">
<button onclick={loadInstances} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">
<RefreshCw class="w-4 h-4" />
</button>
<button onclick={() => showCreateModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
<Plus class="w-4 h-4" />
Create Instance
</button>
</div>
</div>

<div class="flex gap-3 flex-wrap">
<div class="relative flex-1 min-w-48">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search instances..." bind:value={search}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
<select bind:value={engineFilter} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white">
<option value="all">All Engines</option>
{#each engines as engine}<option value={engine}>{engine}</option>{/each}
</select>
</div>

{#if loading}
<div class="text-center py-12 text-slate-500"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div><p>Loading instances...</p></div>
{:else if filtered.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Database class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
<p class="text-slate-600 dark:text-slate-300">No RDS instances found</p>
<button onclick={() => showCreateModal = true} class="mt-4 px-4 py-2 bg-indigo-600 text-white rounded-lg">Create Instance</button>
</div>
{:else}
<div class="grid gap-4">
{#each filtered as instance}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
<div class="flex items-start justify-between mb-4">
<div>
<h3 class="font-semibold text-slate-900 dark:text-white text-lg">{instance.DBInstanceIdentifier}</h3>
<p class="text-sm text-slate-500 dark:text-slate-400">{instance.Engine} {instance.EngineVersion}</p>
</div>
<span class={`px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(instance.DBInstanceStatus)}`}>
{instance.DBInstanceStatus}
</span>
</div>
<div class="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm mb-4">
<div>
<p class="text-xs text-slate-500 uppercase">Class</p>
<p class="font-medium text-slate-900 dark:text-white">{instance.DBInstanceClass}</p>
</div>
<div>
<p class="text-xs text-slate-500 uppercase">Storage</p>
<p class="font-medium text-slate-900 dark:text-white">{instance.AllocatedStorage} GB</p>
</div>
<div>
<p class="text-xs text-slate-500 uppercase">Endpoint</p>
<p class="font-mono text-slate-900 dark:text-white text-xs truncate">{instance.Endpoint?.Address || '—'}</p>
</div>
<div>
<p class="text-xs text-slate-500 uppercase">Port</p>
<p class="font-medium text-slate-900 dark:text-white">{instance.Endpoint?.Port || '—'}</p>
</div>
</div>
<div class="flex gap-2">
<button onclick={() => deleteInstance(instance.DBInstanceIdentifier)} class="px-3 py-2 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center gap-1">
<Trash2 class="w-3 h-3" /> Delete
</button>
</div>
</div>
{/each}
</div>
{/if}
</div>

{#if showCreateModal}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-slate-800 rounded-xl w-full max-w-md p-6">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create RDS Instance</h2>
<p class="text-slate-500 dark:text-slate-400 text-sm mb-4">Use the AWS console or CLI to create RDS instances with full configuration options.</p>
<div class="flex justify-end">
<button onclick={() => showCreateModal = false} class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-700 dark:text-slate-300">Close</button>
</div>
</div>
</div>
{/if}
