<script lang="ts">
import { onMount } from 'svelte';
import { getEC2Client } from '$lib/aws-client';
import {
DescribeInstancesCommand,
StartInstancesCommand,
StopInstancesCommand,
TerminateInstancesCommand,
RebootInstancesCommand
} from '@aws-sdk/client-ec2';
import { toast } from 'svelte-sonner';
import { Cpu, Play, Square, Trash2, RefreshCw, Plus, Search, RotateCcw } from 'lucide-svelte';

const ec2 = getEC2Client();

let instances = $state<any[]>([]);
let loading = $state(true);
let search = $state('');
let stateFilter = $state('all');
let showLaunchModal = $state(false);
let selectedInstance = $state<any | null>(null);
let newInstanceType = $state('t3.micro');
let newInstanceAmi = $state('ami-0c55b159cbfafe1f0');
let newInstanceName = $state('');

const instanceTypes = ['t3.micro', 't3.small', 't3.medium', 't3.large', 'm5.large', 'c5.large', 'r5.large'];

onMount(async () => {
await loadInstances();
});

async function loadInstances() {
try {
loading = true;
const data = await ec2.send(new DescribeInstancesCommand({}));
instances = data.Reservations?.flatMap((r: any) => r.Instances || []) || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load instances');
} finally {
loading = false;
}
}

function getName(instance: any): string {
return instance.Tags?.find((t: any) => t.Key === 'Name')?.Value || instance.InstanceId;
}

function getStatusColor(state: string) {
const map: Record<string, string> = {
running: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300',
stopped: 'bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300',
stopping: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300',
pending: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300',
terminated: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300'
};
return map[state] || 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300';
}

async function startInstance(id: string) {
try {
await ec2.send(new StartInstancesCommand({ InstanceIds: [id] }));
toast.success(`Instance ${id} starting...`);
await loadInstances();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to start');
}
}

async function stopInstance(id: string) {
try {
await ec2.send(new StopInstancesCommand({ InstanceIds: [id] }));
toast.success(`Instance ${id} stopping...`);
await loadInstances();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to stop');
}
}

async function rebootInstance(id: string) {
try {
await ec2.send(new RebootInstancesCommand({ InstanceIds: [id] }));
toast.success(`Instance ${id} rebooting...`);
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to reboot');
}
}

async function terminateInstance(id: string) {
if (!confirm(`Terminate ${id}? This cannot be undone.`)) return;
try {
await ec2.send(new TerminateInstancesCommand({ InstanceIds: [id] }));
toast.success(`Instance ${id} terminated`);
await loadInstances();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to terminate');
}
}

let filtered = $derived(instances.filter(i => {
const name = getName(i).toLowerCase();
const id = (i.InstanceId || '').toLowerCase();
const matchSearch = !search || name.includes(search.toLowerCase()) || id.includes(search.toLowerCase());
const matchState = stateFilter === 'all' || i.State?.Name === stateFilter;
return matchSearch && matchState;
}));

let runningCount = $derived(instances.filter(i => i.State?.Name === 'running').length);
let stoppedCount = $derived(instances.filter(i => i.State?.Name === 'stopped').length);
</script>

<div class="space-y-6">
<div class="flex items-center justify-between">
<div class="flex items-center gap-3">
<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
<Cpu class="w-6 h-6 text-orange-600 dark:text-orange-400" />
</div>
<div>
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">EC2 Instances</h1>
<p class="text-slate-600 dark:text-slate-300">Elastic Compute Cloud</p>
</div>
</div>
<div class="flex gap-2">
<button onclick={loadInstances} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300">
<RefreshCw class="w-4 h-4" />
</button>
<button onclick={() => showLaunchModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
<Plus class="w-4 h-4" />
Launch Instance
</button>
</div>
</div>

{#if !loading && instances.length > 0}
<div class="grid grid-cols-3 gap-4">
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 text-center">
<p class="text-2xl font-bold text-slate-900 dark:text-white">{instances.length} instances total</p>
<p class="text-sm text-slate-600 dark:text-slate-400">Total</p>
</div>
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 text-center">
<p class="text-2xl font-bold text-green-600">{runningCount}</p>
<p class="text-sm text-slate-600 dark:text-slate-400">Running</p>
</div>
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 text-center">
<p class="text-2xl font-bold text-slate-500">{stoppedCount}</p>
<p class="text-sm text-slate-600 dark:text-slate-400">Stopped</p>
</div>
</div>
{/if}

<div class="flex gap-3 flex-wrap">
<div class="relative flex-1 min-w-48">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input
type="text"
placeholder="Search instances..."
bind:value={search}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white"
/>
</div>
<select bind:value={stateFilter} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white">
<option value="all">All States</option>
<option value="running">Running</option>
<option value="stopped">Stopped</option>
<option value="pending">Pending</option>
<option value="terminated">Terminated</option>
</select>
</div>

{#if loading}
<div class="text-center py-12 text-slate-500 dark:text-slate-400">
<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
<p>Loading instances...</p>
</div>
{:else if filtered.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Cpu class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
<p class="text-slate-600 dark:text-slate-300 mb-4">No EC2 instances found</p>
<button onclick={() => showLaunchModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg">Launch Instance</button>
</div>
{:else}
<div class="grid gap-4">
{#each filtered as instance}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
<div class="flex items-start justify-between mb-4">
<div>
<h3 class="font-semibold text-slate-900 dark:text-white text-lg">{getName(instance)}</h3>
<p class="text-sm text-slate-500 dark:text-slate-400 font-mono">{instance.InstanceId}</p>
</div>
<span class={`px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(instance.State?.Name)}`}>
{instance.State?.Name || 'unknown'}
</span>
</div>
<div class="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4 text-sm">
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Type</p>
<p class="font-medium text-slate-900 dark:text-white">{instance.InstanceType || 'N/A'}</p>
</div>
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Public IP</p>
<p class="font-mono text-slate-900 dark:text-white">{instance.PublicIpAddress || '—'}</p>
</div>
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Private IP</p>
<p class="font-mono text-slate-900 dark:text-white">{instance.PrivateIpAddress || '—'}</p>
</div>
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">VPC</p>
<p class="font-mono text-slate-900 dark:text-white text-xs">{instance.VpcId || '—'}</p>
</div>
</div>
<div class="flex gap-2 flex-wrap">
{#if instance.State?.Name === 'stopped'}
<button onclick={() => startInstance(instance.InstanceId)} class="px-3 py-2 bg-green-600 text-white rounded text-sm hover:bg-green-700 flex items-center gap-1">
<Play class="w-3 h-3" /> Start
</button>
{:else if instance.State?.Name === 'running'}
<button onclick={() => stopInstance(instance.InstanceId)} class="px-3 py-2 bg-yellow-600 text-white rounded text-sm hover:bg-yellow-700 flex items-center gap-1">
<Square class="w-3 h-3" /> Stop
</button>
<button onclick={() => rebootInstance(instance.InstanceId)} class="px-3 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 flex items-center gap-1">
<RotateCcw class="w-3 h-3" /> Reboot
</button>
{/if}
<button onclick={() => selectedInstance = instance} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">
Details
</button>
{#if instance.State?.Name !== 'terminated'}
<button onclick={() => terminateInstance(instance.InstanceId)} class="ml-auto px-3 py-2 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center gap-1">
<Trash2 class="w-3 h-3" /> Terminate
</button>
{/if}
</div>
</div>
{/each}
</div>
{/if}
</div>

{#if showLaunchModal}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-md p-6">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Launch EC2 Instance</h2>
<div class="space-y-4">
<div>
<label for="launch-instance-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Instance Name</label>
<input id="launch-instance-name" type="text" bind:value={newInstanceName} placeholder="e.g. web-server-01"
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
</div>
<div>
<label for="launch-instance-ami" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">AMI ID</label>
<input id="launch-instance-ami" type="text" bind:value={newInstanceAmi} placeholder="ami-..."
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
</div>
<div>
<label for="launch-instance-type" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Instance Type</label>
<select id="launch-instance-type" bind:value={newInstanceType} class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white">
{#each instanceTypes as t}<option value={t}>{t}</option>{/each}
</select>
</div>
</div>
<div class="flex justify-end gap-3 mt-6">
<button onclick={() => showLaunchModal = false} class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-700 dark:text-slate-300">Cancel</button>
<button onclick={() => { toast.info('Launch via AWS console or CLI'); showLaunchModal = false; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700">Launch</button>
</div>
</div>
</div>
{/if}

{#if selectedInstance}
<div class="fixed inset-0 bg-black/50 z-50 flex items-end justify-end">
<div class="bg-white dark:bg-slate-800 w-full max-w-lg h-full overflow-y-auto p-6">
<div class="flex items-center justify-between mb-6">
<h2 class="text-xl font-bold text-slate-900 dark:text-white">{getName(selectedInstance)}</h2>
<button onclick={() => selectedInstance = null} class="text-slate-400 hover:text-slate-600 text-2xl">&times;</button>
</div>
<div class="grid grid-cols-2 gap-4 text-sm">
{#each [['Instance ID', selectedInstance.InstanceId], ['Type', selectedInstance.InstanceType], ['State', selectedInstance.State?.Name], ['Public IP', selectedInstance.PublicIpAddress || '—'], ['Private IP', selectedInstance.PrivateIpAddress || '—'], ['VPC', selectedInstance.VpcId || '—'], ['Subnet', selectedInstance.SubnetId || '—'], ['AMI', selectedInstance.ImageId || '—']] as [label, val]}
<div>
<p class="text-slate-500 text-xs uppercase">{label}</p>
<p class="font-mono text-slate-900 dark:text-white text-xs break-all">{val}</p>
</div>
{/each}
</div>
</div>
</div>
{/if}
