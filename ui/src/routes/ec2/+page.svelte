<script lang="ts">
import { onMount } from 'svelte';
import { getEC2Client } from '$lib/aws-client';
import {
	DescribeInstancesCommand,
	StartInstancesCommand,
	StopInstancesCommand,
	TerminateInstancesCommand,
	RebootInstancesCommand,
	DescribeSecurityGroupsCommand,
	DescribeKeyPairsCommand,
	type SecurityGroup,
	type KeyPairInfo
} from '@aws-sdk/client-ec2';
import { toast } from 'svelte-sonner';
import { Cpu, Play, Square, Trash2, RefreshCw, Plus, Search, RotateCcw, Shield, Key } from 'lucide-svelte';

const ec2 = getEC2Client();

type EC2Instance = {
	ImageId?: string;
	InstanceId?: string;
	InstanceType?: string;
	PrivateIpAddress?: string;
	PublicIpAddress?: string;
	State?: { Name?: string };
	SubnetId?: string;
	Tags?: Array<{ Key?: string; Value?: string }>;
	VpcId?: string;
};

let instances = $state<EC2Instance[]>([]);
let loading = $state(true);
let search = $state('');
let stateFilter = $state('all');
let showLaunchModal = $state(false);
let selectedInstance = $state<EC2Instance | null>(null);
let newInstanceType = $state('t3.micro');
let newInstanceAmi = $state('ami-0c55b159cbfafe1f0');
let newInstanceName = $state('');
let activeTab = $state<'instances' | 'secgroups' | 'keypairs'>('instances');
let securityGroups = $state<SecurityGroup[]>([]);
let keyPairs = $state<KeyPairInfo[]>([]);
let sgSearch = $state('');
let kpSearch = $state('');

const instanceTypes = ['t3.micro', 't3.small', 't3.medium', 't3.large', 'm5.large', 'c5.large', 'r5.large'];

onMount(async () => {
	await loadInstances();
});

async function loadInstances() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeInstancesCommand({}));
		instances = data.Reservations?.flatMap((r) => (r.Instances ?? []) as EC2Instance[]) ?? [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load instances');
	} finally {
		loading = false;
	}
}

async function loadSecurityGroups() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeSecurityGroupsCommand({}));
		securityGroups = data.SecurityGroups || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load security groups');
	} finally {
		loading = false;
	}
}

async function loadKeyPairs() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeKeyPairsCommand({}));
		keyPairs = data.KeyPairs || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load key pairs');
	} finally {
		loading = false;
	}
}

async function selectTab(t: typeof activeTab) {
	activeTab = t;
	if (t === 'instances' && instances.length === 0) await loadInstances();
	else if (t === 'secgroups' && securityGroups.length === 0) await loadSecurityGroups();
	else if (t === 'keypairs' && keyPairs.length === 0) await loadKeyPairs();
}

async function refresh() {
	if (activeTab === 'instances') { instances = []; await loadInstances(); }
	else if (activeTab === 'secgroups') { securityGroups = []; await loadSecurityGroups(); }
	else { keyPairs = []; await loadKeyPairs(); }
}

function getName(instance: EC2Instance): string {
	return instance.Tags?.find((t) => t.Key === 'Name')?.Value || instance.InstanceId || '';
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
let filteredSGs = $derived(securityGroups.filter(sg =>
	!sgSearch || sg.GroupName?.toLowerCase().includes(sgSearch.toLowerCase()) || sg.GroupId?.toLowerCase().includes(sgSearch.toLowerCase())
));
let filteredKPs = $derived(keyPairs.filter(kp =>
	!kpSearch || kp.KeyName?.toLowerCase().includes(kpSearch.toLowerCase())
));
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
			<button onclick={refresh} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300" title="Refresh">
				<RefreshCw class="w-4 h-4" />
			</button>
			<button onclick={() => showLaunchModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />
				Launch Instance
			</button>
		</div>
	</div>

	<!-- Stats -->
	{#if !loading}
		<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Total</p>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{instances.length}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Running</p>
				<p class="text-2xl font-bold text-green-600">{runningCount}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Stopped</p>
				<p class="text-2xl font-bold text-slate-500">{stoppedCount}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Security Groups</p>
				<p class="text-2xl font-bold text-indigo-600 dark:text-indigo-400">{securityGroups.length}</p>
			</div>
		</div>
	{/if}

	<!-- Tabs -->
	<div class="border-b border-slate-200 dark:border-slate-700">
		<nav class="flex gap-1 -mb-px">
			<button onclick={() => selectTab('instances')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'instances' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Cpu class="w-4 h-4" /> Instances {#if instances.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{instances.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('secgroups')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'secgroups' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Shield class="w-4 h-4" /> Security Groups
			</button>
			<button onclick={() => selectTab('keypairs')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'keypairs' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Key class="w-4 h-4" /> Key Pairs
			</button>
		</nav>
	</div>

	<!-- Instances Tab -->
	{#if activeTab === 'instances'}
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
<span class={`px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(instance.State?.Name ?? '')}`}>
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
<button onclick={() => startInstance(instance.InstanceId ?? '')} class="px-3 py-2 bg-green-600 text-white rounded text-sm hover:bg-green-700 flex items-center gap-1">
<Play class="w-3 h-3" /> Start
</button>
{:else if instance.State?.Name === 'running'}
<button onclick={() => stopInstance(instance.InstanceId ?? '')} class="px-3 py-2 bg-yellow-600 text-white rounded text-sm hover:bg-yellow-700 flex items-center gap-1">
<Square class="w-3 h-3" /> Stop
</button>
<button onclick={() => rebootInstance(instance.InstanceId ?? '')} class="px-3 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 flex items-center gap-1">
<RotateCcw class="w-3 h-3" /> Reboot
</button>
{/if}
<button onclick={() => selectedInstance = instance} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">
Details
</button>
{#if instance.State?.Name !== 'terminated'}
<button onclick={() => terminateInstance(instance.InstanceId ?? '')} class="ml-auto px-3 py-2 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center gap-1">
<Trash2 class="w-3 h-3" /> Terminate
</button>
{/if}
</div>
</div>
{/each}
</div>
{/if}

<!-- End Instances Tab -->
{/if}

<!-- Security Groups Tab -->
{#if activeTab === 'secgroups'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search security groups..." bind:value={sgSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredSGs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Shield class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No security groups found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Name</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Group ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">VPC</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Rules</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredSGs as sg}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3">
<p class="font-medium text-slate-900 dark:text-white">{sg.GroupName}</p>
{#if sg.Description}<p class="text-xs text-slate-500 dark:text-slate-400">{sg.Description}</p>{/if}
</td>
<td class="px-4 py-3 font-mono text-xs text-slate-600 dark:text-slate-300">{sg.GroupId}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{sg.VpcId || '—'}</td>
<td class="px-4 py-3 text-slate-600 dark:text-slate-300">
<span class="text-green-600 dark:text-green-400">{sg.IpPermissions?.length ?? 0} in</span>
·
<span class="text-red-600 dark:text-red-400">{sg.IpPermissionsEgress?.length ?? 0} out</span>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- Key Pairs Tab -->
{#if activeTab === 'keypairs'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search key pairs..." bind:value={kpSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredKPs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Key class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No key pairs found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Key Name</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Key ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Fingerprint</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Created</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredKPs as kp}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{kp.KeyName}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{kp.KeyPairId || '—'}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400 max-w-xs truncate">{kp.KeyFingerprint || '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{kp.CreateTime ? new Date(kp.CreateTime).toLocaleDateString() : '—'}</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
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
