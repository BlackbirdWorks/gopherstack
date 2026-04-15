<script lang="ts">
	import { onMount } from 'svelte';
	import { EC2Client, DescribeInstancesCommand, RunInstancesCommand, StopInstancesCommand, StartInstancesCommand, TerminateInstancesCommand } from '@aws-sdk/client-ec2';
	import { toast } from 'svelte-sonner';
	import { Cpu, Play, Square, Trash2, RefreshCw, Plus } from 'lucide-svelte';

	let instances = $state<any[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let showCreateModal = $state(false);

	const ec2 = new EC2Client({});

	onMount(async () => {
		await loadInstances();
	});

	async function loadInstances() {
		try {
			loading = true;
			const data = await ec2.send(new DescribeInstancesCommand({}));
			instances = data.Reservations?.flatMap(r => r.Instances || []) || [];
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load instances';
			toast.error(error);
		} finally {
			loading = false;
		}
	}

	async function startInstance(instanceId: string) {
		try {
			await ec2.send(new StartInstancesCommand({ InstanceIds: [instanceId] }));
			toast.success(`Instance ${instanceId} starting...`);
			await loadInstances();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to start instance';
			toast.error(error);
		}
	}

	async function stopInstance(instanceId: string) {
		try {
			await ec2.send(new StopInstancesCommand({ InstanceIds: [instanceId] }));
			toast.success(`Instance ${instanceId} stopping...`);
			await loadInstances();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to stop instance';
			toast.error(error);
		}
	}

	async function terminateInstance(instanceId: string) {
		if (!confirm(`Terminate instance ${instanceId}?`)) return;
		try {
			await ec2.send(new TerminateInstancesCommand({ InstanceIds: [instanceId] }));
			toast.success(`Instance ${instanceId} terminating...`);
			await loadInstances();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to terminate instance';
			toast.error(error);
		}
	}

	function getStatusColor(state: string) {
		switch (state) {
			case 'running': return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
			case 'stopped': return 'bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300';
			case 'stopping': return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
			case 'pending': return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300';
			case 'terminated': return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
			default: return 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300';
		}
	}
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Cpu class="w-6 h-6 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">EC2 Instances</h1>
				<p class="text-slate-600 dark:text-slate-300">Elastic Compute Cloud instances</p>
			</div>
		</div>
		<button onclick={() => showCreateModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
			<Plus class="w-4 h-4" />
			Launch Instance
		</button>
	</div>

	{#if loading}
		<div class="text-center py-8 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
			<p>Loading instances...</p>
		</div>
	{:else if error}
		<div class="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg text-red-700 dark:text-red-300">
			{error}
		</div>
	{:else if instances.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
			<Cpu class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
			<p class="text-slate-600 dark:text-slate-300 mb-4">No EC2 instances found</p>
			<button onclick={() => showCreateModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg">Launch Instance</button>
		</div>
	{:else}
		<div class="grid gap-4">
			{#each instances as instance}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
					<div class="flex items-start justify-between mb-4">
						<div>
							<h3 class="font-semibold text-slate-900 dark:text-white">{instance.InstanceId}</h3>
							<p class="text-sm text-slate-600 dark:text-slate-400">{instance.InstanceType}</p>
						</div>
						<span class={`px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(instance.State?.Name)}`}>
							{instance.State?.Name || 'unknown'}
						</span>
					</div>
					<div class="grid grid-cols-2 gap-4 mb-4 text-sm">
						<div>
							<p class="text-slate-600 dark:text-slate-400">Public IP</p>
							<p class="font-mono text-slate-900 dark:text-white">{instance.PublicIpAddress || 'N/A'}</p>
						</div>
						<div>
							<p class="text-slate-600 dark:text-slate-400">Private IP</p>
							<p class="font-mono text-slate-900 dark:text-white">{instance.PrivateIpAddress || 'N/A'}</p>
						</div>
					</div>
					<div class="flex gap-2">
						{#if instance.State?.Name !== 'running'}
							<button onclick={() => startInstance(instance.InstanceId)} class="flex-1 px-3 py-2 bg-green-600 text-white rounded text-sm hover:bg-green-700 flex items-center justify-center gap-2">
								<Play class="w-4 h-4" />
								Start
							</button>
						{:else}
							<button onclick={() => stopInstance(instance.InstanceId)} class="flex-1 px-3 py-2 bg-yellow-600 text-white rounded text-sm hover:bg-yellow-700 flex items-center justify-center gap-2">
								<Square class="w-4 h-4" />
								Stop
							</button>
						{/if}
						<button onclick={() => terminateInstance(instance.InstanceId)} class="flex-1 px-3 py-2 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center justify-center gap-2">
							<Trash2 class="w-4 h-4" />
							Terminate
						</button>
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>
