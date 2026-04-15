<script lang="ts">
	import { onMount } from 'svelte';
	import { RDSClient, DescribeDBInstancesCommand, ModifyDBInstanceCommand, DeleteDBInstanceCommand, CreateDBInstanceCommand } from '@aws-sdk/client-rds';
	import { toast } from 'svelte-sonner';
	import { Database, Plus, Trash2, Edit2 } from 'lucide-svelte';

	let instances = $state<any[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);

	const rds = new RDSClient({});

	onMount(async () => {
		await loadInstances();
	});

	async function loadInstances() {
		try {
			loading = true;
			const data = await rds.send(new DescribeDBInstancesCommand({}));
			instances = data.DBInstances || [];
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load instances';
			toast.error(error);
		} finally {
			loading = false;
		}
	}

	async function deleteInstance(dbInstanceIdentifier: string) {
		if (!confirm(`Delete RDS instance ${dbInstanceIdentifier}?`)) return;
		try {
			await rds.send(new DeleteDBInstanceCommand({ DBInstanceIdentifier: dbInstanceIdentifier, SkipFinalSnapshot: true }));
			toast.success(`Instance ${dbInstanceIdentifier} deleting...`);
			await loadInstances();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to delete instance';
			toast.error(error);
		}
	}
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Database class="w-6 h-6 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">RDS Instances</h1>
				<p class="text-slate-600 dark:text-slate-300">Relational Database Service</p>
			</div>
		</div>
		<button onclick={() => toast.info('Create DB instance feature coming soon')} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
			<Plus class="w-4 h-4" />
			Create Instance
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
			<Database class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
			<p class="text-slate-600 dark:text-slate-300">No RDS instances found</p>
		</div>
	{:else}
		<div class="grid gap-4">
			{#each instances as instance}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
					<div class="flex items-start justify-between mb-4">
						<div>
							<h3 class="font-semibold text-slate-900 dark:text-white">{instance.DBInstanceIdentifier}</h3>
							<p class="text-sm text-slate-600 dark:text-slate-400">{instance.Engine} - {instance.DBInstanceClass}</p>
						</div>
						<span class="px-3 py-1 rounded-full text-sm font-medium bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">{instance.DBInstanceStatus}</span>
					</div>
					<div class="grid grid-cols-2 gap-4 mb-4 text-sm">
						<div>
							<p class="text-slate-600 dark:text-slate-400">Endpoint</p>
							<p class="font-mono text-slate-900 dark:text-white text-xs break-all">{instance.Endpoint?.Address || 'N/A'}</p>
						</div>
						<div>
							<p class="text-slate-600 dark:text-slate-400">Allocated Storage</p>
							<p class="font-mono text-slate-900 dark:text-white">{instance.AllocatedStorage} GB</p>
						</div>
					</div>
					<div class="flex gap-2">
						<button onclick={() => toast.info('Modify feature coming soon')} class="flex-1 px-3 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 flex items-center justify-center gap-2">
							<Edit2 class="w-4 h-4" />
							Modify
						</button>
						<button onclick={() => deleteInstance(instance.DBInstanceIdentifier)} class="flex-1 px-3 py-2 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center justify-center gap-2">
							<Trash2 class="w-4 h-4" />
							Delete
						</button>
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>
