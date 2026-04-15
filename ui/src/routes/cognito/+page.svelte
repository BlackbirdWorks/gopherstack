<script lang="ts">
	import { onMount } from 'svelte';
	import { CognitoIdentityProviderClient, ListUserPoolsCommand, DescribeUserPoolCommand } from '@aws-sdk/client-cognito-identity-provider';
	import { toast } from 'svelte-sonner';
	import { Users, Plus, Key } from 'lucide-svelte';

	let userPools = $state<any[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);

	const cognito = new CognitoIdentityProviderClient({});

	onMount(async () => {
		await loadUserPools();
	});

	async function loadUserPools() {
		try {
			loading = true;
			const data = await cognito.send(new ListUserPoolsCommand({ MaxResults: 60 }));
			userPools = data.UserPools || [];
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load user pools';
			toast.error(error);
		} finally {
			loading = false;
		}
	}

	function formatDate(date?: Date | number): string {
		return date ? new Date(date).toLocaleDateString() : 'N/A';
	}
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Users class="w-6 h-6 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Cognito User Pools</h1>
				<p class="text-slate-600 dark:text-slate-300">User identity and access management</p>
			</div>
		</div>
		<button onclick={() => toast.info('Create user pool feature coming soon')} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
			<Plus class="w-4 h-4" />
			Create Pool
		</button>
	</div>

	{#if loading}
		<div class="text-center py-8 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
			<p>Loading user pools...</p>
		</div>
	{:else if error}
		<div class="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg text-red-700 dark:text-red-300">
			{error}
		</div>
	{:else if userPools.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
			<Users class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
			<p class="text-slate-600 dark:text-slate-300">No Cognito user pools found</p>
		</div>
	{:else}
		<div class="grid gap-4">
			{#each userPools as pool}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
					<div class="flex items-start justify-between mb-4">
						<div>
							<h3 class="font-semibold text-slate-900 dark:text-white">{pool.Name}</h3>
							<p class="text-sm text-slate-600 dark:text-slate-400 font-mono mt-1">{pool.Id}</p>
						</div>
						<span class="px-3 py-1 rounded-full text-sm font-medium bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">Active</span>
					</div>
					<div class="grid grid-cols-3 gap-4 text-sm">
						<div>
							<p class="text-slate-600 dark:text-slate-400">Status</p>
							<p class="font-mono text-slate-900 dark:text-white">{pool.Status || 'ACTIVE'}</p>
						</div>
						<div>
							<p class="text-slate-600 dark:text-slate-400">Created</p>
							<p class="font-mono text-slate-900 dark:text-white text-xs">{formatDate(pool.CreationDate)}</p>
						</div>
						<div>
							<p class="text-slate-600 dark:text-slate-400">Updated</p>
							<p class="font-mono text-slate-900 dark:text-white text-xs">{formatDate(pool.LastModifiedDate)}</p>
						</div>
					</div>
					<div class="flex gap-2 mt-4">
						<button onclick={() => toast.info('View pool details coming soon')} class="flex-1 px-3 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700">
							View Details
						</button>
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>
