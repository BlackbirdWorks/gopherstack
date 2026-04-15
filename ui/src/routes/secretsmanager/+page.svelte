<script lang="ts">
	import { onMount } from 'svelte';
	import { SecretsManagerClient, ListSecretsCommand, DescribeSecretCommand } from '@aws-sdk/client-secrets-manager';
	import { toast } from 'svelte-sonner';
	import { LockKeyhole, Plus, Eye, Trash2 } from 'lucide-svelte';

	let secrets = $state<any[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);

	const secretsManager = new SecretsManagerClient({});

	onMount(async () => {
		await loadSecrets();
	});

	async function loadSecrets() {
		try {
			loading = true;
			const data = await secretsManager.send(new ListSecretsCommand({ MaxResults: 100 }));
			secrets = data.SecretList || [];
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load secrets';
			toast.error(error);
		} finally {
			loading = false;
		}
	}

	function formatDate(date?: number): string {
		return date ? new Date(date * 1000).toLocaleDateString() : 'N/A';
	}
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-cyan-100 dark:bg-cyan-900/30 rounded-lg">
				<LockKeyhole class="w-6 h-6 text-cyan-600 dark:text-cyan-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Secrets Manager</h1>
				<p class="text-slate-600 dark:text-slate-300">Manage and rotate secrets</p>
			</div>
		</div>
		<button onclick={() => toast.info('Create secret feature coming soon')} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
			<Plus class="w-4 h-4" />
			Create Secret
		</button>
	</div>

	{#if loading}
		<div class="text-center py-8 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
			<p>Loading secrets...</p>
		</div>
	{:else if error}
		<div class="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg text-red-700 dark:text-red-300">
			{error}
		</div>
	{:else if secrets.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
			<LockKeyhole class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
			<p class="text-slate-600 dark:text-slate-300">No secrets found</p>
		</div>
	{:else}
		<div class="grid gap-4">
			{#each secrets as secret}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
					<div class="flex items-start justify-between mb-4">
						<div>
							<h3 class="font-semibold text-slate-900 dark:text-white">{secret.Name}</h3>
							<p class="text-sm text-slate-600 dark:text-slate-400 font-mono mt-1 break-all">{secret.ARN}</p>
						</div>
						<span class="px-3 py-1 rounded-full text-sm font-medium bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">Active</span>
					</div>
					<div class="grid grid-cols-3 gap-4 text-sm mb-4">
						<div>
							<p class="text-slate-600 dark:text-slate-400">Created</p>
							<p class="font-mono text-slate-900 dark:text-white text-xs">{formatDate(secret.CreatedDate)}</p>
						</div>
						<div>
							<p class="text-slate-600 dark:text-slate-400">Last Updated</p>
							<p class="font-mono text-slate-900 dark:text-white text-xs">{formatDate(secret.LastChangedDate)}</p>
						</div>
						<div>
							<p class="text-slate-600 dark:text-slate-400">Description</p>
							<p class="text-xs text-slate-900 dark:text-white">{secret.Description || 'N/A'}</p>
						</div>
					</div>
					<div class="flex gap-2">
						<button onclick={() => toast.info('View secret details coming soon')} class="flex-1 px-3 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 flex items-center justify-center gap-2">
							<Eye class="w-4 h-4" />
							View
						</button>
						<button onclick={() => toast.info('Delete secret feature coming soon')} class="flex-1 px-3 py-2 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center justify-center gap-2">
							<Trash2 class="w-4 h-4" />
							Delete
						</button>
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>
