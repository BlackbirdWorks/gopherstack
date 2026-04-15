<script lang="ts">
	import { onMount } from 'svelte';
	import { KMSClient, ListKeysCommand, DescribeKeyCommand, DisableKeyCommand } from '@aws-sdk/client-kms';
	import { toast } from 'svelte-sonner';
	import { Key, Plus, Lock } from 'lucide-svelte';

	let keys = $state<any[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);

	const kms = new KMSClient({});

	onMount(async () => {
		await loadKeys();
	});

	async function loadKeys() {
		try {
			loading = true;
			const data = await kms.send(new ListKeysCommand({ Limit: 100 }));
			keys = data.Keys || [];
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load keys';
			toast.error(error);
		} finally {
			loading = false;
		}
	}
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-yellow-100 dark:bg-yellow-900/30 rounded-lg">
				<Key class="w-6 h-6 text-yellow-600 dark:text-yellow-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">KMS Customer Master Keys</h1>
				<p class="text-slate-600 dark:text-slate-300">Encryption key management</p>
			</div>
		</div>
		<button onclick={() => toast.info('Create key feature coming soon')} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
			<Plus class="w-4 h-4" />
			Create Key
		</button>
	</div>

	{#if loading}
		<div class="text-center py-8 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
			<p>Loading keys...</p>
		</div>
	{:else if error}
		<div class="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg text-red-700 dark:text-red-300">
			{error}
		</div>
	{:else if keys.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
			<Lock class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
			<p class="text-slate-600 dark:text-slate-300">No KMS keys found</p>
		</div>
	{:else}
		<div class="grid gap-4">
			{#each keys as key}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
					<div class="space-y-2">
						<h3 class="font-semibold text-slate-900 dark:text-white font-mono text-sm break-all">{key.KeyId}</h3>
						<p class="text-xs text-slate-600 dark:text-slate-400 font-mono">ARN: {key.KeyArn}</p>
					</div>
					<div class="flex gap-2 mt-4">
						<button onclick={() => toast.info('View key details coming soon')} class="flex-1 px-3 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700">
							View Details
						</button>
						<button onclick={() => toast.info('Edit key feature coming soon')} class="flex-1 px-3 py-2 bg-slate-600 text-white rounded text-sm hover:bg-slate-700">
							Edit
						</button>
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>
