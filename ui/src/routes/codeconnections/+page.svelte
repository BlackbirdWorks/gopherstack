<script lang="ts">
	import { onMount } from 'svelte';
	import { getCodeConnectionsClient } from '$lib/aws-client';
	import { ListConnectionsCommand } from '@aws-sdk/client-codeconnections';
	import { toast } from 'svelte-sonner';

	const codeConnections = getCodeConnectionsClient();
	let loading = $state(false);
	let connections = $state<Array<{ ConnectionName?: string; ProviderType?: string; ConnectionStatus?: string }>>([]);

	async function loadConnections() {
		loading = true;
		try {
			const out = await codeConnections.send(new ListConnectionsCommand({}));
			connections = out.Connections ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to list connections: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		void loadConnections();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 id="codeconnections-title" class="text-3xl font-bold text-slate-900 dark:text-white">AWS CodeConnections</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">External source control connections</p>
	</div>
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		{#if loading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading connections...</p>
		{:else if connections.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No connections found</p>
		{:else}
			<div class="space-y-2">
				{#each connections as connection}
					<div class="rounded-lg border border-slate-200 p-3 dark:border-slate-700">
						<div class="font-medium text-slate-900 dark:text-white">{connection.ConnectionName}</div>
						<div class="text-xs text-slate-500 dark:text-slate-400">{connection.ProviderType} • {connection.ConnectionStatus}</div>
					</div>
				{/each}
			</div>
		{/if}
	</div>
</div>
