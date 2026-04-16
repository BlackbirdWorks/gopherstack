<script lang="ts">
	import { onMount } from 'svelte';
	import { getGlacierClient } from '$lib/aws-client';
	import {
		ListVaultsCommand,
		type DescribeVaultOutput
	} from '@aws-sdk/client-glacier';
	import { toast } from 'svelte-sonner';
	import { Archive, RefreshCw, Search, Lock, Database } from 'lucide-svelte';

	const glacier = getGlacierClient();

	let loading = $state(false);
	let searchQuery = $state('');
	let vaults = $state<DescribeVaultOutput[]>([]);

	const filteredVaults = $derived(vaults.filter((v) => (v.VaultName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const totalSize = $derived(vaults.reduce((sum, v) => sum + (v.SizeInBytes ?? 0), 0));

	async function loadData() {
		loading = true;
		try {
			const resp = await glacier.send(new ListVaultsCommand({ accountId: '-' }));
			vaults = resp.VaultList ?? [];
		} catch (e) {
			toast.error('Failed to load Glacier data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function formatBytes(bytes: number): string {
		if (bytes < 1024) return bytes + ' B';
		if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
		if (bytes < 1024 * 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
		return (bytes / (1024 * 1024 * 1024)).toFixed(2) + ' GB';
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Archive class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon S3 Glacier</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Low-cost cloud storage service for data archiving and long-term backup</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Archive class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{vaults.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Vaults</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Database class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{formatBytes(totalSize)}</p><p class="text-sm text-gray-500 dark:text-gray-400">Total Storage</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Lock class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{vaults.reduce((s, v) => s + (v.NumberOfArchives ?? 0), 0)}</p><p class="text-sm text-gray-500 dark:text-gray-400">Archives</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex items-center gap-3">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search vaults..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if filteredVaults.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">No vaults found</div>
			{:else}
				<div class="space-y-2">
					{#each filteredVaults as vault}
						<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
							<div class="flex items-center gap-3">
								<Archive class="w-5 h-5 text-blue-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{vault.VaultName}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{formatBytes(vault.SizeInBytes ?? 0)} · {vault.NumberOfArchives} archives</p>
								</div>
							</div>
							<p class="text-xs text-gray-500 dark:text-gray-400">{vault.LastInventoryDate ?? 'Never inventoried'}</p>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	</div>
</div>
