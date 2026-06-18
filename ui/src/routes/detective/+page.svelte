<script lang="ts">
	import { onMount } from 'svelte';
	import { getDetectiveClient } from '$lib/aws-client';
	import {
		ListGraphsCommand,
		type Graph
	} from '@aws-sdk/client-detective';
	import { toast } from 'svelte-sonner';
	import { RefreshCw, Search } from 'lucide-svelte';

	const client = getDetectiveClient();

	let loading = $state(false);
	let activeTab = $state<'graphs'>('graphs');
	let searchQuery = $state('');
	let graphsData = $state<Graph[]>([]);

	const filteredGraphs = $derived(graphsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			if (activeTab === 'graphs') {
				const resp = await client.send(new ListGraphsCommand({}));
				graphsData = resp.GraphList ?? [];
			}
		} catch (e) {
			toast.error('Failed to load Amazon Detective data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		loadData();
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between flex-wrap gap-3">
		<div class="flex items-center gap-3">
			<Search class="w-7 h-7 text-rose-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Detective</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Investigate and analyze security findings</p>
			</div>
		</div>
		<div class="flex items-center gap-2 flex-wrap">
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['graphs', 'Behavior Graphs']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-rose-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'graphs'}
				{#if filteredGraphs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No behavior graphs found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredGraphs as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Search class="w-5 h-5 text-rose-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Arn ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Created: ${a.CreatedTime ? new Date(a.CreatedTime).toLocaleString() : '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
