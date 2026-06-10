<script lang="ts">
	import { onMount } from 'svelte';
	import { getMediaPackageClient } from '$lib/aws-client';
	import {
		ListChannelsCommand,
		ListHarvestJobsCommand,
		ListOriginEndpointsCommand,
		type Channel,
		type HarvestJob,
		type OriginEndpoint
	} from '@aws-sdk/client-mediapackage';
	import { toast } from 'svelte-sonner';
	import { Package, RefreshCw, Search } from 'lucide-svelte';

	const client = getMediaPackageClient();

	const activeStatuses = new Set<string>(['ACTIVE', 'AVAILABLE', 'ENABLED', 'RUNNING', 'COMPLETE', 'COMPLETED', 'IDLE', 'Active', 'opt-in-not-required', 'ENABLED_BY_DEFAULT']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s)) ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let loading = $state(false);
	let activeTab = $state<'channels' | 'endpoints' | 'harvest'>('channels');
	let searchQuery = $state('');
	let channelsData = $state<Channel[]>([]);
	let endpointsData = $state<OriginEndpoint[]>([]);
	let harvestData = $state<HarvestJob[]>([]);

	const filteredChannels = $derived(channelsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredEndpoints = $derived(endpointsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredHarvest = $derived(harvestData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			if (activeTab === 'channels') {
				const resp = await client.send(new ListChannelsCommand({}));
				channelsData = resp.Channels ?? [];
			}
			if (activeTab === 'endpoints') {
				const resp = await client.send(new ListOriginEndpointsCommand({}));
				endpointsData = resp.OriginEndpoints ?? [];
			}
			if (activeTab === 'harvest') {
				const resp = await client.send(new ListHarvestJobsCommand({}));
				harvestData = resp.HarvestJobs ?? [];
			}
		} catch (e) {
			toast.error('Failed to load AWS Elemental MediaPackage data: ' + String(e));
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
			<Package class="w-7 h-7 text-pink-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Elemental MediaPackage</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Video origination and packaging</p>
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
				{#each [['channels', 'Channels'], ['endpoints', 'Origin Endpoints'], ['harvest', 'Harvest Jobs']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-pink-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'channels'}
				{#if filteredChannels.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No channels found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredChannels as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Package class="w-5 h-5 text-pink-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Id ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.Description ?? a.Arn ?? ''}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'endpoints'}
				{#if filteredEndpoints.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No origin endpoints found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredEndpoints as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Package class="w-5 h-5 text-pink-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Id ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Channel: ${a.ChannelId ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'harvest'}
				{#if filteredHarvest.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No harvest jobs found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredHarvest as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Package class="w-5 h-5 text-pink-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Id ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Channel: ${a.ChannelId ?? '-'}`}</p>
									</div>
								</div>
								{#if a.Status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.Status)}">{a.Status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
