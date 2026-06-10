<script lang="ts">
	import { onMount } from 'svelte';
	import { getMediaTailorClient } from '$lib/aws-client';
	import {
		ListChannelsCommand,
		ListPlaybackConfigurationsCommand,
		ListSourceLocationsCommand,
		ListVodSourcesCommand,
		type Channel,
		type PlaybackConfiguration,
		type SourceLocation,
		type VodSource
	} from '@aws-sdk/client-mediatailor';
	import { toast } from 'svelte-sonner';
	import { RefreshCw, Search, Tv } from 'lucide-svelte';

	const client = getMediaTailorClient();

	const activeStatuses = new Set<string>(['ACTIVE', 'AVAILABLE', 'ENABLED', 'RUNNING', 'COMPLETE', 'COMPLETED', 'IDLE', 'Active', 'opt-in-not-required', 'ENABLED_BY_DEFAULT']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s)) ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let loading = $state(false);
	let activeTab = $state<'channels' | 'sources' | 'configs' | 'vod'>('channels');
	let searchQuery = $state('');
	let channelsData = $state<Channel[]>([]);
	let sourcesData = $state<SourceLocation[]>([]);
	let configsData = $state<PlaybackConfiguration[]>([]);
	let vodData = $state<VodSource[]>([]);
	let sourceLocationFilter = $state('');

	const filteredChannels = $derived(channelsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredSources = $derived(sourcesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredConfigs = $derived(configsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredVod = $derived(vodData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			if (activeTab === 'channels') {
				const resp = await client.send(new ListChannelsCommand({}));
				channelsData = resp.Items ?? [];
			}
			if (activeTab === 'sources') {
				const resp = await client.send(new ListSourceLocationsCommand({}));
				sourcesData = resp.Items ?? [];
			}
			if (activeTab === 'configs') {
				const resp = await client.send(new ListPlaybackConfigurationsCommand({}));
				configsData = resp.Items ?? [];
			}
			if (activeTab === 'vod') {
				if (sourceLocationFilter) {
					const resp = await client.send(new ListVodSourcesCommand({ SourceLocationName: sourceLocationFilter }));
					vodData = resp.Items ?? [];
				} else {
					vodData = [];
				}
			}
		} catch (e) {
			toast.error('Failed to load AWS Elemental MediaTailor data: ' + String(e));
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
			<Tv class="w-7 h-7 text-fuchsia-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Elemental MediaTailor</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Personalized ad insertion</p>
			</div>
		</div>
		<div class="flex items-center gap-2 flex-wrap">
			<input bind:value={sourceLocationFilter} onchange={loadData} placeholder="Source location" class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-40" />
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<p class="text-xs text-gray-500 dark:text-gray-400">Enter a source location name above to list its VOD sources.</p>
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['channels', 'Channels'], ['sources', 'Source Locations'], ['configs', 'Playback Configs'], ['vod', 'VOD Sources']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-fuchsia-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
									<Tv class="w-5 h-5 text-fuchsia-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.ChannelName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.PlaybackMode ?? '-'} · ${a.Tier ?? ''}`}</p>
									</div>
								</div>
								{#if a.ChannelState}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.ChannelState)}">{a.ChannelState}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'sources'}
				{#if filteredSources.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No source locations found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredSources as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Tv class="w-5 h-5 text-fuchsia-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.SourceLocationName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.Arn ?? ''}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'configs'}
				{#if filteredConfigs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No playback configs found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredConfigs as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Tv class="w-5 h-5 text-fuchsia-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.PlaybackConfigurationArn ?? ''}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'vod'}
				{#if filteredVod.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No vod sources found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredVod as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Tv class="w-5 h-5 text-fuchsia-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.VodSourceName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Source: ${a.SourceLocationName ?? '-'}`}</p>
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
