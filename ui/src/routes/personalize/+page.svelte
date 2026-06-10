<script lang="ts">
	import { onMount } from 'svelte';
	import { getPersonalizeClient } from '$lib/aws-client';
	import {
		ListCampaignsCommand,
		ListDatasetGroupsCommand,
		ListEventTrackersCommand,
		ListRecommendersCommand,
		ListSolutionsCommand,
		type CampaignSummary,
		type DatasetGroupSummary,
		type EventTrackerSummary,
		type RecommenderSummary,
		type SolutionSummary
	} from '@aws-sdk/client-personalize';
	import { toast } from 'svelte-sonner';
	import { RefreshCw, Search, Sparkles } from 'lucide-svelte';

	const client = getPersonalizeClient();

	const activeStatuses = new Set<string>(['ACTIVE', 'AVAILABLE', 'ENABLED', 'RUNNING', 'COMPLETE', 'COMPLETED', 'IDLE', 'Active', 'opt-in-not-required', 'ENABLED_BY_DEFAULT']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s)) ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let loading = $state(false);
	let activeTab = $state<'datasetgroups' | 'solutions' | 'campaigns' | 'recommenders' | 'trackers'>('datasetgroups');
	let searchQuery = $state('');
	let datasetgroupsData = $state<DatasetGroupSummary[]>([]);
	let solutionsData = $state<SolutionSummary[]>([]);
	let campaignsData = $state<CampaignSummary[]>([]);
	let recommendersData = $state<RecommenderSummary[]>([]);
	let trackersData = $state<EventTrackerSummary[]>([]);

	const filteredDatasetgroups = $derived(datasetgroupsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredSolutions = $derived(solutionsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredCampaigns = $derived(campaignsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredRecommenders = $derived(recommendersData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredTrackers = $derived(trackersData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			if (activeTab === 'datasetgroups') {
				const resp = await client.send(new ListDatasetGroupsCommand({}));
				datasetgroupsData = resp.datasetGroups ?? [];
			}
			if (activeTab === 'solutions') {
				const resp = await client.send(new ListSolutionsCommand({}));
				solutionsData = resp.solutions ?? [];
			}
			if (activeTab === 'campaigns') {
				const resp = await client.send(new ListCampaignsCommand({}));
				campaignsData = resp.campaigns ?? [];
			}
			if (activeTab === 'recommenders') {
				const resp = await client.send(new ListRecommendersCommand({}));
				recommendersData = resp.recommenders ?? [];
			}
			if (activeTab === 'trackers') {
				const resp = await client.send(new ListEventTrackersCommand({}));
				trackersData = resp.eventTrackers ?? [];
			}
		} catch (e) {
			toast.error('Failed to load Amazon Personalize data: ' + String(e));
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
			<Sparkles class="w-7 h-7 text-indigo-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Personalize</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Real-time personalization and recommendations</p>
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
				{#each [['datasetgroups', 'Dataset Groups'], ['solutions', 'Solutions'], ['campaigns', 'Campaigns'], ['recommenders', 'Recommenders'], ['trackers', 'Event Trackers']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-indigo-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'datasetgroups'}
				{#if filteredDatasetgroups.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No dataset groups found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredDatasetgroups as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Sparkles class="w-5 h-5 text-indigo-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.datasetGroupArn ?? ''}`}</p>
									</div>
								</div>
								{#if a.status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.status)}">{a.status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'solutions'}
				{#if filteredSolutions.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No solutions found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredSolutions as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Sparkles class="w-5 h-5 text-indigo-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.solutionArn ?? ''}`}</p>
									</div>
								</div>
								{#if a.status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.status)}">{a.status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'campaigns'}
				{#if filteredCampaigns.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No campaigns found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredCampaigns as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Sparkles class="w-5 h-5 text-indigo-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.campaignArn ?? ''}`}</p>
									</div>
								</div>
								{#if a.status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.status)}">{a.status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'recommenders'}
				{#if filteredRecommenders.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No recommenders found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredRecommenders as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Sparkles class="w-5 h-5 text-indigo-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.recommenderArn ?? ''}`}</p>
									</div>
								</div>
								{#if a.status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.status)}">{a.status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'trackers'}
				{#if filteredTrackers.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No event trackers found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTrackers as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Sparkles class="w-5 h-5 text-indigo-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.eventTrackerArn ?? ''}`}</p>
									</div>
								</div>
								{#if a.status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.status)}">{a.status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
