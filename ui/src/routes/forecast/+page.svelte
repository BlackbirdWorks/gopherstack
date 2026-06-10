<script lang="ts">
	import { onMount } from 'svelte';
	import { getForecastClient } from '$lib/aws-client';
	import {
		ListDatasetGroupsCommand,
		ListForecastsCommand,
		ListMonitorsCommand,
		ListPredictorsCommand,
		type DatasetGroupSummary,
		type ForecastSummary,
		type MonitorSummary,
		type PredictorSummary
	} from '@aws-sdk/client-forecast';
	import { toast } from 'svelte-sonner';
	import { RefreshCw, Search, TrendingUp } from 'lucide-svelte';

	const client = getForecastClient();

	const activeStatuses = new Set<string>(['ACTIVE', 'AVAILABLE', 'ENABLED', 'RUNNING', 'COMPLETE', 'COMPLETED', 'IDLE', 'Active', 'opt-in-not-required', 'ENABLED_BY_DEFAULT']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s)) ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let loading = $state(false);
	let activeTab = $state<'datasetgroups' | 'predictors' | 'forecasts' | 'monitors'>('datasetgroups');
	let searchQuery = $state('');
	let datasetgroupsData = $state<DatasetGroupSummary[]>([]);
	let predictorsData = $state<PredictorSummary[]>([]);
	let forecastsData = $state<ForecastSummary[]>([]);
	let monitorsData = $state<MonitorSummary[]>([]);

	const filteredDatasetgroups = $derived(datasetgroupsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredPredictors = $derived(predictorsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredForecasts = $derived(forecastsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredMonitors = $derived(monitorsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			if (activeTab === 'datasetgroups') {
				const resp = await client.send(new ListDatasetGroupsCommand({}));
				datasetgroupsData = resp.DatasetGroups ?? [];
			}
			if (activeTab === 'predictors') {
				const resp = await client.send(new ListPredictorsCommand({}));
				predictorsData = resp.Predictors ?? [];
			}
			if (activeTab === 'forecasts') {
				const resp = await client.send(new ListForecastsCommand({}));
				forecastsData = resp.Forecasts ?? [];
			}
			if (activeTab === 'monitors') {
				const resp = await client.send(new ListMonitorsCommand({}));
				monitorsData = resp.Monitors ?? [];
			}
		} catch (e) {
			toast.error('Failed to load Amazon Forecast data: ' + String(e));
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
			<TrendingUp class="w-7 h-7 text-violet-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Forecast</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Time-series forecasting service</p>
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
				{#each [['datasetgroups', 'Dataset Groups'], ['predictors', 'Predictors'], ['forecasts', 'Forecasts'], ['monitors', 'Monitors']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-violet-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
									<TrendingUp class="w-5 h-5 text-violet-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.DatasetGroupName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.DatasetGroupArn ?? ''}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'predictors'}
				{#if filteredPredictors.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No predictors found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredPredictors as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<TrendingUp class="w-5 h-5 text-violet-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.PredictorName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.PredictorArn ?? ''}`}</p>
									</div>
								</div>
								{#if a.Status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.Status)}">{a.Status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'forecasts'}
				{#if filteredForecasts.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No forecasts found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredForecasts as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<TrendingUp class="w-5 h-5 text-violet-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.ForecastName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.ForecastArn ?? ''}`}</p>
									</div>
								</div>
								{#if a.Status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.Status)}">{a.Status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'monitors'}
				{#if filteredMonitors.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No monitors found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredMonitors as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<TrendingUp class="w-5 h-5 text-violet-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.MonitorName ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.MonitorArn ?? ''}`}</p>
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
