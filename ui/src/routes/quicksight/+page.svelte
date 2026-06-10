<script lang="ts">
	import { onMount } from 'svelte';
	import { getQuickSightClient } from '$lib/aws-client';
	import {
		ListAnalysesCommand,
		ListDashboardsCommand,
		ListDataSetsCommand,
		ListDataSourcesCommand,
		type AnalysisSummary,
		type DashboardSummary,
		type DataSetSummary,
		type DataSource
	} from '@aws-sdk/client-quicksight';
	import { toast } from 'svelte-sonner';
	import { BarChart3, RefreshCw, Search } from 'lucide-svelte';

	const client = getQuickSightClient();

	const activeStatuses = new Set<string>(['ACTIVE', 'AVAILABLE', 'ENABLED', 'RUNNING', 'COMPLETE', 'COMPLETED', 'IDLE', 'Active', 'opt-in-not-required', 'ENABLED_BY_DEFAULT']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s)) ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let loading = $state(false);
	let activeTab = $state<'dashboards' | 'analyses' | 'datasets' | 'datasources'>('dashboards');
	let searchQuery = $state('');
	let dashboardsData = $state<DashboardSummary[]>([]);
	let analysesData = $state<AnalysisSummary[]>([]);
	let datasetsData = $state<DataSetSummary[]>([]);
	let datasourcesData = $state<DataSource[]>([]);
	let awsAccountId = $state('000000000000');

	const filteredDashboards = $derived(dashboardsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredAnalyses = $derived(analysesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredDatasets = $derived(datasetsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredDatasources = $derived(datasourcesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			if (activeTab === 'dashboards') {
				const resp = await client.send(new ListDashboardsCommand({ AwsAccountId: awsAccountId }));
				dashboardsData = resp.DashboardSummaryList ?? [];
			}
			if (activeTab === 'analyses') {
				const resp = await client.send(new ListAnalysesCommand({ AwsAccountId: awsAccountId }));
				analysesData = resp.AnalysisSummaryList ?? [];
			}
			if (activeTab === 'datasets') {
				const resp = await client.send(new ListDataSetsCommand({ AwsAccountId: awsAccountId }));
				datasetsData = resp.DataSetSummaries ?? [];
			}
			if (activeTab === 'datasources') {
				const resp = await client.send(new ListDataSourcesCommand({ AwsAccountId: awsAccountId }));
				datasourcesData = resp.DataSources ?? [];
			}
		} catch (e) {
			toast.error('Failed to load Amazon QuickSight data: ' + String(e));
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
			<BarChart3 class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon QuickSight</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Business intelligence dashboards</p>
			</div>
		</div>
		<div class="flex items-center gap-2 flex-wrap">
			<input bind:value={awsAccountId} onchange={loadData} placeholder="AWS Account ID" class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-40" />
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['dashboards', 'Dashboards'], ['analyses', 'Analyses'], ['datasets', 'Data Sets'], ['datasources', 'Data Sources']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'dashboards'}
				{#if filteredDashboards.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No dashboards found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredDashboards as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<BarChart3 class="w-5 h-5 text-blue-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`v${a.PublishedVersionNumber ?? '-'} · ${a.DashboardId ?? ''}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'analyses'}
				{#if filteredAnalyses.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No analyses found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredAnalyses as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<BarChart3 class="w-5 h-5 text-blue-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.AnalysisId ?? ''}`}</p>
									</div>
								</div>
								{#if a.Status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.Status)}">{a.Status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'datasets'}
				{#if filteredDatasets.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No data sets found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredDatasets as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<BarChart3 class="w-5 h-5 text-blue-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.DataSetId ?? ''}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'datasources'}
				{#if filteredDatasources.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No data sources found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredDatasources as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<BarChart3 class="w-5 h-5 text-blue-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.Type ?? '-'} · ${a.DataSourceId ?? ''}`}</p>
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
