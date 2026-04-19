<script lang="ts">
	import { onMount } from 'svelte';
	import { getS3ControlClient } from '$lib/aws-client';
	import {
		ListAccessPointsCommand,
		ListMultiRegionAccessPointsCommand,
		ListStorageLensConfigurationsCommand,
		type AccessPoint,
		type MultiRegionAccessPointReport,
		type ListStorageLensConfigurationEntry
	} from '@aws-sdk/client-s3-control';
	import { toast } from 'svelte-sonner';
	import { Database, RefreshCw, Search, Globe, BarChart3 } from 'lucide-svelte';

	const s3ctrl = getS3ControlClient();

	let loading = $state(false);
	let activeTab = $state<'accesspoints' | 'multiregion' | 'lens'>('accesspoints');
	let searchQuery = $state('');
	let accountId = $state('000000000000');
	let accessPoints = $state<AccessPoint[]>([]);
	let multiRegionAPs = $state<MultiRegionAccessPointReport[]>([]);
	let storageLens = $state<ListStorageLensConfigurationEntry[]>([]);

	const filteredAccessPoints = $derived(accessPoints.filter((a) => (a.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredMultiRegion = $derived(multiRegionAPs.filter((m) => (m.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredLens = $derived(storageLens.filter((l) => (l.Id ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const [apResp, mrResp, lensResp] = await Promise.all([
				s3ctrl.send(new ListAccessPointsCommand({ AccountId: accountId })),
				s3ctrl.send(new ListMultiRegionAccessPointsCommand({ AccountId: accountId })),
				s3ctrl.send(new ListStorageLensConfigurationsCommand({ AccountId: accountId }))
			]);
			accessPoints = apResp.AccessPointList ?? [];
			multiRegionAPs = mrResp.AccessPoints ?? [];
			storageLens = lensResp.StorageLensConfigurationList ?? [];
		} catch (e) {
			toast.error('Failed to load S3 Control data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Database class="w-7 h-7 text-amber-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon S3 Control</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Manage S3 resources across accounts and regions</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-lg"><Database class="w-5 h-5 text-amber-600 dark:text-amber-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{accessPoints.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Access Points</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Globe class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{multiRegionAPs.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Multi-Region APs</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><BarChart3 class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{storageLens.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Storage Lens Configs</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between items-start sm:items-center">
			<div class="flex gap-2 flex-wrap">
				{#each [['accesspoints', 'Access Points'], ['multiregion', 'Multi-Region APs'], ['lens', 'Storage Lens']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-amber-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'accesspoints'}
				{#if filteredAccessPoints.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No access points found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredAccessPoints as ap}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Database class="w-5 h-5 text-amber-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{ap.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">Bucket: {ap.Bucket}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{ap.NetworkOrigin}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'multiregion'}
				{#if filteredMultiRegion.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No multi-region access points found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredMultiRegion as ap}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Globe class="w-5 h-5 text-blue-500" />
									<p class="font-medium text-gray-900 dark:text-white">{ap.Name}</p>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{ap.Status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'lens'}
				{#if filteredLens.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No Storage Lens configurations found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredLens as config}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<BarChart3 class="w-5 h-5 text-green-500" />
								<p class="font-medium text-gray-900 dark:text-white">{config.Id}</p>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
