<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getOutpostsClient } from '$lib/aws-client';
	import {
		ListOutpostsCommand,
		ListSitesCommand,
		type Outpost,
		type Site
	} from '@aws-sdk/client-outposts';
	import { toast } from 'svelte-sonner';
	import { Server, RefreshCw, Search, MapPin, CheckCircle } from 'lucide-svelte';

	const op = regionalClient(getOutpostsClient);

	let loading = $state(false);
	let activeTab = $state<'outposts' | 'sites'>('outposts');
	let searchQuery = $state('');
	let outposts = $state<Outpost[]>([]);
	let sites = $state<Site[]>([]);

	const filteredOutposts = $derived(outposts.filter((o) => (o.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredSites = $derived(sites.filter((s) => (s.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const activeOutposts = $derived(outposts.filter((o) => o.LifeCycleStatus === 'ACTIVE').length);

	async function loadData() {
		loading = true;
		try {
			const [opResp, siteResp] = await Promise.all([
				op().send(new ListOutpostsCommand({})),
				op().send(new ListSitesCommand({}))
			]);
			outposts = opResp.Outposts ?? [];
			sites = siteResp.Sites ?? [];
		} catch (e) {
			toast.error('Failed to load Outposts data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Server class="w-7 h-7 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Outposts</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Run AWS infrastructure and services on-premises</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><Server class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{outposts.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Outposts</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{activeOutposts}</p><p class="text-sm text-gray-500 dark:text-gray-400">Active</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><MapPin class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{sites.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Sites</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['outposts', 'Outposts'], ['sites', 'Sites']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-orange-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'outposts'}
				{#if filteredOutposts.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No Outposts found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredOutposts as outpost}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Server class="w-5 h-5 text-orange-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{outpost.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{outpost.OutpostId} · {outpost.AvailabilityZone}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {outpost.LifeCycleStatus === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{outpost.LifeCycleStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'sites'}
				{#if filteredSites.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No sites found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredSites as site}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<MapPin class="w-5 h-5 text-blue-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{site.Name}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{site.SiteId}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
