<script lang="ts">
	import { onMount } from 'svelte';
	import { getPinpointClient } from '$lib/aws-client';
	import {
		GetAppsCommand,
		GetCampaignsCommand,
		GetSegmentsCommand,
		type ApplicationResponse,
		type CampaignResponse,
		type SegmentResponse
	} from '@aws-sdk/client-pinpoint';
	import { toast } from 'svelte-sonner';
	import { MapPin, RefreshCw, Search, Megaphone, Users, Activity } from 'lucide-svelte';

	const pp = getPinpointClient();

	let loading = $state(false);
	let activeTab = $state<'apps' | 'campaigns' | 'segments'>('apps');
	let searchQuery = $state('');
	let apps = $state<ApplicationResponse[]>([]);
	let campaigns = $state<CampaignResponse[]>([]);
	let segments = $state<SegmentResponse[]>([]);
	let selectedAppId = $state<string | null>(null);

	const filteredApps = $derived(apps.filter((a) => (a.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredCampaigns = $derived(campaigns.filter((c) => (c.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredSegments = $derived(segments.filter((s) => (s.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const resp = await pp.send(new GetAppsCommand({}));
			apps = resp.ApplicationsResponse?.Item ?? [];
		} catch (e) {
			toast.error('Failed to load Pinpoint data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadAppDetails(appId: string) {
		selectedAppId = appId;
		try {
			const [campResp, segResp] = await Promise.all([
				pp.send(new GetCampaignsCommand({ ApplicationId: appId })),
				pp.send(new GetSegmentsCommand({ ApplicationId: appId }))
			]);
			campaigns = campResp.CampaignsResponse?.Item ?? [];
			segments = segResp.SegmentsResponse?.Item ?? [];
			activeTab = 'campaigns';
		} catch (e) {
			toast.error('Failed to load app details: ' + String(e));
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<MapPin class="w-7 h-7 text-pink-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Pinpoint</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Multichannel marketing communications service</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-pink-100 dark:bg-pink-900/30 rounded-lg"><MapPin class="w-5 h-5 text-pink-600 dark:text-pink-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{apps.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Applications</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><Megaphone class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{campaigns.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Campaigns</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Users class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{segments.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Segments</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['apps', 'Applications'], ['campaigns', 'Campaigns'], ['segments', 'Segments']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
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
			{:else if activeTab === 'apps'}
				{#if filteredApps.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No applications found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredApps as app}
							<button onclick={() => loadAppDetails(app.Id ?? '')}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left">
								<div class="flex items-center gap-3">
									<MapPin class="w-5 h-5 text-pink-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{app.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{app.Id}</p>
									</div>
								</div>
								<span class="text-xs text-gray-400">View details →</span>
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'campaigns'}
				{#if filteredCampaigns.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No campaigns found. Select an application to view its campaigns.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredCampaigns as camp}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Megaphone class="w-5 h-5 text-orange-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{camp.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{camp.Id}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{camp.State?.CampaignStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'segments'}
				{#if filteredSegments.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No segments found. Select an application to view its segments.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredSegments as seg}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Users class="w-5 h-5 text-blue-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{seg.Name}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{seg.SegmentType} · {seg.Id}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
