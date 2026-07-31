<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getMediaLiveClient } from '$lib/aws-client';
	import {
		ListChannelsCommand,
		ListInputSecurityGroupsCommand,
		ListInputsCommand,
		ListMultiplexesCommand,
		type ChannelSummary,
		type Input,
		type InputSecurityGroup,
		type MultiplexSummary
	} from '@aws-sdk/client-medialive';
	import { toast } from 'svelte-sonner';
	import { Radio, RefreshCw, Search } from 'lucide-svelte';

	const client = regionalClient(getMediaLiveClient);

	const activeStatuses = new Set<string>(['ACTIVE', 'AVAILABLE', 'ENABLED', 'RUNNING', 'COMPLETE', 'COMPLETED', 'IDLE', 'Active', 'opt-in-not-required', 'ENABLED_BY_DEFAULT']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s)) ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let loading = $state(false);
	let activeTab = $state<'channels' | 'inputs' | 'inputsg' | 'multiplexes'>('channels');
	let searchQuery = $state('');
	let channelsData = $state<ChannelSummary[]>([]);
	let inputsData = $state<Input[]>([]);
	let inputsgData = $state<InputSecurityGroup[]>([]);
	let multiplexesData = $state<MultiplexSummary[]>([]);

	const filteredChannels = $derived(channelsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredInputs = $derived(inputsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredInputsg = $derived(inputsgData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredMultiplexes = $derived(multiplexesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			// `activeTab` is read with `untrack` so it never becomes a
			// dependency of the `onRegionChange` effect below -- switchTab()
			// already writes activeTab and calls loadData() directly, so
			// letting the effect also depend on activeTab would double-fetch
			// on every region change.
			const tab = untrack(() => activeTab);
			if (tab === 'channels') {
				const resp = await client().send(new ListChannelsCommand({}));
				channelsData = resp.Channels ?? [];
			}
			if (tab === 'inputs') {
				const resp = await client().send(new ListInputsCommand({}));
				inputsData = resp.Inputs ?? [];
			}
			if (tab === 'inputsg') {
				const resp = await client().send(new ListInputSecurityGroupsCommand({}));
				inputsgData = resp.InputSecurityGroups ?? [];
			}
			if (tab === 'multiplexes') {
				const resp = await client().send(new ListMultiplexesCommand({}));
				multiplexesData = resp.Multiplexes ?? [];
			}
		} catch (e) {
			toast.error('Failed to load AWS Elemental MediaLive data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		loadData();
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between flex-wrap gap-3">
		<div class="flex items-center gap-3">
			<Radio class="w-7 h-7 text-red-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Elemental MediaLive</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Live video processing</p>
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
				{#each [['channels', 'Channels'], ['inputs', 'Inputs'], ['inputsg', 'Input Security Groups'], ['multiplexes', 'Multiplexes']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-red-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
									<Radio class="w-5 h-5 text-red-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.ChannelClass ?? '-'} · ${a.Id ?? ''}`}</p>
									</div>
								</div>
								{#if a.State}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.State)}">{a.State}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'inputs'}
				{#if filteredInputs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No inputs found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredInputs as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Radio class="w-5 h-5 text-red-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.Type ?? '-'} · ${a.Id ?? ''}`}</p>
									</div>
								</div>
								{#if a.State}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.State)}">{a.State}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'inputsg'}
				{#if filteredInputsg.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No input security groups found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredInputsg as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Radio class="w-5 h-5 text-red-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Id ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.Arn ?? ''}`}</p>
									</div>
								</div>
								{#if a.State}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.State)}">{a.State}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'multiplexes'}
				{#if filteredMultiplexes.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No multiplexes found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredMultiplexes as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Radio class="w-5 h-5 text-red-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.Id ?? ''}`}</p>
									</div>
								</div>
								{#if a.State}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.State)}">{a.State}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
