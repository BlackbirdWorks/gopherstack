<script lang="ts">
	import { onMount } from 'svelte';
	import { getTextractClient } from '$lib/aws-client';
	import {
		ListAdaptersCommand,
		ListAdapterVersionsCommand,
		type AdapterOverview,
		type AdapterVersionOverview
	} from '@aws-sdk/client-textract';
	import { toast } from 'svelte-sonner';
	import { ScanLine, RefreshCw, Search, FileText, Layers, Activity } from 'lucide-svelte';

	const tx = getTextractClient();

	let loading = $state(false);
	let activeTab = $state<'adapters' | 'versions'>('adapters');
	let searchQuery = $state('');
	let adapters = $state<AdapterOverview[]>([]);
	let versions = $state<AdapterVersionOverview[]>([]);
	let selectedAdapterId = $state<string | null>(null);

	const filteredAdapters = $derived(adapters.filter((a) => (a.AdapterId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredVersions = $derived(versions.filter((v) => (v.AdapterId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const adaptersResp = await tx.send(new ListAdaptersCommand({}));
			adapters = adaptersResp.Adapters ?? [];
			if (adapters.length > 0 && selectedAdapterId === null) {
				selectedAdapterId = adapters[0].AdapterId ?? null;
			}
			if (selectedAdapterId) {
				const versionsResp = await tx.send(new ListAdapterVersionsCommand({ AdapterId: selectedAdapterId }));
				versions = versionsResp.AdapterVersions ?? [];
			} else {
				versions = [];
			}
		} catch (e) {
			toast.error('Failed to load Textract data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<ScanLine class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Textract</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Automatically extract text and data from scanned documents</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Layers class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{adapters.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Adapters</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Activity class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{versions.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Adapter Versions</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><FileText class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{versions.filter((v) => v.Status === 'ACTIVE').length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Active Versions</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['adapters', 'Adapters'], ['versions', 'Adapter Versions']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
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
			{:else if activeTab === 'adapters'}
				{#if filteredAdapters.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No adapters found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredAdapters as adapter}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Layers class="w-5 h-5 text-blue-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{adapter.AdapterId}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{adapter.AdapterName}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'versions'}
				{#if filteredVersions.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No adapter versions found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredVersions as ver}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-green-500" />
									<p class="font-medium text-gray-900 dark:text-white">{ver.AdapterVersion}</p>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {ver.Status === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{ver.Status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
