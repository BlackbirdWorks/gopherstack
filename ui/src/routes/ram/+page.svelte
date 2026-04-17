<script lang="ts">
	import { onMount } from 'svelte';
	import { getRAMClient } from '$lib/aws-client';
	import {
		GetResourceSharesCommand,
		ListResourcesCommand,
		ListPrincipalsCommand,
		type ResourceShare,
		type Resource,
		type Principal
	} from '@aws-sdk/client-ram';
	import { toast } from 'svelte-sonner';
	import { Share2, RefreshCw, Search, Users, Box, CheckCircle } from 'lucide-svelte';

	const ram = getRAMClient();

	let loading = $state(false);
	let activeTab = $state<'shares' | 'resources' | 'principals'>('shares');
	let searchQuery = $state('');
	let shares = $state<ResourceShare[]>([]);
	let resources = $state<Resource[]>([]);
	let principals = $state<Principal[]>([]);

	const filteredShares = $derived(shares.filter((s) => (s.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredResources = $derived(resources.filter((r) => (r.arn ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredPrincipals = $derived(principals.filter((p) => (p.id ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const activeShares = $derived(shares.filter((s) => s.status === 'ACTIVE').length);

	async function loadData() {
		loading = true;
		try {
			const [sharesResp, resourcesResp, principalsResp] = await Promise.all([
				ram.send(new GetResourceSharesCommand({ resourceOwner: 'SELF' })),
				ram.send(new ListResourcesCommand({ resourceOwner: 'SELF' })),
				ram.send(new ListPrincipalsCommand({ resourceOwner: 'SELF' }))
			]);
			shares = sharesResp.resourceShares ?? [];
			resources = resourcesResp.resources ?? [];
			principals = principalsResp.principals ?? [];
		} catch (e) {
			toast.error('Failed to load RAM data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Share2 class="w-7 h-7 text-cyan-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Resource Access Manager</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Share AWS resources across accounts and organizations</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-cyan-100 dark:bg-cyan-900/30 rounded-lg"><Share2 class="w-5 h-5 text-cyan-600 dark:text-cyan-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{shares.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Resource Shares</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{activeShares}</p><p class="text-sm text-gray-500 dark:text-gray-400">Active Shares</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Box class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{resources.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Shared Resources</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Users class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{principals.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Principals</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['shares', 'Resource Shares'], ['resources', 'Resources'], ['principals', 'Principals']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-cyan-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'shares'}
				{#if filteredShares.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resource shares found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredShares as share}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Share2 class="w-5 h-5 text-cyan-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{share.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{share.resourceShareArn}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {share.status === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{share.status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'resources'}
				{#if filteredResources.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No shared resources found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredResources as res}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Box class="w-5 h-5 text-blue-500" />
								<div>
									<p class="font-medium text-sm text-gray-900 dark:text-white truncate max-w-lg">{res.arn}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{res.type} · {res.status}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'principals'}
				{#if filteredPrincipals.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No principals found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredPrincipals as principal}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Users class="w-5 h-5 text-purple-500" />
								<p class="text-sm text-gray-900 dark:text-white">{principal.id}</p>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
