<script lang="ts">
	import { onMount } from 'svelte';
	import { getRoute53ResolverClient } from '$lib/aws-client';
	import {
		ListResolverEndpointsCommand,
		ListResolverRulesCommand,
		type ResolverEndpoint,
		type ResolverRule
	} from '@aws-sdk/client-route53resolver';
	import { toast } from 'svelte-sonner';
	import { Globe, RefreshCw, Search, ArrowRightLeft, BookOpen } from 'lucide-svelte';

	const r53r = getRoute53ResolverClient();

	let loading = $state(false);
	let activeTab = $state<'endpoints' | 'rules'>('endpoints');
	let searchQuery = $state('');
	let endpoints = $state<ResolverEndpoint[]>([]);
	let rules = $state<ResolverRule[]>([]);

	const filteredEndpoints = $derived(endpoints.filter((e) => (e.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredRules = $derived(rules.filter((r) => (r.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const inboundEndpoints = $derived(endpoints.filter((e) => e.Direction === 'INBOUND').length);
	const outboundEndpoints = $derived(endpoints.filter((e) => e.Direction === 'OUTBOUND').length);

	async function loadData() {
		loading = true;
		try {
			const [epResp, ruleResp] = await Promise.all([
				r53r.send(new ListResolverEndpointsCommand({})),
				r53r.send(new ListResolverRulesCommand({}))
			]);
			endpoints = epResp.ResolverEndpoints ?? [];
			rules = ruleResp.ResolverRules ?? [];
		} catch (e) {
			toast.error('Failed to load Route 53 Resolver data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Globe class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Route 53 Resolver</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">DNS resolution for hybrid cloud environments</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><ArrowRightLeft class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{endpoints.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Endpoints</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Globe class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{inboundEndpoints}</p><p class="text-sm text-gray-500 dark:text-gray-400">Inbound</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><Globe class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{outboundEndpoints}</p><p class="text-sm text-gray-500 dark:text-gray-400">Outbound</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><BookOpen class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{rules.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Rules</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['endpoints', 'Endpoints'], ['rules', 'Rules']] as [tab, label]}
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
			{:else if activeTab === 'endpoints'}
				{#if filteredEndpoints.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resolver endpoints found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredEndpoints as endpoint}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<ArrowRightLeft class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{endpoint.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{endpoint.IpAddressCount} IPs · {endpoint.Direction}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {endpoint.Status === 'OPERATIONAL' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{endpoint.Status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'rules'}
				{#if filteredRules.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resolver rules found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredRules as rule}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<BookOpen class="w-5 h-5 text-purple-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{rule.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{rule.DomainName} · {rule.RuleType}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{rule.Status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
