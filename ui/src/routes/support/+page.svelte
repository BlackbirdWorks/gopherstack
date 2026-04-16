<script lang="ts">
	import { onMount } from 'svelte';
	import { getSupportClient } from '$lib/aws-client';
	import {
		DescribeCasesCommand,
		DescribeSeverityLevelsCommand,
		DescribeServicesCommand,
		type CaseDetails,
		type SeverityLevel,
		type Service
	} from '@aws-sdk/client-support';
	import { toast } from 'svelte-sonner';
	import { LifeBuoy, RefreshCw, Search, AlertTriangle, CheckCircle, Clock } from 'lucide-svelte';

	const support = getSupportClient();

	let loading = $state(false);
	let activeTab = $state<'cases' | 'services'>('cases');
	let searchQuery = $state('');
	let cases = $state<CaseDetails[]>([]);
	let severityLevels = $state<SeverityLevel[]>([]);
	let services = $state<Service[]>([]);
	let includedResolved = $state(false);

	const filteredCases = $derived(cases.filter((c) => ((c.subject ?? '') + (c.caseId ?? '')).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredServices = $derived(services.filter((s) => (s.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const openCases = $derived(cases.filter((c) => c.status !== 'resolved').length);
	const resolvedCases = $derived(cases.filter((c) => c.status === 'resolved').length);

	async function loadData() {
		loading = true;
		try {
			const [caseResp, sevResp, svcResp] = await Promise.all([
				support.send(new DescribeCasesCommand({ includeResolvedCases: includedResolved })),
				support.send(new DescribeSeverityLevelsCommand({})),
				support.send(new DescribeServicesCommand({}))
			]);
			cases = caseResp.cases ?? [];
			severityLevels = sevResp.severityLevels ?? [];
			services = svcResp.services ?? [];
		} catch (e) {
			toast.error('Failed to load Support data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<LifeBuoy class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Support</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Manage support cases and access AWS Support resources</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><LifeBuoy class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{cases.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Total Cases</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><AlertTriangle class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{openCases}</p><p class="text-sm text-gray-500 dark:text-gray-400">Open</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{resolvedCases}</p><p class="text-sm text-gray-500 dark:text-gray-400">Resolved</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Clock class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{severityLevels.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Severity Levels</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between items-start sm:items-center">
			<div class="flex gap-2">
				{#each [['cases', 'Support Cases'], ['services', 'AWS Services']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="flex gap-2 items-center">
				{#if activeTab === 'cases'}
					<label class="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400">
						<input type="checkbox" bind:checked={includedResolved} onchange={loadData} class="rounded" />
						Include resolved
					</label>
				{/if}
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-48" />
				</div>
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'cases'}
				{#if filteredCases.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No support cases found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredCases as c}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<LifeBuoy class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{c.subject}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{c.caseId} · {c.serviceCode}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {c.status === 'resolved' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400'}">{c.status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'services'}
				{#if filteredServices.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No services found</div>
				{:else}
					<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
						{#each filteredServices as svc}
							<div class="p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<p class="font-medium text-gray-900 dark:text-white text-sm">{svc.name}</p>
								<p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">{svc.code}</p>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
