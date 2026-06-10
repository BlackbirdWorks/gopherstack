<script lang="ts">
	import { onMount } from 'svelte';
	import { getMacie2Client } from '$lib/aws-client';
	import {
		ListAllowListsCommand,
		ListClassificationJobsCommand,
		ListCustomDataIdentifiersCommand,
		type AllowListSummary,
		type CustomDataIdentifierSummary,
		type JobSummary
	} from '@aws-sdk/client-macie2';
	import { toast } from 'svelte-sonner';
	import { RefreshCw, Search, ShieldAlert } from 'lucide-svelte';

	const client = getMacie2Client();

	const activeStatuses = new Set<string>(['ACTIVE', 'AVAILABLE', 'ENABLED', 'RUNNING', 'COMPLETE', 'COMPLETED', 'IDLE', 'Active', 'opt-in-not-required', 'ENABLED_BY_DEFAULT']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s)) ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let loading = $state(false);
	let activeTab = $state<'jobs' | 'identifiers' | 'allowlists'>('jobs');
	let searchQuery = $state('');
	let jobsData = $state<JobSummary[]>([]);
	let identifiersData = $state<CustomDataIdentifierSummary[]>([]);
	let allowlistsData = $state<AllowListSummary[]>([]);

	const filteredJobs = $derived(jobsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredIdentifiers = $derived(identifiersData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredAllowlists = $derived(allowlistsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			if (activeTab === 'jobs') {
				const resp = await client.send(new ListClassificationJobsCommand({}));
				jobsData = resp.items ?? [];
			}
			if (activeTab === 'identifiers') {
				const resp = await client.send(new ListCustomDataIdentifiersCommand({}));
				identifiersData = resp.items ?? [];
			}
			if (activeTab === 'allowlists') {
				const resp = await client.send(new ListAllowListsCommand({}));
				allowlistsData = resp.allowLists ?? [];
			}
		} catch (e) {
			toast.error('Failed to load Amazon Macie data: ' + String(e));
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
			<ShieldAlert class="w-7 h-7 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Macie</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Sensitive data discovery for S3</p>
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
				{#each [['jobs', 'Classification Jobs'], ['identifiers', 'Custom Data Identifiers'], ['allowlists', 'Allow Lists']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
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
			{:else if activeTab === 'jobs'}
				{#if filteredJobs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No classification jobs found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredJobs as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<ShieldAlert class="w-5 h-5 text-orange-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.jobType ?? '-'} · ${a.jobId ?? ''}`}</p>
									</div>
								</div>
								{#if a.jobStatus}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.jobStatus)}">{a.jobStatus}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'identifiers'}
				{#if filteredIdentifiers.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No custom data identifiers found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredIdentifiers as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<ShieldAlert class="w-5 h-5 text-orange-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.description ?? a.id ?? ''}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'allowlists'}
				{#if filteredAllowlists.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No allow lists found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredAllowlists as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<ShieldAlert class="w-5 h-5 text-orange-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.description ?? a.id ?? ''}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
