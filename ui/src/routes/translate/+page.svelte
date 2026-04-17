<script lang="ts">
	import { onMount } from 'svelte';
	import { getTranslateClient } from '$lib/aws-client';
	import {
		ListTerminologiesCommand,
		ListParallelDataCommand,
		ListTextTranslationJobsCommand,
		type TerminologyProperties,
		type ParallelDataProperties,
		type TextTranslationJobProperties
	} from '@aws-sdk/client-translate';
	import { toast } from 'svelte-sonner';
	import { Languages, RefreshCw, Search, BookOpen, Activity, Database } from 'lucide-svelte';

	const tl = getTranslateClient();

	let loading = $state(false);
	let activeTab = $state<'terminologies' | 'paralleldata' | 'jobs'>('terminologies');
	let searchQuery = $state('');
	let terminologies = $state<TerminologyProperties[]>([]);
	let parallelData = $state<ParallelDataProperties[]>([]);
	let jobs = $state<TextTranslationJobProperties[]>([]);

	const filteredTerminologies = $derived(terminologies.filter((t) => (t.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredParallelData = $derived(parallelData.filter((p) => (p.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredJobs = $derived(jobs.filter((j) => (j.JobId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const [termResp, parallelResp, jobsResp] = await Promise.all([
				tl.send(new ListTerminologiesCommand({})),
				tl.send(new ListParallelDataCommand({})),
				tl.send(new ListTextTranslationJobsCommand({}))
			]);
			terminologies = termResp.TerminologyPropertiesList ?? [];
			parallelData = parallelResp.ParallelDataPropertiesList ?? [];
			jobs = jobsResp.TextTranslationJobPropertiesList ?? [];
		} catch (e) {
			toast.error('Failed to load Translate data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Languages class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Translate</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Neural machine translation service</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><BookOpen class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{terminologies.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Terminologies</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Database class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{parallelData.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Parallel Data</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Activity class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{jobs.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Translation Jobs</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['terminologies', 'Terminologies'], ['paralleldata', 'Parallel Data'], ['jobs', 'Translation Jobs']] as [tab, label]}
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
			{:else if activeTab === 'terminologies'}
				{#if filteredTerminologies.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No terminologies found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTerminologies as term}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<BookOpen class="w-5 h-5 text-blue-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{term.Name}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{term.SourceLanguageCode} → {(term.TargetLanguageCodes ?? []).join(', ')}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'paralleldata'}
				{#if filteredParallelData.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No parallel data found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredParallelData as pd}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Database class="w-5 h-5 text-purple-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{pd.Name}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{pd.Status}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'jobs'}
				{#if filteredJobs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No translation jobs found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredJobs as job}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-green-500" />
									<p class="font-medium text-gray-900 dark:text-white">{job.JobId}</p>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {job.JobStatus === 'COMPLETED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{job.JobStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
