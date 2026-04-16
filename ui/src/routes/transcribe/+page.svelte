<script lang="ts">
	import { onMount } from 'svelte';
	import { getTranscribeClient } from '$lib/aws-client';
	import {
		ListTranscriptionJobsCommand,
		ListVocabulariesCommand,
		type TranscriptionJobSummary,
		type VocabularyInfo
	} from '@aws-sdk/client-transcribe';
	import { toast } from 'svelte-sonner';
	import { Mic, RefreshCw, Search, BookOpen, Activity, CheckCircle } from 'lucide-svelte';

	const tr = getTranscribeClient();

	let loading = $state(false);
	let activeTab = $state<'jobs' | 'vocabularies'>('jobs');
	let searchQuery = $state('');
	let jobs = $state<TranscriptionJobSummary[]>([]);
	let vocabularies = $state<VocabularyInfo[]>([]);

	const filteredJobs = $derived(jobs.filter((j) => (j.TranscriptionJobName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredVocabularies = $derived(vocabularies.filter((v) => (v.VocabularyName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const completedJobs = $derived(jobs.filter((j) => j.TranscriptionJobStatus === 'COMPLETED').length);

	async function loadData() {
		loading = true;
		try {
			const [jobsResp, vocResp] = await Promise.all([
				tr.send(new ListTranscriptionJobsCommand({})),
				tr.send(new ListVocabulariesCommand({}))
			]);
			jobs = jobsResp.TranscriptionJobSummaries ?? [];
			vocabularies = vocResp.Vocabularies ?? [];
		} catch (e) {
			toast.error('Failed to load Transcribe data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Mic class="w-7 h-7 text-red-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Transcribe</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Automatic speech recognition to convert audio to text</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg"><Activity class="w-5 h-5 text-red-600 dark:text-red-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{jobs.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Transcription Jobs</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{completedJobs}</p><p class="text-sm text-gray-500 dark:text-gray-400">Completed</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><BookOpen class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{vocabularies.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Vocabularies</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Mic class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{jobs.filter((j) => j.TranscriptionJobStatus === 'IN_PROGRESS').length}</p><p class="text-sm text-gray-500 dark:text-gray-400">In Progress</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['jobs', 'Transcription Jobs'], ['vocabularies', 'Vocabularies']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
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
			{:else if activeTab === 'jobs'}
				{#if filteredJobs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No transcription jobs found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredJobs as job}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Mic class="w-5 h-5 text-red-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{job.TranscriptionJobName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{job.LanguageCode}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {job.TranscriptionJobStatus === 'COMPLETED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : job.TranscriptionJobStatus === 'FAILED' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400' : 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400'}">{job.TranscriptionJobStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'vocabularies'}
				{#if filteredVocabularies.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No vocabularies found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredVocabularies as vocab}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<BookOpen class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{vocab.VocabularyName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{vocab.LanguageCode}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">{vocab.VocabularyState}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
