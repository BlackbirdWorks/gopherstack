<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getTranslateClient } from '$lib/aws-client';
	import {
		ListTerminologiesCommand,
		ListParallelDataCommand,
		ListTextTranslationJobsCommand,
		TranslateTextCommand,
		type TerminologyProperties,
		type ParallelDataProperties,
		type TextTranslationJobProperties
	} from '@aws-sdk/client-translate';
	import { toast } from 'svelte-sonner';
	import { Languages, RefreshCw, Search, BookOpen, Activity, Database, Play } from 'lucide-svelte';

	const tl = regionalClient(getTranslateClient);

	let loading = $state(false);
	let activeTab = $state<'terminologies' | 'paralleldata' | 'jobs' | 'translate'>('terminologies');
	let searchQuery = $state('');
	let terminologies = $state<TerminologyProperties[]>([]);
	let parallelData = $state<ParallelDataProperties[]>([]);
	let jobs = $state<TextTranslationJobProperties[]>([]);

	// Run-translation tester
	let sourceText = $state('');
	let sourceLang = $state('auto');
	let targetLang = $state('es');
	let translating = $state(false);
	let translatedText = $state('');
	let detectedSourceLang = $state('');

	const commonLangs = [
		['auto', 'Auto-detect'],
		['en', 'English'],
		['es', 'Spanish'],
		['fr', 'French'],
		['de', 'German'],
		['it', 'Italian'],
		['pt', 'Portuguese'],
		['ja', 'Japanese'],
		['ko', 'Korean'],
		['zh', 'Chinese (Simplified)'],
		['ar', 'Arabic'],
		['hi', 'Hindi'],
		['ru', 'Russian']
	];

	async function runTranslation() {
		if (!sourceText.trim()) {
			toast.error('Enter text to translate');
			return;
		}
		translating = true;
		translatedText = '';
		detectedSourceLang = '';
		try {
			const resp = await tl().send(
				new TranslateTextCommand({
					Text: sourceText,
					SourceLanguageCode: sourceLang,
					TargetLanguageCode: targetLang
				})
			);
			translatedText = resp.TranslatedText ?? '';
			detectedSourceLang = resp.SourceLanguageCode ?? '';
		} catch (e) {
			toast.error('Translation failed: ' + String(e));
		} finally {
			translating = false;
		}
	}

	const filteredTerminologies = $derived(terminologies.filter((t) => (t.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredParallelData = $derived(parallelData.filter((p) => (p.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredJobs = $derived(jobs.filter((j) => (j.JobId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const [termResp, parallelResp, jobsResp] = await Promise.all([
				tl().send(new ListTerminologiesCommand({})),
				tl().send(new ListParallelDataCommand({})),
				tl().send(new ListTextTranslationJobsCommand({}))
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

	onRegionChange(loadData);
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
				{#each [['terminologies', 'Terminologies'], ['paralleldata', 'Parallel Data'], ['jobs', 'Translation Jobs'], ['translate', 'Run Translation']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			{#if activeTab !== 'translate'}
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
				</div>
			{/if}
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
			{:else if activeTab === 'translate'}
				<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
					<div class="space-y-3">
						<div class="flex items-center gap-2">
							<label class="text-xs font-medium text-gray-500 dark:text-gray-400">Source</label>
							<select bind:value={sourceLang} class="text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-2 py-1">
								{#each commonLangs as [code, name]}
									<option value={code}>{name}</option>
								{/each}
							</select>
						</div>
						<textarea bind:value={sourceText} rows="8" placeholder="Enter text to translate..." class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
						<div class="flex items-center gap-2">
							<label class="text-xs font-medium text-gray-500 dark:text-gray-400">Target</label>
							<select bind:value={targetLang} class="text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-2 py-1">
								{#each commonLangs.filter(([c]) => c !== 'auto') as [code, name]}
									<option value={code}>{name}</option>
								{/each}
							</select>
							<button onclick={runTranslation} disabled={translating} class="ml-auto flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm font-medium disabled:opacity-50">
								<Play class="w-4 h-4" /> {translating ? 'Translating...' : 'Translate'}
							</button>
						</div>
					</div>
					<div class="space-y-3">
						<label class="text-xs font-medium text-gray-500 dark:text-gray-400">Result</label>
						<div class="w-full min-h-[200px] px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-gray-50 dark:bg-slate-900 text-gray-900 dark:text-white whitespace-pre-wrap">{translatedText || (translating ? 'Translating…' : 'Translation appears here')}</div>
						{#if detectedSourceLang}
							<p class="text-xs text-gray-500 dark:text-gray-400">Detected source language: <span class="font-mono">{detectedSourceLang}</span></p>
						{/if}
					</div>
				</div>
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
