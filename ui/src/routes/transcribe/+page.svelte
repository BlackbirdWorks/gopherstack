<script lang="ts">
	import { onMount } from 'svelte';
	import { getTranscribeClient } from '$lib/aws-client';
	import {
		ListTranscriptionJobsCommand,
		ListVocabulariesCommand,
		ListCallAnalyticsJobsCommand,
		StartTranscriptionJobCommand,
		CreateVocabularyCommand,
		DeleteVocabularyCommand,
		StartCallAnalyticsJobCommand,
		DeleteCallAnalyticsJobCommand,
		GetTranscriptionJobCommand,
		type TranscribeClient,
		type TranscriptionJobSummary,
		type VocabularyInfo,
		type CallAnalyticsJobSummary
	} from '@aws-sdk/client-transcribe';
	import { toast } from 'svelte-sonner';
	import {
		Mic,
		RefreshCw,
		Search,
		BookOpen,
		Activity,
		CheckCircle,
		Plus,
		Trash2,
		Phone,
		Download
	} from 'lucide-svelte';

	let tr: TranscribeClient | undefined;
	function client(): TranscribeClient {
		return (tr ??= getTranscribeClient());
	}

	let downloadingJob = $state<string | null>(null);

	let loading = $state(false);
	let activeTab = $state<'jobs' | 'vocabularies' | 'analytics'>('jobs');
	let searchQuery = $state('');
	let jobs = $state<TranscriptionJobSummary[]>([]);
	let vocabularies = $state<VocabularyInfo[]>([]);
	let analyticsJobs = $state<CallAnalyticsJobSummary[]>([]);

	// Start job form
	let showStartJobForm = $state(false);
	let newJobName = $state('');
	let newJobLanguage = $state('en-US');
	let newJobMediaUri = $state('');
	let submittingJob = $state(false);

	// Vocab create form
	let showCreateVocabForm = $state(false);
	let newVocabName = $state('');
	let newVocabLanguage = $state('en-US');
	let submittingVocab = $state(false);

	// Analytics job form
	let showStartAnalyticsForm = $state(false);
	let newAnalyticsJobName = $state('');
	let newAnalyticsLanguage = $state('en-US');
	let newAnalyticsMediaUri = $state('');
	let submittingAnalytics = $state(false);

	const filteredJobs = $derived(
		jobs.filter((j) =>
			(j.TranscriptionJobName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredVocabularies = $derived(
		vocabularies.filter((v) =>
			(v.VocabularyName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredAnalyticsJobs = $derived(
		analyticsJobs.filter((j) =>
			(j.CallAnalyticsJobName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const completedJobs = $derived(
		jobs.filter((j) => j.TranscriptionJobStatus === 'COMPLETED').length
	);

	async function loadData() {
		loading = true;
		try {
			const [jobsResp, vocResp, analyticsResp] = await Promise.all([
				client().send(new ListTranscriptionJobsCommand({})),
				client().send(new ListVocabulariesCommand({})),
				client().send(new ListCallAnalyticsJobsCommand({}))
			]);
			jobs = jobsResp.TranscriptionJobSummaries ?? [];
			vocabularies = vocResp.Vocabularies ?? [];
			analyticsJobs = analyticsResp.CallAnalyticsJobSummaries ?? [];
		} catch (e) {
			toast.error('Failed to load Transcribe data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function startTranscriptionJob() {
		if (!newJobName.trim() || !newJobMediaUri.trim()) {
			toast.error('Job name and media URI are required');
			return;
		}
		submittingJob = true;
		try {
			await client().send(
				new StartTranscriptionJobCommand({
					TranscriptionJobName: newJobName.trim(),
					LanguageCode: newJobLanguage as 'en-US',
					Media: { MediaFileUri: newJobMediaUri.trim() }
				})
			);
			toast.success('Transcription job started: ' + newJobName.trim());
			newJobName = '';
			newJobMediaUri = '';
			showStartJobForm = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to start job: ' + String(e));
		} finally {
			submittingJob = false;
		}
	}

	async function createVocabulary() {
		if (!newVocabName.trim()) {
			toast.error('Vocabulary name is required');
			return;
		}
		submittingVocab = true;
		try {
			await client().send(
				new CreateVocabularyCommand({
					VocabularyName: newVocabName.trim(),
					LanguageCode: newVocabLanguage as 'en-US'
				})
			);
			toast.success('Vocabulary created: ' + newVocabName.trim());
			newVocabName = '';
			showCreateVocabForm = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to create vocabulary: ' + String(e));
		} finally {
			submittingVocab = false;
		}
	}

	async function deleteVocabulary(name: string) {
		try {
			await client().send(new DeleteVocabularyCommand({ VocabularyName: name }));
			toast.success('Vocabulary deleted: ' + name);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete vocabulary: ' + String(e));
		}
	}

	async function startCallAnalyticsJob() {
		if (!newAnalyticsJobName.trim() || !newAnalyticsMediaUri.trim()) {
			toast.error('Job name and media URI are required');
			return;
		}
		submittingAnalytics = true;
		try {
			await client().send(
				new StartCallAnalyticsJobCommand({
					CallAnalyticsJobName: newAnalyticsJobName.trim(),
					Media: { MediaFileUri: newAnalyticsMediaUri.trim() }
				})
			);
			toast.success('Call analytics job started: ' + newAnalyticsJobName.trim());
			newAnalyticsJobName = '';
			newAnalyticsMediaUri = '';
			showStartAnalyticsForm = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to start analytics job: ' + String(e));
		} finally {
			submittingAnalytics = false;
		}
	}

	async function deleteCallAnalyticsJob(name: string) {
		try {
			await client().send(new DeleteCallAnalyticsJobCommand({ CallAnalyticsJobName: name }));
			toast.success('Call analytics job deleted: ' + name);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete analytics job: ' + String(e));
		}
	}

	async function downloadTranscript(name: string) {
		downloadingJob = name;
		try {
			const detail = await client().send(
				new GetTranscriptionJobCommand({ TranscriptionJobName: name })
			);
			const uri = detail.TranscriptionJob?.Transcript?.TranscriptFileUri;
			if (!uri) {
				toast.error('No transcript file available for this job');
				return;
			}
			// Fetch the transcript JSON from the (presigned/service) URI and save locally.
			const resp = await fetch(uri);
			if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
			const text = await resp.text();
			const blob = new Blob([text], { type: 'application/json' });
			const url = URL.createObjectURL(blob);
			const a = document.createElement('a');
			a.href = url;
			a.download = `${name}-transcript.json`;
			a.click();
			URL.revokeObjectURL(url);
			toast.success('Transcript downloaded');
		} catch (e) {
			toast.error('Failed to download transcript: ' + String(e));
		} finally {
			downloadingJob = null;
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
				<p class="text-sm text-gray-500 dark:text-gray-400">
					Automatic speech recognition to convert audio to text
				</p>
			</div>
		</div>
		<button
			onclick={loadData}
			title="Refresh"
			class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
		>
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg">
				<Activity class="w-5 h-5 text-red-600 dark:text-red-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{jobs.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Transcription Jobs</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{completedJobs}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Completed</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<BookOpen class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{vocabularies.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Vocabularies</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Phone class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{analyticsJobs.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Analytics Jobs</p>
			</div>
		</div>
	</div>

	<div
		class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700"
	>
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<div class="flex gap-2">
				{#each [['jobs', 'Transcription Jobs'], ['vocabularies', 'Vocabularies'], ['analytics', 'Call Analytics']] as [tab, label]}
					<button
						onclick={() => {
							// eslint-disable-next-line @typescript-eslint/no-explicit-any
							activeTab = tab as any;
							searchQuery = '';
						}}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab
							? 'bg-red-600 text-white'
							: 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}"
					>
						{label}
					</button>
				{/each}
			</div>
			<div class="flex items-center gap-2">
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input
						bind:value={searchQuery}
						placeholder="Search..."
						class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64"
					/>
				</div>
				{#if activeTab === 'jobs'}
					<button
						onclick={() => (showStartJobForm = !showStartJobForm)}
						class="flex items-center gap-1 px-3 py-2 rounded-lg bg-red-600 text-white text-sm hover:bg-red-700"
					>
						<Plus class="w-4 h-4" /> Start Job
					</button>
				{:else if activeTab === 'vocabularies'}
					<button
						onclick={() => (showCreateVocabForm = !showCreateVocabForm)}
						class="flex items-center gap-1 px-3 py-2 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700"
					>
						<Plus class="w-4 h-4" /> Create
					</button>
				{:else if activeTab === 'analytics'}
					<button
						onclick={() => (showStartAnalyticsForm = !showStartAnalyticsForm)}
						class="flex items-center gap-1 px-3 py-2 rounded-lg bg-purple-600 text-white text-sm hover:bg-purple-700"
					>
						<Plus class="w-4 h-4" /> Start Job
					</button>
				{/if}
			</div>
		</div>

		{#if showStartJobForm && activeTab === 'jobs'}
			<div class="p-4 border-b border-slate-200 dark:border-slate-700 bg-gray-50 dark:bg-slate-700/30">
				<h3 class="text-sm font-semibold text-gray-900 dark:text-white mb-3">
					Start Transcription Job
				</h3>
				<div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
					<input
						bind:value={newJobName}
						placeholder="Job name"
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
					<select
						bind:value={newJobLanguage}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="en-US">English (US)</option>
						<option value="en-GB">English (UK)</option>
						<option value="es-US">Spanish (US)</option>
						<option value="fr-FR">French</option>
						<option value="de-DE">German</option>
					</select>
					<input
						bind:value={newJobMediaUri}
						placeholder="s3://bucket/key.mp3"
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div class="flex gap-2 mt-3">
					<button
						onclick={startTranscriptionJob}
						disabled={submittingJob}
						class="px-4 py-2 text-sm rounded-lg bg-red-600 text-white hover:bg-red-700 disabled:opacity-50"
					>
						{submittingJob ? 'Starting...' : 'Start'}
					</button>
					<button
						onclick={() => (showStartJobForm = false)}
						class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-slate-600"
					>
						Cancel
					</button>
				</div>
			</div>
		{/if}

		{#if showCreateVocabForm && activeTab === 'vocabularies'}
			<div class="p-4 border-b border-slate-200 dark:border-slate-700 bg-gray-50 dark:bg-slate-700/30">
				<h3 class="text-sm font-semibold text-gray-900 dark:text-white mb-3">Create Vocabulary</h3>
				<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
					<input
						bind:value={newVocabName}
						placeholder="Vocabulary name"
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
					<select
						bind:value={newVocabLanguage}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="en-US">English (US)</option>
						<option value="en-GB">English (UK)</option>
						<option value="es-US">Spanish (US)</option>
						<option value="fr-FR">French</option>
						<option value="de-DE">German</option>
					</select>
				</div>
				<div class="flex gap-2 mt-3">
					<button
						onclick={createVocabulary}
						disabled={submittingVocab}
						class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
					>
						{submittingVocab ? 'Creating...' : 'Create'}
					</button>
					<button
						onclick={() => (showCreateVocabForm = false)}
						class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-slate-600"
					>
						Cancel
					</button>
				</div>
			</div>
		{/if}

		{#if showStartAnalyticsForm && activeTab === 'analytics'}
			<div class="p-4 border-b border-slate-200 dark:border-slate-700 bg-gray-50 dark:bg-slate-700/30">
				<h3 class="text-sm font-semibold text-gray-900 dark:text-white mb-3">
					Start Call Analytics Job
				</h3>
				<div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
					<input
						bind:value={newAnalyticsJobName}
						placeholder="Job name"
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
					<select
						bind:value={newAnalyticsLanguage}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="en-US">English (US)</option>
						<option value="en-GB">English (UK)</option>
						<option value="es-US">Spanish (US)</option>
					</select>
					<input
						bind:value={newAnalyticsMediaUri}
						placeholder="s3://bucket/call.mp3"
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div class="flex gap-2 mt-3">
					<button
						onclick={startCallAnalyticsJob}
						disabled={submittingAnalytics}
						class="px-4 py-2 text-sm rounded-lg bg-purple-600 text-white hover:bg-purple-700 disabled:opacity-50"
					>
						{submittingAnalytics ? 'Starting...' : 'Start'}
					</button>
					<button
						onclick={() => (showStartAnalyticsForm = false)}
						class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-slate-600"
					>
						Cancel
					</button>
				</div>
			</div>
		{/if}

		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'jobs'}
				{#if filteredJobs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No transcription jobs found
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredJobs as job}
							<div
								class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50"
							>
								<div class="flex items-center gap-3">
									<Mic class="w-5 h-5 text-red-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">
											{job.TranscriptionJobName}
										</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{job.LanguageCode}</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									<span
										class="text-xs px-2 py-1 rounded-full {job.TranscriptionJobStatus === 'COMPLETED'
											? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
											: job.TranscriptionJobStatus === 'FAILED'
												? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
												: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400'}"
										>{job.TranscriptionJobStatus}</span
									>
									{#if job.TranscriptionJobStatus === 'COMPLETED'}
										<button
											onclick={() => downloadTranscript(job.TranscriptionJobName ?? '')}
											disabled={downloadingJob === job.TranscriptionJobName}
											title="Download transcript"
											class="flex items-center gap-1 px-2 py-1 text-xs rounded-lg border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-slate-600 disabled:opacity-50"
										>
											{#if downloadingJob === job.TranscriptionJobName}
												<div class="w-3.5 h-3.5 animate-spin rounded-full border-b-2 border-gray-500"></div>
											{:else}
												<Download class="w-3.5 h-3.5" />
											{/if}
											Transcript
										</button>
									{/if}
								</div>
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
							<div
								class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50"
							>
								<div class="flex items-center gap-3">
									<BookOpen class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{vocab.VocabularyName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{vocab.LanguageCode}</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									<span
										class="text-xs px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400"
										>{vocab.VocabularyState}</span
									>
									<button
										onclick={() => deleteVocabulary(vocab.VocabularyName ?? '')}
										title="Delete vocabulary"
										class="p-1 text-red-500 hover:text-red-700 dark:hover:text-red-400"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'analytics'}
				{#if filteredAnalyticsJobs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No call analytics jobs found
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredAnalyticsJobs as job}
							<div
								class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50"
							>
								<div class="flex items-center gap-3">
									<Phone class="w-5 h-5 text-purple-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">
											{job.CallAnalyticsJobName}
										</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{job.LanguageCode}</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									<span
										class="text-xs px-2 py-1 rounded-full {job.CallAnalyticsJobStatus === 'COMPLETED'
											? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
											: job.CallAnalyticsJobStatus === 'FAILED'
												? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
												: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400'}"
										>{job.CallAnalyticsJobStatus}</span
									>
									<button
										onclick={() => deleteCallAnalyticsJob(job.CallAnalyticsJobName ?? '')}
										title="Delete job"
										class="p-1 text-red-500 hover:text-red-700 dark:hover:text-red-400"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
