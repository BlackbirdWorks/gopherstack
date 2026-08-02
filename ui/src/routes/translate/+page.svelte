<script lang="ts">
	// Amazon Translate is a data plane (TranslateText/TranslateDocument, both
	// stateless, no backing resource) with a few managed-resource families
	// layered on top: Terminology (custom glossaries), ParallelData
	// (translation memory), and TextTranslationJob (async batch jobs). This
	// page's floor: full CRUD on Terminology/ParallelData, start/stop/detail
	// on TextTranslationJob (no delete op in the real API -- a job's outcome
	// is permanent, matching redshiftdata/rdsdata's precedent of not
	// inventing a delete for a data-plane execution record), plus the
	// existing TranslateText tester.
	//
	// Out of scope for this page (real, implemented, just not surfaced):
	// TranslateDocument (a second data-plane op alongside TranslateText,
	// requiring a base64 document payload + ContentType -- lower-traffic
	// than the interactive text tester) and ListLanguages (a static
	// read-only catalog already partially hardcoded into the tester's
	// language picker).
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getTranslateClient } from '$lib/aws-client';
	import {
		ListTerminologiesCommand,
		GetTerminologyCommand,
		ImportTerminologyCommand,
		DeleteTerminologyCommand,
		ListParallelDataCommand,
		GetParallelDataCommand,
		CreateParallelDataCommand,
		UpdateParallelDataCommand,
		DeleteParallelDataCommand,
		ListTextTranslationJobsCommand,
		DescribeTextTranslationJobCommand,
		StartTextTranslationJobCommand,
		StopTextTranslationJobCommand,
		TranslateTextCommand,
		type TerminologyProperties,
		type ParallelDataProperties,
		type TextTranslationJobProperties,
		type GetTerminologyCommandOutput,
		type GetParallelDataCommandOutput
	} from '@aws-sdk/client-translate';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate, formatBytes } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { Languages, RefreshCw, Plus, Trash2, Eye, Square, Play, BookOpen, Database, Activity } from 'lucide-svelte';

	const tl = regionalClient(getTranslateClient);

	type TabId = 'terminologies' | 'paralleldata' | 'jobs' | 'translate';

	const tabs: TabDef[] = [
		{ id: 'terminologies', label: 'Terminologies' },
		{ id: 'paralleldata', label: 'Parallel Data' },
		{ id: 'jobs', label: 'Translation Jobs' },
		{ id: 'translate', label: 'Run Translation' }
	];

	const FORMATS = ['CSV', 'TMX', 'TSV'];

	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	function statusClass(status: string | undefined): string {
		if (status === 'ACTIVE' || status === 'COMPLETED') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (status?.includes('FAILED')) return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		if (status === 'CREATING' || status === 'UPDATING' || status === 'IN_PROGRESS' || status === 'SUBMITTED') {
			return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400';
		}
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('terminologies');
	let searchQuery = $state('');

	let terminologies = $state<TerminologyProperties[]>([]);
	let parallelData = $state<ParallelDataProperties[]>([]);
	let jobs = $state<TextTranslationJobProperties[]>([]);

	async function fetchTerminologies(): Promise<void> {
		const resp = await tl().send(new ListTerminologiesCommand({}));
		terminologies = resp.TerminologyPropertiesList ?? [];
	}
	async function fetchParallelData(): Promise<void> {
		const resp = await tl().send(new ListParallelDataCommand({}));
		parallelData = resp.ParallelDataPropertiesList ?? [];
	}
	async function fetchJobs(): Promise<void> {
		const resp = await tl().send(new ListTextTranslationJobsCommand({}));
		jobs = resp.TextTranslationJobPropertiesList ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		terminologies: () => fetchTerminologies().catch(rethrowDescribed),
		paralleldata: () => fetchParallelData().catch(rethrowDescribed),
		jobs: () => fetchJobs().catch(rethrowDescribed),
		translate: () => Promise.resolve()
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		detailModal?.close();
		detailKind = null;
		tabLoader.refresh(untrack(() => activeTab));
	});

	function matches(q: string, ...fields: (string | undefined)[]): boolean {
		if (!q) return true;
		return fields.some((f) => (f ?? '').toLowerCase().includes(q));
	}

	const filteredTerminologies = $derived(terminologies.filter((t) => matches(searchQuery.toLowerCase(), t.Name)));
	const filteredParallelData = $derived(parallelData.filter((p) => matches(searchQuery.toLowerCase(), p.Name)));
	const filteredJobs = $derived(jobs.filter((j) => matches(searchQuery.toLowerCase(), j.JobName, j.JobId)));

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Terminology: create (import) / delete / detail ---

	let createTerminologyModal = $state<Modal | null>(null);
	let creatingTerminology = $state(false);
	let createTerminologyError = $state<string | null>(null);
	let newTerminologyName = $state('');
	let newTerminologyDescription = $state('');
	let newTerminologyFormat = $state('CSV');
	let newTerminologyData = $state('en,es\nhello,hola\n');

	function openCreateTerminologyModal(): void {
		createTerminologyError = null;
		newTerminologyName = '';
		newTerminologyDescription = '';
		newTerminologyFormat = 'CSV';
		newTerminologyData = 'en,es\nhello,hola\n';
		createTerminologyModal?.open();
	}

	async function submitCreateTerminology(): Promise<void> {
		if (!newTerminologyName || !newTerminologyData) {
			createTerminologyError = 'Name and terminology data are required.';
			return;
		}
		creatingTerminology = true;
		createTerminologyError = null;
		try {
			await tl().send(
				new ImportTerminologyCommand({
					Name: newTerminologyName,
					MergeStrategy: 'OVERWRITE',
					Description: newTerminologyDescription || undefined,
					TerminologyData: {
						File: new TextEncoder().encode(newTerminologyData),
						Format: newTerminologyFormat as never
					}
				})
			);
			toast.success('Terminology imported');
			createTerminologyModal?.close();
			await tabLoader.refresh('terminologies');
		} catch (e) {
			const msg = describeError(e);
			createTerminologyError = msg;
			toast.error(msg);
		} finally {
			creatingTerminology = false;
		}
	}

	async function deleteTerminology(t: TerminologyProperties): Promise<void> {
		if (!t.Name) return;
		const confirmed = await confirmDestructive({ title: 'Delete terminology', message: `Delete terminology "${t.Name}"? This cannot be undone.` });
		if (!confirmed) return;
		try {
			await tl().send(new DeleteTerminologyCommand({ Name: t.Name }));
			toast.success('Terminology deleted');
			await tabLoader.refresh('terminologies');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Parallel Data: create / update / delete / detail ---

	let createParallelModal = $state<Modal | null>(null);
	let creatingParallel = $state(false);
	let createParallelError = $state<string | null>(null);
	let newParallelName = $state('');
	let newParallelDescription = $state('');
	let newParallelS3Uri = $state('');
	let newParallelFormat = $state('TSV');

	function openCreateParallelModal(): void {
		createParallelError = null;
		newParallelName = '';
		newParallelDescription = '';
		newParallelS3Uri = '';
		newParallelFormat = 'TSV';
		createParallelModal?.open();
	}

	async function submitCreateParallel(): Promise<void> {
		if (!newParallelName || !newParallelS3Uri) {
			createParallelError = 'Name and S3 URI are required.';
			return;
		}
		creatingParallel = true;
		createParallelError = null;
		try {
			await tl().send(
				new CreateParallelDataCommand({
					Name: newParallelName,
					Description: newParallelDescription || undefined,
					ParallelDataConfig: { S3Uri: newParallelS3Uri, Format: newParallelFormat as never }
				})
			);
			toast.success('Parallel data creation started');
			createParallelModal?.close();
			await tabLoader.refresh('paralleldata');
		} catch (e) {
			const msg = describeError(e);
			createParallelError = msg;
			toast.error(msg);
		} finally {
			creatingParallel = false;
		}
	}

	let editParallelModal = $state<Modal | null>(null);
	let editingParallel = $state(false);
	let editParallelError = $state<string | null>(null);
	let editParallelName = $state('');
	let editParallelDescription = $state('');
	let editParallelS3Uri = $state('');
	let editParallelFormat = $state('TSV');

	function openEditParallel(): void {
		editParallelError = null;
		editParallelName = viewedParallelArn;
		editParallelDescription = viewedParallel?.ParallelDataProperties?.Description ?? '';
		editParallelS3Uri = '';
		editParallelFormat = viewedParallel?.ParallelDataProperties?.ParallelDataConfig?.Format ?? 'TSV';
		editParallelModal?.open();
	}

	async function submitEditParallel(): Promise<void> {
		if (!editParallelS3Uri) {
			editParallelError = 'A new S3 URI is required.';
			return;
		}
		editingParallel = true;
		editParallelError = null;
		try {
			await tl().send(
				new UpdateParallelDataCommand({
					Name: editParallelName,
					Description: editParallelDescription || undefined,
					ParallelDataConfig: { S3Uri: editParallelS3Uri, Format: editParallelFormat as never }
				})
			);
			toast.success('Parallel data update started');
			editParallelModal?.close();
			await tabLoader.refresh('paralleldata');
			await refreshParallelDetail();
		} catch (e) {
			const msg = describeError(e);
			editParallelError = msg;
			toast.error(msg);
		} finally {
			editingParallel = false;
		}
	}

	async function deleteParallel(p: ParallelDataProperties): Promise<void> {
		if (!p.Name) return;
		const confirmed = await confirmDestructive({ title: 'Delete parallel data', message: `Delete parallel data "${p.Name}"? This cannot be undone.` });
		if (!confirmed) return;
		try {
			await tl().send(new DeleteParallelDataCommand({ Name: p.Name }));
			toast.success('Parallel data deleted');
			await tabLoader.refresh('paralleldata');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Translation Jobs: start / stop / detail (no delete op) ---

	let createJobModal = $state<Modal | null>(null);
	let creatingJob = $state(false);
	let createJobError = $state<string | null>(null);
	let newJobName = $state('');
	let newJobRoleArn = $state('');
	let newJobInputS3Uri = $state('');
	let newJobInputContentType = $state('text/plain');
	let newJobOutputS3Uri = $state('');
	let newJobSourceLang = $state('en');
	let newJobTargetLangs = $state('es');

	function openCreateJobModal(): void {
		createJobError = null;
		newJobName = '';
		newJobRoleArn = '';
		newJobInputS3Uri = '';
		newJobInputContentType = 'text/plain';
		newJobOutputS3Uri = '';
		newJobSourceLang = 'en';
		newJobTargetLangs = 'es';
		createJobModal?.open();
	}

	async function submitCreateJob(): Promise<void> {
		const targetLangs = newJobTargetLangs.split(',').map((l) => l.trim()).filter(Boolean);
		if (!newJobRoleArn || !newJobInputS3Uri || !newJobOutputS3Uri || !newJobSourceLang || targetLangs.length === 0) {
			createJobError = 'Data access role, input/output S3 URIs, source language, and at least one target language are required.';
			return;
		}
		creatingJob = true;
		createJobError = null;
		try {
			await tl().send(
				new StartTextTranslationJobCommand({
					JobName: newJobName || undefined,
					DataAccessRoleArn: newJobRoleArn,
					InputDataConfig: { S3Uri: newJobInputS3Uri, ContentType: newJobInputContentType },
					OutputDataConfig: { S3Uri: newJobOutputS3Uri },
					SourceLanguageCode: newJobSourceLang,
					TargetLanguageCodes: targetLangs
				})
			);
			toast.success('Translation job started');
			createJobModal?.close();
			await tabLoader.refresh('jobs');
		} catch (e) {
			const msg = describeError(e);
			createJobError = msg;
			toast.error(msg);
		} finally {
			creatingJob = false;
		}
	}

	async function stopJob(j: TextTranslationJobProperties): Promise<void> {
		if (!j.JobId) return;
		const confirmed = await confirmDestructive({ title: 'Stop translation job', message: `Stop job "${j.JobName ?? j.JobId}"?`, confirmLabel: 'Stop' });
		if (!confirmed) return;
		try {
			await tl().send(new StopTextTranslationJobCommand({ JobId: j.JobId }));
			toast.success('Stop requested');
			await tabLoader.refresh('jobs');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Detail (shared modal per family) ---

	let detailModal = $state<Modal | null>(null);
	let detailKind = $state<'terminology' | 'paralleldata' | 'job' | null>(null);
	let detailLoading = $state(false);
	let detailError = $state<string | null>(null);
	let viewedTerminology = $state<GetTerminologyCommandOutput | null>(null);
	let viewedParallel = $state<GetParallelDataCommandOutput | null>(null);
	let viewedJob = $state<TextTranslationJobProperties | null>(null);
	let viewedTerminologyName = $state('');
	let viewedParallelArn = $state('');
	let viewedJobId = $state('');

	async function openTerminologyDetail(t: TerminologyProperties): Promise<void> {
		detailKind = 'terminology';
		viewedTerminology = null;
		detailError = null;
		detailModal?.open();
		if (!t.Name) return;
		viewedTerminologyName = t.Name;
		detailLoading = true;
		try {
			viewedTerminology = await tl().send(new GetTerminologyCommand({ Name: viewedTerminologyName }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openParallelDetail(p: ParallelDataProperties): Promise<void> {
		detailKind = 'paralleldata';
		viewedParallel = null;
		detailError = null;
		detailModal?.open();
		if (!p.Name) return;
		viewedParallelArn = p.Name;
		await refreshParallelDetail();
	}
	async function refreshParallelDetail(): Promise<void> {
		if (!viewedParallelArn) return;
		detailLoading = true;
		try {
			viewedParallel = await tl().send(new GetParallelDataCommand({ Name: viewedParallelArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openJobDetail(j: TextTranslationJobProperties): Promise<void> {
		detailKind = 'job';
		viewedJob = null;
		detailError = null;
		detailModal?.open();
		if (!j.JobId) return;
		viewedJobId = j.JobId;
		await refreshJobDetail();
	}
	async function refreshJobDetail(): Promise<void> {
		if (!viewedJobId) return;
		detailLoading = true;
		try {
			const resp = await tl().send(new DescribeTextTranslationJobCommand({ JobId: viewedJobId }));
			viewedJob = resp.TextTranslationJobProperties ?? null;
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	function refreshDetail(): void {
		if (detailKind === 'paralleldata') void refreshParallelDetail();
		else if (detailKind === 'job') void refreshJobDetail();
	}

	// --- Run Translation (stateless tester, unchanged) ---

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

	async function runTranslation(): Promise<void> {
		if (!sourceText.trim()) {
			toast.error('Enter text to translate');
			return;
		}
		translating = true;
		translatedText = '';
		detectedSourceLang = '';
		try {
			const resp = await tl().send(new TranslateTextCommand({ Text: sourceText, SourceLanguageCode: sourceLang, TargetLanguageCode: targetLang }));
			translatedText = resp.TranslatedText ?? '';
			detectedSourceLang = resp.SourceLanguageCode ?? '';
		} catch (e) {
			toast.error('Translation failed: ' + describeError(e));
		} finally {
			translating = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader icon={Languages} title="Amazon Translate" description="Neural machine translation service" onRefresh={handleRefresh} color="blue">
		{#snippet actions()}
			{#if activeTab === 'terminologies'}
				<button onclick={openCreateTerminologyModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Import terminology
				</button>
			{:else if activeTab === 'paralleldata'}
				<button onclick={openCreateParallelModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Create parallel data
				</button>
			{:else if activeTab === 'jobs'}
				<button onclick={openCreateJobModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Start translation job
				</button>
			{/if}
		{/snippet}
	</PageHeader>

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
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
			{#if activeTab !== 'translate'}
				<SearchInput bind:value={searchQuery} />
			{/if}
		</div>
		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'terminologies'}
				{#snippet terminologyLangsCell(t: TerminologyProperties)}
					<span>{t.SourceLanguageCode} → {(t.TargetLanguageCodes ?? []).join(', ')}</span>
				{/snippet}
				{#snippet terminologyActionsCell(t: TerminologyProperties)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openTerminologyDetail(t)} title="View" aria-label="View terminology {t.Name}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteTerminology(t)} title="Delete" aria-label="Delete terminology {t.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const terminologyColumns = defineColumns<TerminologyProperties>([
					{ key: 'Name', label: 'Name' },
					{ key: 'langs', label: 'Languages', render: terminologyLangsCell },
					{ key: 'actions', label: '', render: terminologyActionsCell }
				])}
				<DataTable rows={filteredTerminologies} rowKey={(t) => t.Name ?? ''} columns={terminologyColumns} loading={tabLoader.isLoading('terminologies')} emptyMessage="No terminologies found" />
			{:else if activeTab === 'paralleldata'}
				{#snippet parallelStatusCell(p: ParallelDataProperties)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(p.Status)}">{p.Status ?? '—'}</span>
				{/snippet}
				{#snippet parallelActionsCell(p: ParallelDataProperties)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openParallelDetail(p)} title="View" aria-label="View parallel data {p.Name}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteParallel(p)} title="Delete" aria-label="Delete parallel data {p.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const parallelColumns = defineColumns<ParallelDataProperties>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Status', label: 'Status', render: parallelStatusCell },
					{ key: 'actions', label: '', render: parallelActionsCell }
				])}
				<DataTable rows={filteredParallelData} rowKey={(p) => p.Name ?? ''} columns={parallelColumns} loading={tabLoader.isLoading('paralleldata')} emptyMessage="No parallel data found" />
			{:else if activeTab === 'jobs'}
				{#snippet jobStatusCell(j: TextTranslationJobProperties)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(j.JobStatus)}">{j.JobStatus ?? '—'}</span>
				{/snippet}
				{#snippet jobActionsCell(j: TextTranslationJobProperties)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openJobDetail(j)} title="View" aria-label="View job {j.JobName}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						{#if j.JobStatus === 'SUBMITTED' || j.JobStatus === 'IN_PROGRESS'}
							<button onclick={() => stopJob(j)} title="Stop" aria-label="Stop job {j.JobName}" class="text-gray-400 hover:text-red-500"><Square class="w-4 h-4" /></button>
						{/if}
					</div>
				{/snippet}
				{@const jobColumns = defineColumns<TextTranslationJobProperties>([
					{ key: 'JobName', label: 'Name' },
					{ key: 'JobId', label: 'Job ID' },
					{ key: 'JobStatus', label: 'Status', render: jobStatusCell },
					{ key: 'actions', label: '', render: jobActionsCell }
				])}
				<DataTable rows={filteredJobs} rowKey={(j) => j.JobId ?? ''} columns={jobColumns} loading={tabLoader.isLoading('jobs')} emptyMessage="No translation jobs found" />
			{:else if activeTab === 'translate'}
				<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
					<div class="space-y-3">
						<div class="flex items-center gap-2">
							<label for="tl-source-lang" class="text-xs font-medium text-gray-500 dark:text-gray-400">Source</label>
							<select id="tl-source-lang" bind:value={sourceLang} class="text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-2 py-1">
								{#each commonLangs as [code, name] (code)}
									<option value={code}>{name}</option>
								{/each}
							</select>
						</div>
						<textarea bind:value={sourceText} rows="8" placeholder="Enter text to translate..." class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
						<div class="flex items-center gap-2">
							<label for="tl-target-lang" class="text-xs font-medium text-gray-500 dark:text-gray-400">Target</label>
							<select id="tl-target-lang" bind:value={targetLang} class="text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-2 py-1">
								{#each commonLangs.filter(([c]) => c !== 'auto') as [code, name] (code)}
									<option value={code}>{name}</option>
								{/each}
							</select>
							<button onclick={runTranslation} disabled={translating} class="ml-auto flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 text-sm font-medium disabled:opacity-50">
								<Play class="w-4 h-4" /> {translating ? 'Translating...' : 'Translate'}
							</button>
						</div>
					</div>
					<div class="space-y-3">
						<span class="text-xs font-medium text-gray-500 dark:text-gray-400">Result</span>
						<div class="w-full min-h-[200px] px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-gray-50 dark:bg-slate-900 text-gray-900 dark:text-white whitespace-pre-wrap">{translatedText || (translating ? 'Translating…' : 'Translation appears here')}</div>
						{#if detectedSourceLang}
							<p class="text-xs text-gray-500 dark:text-gray-400">Detected source language: <span class="font-mono">{detectedSourceLang}</span></p>
						{/if}
					</div>
				</div>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createTerminologyModal} title="Import Terminology">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="tl-term-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="tl-term-name" bind:value={newTerminologyName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="tl-term-desc" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input id="tl-term-desc" bind:value={newTerminologyDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="tl-term-format" class="text-sm text-slate-600 dark:text-slate-300">Format</label>
				<select id="tl-term-format" bind:value={newTerminologyFormat} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each FORMATS as f (f)}<option value={f}>{f}</option>{/each}
				</select>
			</div>
			<div>
				<label for="tl-term-data" class="text-sm text-slate-600 dark:text-slate-300">Terminology data ({newTerminologyFormat})</label>
				<textarea id="tl-term-data" bind:value={newTerminologyData} rows="6" class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createTerminologyError}<p class="text-sm text-red-600 dark:text-red-400">{createTerminologyError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createTerminologyModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateTerminology} disabled={creatingTerminology} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{creatingTerminology ? 'Importing…' : 'Import'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createParallelModal} title="Create Parallel Data">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="tl-pd-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="tl-pd-name" bind:value={newParallelName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="tl-pd-desc" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input id="tl-pd-desc" bind:value={newParallelDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="tl-pd-s3" class="text-sm text-slate-600 dark:text-slate-300">S3 URI</label>
				<input id="tl-pd-s3" bind:value={newParallelS3Uri} placeholder="s3://my-bucket/parallel-data.tsv" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="tl-pd-format" class="text-sm text-slate-600 dark:text-slate-300">Format</label>
				<select id="tl-pd-format" bind:value={newParallelFormat} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each FORMATS as f (f)}<option value={f}>{f}</option>{/each}
				</select>
			</div>
			{#if createParallelError}<p class="text-sm text-red-600 dark:text-red-400">{createParallelError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createParallelModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateParallel} disabled={creatingParallel} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{creatingParallel ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editParallelModal} title="Update Parallel Data">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="tl-edit-pd-s3" class="text-sm text-slate-600 dark:text-slate-300">New S3 URI</label>
				<input id="tl-edit-pd-s3" bind:value={editParallelS3Uri} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="tl-edit-pd-format" class="text-sm text-slate-600 dark:text-slate-300">Format</label>
				<select id="tl-edit-pd-format" bind:value={editParallelFormat} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each FORMATS as f (f)}<option value={f}>{f}</option>{/each}
				</select>
			</div>
			{#if editParallelError}<p class="text-sm text-red-600 dark:text-red-400">{editParallelError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editParallelModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditParallel} disabled={editingParallel} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{editingParallel ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createJobModal} title="Start Translation Job">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="tl-job-name" class="text-sm text-slate-600 dark:text-slate-300">Job name (optional)</label>
				<input id="tl-job-name" bind:value={newJobName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="tl-job-role" class="text-sm text-slate-600 dark:text-slate-300">Data access role ARN</label>
				<input id="tl-job-role" bind:value={newJobRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="tl-job-input" class="text-sm text-slate-600 dark:text-slate-300">Input S3 URI</label>
					<input id="tl-job-input" bind:value={newJobInputS3Uri} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="tl-job-output" class="text-sm text-slate-600 dark:text-slate-300">Output S3 URI</label>
					<input id="tl-job-output" bind:value={newJobOutputS3Uri} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			<div>
				<label for="tl-job-content-type" class="text-sm text-slate-600 dark:text-slate-300">Input content type</label>
				<select id="tl-job-content-type" bind:value={newJobInputContentType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="text/plain">text/plain</option>
					<option value="text/html">text/html</option>
				</select>
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="tl-job-source" class="text-sm text-slate-600 dark:text-slate-300">Source language</label>
					<input id="tl-job-source" bind:value={newJobSourceLang} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="tl-job-targets" class="text-sm text-slate-600 dark:text-slate-300">Target languages (comma-separated)</label>
					<input id="tl-job-targets" bind:value={newJobTargetLangs} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			{#if createJobError}<p class="text-sm text-red-600 dark:text-red-400">{createJobError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createJobModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateJob} disabled={creatingJob} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{creatingJob ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<Modal
	bind:this={detailModal}
	title={detailKind === 'terminology' ? 'Terminology' : detailKind === 'paralleldata' ? 'Parallel Data' : 'Translation Job'}
>
	{#snippet children()}
		{#if detailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if detailError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>
		{:else if detailKind === 'terminology' && viewedTerminology?.TerminologyProperties}
			{@const t = viewedTerminology.TerminologyProperties}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{t.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{t.Arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Languages</dt><dd class="text-slate-900 dark:text-white">{t.SourceLanguageCode} → {(t.TargetLanguageCodes ?? []).join(', ')}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Term count</dt><dd class="text-slate-900 dark:text-white">{t.TermCount ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Size</dt><dd class="text-slate-900 dark:text-white">{formatBytes(t.SizeBytes)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Format</dt><dd class="text-slate-900 dark:text-white">{t.Format ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(t.CreatedAt)}</dd></div>
			</dl>
		{:else if detailKind === 'paralleldata' && viewedParallel?.ParallelDataProperties}
			{@const p = viewedParallel.ParallelDataProperties}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{p.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{p.Arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(p.Status)}">{p.Status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">S3 URI</dt><dd class="break-all text-slate-900 dark:text-white">{p.ParallelDataConfig?.S3Uri ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Format</dt><dd class="text-slate-900 dark:text-white">{p.ParallelDataConfig?.Format ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Imported records</dt><dd class="text-slate-900 dark:text-white">{p.ImportedRecordCount ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Latest update attempt</dt><dd class="text-slate-900 dark:text-white">{p.LatestUpdateAttemptStatus ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(p.CreatedAt)}</dd></div>
			</dl>
		{:else if detailKind === 'job' && viewedJob}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedJob.JobName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Job ID</dt><dd class="text-slate-900 dark:text-white">{viewedJob.JobId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(viewedJob.JobStatus)}">{viewedJob.JobStatus ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Languages</dt><dd class="text-slate-900 dark:text-white">{viewedJob.SourceLanguageCode} → {(viewedJob.TargetLanguageCodes ?? []).join(', ')}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Message</dt><dd class="text-slate-900 dark:text-white">{viewedJob.Message ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Submitted</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedJob.SubmittedTime)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Ended</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedJob.EndTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if detailKind === 'paralleldata' || detailKind === 'job'}
			<button type="button" onclick={refreshDetail} class="flex items-center gap-2 rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"><RefreshCw class="w-4 h-4" /> Refresh</button>
		{/if}
		{#if detailKind === 'paralleldata' && viewedParallel?.ParallelDataProperties}
			<button type="button" onclick={openEditParallel} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700">Edit</button>
		{/if}
	{/snippet}
</Modal>
