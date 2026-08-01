<script lang="ts">
	// Textract is largely stateless document analysis (AnalyzeDocument,
	// AnalyzeExpense, AnalyzeID, DetectDocumentText and their async
	// Start*/Get* counterparts) plus two genuinely listable/CRUD-capable
	// resource families: Adapters and AdapterVersions. Per the task brief,
	// analysis calls are actions, not resources -- they get no fabricated
	// CRUD, just an operation-shaped UI (mirroring how redshiftdata/sts/
	// rdsdata/bedrockruntime were reshaped). There is no ListDocumentAnalysis
	// -style API for async jobs, so "Async Jobs" tracks job IDs client-side
	// for this browser tab only, the same pattern sts/+page.svelte uses for
	// "credentials issued this session".
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getTextractClient } from '$lib/aws-client';
	import {
		ListAdaptersCommand,
		GetAdapterCommand,
		CreateAdapterCommand,
		UpdateAdapterCommand,
		DeleteAdapterCommand,
		ListAdapterVersionsCommand,
		GetAdapterVersionCommand,
		CreateAdapterVersionCommand,
		DeleteAdapterVersionCommand,
		DetectDocumentTextCommand,
		AnalyzeDocumentCommand,
		AnalyzeExpenseCommand,
		AnalyzeIDCommand,
		StartDocumentTextDetectionCommand,
		GetDocumentTextDetectionCommand,
		StartDocumentAnalysisCommand,
		GetDocumentAnalysisCommand,
		StartExpenseAnalysisCommand,
		GetExpenseAnalysisCommand,
		StartLendingAnalysisCommand,
		GetLendingAnalysisCommand,
		GetLendingAnalysisSummaryCommand,
		type AdapterOverview,
		type AdapterVersionOverview,
		type FeatureType,
		type Block,
		type ExpenseDocument,
		type IdentityDocument
	} from '@aws-sdk/client-textract';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { ScanLine, Plus, Trash2, Pencil, Eye, Layers, Upload, Play, RefreshCw } from 'lucide-svelte';

	const client = regionalClient(getTextractClient);

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

	type TabId = 'adapters' | 'adapterVersions' | 'analyze' | 'asyncJobs';

	const tabs: TabDef[] = [
		{ id: 'adapters', label: 'Adapters' },
		{ id: 'adapterVersions', label: 'Adapter Versions' },
		{ id: 'analyze', label: 'Analyze Document' },
		{ id: 'asyncJobs', label: 'Async Jobs' }
	];

	let activeTab = $state<TabId>('adapters');
	let searchQuery = $state('');

	// ==================== Adapters ====================

	let adapters = $state<AdapterOverview[]>([]);

	async function fetchAdapters(): Promise<void> {
		const resp = await client().send(new ListAdaptersCommand({}));
		adapters = resp.Adapters ?? [];
	}

	const filteredAdapters = $derived(
		adapters.filter((a) => (a.AdapterId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
			(a.AdapterName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ==================== Adapter Versions (scoped to selectedAdapterId) ====================

	let selectedAdapterId = $state<string | null>(null);
	let adapterVersions = $state<AdapterVersionOverview[]>([]);

	async function fetchAdapterVersions(): Promise<void> {
		// Read untracked -- selectAdapterForVersions() already writes
		// selectedAdapterId and forces a reload itself (same reasoning as
		// timestream's Tables tab / workmail's org-scoped tabs).
		const adapterId = untrack(() => selectedAdapterId);
		if (!adapterId) {
			adapterVersions = [];
			return;
		}
		const resp = await client().send(new ListAdapterVersionsCommand({ AdapterId: adapterId }));
		adapterVersions = resp.AdapterVersions ?? [];
	}

	function selectAdapterForVersions(adapterId: string): void {
		selectedAdapterId = adapterId;
		activeTab = 'adapterVersions';
		searchQuery = '';
		tabLoader.refresh('adapterVersions');
	}

	const filteredAdapterVersions = $derived(
		adapterVersions.filter((v) => (v.AdapterVersion ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ==================== Tab loader ====================

	const tabLoader = createTabLoader<TabId>({
		adapters: () => fetchAdapters().catch(rethrowDescribed),
		adapterVersions: () => fetchAdapterVersions().catch(rethrowDescribed),
		// Analyze / Async Jobs have nothing to bootstrap-fetch -- results only
		// appear once the operator submits a form from within the tab.
		analyze: () => Promise.resolve(),
		asyncJobs: () => Promise.resolve()
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
		selectedAdapterId = null;
		adapterVersions = [];
		tabLoader.refresh('adapters');
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// ==================== Adapter detail (Get) ====================

	let adapterDetailModal = $state<Modal | null>(null);
	let adapterDetail = $state<{
		AdapterId?: string;
		AdapterName?: string;
		Description?: string;
		FeatureTypes?: FeatureType[];
		AutoUpdate?: string;
		CreationTime?: Date;
	} | null>(null);
	let adapterDetailLoading = $state(false);

	async function openAdapterDetail(a: AdapterOverview): Promise<void> {
		if (!a.AdapterId) return;
		adapterDetail = null;
		adapterDetailLoading = true;
		adapterDetailModal?.open();
		try {
			const resp = await client().send(new GetAdapterCommand({ AdapterId: a.AdapterId }));
			adapterDetail = resp;
		} catch (e) {
			toast.error(describeError(e));
			adapterDetailModal?.close();
		} finally {
			adapterDetailLoading = false;
		}
	}

	// ==================== Create / Edit / Delete Adapter ====================

	let adapterCreateModal = $state<Modal | null>(null);
	let adapterCreating = $state(false);
	let adapterCreateError = $state<string | null>(null);
	let newAdapterName = $state('');
	let newAdapterDescription = $state('');
	let newAdapterAutoUpdate = $state<'ENABLED' | 'DISABLED'>('DISABLED');

	function openAdapterCreateModal(): void {
		adapterCreateError = null;
		newAdapterName = '';
		newAdapterDescription = '';
		newAdapterAutoUpdate = 'DISABLED';
		adapterCreateModal?.open();
	}

	async function submitCreateAdapter(): Promise<void> {
		if (!newAdapterName) {
			adapterCreateError = 'Adapter name is required.';
			return;
		}
		adapterCreating = true;
		adapterCreateError = null;
		try {
			// FeatureTypes is required on the wire; QUERIES is the only value
			// Textract currently supports for adapters (CreateAdapterRequest's
			// own doc comment).
			await client().send(
				new CreateAdapterCommand({
					AdapterName: newAdapterName,
					Description: newAdapterDescription || undefined,
					FeatureTypes: ['QUERIES'],
					AutoUpdate: newAdapterAutoUpdate
				})
			);
			toast.success(`Adapter "${newAdapterName}" created`);
			adapterCreateModal?.close();
			await tabLoader.refresh('adapters');
		} catch (e) {
			const msg = describeError(e);
			adapterCreateError = msg;
			toast.error(msg);
		} finally {
			adapterCreating = false;
		}
	}

	let adapterEditModal = $state<Modal | null>(null);
	let adapterEditing = $state(false);
	let adapterEditError = $state<string | null>(null);
	let adapterEditLoading = $state(false);
	let editAdapterId = $state('');
	let editAdapterDescription = $state('');
	let editAdapterAutoUpdate = $state<'ENABLED' | 'DISABLED'>('DISABLED');

	async function openAdapterEditModal(a: AdapterOverview): Promise<void> {
		if (!a.AdapterId) return;
		adapterEditError = null;
		editAdapterId = a.AdapterId;
		editAdapterDescription = '';
		editAdapterAutoUpdate = 'DISABLED';
		adapterEditLoading = true;
		adapterEditModal?.open();
		try {
			// UpdateAdapter always applies AutoUpdate (unlike Description, which
			// is a no-op when empty -- see services/textract/adapters.go's
			// UpdateAdapter), so the edit form must be seeded with the
			// adapter's CURRENT AutoUpdate value. Without this, saving after
			// only touching Description would silently reset AutoUpdate to
			// DISABLED for any adapter that had it ENABLED.
			const resp = await client().send(new GetAdapterCommand({ AdapterId: a.AdapterId }));
			editAdapterDescription = resp.Description ?? '';
			editAdapterAutoUpdate = resp.AutoUpdate === 'ENABLED' ? 'ENABLED' : 'DISABLED';
		} catch (e) {
			toast.error(describeError(e));
			adapterEditModal?.close();
		} finally {
			adapterEditLoading = false;
		}
	}

	async function submitEditAdapter(): Promise<void> {
		if (!editAdapterId) return;
		adapterEditing = true;
		adapterEditError = null;
		try {
			await client().send(
				new UpdateAdapterCommand({
					AdapterId: editAdapterId,
					Description: editAdapterDescription || undefined,
					AutoUpdate: editAdapterAutoUpdate
				})
			);
			toast.success('Adapter updated');
			adapterEditModal?.close();
			await tabLoader.refresh('adapters');
		} catch (e) {
			const msg = describeError(e);
			adapterEditError = msg;
			toast.error(msg);
		} finally {
			adapterEditing = false;
		}
	}

	async function deleteAdapter(a: AdapterOverview): Promise<void> {
		if (!a.AdapterId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete adapter',
			message: `Delete adapter "${a.AdapterName ?? a.AdapterId}"? All of its versions will be deleted too.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAdapterCommand({ AdapterId: a.AdapterId }));
			toast.success('Adapter deleted');
			if (selectedAdapterId === a.AdapterId) selectedAdapterId = null;
			await tabLoader.refresh('adapters');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Adapter Version detail (Get) ====================

	let versionDetailModal = $state<Modal | null>(null);
	let versionDetail = $state<{
		AdapterId?: string;
		AdapterVersion?: string;
		Status?: string;
		StatusMessage?: string;
		FeatureTypes?: FeatureType[];
		CreationTime?: Date;
		EvaluationMetrics?: { FeatureType?: string; Baseline?: { F1Score?: number }; AdapterVersion?: { F1Score?: number } }[];
	} | null>(null);
	let versionDetailLoading = $state(false);

	async function openVersionDetail(v: AdapterVersionOverview): Promise<void> {
		if (!v.AdapterId || !v.AdapterVersion) return;
		versionDetail = null;
		versionDetailLoading = true;
		versionDetailModal?.open();
		try {
			const resp = await client().send(
				new GetAdapterVersionCommand({ AdapterId: v.AdapterId, AdapterVersion: v.AdapterVersion })
			);
			versionDetail = resp;
		} catch (e) {
			toast.error(describeError(e));
			versionDetailModal?.close();
		} finally {
			versionDetailLoading = false;
		}
	}

	// ==================== Create / Delete Adapter Version ====================
	// (no UpdateAdapterVersion operation exists on the real API)

	let versionCreateModal = $state<Modal | null>(null);
	let versionCreating = $state(false);
	let versionCreateError = $state<string | null>(null);
	let newVersionManifestBucket = $state('');
	let newVersionManifestKey = $state('');
	let newVersionOutputBucket = $state('');
	let newVersionOutputPrefix = $state('');

	function openVersionCreateModal(): void {
		if (!selectedAdapterId) {
			toast.error('Select an adapter first');
			return;
		}
		versionCreateError = null;
		newVersionManifestBucket = '';
		newVersionManifestKey = '';
		newVersionOutputBucket = '';
		newVersionOutputPrefix = '';
		versionCreateModal?.open();
	}

	async function submitCreateVersion(): Promise<void> {
		if (!selectedAdapterId || !newVersionManifestBucket || !newVersionManifestKey || !newVersionOutputBucket) {
			versionCreateError = 'Manifest S3 location and output bucket are required.';
			return;
		}
		versionCreating = true;
		versionCreateError = null;
		try {
			await client().send(
				new CreateAdapterVersionCommand({
					AdapterId: selectedAdapterId,
					DatasetConfig: {
						ManifestS3Object: { Bucket: newVersionManifestBucket, Name: newVersionManifestKey }
					},
					OutputConfig: { S3Bucket: newVersionOutputBucket, S3Prefix: newVersionOutputPrefix || undefined }
				})
			);
			toast.success('Adapter version created');
			versionCreateModal?.close();
			await tabLoader.refresh('adapterVersions');
		} catch (e) {
			const msg = describeError(e);
			versionCreateError = msg;
			toast.error(msg);
		} finally {
			versionCreating = false;
		}
	}

	async function deleteVersion(v: AdapterVersionOverview): Promise<void> {
		if (!v.AdapterId || !v.AdapterVersion) return;
		const confirmed = await confirmDestructive({
			title: 'Delete adapter version',
			message: `Delete adapter version "${v.AdapterVersion}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAdapterVersionCommand({ AdapterId: v.AdapterId, AdapterVersion: v.AdapterVersion }));
			toast.success('Adapter version deleted');
			await tabLoader.refresh('adapterVersions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Synchronous analysis ====================

	type SyncOp = 'DETECT_TEXT' | 'ANALYZE_DOCUMENT' | 'ANALYZE_EXPENSE' | 'ANALYZE_ID';

	let syncOp = $state<SyncOp>('DETECT_TEXT');
	let syncSource = $state<'s3' | 'upload'>('upload');
	let syncBucket = $state('');
	let syncKey = $state('');
	let syncUploadName = $state('');
	let syncUploadBytes = $state<Uint8Array | null>(null);
	let syncFeatTables = $state(true);
	let syncFeatForms = $state(true);
	let syncFeatSignatures = $state(false);
	let syncFeatLayout = $state(false);
	let syncRunning = $state(false);
	let syncError = $state<string | null>(null);

	let syncBlocks = $state<Block[]>([]);
	let syncExpenseDocs = $state<ExpenseDocument[]>([]);
	let syncIdentityDocs = $state<IdentityDocument[]>([]);

	const syncFeatureTypes = $derived(
		[
			syncFeatTables ? 'TABLES' : null,
			syncFeatForms ? 'FORMS' : null,
			syncFeatSignatures ? 'SIGNATURES' : null,
			syncFeatLayout ? 'LAYOUT' : null
		].filter(Boolean) as FeatureType[]
	);

	async function onSyncFileSelected(e: Event): Promise<void> {
		const input = e.target as HTMLInputElement;
		const file = input.files?.[0];
		if (!file) return;
		syncUploadName = file.name;
		syncUploadBytes = new Uint8Array(await file.arrayBuffer());
	}

	function buildDocument(): { Bytes?: Uint8Array; S3Object?: { Bucket: string; Name: string } } | null {
		if (syncSource === 'upload') {
			if (!syncUploadBytes) return null;
			return { Bytes: syncUploadBytes };
		}
		if (!syncBucket || !syncKey) return null;
		return { S3Object: { Bucket: syncBucket, Name: syncKey } };
	}

	async function runSyncAnalysis(): Promise<void> {
		const doc = buildDocument();
		if (!doc) {
			syncError = 'Provide a document (upload a file or an S3 bucket/key).';
			return;
		}
		syncRunning = true;
		syncError = null;
		syncBlocks = [];
		syncExpenseDocs = [];
		syncIdentityDocs = [];
		try {
			if (syncOp === 'DETECT_TEXT') {
				const resp = await client().send(new DetectDocumentTextCommand({ Document: doc }));
				syncBlocks = resp.Blocks ?? [];
				toast.success(`Detected ${syncBlocks.length} block(s)`);
			} else if (syncOp === 'ANALYZE_DOCUMENT') {
				if (syncFeatureTypes.length === 0) {
					syncError = 'Select at least one feature type';
					return;
				}
				const resp = await client().send(new AnalyzeDocumentCommand({ Document: doc, FeatureTypes: syncFeatureTypes }));
				syncBlocks = resp.Blocks ?? [];
				toast.success(`Analyzed document (${syncBlocks.length} block(s))`);
			} else if (syncOp === 'ANALYZE_EXPENSE') {
				const resp = await client().send(new AnalyzeExpenseCommand({ Document: doc }));
				syncExpenseDocs = resp.ExpenseDocuments ?? [];
				toast.success(`Analyzed ${syncExpenseDocs.length} expense document(s)`);
			} else if (syncOp === 'ANALYZE_ID') {
				const resp = await client().send(new AnalyzeIDCommand({ DocumentPages: [doc] }));
				syncIdentityDocs = resp.IdentityDocuments ?? [];
				toast.success(`Analyzed ${syncIdentityDocs.length} identity document(s)`);
			}
		} catch (e) {
			syncError = describeError(e);
			toast.error(syncError);
		} finally {
			syncRunning = false;
		}
	}

	// ==================== Async jobs (client-side session tracking) ====================
	// Textract has no ListDocumentAnalysis-style API for async jobs -- this
	// list is this browser tab's own record of jobs it started, the same
	// pattern sts/+page.svelte uses for "credentials issued this session".

	type AsyncOp = 'TEXT_DETECTION' | 'DOCUMENT_ANALYSIS' | 'EXPENSE_ANALYSIS' | 'LENDING_ANALYSIS';

	type AsyncJob = {
		id: number;
		op: AsyncOp;
		jobId: string;
		status: string;
		blockCount?: number;
		summary?: string;
	};

	let asyncOp = $state<AsyncOp>('TEXT_DETECTION');
	let asyncBucket = $state('');
	let asyncKey = $state('');
	let asyncFeatTables = $state(true);
	let asyncFeatForms = $state(true);
	let asyncStarting = $state(false);
	let asyncError = $state<string | null>(null);
	let jobSeq = 0;
	let jobs = $state<AsyncJob[]>([]);

	async function startAsyncJob(): Promise<void> {
		if (!asyncBucket || !asyncKey) {
			asyncError = 'S3 bucket and key are required for async jobs.';
			return;
		}
		asyncStarting = true;
		asyncError = null;
		try {
			const DocumentLocation = { S3Object: { Bucket: asyncBucket, Name: asyncKey } };
			let jobId = '';
			if (asyncOp === 'TEXT_DETECTION') {
				const resp = await client().send(new StartDocumentTextDetectionCommand({ DocumentLocation }));
				jobId = resp.JobId ?? '';
			} else if (asyncOp === 'DOCUMENT_ANALYSIS') {
				const featureTypes = [asyncFeatTables ? 'TABLES' : null, asyncFeatForms ? 'FORMS' : null].filter(
					Boolean
				) as FeatureType[];
				if (featureTypes.length === 0) {
					asyncError = 'Select at least one feature type';
					return;
				}
				const resp = await client().send(new StartDocumentAnalysisCommand({ DocumentLocation, FeatureTypes: featureTypes }));
				jobId = resp.JobId ?? '';
			} else if (asyncOp === 'EXPENSE_ANALYSIS') {
				const resp = await client().send(new StartExpenseAnalysisCommand({ DocumentLocation }));
				jobId = resp.JobId ?? '';
			} else if (asyncOp === 'LENDING_ANALYSIS') {
				const resp = await client().send(new StartLendingAnalysisCommand({ DocumentLocation }));
				jobId = resp.JobId ?? '';
			}
			jobSeq += 1;
			jobs = [{ id: jobSeq, op: asyncOp, jobId, status: 'IN_PROGRESS' }, ...jobs];
			toast.success(`Job started: ${jobId}`);
		} catch (e) {
			asyncError = describeError(e);
			toast.error(asyncError);
		} finally {
			asyncStarting = false;
		}
	}

	async function refreshJob(job: AsyncJob): Promise<void> {
		try {
			if (job.op === 'TEXT_DETECTION') {
				const resp = await client().send(new GetDocumentTextDetectionCommand({ JobId: job.jobId }));
				job.status = resp.JobStatus ?? job.status;
				job.blockCount = resp.Blocks?.length ?? 0;
			} else if (job.op === 'DOCUMENT_ANALYSIS') {
				const resp = await client().send(new GetDocumentAnalysisCommand({ JobId: job.jobId }));
				job.status = resp.JobStatus ?? job.status;
				job.blockCount = resp.Blocks?.length ?? 0;
			} else if (job.op === 'EXPENSE_ANALYSIS') {
				const resp = await client().send(new GetExpenseAnalysisCommand({ JobId: job.jobId }));
				job.status = resp.JobStatus ?? job.status;
				job.summary = `${resp.ExpenseDocuments?.length ?? 0} expense document(s)`;
			} else if (job.op === 'LENDING_ANALYSIS') {
				const resp = await client().send(new GetLendingAnalysisCommand({ JobId: job.jobId }));
				job.status = resp.JobStatus ?? job.status;
				job.summary = `${resp.Results?.length ?? 0} page result(s)`;
			}
			jobs = jobs.map((j) => (j.id === job.id ? job : j));
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function getLendingSummary(job: AsyncJob): Promise<void> {
		try {
			const resp = await client().send(new GetLendingAnalysisSummaryCommand({ JobId: job.jobId }));
			job.status = resp.JobStatus ?? job.status;
			job.summary = `Summary: ${resp.Summary?.DocumentGroups?.length ?? 0} document group(s)`;
			jobs = jobs.map((j) => (j.id === job.id ? job : j));
			toast.success('Lending analysis summary refreshed');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	function asyncOpLabel(op: AsyncOp): string {
		if (op === 'TEXT_DETECTION') return 'Text Detection';
		if (op === 'DOCUMENT_ANALYSIS') return 'Document Analysis';
		if (op === 'EXPENSE_ANALYSIS') return 'Expense Analysis';
		return 'Lending Analysis';
	}

	function jobStatusClass(status: string): string {
		if (status === 'SUCCEEDED') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (status === 'FAILED' || status === 'PARTIAL_SUCCESS') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400';
	}
</script>

{#snippet adapterActionsCell(a: AdapterOverview)}
	<div class="flex items-center gap-2 justify-end">
		<button onclick={() => selectAdapterForVersions(a.AdapterId ?? '')} title="View versions" aria-label="View versions of {a.AdapterId}" class="text-gray-400 hover:text-blue-500"><Layers class="w-4 h-4" /></button>
		<button onclick={() => openAdapterDetail(a)} title="View" aria-label="View adapter {a.AdapterId}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
		<button onclick={() => openAdapterEditModal(a)} title="Edit" aria-label="Edit adapter {a.AdapterId}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
		<button onclick={() => deleteAdapter(a)} title="Delete" aria-label="Delete adapter {a.AdapterId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
	</div>
{/snippet}
{#snippet adapterCreatedCell(a: AdapterOverview)}
	<span class="text-xs text-gray-500 dark:text-gray-400">{formatDate(a.CreationTime)}</span>
{/snippet}

{#snippet versionStatusCell(v: AdapterVersionOverview)}
	<span class="text-xs px-2 py-1 rounded-full {v.Status === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{v.Status}</span>
{/snippet}
{#snippet versionCreatedCell(v: AdapterVersionOverview)}
	<span class="text-xs text-gray-500 dark:text-gray-400">{formatDate(v.CreationTime)}</span>
{/snippet}
{#snippet versionActionsCell(v: AdapterVersionOverview)}
	<div class="flex items-center gap-2 justify-end">
		<button onclick={() => openVersionDetail(v)} title="View" aria-label="View version {v.AdapterVersion}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
		<button onclick={() => deleteVersion(v)} title="Delete" aria-label="Delete version {v.AdapterVersion}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
	</div>
{/snippet}

<div class="p-6 space-y-6">
	<PageHeader
		icon={ScanLine}
		title="Amazon Textract"
		description="Extract text and data from documents; manage custom analysis adapters"
		onRefresh={handleRefresh}
		color="blue"
	>
		{#snippet actions()}
			{#if activeTab === 'adapters'}
				<button onclick={openAdapterCreateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Create adapter
				</button>
			{:else if activeTab === 'adapterVersions'}
				<button onclick={openVersionCreateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Create version
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
			{#if activeTab === 'adapters' || activeTab === 'adapterVersions'}
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

			{#if activeTab === 'adapters'}
				{@const adapterColumns = defineColumns<AdapterOverview>([
					{ key: 'AdapterId', label: 'Adapter ID' },
					{ key: 'AdapterName', label: 'Name' },
					{ key: 'CreationTime', label: 'Created', render: adapterCreatedCell },
					{ key: 'actions', label: '', render: adapterActionsCell }
				])}
				<DataTable
					rows={filteredAdapters}
					rowKey={(a) => a.AdapterId ?? ''}
					columns={adapterColumns}
					loading={tabLoader.isLoading('adapters')}
					emptyMessage="No adapters found"
				/>
			{:else if activeTab === 'adapterVersions'}
				<div class="flex items-center gap-2 text-sm">
					<Layers class="w-4 h-4 text-gray-400" />
					<label for="tx-adapter-select" class="text-gray-500 dark:text-gray-400">Adapter</label>
					<select
						id="tx-adapter-select"
						value={selectedAdapterId ?? ''}
						onchange={(e) => selectAdapterForVersions((e.target as HTMLSelectElement).value)}
						class="px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"
					>
						<option value="" disabled>Select an adapter…</option>
						{#each adapters as a (a.AdapterId)}
							<option value={a.AdapterId}>{a.AdapterName ?? a.AdapterId}</option>
						{/each}
					</select>
				</div>
				{#if !selectedAdapterId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select an adapter to view its versions</div>
				{:else}
					{@const versionColumns = defineColumns<AdapterVersionOverview>([
						{ key: 'AdapterVersion', label: 'Version' },
						{ key: 'Status', label: 'Status', render: versionStatusCell },
						{ key: 'CreationTime', label: 'Created', render: versionCreatedCell },
						{ key: 'actions', label: '', render: versionActionsCell }
					])}
					<DataTable
						rows={filteredAdapterVersions}
						rowKey={(v) => v.AdapterVersion ?? ''}
						columns={versionColumns}
						loading={tabLoader.isLoading('adapterVersions')}
						emptyMessage="No versions found for this adapter"
					/>
				{/if}
			{:else if activeTab === 'analyze'}
				<div class="space-y-4">
					<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
						<div>
							<label for="sync-op" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Operation</label>
							<select id="sync-op" bind:value={syncOp} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
								<option value="DETECT_TEXT">Detect Document Text</option>
								<option value="ANALYZE_DOCUMENT">Analyze Document</option>
								<option value="ANALYZE_EXPENSE">Analyze Expense</option>
								<option value="ANALYZE_ID">Analyze ID</option>
							</select>
						</div>
						<div>
							<p class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Source</p>
							<div class="flex gap-1">
								{#each [['upload', 'Local Upload'], ['s3', 'S3 Object']] as [m, lbl]}
									<button onclick={() => (syncSource = m as typeof syncSource)} class="px-3 py-1.5 text-xs rounded-lg font-medium {syncSource === m ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300'}">{lbl}</button>
								{/each}
							</div>
						</div>
					</div>

					{#if syncSource === 's3'}
						<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
							<div>
								<label for="sync-bucket" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">S3 Bucket</label>
								<input id="sync-bucket" bind:value={syncBucket} placeholder="my-bucket" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							</div>
							<div>
								<label for="sync-key" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">S3 Key</label>
								<input id="sync-key" bind:value={syncKey} placeholder="documents/file.pdf" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							</div>
						</div>
					{:else}
						<div>
							<label for="sync-file" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Document file (PNG / JPEG / PDF)</label>
							<input id="sync-file" type="file" accept="image/png,image/jpeg,application/pdf" onchange={onSyncFileSelected} class="block w-full text-sm text-gray-700 dark:text-gray-300 file:mr-3 file:rounded-lg file:border-0 file:bg-blue-600 file:px-3 file:py-2 file:text-white" />
							{#if syncUploadName}
								<p class="mt-1 text-xs text-gray-500 dark:text-gray-400 flex items-center gap-1"><Upload class="w-3.5 h-3.5" /> {syncUploadName} ({syncUploadBytes?.length ?? 0} bytes)</p>
							{/if}
						</div>
					{/if}

					{#if syncOp === 'ANALYZE_DOCUMENT'}
						<div>
							<p class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Feature types</p>
							<div class="flex flex-wrap gap-4">
								<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={syncFeatTables} class="rounded" /> Tables</label>
								<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={syncFeatForms} class="rounded" /> Forms</label>
								<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={syncFeatSignatures} class="rounded" /> Signatures</label>
								<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={syncFeatLayout} class="rounded" /> Layout</label>
							</div>
						</div>
					{/if}

					{#if syncError}
						<div class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">{syncError}</div>
					{/if}

					<button onclick={runSyncAnalysis} disabled={syncRunning} class="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
						<Play class="w-4 h-4" /> {syncRunning ? 'Running…' : 'Run'}
					</button>

					{#if syncBlocks.length > 0}
						<div>
							<h3 class="text-sm font-semibold text-gray-900 dark:text-white mb-2">Result Blocks ({syncBlocks.length})</h3>
							<div class="space-y-1 max-h-96 overflow-y-auto">
								{#each syncBlocks as block, i (i)}
									<div class="flex items-start gap-3 p-2 rounded bg-gray-50 dark:bg-slate-700/50 text-sm">
										<span class="text-xs font-mono px-1.5 py-0.5 rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 shrink-0">{block.BlockType}</span>
										{#if block.Text}<span class="text-gray-900 dark:text-white flex-1 break-all">{block.Text}</span>{/if}
										{#if block.Confidence != null}<span class="text-xs text-gray-400 shrink-0">{block.Confidence?.toFixed(1)}%</span>{/if}
									</div>
								{/each}
							</div>
						</div>
					{/if}
					{#if syncExpenseDocs.length > 0}
						<div class="space-y-2">
							<h3 class="text-sm font-semibold text-gray-900 dark:text-white">Expense Documents ({syncExpenseDocs.length})</h3>
							{#each syncExpenseDocs as doc, i (i)}
								<div class="p-3 rounded bg-gray-50 dark:bg-slate-700/50 text-sm">
									<p class="font-medium">Expense #{doc.ExpenseIndex}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{doc.SummaryFields?.length ?? 0} summary field(s), {doc.LineItemGroups?.length ?? 0} line item group(s)</p>
								</div>
							{/each}
						</div>
					{/if}
					{#if syncIdentityDocs.length > 0}
						<div class="space-y-2">
							<h3 class="text-sm font-semibold text-gray-900 dark:text-white">Identity Documents ({syncIdentityDocs.length})</h3>
							{#each syncIdentityDocs as doc, i (i)}
								<div class="p-3 rounded bg-gray-50 dark:bg-slate-700/50 text-sm">
									<p class="font-medium">Document #{doc.DocumentIndex}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{doc.IdentityDocumentFields?.length ?? 0} field(s) extracted</p>
								</div>
							{/each}
						</div>
					{/if}
				</div>
			{:else if activeTab === 'asyncJobs'}
				<div class="space-y-4">
					<p class="text-xs text-gray-500 dark:text-gray-400">
						Textract has no list API for async jobs -- this table tracks job IDs started from this browser tab only.
					</p>
					<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
						<div>
							<label for="async-op" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Operation</label>
							<select id="async-op" bind:value={asyncOp} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
								<option value="TEXT_DETECTION">Text Detection</option>
								<option value="DOCUMENT_ANALYSIS">Document Analysis</option>
								<option value="EXPENSE_ANALYSIS">Expense Analysis</option>
								<option value="LENDING_ANALYSIS">Lending Analysis</option>
							</select>
						</div>
						<div class="grid grid-cols-2 gap-3">
							<div>
								<label for="async-bucket" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">S3 Bucket</label>
								<input id="async-bucket" bind:value={asyncBucket} placeholder="my-bucket" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							</div>
							<div>
								<label for="async-key" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">S3 Key</label>
								<input id="async-key" bind:value={asyncKey} placeholder="documents/file.pdf" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							</div>
						</div>
					</div>

					{#if asyncOp === 'DOCUMENT_ANALYSIS'}
						<div class="flex flex-wrap gap-4">
							<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={asyncFeatTables} class="rounded" /> Tables</label>
							<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={asyncFeatForms} class="rounded" /> Forms</label>
						</div>
					{/if}

					{#if asyncError}
						<div class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">{asyncError}</div>
					{/if}

					<button onclick={startAsyncJob} disabled={asyncStarting} class="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
						<Play class="w-4 h-4" /> {asyncStarting ? 'Starting…' : 'Start Job'}
					</button>

					{#if jobs.length === 0}
						<div class="text-center py-8 text-gray-500 dark:text-gray-400">No jobs started this session</div>
					{:else}
						<div class="space-y-2">
							{#each jobs as job (job.id)}
								<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
									<div class="min-w-0">
										<div class="flex items-center gap-2">
											<span class="text-xs font-mono px-1.5 py-0.5 rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400">{asyncOpLabel(job.op)}</span>
											<span class="text-xs px-2 py-0.5 rounded-full {jobStatusClass(job.status)}">{job.status}</span>
										</div>
										<p class="text-xs font-mono text-gray-500 dark:text-gray-400 truncate mt-1">{job.jobId}</p>
										{#if job.blockCount != null}<p class="text-xs text-gray-500 dark:text-gray-400">{job.blockCount} block(s)</p>{/if}
										{#if job.summary}<p class="text-xs text-gray-500 dark:text-gray-400">{job.summary}</p>{/if}
									</div>
									<div class="flex items-center gap-2 shrink-0">
										<button onclick={() => refreshJob(job)} title="Refresh status" aria-label="Refresh status for {job.jobId}" class="text-gray-400 hover:text-blue-500"><RefreshCw class="w-4 h-4" /></button>
										{#if job.op === 'LENDING_ANALYSIS'}
											<button onclick={() => getLendingSummary(job)} class="text-xs text-blue-600 dark:text-blue-400 hover:underline">Get summary</button>
										{/if}
									</div>
								</div>
							{/each}
						</div>
					{/if}
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Adapter -->
<Modal bind:this={adapterCreateModal} title="Create Adapter">
	{#snippet children()}
		<div class="space-y-3">
			{#if adapterCreateError}<p class="text-sm text-red-600 dark:text-red-400">{adapterCreateError}</p>{/if}
			<div>
				<label for="new-adapter-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Adapter Name</label>
				<input id="new-adapter-name" bind:value={newAdapterName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="new-adapter-desc" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Description <span class="text-gray-400">(optional)</span></label>
				<input id="new-adapter-desc" bind:value={newAdapterDescription} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="new-adapter-autoupdate" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Auto Update</label>
				<select id="new-adapter-autoupdate" bind:value={newAdapterAutoUpdate} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm">
					<option value="DISABLED">DISABLED</option>
					<option value="ENABLED">ENABLED</option>
				</select>
			</div>
			<p class="text-xs text-gray-400">Feature type is fixed to QUERIES -- the only type Textract currently supports for adapters.</p>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => adapterCreateModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateAdapter} disabled={adapterCreating} class="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
			{adapterCreating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<!-- Edit Adapter -->
<Modal bind:this={adapterEditModal} title="Edit Adapter">
	{#snippet children()}
		<div class="space-y-3">
			{#if adapterEditError}<p class="text-sm text-red-600 dark:text-red-400">{adapterEditError}</p>{/if}
			<p class="text-sm text-gray-500 dark:text-gray-400 font-mono">{editAdapterId}</p>
			<div>
				<label for="edit-adapter-desc" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Description</label>
				<input id="edit-adapter-desc" bind:value={editAdapterDescription} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="edit-adapter-autoupdate" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Auto Update</label>
				<select id="edit-adapter-autoupdate" bind:value={editAdapterAutoUpdate} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm">
					<option value="DISABLED">DISABLED</option>
					<option value="ENABLED">ENABLED</option>
				</select>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => adapterEditModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitEditAdapter} disabled={adapterEditing || adapterEditLoading} class="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
			{adapterEditing ? 'Saving…' : 'Save'}
		</button>
	{/snippet}
</Modal>

<!-- Adapter Detail -->
<Modal bind:this={adapterDetailModal} title="Adapter Detail">
	{#snippet children()}
		{#if adapterDetailLoading}
			<p class="text-sm text-gray-500 dark:text-gray-400">Loading…</p>
		{:else if adapterDetail}
			<div class="space-y-2 text-sm">
				<div class="flex justify-between gap-2"><span class="text-gray-500">Adapter ID</span><span class="font-mono text-xs">{adapterDetail.AdapterId}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Name</span><span>{adapterDetail.AdapterName}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Description</span><span>{adapterDetail.Description ?? '—'}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Feature Types</span><span>{adapterDetail.FeatureTypes?.join(', ') ?? '—'}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Auto Update</span><span>{adapterDetail.AutoUpdate}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Created</span><span>{formatDate(adapterDetail.CreationTime)}</span></div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => adapterDetailModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Create Adapter Version -->
<Modal bind:this={versionCreateModal} title="Create Adapter Version">
	{#snippet children()}
		<div class="space-y-3">
			{#if versionCreateError}<p class="text-sm text-red-600 dark:text-red-400">{versionCreateError}</p>{/if}
			<p class="text-xs text-gray-500 dark:text-gray-400">Adapter: {selectedAdapterId}</p>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-version-manifest-bucket" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Manifest S3 Bucket</label>
					<input id="new-version-manifest-bucket" bind:value={newVersionManifestBucket} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
				<div>
					<label for="new-version-manifest-key" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Manifest Key</label>
					<input id="new-version-manifest-key" bind:value={newVersionManifestKey} placeholder="manifests/dataset.jsonl" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-version-output-bucket" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Output S3 Bucket</label>
					<input id="new-version-output-bucket" bind:value={newVersionOutputBucket} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
				<div>
					<label for="new-version-output-prefix" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Output Prefix <span class="text-gray-400">(optional)</span></label>
					<input id="new-version-output-prefix" bind:value={newVersionOutputPrefix} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => versionCreateModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateVersion} disabled={versionCreating} class="px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
			{versionCreating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<!-- Adapter Version Detail -->
<Modal bind:this={versionDetailModal} title="Adapter Version Detail">
	{#snippet children()}
		{#if versionDetailLoading}
			<p class="text-sm text-gray-500 dark:text-gray-400">Loading…</p>
		{:else if versionDetail}
			<div class="space-y-2 text-sm">
				<div class="flex justify-between gap-2"><span class="text-gray-500">Version</span><span class="font-mono text-xs">{versionDetail.AdapterVersion}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Status</span><span>{versionDetail.Status}</span></div>
				{#if versionDetail.StatusMessage}
					<div class="flex justify-between gap-2"><span class="text-gray-500">Status Message</span><span class="text-xs break-all">{versionDetail.StatusMessage}</span></div>
				{/if}
				<div class="flex justify-between gap-2"><span class="text-gray-500">Feature Types</span><span>{versionDetail.FeatureTypes?.join(', ') ?? '—'}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Created</span><span>{formatDate(versionDetail.CreationTime)}</span></div>
				{#if versionDetail.EvaluationMetrics && versionDetail.EvaluationMetrics.length > 0}
					<div class="pt-2 border-t border-gray-100 dark:border-gray-800">
						<p class="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-1">Evaluation Metrics</p>
						{#each versionDetail.EvaluationMetrics as m, i (i)}
							<div class="flex justify-between gap-2"><span class="text-gray-500">{m.FeatureType}</span><span>F1 {m.AdapterVersion?.F1Score?.toFixed(3) ?? '—'} (baseline {m.Baseline?.F1Score?.toFixed(3) ?? '—'})</span></div>
						{/each}
					</div>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => versionDetailModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>
