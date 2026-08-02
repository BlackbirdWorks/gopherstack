<script lang="ts">
	// Macie2 is discovery output more than user-created resources: findings
	// are produced by classification jobs and can only be seeded via
	// CreateSampleFindings, not created/updated/deleted directly (no such
	// ops exist in the real API), so the Findings tab is read-only plus a
	// "Create sample findings" action. Classification jobs have no Delete
	// op either -- only a status lifecycle (RUNNING/USER_PAUSED/CANCELLED)
	// via UpdateClassificationJob -- so its "delete" affordance is Cancel.
	// Custom data identifiers have Create/Get/Delete but no Update (no such
	// op exists; AWS custom data identifiers are immutable once created).
	// Allow lists and findings filters get full CRUD.
	//
	// Other listable families in the real API (members, invitations,
	// organization admin accounts, automated discovery accounts, resource
	// profiles, classification/sensitivity-inspection scopes) are
	// multi-account-administration or singleton-per-account configuration
	// surfaces, not independent user-managed resource collections -- left
	// out of this floor, same as this dashboard's treatment of comparable
	// admin-only surfaces elsewhere.
	import { untrack } from 'svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getMacie2Client } from '$lib/aws-client';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import {
		ListAllowListsCommand,
		GetAllowListCommand,
		CreateAllowListCommand,
		UpdateAllowListCommand,
		DeleteAllowListCommand,
		ListClassificationJobsCommand,
		DescribeClassificationJobCommand,
		CreateClassificationJobCommand,
		UpdateClassificationJobCommand,
		ListCustomDataIdentifiersCommand,
		GetCustomDataIdentifierCommand,
		CreateCustomDataIdentifierCommand,
		DeleteCustomDataIdentifierCommand,
		ListFindingsFiltersCommand,
		GetFindingsFilterCommand,
		CreateFindingsFilterCommand,
		UpdateFindingsFilterCommand,
		DeleteFindingsFilterCommand,
		ListFindingsCommand,
		GetFindingsCommand,
		CreateSampleFindingsCommand,
		JobStatus,
		JobType,
		FindingsFilterAction,
		type AllowListSummary,
		type AllowListCriteria,
		type GetAllowListResponse,
		type JobSummary,
		type DescribeClassificationJobResponse,
		type CustomDataIdentifierSummary,
		type GetCustomDataIdentifierResponse,
		type FindingsFilterListItem,
		type GetFindingsFilterResponse,
		type Finding
	} from '@aws-sdk/client-macie2';
	import { toast } from 'svelte-sonner';
	import { ShieldAlert, Plus, Trash2, Eye, Pencil, Ban, Play, Pause, Sparkles } from 'lucide-svelte';

	const client = regionalClient(getMacie2Client);

	type TabId = 'jobs' | 'identifiers' | 'allowlists' | 'filters' | 'findings';

	const tabs: TabDef[] = [
		{ id: 'jobs', label: 'Classification Jobs' },
		{ id: 'identifiers', label: 'Custom Data Identifiers' },
		{ id: 'allowlists', label: 'Allow Lists' },
		{ id: 'filters', label: 'Findings Filters' },
		{ id: 'findings', label: 'Findings' }
	];

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

	const activeStatuses = new Set(['RUNNING', 'IDLE', 'COMPLETE', 'ENABLED']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s))
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('jobs');
	let searchQuery = $state('');

	// --- Jobs ---
	let jobs = $state<JobSummary[]>([]);
	async function fetchJobs(): Promise<void> {
		const resp = await client().send(new ListClassificationJobsCommand({}));
		jobs = resp.items ?? [];
	}

	// --- Custom Data Identifiers ---
	let identifiers = $state<CustomDataIdentifierSummary[]>([]);
	async function fetchIdentifiers(): Promise<void> {
		const resp = await client().send(new ListCustomDataIdentifiersCommand({}));
		identifiers = resp.items ?? [];
	}

	// --- Allow Lists ---
	let allowLists = $state<AllowListSummary[]>([]);
	async function fetchAllowLists(): Promise<void> {
		const resp = await client().send(new ListAllowListsCommand({}));
		allowLists = resp.allowLists ?? [];
	}

	// --- Findings Filters ---
	let filters = $state<FindingsFilterListItem[]>([]);
	async function fetchFilters(): Promise<void> {
		const resp = await client().send(new ListFindingsFiltersCommand({}));
		filters = resp.findingsFilterListItems ?? [];
	}

	// --- Findings ---
	let findings = $state<Finding[]>([]);
	async function fetchFindings(): Promise<void> {
		const listResp = await client().send(new ListFindingsCommand({}));
		const ids = listResp.findingIds ?? [];
		if (ids.length === 0) {
			findings = [];
			return;
		}
		const getResp = await client().send(new GetFindingsCommand({ findingIds: ids }));
		findings = getResp.findings ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		jobs: () => fetchJobs().catch(rethrowDescribed),
		identifiers: () => fetchIdentifiers().catch(rethrowDescribed),
		allowlists: () => fetchAllowLists().catch(rethrowDescribed),
		filters: () => fetchFilters().catch(rethrowDescribed),
		findings: () => fetchFindings().catch(rethrowDescribed)
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
		// Clear every detail/selection modal's state on region change --
		// none of these IDs are unique across regions.
		viewedJob = null;
		viewedIdentifier = null;
		viewedAllowList = null;
		viewedFilter = null;
		const tab = untrack(() => activeTab);
		tabLoader.refresh(tab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredJobs = $derived(
		jobs.filter((j) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(j.name ?? '').toLowerCase().includes(q) ||
				(j.jobId ?? '').toLowerCase().includes(q) ||
				(j.jobStatus ?? '').toLowerCase().includes(q) ||
				(j.jobType ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredIdentifiers = $derived(
		identifiers.filter((i) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(i.name ?? '').toLowerCase().includes(q) ||
				(i.id ?? '').toLowerCase().includes(q) ||
				(i.description ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredAllowLists = $derived(
		allowLists.filter((a) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(a.name ?? '').toLowerCase().includes(q) ||
				(a.id ?? '').toLowerCase().includes(q) ||
				(a.description ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredFilters = $derived(
		filters.filter((f) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(f.name ?? '').toLowerCase().includes(q) ||
				(f.id ?? '').toLowerCase().includes(q) ||
				(f.action ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredFindings = $derived(
		findings.filter((f) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(f.title ?? '').toLowerCase().includes(q) ||
				(f.id ?? '').toLowerCase().includes(q) ||
				(f.type ?? '').toLowerCase().includes(q) ||
				(f.category ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ── Create Classification Job ──────────────────────────────────────────
	let createJobModal = $state<Modal | null>(null);
	let creatingJob = $state(false);
	let createJobError = $state<string | null>(null);
	let newJobName = $state('');
	let newJobDescription = $state('');
	let newJobType = $state<'ONE_TIME' | 'SCHEDULED'>('ONE_TIME');
	let newJobAccountId = $state('');
	let newJobBuckets = $state('');

	function openCreateJobModal(): void {
		createJobError = null;
		newJobName = '';
		newJobDescription = '';
		newJobType = 'ONE_TIME';
		newJobAccountId = '';
		newJobBuckets = '';
		createJobModal?.open();
	}

	async function submitCreateJob(): Promise<void> {
		const buckets = newJobBuckets
			.split(',')
			.map((b) => b.trim())
			.filter(Boolean);
		if (!newJobName || !newJobAccountId || buckets.length === 0) {
			createJobError = 'Name, account ID, and at least one bucket are required.';
			return;
		}
		creatingJob = true;
		createJobError = null;
		try {
			await client().send(
				new CreateClassificationJobCommand({
					name: newJobName,
					description: newJobDescription || undefined,
					jobType: newJobType,
					s3JobDefinition: {
						bucketDefinitions: [{ accountId: newJobAccountId, buckets }]
					}
				})
			);
			toast.success('Classification job created');
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

	// ── Job detail + status transitions ─────────────────────────────────────
	let jobDetailModal = $state<Modal | null>(null);
	let viewedJob = $state<DescribeClassificationJobResponse | null>(null);
	let jobDetailLoading = $state(false);
	let jobDetailError = $state<string | null>(null);
	let jobActionError = $state<string | null>(null);

	async function openJobDetail(j: JobSummary): Promise<void> {
		viewedJob = null;
		jobDetailError = null;
		jobActionError = null;
		jobDetailModal?.open();
		if (!j.jobId) return;
		jobDetailLoading = true;
		try {
			const resp = await client().send(new DescribeClassificationJobCommand({ jobId: j.jobId }));
			viewedJob = resp;
		} catch (e) {
			jobDetailError = describeError(e);
		} finally {
			jobDetailLoading = false;
		}
	}

	async function setJobStatus(jobId: string | undefined, status: (typeof JobStatus)[keyof typeof JobStatus]): Promise<void> {
		if (!jobId) return;
		jobActionError = null;
		try {
			await client().send(new UpdateClassificationJobCommand({ jobId, jobStatus: status }));
			toast.success(`Job status set to ${status}`);
			await tabLoader.refresh('jobs');
			if (viewedJob) {
				const resp = await client().send(new DescribeClassificationJobCommand({ jobId }));
				viewedJob = resp;
			}
		} catch (e) {
			const msg = describeError(e);
			jobActionError = msg;
			toast.error(msg);
		}
	}

	async function cancelJob(j: JobSummary): Promise<void> {
		if (!j.jobId) return;
		const confirmed = await confirmDestructive({
			title: 'Cancel classification job',
			message: `Cancel job "${j.name ?? j.jobId}"? Amazon Macie has no delete operation for classification jobs -- cancelling stops it permanently.`
		});
		if (!confirmed) return;
		await setJobStatus(j.jobId, JobStatus.CANCELLED);
	}

	// ── Custom Data Identifiers: create / detail / delete ──────────────────
	let createIdModal = $state<Modal | null>(null);
	let creatingId = $state(false);
	let createIdError = $state<string | null>(null);
	let newIdName = $state('');
	let newIdRegex = $state('');
	let newIdDescription = $state('');
	let newIdKeywords = $state('');
	let newIdIgnoreWords = $state('');

	function openCreateIdModal(): void {
		createIdError = null;
		newIdName = '';
		newIdRegex = '';
		newIdDescription = '';
		newIdKeywords = '';
		newIdIgnoreWords = '';
		createIdModal?.open();
	}

	async function submitCreateId(): Promise<void> {
		if (!newIdName || !newIdRegex) {
			createIdError = 'Name and regex are required.';
			return;
		}
		creatingId = true;
		createIdError = null;
		try {
			await client().send(
				new CreateCustomDataIdentifierCommand({
					name: newIdName,
					regex: newIdRegex,
					description: newIdDescription || undefined,
					keywords: newIdKeywords
						? newIdKeywords.split(',').map((k) => k.trim()).filter(Boolean)
						: undefined,
					ignoreWords: newIdIgnoreWords
						? newIdIgnoreWords.split(',').map((k) => k.trim()).filter(Boolean)
						: undefined
				})
			);
			toast.success('Custom data identifier created');
			createIdModal?.close();
			await tabLoader.refresh('identifiers');
		} catch (e) {
			const msg = describeError(e);
			createIdError = msg;
			toast.error(msg);
		} finally {
			creatingId = false;
		}
	}

	let idDetailModal = $state<Modal | null>(null);
	let viewedIdentifier = $state<GetCustomDataIdentifierResponse | null>(null);
	let idDetailLoading = $state(false);
	let idDetailError = $state<string | null>(null);

	async function openIdentifierDetail(i: CustomDataIdentifierSummary): Promise<void> {
		viewedIdentifier = null;
		idDetailError = null;
		idDetailModal?.open();
		if (!i.id) return;
		idDetailLoading = true;
		try {
			const resp = await client().send(new GetCustomDataIdentifierCommand({ id: i.id }));
			viewedIdentifier = resp;
		} catch (e) {
			idDetailError = describeError(e);
		} finally {
			idDetailLoading = false;
		}
	}

	async function deleteIdentifier(i: CustomDataIdentifierSummary): Promise<void> {
		if (!i.id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete custom data identifier',
			message: `Delete custom data identifier "${i.name ?? i.id}"? Amazon Macie soft-deletes identifiers -- it stops matching in future jobs but the record remains visible via Get.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteCustomDataIdentifierCommand({ id: i.id }));
			toast.success('Custom data identifier deleted');
			idDetailModal?.close();
			await tabLoader.refresh('identifiers');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Allow Lists: create / detail / edit / delete ────────────────────────
	let createAllowListModal = $state<Modal | null>(null);
	let creatingAllowList = $state(false);
	let createAllowListError = $state<string | null>(null);
	let newAllowListName = $state('');
	let newAllowListDescription = $state('');
	let newAllowListRegex = $state('');

	function openCreateAllowListModal(): void {
		createAllowListError = null;
		newAllowListName = '';
		newAllowListDescription = '';
		newAllowListRegex = '';
		createAllowListModal?.open();
	}

	async function submitCreateAllowList(): Promise<void> {
		if (!newAllowListName || !newAllowListRegex) {
			createAllowListError = 'Name and regex are required.';
			return;
		}
		creatingAllowList = true;
		createAllowListError = null;
		try {
			await client().send(
				new CreateAllowListCommand({
					name: newAllowListName,
					description: newAllowListDescription || undefined,
					criteria: { regex: newAllowListRegex }
				})
			);
			toast.success('Allow list created');
			createAllowListModal?.close();
			await tabLoader.refresh('allowlists');
		} catch (e) {
			const msg = describeError(e);
			createAllowListError = msg;
			toast.error(msg);
		} finally {
			creatingAllowList = false;
		}
	}

	let allowListDetailModal = $state<Modal | null>(null);
	let viewedAllowList = $state<GetAllowListResponse | null>(null);
	let allowListDetailLoading = $state(false);
	let allowListDetailError = $state<string | null>(null);

	async function openAllowListDetail(a: AllowListSummary): Promise<void> {
		viewedAllowList = null;
		allowListDetailError = null;
		allowListDetailModal?.open();
		if (!a.id) return;
		allowListDetailLoading = true;
		try {
			const resp = await client().send(new GetAllowListCommand({ id: a.id }));
			viewedAllowList = resp;
		} catch (e) {
			allowListDetailError = describeError(e);
		} finally {
			allowListDetailLoading = false;
		}
	}

	let editAllowListModal = $state<Modal | null>(null);
	let editingAllowList = $state(false);
	let editAllowListError = $state<string | null>(null);
	let editAllowListId = $state('');
	let editAllowListName = $state('');
	let editAllowListDescription = $state('');
	let editAllowListRegex = $state('');

	function openEditAllowListModal(a: GetAllowListResponse & { id?: string }): void {
		editAllowListError = null;
		editAllowListId = a.id ?? '';
		editAllowListName = a.name ?? '';
		editAllowListDescription = a.description ?? '';
		editAllowListRegex = a.criteria?.regex ?? '';
		editAllowListModal?.open();
	}

	async function submitEditAllowList(): Promise<void> {
		if (!editAllowListId || !editAllowListName || !editAllowListRegex) {
			editAllowListError = 'Name and regex are required.';
			return;
		}
		editingAllowList = true;
		editAllowListError = null;
		try {
			const criteria: AllowListCriteria = { regex: editAllowListRegex };
			await client().send(
				new UpdateAllowListCommand({
					id: editAllowListId,
					name: editAllowListName,
					description: editAllowListDescription || undefined,
					criteria
				})
			);
			toast.success('Allow list updated');
			editAllowListModal?.close();
			await tabLoader.refresh('allowlists');
			const resp = await client().send(new GetAllowListCommand({ id: editAllowListId }));
			viewedAllowList = resp;
		} catch (e) {
			const msg = describeError(e);
			editAllowListError = msg;
			toast.error(msg);
		} finally {
			editingAllowList = false;
		}
	}

	async function deleteAllowList(a: AllowListSummary): Promise<void> {
		if (!a.id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete allow list',
			message: `Delete allow list "${a.name ?? a.id}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAllowListCommand({ id: a.id }));
			toast.success('Allow list deleted');
			allowListDetailModal?.close();
			await tabLoader.refresh('allowlists');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Findings Filters: create / detail / edit / delete ──────────────────
	let createFilterModal = $state<Modal | null>(null);
	let creatingFilter = $state(false);
	let createFilterError = $state<string | null>(null);
	let newFilterName = $state('');
	let newFilterDescription = $state('');
	let newFilterAction = $state<'ARCHIVE' | 'NOOP'>('NOOP');
	let newFilterField = $state('type');
	let newFilterValue = $state('');

	function openCreateFilterModal(): void {
		createFilterError = null;
		newFilterName = '';
		newFilterDescription = '';
		newFilterAction = 'NOOP';
		newFilterField = 'type';
		newFilterValue = '';
		createFilterModal?.open();
	}

	async function submitCreateFilter(): Promise<void> {
		if (!newFilterName || !newFilterField || !newFilterValue) {
			createFilterError = 'Name, field, and value are required.';
			return;
		}
		creatingFilter = true;
		createFilterError = null;
		try {
			await client().send(
				new CreateFindingsFilterCommand({
					name: newFilterName,
					description: newFilterDescription || undefined,
					action: newFilterAction,
					findingCriteria: {
						criterion: {
							[newFilterField]: { eq: newFilterValue.split(',').map((v) => v.trim()).filter(Boolean) }
						}
					}
				})
			);
			toast.success('Findings filter created');
			createFilterModal?.close();
			await tabLoader.refresh('filters');
		} catch (e) {
			const msg = describeError(e);
			createFilterError = msg;
			toast.error(msg);
		} finally {
			creatingFilter = false;
		}
	}

	let filterDetailModal = $state<Modal | null>(null);
	let viewedFilter = $state<GetFindingsFilterResponse | null>(null);
	let filterDetailLoading = $state(false);
	let filterDetailError = $state<string | null>(null);

	async function openFilterDetail(f: FindingsFilterListItem): Promise<void> {
		viewedFilter = null;
		filterDetailError = null;
		filterDetailModal?.open();
		if (!f.id) return;
		filterDetailLoading = true;
		try {
			const resp = await client().send(new GetFindingsFilterCommand({ id: f.id }));
			viewedFilter = resp;
		} catch (e) {
			filterDetailError = describeError(e);
		} finally {
			filterDetailLoading = false;
		}
	}

	let editFilterModal = $state<Modal | null>(null);
	let editingFilter = $state(false);
	let editFilterError = $state<string | null>(null);
	let editFilterId = $state('');
	let editFilterName = $state('');
	let editFilterDescription = $state('');
	let editFilterAction = $state<'ARCHIVE' | 'NOOP'>('NOOP');

	function openEditFilterModal(f: GetFindingsFilterResponse & { id?: string }): void {
		editFilterError = null;
		editFilterId = f.id ?? '';
		editFilterName = f.name ?? '';
		editFilterDescription = f.description ?? '';
		editFilterAction = f.action === 'ARCHIVE' ? 'ARCHIVE' : 'NOOP';
		editFilterModal?.open();
	}

	async function submitEditFilter(): Promise<void> {
		if (!editFilterId || !editFilterName) {
			editFilterError = 'Name is required.';
			return;
		}
		editingFilter = true;
		editFilterError = null;
		try {
			await client().send(
				new UpdateFindingsFilterCommand({
					id: editFilterId,
					name: editFilterName,
					description: editFilterDescription || undefined,
					action: editFilterAction,
					findingCriteria: viewedFilter?.findingCriteria
				})
			);
			toast.success('Findings filter updated');
			editFilterModal?.close();
			await tabLoader.refresh('filters');
			const resp = await client().send(new GetFindingsFilterCommand({ id: editFilterId }));
			viewedFilter = resp;
		} catch (e) {
			const msg = describeError(e);
			editFilterError = msg;
			toast.error(msg);
		} finally {
			editingFilter = false;
		}
	}

	async function deleteFilter(f: FindingsFilterListItem): Promise<void> {
		if (!f.id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete findings filter',
			message: `Delete findings filter "${f.name ?? f.id}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteFindingsFilterCommand({ id: f.id }));
			toast.success('Findings filter deleted');
			filterDetailModal?.close();
			await tabLoader.refresh('filters');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Findings: detail + sample generation ────────────────────────────────
	let findingDetailModal = $state<Modal | null>(null);
	let viewedFinding = $state<Finding | null>(null);

	function openFindingDetail(f: Finding): void {
		viewedFinding = f;
		findingDetailModal?.open();
	}

	let creatingSamples = $state(false);
	async function createSampleFindings(): Promise<void> {
		creatingSamples = true;
		try {
			await client().send(new CreateSampleFindingsCommand({}));
			toast.success('Sample findings requested');
			await tabLoader.refresh('findings');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			creatingSamples = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={ShieldAlert}
		title="Amazon Macie"
		description="Sensitive data discovery for S3"
		onRefresh={handleRefresh}
		color="orange"
	>
		{#snippet actions()}
			{#if activeTab === 'jobs'}
				<button
					onclick={openCreateJobModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create job
				</button>
			{:else if activeTab === 'identifiers'}
				<button
					onclick={openCreateIdModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create identifier
				</button>
			{:else if activeTab === 'allowlists'}
				<button
					onclick={openCreateAllowListModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create allow list
				</button>
			{:else if activeTab === 'filters'}
				<button
					onclick={openCreateFilterModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create filter
				</button>
			{:else if activeTab === 'findings'}
				<button
					onclick={createSampleFindings}
					disabled={creatingSamples}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm disabled:opacity-50"
				>
					<Sparkles class="w-4 h-4" /> {creatingSamples ? 'Requesting…' : 'Create sample findings'}
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="orange" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'jobs'}
				{#snippet jobStatusCell(j: JobSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(j.jobStatus)}">{j.jobStatus ?? '—'}</span>
				{/snippet}
				{#snippet jobActionsCell(j: JobSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openJobDetail(j)} title="View" aria-label="View job {j.jobId}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => cancelJob(j)} title="Cancel" aria-label="Cancel job {j.jobId}" class="text-gray-400 hover:text-red-500"><Ban class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const jobColumns = defineColumns<JobSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'jobId', label: 'Job ID' },
					{ key: 'jobType', label: 'Type' },
					{ key: 'jobStatus', label: 'Status', render: jobStatusCell },
					{ key: 'actions', label: '', render: jobActionsCell }
				])}
				<DataTable
					rows={filteredJobs}
					rowKey={(j) => j.jobId ?? ''}
					columns={jobColumns}
					loading={tabLoader.isLoading('jobs')}
					emptyMessage="No classification jobs found"
				/>
			{:else if activeTab === 'identifiers'}
				{#snippet idActionsCell(i: CustomDataIdentifierSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openIdentifierDetail(i)} title="View" aria-label="View identifier {i.id}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteIdentifier(i)} title="Delete" aria-label="Delete identifier {i.id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const idColumns = defineColumns<CustomDataIdentifierSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'id', label: 'ID' },
					{ key: 'description', label: 'Description' },
					{ key: 'actions', label: '', render: idActionsCell }
				])}
				<DataTable
					rows={filteredIdentifiers}
					rowKey={(i) => i.id ?? ''}
					columns={idColumns}
					loading={tabLoader.isLoading('identifiers')}
					emptyMessage="No custom data identifiers found"
				/>
			{:else if activeTab === 'allowlists'}
				{#snippet allowListActionsCell(a: AllowListSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openAllowListDetail(a)} title="View" aria-label="View allow list {a.id}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteAllowList(a)} title="Delete" aria-label="Delete allow list {a.id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const allowListColumns = defineColumns<AllowListSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'id', label: 'ID' },
					{ key: 'description', label: 'Description' },
					{ key: 'actions', label: '', render: allowListActionsCell }
				])}
				<DataTable
					rows={filteredAllowLists}
					rowKey={(a) => a.id ?? ''}
					columns={allowListColumns}
					loading={tabLoader.isLoading('allowlists')}
					emptyMessage="No allow lists found"
				/>
			{:else if activeTab === 'filters'}
				{#snippet filterActionCell(f: FindingsFilterListItem)}
					<span class="text-xs px-2 py-1 rounded-full {f.action === 'ARCHIVE' ? 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{f.action ?? '—'}</span>
				{/snippet}
				{#snippet filterActionsCell(f: FindingsFilterListItem)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openFilterDetail(f)} title="View" aria-label="View filter {f.id}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteFilter(f)} title="Delete" aria-label="Delete filter {f.id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const filterColumns = defineColumns<FindingsFilterListItem>([
					{ key: 'name', label: 'Name' },
					{ key: 'id', label: 'ID' },
					{ key: 'action', label: 'Action', render: filterActionCell },
					{ key: 'actions', label: '', render: filterActionsCell }
				])}
				<DataTable
					rows={filteredFilters}
					rowKey={(f) => f.id ?? ''}
					columns={filterColumns}
					loading={tabLoader.isLoading('filters')}
					emptyMessage="No findings filters found"
				/>
			{:else if activeTab === 'findings'}
				{#snippet findingSeverityCell(f: Finding)}
					<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{f.severity?.description ?? '—'}</span>
				{/snippet}
				{#snippet findingActionsCell(f: Finding)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openFindingDetail(f)} title="View" aria-label="View finding {f.id}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const findingColumns = defineColumns<Finding>([
					{ key: 'title', label: 'Title' },
					{ key: 'type', label: 'Type' },
					{ key: 'category', label: 'Category' },
					{ key: 'severity', label: 'Severity', render: findingSeverityCell },
					{ key: 'actions', label: '', render: findingActionsCell }
				])}
				<DataTable
					rows={filteredFindings}
					rowKey={(f) => f.id ?? ''}
					columns={findingColumns}
					loading={tabLoader.isLoading('findings')}
					emptyMessage="No findings found — Macie only produces findings from classification jobs or CreateSampleFindings"
				/>
			{/if}
		</div>
	</div>
</div>

<!-- Create Classification Job -->
<Modal bind:this={createJobModal} title="Create Classification Job">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="job-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="job-name" bind:value={newJobName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="job-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="job-description" bind:value={newJobDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="job-type" class="text-sm text-slate-600 dark:text-slate-300">Schedule</label>
				<select id="job-type" bind:value={newJobType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value={JobType.ONE_TIME}>ONE_TIME</option>
					<option value={JobType.SCHEDULED}>SCHEDULED</option>
				</select>
			</div>
			<div>
				<label for="job-account" class="text-sm text-slate-600 dark:text-slate-300">Bucket owner account ID</label>
				<input id="job-account" bind:value={newJobAccountId} placeholder="123456789012" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="job-buckets" class="text-sm text-slate-600 dark:text-slate-300">Buckets (comma-separated)</label>
				<input id="job-buckets" bind:value={newJobBuckets} placeholder="my-bucket-1, my-bucket-2" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createJobError}
				<p class="text-sm text-red-600 dark:text-red-400">{createJobError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createJobModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateJob} disabled={creatingJob} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingJob ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Job detail -->
<Modal bind:this={jobDetailModal} title="Classification Job">
	{#snippet children()}
		{#if jobDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if jobDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{jobDetailError}</p>
		{:else if viewedJob}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Job ID</dt><dd class="text-slate-900 dark:text-white">{viewedJob.jobId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedJob.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedJob.jobStatus ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedJob.jobType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedJob.createdAt)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Buckets</dt>
					<dd class="text-slate-900 dark:text-white">
						{#each viewedJob.s3JobDefinition?.bucketDefinitions ?? [] as bd (bd.accountId)}
							<div>{bd.accountId}: {(bd.buckets ?? []).join(', ')}</div>
						{/each}
					</dd>
				</div>
				{#if jobActionError}
					<p class="text-sm text-red-600 dark:text-red-400">{jobActionError}</p>
				{/if}
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => jobDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedJob}
			{#if viewedJob.jobStatus === 'USER_PAUSED'}
				<button type="button" onclick={() => setJobStatus(viewedJob?.jobId, JobStatus.RUNNING)} class="flex items-center gap-2 rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white hover:bg-green-700"><Play class="w-4 h-4" /> Resume</button>
			{:else}
				<button type="button" onclick={() => setJobStatus(viewedJob?.jobId, JobStatus.USER_PAUSED)} class="flex items-center gap-2 rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700"><Pause class="w-4 h-4" /> Pause</button>
			{/if}
			<button type="button" onclick={() => setJobStatus(viewedJob?.jobId, JobStatus.CANCELLED)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Ban class="w-4 h-4" /> Cancel</button>
		{/if}
	{/snippet}
</Modal>

<!-- Create Custom Data Identifier -->
<Modal bind:this={createIdModal} title="Create Custom Data Identifier">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="id-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="id-name" bind:value={newIdName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="id-regex" class="text-sm text-slate-600 dark:text-slate-300">Regex</label>
				<input id="id-regex" bind:value={newIdRegex} placeholder="[0-9]{'{'}9{'}'}" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono" />
			</div>
			<div>
				<label for="id-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="id-description" bind:value={newIdDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="id-keywords" class="text-sm text-slate-600 dark:text-slate-300">Keywords (comma-separated)</label>
				<input id="id-keywords" bind:value={newIdKeywords} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="id-ignorewords" class="text-sm text-slate-600 dark:text-slate-300">Ignore words (comma-separated)</label>
				<input id="id-ignorewords" bind:value={newIdIgnoreWords} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">Custom data identifiers cannot be edited once created — the real API has no Update operation. Delete soft-deletes it.</p>
			{#if createIdError}
				<p class="text-sm text-red-600 dark:text-red-400">{createIdError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createIdModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateId} disabled={creatingId} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingId ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Custom Data Identifier detail -->
<Modal bind:this={idDetailModal} title="Custom Data Identifier">
	{#snippet children()}
		{#if idDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if idDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{idDetailError}</p>
		{:else if viewedIdentifier}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white break-all">{viewedIdentifier.id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedIdentifier.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Regex</dt><dd class="text-slate-900 dark:text-white font-mono">{viewedIdentifier.regex ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedIdentifier.description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Deleted</dt><dd class="text-slate-900 dark:text-white">{viewedIdentifier.deleted ? 'Yes (soft-deleted)' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedIdentifier.createdAt)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => idDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedIdentifier && !viewedIdentifier.deleted}
			<button type="button" onclick={() => deleteIdentifier({ id: viewedIdentifier?.id, name: viewedIdentifier?.name })} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Create Allow List -->
<Modal bind:this={createAllowListModal} title="Create Allow List">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="al-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="al-name" bind:value={newAllowListName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="al-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="al-description" bind:value={newAllowListDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="al-regex" class="text-sm text-slate-600 dark:text-slate-300">Regex to ignore</label>
				<input id="al-regex" bind:value={newAllowListRegex} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono" />
			</div>
			{#if createAllowListError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAllowListError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createAllowListModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateAllowList} disabled={creatingAllowList} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingAllowList ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Allow List detail -->
<Modal bind:this={allowListDetailModal} title="Allow List">
	{#snippet children()}
		{#if allowListDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if allowListDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{allowListDetailError}</p>
		{:else if viewedAllowList}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white break-all">{viewedAllowList.id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedAllowList.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedAllowList.description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Criteria</dt><dd class="text-slate-900 dark:text-white font-mono">{viewedAllowList.criteria?.regex ?? viewedAllowList.criteria?.s3WordsList?.objectKey ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedAllowList.status?.code ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedAllowList.createdAt)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => allowListDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedAllowList}
			<button type="button" onclick={() => openEditAllowListModal(viewedAllowList as GetAllowListResponse & { id?: string })} class="flex items-center gap-2 rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => deleteAllowList(viewedAllowList as AllowListSummary)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Allow List -->
<Modal bind:this={editAllowListModal} title="Edit Allow List">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="al-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="al-edit-name" bind:value={editAllowListName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="al-edit-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="al-edit-description" bind:value={editAllowListDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="al-edit-regex" class="text-sm text-slate-600 dark:text-slate-300">Regex to ignore</label>
				<input id="al-edit-regex" bind:value={editAllowListRegex} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono" />
			</div>
			{#if editAllowListError}
				<p class="text-sm text-red-600 dark:text-red-400">{editAllowListError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editAllowListModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditAllowList} disabled={editingAllowList} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{editingAllowList ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Findings Filter -->
<Modal bind:this={createFilterModal} title="Create Findings Filter">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="filter-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="filter-name" bind:value={newFilterName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="filter-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="filter-description" bind:value={newFilterDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="filter-action" class="text-sm text-slate-600 dark:text-slate-300">Action</label>
				<select id="filter-action" bind:value={newFilterAction} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value={FindingsFilterAction.NOOP}>NOOP</option>
					<option value={FindingsFilterAction.ARCHIVE}>ARCHIVE</option>
				</select>
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="filter-field" class="text-sm text-slate-600 dark:text-slate-300">Criterion field</label>
					<input id="filter-field" bind:value={newFilterField} placeholder="type" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono" />
				</div>
				<div>
					<label for="filter-value" class="text-sm text-slate-600 dark:text-slate-300">Equals (comma-separated)</label>
					<input id="filter-value" bind:value={newFilterValue} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			{#if createFilterError}
				<p class="text-sm text-red-600 dark:text-red-400">{createFilterError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createFilterModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateFilter} disabled={creatingFilter} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingFilter ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Findings Filter detail -->
<Modal bind:this={filterDetailModal} title="Findings Filter">
	{#snippet children()}
		{#if filterDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if filterDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{filterDetailError}</p>
		{:else if viewedFilter}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white break-all">{viewedFilter.id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedFilter.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedFilter.description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Action</dt><dd class="text-slate-900 dark:text-white">{viewedFilter.action ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Criteria</dt>
					<dd class="text-slate-900 dark:text-white">
						<pre class="mt-1 max-h-40 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(viewedFilter.findingCriteria ?? {}, null, 2)}</pre>
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => filterDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedFilter}
			<button type="button" onclick={() => openEditFilterModal(viewedFilter as GetFindingsFilterResponse & { id?: string })} class="flex items-center gap-2 rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => deleteFilter(viewedFilter as FindingsFilterListItem)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Findings Filter -->
<Modal bind:this={editFilterModal} title="Edit Findings Filter">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="filter-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="filter-edit-name" bind:value={editFilterName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="filter-edit-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="filter-edit-description" bind:value={editFilterDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="filter-edit-action" class="text-sm text-slate-600 dark:text-slate-300">Action</label>
				<select id="filter-edit-action" bind:value={editFilterAction} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value={FindingsFilterAction.NOOP}>NOOP</option>
					<option value={FindingsFilterAction.ARCHIVE}>ARCHIVE</option>
				</select>
			</div>
			{#if editFilterError}
				<p class="text-sm text-red-600 dark:text-red-400">{editFilterError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editFilterModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditFilter} disabled={editingFilter} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{editingFilter ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Finding detail -->
<Modal bind:this={findingDetailModal} title="Finding">
	{#snippet children()}
		{#if viewedFinding}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white break-all">{viewedFinding.id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Title</dt><dd class="text-slate-900 dark:text-white">{viewedFinding.title ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedFinding.description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedFinding.type ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Category</dt><dd class="text-slate-900 dark:text-white">{viewedFinding.category ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Severity</dt><dd class="text-slate-900 dark:text-white">{viewedFinding.severity?.description ?? '—'} ({viewedFinding.severity?.score ?? '—'})</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Count</dt><dd class="text-slate-900 dark:text-white">{viewedFinding.count ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Archived</dt><dd class="text-slate-900 dark:text-white">{viewedFinding.archived ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Sample</dt><dd class="text-slate-900 dark:text-white">{viewedFinding.sample ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedFinding.createdAt)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Resources affected</dt>
					<dd class="text-slate-900 dark:text-white">
						<pre class="mt-1 max-h-40 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(viewedFinding.resourcesAffected ?? {}, null, 2)}</pre>
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => findingDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
