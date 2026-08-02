<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getInspectorClient } from '$lib/aws-client';
	import {
		ListFindingsCommand,
		ListCoverageCommand,
		ListFiltersCommand,
		CreateFilterCommand,
		UpdateFilterCommand,
		DeleteFilterCommand,
		EnableCommand,
		DisableCommand,
		BatchGetAccountStatusCommand,
		type Finding,
		type CoveredResource,
		type Filter,
		type FilterAction,
		type FilterCriteria,
		type ResourceScanType,
		type AccountState
	} from '@aws-sdk/client-inspector2';
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
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { ShieldCheck, Plus, Trash2, Eye, Pencil, Power, PowerOff } from 'lucide-svelte';

	const client = regionalClient(getInspectorClient);

	type TabId = 'findings' | 'coverage' | 'filters' | 'enablement';

	const tabs: TabDef[] = [
		{ id: 'findings', label: 'Findings' },
		{ id: 'coverage', label: 'Coverage' },
		{ id: 'filters', label: 'Filters & Suppression Rules' },
		{ id: 'enablement', label: 'Enablement' }
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

	let activeTab = $state<TabId>('findings');
	let searchQuery = $state('');
	let severityFilter = $state<'all' | 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW'>('all');

	let findings = $state<Finding[]>([]);
	let findingsNextToken = $state<string | undefined>();
	let loadingMoreFindings = $state(false);

	let coverage = $state<CoveredResource[]>([]);
	let coverageNextToken = $state<string | undefined>();
	let loadingMoreCoverage = $state(false);

	let filters = $state<Filter[]>([]);
	let filtersNextToken = $state<string | undefined>();
	let loadingMoreFilters = $state(false);

	let accounts = $state<AccountState[]>([]);

	async function fetchFindings(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListFindingsCommand({ maxResults: 50, nextToken: reset ? undefined : findingsNextToken })
		);
		findings = reset ? (resp.findings ?? []) : [...findings, ...(resp.findings ?? [])];
		findingsNextToken = resp.nextToken;
	}

	async function fetchCoverage(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListCoverageCommand({ maxResults: 100, nextToken: reset ? undefined : coverageNextToken })
		);
		coverage = reset ? (resp.coveredResources ?? []) : [...coverage, ...(resp.coveredResources ?? [])];
		coverageNextToken = resp.nextToken;
	}

	async function fetchFilters(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListFiltersCommand({ maxResults: 50, nextToken: reset ? undefined : filtersNextToken })
		);
		filters = reset ? (resp.filters ?? []) : [...filters, ...(resp.filters ?? [])];
		filtersNextToken = resp.nextToken;
	}

	async function fetchEnablement(): Promise<void> {
		const resp = await client().send(new BatchGetAccountStatusCommand({}));
		accounts = resp.accounts ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		findings: () => fetchFindings(true).catch(rethrowDescribed),
		coverage: () => fetchCoverage(true).catch(rethrowDescribed),
		filters: () => fetchFilters(true).catch(rethrowDescribed),
		enablement: () => fetchEnablement().catch(rethrowDescribed)
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
		findings = [];
		findingsNextToken = undefined;
		coverage = [];
		coverageNextToken = undefined;
		filters = [];
		filtersNextToken = undefined;
		accounts = [];
		tabLoader.refresh(activeTab);
	});

	const filteredFindings = $derived(
		findings.filter((f) => {
			const q = searchQuery.toLowerCase();
			const text =
				(f.title ?? '').toLowerCase().includes(q) ||
				(f.packageVulnerabilityDetails?.vulnerabilityId ?? '').toLowerCase().includes(q) ||
				(f.resources?.[0]?.id ?? '').toLowerCase().includes(q);
			const sev = severityFilter === 'all' || f.severity === severityFilter;
			return text && sev;
		})
	);
	const filteredCoverage = $derived(
		coverage.filter((c) => {
			const q = searchQuery.toLowerCase();
			return (c.resourceId ?? '').toLowerCase().includes(q) || (c.resourceType ?? '').toLowerCase().includes(q);
		})
	);
	const filteredFilters = $derived(
		filters.filter((f) => (f.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreFindings(): Promise<void> {
		loadingMoreFindings = true;
		try {
			await fetchFindings(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreFindings = false;
		}
	}

	async function loadMoreCoverage(): Promise<void> {
		loadingMoreCoverage = true;
		try {
			await fetchCoverage(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCoverage = false;
		}
	}

	async function loadMoreFilters(): Promise<void> {
		loadingMoreFilters = true;
		try {
			await fetchFilters(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreFilters = false;
		}
	}

	function severityBadgeClass(sev?: string): string {
		if (sev === 'CRITICAL') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		if (sev === 'HIGH') return 'text-orange-700 bg-orange-100 dark:text-orange-300 dark:bg-orange-900';
		if (sev === 'MEDIUM') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		if (sev === 'LOW') return 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-900';
		return 'text-muted-foreground bg-muted';
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// --- Findings: detail only. Findings are scan output generated by
	// Inspector's managed scanning engines -- the real API has no
	// CreateFinding/DeleteFinding operation, so this tab is list + detail
	// only. ListFindingsResponse.Finding already carries the full shape (no
	// GetFinding operation exists), so the detail modal needs no extra
	// round-trip. ---

	let findingDetailModal = $state<Modal | null>(null);
	let viewedFinding = $state<Finding | null>(null);

	function openFindingDetail(f: Finding): void {
		viewedFinding = f;
		findingDetailModal?.open();
	}

	// --- Coverage: detail only. Coverage rows describe what Inspector's
	// scanning engines have discovered and are covering -- not user-created
	// resources, and there is no per-resource Get operation (ListCoverage's
	// CoveredResource is already the full shape). ---

	let coverageDetailModal = $state<Modal | null>(null);
	let viewedCoverage = $state<CoveredResource | null>(null);

	function openCoverageDetail(c: CoveredResource): void {
		viewedCoverage = c;
		coverageDetailModal?.open();
	}

	// --- Filters & suppression rules: full CRUD. A "suppression rule" in the
	// Inspector console is exactly a Filter with action=SUPPRESS -- there is
	// no separate suppression-rule resource or API family; action=NONE
	// filters just group/tag matching findings without suppressing them. ---

	let createFilterModal = $state<Modal | null>(null);
	let creatingFilter = $state(false);
	let createFilterError = $state<string | null>(null);
	let newFilterName = $state('');
	let newFilterAction = $state<FilterAction>('SUPPRESS');
	let newFilterReason = $state('');
	let newFilterCriteria = $state('{}');

	function openCreateFilterModal(): void {
		createFilterError = null;
		newFilterName = '';
		newFilterAction = 'SUPPRESS';
		newFilterReason = '';
		newFilterCriteria = '{}';
		createFilterModal?.open();
	}

	async function submitCreateFilter(): Promise<void> {
		if (!newFilterName) {
			createFilterError = 'Filter name is required.';
			return;
		}
		let filterCriteria: FilterCriteria;
		try {
			filterCriteria = JSON.parse(newFilterCriteria);
		} catch {
			createFilterError = 'Filter criteria must be valid JSON.';
			return;
		}
		creatingFilter = true;
		createFilterError = null;
		try {
			await client().send(
				new CreateFilterCommand({
					name: newFilterName,
					action: newFilterAction,
					reason: newFilterReason || undefined,
					filterCriteria
				})
			);
			toast.success('Filter created');
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

	async function handleDeleteFilter(f: Filter): Promise<void> {
		if (!f.arn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete filter',
			message: `Delete filter ${f.name}? Findings it currently suppresses will no longer be suppressed.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteFilterCommand({ arn: f.arn }));
			toast.success('Filter deleted');
			await tabLoader.refresh('filters');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let filterDetailModal = $state<Modal | null>(null);
	let viewedFilter = $state<Filter | null>(null);

	function openFilterDetail(f: Filter): void {
		viewedFilter = f;
		filterDetailModal?.open();
	}

	let editFilterModal = $state<Modal | null>(null);
	let editingFilter = $state(false);
	let editFilterError = $state<string | null>(null);
	let editFilterArn = $state('');
	let editFilterName = $state('');
	let editFilterAction = $state<FilterAction>('SUPPRESS');
	let editFilterReason = $state('');
	let editFilterCriteria = $state('{}');

	function openEditFilterModal(f: Filter): void {
		if (!f.arn) return;
		editFilterError = null;
		editFilterArn = f.arn;
		editFilterName = f.name ?? '';
		editFilterAction = f.action ?? 'SUPPRESS';
		editFilterReason = f.reason ?? '';
		editFilterCriteria = JSON.stringify(f.criteria ?? {}, null, 2);
		editFilterModal?.open();
	}

	async function submitEditFilter(): Promise<void> {
		if (!editFilterArn) return;
		let filterCriteria: FilterCriteria;
		try {
			filterCriteria = JSON.parse(editFilterCriteria);
		} catch {
			editFilterError = 'Filter criteria must be valid JSON.';
			return;
		}
		editingFilter = true;
		editFilterError = null;
		try {
			await client().send(
				new UpdateFilterCommand({
					filterArn: editFilterArn,
					name: editFilterName || undefined,
					action: editFilterAction,
					reason: editFilterReason || undefined,
					filterCriteria
				})
			);
			toast.success('Filter updated');
			editFilterModal?.close();
			await tabLoader.refresh('filters');
		} catch (e) {
			const msg = describeError(e);
			editFilterError = msg;
			toast.error(msg);
		} finally {
			editingFilter = false;
		}
	}

	// --- Enablement: account-wide settings, not a list of resources. Enable
	// and Disable act on the whole account for the selected resource scan
	// types; BatchGetAccountStatus reports the resulting per-resource-type
	// status. There is no create/delete here because there is no resource to
	// create or delete -- only a status to flip. ---

	const RESOURCE_SCAN_TYPES: ResourceScanType[] = ['EC2', 'ECR', 'LAMBDA', 'LAMBDA_CODE', 'CODE_REPOSITORY'];
	let selectedScanTypes = $state<Set<ResourceScanType>>(new Set(['EC2', 'ECR']));
	let enablementBusy = $state(false);

	function toggleScanType(t: ResourceScanType): void {
		const next = new Set(selectedScanTypes);
		if (next.has(t)) next.delete(t);
		else next.add(t);
		selectedScanTypes = next;
	}

	async function handleEnable(): Promise<void> {
		if (selectedScanTypes.size === 0) {
			toast.error('Select at least one resource type.');
			return;
		}
		enablementBusy = true;
		try {
			await client().send(new EnableCommand({ resourceTypes: Array.from(selectedScanTypes) }));
			toast.success('Inspector scanning enabled');
			await tabLoader.refresh('enablement');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			enablementBusy = false;
		}
	}

	async function handleDisable(): Promise<void> {
		if (selectedScanTypes.size === 0) {
			toast.error('Select at least one resource type.');
			return;
		}
		const confirmed = await confirmDestructive({
			title: 'Disable Inspector scanning',
			message: `Disable Inspector scanning for ${Array.from(selectedScanTypes).join(', ')}?`,
			confirmLabel: 'Disable'
		});
		if (!confirmed) return;
		enablementBusy = true;
		try {
			await client().send(new DisableCommand({ resourceTypes: Array.from(selectedScanTypes) }));
			toast.success('Inspector scanning disabled');
			await tabLoader.refresh('enablement');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			enablementBusy = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={ShieldCheck}
		title="Amazon Inspector"
		description="Automated vulnerability management for workloads"
		onRefresh={handleRefresh}
		color="green"
	>
		{#snippet actions()}
			{#if activeTab === 'filters'}
				<button
					onclick={openCreateFilterModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create filter
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="green" />
			{#if activeTab !== 'enablement'}
				<div class="flex gap-2">
					{#if activeTab === 'findings'}
						<select
							bind:value={severityFilter}
							class="rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-3 py-2 text-sm"
						>
							<option value="all">All Severities</option>
							<option value="CRITICAL">Critical</option>
							<option value="HIGH">High</option>
							<option value="MEDIUM">Medium</option>
							<option value="LOW">Low</option>
						</select>
					{/if}
					<SearchInput bind:value={searchQuery} />
				</div>
			{/if}
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'findings'}
				{#snippet severityCell(f: Finding)}
					<span class="rounded-full px-2 py-0.5 text-xs font-medium {severityBadgeClass(f.severity)}">
						{f.severity ?? '—'}
					</span>
				{/snippet}
				{#snippet resourceCell(f: Finding)}
					<span class="text-xs">{f.resources?.[0]?.id ?? '—'}</span>
				{/snippet}
				{#snippet fixCell(f: Finding)}
					<span class="text-xs">{f.fixAvailable ?? '—'}</span>
				{/snippet}
				{#snippet findingActionsCell(f: Finding)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => openFindingDetail(f)}
							title="View"
							aria-label="View finding {f.title}"
							class="text-gray-400 hover:text-green-500"><Eye class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const findingColumns = defineColumns<Finding>([
					{ key: 'severity', label: 'Severity', render: severityCell },
					{ key: 'title', label: 'Title' },
					{ key: 'type', label: 'Type' },
					{ key: 'resource', label: 'Resource', render: resourceCell },
					{ key: 'fixAvailable', label: 'Fix', render: fixCell },
					{ key: 'actions', label: '', render: findingActionsCell }
				])}
				<DataTable
					rows={filteredFindings}
					rowKey={(f) => f.findingArn ?? ''}
					columns={findingColumns}
					loading={tabLoader.isLoading('findings')}
					emptyMessage="No findings found"
				/>
				<LoadMore
					hasMore={!!findingsNextToken}
					loading={loadingMoreFindings}
					onLoadMore={loadMoreFindings}
				/>
			{:else if activeTab === 'coverage'}
				{#snippet coverageActionsCell(c: CoveredResource)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => openCoverageDetail(c)}
							title="View"
							aria-label="View covered resource {c.resourceId}"
							class="text-gray-400 hover:text-green-500"><Eye class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const coverageColumns = defineColumns<CoveredResource>([
					{ key: 'resourceId', label: 'Resource ID' },
					{ key: 'resourceType', label: 'Type' },
					{ key: 'scanType', label: 'Scan Type' },
					{ key: 'accountId', label: 'Account' },
					{ key: 'actions', label: '', render: coverageActionsCell }
				])}
				<DataTable
					rows={filteredCoverage}
					rowKey={(c) => `${c.resourceId ?? ''}/${c.scanType ?? ''}`}
					columns={coverageColumns}
					loading={tabLoader.isLoading('coverage')}
					emptyMessage="No covered resources"
				/>
				<LoadMore
					hasMore={!!coverageNextToken}
					loading={loadingMoreCoverage}
					onLoadMore={loadMoreCoverage}
				/>
			{:else if activeTab === 'filters'}
				{#snippet filterActionCell(f: Filter)}
					<span
						class="rounded-full px-2 py-0.5 text-xs font-medium {f.action === 'SUPPRESS'
							? 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400'
							: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}"
					>
						{f.action === 'SUPPRESS' ? 'Suppression rule' : 'Filter'}
					</span>
				{/snippet}
				{#snippet filterUpdatedCell(f: Filter)}
					{formatDate(f.updatedAt)}
				{/snippet}
				{#snippet filterActionsCell(f: Filter)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openFilterDetail(f)}
							title="View"
							aria-label="View filter {f.name}"
							class="text-gray-400 hover:text-green-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditFilterModal(f)}
							title="Edit"
							aria-label="Edit filter {f.name}"
							class="text-gray-400 hover:text-green-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteFilter(f)}
							title="Delete"
							aria-label="Delete filter {f.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const filterColumns = defineColumns<Filter>([
					{ key: 'name', label: 'Name' },
					{ key: 'action', label: 'Kind', render: filterActionCell },
					{ key: 'updatedAt', label: 'Updated', render: filterUpdatedCell },
					{ key: 'actions', label: '', render: filterActionsCell }
				])}
				<DataTable
					rows={filteredFilters}
					rowKey={(f) => f.arn ?? ''}
					columns={filterColumns}
					loading={tabLoader.isLoading('filters')}
					emptyMessage="No filters found"
				/>
				<LoadMore
					hasMore={!!filtersNextToken}
					loading={loadingMoreFilters}
					onLoadMore={loadMoreFilters}
				/>
			{:else if activeTab === 'enablement'}
				{#snippet accountStatusCell(a: AccountState)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(a.state?.status === 'ENABLED')}">{a.state?.status ?? '—'}</span>
				{/snippet}
				{#snippet ec2Cell(a: AccountState)}
					<span class="text-xs">{a.resourceState?.ec2?.status ?? '—'}</span>
				{/snippet}
				{#snippet ecrCell(a: AccountState)}
					<span class="text-xs">{a.resourceState?.ecr?.status ?? '—'}</span>
				{/snippet}
				{#snippet lambdaCell(a: AccountState)}
					<span class="text-xs">{a.resourceState?.lambda?.status ?? '—'}</span>
				{/snippet}
				{#snippet lambdaCodeCell(a: AccountState)}
					<span class="text-xs">{a.resourceState?.lambdaCode?.status ?? '—'}</span>
				{/snippet}
				{#snippet codeRepoCell(a: AccountState)}
					<span class="text-xs">{a.resourceState?.codeRepository?.status ?? '—'}</span>
				{/snippet}
				{@const accountColumns = defineColumns<AccountState>([
					{ key: 'accountId', label: 'Account' },
					{ key: 'status', label: 'Status', render: accountStatusCell },
					{ key: 'ec2', label: 'EC2', render: ec2Cell },
					{ key: 'ecr', label: 'ECR', render: ecrCell },
					{ key: 'lambda', label: 'Lambda', render: lambdaCell },
					{ key: 'lambdaCode', label: 'Lambda Code', render: lambdaCodeCell },
					{ key: 'codeRepository', label: 'Code Repo', render: codeRepoCell }
				])}
				<div class="space-y-4">
					<div>
						<p class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">Resource scan types</p>
						<div class="flex flex-wrap gap-2" role="group" aria-label="Resource scan types">
							{#each RESOURCE_SCAN_TYPES as t}
								<button
									type="button"
									aria-pressed={selectedScanTypes.has(t)}
									onclick={() => toggleScanType(t)}
									class="px-3 py-1.5 rounded-lg text-xs font-medium transition-colors {selectedScanTypes.has(t)
										? 'bg-green-600 text-white'
										: 'bg-gray-100 dark:bg-slate-700 text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-slate-600'}"
								>
									{t}
								</button>
							{/each}
						</div>
					</div>
					<div class="flex gap-2">
						<button
							onclick={handleEnable}
							disabled={enablementBusy}
							class="flex items-center gap-2 px-3 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm disabled:opacity-50"
						>
							<Power class="w-4 h-4" /> Enable
						</button>
						<button
							onclick={handleDisable}
							disabled={enablementBusy}
							class="flex items-center gap-2 px-3 py-2 rounded-lg border border-red-300 text-red-600 hover:bg-red-50 dark:border-red-800 dark:text-red-400 dark:hover:bg-red-900/20 text-sm disabled:opacity-50"
						>
							<PowerOff class="w-4 h-4" /> Disable
						</button>
					</div>

					<DataTable
						rows={accounts}
						rowKey={(a) => a.accountId ?? ''}
						columns={accountColumns}
						loading={tabLoader.isLoading('enablement')}
						emptyMessage="No account status found"
					/>
				</div>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={findingDetailModal} title="Finding">
	{#snippet children()}
		{#if viewedFinding}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Title</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFinding.title ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedFinding.findingArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Severity</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFinding.severity ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFinding.type ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFinding.status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Fix available</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFinding.fixAvailable ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Description</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFinding.description ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">First observed</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedFinding.firstObservedAt)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last observed</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedFinding.lastObservedAt)}</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => findingDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={coverageDetailModal} title="Covered Resource">
	{#snippet children()}
		{#if viewedCoverage}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Resource ID</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedCoverage.resourceId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedCoverage.resourceType ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Scan type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedCoverage.scanType ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Scan status</dt>
					<dd class="text-slate-900 dark:text-white"
						>{viewedCoverage.scanStatus?.statusCode ?? '—'} {viewedCoverage.scanStatus?.reason
							? `(${viewedCoverage.scanStatus.reason})`
							: ''}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Account</dt>
					<dd class="text-slate-900 dark:text-white">{viewedCoverage.accountId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last scanned</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedCoverage.lastScannedAt)}</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => coverageDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createFilterModal} title="Create Filter">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="filter-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="filter-name"
					bind:value={newFilterName}
					placeholder="suppress-test-findings"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="filter-action" class="text-sm text-slate-600 dark:text-slate-300">Action</label>
				<select
					id="filter-action"
					bind:value={newFilterAction}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SUPPRESS">Suppress matching findings</option>
					<option value="NONE">No action (tag/group only)</option>
				</select>
			</div>
			<div>
				<label for="filter-reason" class="text-sm text-slate-600 dark:text-slate-300">Reason (optional)</label>
				<input
					id="filter-reason"
					bind:value={newFilterReason}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="filter-criteria" class="text-sm text-slate-600 dark:text-slate-300"
					>Filter criteria (JSON, e.g. {'{"severity":[{"comparison":"EQUALS","value":"LOW"}]}'})</label
				>
				<textarea
					id="filter-criteria"
					bind:value={newFilterCriteria}
					rows="5"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createFilterError}
				<p class="text-sm text-red-600 dark:text-red-400">{createFilterError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createFilterModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateFilter}
			disabled={creatingFilter}
			class="rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white hover:bg-green-700 disabled:opacity-50"
			>{creatingFilter ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={filterDetailModal} title="Filter">
	{#snippet children()}
		{#if viewedFilter}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFilter.name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedFilter.arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Action</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFilter.action ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Reason</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFilter.reason ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Criteria</dt>
					<dd class="font-mono text-xs break-all text-slate-900 dark:text-white"
						>{JSON.stringify(viewedFilter.criteria ?? {}, null, 2)}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedFilter.createdAt)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedFilter.updatedAt)}</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => filterDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editFilterModal} title="Edit Filter">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="filter-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="filter-edit-name"
					bind:value={editFilterName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="filter-edit-action" class="text-sm text-slate-600 dark:text-slate-300">Action</label>
				<select
					id="filter-edit-action"
					bind:value={editFilterAction}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SUPPRESS">Suppress matching findings</option>
					<option value="NONE">No action (tag/group only)</option>
				</select>
			</div>
			<div>
				<label for="filter-edit-reason" class="text-sm text-slate-600 dark:text-slate-300">Reason (optional)</label>
				<input
					id="filter-edit-reason"
					bind:value={editFilterReason}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="filter-edit-criteria" class="text-sm text-slate-600 dark:text-slate-300"
					>Filter criteria (JSON)</label
				>
				<textarea
					id="filter-edit-criteria"
					bind:value={editFilterCriteria}
					rows="5"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editFilterError}
				<p class="text-sm text-red-600 dark:text-red-400">{editFilterError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editFilterModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditFilter}
			disabled={editingFilter}
			class="rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white hover:bg-green-700 disabled:opacity-50"
			>{editingFilter ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>
