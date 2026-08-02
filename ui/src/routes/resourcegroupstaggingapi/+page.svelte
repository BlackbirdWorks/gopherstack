<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getResourceGroupsTaggingAPIClient } from '$lib/aws-client';
	import {
		GetResourcesCommand,
		GetTagKeysCommand,
		GetTagValuesCommand,
		GetComplianceSummaryCommand,
		ListRequiredTagsCommand,
		TagResourcesCommand,
		UntagResourcesCommand,
		StartReportCreationCommand,
		DescribeReportCreationCommand,
		type ResourceTagMapping,
		type Summary,
		type RequiredTag,
		type GroupByAttribute,
		type DescribeReportCreationOutput
	} from '@aws-sdk/client-resource-groups-tagging-api';
	import { toast } from 'svelte-sonner';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { Tag, Plus, Trash2, Eye, X, FileText, ShieldCheck } from 'lucide-svelte';

	const client = regionalClient(getResourceGroupsTaggingAPIClient);

	type TabId = 'resources' | 'tagKeys' | 'compliance' | 'reports';

	const tabs: TabDef[] = [
		{ id: 'resources', label: 'Tagged Resources' },
		{ id: 'tagKeys', label: 'Tag Keys' },
		{ id: 'compliance', label: 'Compliance' },
		{ id: 'reports', label: 'Reports' }
	];

	// The SDK puts the AWS error code on err.name and status on
	// err.$metadata.httpStatusCode; err.message alone is usually just the
	// human-readable text. Combine them so both the toast and the inline
	// error banner show the actual code, not just a generic message.
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

	function parseCommaList(s: string): string[] {
		return s
			.split(',')
			.map((x) => x.trim())
			.filter((x) => x.length > 0);
	}

	let activeTab = $state<TabId>('resources');
	let searchQuery = $state('');

	// This service has no AwsAccountId/AccountId input on ANY of its nine
	// operations (confirmed against every *Input interface in the installed
	// @aws-sdk/client-resource-groups-tagging-api's models -- unlike
	// quicksight/s3control, which both bind account scoping to an explicit
	// field). There is no ensureAccountId step to mirror from those pages.

	// ==================== Tagged Resources ====================
	//
	// This is a CROSS-SERVICE AGGREGATOR, not an owner of any resources of
	// its own. GetResources only ever returns what has been wired into the
	// backend via cli.go's wireResourceGroupsTagging -- as of this build that
	// is 11 of the ~90 gopherstack services with native tagging support:
	// DynamoDB, SQS, SNS, Lambda, KMS, Secrets Manager, ECS, Athena, Glue,
	// ECR, and Kinesis (see cli.go:5220 and PARITY.md's gaps section, tracked
	// under bd: gopherstack-3xne). Resources tagged through any OTHER
	// service's own native TagResource API are invisible here, and
	// TagResources/UntagResources against their ARNs will land in
	// FailedResourcesMap with InvalidParameterException below. That is
	// correct, honest behavior for what is currently wired -- not a bug in
	// this page.
	//
	// Because "resources" here are discovered from other services rather
	// than owned by this one, there is no create-a-resource or delete-a-
	// resource operation to expose -- there never can be, no matter how much
	// more cross-service wiring lands. What the real API DOES offer at the
	// resource level is tag mutation: TagResources (used below both to add
	// tags to an already-visible resource, and as the nearest analog to
	// "create" -- tagging a previously untagged ARN is what makes it start
	// appearing in GetResources at all) and UntagResources (the nearest
	// analog to "delete", removing a tag rather than the resource itself).

	let resources = $state<ResourceTagMapping[]>([]);
	let resourcesPaginationToken = $state<string | undefined>();
	let loadingMoreResources = $state(false);

	let tagFilters = $state<Array<{ key: string; values: string[] }>>([]);
	let typeFilters = $state<string[]>([]);
	let filterKeyInput = $state('');
	let filterValueInput = $state('');
	let filterTypeInput = $state('');
	let includeComplianceDetails = $state(false);
	let excludeCompliantResources = $state(false);

	async function fetchResources(reset: boolean): Promise<void> {
		const awsTagFilters = tagFilters.map((f) => ({
			Key: f.key,
			Values: f.values.length > 0 ? f.values : undefined
		}));
		const resp = await client().send(
			new GetResourcesCommand({
				PaginationToken: reset ? undefined : resourcesPaginationToken,
				TagFilters: awsTagFilters.length > 0 ? awsTagFilters : undefined,
				ResourceTypeFilters: typeFilters.length > 0 ? typeFilters : undefined,
				IncludeComplianceDetails: includeComplianceDetails || undefined,
				ExcludeCompliantResources:
					includeComplianceDetails && excludeCompliantResources ? true : undefined
			})
		);
		resources = reset
			? (resp.ResourceTagMappingList ?? [])
			: [...resources, ...(resp.ResourceTagMappingList ?? [])];
		resourcesPaginationToken = resp.PaginationToken;
	}

	function addTagFilter(): void {
		if (!filterKeyInput.trim()) return;
		const vals = filterValueInput.trim() ? [filterValueInput.trim()] : [];
		tagFilters = [...tagFilters, { key: filterKeyInput.trim(), values: vals }];
		filterKeyInput = '';
		filterValueInput = '';
	}

	function removeTagFilter(idx: number): void {
		tagFilters = tagFilters.filter((_, i) => i !== idx);
	}

	function addTypeFilter(): void {
		const t = filterTypeInput.trim();
		if (!t || typeFilters.includes(t)) return;
		typeFilters = [...typeFilters, t];
		filterTypeInput = '';
	}

	function removeTypeFilter(t: string): void {
		typeFilters = typeFilters.filter((x) => x !== t);
	}

	async function applyResourceFilters(): Promise<void> {
		await tabLoader.refresh('resources');
	}

	async function loadMoreResources(): Promise<void> {
		loadingMoreResources = true;
		try {
			await fetchResources(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreResources = false;
		}
	}

	// ---- Tag Resource modal (create/update analog) ----

	let tagModal = $state<Modal | null>(null);
	let tagModalArn = $state('');
	let tagModalKey = $state('');
	let tagModalValue = $state('');
	let tagModalError = $state<string | null>(null);
	let applyingTag = $state(false);

	function openTagModal(arn = ''): void {
		tagModalArn = arn;
		tagModalKey = '';
		tagModalValue = '';
		tagModalError = null;
		tagModal?.open();
	}

	async function submitTagResource(): Promise<void> {
		if (!tagModalArn.trim() || !tagModalKey.trim()) {
			tagModalError = 'Resource ARN and tag key are required.';
			return;
		}
		applyingTag = true;
		tagModalError = null;
		try {
			const resp = await client().send(
				new TagResourcesCommand({
					ResourceARNList: [tagModalArn.trim()],
					Tags: { [tagModalKey.trim()]: tagModalValue.trim() }
				})
			);
			const failure = resp.FailedResourcesMap?.[tagModalArn.trim()];
			if (failure) {
				throw new Error(`${failure.ErrorCode ?? 'Error'}: ${failure.ErrorMessage ?? 'tag failed'}`);
			}
			toast.success('Tag applied');
			tagModal?.close();
			await tabLoader.refresh('resources');
		} catch (e) {
			const msg = describeError(e);
			tagModalError = msg;
			toast.error(msg);
		} finally {
			applyingTag = false;
		}
	}

	// ---- Remove a single tag from a resource (delete analog) ----

	async function removeResourceTag(arn: string, key: string): Promise<void> {
		try {
			const resp = await client().send(
				new UntagResourcesCommand({ ResourceARNList: [arn], TagKeys: [key] })
			);
			const failure = resp.FailedResourcesMap?.[arn];
			if (failure) {
				throw new Error(`${failure.ErrorCode ?? 'Error'}: ${failure.ErrorMessage ?? 'untag failed'}`);
			}
			toast.success(`Removed tag "${key}"`);
			await tabLoader.refresh('resources');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ---- Resource detail (re-fetches with IncludeComplianceDetails via
	// ResourceARNList, a real single-ARN lookup mode distinct from the
	// paginated/filtered list above) ----

	let resourceDetailModal = $state<Modal | null>(null);
	let viewedResource = $state<ResourceTagMapping | null>(null);
	let resourceDetailLoading = $state(false);
	let resourceDetailError = $state<string | null>(null);

	async function openResourceDetail(r: ResourceTagMapping): Promise<void> {
		viewedResource = r;
		resourceDetailError = null;
		resourceDetailModal?.open();
		if (!r.ResourceARN) return;
		resourceDetailLoading = true;
		try {
			const resp = await client().send(
				new GetResourcesCommand({
					ResourceARNList: [r.ResourceARN],
					IncludeComplianceDetails: true
				})
			);
			viewedResource = resp.ResourceTagMappingList?.[0] ?? r;
		} catch (e) {
			resourceDetailError = describeError(e);
		} finally {
			resourceDetailLoading = false;
		}
	}

	const filteredResources = $derived(
		resources.filter((r) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			if ((r.ResourceARN ?? '').toLowerCase().includes(q)) return true;
			return (r.Tags ?? []).some(
				(t) => (t.Key ?? '').toLowerCase().includes(q) || (t.Value ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ==================== Tag Keys ====================
	//
	// GetTagKeys/GetTagValues are pure enumeration operations derived from
	// whatever resources are currently tagged -- there is nothing to create,
	// update, or delete about a tag key or value directly (you mutate them
	// indirectly, by tagging/untagging a resource on the Tagged Resources
	// tab). "Detail" here is a key's list of values, fetched with its own
	// GetTagValues call and its own PaginationToken.

	type TagKeyRow = { Key: string };

	let tagKeys = $state<TagKeyRow[]>([]);
	let tagKeysPaginationToken = $state<string | undefined>();
	let loadingMoreTagKeys = $state(false);

	async function fetchTagKeys(reset: boolean): Promise<void> {
		const resp = await client().send(
			new GetTagKeysCommand({ PaginationToken: reset ? undefined : tagKeysPaginationToken })
		);
		const rows = (resp.TagKeys ?? []).map((k) => ({ Key: k }));
		tagKeys = reset ? rows : [...tagKeys, ...rows];
		tagKeysPaginationToken = resp.PaginationToken;
	}

	async function loadMoreTagKeys(): Promise<void> {
		loadingMoreTagKeys = true;
		try {
			await fetchTagKeys(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTagKeys = false;
		}
	}

	const filteredTagKeys = $derived(
		tagKeys.filter((k) => k.Key.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	let tagValuesModal = $state<Modal | null>(null);
	let viewedTagKey = $state('');
	let tagValues = $state<string[]>([]);
	let tagValuesPaginationToken = $state<string | undefined>();
	let tagValuesLoading = $state(false);
	let loadingMoreTagValues = $state(false);
	let tagValuesError = $state<string | null>(null);

	async function fetchTagValues(reset: boolean): Promise<void> {
		const resp = await client().send(
			new GetTagValuesCommand({
				Key: viewedTagKey,
				PaginationToken: reset ? undefined : tagValuesPaginationToken
			})
		);
		tagValues = reset ? (resp.TagValues ?? []) : [...tagValues, ...(resp.TagValues ?? [])];
		tagValuesPaginationToken = resp.PaginationToken;
	}

	async function openTagValues(key: string): Promise<void> {
		viewedTagKey = key;
		tagValues = [];
		tagValuesPaginationToken = undefined;
		tagValuesError = null;
		tagValuesModal?.open();
		tagValuesLoading = true;
		try {
			await fetchTagValues(true);
		} catch (e) {
			tagValuesError = describeError(e);
		} finally {
			tagValuesLoading = false;
		}
	}

	async function loadMoreTagValues(): Promise<void> {
		loadingMoreTagValues = true;
		try {
			await fetchTagValues(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTagValues = false;
		}
	}

	// ==================== Compliance ====================
	//
	// GetComplianceSummary and ListRequiredTags are both read-only reporting
	// operations on real AWS -- there is no Put/Update/Delete counterpart for
	// either, so no create/update/delete applies to this tab, only list and
	// filter. Both are architecturally limited in gopherstack today: real
	// GetComplianceSummary aggregates noncompliant-resource counts ACROSS
	// EVERY MEMBER ACCOUNT of an organization (its management-account-only
	// contract), and gopherstack simulates exactly one account per running
	// instance, so NonCompliantResources is always 0 here -- not a UI bug,
	// see PARITY.md's GetComplianceSummary note and bd: gopherstack-i710
	// (investigated and closed as "documented, not fabricated"). The same
	// architectural gap makes ListRequiredTags always return an empty list.
	// Both are still wired up below so the page reflects the real (empty)
	// output rather than hiding the operations.

	let summaryList = $state<Summary[]>([]);
	let compliancePaginationToken = $state<string | undefined>();
	let loadingMoreCompliance = $state(false);

	let regionFiltersInput = $state('');
	let resourceTypeFiltersInput = $state('');
	let tagKeyFiltersInput = $state('');
	let targetIdFiltersInput = $state('');
	let groupByRegion = $state(true);
	let groupByResourceType = $state(true);
	let groupByTargetId = $state(false);

	function selectedGroupBy(): GroupByAttribute[] {
		const g: GroupByAttribute[] = [];
		if (groupByRegion) g.push('REGION');
		if (groupByResourceType) g.push('RESOURCE_TYPE');
		if (groupByTargetId) g.push('TARGET_ID');
		return g;
	}

	async function fetchCompliance(reset: boolean): Promise<void> {
		const regionFilters = parseCommaList(regionFiltersInput);
		const resourceTypeFilters = parseCommaList(resourceTypeFiltersInput);
		const tagKeyFilters = parseCommaList(tagKeyFiltersInput);
		const targetIdFilters = parseCommaList(targetIdFiltersInput);
		const groupBy = selectedGroupBy();
		const resp = await client().send(
			new GetComplianceSummaryCommand({
				PaginationToken: reset ? undefined : compliancePaginationToken,
				RegionFilters: regionFilters.length > 0 ? regionFilters : undefined,
				ResourceTypeFilters: resourceTypeFilters.length > 0 ? resourceTypeFilters : undefined,
				TagKeyFilters: tagKeyFilters.length > 0 ? tagKeyFilters : undefined,
				TargetIdFilters: targetIdFilters.length > 0 ? targetIdFilters : undefined,
				GroupBy: groupBy.length > 0 ? groupBy : undefined
			})
		);
		summaryList = reset ? (resp.SummaryList ?? []) : [...summaryList, ...(resp.SummaryList ?? [])];
		compliancePaginationToken = resp.PaginationToken;
	}

	async function applyComplianceFilters(): Promise<void> {
		await tabLoader.refresh('compliance');
	}

	async function loadMoreCompliance(): Promise<void> {
		loadingMoreCompliance = true;
		try {
			await fetchCompliance(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCompliance = false;
		}
	}

	// ListRequiredTags paginates with NextToken/MaxResults -- NOT
	// PaginationToken, unlike every other paginated operation on this page.
	// Confirmed against the installed SDK's ListRequiredTagsInput/Output
	// (models_0.d.ts): every other Input/Output pair here declares
	// PaginationToken; only this one uses NextToken.
	let requiredTags = $state<RequiredTag[]>([]);
	let requiredTagsNextToken = $state<string | undefined>();
	let requiredTagsLoading = $state(false);
	let requiredTagsError = $state<string | null>(null);
	let requiredTagsLoaded = $state(false);

	async function fetchRequiredTags(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListRequiredTagsCommand({ NextToken: reset ? undefined : requiredTagsNextToken })
		);
		requiredTags = reset
			? (resp.RequiredTags ?? [])
			: [...requiredTags, ...(resp.RequiredTags ?? [])];
		requiredTagsNextToken = resp.NextToken;
	}

	async function loadRequiredTags(): Promise<void> {
		requiredTagsLoading = true;
		requiredTagsError = null;
		try {
			await fetchRequiredTags(true);
			requiredTagsLoaded = true;
		} catch (e) {
			requiredTagsError = describeError(e);
		} finally {
			requiredTagsLoading = false;
		}
	}

	const filteredSummary = $derived(
		summaryList.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(s.Region ?? '').toLowerCase().includes(q) ||
				(s.ResourceType ?? '').toLowerCase().includes(q) ||
				(s.TargetId ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ==================== Reports ====================
	//
	// StartReportCreation/DescribeReportCreation model a single background
	// job per region, not a list of resources -- there is nothing to select
	// a row from, so no DataTable/LoadMore here, and no update/delete
	// operation exists on real AWS for a tagging report once started. "Create"
	// is StartReportCreation; "detail" is the DescribeReportCreation status
	// panel, kept live via the same tab-loader refresh every other tab uses.

	let reportStatus = $state<DescribeReportCreationOutput | null>(null);
	let s3BucketInput = $state('');
	let startingReport = $state(false);
	let startReportError = $state<string | null>(null);

	async function fetchReportStatus(): Promise<void> {
		reportStatus = await client().send(new DescribeReportCreationCommand({}));
	}

	async function startReport(): Promise<void> {
		if (!s3BucketInput.trim()) {
			startReportError = 'S3 bucket name is required.';
			return;
		}
		startingReport = true;
		startReportError = null;
		try {
			await client().send(new StartReportCreationCommand({ S3Bucket: s3BucketInput.trim() }));
			toast.success('Report creation started');
			await tabLoader.refresh('reports');
		} catch (e) {
			const msg = describeError(e);
			startReportError = msg;
			toast.error(msg);
		} finally {
			startingReport = false;
		}
	}

	// ==================== Tab wiring ====================

	const tabLoader = createTabLoader<TabId>({
		resources: () => fetchResources(true).catch(rethrowDescribed),
		tagKeys: () => fetchTagKeys(true).catch(rethrowDescribed),
		compliance: () => fetchCompliance(true).catch(rethrowDescribed),
		reports: () => fetchReportStatus().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// Every tab's data is scoped to the currently selected region (this
	// service's Backend.Region(), resolved per-request from the same header
	// every other regional service reads -- see handler.go). See
	// quicksight/+page.svelte's identical comment for the on-mount-vs-real-
	// change reasoning this mirrors; there is no account id step to gate on
	// here, so the region effect drives the loads directly.
	let regionChangeCount = 0;
	onRegionChange(() => {
		const isInitialMount = regionChangeCount === 0;
		regionChangeCount++;
		if (isInitialMount) {
			void tabLoader.refresh(activeTab);
			return;
		}
		for (const t of tabs) {
			void tabLoader.refresh(t.id as TabId);
		}
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Tag}
		title="Resource Groups Tagging API"
		description="Cross-service tag discovery, tag mutation, and compliance reporting"
		onRefresh={handleRefresh}
		color="indigo"
	>
		{#snippet actions()}
			{#if activeTab === 'resources'}
				<button
					onclick={() => openTagModal()}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Tag a resource
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="indigo" />
			{#if activeTab !== 'reports'}
				<SearchInput bind:value={searchQuery} />
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

			{#if activeTab === 'resources'}
				<div class="rounded-lg border border-indigo-200 bg-indigo-50 px-4 py-3 text-xs text-indigo-800 dark:border-indigo-800 dark:bg-indigo-900/20 dark:text-indigo-300">
					This aggregator only sees resources tagged through services wired into it. As of this
					build that is: DynamoDB, SQS, SNS, Lambda, KMS, Secrets Manager, ECS, Athena, Glue, ECR,
					and Kinesis (11 of ~90 gopherstack services with native tagging support -- see
					PARITY.md). Resources tagged via any other service's own API will not appear here.
				</div>

				<div class="space-y-3 rounded-lg border border-slate-100 p-3 dark:border-slate-700">
					<div>
						<p class="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Tag filters</p>
						<div class="flex flex-wrap gap-2 mb-2">
							{#each tagFilters as f, idx (f.key + idx)}
								<span class="flex items-center gap-1 rounded-full bg-indigo-100 px-3 py-1 text-xs font-medium text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-300">
									{f.key}{f.values.length > 0 ? '=' + f.values.join(',') : ''}
									<button onclick={() => removeTagFilter(idx)} aria-label="Remove tag filter {f.key}" class="ml-1 hover:text-indigo-600">
										<X class="h-3 w-3" />
									</button>
								</span>
							{/each}
						</div>
						<div class="flex items-center gap-2">
							<input
								type="text"
								bind:value={filterKeyInput}
								placeholder="Tag key"
								aria-label="Tag filter key"
								class="flex-1 rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
							/>
							<input
								type="text"
								bind:value={filterValueInput}
								placeholder="Value (optional)"
								aria-label="Tag filter value"
								class="flex-1 rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
							/>
							<button onclick={addTagFilter} class="rounded-lg bg-slate-100 px-3 py-2 text-sm hover:bg-slate-200 dark:bg-slate-700 dark:hover:bg-slate-600">Add</button>
						</div>
					</div>

					<div>
						<p class="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Resource type filters</p>
						<div class="flex flex-wrap gap-2 mb-2">
							{#each typeFilters as t (t)}
								<span class="flex items-center gap-1 rounded-full bg-emerald-100 px-3 py-1 text-xs font-medium text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300">
									{t}
									<button onclick={() => removeTypeFilter(t)} aria-label="Remove resource type filter {t}" class="ml-1 hover:text-emerald-600">
										<X class="h-3 w-3" />
									</button>
								</span>
							{/each}
						</div>
						<div class="flex items-center gap-2">
							<input
								type="text"
								bind:value={filterTypeInput}
								placeholder="e.g. dynamodb:table"
								aria-label="Resource type filter"
								class="flex-1 rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
								onkeydown={(e) => { if (e.key === 'Enter') addTypeFilter(); }}
							/>
							<button onclick={addTypeFilter} class="rounded-lg bg-slate-100 px-3 py-2 text-sm hover:bg-slate-200 dark:bg-slate-700 dark:hover:bg-slate-600">Add</button>
						</div>
					</div>

					<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300">
						<input type="checkbox" bind:checked={includeComplianceDetails} />
						Include compliance details
					</label>
					{#if includeComplianceDetails}
						<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 pl-6">
							<input type="checkbox" bind:checked={excludeCompliantResources} />
							Exclude compliant resources
						</label>
					{/if}

					<button
						onclick={() => void applyResourceFilters()}
						class="rounded-lg bg-slate-800 px-4 py-2 text-sm font-medium text-white hover:bg-slate-700 dark:bg-slate-600 dark:hover:bg-slate-500"
					>
						Apply filters
					</button>
				</div>

				{#snippet resourceTagsCell(r: ResourceTagMapping)}
					<div class="flex flex-wrap gap-1">
						{#each r.Tags ?? [] as t (t.Key)}
							<span class="rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-600 dark:bg-slate-700 dark:text-slate-300">
								{t.Key}={t.Value}
							</span>
						{/each}
						{#if (r.Tags ?? []).length === 0}<span class="text-xs text-slate-400">No tags</span>{/if}
					</div>
				{/snippet}
				{#snippet resourceActionsCell(r: ResourceTagMapping)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openResourceDetail(r)}
							title="View"
							aria-label="View resource {r.ResourceARN}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openTagModal(r.ResourceARN ?? '')}
							title="Add tag"
							aria-label="Add tag to resource {r.ResourceARN}"
							class="text-gray-400 hover:text-indigo-500"><Plus class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const resourceColumns = defineColumns<ResourceTagMapping>([
					{ key: 'ResourceARN', label: 'Resource ARN' },
					{ key: 'Tags', label: 'Tags', render: resourceTagsCell },
					{ key: 'actions', label: '', render: resourceActionsCell }
				])}
				<DataTable
					rows={filteredResources}
					rowKey={(r) => r.ResourceARN ?? ''}
					columns={resourceColumns}
					loading={tabLoader.isLoading('resources')}
					emptyMessage="No tagged resources found"
				/>
				<LoadMore
					hasMore={!!resourcesPaginationToken}
					loading={loadingMoreResources}
					onLoadMore={loadMoreResources}
				/>
			{:else if activeTab === 'tagKeys'}
				{#snippet tagKeyActionsCell(k: TagKeyRow)}
					<div class="flex justify-end">
						<button
							onclick={() => openTagValues(k.Key)}
							title="View values"
							aria-label="View values for tag key {k.Key}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const tagKeyColumns = defineColumns<TagKeyRow>([
					{ key: 'Key', label: 'Tag Key' },
					{ key: 'actions', label: '', render: tagKeyActionsCell }
				])}
				<DataTable
					rows={filteredTagKeys}
					rowKey={(k) => k.Key}
					columns={tagKeyColumns}
					loading={tabLoader.isLoading('tagKeys')}
					emptyMessage="No tag keys found"
				/>
				<LoadMore
					hasMore={!!tagKeysPaginationToken}
					loading={loadingMoreTagKeys}
					onLoadMore={loadMoreTagKeys}
				/>
			{:else if activeTab === 'compliance'}
				<div class="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-xs text-amber-800 dark:border-amber-800 dark:bg-amber-900/20 dark:text-amber-300 flex items-start gap-2">
					<ShieldCheck class="w-4 h-4 mt-0.5 flex-shrink-0" />
					<span>
						Real GetComplianceSummary aggregates across every member account of an organization.
						gopherstack simulates one account per instance, so non-compliant counts are always 0
						here -- documented, not a bug (see PARITY.md, bd: gopherstack-i710).
					</span>
				</div>

				<div class="grid grid-cols-1 sm:grid-cols-2 gap-3 rounded-lg border border-slate-100 p-3 dark:border-slate-700">
					<label class="text-sm text-slate-700 dark:text-slate-300">
						Region filters (comma-separated)
						<input
							type="text"
							bind:value={regionFiltersInput}
							class="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
						/>
					</label>
					<label class="text-sm text-slate-700 dark:text-slate-300">
						Resource type filters (comma-separated)
						<input
							type="text"
							bind:value={resourceTypeFiltersInput}
							class="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
						/>
					</label>
					<label class="text-sm text-slate-700 dark:text-slate-300">
						Tag key filters (comma-separated)
						<input
							type="text"
							bind:value={tagKeyFiltersInput}
							class="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
						/>
					</label>
					<label class="text-sm text-slate-700 dark:text-slate-300">
						Target ID filters (comma-separated)
						<input
							type="text"
							bind:value={targetIdFiltersInput}
							class="mt-1 w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
						/>
					</label>
					<div class="sm:col-span-2 flex flex-wrap items-center gap-4">
						<span class="text-xs font-semibold uppercase tracking-wide text-slate-500">Group by</span>
						<label class="flex items-center gap-1 text-sm text-slate-700 dark:text-slate-300">
							<input type="checkbox" bind:checked={groupByRegion} /> Region
						</label>
						<label class="flex items-center gap-1 text-sm text-slate-700 dark:text-slate-300">
							<input type="checkbox" bind:checked={groupByResourceType} /> Resource type
						</label>
						<label class="flex items-center gap-1 text-sm text-slate-700 dark:text-slate-300">
							<input type="checkbox" bind:checked={groupByTargetId} /> Target ID
						</label>
					</div>
					<button
						onclick={() => void applyComplianceFilters()}
						class="sm:col-span-2 rounded-lg bg-slate-800 px-4 py-2 text-sm font-medium text-white hover:bg-slate-700 dark:bg-slate-600 dark:hover:bg-slate-500 w-fit"
					>
						Apply filters
					</button>
				</div>

				{@const summaryColumns = defineColumns<Summary>([
					{ key: 'Region', label: 'Region' },
					{ key: 'ResourceType', label: 'Resource Type' },
					{ key: 'TargetId', label: 'Target ID' },
					{ key: 'TargetIdType', label: 'Target Type' },
					{ key: 'NonCompliantResources', label: 'Non-Compliant' }
				])}
				<DataTable
					rows={filteredSummary}
					rowKey={(s) => `${s.TargetId ?? ''}|${s.Region ?? ''}|${s.ResourceType ?? ''}`}
					columns={summaryColumns}
					loading={tabLoader.isLoading('compliance')}
					emptyMessage="No compliance summary rows"
				/>
				<LoadMore
					hasMore={!!compliancePaginationToken}
					loading={loadingMoreCompliance}
					onLoadMore={loadMoreCompliance}
				/>

				<div class="rounded-lg border border-slate-100 p-3 dark:border-slate-700 space-y-2">
					<div class="flex items-center justify-between">
						<p class="text-sm font-semibold text-slate-700 dark:text-slate-300">Required tags (ListRequiredTags)</p>
						<button
							onclick={() => void loadRequiredTags()}
							class="rounded-lg bg-slate-100 px-3 py-1.5 text-xs hover:bg-slate-200 dark:bg-slate-700 dark:hover:bg-slate-600"
						>
							{requiredTagsLoading ? 'Loading...' : 'Load required tags'}
						</button>
					</div>
					{#if requiredTagsError}
						<p class="text-xs text-red-600 dark:text-red-400">{requiredTagsError}</p>
					{:else if requiredTagsLoaded && requiredTags.length === 0}
						<p class="text-xs text-slate-500 dark:text-slate-400">
							No required tags -- always empty here (no attached tag policy to derive them from).
						</p>
					{:else if requiredTags.length > 0}
						<ul class="text-xs text-slate-600 dark:text-slate-300 space-y-1">
							{#each requiredTags as rt, i (rt.ResourceType ?? i)}
								<li>{rt.ResourceType}: {(rt.ReportingTagKeys ?? []).join(', ') || '—'}</li>
							{/each}
						</ul>
					{/if}
				</div>
			{:else if activeTab === 'reports'}
				<div class="rounded-lg border border-slate-100 p-4 dark:border-slate-700 space-y-4 max-w-xl">
					<div class="flex items-center gap-2">
						<FileText class="w-4 h-4 text-slate-500" />
						<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Tagging report status</h3>
					</div>
					{#if tabLoader.isLoading('reports')}
						<p class="text-sm text-slate-500 dark:text-slate-400">Loading...</p>
					{:else if reportStatus}
						<dl class="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 text-sm">
							<dt class="text-slate-500 dark:text-slate-400">Status</dt>
							<dd class="text-slate-800 dark:text-slate-200">{reportStatus.Status ?? '—'}</dd>
							<dt class="text-slate-500 dark:text-slate-400">S3 Location</dt>
							<dd class="text-slate-800 dark:text-slate-200">{reportStatus.S3Location ?? '—'}</dd>
							<dt class="text-slate-500 dark:text-slate-400">Start Date</dt>
							<dd class="text-slate-800 dark:text-slate-200">{reportStatus.StartDate ?? '—'}</dd>
							{#if reportStatus.ErrorMessage}
								<dt class="text-slate-500 dark:text-slate-400">Error</dt>
								<dd class="text-red-600 dark:text-red-400">{reportStatus.ErrorMessage}</dd>
							{/if}
						</dl>
					{/if}

					<div class="border-t border-slate-100 dark:border-slate-700 pt-4 space-y-2">
						<label class="text-sm text-slate-700 dark:text-slate-300" for="report-s3-bucket">
							Start a new report -- S3 bucket
						</label>
						<div class="flex items-center gap-2">
							<input
								id="report-s3-bucket"
								type="text"
								bind:value={s3BucketInput}
								placeholder="my-tagging-report-bucket"
								class="flex-1 rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
							/>
							<button
								onclick={() => void startReport()}
								disabled={startingReport}
								class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
							>
								{startingReport ? 'Starting...' : 'Start report'}
							</button>
						</div>
						{#if startReportError}
							<p class="text-xs text-red-600 dark:text-red-400">{startReportError}</p>
						{/if}
					</div>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Tag Resource Modal -->
<Modal bind:this={tagModal} title={tagModalArn ? 'Add Tag' : 'Tag a Resource'}>
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label class="mb-1 block text-sm font-medium text-slate-700 dark:text-slate-300" for="tag-modal-arn">
					Resource ARN
				</label>
				<input
					id="tag-modal-arn"
					type="text"
					bind:value={tagModalArn}
					placeholder="arn:aws:..."
					disabled={applyingTag}
					class="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
				/>
			</div>
			<div>
				<label class="mb-1 block text-sm font-medium text-slate-700 dark:text-slate-300" for="tag-modal-key">
					Tag Key
				</label>
				<input
					id="tag-modal-key"
					type="text"
					bind:value={tagModalKey}
					placeholder="Key"
					class="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
				/>
			</div>
			<div>
				<label class="mb-1 block text-sm font-medium text-slate-700 dark:text-slate-300" for="tag-modal-value">
					Tag Value
				</label>
				<input
					id="tag-modal-value"
					type="text"
					bind:value={tagModalValue}
					placeholder="Value"
					class="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-700 dark:text-white"
				/>
			</div>
			{#if tagModalError}
				<p class="text-sm text-red-600 dark:text-red-400">{tagModalError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<div class="flex justify-end gap-2">
			<button
				onclick={() => tagModal?.close()}
				class="rounded-lg border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700"
			>
				Cancel
			</button>
			<button
				onclick={() => void submitTagResource()}
				disabled={applyingTag || !tagModalArn.trim() || !tagModalKey.trim()}
				class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>
				{applyingTag ? 'Applying...' : 'Apply Tag'}
			</button>
		</div>
	{/snippet}
</Modal>

<!-- Resource Detail Modal -->
<Modal bind:this={resourceDetailModal} title="Resource Detail">
	{#snippet children()}
		{#if resourceDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading...</p>
		{:else if resourceDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{resourceDetailError}</p>
		{:else if viewedResource}
			<div class="space-y-3">
				<p class="text-sm font-mono break-all text-slate-800 dark:text-slate-200">{viewedResource.ResourceARN}</p>
				<div>
					<p class="text-xs font-semibold uppercase tracking-wide text-slate-500 mb-1">Tags</p>
					{#if (viewedResource.Tags ?? []).length === 0}
						<p class="text-xs text-slate-400">No tags</p>
					{:else}
						<div class="space-y-1">
							{#each viewedResource.Tags ?? [] as t (t.Key)}
								<div class="flex items-center justify-between rounded bg-slate-50 px-2 py-1 dark:bg-slate-700">
									<span class="text-xs font-mono text-slate-700 dark:text-slate-200">{t.Key} = {t.Value}</span>
									<button
										onclick={() => void removeResourceTag(viewedResource?.ResourceARN ?? '', t.Key ?? '')}
										aria-label="Remove tag {t.Key}"
										class="text-slate-400 hover:text-red-500"
									>
										<Trash2 class="h-3 w-3" />
									</button>
								</div>
							{/each}
						</div>
					{/if}
				</div>
				{#if viewedResource.ComplianceDetails}
					<div>
						<p class="text-xs font-semibold uppercase tracking-wide text-slate-500 mb-1">Compliance</p>
						<p class="text-xs text-slate-600 dark:text-slate-300">
							Status: {viewedResource.ComplianceDetails.ComplianceStatus ? 'Compliant' : 'Non-compliant'}
						</p>
						{#if (viewedResource.ComplianceDetails.MissingTagKeys ?? []).length > 0}
							<p class="text-xs text-slate-600 dark:text-slate-300">
								Missing: {viewedResource.ComplianceDetails.MissingTagKeys?.join(', ')}
							</p>
						{/if}
					</div>
				{/if}
			</div>
		{/if}
	{/snippet}
</Modal>

<!-- Tag Values Modal -->
<Modal bind:this={tagValuesModal} title="Values for &quot;{viewedTagKey}&quot;">
	{#snippet children()}
		{#if tagValuesLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading...</p>
		{:else if tagValuesError}
			<p class="text-sm text-red-600 dark:text-red-400">{tagValuesError}</p>
		{:else if tagValues.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No values found for this key.</p>
		{:else}
			<ul class="space-y-1">
				{#each tagValues as v, i (v + i)}
					<li class="text-sm font-mono text-slate-700 dark:text-slate-200">{v}</li>
				{/each}
			</ul>
			<div class="mt-3">
				<LoadMore
					hasMore={!!tagValuesPaginationToken}
					loading={loadingMoreTagValues}
					onLoadMore={loadMoreTagValues}
				/>
			</div>
		{/if}
	{/snippet}
</Modal>
