<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getSWFClient } from '$lib/aws-client';
	import {
		ListDomainsCommand,
		RegisterDomainCommand,
		DescribeDomainCommand,
		DeprecateDomainCommand,
		UndeprecateDomainCommand,
		ListWorkflowTypesCommand,
		RegisterWorkflowTypeCommand,
		DescribeWorkflowTypeCommand,
		DeprecateWorkflowTypeCommand,
		UndeprecateWorkflowTypeCommand,
		DeleteWorkflowTypeCommand,
		ListActivityTypesCommand,
		RegisterActivityTypeCommand,
		DescribeActivityTypeCommand,
		DeprecateActivityTypeCommand,
		UndeprecateActivityTypeCommand,
		DeleteActivityTypeCommand,
		ListOpenWorkflowExecutionsCommand,
		ListClosedWorkflowExecutionsCommand,
		StartWorkflowExecutionCommand,
		DescribeWorkflowExecutionCommand,
		GetWorkflowExecutionHistoryCommand,
		TerminateWorkflowExecutionCommand,
		RequestCancelWorkflowExecutionCommand,
		SignalWorkflowExecutionCommand,
		ListTagsForResourceCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type DomainInfo,
		type DomainDetail,
		type WorkflowTypeInfo,
		type WorkflowTypeDetail,
		type ActivityTypeInfo,
		type ActivityTypeDetail,
		type WorkflowExecutionInfo,
		type WorkflowExecutionDetail,
		type HistoryEvent,
		type SWFClient
	} from '@aws-sdk/client-swf';
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
	import {
		Workflow,
		Globe,
		Play,
		Plus,
		Eye,
		Ban,
		RotateCcw,
		Trash2,
		Bell,
		XCircle,
		Square,
		ChevronRight,
		ChevronDown
	} from 'lucide-svelte';

	const client = regionalClient(getSWFClient);

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

	// Replaces the JSON.stringify(x) antipattern: search only over the named
	// fields that are actually meaningful for each resource.
	function matches(q: string, ...vals: (string | undefined)[]): boolean {
		if (!q) return true;
		const needle = q.toLowerCase();
		return vals.some((v) => (v ?? '').toLowerCase().includes(needle));
	}

	function parseCommaList(s: string): string[] {
		return s
			.split(',')
			.map((x) => x.trim())
			.filter((x) => x.length > 0);
	}

	type TabId = 'domains' | 'workflowTypes' | 'activityTypes' | 'executions';

	const tabs: TabDef[] = [
		{ id: 'domains', label: 'Domains' },
		{ id: 'workflowTypes', label: 'Workflow Types' },
		{ id: 'activityTypes', label: 'Activity Types' },
		{ id: 'executions', label: 'Executions' }
	];

	let activeTab = $state<TabId>('domains');
	let searchQuery = $state('');

	// ==================== Domain scoping ====================
	//
	// Workflow types, activity types, and executions are all listed
	// per-domain in the real API (ListWorkflowTypes/ListActivityTypes/
	// ListOpen|ClosedWorkflowExecutions all require a `domain` argument) --
	// confirmed against services/swf/handler_workflow_types.go,
	// handler_activity_types.go, handler_workflow_executions.go. A domain
	// must be selected before those three tabs have anything to show.

	let domains = $state<DomainInfo[]>([]);
	let selectedDomain = $state('');

	async function fetchDomains(): Promise<void> {
		// ListDomains's registrationStatus argument is required by the wire
		// shape (services/swf/handler_domains.go's handleListDomainsInput has
		// no `omitempty` on it) -- there is no "give me everything" call, so
		// REGISTERED and DEPRECATED are fetched separately and merged, same
		// as every other Register/Deprecate/Undeprecate family in this
		// service.
		const [reg, dep] = await Promise.all([
			client().send(new ListDomainsCommand({ registrationStatus: 'REGISTERED', maximumPageSize: 100 })),
			client().send(new ListDomainsCommand({ registrationStatus: 'DEPRECATED', maximumPageSize: 100 }))
		]);
		domains = [...(reg.domainInfos ?? []), ...(dep.domainInfos ?? [])];
	}

	async function fetchWorkflowTypes(): Promise<void> {
		if (!selectedDomain) {
			workflowTypes = [];
			return;
		}
		const [reg, dep] = await Promise.all([
			client().send(
				new ListWorkflowTypesCommand({ domain: selectedDomain, registrationStatus: 'REGISTERED', maximumPageSize: 100 })
			),
			client().send(
				new ListWorkflowTypesCommand({ domain: selectedDomain, registrationStatus: 'DEPRECATED', maximumPageSize: 100 })
			)
		]);
		workflowTypes = [...(reg.typeInfos ?? []), ...(dep.typeInfos ?? [])];
	}

	async function fetchActivityTypes(): Promise<void> {
		if (!selectedDomain) {
			activityTypes = [];
			return;
		}
		const [reg, dep] = await Promise.all([
			client().send(
				new ListActivityTypesCommand({ domain: selectedDomain, registrationStatus: 'REGISTERED', maximumPageSize: 100 })
			),
			client().send(
				new ListActivityTypesCommand({ domain: selectedDomain, registrationStatus: 'DEPRECATED', maximumPageSize: 100 })
			)
		]);
		activityTypes = [...(reg.typeInfos ?? []), ...(dep.typeInfos ?? [])];
	}

	let workflowTypes = $state<WorkflowTypeInfo[]>([]);
	let activityTypes = $state<ActivityTypeInfo[]>([]);

	// Open/closed executions paginate independently (two separate visibility
	// APIs, each with its own nextPageToken), so each gets its own marker and
	// Load More rather than being merged into one list.
	let openExecs = $state<WorkflowExecutionInfo[]>([]);
	let openExecsNextToken = $state<string | undefined>();
	let loadingMoreOpen = $state(false);

	let closedExecs = $state<WorkflowExecutionInfo[]>([]);
	let closedExecsNextToken = $state<string | undefined>();
	let loadingMoreClosed = $state(false);

	async function fetchOpenExecutions(reset: boolean): Promise<void> {
		if (!selectedDomain) {
			openExecs = [];
			openExecsNextToken = undefined;
			return;
		}
		const now = new Date();
		const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
		const resp = await client().send(
			new ListOpenWorkflowExecutionsCommand({
				domain: selectedDomain,
				startTimeFilter: { oldestDate: oneWeekAgo, latestDate: now },
				maximumPageSize: 50,
				nextPageToken: reset ? undefined : openExecsNextToken
			})
		);
		openExecs = reset ? (resp.executionInfos ?? []) : [...openExecs, ...(resp.executionInfos ?? [])];
		openExecsNextToken = resp.nextPageToken;
	}

	async function fetchClosedExecutions(reset: boolean): Promise<void> {
		if (!selectedDomain) {
			closedExecs = [];
			closedExecsNextToken = undefined;
			return;
		}
		const now = new Date();
		const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
		const resp = await client().send(
			new ListClosedWorkflowExecutionsCommand({
				domain: selectedDomain,
				startTimeFilter: { oldestDate: oneWeekAgo, latestDate: now },
				maximumPageSize: 50,
				nextPageToken: reset ? undefined : closedExecsNextToken
			})
		);
		closedExecs = reset ? (resp.executionInfos ?? []) : [...closedExecs, ...(resp.executionInfos ?? [])];
		closedExecsNextToken = resp.nextPageToken;
	}

	async function loadMoreOpen(): Promise<void> {
		loadingMoreOpen = true;
		try {
			await fetchOpenExecutions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreOpen = false;
		}
	}

	async function loadMoreClosed(): Promise<void> {
		loadingMoreClosed = true;
		try {
			await fetchClosedExecutions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreClosed = false;
		}
	}

	const tabLoader = createTabLoader<TabId>({
		domains: () => fetchDomains().catch(rethrowDescribed),
		workflowTypes: () => fetchWorkflowTypes().catch(rethrowDescribed),
		activityTypes: () => fetchActivityTypes().catch(rethrowDescribed),
		executions: () =>
			Promise.all([fetchOpenExecutions(true), fetchClosedExecutions(true)])
				.then(() => {})
				.catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// Domain-scoped tabs need a forced reload (not the tabLoader's normal
	// "skip if already loaded" behavior) whenever the selected domain
	// changes, since the cached data belongs to the *previous* domain.
	function handleDomainChange(): void {
		tabLoader.refresh(activeTab);
	}

	// Only 'domains' is refreshed here -- reading `activeTab` inside this
	// effect would make it re-run on every switchTab() call too (an
	// `$effect` tracks every reactive value it reads, not just the region),
	// double-fetching on every tab switch. Resetting selectedDomain is
	// enough to keep the other tabs honest across a region change: the
	// template's `{#if !selectedDomain}` guard hides any stale
	// previous-region data immediately, without needing those tabs to
	// re-fetch until the user picks a domain again.
	onRegionChange(() => {
		selectedDomain = '';
		tabLoader.refresh('domains');
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredDomains = $derived(
		domains.filter((d) => matches(searchQuery, d.name, d.description, d.status))
	);
	const filteredWorkflowTypes = $derived(
		workflowTypes.filter((w) =>
			matches(searchQuery, w.workflowType?.name, w.workflowType?.version, w.status, w.description)
		)
	);
	const filteredActivityTypes = $derived(
		activityTypes.filter((a) =>
			matches(searchQuery, a.activityType?.name, a.activityType?.version, a.status, a.description)
		)
	);
	const filteredOpenExecs = $derived(
		openExecs.filter((e) =>
			matches(searchQuery, e.execution?.workflowId, e.execution?.runId, e.workflowType?.name)
		)
	);
	const filteredClosedExecs = $derived(
		closedExecs.filter((e) =>
			matches(searchQuery, e.execution?.workflowId, e.execution?.runId, e.workflowType?.name, e.closeStatus)
		)
	);

	function statusClass(status: string | undefined, activeValue: string): string {
		return status === activeValue
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	function closeStatusClass(status: string | undefined): string {
		if (status === 'COMPLETED') return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400';
		if (status === 'TERMINATED' || status === 'FAILED')
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		if (status === 'CANCELED') return 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400';
		if (status === 'TIMED_OUT') return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// ==================== Domains: register / deprecate / undeprecate / detail+tags ====================
	//
	// Domains are registered and deprecated/undeprecated, never created or
	// deleted -- there is no DeleteDomain in the real API (confirmed absent
	// from both services/swf/handler.go's GetSupportedOperations() and the
	// installed SDK's command list).

	let registerDomainModal = $state<Modal | null>(null);
	let registeringDomain = $state(false);
	let registerDomainError = $state<string | null>(null);
	let newDomainName = $state('');
	let newDomainDescription = $state('');
	let newDomainRetentionDays = $state('30');

	function openRegisterDomainModal(): void {
		registerDomainError = null;
		newDomainName = '';
		newDomainDescription = '';
		newDomainRetentionDays = '30';
		registerDomainModal?.open();
	}

	async function submitRegisterDomain(): Promise<void> {
		if (!newDomainName.trim()) {
			registerDomainError = 'Domain name is required.';
			return;
		}
		registeringDomain = true;
		registerDomainError = null;
		try {
			await client().send(
				new RegisterDomainCommand({
					name: newDomainName.trim(),
					description: newDomainDescription || undefined,
					workflowExecutionRetentionPeriodInDays: newDomainRetentionDays || '30'
				})
			);
			toast.success(`Domain "${newDomainName}" registered`);
			registerDomainModal?.close();
			await tabLoader.refresh('domains');
		} catch (e) {
			const msg = describeError(e);
			registerDomainError = msg;
			toast.error(msg);
		} finally {
			registeringDomain = false;
		}
	}

	async function handleDeprecateDomain(d: DomainInfo): Promise<void> {
		if (!d.name) return;
		// DeprecateDomain cascades DEPRECATED onto every REGISTERED workflow
		// and activity type in the domain (confirmed in
		// services/swf/PARITY.md's Notes #5 / services/swf/domains.go) --
		// warn about that side effect rather than treating this as a
		// no-consequence toggle.
		const confirmed = await confirmDestructive({
			title: 'Deprecate domain',
			message: `Deprecate domain ${d.name}? This also deprecates every registered workflow type and activity type in the domain. Open executions are not affected.`,
			confirmLabel: 'Deprecate',
			dangerous: false
		});
		if (!confirmed) return;
		try {
			await client().send(new DeprecateDomainCommand({ name: d.name }));
			toast.success(`Domain ${d.name} deprecated`);
			await tabLoader.refresh('domains');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// UndeprecateDomain does NOT cascade back onto the domain's types
	// (one-directional, per PARITY.md) and is reversible, so no confirmation
	// dialog -- matching how dms/+page.svelte treats RebootReplicationInstance.
	async function handleUndeprecateDomain(d: DomainInfo): Promise<void> {
		if (!d.name) return;
		try {
			await client().send(new UndeprecateDomainCommand({ name: d.name }));
			toast.success(`Domain ${d.name} undeprecated`);
			await tabLoader.refresh('domains');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let domainDetailModal = $state<Modal | null>(null);
	let viewedDomain = $state<DomainInfo | null>(null);
	let domainDetail = $state<DomainDetail | null>(null);
	let domainTags = $state<Array<{ key?: string; value?: string }>>([]);
	let loadingDomainDetail = $state(false);
	let newTagKey = $state('');
	let newTagValue = $state('');

	async function openDomainDetail(d: DomainInfo): Promise<void> {
		viewedDomain = d;
		domainDetail = null;
		domainTags = [];
		newTagKey = '';
		newTagValue = '';
		domainDetailModal?.open();
		if (!d.name) return;
		loadingDomainDetail = true;
		try {
			const [descResp, tagsResp] = await Promise.all([
				client().send(new DescribeDomainCommand({ name: d.name })),
				d.arn
					? client().send(new ListTagsForResourceCommand({ resourceArn: d.arn }))
					: Promise.resolve({ tags: [] })
			]);
			domainDetail = descResp;
			domainTags = tagsResp.tags ?? [];
		} catch (e) {
			toast.error('Failed to load domain detail: ' + describeError(e));
		} finally {
			loadingDomainDetail = false;
		}
	}

	// Tagging in this service applies to domains only -- confirmed against
	// services/swf/tags.go's validateDomainARNLocked, which rejects any ARN
	// that doesn't match arn:aws:swf:{region}:{account}:/domain/{name}. No
	// workflow-type/activity-type/execution ARN is ever accepted.
	async function addDomainTag(): Promise<void> {
		if (!viewedDomain?.arn || !newTagKey.trim()) return;
		try {
			await client().send(
				new TagResourceCommand({ resourceArn: viewedDomain.arn, tags: [{ key: newTagKey.trim(), value: newTagValue }] })
			);
			domainTags = [...domainTags.filter((t) => t.key !== newTagKey.trim()), { key: newTagKey.trim(), value: newTagValue }];
			newTagKey = '';
			newTagValue = '';
			toast.success('Tag added');
		} catch (e) {
			toast.error('Failed to add tag: ' + describeError(e));
		}
	}

	async function removeDomainTag(key: string | undefined): Promise<void> {
		if (!viewedDomain?.arn || !key) return;
		try {
			await client().send(new UntagResourceCommand({ resourceArn: viewedDomain.arn, tagKeys: [key] }));
			domainTags = domainTags.filter((t) => t.key !== key);
			toast.success('Tag removed');
		} catch (e) {
			toast.error('Failed to remove tag: ' + describeError(e));
		}
	}

	// ==================== Workflow Types: register / deprecate / undeprecate / delete / detail ====================

	let registerWfTypeModal = $state<Modal | null>(null);
	let registeringWfType = $state(false);
	let registerWfTypeError = $state<string | null>(null);
	let newWfName = $state('');
	let newWfVersion = $state('');
	let newWfDescription = $state('');
	let newWfTaskList = $state('default');
	let newWfTaskPriority = $state('');
	let newWfTaskStartToCloseTimeout = $state('');
	let newWfExecutionStartToCloseTimeout = $state('3600');
	let newWfChildPolicy = $state<'TERMINATE' | 'REQUEST_CANCEL' | 'ABANDON'>('TERMINATE');
	let newWfLambdaRole = $state('');

	function openRegisterWfTypeModal(): void {
		registerWfTypeError = null;
		newWfName = '';
		newWfVersion = '1.0';
		newWfDescription = '';
		newWfTaskList = 'default';
		newWfTaskPriority = '';
		newWfTaskStartToCloseTimeout = '';
		newWfExecutionStartToCloseTimeout = '3600';
		newWfChildPolicy = 'TERMINATE';
		newWfLambdaRole = '';
		registerWfTypeModal?.open();
	}

	async function submitRegisterWfType(): Promise<void> {
		if (!newWfName.trim() || !newWfVersion.trim()) {
			registerWfTypeError = 'Name and version are required.';
			return;
		}
		registeringWfType = true;
		registerWfTypeError = null;
		try {
			await client().send(
				new RegisterWorkflowTypeCommand({
					domain: selectedDomain,
					name: newWfName.trim(),
					version: newWfVersion.trim(),
					description: newWfDescription || undefined,
					defaultTaskList: newWfTaskList ? { name: newWfTaskList } : undefined,
					defaultTaskPriority: newWfTaskPriority || undefined,
					defaultTaskStartToCloseTimeout: newWfTaskStartToCloseTimeout || undefined,
					defaultExecutionStartToCloseTimeout: newWfExecutionStartToCloseTimeout || undefined,
					defaultChildPolicy: newWfChildPolicy,
					defaultLambdaRole: newWfLambdaRole || undefined
				})
			);
			toast.success(`Workflow type "${newWfName}" registered`);
			registerWfTypeModal?.close();
			await tabLoader.refresh('workflowTypes');
		} catch (e) {
			const msg = describeError(e);
			registerWfTypeError = msg;
			toast.error(msg);
		} finally {
			registeringWfType = false;
		}
	}

	async function handleDeprecateWfType(w: WorkflowTypeInfo): Promise<void> {
		const name = w.workflowType?.name;
		const version = w.workflowType?.version;
		if (!name || !version) return;
		try {
			await client().send(new DeprecateWorkflowTypeCommand({ domain: selectedDomain, workflowType: { name, version } }));
			toast.success(`Workflow type ${name} v${version} deprecated`);
			await tabLoader.refresh('workflowTypes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleUndeprecateWfType(w: WorkflowTypeInfo): Promise<void> {
		const name = w.workflowType?.name;
		const version = w.workflowType?.version;
		if (!name || !version) return;
		try {
			await client().send(new UndeprecateWorkflowTypeCommand({ domain: selectedDomain, workflowType: { name, version } }));
			toast.success(`Workflow type ${name} v${version} undeprecated`);
			await tabLoader.refresh('workflowTypes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleDeleteWfType(w: WorkflowTypeInfo): Promise<void> {
		const name = w.workflowType?.name;
		const version = w.workflowType?.version;
		if (!name || !version) return;
		const confirmed = await confirmDestructive({
			title: 'Delete workflow type',
			message: `Permanently delete workflow type ${name} v${version}? This cannot be reversed.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteWorkflowTypeCommand({ domain: selectedDomain, workflowType: { name, version } }));
			toast.success(`Workflow type ${name} v${version} deleted`);
			await tabLoader.refresh('workflowTypes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let wfTypeDetailModal = $state<Modal | null>(null);
	let viewedWfType = $state<WorkflowTypeInfo | null>(null);
	let wfTypeDetail = $state<WorkflowTypeDetail | null>(null);
	let loadingWfTypeDetail = $state(false);

	async function openWfTypeDetail(w: WorkflowTypeInfo): Promise<void> {
		viewedWfType = w;
		wfTypeDetail = null;
		wfTypeDetailModal?.open();
		const name = w.workflowType?.name;
		const version = w.workflowType?.version;
		if (!name || !version) return;
		loadingWfTypeDetail = true;
		try {
			wfTypeDetail = await client().send(
				new DescribeWorkflowTypeCommand({ domain: selectedDomain, workflowType: { name, version } })
			);
		} catch (e) {
			toast.error('Failed to load workflow type detail: ' + describeError(e));
		} finally {
			loadingWfTypeDetail = false;
		}
	}

	// ==================== Activity Types: register / deprecate / undeprecate / delete / detail ====================

	let registerAtTypeModal = $state<Modal | null>(null);
	let registeringAtType = $state(false);
	let registerAtTypeError = $state<string | null>(null);
	let newAtName = $state('');
	let newAtVersion = $state('');
	let newAtDescription = $state('');
	let newAtTaskList = $state('default');
	let newAtTaskPriority = $state('');
	let newAtHeartbeatTimeout = $state('');
	let newAtScheduleToStartTimeout = $state('');
	let newAtScheduleToCloseTimeout = $state('');
	let newAtStartToCloseTimeout = $state('300');

	function openRegisterAtTypeModal(): void {
		registerAtTypeError = null;
		newAtName = '';
		newAtVersion = '1.0';
		newAtDescription = '';
		newAtTaskList = 'default';
		newAtTaskPriority = '';
		newAtHeartbeatTimeout = '';
		newAtScheduleToStartTimeout = '';
		newAtScheduleToCloseTimeout = '';
		newAtStartToCloseTimeout = '300';
		registerAtTypeModal?.open();
	}

	async function submitRegisterAtType(): Promise<void> {
		if (!newAtName.trim() || !newAtVersion.trim()) {
			registerAtTypeError = 'Name and version are required.';
			return;
		}
		registeringAtType = true;
		registerAtTypeError = null;
		try {
			await client().send(
				new RegisterActivityTypeCommand({
					domain: selectedDomain,
					name: newAtName.trim(),
					version: newAtVersion.trim(),
					description: newAtDescription || undefined,
					defaultTaskList: newAtTaskList ? { name: newAtTaskList } : undefined,
					defaultTaskPriority: newAtTaskPriority || undefined,
					defaultTaskHeartbeatTimeout: newAtHeartbeatTimeout || undefined,
					defaultTaskScheduleToStartTimeout: newAtScheduleToStartTimeout || undefined,
					defaultTaskScheduleToCloseTimeout: newAtScheduleToCloseTimeout || undefined,
					defaultTaskStartToCloseTimeout: newAtStartToCloseTimeout || undefined
				})
			);
			toast.success(`Activity type "${newAtName}" registered`);
			registerAtTypeModal?.close();
			await tabLoader.refresh('activityTypes');
		} catch (e) {
			const msg = describeError(e);
			registerAtTypeError = msg;
			toast.error(msg);
		} finally {
			registeringAtType = false;
		}
	}

	async function handleDeprecateAtType(a: ActivityTypeInfo): Promise<void> {
		const name = a.activityType?.name;
		const version = a.activityType?.version;
		if (!name || !version) return;
		try {
			await client().send(new DeprecateActivityTypeCommand({ domain: selectedDomain, activityType: { name, version } }));
			toast.success(`Activity type ${name} v${version} deprecated`);
			await tabLoader.refresh('activityTypes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleUndeprecateAtType(a: ActivityTypeInfo): Promise<void> {
		const name = a.activityType?.name;
		const version = a.activityType?.version;
		if (!name || !version) return;
		try {
			await client().send(new UndeprecateActivityTypeCommand({ domain: selectedDomain, activityType: { name, version } }));
			toast.success(`Activity type ${name} v${version} undeprecated`);
			await tabLoader.refresh('activityTypes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleDeleteAtType(a: ActivityTypeInfo): Promise<void> {
		const name = a.activityType?.name;
		const version = a.activityType?.version;
		if (!name || !version) return;
		const confirmed = await confirmDestructive({
			title: 'Delete activity type',
			message: `Permanently delete activity type ${name} v${version}? This cannot be reversed.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteActivityTypeCommand({ domain: selectedDomain, activityType: { name, version } }));
			toast.success(`Activity type ${name} v${version} deleted`);
			await tabLoader.refresh('activityTypes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let atTypeDetailModal = $state<Modal | null>(null);
	let viewedAtType = $state<ActivityTypeInfo | null>(null);
	let atTypeDetail = $state<ActivityTypeDetail | null>(null);
	let loadingAtTypeDetail = $state(false);

	async function openAtTypeDetail(a: ActivityTypeInfo): Promise<void> {
		viewedAtType = a;
		atTypeDetail = null;
		atTypeDetailModal?.open();
		const name = a.activityType?.name;
		const version = a.activityType?.version;
		if (!name || !version) return;
		loadingAtTypeDetail = true;
		try {
			atTypeDetail = await client().send(
				new DescribeActivityTypeCommand({ domain: selectedDomain, activityType: { name, version } })
			);
		} catch (e) {
			toast.error('Failed to load activity type detail: ' + describeError(e));
		} finally {
			loadingAtTypeDetail = false;
		}
	}

	// ==================== Executions: start / signal / cancel / terminate / detail+history ====================
	//
	// Executions are started, signalled, cancel-requested, and terminated --
	// never "created" or "deleted". PollForDecisionTask/PollForActivityTask
	// and the RespondXxx family are long-poll worker APIs meant to be called
	// by a decider/activity-worker process in a tight loop, not clicked from
	// a dashboard -- intentionally not wired up here.

	let startExecModal = $state<Modal | null>(null);
	let startingExec = $state(false);
	let startExecError = $state<string | null>(null);
	let newExecWorkflowId = $state('');
	let newExecWorkflowTypeKey = $state('');
	let newExecTaskList = $state('default');
	let newExecInput = $state('');
	let newExecExecutionTimeout = $state('3600');
	let newExecTaskTimeout = $state('');
	let newExecChildPolicy = $state<'TERMINATE' | 'REQUEST_CANCEL' | 'ABANDON'>('TERMINATE');
	let newExecTagList = $state('');

	const registeredWorkflowTypesForDomain = $derived(workflowTypes.filter((w) => w.status === 'REGISTERED'));

	function openStartExecModal(): void {
		startExecError = null;
		newExecWorkflowId = '';
		newExecWorkflowTypeKey = registeredWorkflowTypesForDomain[0]
			? `${registeredWorkflowTypesForDomain[0].workflowType?.name}:${registeredWorkflowTypesForDomain[0].workflowType?.version}`
			: '';
		newExecTaskList = 'default';
		newExecInput = '';
		newExecExecutionTimeout = '3600';
		newExecTaskTimeout = '';
		newExecChildPolicy = 'TERMINATE';
		newExecTagList = '';
		startExecModal?.open();
	}

	async function submitStartExec(): Promise<void> {
		const [wfName, wfVersion] = newExecWorkflowTypeKey.split(':');
		if (!newExecWorkflowId.trim() || !wfName || !wfVersion) {
			startExecError = 'Workflow ID and a registered workflow type are required.';
			return;
		}
		startingExec = true;
		startExecError = null;
		try {
			await client().send(
				new StartWorkflowExecutionCommand({
					domain: selectedDomain,
					workflowId: newExecWorkflowId.trim(),
					workflowType: { name: wfName, version: wfVersion },
					taskList: newExecTaskList ? { name: newExecTaskList } : undefined,
					input: newExecInput || undefined,
					executionStartToCloseTimeout: newExecExecutionTimeout || undefined,
					taskStartToCloseTimeout: newExecTaskTimeout || undefined,
					childPolicy: newExecChildPolicy,
					tagList: parseCommaList(newExecTagList)
				})
			);
			toast.success(`Execution "${newExecWorkflowId}" started`);
			startExecModal?.close();
			await tabLoader.refresh('executions');
		} catch (e) {
			const msg = describeError(e);
			startExecError = msg;
			toast.error(msg);
		} finally {
			startingExec = false;
		}
	}

	let signalModal = $state<Modal | null>(null);
	let sendingSignal = $state(false);
	let signalError = $state<string | null>(null);
	let signalTarget = $state<WorkflowExecutionInfo | null>(null);
	let signalName = $state('');
	let signalInput = $state('');

	function openSignalModal(e: WorkflowExecutionInfo): void {
		signalTarget = e;
		signalName = '';
		signalInput = '';
		signalError = null;
		signalModal?.open();
	}

	async function submitSignal(): Promise<void> {
		const workflowId = signalTarget?.execution?.workflowId;
		if (!workflowId || !signalName.trim()) {
			signalError = 'Signal name is required.';
			return;
		}
		sendingSignal = true;
		signalError = null;
		try {
			await client().send(
				new SignalWorkflowExecutionCommand({
					domain: selectedDomain,
					workflowId,
					runId: signalTarget?.execution?.runId,
					signalName: signalName.trim(),
					input: signalInput || undefined
				})
			);
			toast.success(`Signal "${signalName}" sent to ${workflowId}`);
			signalModal?.close();
		} catch (e) {
			const msg = describeError(e);
			signalError = msg;
			toast.error(msg);
		} finally {
			sendingSignal = false;
		}
	}

	async function handleCancelExecution(e: WorkflowExecutionInfo): Promise<void> {
		const workflowId = e.execution?.workflowId;
		if (!workflowId) return;
		try {
			await client().send(
				new RequestCancelWorkflowExecutionCommand({ domain: selectedDomain, workflowId, runId: e.execution?.runId })
			);
			toast.success(`Cancellation requested for ${workflowId}`);
			await tabLoader.refresh('executions');
		} catch (err) {
			toast.error(describeError(err));
		}
	}

	async function handleTerminateExecution(e: WorkflowExecutionInfo): Promise<void> {
		const workflowId = e.execution?.workflowId;
		if (!workflowId) return;
		const confirmed = await confirmDestructive({
			title: 'Terminate execution',
			message: `Forcibly terminate execution ${workflowId}? This immediately closes the run; a graceful decider-driven shutdown is not attempted.`
		});
		if (!confirmed) return;
		try {
			// childPolicy is deliberately not offered here: real
			// TerminateWorkflowExecutionInput accepts an optional childPolicy
			// override, but services/swf/handler_workflow_executions.go's
			// handleTerminateWorkflowExecution parses it into
			// handleTerminateWorkflowExecutionInput.ChildPolicy and then never
			// forwards it to Backend.TerminateWorkflowExecution (whose
			// signature only takes domain/workflowId/runId/reason/details) --
			// it would be silently discarded. The execution's own stored
			// ChildPolicy (set at StartWorkflowExecution time) is what
			// actually gets applied. See report.
			await client().send(
				new TerminateWorkflowExecutionCommand({
					domain: selectedDomain,
					workflowId,
					runId: e.execution?.runId,
					reason: 'Terminated via gopherstack UI'
				})
			);
			toast.success(`Execution ${workflowId} terminated`);
			await tabLoader.refresh('executions');
		} catch (err) {
			toast.error(describeError(err));
		}
	}

	let execDetailModal = $state<Modal | null>(null);
	let viewedExec = $state<WorkflowExecutionInfo | null>(null);
	let execDetail = $state<WorkflowExecutionDetail | null>(null);
	let loadingExecDetail = $state(false);
	let historyEvents = $state<HistoryEvent[]>([]);
	let historyNextToken = $state<string | undefined>();
	let loadingMoreHistory = $state(false);
	let expandedEventId = $state<number | undefined>();

	// Real AWS keys every run of a workflow (domain+workflowId+runId) as an
	// independently queryable record. This backend's executions/history
	// tables are keyed by domain+workflowId ONLY -- confirmed against
	// services/swf/handler_workflow_executions.go's handleDescribeWorkflowExecution
	// (calls h.Backend.DescribeWorkflowExecution(domain, workflowId), the
	// wire-parsed runId is never passed through) and
	// services/swf/handler_history.go's handleGetWorkflowExecutionHistory
	// (same: only domain+workflowId reach the backend). So both calls always
	// return the CURRENT (latest) run for that workflowId, regardless of
	// which runId was requested. See gopherstack-jsi8 and PARITY.md's gaps
	// section -- this is a real, currently-open architectural limitation, not
	// a UI bug. If the row being viewed is a CLOSED execution that was later
	// superseded by a ContinueAsNew under the same workflowId, the detail and
	// history shown here will actually belong to that newer run. The banner
	// below detects and surfaces exactly that case rather than silently
	// presenting the wrong run's history as if it were this row's.
	const execDetailRunMismatch = $derived.by(() => {
		const requested = viewedExec?.execution?.runId;
		const returned = execDetail?.executionInfo?.execution?.runId;
		return !!requested && !!returned && requested !== returned;
	});

	async function openExecDetail(e: WorkflowExecutionInfo): Promise<void> {
		viewedExec = e;
		execDetail = null;
		historyEvents = [];
		historyNextToken = undefined;
		expandedEventId = undefined;
		execDetailModal?.open();
		const workflowId = e.execution?.workflowId;
		if (!workflowId) return;
		loadingExecDetail = true;
		try {
			const [detResp, histResp] = await Promise.all([
				client().send(
					new DescribeWorkflowExecutionCommand({
						domain: selectedDomain,
						execution: { workflowId, runId: e.execution?.runId ?? '' }
					})
				),
				client().send(
					new GetWorkflowExecutionHistoryCommand({
						domain: selectedDomain,
						execution: { workflowId, runId: e.execution?.runId ?? '' },
						maximumPageSize: 50
					})
				)
			]);
			execDetail = detResp;
			historyEvents = histResp.events ?? [];
			historyNextToken = histResp.nextPageToken;
		} catch (err) {
			toast.error('Failed to load execution detail: ' + describeError(err));
		} finally {
			loadingExecDetail = false;
		}
	}

	async function loadMoreHistory(): Promise<void> {
		const workflowId = viewedExec?.execution?.workflowId;
		if (!workflowId) return;
		loadingMoreHistory = true;
		try {
			const resp = await client().send(
				new GetWorkflowExecutionHistoryCommand({
					domain: selectedDomain,
					execution: { workflowId, runId: viewedExec?.execution?.runId ?? '' },
					maximumPageSize: 50,
					nextPageToken: historyNextToken
				})
			);
			historyEvents = [...historyEvents, ...(resp.events ?? [])];
			historyNextToken = resp.nextPageToken;
		} catch (e) {
			toast.error('Failed to load more history: ' + describeError(e));
		} finally {
			loadingMoreHistory = false;
		}
	}

	function toggleEvent(id: number | undefined): void {
		if (id === undefined) return;
		expandedEventId = expandedEventId === id ? undefined : id;
	}

	// Extracts the input/result/details payload attributes attached to a
	// history event so the operator can inspect the workflow/activity I/O,
	// without dumping the entire raw event via JSON.stringify.
	function eventPayload(event: HistoryEvent): Record<string, unknown> | null {
		const ev = event as unknown as Record<string, unknown>;
		const attrKey = Object.keys(ev).find((k) => k.endsWith('EventAttributes'));
		if (!attrKey) return null;
		const attrs = ev[attrKey] as Record<string, unknown> | undefined;
		if (!attrs) return null;
		const picked: Record<string, unknown> = {};
		for (const key of ['input', 'result', 'details', 'reason', 'control', 'signalName', 'cause']) {
			if (attrs[key] !== undefined) picked[key] = attrs[key];
		}
		return Object.keys(picked).length > 0 ? picked : attrs;
	}

	function eventTypeClass(type: string | undefined): string {
		if (!type) return 'text-gray-400';
		if (type.includes('Started') || type.includes('Initiated')) return 'text-green-600 dark:text-green-400';
		if (type.includes('Completed') || type.includes('Success')) return 'text-blue-600 dark:text-blue-400';
		if (type.includes('Failed') || type.includes('Terminated') || type.includes('Timed'))
			return 'text-red-600 dark:text-red-400';
		if (type.includes('Signaled') || type.includes('Cancel')) return 'text-orange-600 dark:text-orange-400';
		return 'text-slate-600 dark:text-slate-400';
	}
</script>

<!-- Domain modals -->
<Modal bind:this={registerDomainModal} title="Register Domain">
	{#snippet children()}
		<div class="space-y-4">
			{#if registerDomainError}
				<div class="text-sm text-red-600 dark:text-red-400">{registerDomainError}</div>
			{/if}
			<div>
				<label for="new-domain-name" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Domain Name</label>
				<input id="new-domain-name" bind:value={newDomainName} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="my-domain" />
			</div>
			<div>
				<label for="new-domain-desc" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Domain Description</label>
				<input id="new-domain-desc" bind:value={newDomainDescription} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="new-domain-retention" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Retention Period (days)</label>
				<input id="new-domain-retention" bind:value={newDomainRetentionDays} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="30" />
				<p class="text-xs text-gray-500 dark:text-gray-400 mt-1">"NONE" or "0" disables history retention for closed executions. Max 90.</p>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => registerDomainModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitRegisterDomain} disabled={registeringDomain} class="px-4 py-2 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">{registeringDomain ? 'Registering…' : 'Register'}</button>
	{/snippet}
</Modal>

<Modal bind:this={domainDetailModal} title="Domain Details">
	{#snippet children()}
		{#if loadingDomainDetail}
			<p class="text-sm text-gray-500">Loading…</p>
		{:else if viewedDomain}
			<div class="space-y-4 text-sm">
				<div class="space-y-2">
					{#each [
						['Name', viewedDomain.name],
						['ARN', viewedDomain.arn],
						['Status', viewedDomain.status],
						['Description', viewedDomain.description],
						['Retention (days)', domainDetail?.configuration?.workflowExecutionRetentionPeriodInDays]
					] as [label, value] (label)}
						<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
							<span class="text-gray-500">{label}</span>
							<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
						</div>
					{/each}
				</div>
				<div>
					<h3 class="text-xs font-semibold uppercase text-gray-500 mb-2">Tags</h3>
					{#if domainTags.length === 0}
						<p class="text-xs text-gray-500">No tags.</p>
					{:else}
						<div class="space-y-1 mb-2">
							{#each domainTags as tag (tag.key)}
								<div class="flex items-center justify-between gap-2 rounded bg-gray-50 dark:bg-gray-800 px-2 py-1">
									<span class="font-mono text-xs"><span class="text-violet-600 dark:text-violet-400">{tag.key}</span> = {tag.value}</span>
									<button onclick={() => removeDomainTag(tag.key)} class="text-gray-400 hover:text-red-500" title="Remove tag" aria-label="Remove tag {tag.key}"><Trash2 class="w-3.5 h-3.5" /></button>
								</div>
							{/each}
						</div>
					{/if}
					<div class="flex gap-2">
						<input bind:value={newTagKey} placeholder="Key" aria-label="Tag key" class="flex-1 px-2 py-1.5 rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs" />
						<input bind:value={newTagValue} placeholder="Value" aria-label="Tag value" class="flex-1 px-2 py-1.5 rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs" />
						<button onclick={addDomainTag} class="px-2 py-1.5 rounded bg-violet-600 text-white text-xs hover:bg-violet-700">Add</button>
					</div>
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => domainDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Workflow Type modals -->
<Modal bind:this={registerWfTypeModal} title="Register Workflow Type">
	{#snippet children()}
		<div class="space-y-4">
			{#if registerWfTypeError}
				<div class="text-sm text-red-600 dark:text-red-400">{registerWfTypeError}</div>
			{/if}
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-wf-name" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Workflow Type Name</label>
					<input id="new-wf-name" bind:value={newWfName} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="order-processing" />
				</div>
				<div>
					<label for="new-wf-version" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Workflow Type Version</label>
					<input id="new-wf-version" bind:value={newWfVersion} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="1.0" />
				</div>
			</div>
			<div>
				<label for="new-wf-desc" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Workflow Type Description</label>
				<input id="new-wf-desc" bind:value={newWfDescription} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-wf-tasklist" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Workflow Default Task List</label>
					<input id="new-wf-tasklist" bind:value={newWfTaskList} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="new-wf-priority" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Workflow Default Task Priority</label>
					<input id="new-wf-priority" bind:value={newWfTaskPriority} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-wf-task-timeout" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Default Task Start-to-Close (s)</label>
					<input id="new-wf-task-timeout" bind:value={newWfTaskStartToCloseTimeout} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="NONE" />
				</div>
				<div>
					<label for="new-wf-exec-timeout" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Default Execution Start-to-Close (s)</label>
					<input id="new-wf-exec-timeout" bind:value={newWfExecutionStartToCloseTimeout} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-wf-childpolicy" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Default Child Policy</label>
					<select id="new-wf-childpolicy" bind:value={newWfChildPolicy} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="TERMINATE">TERMINATE</option>
						<option value="REQUEST_CANCEL">REQUEST_CANCEL</option>
						<option value="ABANDON">ABANDON</option>
					</select>
				</div>
				<div>
					<label for="new-wf-lambdarole" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Default Lambda Role</label>
					<input id="new-wf-lambdarole" bind:value={newWfLambdaRole} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="Optional" />
				</div>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => registerWfTypeModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitRegisterWfType} disabled={registeringWfType} class="px-4 py-2 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">{registeringWfType ? 'Registering…' : 'Register'}</button>
	{/snippet}
</Modal>

<Modal bind:this={wfTypeDetailModal} title="Workflow Type Details">
	{#snippet children()}
		{#if loadingWfTypeDetail}
			<p class="text-sm text-gray-500">Loading…</p>
		{:else if viewedWfType}
			<div class="space-y-2 text-sm">
				{#each [
					['Name', viewedWfType.workflowType?.name],
					['Version', viewedWfType.workflowType?.version],
					['Status', viewedWfType.status],
					['Description', viewedWfType.description],
					['Created', formatDate(viewedWfType.creationDate)],
					['Default Task List', wfTypeDetail?.configuration?.defaultTaskList?.name],
					['Default Task Priority', wfTypeDetail?.configuration?.defaultTaskPriority],
					['Default Task Start-to-Close', wfTypeDetail?.configuration?.defaultTaskStartToCloseTimeout],
					['Default Execution Start-to-Close', wfTypeDetail?.configuration?.defaultExecutionStartToCloseTimeout],
					['Default Child Policy', wfTypeDetail?.configuration?.defaultChildPolicy],
					['Default Lambda Role', wfTypeDetail?.configuration?.defaultLambdaRole]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => wfTypeDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Activity Type modals -->
<Modal bind:this={registerAtTypeModal} title="Register Activity Type">
	{#snippet children()}
		<div class="space-y-4">
			{#if registerAtTypeError}
				<div class="text-sm text-red-600 dark:text-red-400">{registerAtTypeError}</div>
			{/if}
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-at-name" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Activity Type Name</label>
					<input id="new-at-name" bind:value={newAtName} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="charge-card" />
				</div>
				<div>
					<label for="new-at-version" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Activity Type Version</label>
					<input id="new-at-version" bind:value={newAtVersion} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="1.0" />
				</div>
			</div>
			<div>
				<label for="new-at-desc" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Activity Type Description</label>
				<input id="new-at-desc" bind:value={newAtDescription} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-at-tasklist" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Activity Default Task List</label>
					<input id="new-at-tasklist" bind:value={newAtTaskList} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="new-at-priority" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Activity Default Task Priority</label>
					<input id="new-at-priority" bind:value={newAtTaskPriority} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-at-heartbeat" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Default Heartbeat Timeout (s)</label>
					<input id="new-at-heartbeat" bind:value={newAtHeartbeatTimeout} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="NONE" />
				</div>
				<div>
					<label for="new-at-start-close" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Default Start-to-Close (s)</label>
					<input id="new-at-start-close" bind:value={newAtStartToCloseTimeout} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-at-sched-start" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Default Schedule-to-Start (s)</label>
					<input id="new-at-sched-start" bind:value={newAtScheduleToStartTimeout} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="NONE" />
				</div>
				<div>
					<label for="new-at-sched-close" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Default Schedule-to-Close (s)</label>
					<input id="new-at-sched-close" bind:value={newAtScheduleToCloseTimeout} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="NONE" />
				</div>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => registerAtTypeModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitRegisterAtType} disabled={registeringAtType} class="px-4 py-2 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">{registeringAtType ? 'Registering…' : 'Register'}</button>
	{/snippet}
</Modal>

<Modal bind:this={atTypeDetailModal} title="Activity Type Details">
	{#snippet children()}
		{#if loadingAtTypeDetail}
			<p class="text-sm text-gray-500">Loading…</p>
		{:else if viewedAtType}
			<div class="space-y-2 text-sm">
				{#each [
					['Name', viewedAtType.activityType?.name],
					['Version', viewedAtType.activityType?.version],
					['Status', viewedAtType.status],
					['Description', viewedAtType.description],
					['Created', formatDate(viewedAtType.creationDate)],
					['Default Task List', atTypeDetail?.configuration?.defaultTaskList?.name],
					['Default Task Priority', atTypeDetail?.configuration?.defaultTaskPriority],
					['Default Heartbeat Timeout', atTypeDetail?.configuration?.defaultTaskHeartbeatTimeout],
					['Default Start-to-Close', atTypeDetail?.configuration?.defaultTaskStartToCloseTimeout],
					['Default Schedule-to-Start', atTypeDetail?.configuration?.defaultTaskScheduleToStartTimeout],
					['Default Schedule-to-Close', atTypeDetail?.configuration?.defaultTaskScheduleToCloseTimeout]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => atTypeDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Execution modals -->
<Modal bind:this={startExecModal} title="Start Workflow Execution">
	{#snippet children()}
		<div class="space-y-4">
			{#if startExecError}
				<div class="text-sm text-red-600 dark:text-red-400">{startExecError}</div>
			{/if}
			<div>
				<label for="new-exec-workflowid" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Workflow ID</label>
				<input id="new-exec-workflowid" bind:value={newExecWorkflowId} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="order-12345" />
			</div>
			<div>
				<label for="new-exec-wftype" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Workflow Type</label>
				{#if registeredWorkflowTypesForDomain.length === 0}
					<p class="text-sm text-gray-500">No registered workflow types in this domain. Register one first.</p>
				{:else}
					<select id="new-exec-wftype" bind:value={newExecWorkflowTypeKey} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						{#each registeredWorkflowTypesForDomain as w (`${w.workflowType?.name}:${w.workflowType?.version}`)}
							<option value={`${w.workflowType?.name}:${w.workflowType?.version}`}>{w.workflowType?.name} v{w.workflowType?.version}</option>
						{/each}
					</select>
				{/if}
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-exec-tasklist" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Task List</label>
					<input id="new-exec-tasklist" bind:value={newExecTaskList} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="new-exec-childpolicy" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Child Policy</label>
					<select id="new-exec-childpolicy" bind:value={newExecChildPolicy} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="TERMINATE">TERMINATE</option>
						<option value="REQUEST_CANCEL">REQUEST_CANCEL</option>
						<option value="ABANDON">ABANDON</option>
					</select>
				</div>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-exec-exec-timeout" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Execution Start-to-Close (s)</label>
					<input id="new-exec-exec-timeout" bind:value={newExecExecutionTimeout} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="new-exec-task-timeout" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Task Start-to-Close (s)</label>
					<input id="new-exec-task-timeout" bind:value={newExecTaskTimeout} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="NONE" />
				</div>
			</div>
			<div>
				<label for="new-exec-taglist" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Tags (comma-separated, max 5)</label>
				<input id="new-exec-taglist" bind:value={newExecTagList} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="priority:high, region:us" />
			</div>
			<div>
				<label for="new-exec-input" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Execution Input</label>
				<textarea id="new-exec-input" bind:value={newExecInput} rows="3" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => startExecModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitStartExec} disabled={startingExec} class="px-4 py-2 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">{startingExec ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<Modal bind:this={signalModal} title="Signal Workflow Execution">
	{#snippet children()}
		<div class="space-y-4">
			{#if signalError}
				<div class="text-sm text-red-600 dark:text-red-400">{signalError}</div>
			{/if}
			<p class="text-xs text-gray-500 font-mono">{signalTarget?.execution?.workflowId}</p>
			<div>
				<label for="signal-name" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Signal Name</label>
				<input id="signal-name" bind:value={signalName} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="approval-received" />
			</div>
			<div>
				<label for="signal-input" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Signal Input</label>
				<textarea id="signal-input" bind:value={signalInput} rows="3" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => signalModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitSignal} disabled={sendingSignal} class="px-4 py-2 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">{sendingSignal ? 'Sending…' : 'Send Signal'}</button>
	{/snippet}
</Modal>

<Modal bind:this={execDetailModal} title="Execution Details">
	{#snippet children()}
		{#if loadingExecDetail}
			<p class="text-sm text-gray-500">Loading…</p>
		{:else if viewedExec}
			<div class="space-y-4 text-sm max-h-[70vh] overflow-y-auto">
				{#if execDetailRunMismatch}
					<div role="alert" class="rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-3 py-2 text-xs text-amber-800 dark:text-amber-300">
						This backend keys execution state by workflow ID only, not run ID (see gopherstack-jsi8). The
						detail/history below belongs to the CURRENT run
						({execDetail?.executionInfo?.execution?.runId?.slice(0, 8)}…), which differs from this row's run
						({viewedExec.execution?.runId?.slice(0, 8)}…) -- likely because the workflow continued as new.
					</div>
				{/if}
				<div class="space-y-2">
					{#each [
						['Workflow ID', viewedExec.execution?.workflowId],
						['Run ID', viewedExec.execution?.runId],
						['Workflow Type', viewedExec.workflowType ? `${viewedExec.workflowType.name} v${viewedExec.workflowType.version}` : undefined],
						['Status', execDetail?.executionInfo?.executionStatus ?? viewedExec.executionStatus],
						['Close Status', execDetail?.executionInfo?.closeStatus ?? viewedExec.closeStatus],
						['Started', formatDate(viewedExec.startTimestamp)],
						['Closed', formatDate(viewedExec.closeTimestamp)],
						['Cancel Requested', execDetail?.executionInfo?.cancelRequested ? 'Yes' : 'No'],
						['Task List', execDetail?.executionConfiguration?.taskList?.name],
						['Child Policy', execDetail?.executionConfiguration?.childPolicy]
					] as [label, value] (label)}
						<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
							<span class="text-gray-500">{label}</span>
							<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
						</div>
					{/each}
				</div>
				{#if execDetail?.openCounts}
					<div>
						<h3 class="text-xs font-semibold uppercase text-gray-500 mb-2">Open Counts</h3>
						<p class="font-mono text-xs">
							activity:{execDetail.openCounts.openActivityTasks ?? 0}
							decision:{execDetail.openCounts.openDecisionTasks ?? 0}
							timers:{execDetail.openCounts.openTimers ?? 0}
							children:{execDetail.openCounts.openChildWorkflowExecutions ?? 0}
						</p>
					</div>
				{/if}
				<div>
					<h3 class="text-xs font-semibold uppercase text-gray-500 mb-2">History</h3>
					{#if historyEvents.length === 0}
						<p class="text-xs text-gray-500">No history events.</p>
					{:else}
						<div class="space-y-1">
							{#each historyEvents as event (event.eventId)}
								{@const payload = eventPayload(event)}
								<div class="rounded hover:bg-gray-50 dark:hover:bg-slate-700/50">
									<button type="button" onclick={() => toggleEvent(event.eventId)} class="flex w-full items-start gap-2 p-1.5 text-left">
										<span class="mt-0.5 min-w-7 rounded bg-gray-100 dark:bg-gray-700 px-1 py-0.5 text-center text-[10px] font-mono text-gray-500 dark:text-gray-400">{event.eventId}</span>
										<span class="flex-1 text-xs font-medium {eventTypeClass(event.eventType)}">{event.eventType}</span>
										<span class="text-[10px] text-gray-400">{formatDate(event.eventTimestamp)}</span>
										{#if payload}
											{#if expandedEventId === event.eventId}
												<ChevronDown class="w-3.5 h-3.5 text-gray-400" />
											{:else}
												<ChevronRight class="w-3.5 h-3.5 text-gray-400" />
											{/if}
										{/if}
									</button>
									{#if expandedEventId === event.eventId && payload}
										<pre class="mx-2 mb-2 overflow-auto rounded bg-gray-900 px-2 py-1.5 text-[10px] font-mono text-gray-100">{JSON.stringify(payload, null, 2)}</pre>
									{/if}
								</div>
							{/each}
						</div>
						<div class="mt-2">
							<LoadMore hasMore={!!historyNextToken} loading={loadingMoreHistory} onLoadMore={loadMoreHistory} />
						</div>
					{/if}
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => execDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Workflow}
		title="Simple Workflow Service"
		description="Register workflow/activity types and manage workflow executions"
		onRefresh={handleRefresh}
		color="violet"
	>
		{#snippet actions()}
			{#if activeTab === 'domains'}
				<button onclick={openRegisterDomainModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Register domain
				</button>
			{:else if activeTab === 'workflowTypes' && selectedDomain}
				<button onclick={openRegisterWfTypeModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Register workflow type
				</button>
			{:else if activeTab === 'activityTypes' && selectedDomain}
				<button onclick={openRegisterAtTypeModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Register activity type
				</button>
			{:else if activeTab === 'executions' && selectedDomain}
				<button onclick={openStartExecModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Start execution
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="violet" />
			<div class="flex flex-wrap items-center gap-2">
				{#if activeTab !== 'domains'}
					<select
						bind:value={selectedDomain}
						onchange={handleDomainChange}
						class="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm"
					>
						<option value="">— select domain —</option>
						{#each domains as d (d.name)}
							<option value={d.name}>{d.name}{d.status === 'DEPRECATED' ? ' (deprecated)' : ''}</option>
						{/each}
					</select>
				{/if}
				<SearchInput bind:value={searchQuery} />
			</div>
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'domains'}
				{#snippet domainNameCell(d: DomainInfo)}
					<div class="flex items-center gap-2">
						<Globe class="w-4 h-4 text-violet-500 shrink-0" />
						<span class="font-medium">{d.name}</span>
					</div>
				{/snippet}
				{#snippet domainStatusCell(d: DomainInfo)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(d.status, 'REGISTERED')}">{d.status ?? '—'}</span>
				{/snippet}
				{#snippet domainActionsCell(d: DomainInfo)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openDomainDetail(d)} title="View" aria-label="View domain {d.name}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
						{#if d.status === 'DEPRECATED'}
							<button onclick={() => handleUndeprecateDomain(d)} title="Undeprecate" aria-label="Undeprecate domain {d.name}" class="text-gray-400 hover:text-green-500"><RotateCcw class="w-4 h-4" /></button>
						{:else}
							<button onclick={() => handleDeprecateDomain(d)} title="Deprecate" aria-label="Deprecate domain {d.name}" class="text-gray-400 hover:text-amber-500"><Ban class="w-4 h-4" /></button>
						{/if}
					</div>
				{/snippet}
				{@const domainColumns = defineColumns<DomainInfo>([
					{ key: 'name', label: 'Name', render: domainNameCell },
					{ key: 'description', label: 'Description' },
					{ key: 'status', label: 'Status', render: domainStatusCell },
					{ key: 'actions', label: '', render: domainActionsCell }
				])}
				<DataTable
					rows={filteredDomains}
					rowKey={(d) => d.name ?? ''}
					columns={domainColumns}
					loading={tabLoader.isLoading('domains')}
					emptyMessage="No domains found"
				/>
				<p class="text-xs text-gray-500 dark:text-gray-400">
					Domains are registered and deprecated/undeprecated -- there is no delete for a domain in the real API.
				</p>
			{:else if activeTab === 'workflowTypes'}
				{#if !selectedDomain}
					<div class="py-8 text-center text-sm text-gray-500 dark:text-gray-400">Select a domain to view workflow types</div>
				{:else}
					{#snippet wfNameCell(w: WorkflowTypeInfo)}
						<span class="font-medium">{w.workflowType?.name}</span> <span class="text-xs text-gray-400">v{w.workflowType?.version}</span>
					{/snippet}
					{#snippet wfStatusCell(w: WorkflowTypeInfo)}
						<span class="text-xs px-2 py-1 rounded-full {statusClass(w.status, 'REGISTERED')}">{w.status ?? '—'}</span>
					{/snippet}
					{#snippet wfActionsCell(w: WorkflowTypeInfo)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openWfTypeDetail(w)} title="View" aria-label="View workflow type {w.workflowType?.name}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
							{#if w.status === 'DEPRECATED'}
								<button onclick={() => handleUndeprecateWfType(w)} title="Undeprecate" aria-label="Undeprecate workflow type {w.workflowType?.name}" class="text-gray-400 hover:text-green-500"><RotateCcw class="w-4 h-4" /></button>
							{:else}
								<button onclick={() => handleDeprecateWfType(w)} title="Deprecate" aria-label="Deprecate workflow type {w.workflowType?.name}" class="text-gray-400 hover:text-amber-500"><Ban class="w-4 h-4" /></button>
							{/if}
							<button onclick={() => handleDeleteWfType(w)} title="Delete" aria-label="Delete workflow type {w.workflowType?.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const wfColumns = defineColumns<WorkflowTypeInfo>([
						{ key: 'name', label: 'Name', render: wfNameCell },
						{ key: 'description', label: 'Description' },
						{ key: 'status', label: 'Status', render: wfStatusCell },
						{ key: 'actions', label: '', render: wfActionsCell }
					])}
					<DataTable
						rows={filteredWorkflowTypes}
						rowKey={(w) => `${w.workflowType?.name}:${w.workflowType?.version}`}
						columns={wfColumns}
						loading={tabLoader.isLoading('workflowTypes')}
						emptyMessage="No workflow types registered in this domain"
					/>
				{/if}
			{:else if activeTab === 'activityTypes'}
				{#if !selectedDomain}
					<div class="py-8 text-center text-sm text-gray-500 dark:text-gray-400">Select a domain to view activity types</div>
				{:else}
					{#snippet atNameCell(a: ActivityTypeInfo)}
						<span class="font-medium">{a.activityType?.name}</span> <span class="text-xs text-gray-400">v{a.activityType?.version}</span>
					{/snippet}
					{#snippet atStatusCell(a: ActivityTypeInfo)}
						<span class="text-xs px-2 py-1 rounded-full {statusClass(a.status, 'REGISTERED')}">{a.status ?? '—'}</span>
					{/snippet}
					{#snippet atActionsCell(a: ActivityTypeInfo)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openAtTypeDetail(a)} title="View" aria-label="View activity type {a.activityType?.name}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
							{#if a.status === 'DEPRECATED'}
								<button onclick={() => handleUndeprecateAtType(a)} title="Undeprecate" aria-label="Undeprecate activity type {a.activityType?.name}" class="text-gray-400 hover:text-green-500"><RotateCcw class="w-4 h-4" /></button>
							{:else}
								<button onclick={() => handleDeprecateAtType(a)} title="Deprecate" aria-label="Deprecate activity type {a.activityType?.name}" class="text-gray-400 hover:text-amber-500"><Ban class="w-4 h-4" /></button>
							{/if}
							<button onclick={() => handleDeleteAtType(a)} title="Delete" aria-label="Delete activity type {a.activityType?.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const atColumns = defineColumns<ActivityTypeInfo>([
						{ key: 'name', label: 'Name', render: atNameCell },
						{ key: 'description', label: 'Description' },
						{ key: 'status', label: 'Status', render: atStatusCell },
						{ key: 'actions', label: '', render: atActionsCell }
					])}
					<DataTable
						rows={filteredActivityTypes}
						rowKey={(a) => `${a.activityType?.name}:${a.activityType?.version}`}
						columns={atColumns}
						loading={tabLoader.isLoading('activityTypes')}
						emptyMessage="No activity types registered in this domain"
					/>
				{/if}
			{:else if activeTab === 'executions'}
				{#if !selectedDomain}
					<div class="py-8 text-center text-sm text-gray-500 dark:text-gray-400">Select a domain to view executions</div>
				{:else}
					{#snippet execIdCell(e: WorkflowExecutionInfo)}
						<div>
							<p class="font-mono text-xs">{e.execution?.workflowId}</p>
							<p class="text-[10px] text-gray-400">run: {e.execution?.runId?.slice(0, 8)}…</p>
						</div>
					{/snippet}
					{#snippet execTypeCell(e: WorkflowExecutionInfo)}
						{e.workflowType ? `${e.workflowType.name} v${e.workflowType.version}` : '—'}
					{/snippet}
					{#snippet execStartedCell(e: WorkflowExecutionInfo)}
						{formatDate(e.startTimestamp)}
					{/snippet}
					{#snippet openStatusCell()}
						<span class="text-xs px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">RUNNING</span>
					{/snippet}
					{#snippet openActionsCell(e: WorkflowExecutionInfo)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openExecDetail(e)} title="View" aria-label="View execution {e.execution?.workflowId}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
							<button onclick={() => openSignalModal(e)} title="Signal" aria-label="Signal execution {e.execution?.workflowId}" class="text-gray-400 hover:text-amber-500"><Bell class="w-4 h-4" /></button>
							<button onclick={() => handleCancelExecution(e)} title="Request Cancel" aria-label="Request cancel for {e.execution?.workflowId}" class="text-gray-400 hover:text-orange-500"><XCircle class="w-4 h-4" /></button>
							<button onclick={() => handleTerminateExecution(e)} title="Terminate" aria-label="Terminate execution {e.execution?.workflowId}" class="text-gray-400 hover:text-red-500"><Square class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const openColumns = defineColumns<WorkflowExecutionInfo>([
						{ key: 'workflowId', label: 'Execution', render: execIdCell },
						{ key: 'workflowType', label: 'Workflow Type', render: execTypeCell },
						{ key: 'status', label: 'Status', render: openStatusCell },
						{ key: 'startTimestamp', label: 'Started', render: execStartedCell },
						{ key: 'actions', label: '', render: openActionsCell }
					])}
					<div>
						<h3 class="mb-2 flex items-center gap-2 text-sm font-semibold text-gray-700 dark:text-gray-300">
							<Play class="w-4 h-4 text-green-500" /> Open Executions
							<span class="rounded-full bg-green-100 dark:bg-green-900/30 px-2 py-0.5 text-xs text-green-700 dark:text-green-400">{filteredOpenExecs.length}</span>
						</h3>
						<DataTable
							rows={filteredOpenExecs}
							rowKey={(e) => `${e.execution?.workflowId}:${e.execution?.runId}`}
							columns={openColumns}
							loading={tabLoader.isLoading('executions')}
							emptyMessage="No open executions"
						/>
						<div class="mt-2"><LoadMore hasMore={!!openExecsNextToken} loading={loadingMoreOpen} onLoadMore={loadMoreOpen} /></div>
					</div>

					{#snippet closedStatusCell(e: WorkflowExecutionInfo)}
						<span class="text-xs px-2 py-1 rounded-full {closeStatusClass(e.closeStatus)}">{e.closeStatus ?? '—'}</span>
					{/snippet}
					{#snippet closedActionsCell(e: WorkflowExecutionInfo)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openExecDetail(e)} title="View" aria-label="View execution {e.execution?.workflowId}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const closedColumns = defineColumns<WorkflowExecutionInfo>([
						{ key: 'workflowId', label: 'Execution', render: execIdCell },
						{ key: 'workflowType', label: 'Workflow Type', render: execTypeCell },
						{ key: 'closeStatus', label: 'Close Status', render: closedStatusCell },
						{ key: 'startTimestamp', label: 'Started', render: execStartedCell },
						{ key: 'actions', label: '', render: closedActionsCell }
					])}
					<div>
						<h3 class="mb-2 flex items-center gap-2 text-sm font-semibold text-gray-700 dark:text-gray-300">
							Closed Executions
							<span class="rounded-full bg-gray-100 dark:bg-gray-700 px-2 py-0.5 text-xs text-gray-600 dark:text-gray-300">{filteredClosedExecs.length}</span>
						</h3>
						<p class="mb-2 text-xs text-gray-500 dark:text-gray-400">
							If a closed execution was later superseded by a ContinueAsNew run under the same workflow ID, View here
							shows the CURRENT run's data (this backend cannot retrieve a superseded run independently -- see
							gopherstack-jsi8).
						</p>
						<DataTable
							rows={filteredClosedExecs}
							rowKey={(e) => `${e.execution?.workflowId}:${e.execution?.runId}`}
							columns={closedColumns}
							loading={tabLoader.isLoading('executions')}
							emptyMessage="No closed executions"
						/>
						<div class="mt-2"><LoadMore hasMore={!!closedExecsNextToken} loading={loadingMoreClosed} onLoadMore={loadMoreClosed} /></div>
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
