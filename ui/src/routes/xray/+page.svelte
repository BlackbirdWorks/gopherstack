<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getXRayClient } from '$lib/aws-client';
	import {
		GetTraceSummariesCommand,
		BatchGetTracesCommand,
		GetServiceGraphCommand,
		GetGroupsCommand,
		CreateGroupCommand,
		UpdateGroupCommand,
		DeleteGroupCommand,
		GetSamplingRulesCommand,
		CreateSamplingRuleCommand,
		UpdateSamplingRuleCommand,
		DeleteSamplingRuleCommand,
		ListResourcePoliciesCommand,
		PutResourcePolicyCommand,
		DeleteResourcePolicyCommand,
		GetEncryptionConfigCommand,
		PutEncryptionConfigCommand,
		type TraceSummary,
		type Trace,
		type Service,
		type GroupSummary,
		type SamplingRuleRecord,
		type ResourcePolicy,
		type EncryptionConfig
	} from '@aws-sdk/client-xray';
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
		Activity,
		RefreshCw,
		AlertCircle,
		Layers,
		Share2,
		X,
		ChevronRight,
		Plus,
		Trash2,
		Eye,
		Pencil,
		Lock
	} from 'lucide-svelte';

	const client = regionalClient(getXRayClient);

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

	// "key=value,key2=value2" <-> {key: value, key2: value2}. Used for
	// SamplingRule.Attributes, a plain string map on the wire.
	function parseKeyValueList(s: string): Record<string, string> | undefined {
		const entries = s
			.split(',')
			.map((p) => p.trim())
			.filter((p) => p.length > 0)
			.map((p): [string, string] => {
				const idx = p.indexOf('=');
				return idx === -1 ? [p, ''] : [p.slice(0, idx), p.slice(idx + 1)];
			});
		return entries.length > 0 ? Object.fromEntries(entries) : undefined;
	}

	function formatKeyValueList(m: Record<string, string> | undefined): string {
		return Object.entries(m ?? {})
			.map(([k, v]) => `${k}=${v}`)
			.join(', ');
	}

	type TabId = 'traces' | 'serviceGraph' | 'groups' | 'samplingRules' | 'resourcePolicies' | 'encryptionConfig';

	const tabs: TabDef[] = [
		{ id: 'traces', label: 'Traces' },
		{ id: 'serviceGraph', label: 'Service Graph' },
		{ id: 'groups', label: 'Groups' },
		{ id: 'samplingRules', label: 'Sampling Rules' },
		{ id: 'resourcePolicies', label: 'Resource Policies' },
		{ id: 'encryptionConfig', label: 'Encryption Config' }
	];

	let activeTab = $state<TabId>('traces');
	let searchQuery = $state('');

	// ==================== Traces: time-window query, not a CRUD resource ====================
	//
	// GetTraceSummaries/BatchGetTraces retrieve trace data over a StartTime/EndTime
	// window -- there is no CreateTrace/DeleteTrace in real X-Ray (confirmed absent
	// from both the installed SDK's command list and services/xray's
	// GetSupportedOperations()); trace segments only arrive via PutTraceSegments from
	// an instrumented application. So this tab is a time-range search, not a table
	// with a Create button.

	let traceSummaries = $state<TraceSummary[]>([]);
	let tracesNextToken = $state<string | undefined>();
	let loadingMoreTraces = $state(false);
	let errorFilter = $state<'all' | 'error' | 'fault' | 'throttle'>('all');
	let startTime = $state(new Date(Date.now() - 3600000).toISOString().slice(0, 16));
	let endTime = $state(new Date().toISOString().slice(0, 16));

	async function fetchTraces(reset: boolean): Promise<void> {
		const res = await client().send(
			new GetTraceSummariesCommand({
				StartTime: new Date(startTime),
				EndTime: new Date(endTime),
				Sampling: false,
				NextToken: reset ? undefined : tracesNextToken
			})
		);
		traceSummaries = reset ? (res.TraceSummaries ?? []) : [...traceSummaries, ...(res.TraceSummaries ?? [])];
		tracesNextToken = res.NextToken;
	}

	async function loadMoreTraces(): Promise<void> {
		loadingMoreTraces = true;
		try {
			await fetchTraces(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTraces = false;
		}
	}

	const filteredTraces = $derived(
		traceSummaries.filter((t) => {
			const text = matches(searchQuery, t.Id, t.Http?.HttpURL);
			const filterMatch =
				errorFilter === 'all' ||
				(errorFilter === 'error' && t.HasError) ||
				(errorFilter === 'fault' && t.HasFault) ||
				(errorFilter === 'throttle' && t.HasThrottle);
			return text && filterMatch;
		})
	);

	function statusIndicator(trace: TraceSummary) {
		if (trace.HasFault) return { color: 'text-red-500', label: 'Fault' };
		if (trace.HasError) return { color: 'text-orange-500', label: 'Error' };
		if (trace.HasThrottle) return { color: 'text-yellow-500', label: 'Throttle' };
		return { color: 'text-green-500', label: 'OK' };
	}

	// Trace detail: a segment-timeline visualization, not a generic field list, so
	// it uses its own overlay (max-w-3xl) rather than the shared Modal primitive
	// (which is fixed at max-w-md across every other dialog on this page).
	type SegmentRow = {
		name: string;
		start: number;
		end: number;
		depth: number;
		hasError: boolean;
		hasFault: boolean;
		annotations: [string, string][];
		metadata: [string, string][];
	};
	let expandedSegment = $state<number | null>(null);
	let selectedTrace = $state<Trace | null>(null);
	let traceDetailLoading = $state(false);
	let segmentRows = $state<SegmentRow[]>([]);
	let traceStart = $state(0);
	let traceEnd = $state(0);

	function pairsFrom(obj: unknown): [string, string][] {
		if (!obj || typeof obj !== 'object') return [];
		return Object.entries(obj as Record<string, unknown>).map(([k, v]) => [
			k,
			typeof v === 'object' ? JSON.stringify(v) : String(v)
		]);
	}

	function flattenSegments(doc: Record<string, unknown>, depth: number, rows: SegmentRow[]) {
		const start = typeof doc.start_time === 'number' ? doc.start_time : 0;
		const end = typeof doc.end_time === 'number' ? doc.end_time : start;
		// X-Ray metadata is namespaced (e.g. { default: {...} }); flatten one level.
		const metaPairs: [string, string][] = [];
		if (doc.metadata && typeof doc.metadata === 'object') {
			for (const [ns, val] of Object.entries(doc.metadata as Record<string, unknown>)) {
				for (const [k, v] of pairsFrom(val)) metaPairs.push([`${ns}.${k}`, v]);
			}
		}
		rows.push({
			name: typeof doc.name === 'string' ? doc.name : '(unnamed)',
			start,
			end,
			depth,
			hasError: doc.error === true,
			hasFault: doc.fault === true,
			annotations: pairsFrom(doc.annotations),
			metadata: metaPairs
		});
		const subs = Array.isArray(doc.subsegments) ? (doc.subsegments as Record<string, unknown>[]) : [];
		for (const sub of subs) flattenSegments(sub, depth + 1, rows);
	}

	async function openTraceDetail(traceId: string | undefined) {
		if (!traceId) return;
		traceDetailLoading = true;
		segmentRows = [];
		selectedTrace = null;
		try {
			const res = await client().send(new BatchGetTracesCommand({ TraceIds: [traceId] }));
			const trace = (res.Traces ?? [])[0] ?? null;
			selectedTrace = trace;
			const rows: SegmentRow[] = [];
			for (const seg of trace?.Segments ?? []) {
				if (!seg.Document) continue;
				try {
					flattenSegments(JSON.parse(seg.Document) as Record<string, unknown>, 0, rows);
				} catch {
					// skip unparseable segment document
				}
			}
			rows.sort((a, b) => a.start - b.start);
			segmentRows = rows;
			traceStart = rows.length > 0 ? Math.min(...rows.map((r) => r.start)) : 0;
			traceEnd = rows.length > 0 ? Math.max(...rows.map((r) => r.end)) : 0;
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			traceDetailLoading = false;
		}
	}

	function closeTraceDetail() {
		selectedTrace = null;
		segmentRows = [];
		expandedSegment = null;
	}

	function barStyle(row: SegmentRow): string {
		const span = traceEnd - traceStart || 1;
		const left = ((row.start - traceStart) / span) * 100;
		const width = Math.max(((row.end - row.start) / span) * 100, 0.5);
		return `left:${left}%;width:${width}%`;
	}

	// Stats (derived from the currently loaded trace page)
	const totalTraces = $derived(traceSummaries.length);
	const faultTraces = $derived(traceSummaries.filter((t) => t.HasFault).length);
	const errorTraces = $derived(traceSummaries.filter((t) => t.HasError).length);
	const avgDuration = $derived(
		traceSummaries.length > 0
			? (traceSummaries.reduce((sum, t) => sum + (t.Duration ?? 0), 0) / traceSummaries.length).toFixed(3)
			: '—'
	);

	// ==================== Service Graph: also a time-window query, not a resource ====================

	let serviceGraphNodes = $state<Service[]>([]);
	let sgNextToken = $state<string | undefined>();
	let loadingMoreSG = $state(false);
	let serviceGraphStartTime = $state(new Date(Date.now() - 3600000).toISOString().slice(0, 16));
	let serviceGraphEndTime = $state(new Date().toISOString().slice(0, 16));

	async function fetchServiceGraph(reset: boolean): Promise<void> {
		const res = await client().send(
			new GetServiceGraphCommand({
				StartTime: new Date(serviceGraphStartTime),
				EndTime: new Date(serviceGraphEndTime),
				NextToken: reset ? undefined : sgNextToken
			})
		);
		serviceGraphNodes = reset ? (res.Services ?? []) : [...serviceGraphNodes, ...(res.Services ?? [])];
		sgNextToken = res.NextToken;
	}

	async function loadMoreServiceGraph(): Promise<void> {
		loadingMoreSG = true;
		try {
			await fetchServiceGraph(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSG = false;
		}
	}

	// ==================== Groups: genuine CRUD ====================
	//
	// GetGroups' GroupSummary already carries every field Group itself has
	// (GroupName/GroupARN/FilterExpression/InsightsConfiguration -- confirmed
	// identical shape in the installed SDK model), so the detail/update forms below
	// read straight from the list row instead of issuing a separate GetGroup call.

	let groups = $state<GroupSummary[]>([]);
	let groupsNextToken = $state<string | undefined>();
	let loadingMoreGroups = $state(false);

	async function fetchGroups(reset: boolean): Promise<void> {
		const res = await client().send(new GetGroupsCommand({ NextToken: reset ? undefined : groupsNextToken }));
		groups = reset ? (res.Groups ?? []) : [...groups, ...(res.Groups ?? [])];
		groupsNextToken = res.NextToken;
	}

	async function loadMoreGroups(): Promise<void> {
		loadingMoreGroups = true;
		try {
			await fetchGroups(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGroups = false;
		}
	}

	const filteredGroups = $derived(
		groups.filter((g) => matches(searchQuery, g.GroupName, g.GroupARN, g.FilterExpression))
	);

	let groupFormModal = $state<Modal | null>(null);
	let groupFormMode = $state<'create' | 'update'>('create');
	let groupFormSaving = $state(false);
	let groupFormError = $state<string | null>(null);
	let editingGroupName = $state<string | null>(null);
	let gfName = $state('');
	let gfFilterExpression = $state('');
	let gfInsightsEnabled = $state(false);
	let gfNotificationsEnabled = $state(false);

	function openCreateGroupModal(): void {
		groupFormMode = 'create';
		editingGroupName = null;
		groupFormError = null;
		gfName = '';
		gfFilterExpression = '';
		gfInsightsEnabled = false;
		gfNotificationsEnabled = false;
		groupFormModal?.open();
	}

	function openUpdateGroupModal(g: GroupSummary): void {
		groupFormMode = 'update';
		editingGroupName = g.GroupName ?? null;
		groupFormError = null;
		gfName = g.GroupName ?? '';
		gfFilterExpression = g.FilterExpression ?? '';
		gfInsightsEnabled = g.InsightsConfiguration?.InsightsEnabled ?? false;
		gfNotificationsEnabled = g.InsightsConfiguration?.NotificationsEnabled ?? false;
		groupFormModal?.open();
	}

	async function submitGroupForm(): Promise<void> {
		if (groupFormMode === 'create' && !gfName.trim()) {
			groupFormError = 'Group name is required.';
			return;
		}
		if (gfNotificationsEnabled && !gfInsightsEnabled) {
			groupFormError = 'Notifications require insights to be enabled.';
			return;
		}
		groupFormSaving = true;
		groupFormError = null;
		try {
			if (groupFormMode === 'create') {
				await client().send(
					new CreateGroupCommand({
						GroupName: gfName.trim(),
						FilterExpression: gfFilterExpression || undefined,
						InsightsConfiguration: {
							InsightsEnabled: gfInsightsEnabled,
							NotificationsEnabled: gfNotificationsEnabled
						}
					})
				);
				toast.success('Group created');
			} else if (editingGroupName) {
				await client().send(
					new UpdateGroupCommand({
						GroupName: editingGroupName,
						FilterExpression: gfFilterExpression,
						InsightsConfiguration: {
							InsightsEnabled: gfInsightsEnabled,
							NotificationsEnabled: gfNotificationsEnabled
						}
					})
				);
				toast.success('Group updated');
			}
			groupFormModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			const msg = describeError(e);
			groupFormError = msg;
			toast.error(msg);
		} finally {
			groupFormSaving = false;
		}
	}

	async function handleDeleteGroup(g: GroupSummary): Promise<void> {
		if (!g.GroupName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete group',
			message: `Delete group ${g.GroupName}? Trace filtering and insights configured for it are removed.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteGroupCommand({ GroupName: g.GroupName }));
			toast.success('Group deleted');
			await tabLoader.refresh('groups');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let groupDetailModal = $state<Modal | null>(null);
	let viewedGroup = $state<GroupSummary | null>(null);

	function openGroupDetail(g: GroupSummary): void {
		viewedGroup = g;
		groupDetailModal?.open();
	}

	// ==================== Sampling Rules: genuine CRUD ====================
	//
	// GetSamplingRulesResult's SamplingRuleRecord already nests the full SamplingRule
	// (every field, including Attributes/SamplingRateBoost) plus CreatedAt/ModifiedAt
	// -- there is no separate GetSamplingRule op in real X-Ray (confirmed absent from
	// the SDK's command list), so list rows double as detail data.
	//
	// WIRE-SHAPE GAP found while building this form: the real SDK's SamplingRuleUpdate
	// type (used by UpdateSamplingRule) has an Attributes field (confirmed in the
	// installed model), but services/xray/handler_sampling_rules.go's
	// samplingRuleUpdateInput struct has no Attributes field at all -- so a real
	// client's UpdateSamplingRule Attributes value is silently dropped by
	// json.Unmarshal (the target struct simply has nowhere to put it), even though
	// Attributes is correctly stored and returned on CreateSamplingRule. The update
	// form below deliberately shows Attributes as read-only for this reason, instead
	// of building an editable field the backend would silently ignore.

	let samplingRules = $state<SamplingRuleRecord[]>([]);
	let samplingRulesNextToken = $state<string | undefined>();
	let loadingMoreSamplingRules = $state(false);

	async function fetchSamplingRules(reset: boolean): Promise<void> {
		const res = await client().send(
			new GetSamplingRulesCommand({ NextToken: reset ? undefined : samplingRulesNextToken })
		);
		samplingRules = reset
			? (res.SamplingRuleRecords ?? [])
			: [...samplingRules, ...(res.SamplingRuleRecords ?? [])];
		samplingRulesNextToken = res.NextToken;
	}

	async function loadMoreSamplingRules(): Promise<void> {
		loadingMoreSamplingRules = true;
		try {
			await fetchSamplingRules(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSamplingRules = false;
		}
	}

	const filteredSamplingRules = $derived(
		samplingRules.filter((r) =>
			matches(searchQuery, r.SamplingRule?.RuleName, r.SamplingRule?.ServiceName, r.SamplingRule?.ResourceARN)
		)
	);

	let srFormModal = $state<Modal | null>(null);
	let srFormMode = $state<'create' | 'update'>('create');
	let srFormSaving = $state(false);
	let srFormError = $state<string | null>(null);
	let editingRuleName = $state<string | null>(null);
	let srRuleName = $state('');
	let srResourceARN = $state('*');
	let srServiceName = $state('*');
	let srServiceType = $state('*');
	let srHost = $state('*');
	let srHTTPMethod = $state('*');
	let srURLPath = $state('*');
	let srPriority = $state(1000);
	let srFixedRate = $state(0.05);
	let srReservoirSize = $state(1);
	let srAttributes = $state('');
	let srBoostEnabled = $state(false);
	let srBoostMaxRate = $state(0);
	let srBoostCooldown = $state(0);

	function openCreateSamplingRuleModal(): void {
		srFormMode = 'create';
		editingRuleName = null;
		srFormError = null;
		srRuleName = '';
		srResourceARN = '*';
		srServiceName = '*';
		srServiceType = '*';
		srHost = '*';
		srHTTPMethod = '*';
		srURLPath = '*';
		srPriority = 1000;
		srFixedRate = 0.05;
		srReservoirSize = 1;
		srAttributes = '';
		srBoostEnabled = false;
		srBoostMaxRate = 0;
		srBoostCooldown = 0;
		srFormModal?.open();
	}

	function openUpdateSamplingRuleModal(r: SamplingRuleRecord): void {
		const rule = r.SamplingRule;
		if (!rule?.RuleName) return;
		srFormMode = 'update';
		editingRuleName = rule.RuleName;
		srFormError = null;
		srRuleName = rule.RuleName;
		srResourceARN = rule.ResourceARN ?? '*';
		srServiceName = rule.ServiceName ?? '*';
		srServiceType = rule.ServiceType ?? '*';
		srHost = rule.Host ?? '*';
		srHTTPMethod = rule.HTTPMethod ?? '*';
		srURLPath = rule.URLPath ?? '*';
		srPriority = rule.Priority ?? 1000;
		srFixedRate = rule.FixedRate ?? 0.05;
		srReservoirSize = rule.ReservoirSize ?? 1;
		srAttributes = formatKeyValueList(rule.Attributes);
		srBoostEnabled = !!rule.SamplingRateBoost;
		srBoostMaxRate = rule.SamplingRateBoost?.MaxRate ?? 0;
		srBoostCooldown = rule.SamplingRateBoost?.CooldownWindowMinutes ?? 0;
		srFormModal?.open();
	}

	type SamplingRuleBoost = NonNullable<SamplingRuleRecord['SamplingRule']>['SamplingRateBoost'];

	function validateSamplingRuleForm(): string | null {
		if (srFormMode === 'create' && !srRuleName.trim()) {
			return 'Rule name is required.';
		}
		if (srPriority < 1 || srPriority > 9999) {
			return 'Priority must be between 1 and 9999.';
		}
		if (srFixedRate < 0 || srFixedRate > 1) {
			return 'Fixed rate must be between 0.0 and 1.0.';
		}
		return null;
	}

	async function createSamplingRuleFromForm(boost: SamplingRuleBoost): Promise<void> {
		await client().send(
			new CreateSamplingRuleCommand({
				SamplingRule: {
					RuleName: srRuleName.trim(),
					ResourceARN: srResourceARN.trim() || '*',
					Priority: srPriority,
					FixedRate: srFixedRate,
					ReservoirSize: srReservoirSize,
					ServiceName: srServiceName.trim() || '*',
					ServiceType: srServiceType.trim() || '*',
					Host: srHost.trim() || '*',
					HTTPMethod: srHTTPMethod.trim() || '*',
					URLPath: srURLPath.trim() || '*',
					Version: 1,
					Attributes: parseKeyValueList(srAttributes),
					SamplingRateBoost: boost
				}
			})
		);
		toast.success('Sampling rule created');
	}

	async function updateSamplingRuleFromForm(ruleName: string, boost: SamplingRuleBoost): Promise<void> {
		await client().send(
			new UpdateSamplingRuleCommand({
				SamplingRuleUpdate: {
					RuleName: ruleName,
					ResourceARN: srResourceARN.trim() || '*',
					Priority: srPriority,
					FixedRate: srFixedRate,
					ReservoirSize: srReservoirSize,
					ServiceName: srServiceName.trim() || '*',
					ServiceType: srServiceType.trim() || '*',
					Host: srHost.trim() || '*',
					HTTPMethod: srHTTPMethod.trim() || '*',
					URLPath: srURLPath.trim() || '*',
					SamplingRateBoost: boost
				}
			})
		);
		toast.success('Sampling rule updated');
	}

	async function submitSamplingRuleForm(): Promise<void> {
		const validationError = validateSamplingRuleForm();
		if (validationError) {
			srFormError = validationError;
			return;
		}
		srFormSaving = true;
		srFormError = null;
		const boost = srBoostEnabled ? { MaxRate: srBoostMaxRate, CooldownWindowMinutes: srBoostCooldown } : undefined;
		try {
			if (srFormMode === 'create') {
				await createSamplingRuleFromForm(boost);
			} else if (editingRuleName) {
				await updateSamplingRuleFromForm(editingRuleName, boost);
			}
			srFormModal?.close();
			await tabLoader.refresh('samplingRules');
		} catch (e) {
			const msg = describeError(e);
			srFormError = msg;
			toast.error(msg);
		} finally {
			srFormSaving = false;
		}
	}

	async function handleDeleteSamplingRule(r: SamplingRuleRecord): Promise<void> {
		const name = r.SamplingRule?.RuleName;
		if (!name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete sampling rule',
			message: `Delete sampling rule ${name}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteSamplingRuleCommand({ RuleName: name }));
			toast.success('Sampling rule deleted');
			await tabLoader.refresh('samplingRules');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let srDetailModal = $state<Modal | null>(null);
	let viewedSamplingRule = $state<SamplingRuleRecord | null>(null);

	function openSamplingRuleDetail(r: SamplingRuleRecord): void {
		viewedSamplingRule = r;
		srDetailModal?.open();
	}

	// ==================== Resource Policies ====================
	//
	// Real X-Ray has a single PutResourcePolicy op that both creates and updates a
	// policy (identified by PolicyName) -- there is no separate CreateResourcePolicy,
	// confirmed absent from the installed SDK's command list. This form uses the one
	// real verb for both modes rather than inventing a Create op that doesn't exist.

	let resourcePolicies = $state<ResourcePolicy[]>([]);
	let rpNextToken = $state<string | undefined>();
	let loadingMoreRP = $state(false);

	async function fetchResourcePolicies(reset: boolean): Promise<void> {
		const res = await client().send(
			new ListResourcePoliciesCommand({ NextToken: reset ? undefined : rpNextToken })
		);
		resourcePolicies = reset
			? (res.ResourcePolicies ?? [])
			: [...resourcePolicies, ...(res.ResourcePolicies ?? [])];
		rpNextToken = res.NextToken;
	}

	async function loadMoreRP(): Promise<void> {
		loadingMoreRP = true;
		try {
			await fetchResourcePolicies(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreRP = false;
		}
	}

	const filteredResourcePolicies = $derived(
		resourcePolicies.filter((p) => matches(searchQuery, p.PolicyName, p.PolicyDocument))
	);

	const DEFAULT_POLICY_DOCUMENT = '{\n  "Version": "2012-10-17",\n  "Statement": []\n}';

	let rpFormModal = $state<Modal | null>(null);
	let rpFormMode = $state<'create' | 'update'>('create');
	let rpFormSaving = $state(false);
	let rpFormError = $state<string | null>(null);
	let rpName = $state('');
	let rpDocument = $state('');
	let rpRevisionId = $state('');
	let rpBypassLockout = $state(false);

	function openCreateRP(): void {
		rpFormMode = 'create';
		rpFormError = null;
		rpName = '';
		rpDocument = DEFAULT_POLICY_DOCUMENT;
		rpRevisionId = '';
		rpBypassLockout = false;
		rpFormModal?.open();
	}

	function openUpdateRP(p: ResourcePolicy): void {
		rpFormMode = 'update';
		rpFormError = null;
		rpName = p.PolicyName ?? '';
		rpDocument = p.PolicyDocument ?? '';
		rpRevisionId = p.PolicyRevisionId ?? '';
		rpBypassLockout = false;
		rpFormModal?.open();
	}

	async function submitRPForm(): Promise<void> {
		if (!rpName.trim()) {
			rpFormError = 'Policy name is required.';
			return;
		}
		if (!rpDocument.trim()) {
			rpFormError = 'Policy document is required.';
			return;
		}
		try {
			JSON.parse(rpDocument);
		} catch {
			rpFormError = 'Policy document must be valid JSON.';
			return;
		}
		rpFormSaving = true;
		rpFormError = null;
		try {
			await client().send(
				new PutResourcePolicyCommand({
					PolicyName: rpName.trim(),
					PolicyDocument: rpDocument,
					PolicyRevisionId: rpFormMode === 'update' ? rpRevisionId || undefined : undefined,
					BypassPolicyLockoutCheck: rpBypassLockout
				})
			);
			toast.success(rpFormMode === 'create' ? 'Resource policy created' : 'Resource policy updated');
			rpFormModal?.close();
			await tabLoader.refresh('resourcePolicies');
		} catch (e) {
			const msg = describeError(e);
			rpFormError = msg;
			toast.error(msg);
		} finally {
			rpFormSaving = false;
		}
	}

	async function handleDeleteRP(p: ResourcePolicy): Promise<void> {
		if (!p.PolicyName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete resource policy',
			message: `Delete resource policy ${p.PolicyName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteResourcePolicyCommand({ PolicyName: p.PolicyName, PolicyRevisionId: p.PolicyRevisionId })
			);
			toast.success('Resource policy deleted');
			await tabLoader.refresh('resourcePolicies');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let rpDetailModal = $state<Modal | null>(null);
	let viewedRP = $state<ResourcePolicy | null>(null);

	function openRPDetail(p: ResourcePolicy): void {
		viewedRP = p;
		rpDetailModal?.open();
	}

	// ==================== Encryption Config: account-level singleton ====================
	//
	// Same shape as FIS's safety lever: GetEncryptionConfig/PutEncryptionConfig act on
	// exactly one account-level record, not a collection -- no List/Create/Delete op
	// exists (confirmed absent from the SDK's command list), so this tab is a
	// single-record panel (current value + an update form), not a table.

	let encryptionConfig = $state<EncryptionConfig | null>(null);
	let ecFormType = $state<'NONE' | 'KMS'>('NONE');
	let ecFormKeyId = $state('');
	let ecSaving = $state(false);

	async function fetchEncryptionConfig(): Promise<void> {
		const res = await client().send(new GetEncryptionConfigCommand({}));
		encryptionConfig = res.EncryptionConfig ?? null;
		ecFormType = (encryptionConfig?.Type as typeof ecFormType) ?? 'NONE';
		ecFormKeyId = encryptionConfig?.KeyId ?? '';
	}

	function handleEcTypeChange(): void {
		if (ecFormType === 'NONE') ecFormKeyId = '';
	}

	async function submitEncryptionConfig(): Promise<void> {
		if (ecFormType === 'KMS' && !ecFormKeyId.trim()) {
			toast.error('A KMS key ID is required when Type is KMS.');
			return;
		}
		ecSaving = true;
		try {
			const res = await client().send(
				new PutEncryptionConfigCommand({
					Type: ecFormType,
					KeyId: ecFormType === 'KMS' ? ecFormKeyId.trim() : undefined
				})
			);
			encryptionConfig = res.EncryptionConfig ?? null;
			toast.success('Encryption configuration updated');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			ecSaving = false;
		}
	}

	// ==================== Tab loader ====================

	const tabLoader = createTabLoader<TabId>({
		traces: () => fetchTraces(true).catch(rethrowDescribed),
		serviceGraph: () => fetchServiceGraph(true).catch(rethrowDescribed),
		groups: () => fetchGroups(true).catch(rethrowDescribed),
		samplingRules: () => fetchSamplingRules(true).catch(rethrowDescribed),
		resourcePolicies: () => fetchResourcePolicies(true).catch(rethrowDescribed),
		encryptionConfig: () => fetchEncryptionConfig().catch(rethrowDescribed)
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
		tabLoader.refresh(activeTab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));
</script>

<!-- Group modals -->
<Modal bind:this={groupFormModal} title={groupFormMode === 'create' ? 'Create Group' : 'Update Group'}>
	{#snippet children()}
		<div class="space-y-4">
			{#if groupFormError}
				<div class="text-sm text-red-600 dark:text-red-400">{groupFormError}</div>
			{/if}
			<div>
				<label for="group-name" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Group Name</label>
				<input id="group-name" bind:value={gfName} type="text" disabled={groupFormMode === 'update'} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm disabled:opacity-60" placeholder="my-service-group" />
			</div>
			<div>
				<label for="group-filter" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Filter Expression</label>
				<input id="group-filter" bind:value={gfFilterExpression} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" placeholder={'service("my-service") { fault OR error }'} />
			</div>
			<div class="flex items-center gap-2">
				<input id="group-insights" type="checkbox" bind:checked={gfInsightsEnabled} onchange={() => { if (!gfInsightsEnabled) gfNotificationsEnabled = false; }} class="rounded border-gray-300" />
				<label for="group-insights" class="text-sm text-gray-700 dark:text-gray-300">Insights enabled</label>
			</div>
			<div class="flex items-center gap-2">
				<input id="group-notifications" type="checkbox" bind:checked={gfNotificationsEnabled} disabled={!gfInsightsEnabled} class="rounded border-gray-300 disabled:opacity-50" />
				<label for="group-notifications" class="text-sm text-gray-700 dark:text-gray-300 {!gfInsightsEnabled ? 'opacity-50' : ''}">Insights notifications enabled</label>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => groupFormModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitGroupForm} disabled={groupFormSaving} class="px-4 py-2 text-sm rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">{groupFormSaving ? 'Saving…' : groupFormMode === 'create' ? 'Create' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={groupDetailModal} title="Group Details">
	{#snippet children()}
		{#if viewedGroup}
			<div class="space-y-3 text-sm">
				{#each [
					['Name', viewedGroup.GroupName],
					['ARN', viewedGroup.GroupARN],
					['Filter Expression', viewedGroup.FilterExpression || '—'],
					['Insights Enabled', viewedGroup.InsightsConfiguration?.InsightsEnabled ? 'Yes' : 'No'],
					['Notifications Enabled', viewedGroup.InsightsConfiguration?.NotificationsEnabled ? 'Yes' : 'No']
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
		<button onclick={() => groupDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Sampling rule modals -->
<Modal bind:this={srFormModal} title={srFormMode === 'create' ? 'Create Sampling Rule' : 'Update Sampling Rule'}>
	{#snippet children()}
		<div class="space-y-4 max-h-[65vh] overflow-y-auto pr-1">
			{#if srFormError}
				<div class="text-sm text-red-600 dark:text-red-400">{srFormError}</div>
			{/if}
			<div>
				<label for="sr-name" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Rule Name</label>
				<input id="sr-name" bind:value={srRuleName} type="text" disabled={srFormMode === 'update'} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm disabled:opacity-60" placeholder="my-sampling-rule" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="sr-priority" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Priority (1-9999)</label>
					<input id="sr-priority" bind:value={srPriority} type="number" min="1" max="9999" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="sr-reservoir" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Reservoir Size</label>
					<input id="sr-reservoir" bind:value={srReservoirSize} type="number" min="0" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div>
				<label for="sr-fixedrate" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Fixed Rate (0.0-1.0)</label>
				<input id="sr-fixedrate" bind:value={srFixedRate} type="number" min="0" max="1" step="0.01" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="sr-resource-arn" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Resource ARN</label>
					<input id="sr-resource-arn" bind:value={srResourceARN} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
				</div>
				<div>
					<label for="sr-service-name" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Service Name</label>
					<input id="sr-service-name" bind:value={srServiceName} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
				</div>
				<div>
					<label for="sr-service-type" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Service Type</label>
					<input id="sr-service-type" bind:value={srServiceType} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
				</div>
				<div>
					<label for="sr-host" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Host</label>
					<input id="sr-host" bind:value={srHost} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
				</div>
				<div>
					<label for="sr-http-method" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">HTTP Method</label>
					<input id="sr-http-method" bind:value={srHTTPMethod} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
				</div>
				<div>
					<label for="sr-url-path" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">URL Path</label>
					<input id="sr-url-path" bind:value={srURLPath} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
				</div>
			</div>
			{#if srFormMode === 'create'}
				<div>
					<label for="sr-attributes" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Attributes (key=value, comma-separated)</label>
					<input id="sr-attributes" bind:value={srAttributes} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="env=prod" />
				</div>
			{:else}
				<div>
					<span class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Attributes (not editable via UpdateSamplingRule)</span>
					<p class="text-xs font-mono text-gray-500 dark:text-gray-400">{srAttributes || '—'}</p>
				</div>
			{/if}
			<div class="border-t border-gray-100 dark:border-gray-800 pt-3">
				<div class="flex items-center gap-2 mb-2">
					<input id="sr-boost-enabled" type="checkbox" bind:checked={srBoostEnabled} class="rounded border-gray-300" />
					<label for="sr-boost-enabled" class="text-sm text-gray-700 dark:text-gray-300">Sampling rate boost</label>
				</div>
				{#if srBoostEnabled}
					<div class="grid grid-cols-2 gap-3">
						<div>
							<label for="sr-boost-maxrate" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Max Rate</label>
							<input id="sr-boost-maxrate" bind:value={srBoostMaxRate} type="number" min="0" max="1" step="0.01" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
						</div>
						<div>
							<label for="sr-boost-cooldown" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Cooldown (minutes)</label>
							<input id="sr-boost-cooldown" bind:value={srBoostCooldown} type="number" min="0" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
						</div>
					</div>
				{/if}
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => srFormModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitSamplingRuleForm} disabled={srFormSaving} class="px-4 py-2 text-sm rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">{srFormSaving ? 'Saving…' : srFormMode === 'create' ? 'Create' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={srDetailModal} title="Sampling Rule Details">
	{#snippet children()}
		{#if viewedSamplingRule?.SamplingRule}
			{@const rule = viewedSamplingRule.SamplingRule}
			<div class="space-y-3 text-sm max-h-[65vh] overflow-y-auto pr-1">
				{#each [
					['Rule Name', rule.RuleName],
					['ARN', rule.RuleARN],
					['Priority', String(rule.Priority ?? '—')],
					['Fixed Rate', String(rule.FixedRate ?? '—')],
					['Reservoir Size', String(rule.ReservoirSize ?? '—')],
					['Resource ARN', rule.ResourceARN],
					['Service Name', rule.ServiceName],
					['Service Type', rule.ServiceType],
					['Host', rule.Host],
					['HTTP Method', rule.HTTPMethod],
					['URL Path', rule.URLPath],
					['Attributes', formatKeyValueList(rule.Attributes) || '—'],
					['Sampling Rate Boost', rule.SamplingRateBoost ? `max ${rule.SamplingRateBoost.MaxRate}, cooldown ${rule.SamplingRateBoost.CooldownWindowMinutes}m` : '—'],
					['Created', formatDate(viewedSamplingRule.CreatedAt)],
					['Modified', formatDate(viewedSamplingRule.ModifiedAt)]
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
		<button onclick={() => srDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Resource policy modals -->
<Modal bind:this={rpFormModal} title={rpFormMode === 'create' ? 'Create Resource Policy' : 'Update Resource Policy'}>
	{#snippet children()}
		<div class="space-y-4">
			{#if rpFormError}
				<div class="text-sm text-red-600 dark:text-red-400">{rpFormError}</div>
			{/if}
			<div>
				<label for="rp-name" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Policy Name</label>
				<input id="rp-name" bind:value={rpName} type="text" disabled={rpFormMode === 'update'} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm disabled:opacity-60" placeholder="my-resource-policy" />
			</div>
			<div>
				<label for="rp-document" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Policy Document (JSON, up to 5KB)</label>
				<textarea id="rp-document" bind:value={rpDocument} rows="8" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs font-mono"></textarea>
			</div>
			{#if rpFormMode === 'update'}
				<div>
					<label for="rp-revision" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Policy Revision ID (optional, for atomic update)</label>
					<input id="rp-revision" bind:value={rpRevisionId} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
				</div>
			{/if}
			<div class="flex items-center gap-2">
				<input id="rp-bypass" type="checkbox" bind:checked={rpBypassLockout} class="rounded border-gray-300" />
				<label for="rp-bypass" class="text-sm text-gray-700 dark:text-gray-300">Bypass policy lockout check</label>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => rpFormModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitRPForm} disabled={rpFormSaving} class="px-4 py-2 text-sm rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">{rpFormSaving ? 'Saving…' : rpFormMode === 'create' ? 'Create' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={rpDetailModal} title="Resource Policy Details">
	{#snippet children()}
		{#if viewedRP}
			<div class="space-y-3 text-sm">
				<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
					<span class="text-gray-500">Name</span>
					<span class="font-mono text-right break-all text-gray-900 dark:text-white">{viewedRP.PolicyName}</span>
				</div>
				<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
					<span class="text-gray-500">Revision ID</span>
					<span class="font-mono text-right break-all text-gray-900 dark:text-white">{viewedRP.PolicyRevisionId ?? '—'}</span>
				</div>
				<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
					<span class="text-gray-500">Last Updated</span>
					<span class="font-mono text-right break-all text-gray-900 dark:text-white">{formatDate(viewedRP.LastUpdatedTime)}</span>
				</div>
				<div>
					<span class="block text-gray-500 mb-1">Document</span>
					<pre class="text-xs font-mono whitespace-pre-wrap break-all bg-gray-50 dark:bg-gray-800 rounded-lg p-3">{viewedRP.PolicyDocument}</pre>
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => rpDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Activity}
		title="AWS X-Ray"
		description="Distributed tracing for application analysis"
		onRefresh={handleRefresh}
		color="indigo"
	>
		{#snippet actions()}
			{#if activeTab === 'groups'}
				<button onclick={openCreateGroupModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Create group
				</button>
			{:else if activeTab === 'samplingRules'}
				<button onclick={openCreateSamplingRuleModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Create rule
				</button>
			{:else if activeTab === 'resourcePolicies'}
				<button onclick={openCreateRP} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Create policy
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<!-- Summary cards, from the currently loaded page of trace summaries -->
	<div class="grid gap-4 sm:grid-cols-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg">
				<Layers class="w-5 h-5 text-indigo-600 dark:text-indigo-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{totalTraces}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Total Traces</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg">
				<AlertCircle class="w-5 h-5 text-red-600 dark:text-red-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-red-600 dark:text-red-400">{faultTraces}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Faults</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<AlertCircle class="w-5 h-5 text-orange-500 dark:text-orange-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-orange-500 dark:text-orange-400">{errorTraces}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Errors</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Activity class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-blue-600 dark:text-blue-400">{avgDuration}s</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Avg Duration</p>
			</div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="indigo" />
			{#if activeTab === 'groups' || activeTab === 'samplingRules' || activeTab === 'resourcePolicies' || activeTab === 'traces'}
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

			{#if activeTab === 'traces'}
				<div class="flex flex-wrap gap-3 rounded-lg border p-3 bg-muted/20">
					<div class="flex gap-2 items-center text-sm">
						<label for="start-time" class="font-medium">From:</label>
						<input id="start-time" type="datetime-local" bind:value={startTime} class="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
					<div class="flex gap-2 items-center text-sm">
						<label for="end-time" class="font-medium">To:</label>
						<input id="end-time" type="datetime-local" bind:value={endTime} class="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
					<select bind:value={errorFilter} class="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="all">All Traces</option>
						<option value="error">Errors Only</option>
						<option value="fault">Faults Only</option>
						<option value="throttle">Throttles Only</option>
					</select>
					<button onclick={() => tabLoader.refresh('traces')} disabled={tabLoader.isLoading('traces')} class="flex items-center gap-2 rounded-md bg-indigo-600 px-4 py-1.5 text-sm text-white hover:bg-indigo-700 disabled:opacity-50">
						<RefreshCw class="h-3.5 w-3.5 {tabLoader.isLoading('traces') ? 'animate-spin' : ''}" />
						Search
					</button>
				</div>

				{#snippet traceStatusCell(trace: TraceSummary)}
					{@const status = statusIndicator(trace)}
					<span class="text-xs font-medium {status.color}">{status.label}</span>
				{/snippet}
				{#snippet traceIdCell(trace: TraceSummary)}
					<button onclick={() => openTraceDetail(trace.Id)} class="text-indigo-600 dark:text-indigo-400 hover:underline font-mono text-xs">{trace.Id}</button>
				{/snippet}
				{#snippet traceHttpCell(trace: TraceSummary)}
					{#if trace.Http}
						<span class="font-medium">{trace.Http.HttpStatus ?? ''}</span>
						<span class="ml-1 truncate max-w-[200px] block">{trace.Http.HttpURL ?? ''}</span>
					{:else}
						—
					{/if}
				{/snippet}
				{#snippet traceDurationCell(trace: TraceSummary)}
					{trace.Duration != null ? `${trace.Duration.toFixed(3)}s` : '—'}
				{/snippet}
				{#snippet traceResponseTimeCell(trace: TraceSummary)}
					{trace.ResponseTime != null ? `${(trace.ResponseTime * 1000).toFixed(0)}ms` : '—'}
				{/snippet}
				{@const traceColumns = defineColumns<TraceSummary>([
					{ key: 'status', label: 'Status', render: traceStatusCell },
					{ key: 'id', label: 'Trace ID', render: traceIdCell },
					{ key: 'duration', label: 'Duration', render: traceDurationCell },
					{ key: 'http', label: 'HTTP', render: traceHttpCell },
					{ key: 'responseTime', label: 'Response Time', render: traceResponseTimeCell }
				])}
				<DataTable
					rows={filteredTraces}
					rowKey={(t) => t.Id ?? ''}
					columns={traceColumns}
					loading={tabLoader.isLoading('traces')}
					emptyMessage="No traces found for this time range"
				/>
				<LoadMore hasMore={!!tracesNextToken} loading={loadingMoreTraces} onLoadMore={loadMoreTraces} />
			{:else if activeTab === 'serviceGraph'}
				<div class="flex flex-wrap gap-3 rounded-lg border p-3 bg-muted/20">
					<div class="flex gap-2 items-center text-sm">
						<label for="sg-start" class="font-medium">From:</label>
						<input id="sg-start" type="datetime-local" bind:value={serviceGraphStartTime} class="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
					<div class="flex gap-2 items-center text-sm">
						<label for="sg-end" class="font-medium">To:</label>
						<input id="sg-end" type="datetime-local" bind:value={serviceGraphEndTime} class="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
					<button onclick={() => tabLoader.refresh('serviceGraph')} disabled={tabLoader.isLoading('serviceGraph')} class="flex items-center gap-2 rounded-md bg-indigo-600 px-4 py-1.5 text-sm text-white hover:bg-indigo-700 disabled:opacity-50">
						<RefreshCw class="h-3.5 w-3.5 {tabLoader.isLoading('serviceGraph') ? 'animate-spin' : ''}" /> Load
					</button>
				</div>
				{#snippet sgNameCell(svc: Service)}{svc.Name ?? '—'}{/snippet}
				{#snippet sgTypeCell(svc: Service)}{svc.Type ?? '—'}{/snippet}
				{#snippet sgRequestsCell(svc: Service)}{svc.SummaryStatistics?.TotalCount ?? 0}{/snippet}
				{#snippet sgFaultsCell(svc: Service)}
					<span class="text-red-500">{svc.SummaryStatistics?.FaultStatistics?.TotalCount ?? 0}</span>
				{/snippet}
				{#snippet sgErrorsCell(svc: Service)}
					<span class="text-orange-500">{svc.SummaryStatistics?.ErrorStatistics?.TotalCount ?? 0}</span>
				{/snippet}
				{#snippet sgLatencyCell(svc: Service)}
					{svc.SummaryStatistics?.TotalResponseTime != null ? `${(svc.SummaryStatistics.TotalResponseTime * 1000).toFixed(0)}ms` : '—'}
				{/snippet}
				{@const sgColumns = defineColumns<Service>([
					{ key: 'name', label: 'Service', render: sgNameCell },
					{ key: 'type', label: 'Type', render: sgTypeCell },
					{ key: 'requests', label: 'Requests', render: sgRequestsCell },
					{ key: 'faults', label: 'Faults', render: sgFaultsCell },
					{ key: 'errors', label: 'Errors', render: sgErrorsCell },
					{ key: 'latency', label: 'Avg Latency', render: sgLatencyCell }
				])}
				<DataTable
					rows={serviceGraphNodes}
					rowKey={(svc) => svc.Name ?? ''}
					columns={sgColumns}
					loading={tabLoader.isLoading('serviceGraph')}
					emptyMessage="No service-graph data for this range"
				/>
				<LoadMore hasMore={!!sgNextToken} loading={loadingMoreSG} onLoadMore={loadMoreServiceGraph} />
			{:else if activeTab === 'groups'}
				{#snippet groupActionsCell(g: GroupSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openGroupDetail(g)} title="View" aria-label="View group {g.GroupName}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateGroupModal(g)} title="Update" aria-label="Update group {g.GroupName}" class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteGroup(g)} title="Delete" aria-label="Delete group {g.GroupName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{#snippet groupNameCell(g: GroupSummary)}{g.GroupName ?? '—'}{/snippet}
				{#snippet groupFilterCell(g: GroupSummary)}{g.FilterExpression || '—'}{/snippet}
				{#snippet groupInsightsCell(g: GroupSummary)}{g.InsightsConfiguration?.InsightsEnabled ? 'Enabled' : 'Disabled'}{/snippet}
				{@const groupColumns = defineColumns<GroupSummary>([
					{ key: 'name', label: 'Group Name', render: groupNameCell },
					{ key: 'filter', label: 'Filter Expression', render: groupFilterCell },
					{ key: 'insights', label: 'Insights', render: groupInsightsCell },
					{ key: 'actions', label: '', render: groupActionsCell }
				])}
				<DataTable
					rows={filteredGroups}
					rowKey={(g) => g.GroupName ?? ''}
					columns={groupColumns}
					loading={tabLoader.isLoading('groups')}
					emptyMessage="No groups found"
				/>
				<LoadMore hasMore={!!groupsNextToken} loading={loadingMoreGroups} onLoadMore={loadMoreGroups} />
			{:else if activeTab === 'samplingRules'}
				{#snippet srActionsCell(r: SamplingRuleRecord)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openSamplingRuleDetail(r)} title="View" aria-label="View sampling rule {r.SamplingRule?.RuleName}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateSamplingRuleModal(r)} title="Update" aria-label="Update sampling rule {r.SamplingRule?.RuleName}" class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button>
						{#if r.SamplingRule?.RuleName === 'Default'}
							<span title="The built-in Default rule cannot be deleted" class="text-gray-300 dark:text-gray-600"><Lock class="w-4 h-4" /></span>
						{:else}
							<button onclick={() => handleDeleteSamplingRule(r)} title="Delete" aria-label="Delete sampling rule {r.SamplingRule?.RuleName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						{/if}
					</div>
				{/snippet}
				{#snippet srNameCell(r: SamplingRuleRecord)}{r.SamplingRule?.RuleName ?? '—'}{/snippet}
				{#snippet srPriorityCell(r: SamplingRuleRecord)}{r.SamplingRule?.Priority ?? '—'}{/snippet}
				{#snippet srFixedRateCell(r: SamplingRuleRecord)}{r.SamplingRule?.FixedRate ?? '—'}{/snippet}
				{#snippet srReservoirCell(r: SamplingRuleRecord)}{r.SamplingRule?.ReservoirSize ?? '—'}{/snippet}
				{#snippet srServiceCell(r: SamplingRuleRecord)}{r.SamplingRule?.ServiceName ?? '—'}{/snippet}
				{@const srColumns = defineColumns<SamplingRuleRecord>([
					{ key: 'name', label: 'Rule Name', render: srNameCell },
					{ key: 'priority', label: 'Priority', render: srPriorityCell },
					{ key: 'fixedRate', label: 'Fixed Rate', render: srFixedRateCell },
					{ key: 'reservoir', label: 'Reservoir', render: srReservoirCell },
					{ key: 'service', label: 'Service', render: srServiceCell },
					{ key: 'actions', label: '', render: srActionsCell }
				])}
				<DataTable
					rows={filteredSamplingRules}
					rowKey={(r) => r.SamplingRule?.RuleName ?? ''}
					columns={srColumns}
					loading={tabLoader.isLoading('samplingRules')}
					emptyMessage="No sampling rules found"
				/>
				<LoadMore hasMore={!!samplingRulesNextToken} loading={loadingMoreSamplingRules} onLoadMore={loadMoreSamplingRules} />
			{:else if activeTab === 'resourcePolicies'}
				{#snippet rpActionsCell(p: ResourcePolicy)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openRPDetail(p)} title="View" aria-label="View resource policy {p.PolicyName}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateRP(p)} title="Update" aria-label="Update resource policy {p.PolicyName}" class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteRP(p)} title="Delete" aria-label="Delete resource policy {p.PolicyName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{#snippet rpNameCell(p: ResourcePolicy)}{p.PolicyName ?? '—'}{/snippet}
				{#snippet rpRevisionCell(p: ResourcePolicy)}{p.PolicyRevisionId ?? '—'}{/snippet}
				{#snippet rpUpdatedCell(p: ResourcePolicy)}{formatDate(p.LastUpdatedTime)}{/snippet}
				{@const rpColumns = defineColumns<ResourcePolicy>([
					{ key: 'name', label: 'Policy Name', render: rpNameCell },
					{ key: 'revision', label: 'Revision ID', render: rpRevisionCell },
					{ key: 'updated', label: 'Last Updated', render: rpUpdatedCell },
					{ key: 'actions', label: '', render: rpActionsCell }
				])}
				<DataTable
					rows={filteredResourcePolicies}
					rowKey={(p) => p.PolicyName ?? ''}
					columns={rpColumns}
					loading={tabLoader.isLoading('resourcePolicies')}
					emptyMessage="No resource policies found"
				/>
				<LoadMore hasMore={!!rpNextToken} loading={loadingMoreRP} onLoadMore={loadMoreRP} />
			{:else if activeTab === 'encryptionConfig'}
				{#if tabLoader.isLoading('encryptionConfig')}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
				{:else if encryptionConfig}
					<div class="max-w-lg space-y-4 p-6 rounded-lg border border-slate-200 dark:border-slate-700">
						<div class="flex justify-between text-sm border-b border-gray-100 dark:border-gray-800 pb-2">
							<span class="text-gray-500">Current Status</span>
							<span class="font-mono">{encryptionConfig.Status ?? '—'}</span>
						</div>
						<div class="flex justify-between text-sm border-b border-gray-100 dark:border-gray-800 pb-2">
							<span class="text-gray-500">Current Type</span>
							<span class="font-mono">{encryptionConfig.Type ?? '—'}</span>
						</div>
						{#if encryptionConfig.KeyId}
							<div class="flex justify-between text-sm border-b border-gray-100 dark:border-gray-800 pb-2">
								<span class="text-gray-500">Current Key ID</span>
								<span class="font-mono text-right break-all">{encryptionConfig.KeyId}</span>
							</div>
						{/if}
						<div>
							<label for="ec-type" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Type</label>
							<select id="ec-type" bind:value={ecFormType} onchange={handleEcTypeChange} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
								<option value="NONE">NONE</option>
								<option value="KMS">KMS</option>
							</select>
						</div>
						{#if ecFormType === 'KMS'}
							<div>
								<label for="ec-key" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">KMS Key (alias, ARN, or key ID)</label>
								<input id="ec-key" bind:value={ecFormKeyId} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" placeholder="alias/aws/xray" />
							</div>
						{/if}
						<button onclick={submitEncryptionConfig} disabled={ecSaving} class="w-full px-4 py-2 text-sm rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">
							{ecSaving ? 'Saving…' : 'Update encryption configuration'}
						</button>
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>

<!-- Trace Detail: segment timeline (own overlay, not the shared Modal -- see comment above) -->
{#if selectedTrace || traceDetailLoading}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
		<div class="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 shadow-xl w-full max-w-3xl max-h-[85vh] overflow-hidden flex flex-col">
			<div class="flex items-center justify-between px-5 py-3 border-b border-slate-100 dark:border-slate-800">
				<div class="flex items-center gap-2">
					<Share2 class="w-4 h-4 text-indigo-500" />
					<h2 class="text-sm font-semibold text-slate-900 dark:text-white">Trace {selectedTrace?.Id ?? ''}</h2>
					{#if selectedTrace?.Duration !== undefined && selectedTrace?.Duration !== null}<span class="text-xs text-slate-400">{selectedTrace.Duration.toFixed(3)}s</span>{/if}
				</div>
				<button onclick={closeTraceDetail} class="text-slate-400 hover:text-slate-600" aria-label="Close trace detail"><X class="w-5 h-5" /></button>
			</div>
			<div class="overflow-y-auto p-5">
				{#if traceDetailLoading}
					<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
				{:else if segmentRows.length === 0}
					<div class="text-center py-10 text-slate-500 text-sm">No segment documents for this trace.</div>
				{:else}
					<div class="space-y-1.5">
						{#each segmentRows as row, i (i)}
							{@const hasDetail = row.annotations.length > 0 || row.metadata.length > 0}
							<div class="flex items-center gap-3">
								<div class="w-1/3 truncate text-xs flex items-center" style={`padding-left:${row.depth * 12}px`}>
									{#if row.depth > 0}<ChevronRight class="inline w-3 h-3 text-slate-400 flex-shrink-0" />{/if}
									{#if hasDetail}
										<button
											onclick={() => (expandedSegment = expandedSegment === i ? null : i)}
											class="truncate text-left hover:underline {row.hasFault ? 'text-red-500' : row.hasError ? 'text-orange-500' : 'text-indigo-600 dark:text-indigo-400'}"
											title="Show annotations / metadata"
										>{row.name}</button>
									{:else}
										<span class="truncate {row.hasFault ? 'text-red-500' : row.hasError ? 'text-orange-500' : 'text-slate-700 dark:text-slate-200'}">{row.name}</span>
									{/if}
								</div>
								<div class="flex-1 relative h-4 bg-slate-100 dark:bg-slate-800 rounded">
									<div class={`absolute top-0 h-4 rounded ${row.hasFault ? 'bg-red-500' : row.hasError ? 'bg-orange-500' : 'bg-indigo-500'}`} style={barStyle(row)}></div>
								</div>
								<div class="w-16 text-right text-xs text-slate-400 tabular-nums">{((row.end - row.start) * 1000).toFixed(0)}ms</div>
							</div>
							{#if expandedSegment === i && hasDetail}
								<div class="ml-4 mb-2 rounded-md border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-800/50 p-3 text-xs space-y-2">
									{#if row.annotations.length > 0}
										<div>
											<div class="font-medium text-slate-500 dark:text-slate-400 mb-1">Annotations</div>
											<div class="space-y-0.5">
												{#each row.annotations as [k, v] (k)}
													<div class="flex gap-2"><span class="font-mono text-indigo-600 dark:text-indigo-400">{k}</span><span class="font-mono text-slate-700 dark:text-slate-200 break-all">{v}</span></div>
												{/each}
											</div>
										</div>
									{/if}
									{#if row.metadata.length > 0}
										<div>
											<div class="font-medium text-slate-500 dark:text-slate-400 mb-1">Metadata</div>
											<div class="space-y-0.5">
												{#each row.metadata as [k, v] (k)}
													<div class="flex gap-2"><span class="font-mono text-emerald-600 dark:text-emerald-400">{k}</span><span class="font-mono text-slate-700 dark:text-slate-200 break-all">{v}</span></div>
												{/each}
											</div>
										</div>
									{/if}
								</div>
							{/if}
						{/each}
					</div>
				{/if}
			</div>
		</div>
	</div>
{/if}
