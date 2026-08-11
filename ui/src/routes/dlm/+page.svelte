<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getDLMClient } from '$lib/aws-client';
	import {
		GetLifecyclePoliciesCommand,
		GetLifecyclePolicyCommand,
		CreateLifecyclePolicyCommand,
		UpdateLifecyclePolicyCommand,
		DeleteLifecyclePolicyCommand,
		TagResourceCommand,
		UntagResourceCommand,
		PolicyTypeValues,
		PolicyLanguageValues,
		ResourceTypeValues,
		ResourceLocationValues,
		LocationValues,
		RetentionIntervalUnitValues,
		type LifecyclePolicySummary,
		type LifecyclePolicy,
		type PolicyDetails,
		type Schedule,
		type Tag as PolicyDetailTag
	} from '@aws-sdk/client-dlm';
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
	import { CalendarClock, Plus, Trash2, Eye, Pencil, X } from 'lucide-svelte';

	// DLM has exactly one listable resource family (lifecycle policies) --
	// TagResource/UntagResource/ListTagsForResource operate on a policy's own
	// ARN and are not a separately listable family, so tag management lives
	// inside the policy detail modal instead of getting its own tab.
	const client = regionalClient(getDLMClient);

	type TabId = 'policies';

	const tabs: TabDef[] = [{ id: 'policies', label: 'Lifecycle Policies' }];

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

	function stateClass(state: string | undefined): string {
		if (state === 'ENABLED') {
			return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		}
		if (state === 'ERROR') {
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		}
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('policies');
	let searchQuery = $state('');

	let policies = $state<LifecyclePolicySummary[]>([]);

	// GetLifecyclePolicies has no NextToken/pagination on the wire (confirmed
	// against GetLifecyclePoliciesResponse in the vendored SDK types -- it
	// carries only `Policies`), so there is no LoadMore here.
	async function fetchPolicies(): Promise<void> {
		const resp = await client().send(new GetLifecyclePoliciesCommand({}));
		policies = resp.Policies ?? [];
	}

	// Wrap so a failure's message (captured by tab-loader as err.message)
	// already contains the AWS error code/status rather than a bare string.
	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	const tabLoader = createTabLoader<TabId>({
		policies: () => fetchPolicies().catch(rethrowDescribed)
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
		tabLoader.refresh('policies');
	});

	const filteredPolicies = $derived(
		policies.filter((p) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			const tagsMatch = Object.entries(p.Tags ?? {}).some(([k, v]) =>
				`${k}=${v}`.toLowerCase().includes(q)
			);
			return (
				(p.PolicyId ?? '').toLowerCase().includes(q) ||
				(p.Description ?? '').toLowerCase().includes(q) ||
				(p.State ?? '').toLowerCase().includes(q) ||
				(p.PolicyType ?? '').toLowerCase().includes(q) ||
				tagsMatch
			);
		})
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- PolicyDetails editor (shared by Create and Edit) ---
	//
	// policyDetailsDraft covers PolicyType, PolicyLanguage, ResourceTypes,
	// ResourceLocations, TargetTags, the default-policy fields (ResourceType,
	// CreateInterval, RetainInterval, CopyTags, ExtendDeletion,
	// CrossRegionCopyTargets, Exclusions), and per-schedule Name, CopyTags,
	// TagsToAdd, VariableTags, CreateRule, RetainRule. Actions, EventSource,
	// Parameters (event-based-policy fields) and each schedule's
	// FastRestoreRule/CrossRegionCopyRules/ShareRules/DeprecateRule/
	// ArchiveRule/CreateRule.Scripts have no dedicated control -- offering one
	// without verifying every nested shape risked a silent no-op. They are
	// preserved verbatim when editing a policy that already has them (the
	// draft is the real loaded object; controls only touch the paths listed
	// above), and Actions/EventSource/Parameters can still be set through the
	// "Advanced (JSON)" box below, which is merged over the structured draft
	// on submit.
	let policyDetailsDraft = $state<PolicyDetails>({});
	let advancedDetailsJSON = $state('');
	let advancedDetailsJSONError = $state<string | null>(null);

	function pruneEmpty(value: unknown): unknown {
		if (Array.isArray(value)) {
			const pruned = value.map((v) => pruneEmpty(v)).filter((v) => v !== undefined);
			return pruned.length > 0 ? pruned : undefined;
		}
		if (value && typeof value === 'object') {
			const out: Record<string, unknown> = {};
			for (const [k, v] of Object.entries(value)) {
				const p = pruneEmpty(v);
				if (p !== undefined) out[k] = p;
			}
			return Object.keys(out).length > 0 ? out : undefined;
		}
		if (value === '' || value === null || value === undefined) return undefined;
		if (typeof value === 'number' && Number.isNaN(value)) return undefined;
		return value;
	}

	// splitAdvancedFields pulls the fields with no dedicated control out of a
	// loaded PolicyDetails so they seed the Advanced JSON box instead of
	// silently vanishing from the structured draft.
	function splitAdvancedFields(details: PolicyDetails): {
		rest: PolicyDetails;
		advanced: Pick<PolicyDetails, 'Actions' | 'EventSource' | 'Parameters'>;
	} {
		const { Actions, EventSource, Parameters, ...rest } = details;
		const advanced: Pick<PolicyDetails, 'Actions' | 'EventSource' | 'Parameters'> = {};
		if (Actions !== undefined) advanced.Actions = Actions;
		if (EventSource !== undefined) advanced.EventSource = EventSource;
		if (Parameters !== undefined) advanced.Parameters = Parameters;
		return { rest, advanced };
	}

	function loadPolicyDetailsDraft(details?: PolicyDetails): void {
		// $state.snapshot, not structuredClone: details may be (nested inside)
		// a $state proxy (e.g. viewedPolicy.PolicyDetails), and structuredClone
		// throws DataCloneError on Svelte's reactive proxy internals.
		const { rest, advanced } = splitAdvancedFields($state.snapshot(details ?? {}));
		policyDetailsDraft = rest;
		advancedDetailsJSON = Object.keys(advanced).length > 0 ? JSON.stringify(advanced, null, 2) : '';
		advancedDetailsJSONError = null;
	}

	// buildPolicyDetailsPayload prunes the structured draft, then merges the
	// Advanced JSON box (if any) over it -- the advanced fields always win on
	// key collision, since they were the more recent/explicit edit.
	function buildPolicyDetailsPayload(): PolicyDetails | undefined {
		const pruned = (pruneEmpty(policyDetailsDraft) as PolicyDetails | undefined) ?? {};
		advancedDetailsJSONError = null;
		if (!advancedDetailsJSON.trim()) {
			return Object.keys(pruned).length > 0 ? pruned : undefined;
		}
		let parsed: unknown;
		try {
			parsed = JSON.parse(advancedDetailsJSON);
		} catch {
			advancedDetailsJSONError = 'Advanced JSON is not valid JSON.';
			throw new Error(advancedDetailsJSONError);
		}
		const merged = { ...pruned, ...(parsed && typeof parsed === 'object' ? parsed : {}) };
		return Object.keys(merged).length > 0 ? merged : undefined;
	}

	function numOrUndef(v: string): number | undefined {
		return v === '' ? undefined : Number(v);
	}

	function toggleInList<T>(list: T[] | undefined, value: T): T[] {
		const set = new Set(list ?? []);
		if (set.has(value)) set.delete(value);
		else set.add(value);
		return [...set];
	}

	function tagListOps(getList: () => PolicyDetailTag[] | undefined, setList: (v: PolicyDetailTag[]) => void) {
		return {
			add: () => setList([...(getList() ?? []), { Key: '', Value: '' }]),
			remove: (i: number) => setList((getList() ?? []).filter((_, idx) => idx !== i)),
			setKey: (i: number, v: string) => {
				const list = [...(getList() ?? [])];
				list[i] = { ...list[i], Key: v };
				setList(list);
			},
			setValue: (i: number, v: string) => {
				const list = [...(getList() ?? [])];
				list[i] = { ...list[i], Value: v };
				setList(list);
			}
		};
	}

	const targetTagsOps = tagListOps(
		() => policyDetailsDraft.TargetTags,
		(v) => (policyDetailsDraft.TargetTags = v)
	);

	function updateExclusions(patch: Partial<PolicyDetails['Exclusions']>): void {
		policyDetailsDraft.Exclusions = { ...policyDetailsDraft.Exclusions, ...patch };
	}

	const excludeTagsOps = tagListOps(
		() => policyDetailsDraft.Exclusions?.ExcludeTags,
		(v) => updateExclusions({ ExcludeTags: v })
	);

	function addExcludeVolumeType(): void {
		updateExclusions({ ExcludeVolumeTypes: [...(policyDetailsDraft.Exclusions?.ExcludeVolumeTypes ?? []), ''] });
	}
	function removeExcludeVolumeType(i: number): void {
		updateExclusions({
			ExcludeVolumeTypes: (policyDetailsDraft.Exclusions?.ExcludeVolumeTypes ?? []).filter((_, idx) => idx !== i)
		});
	}
	function setExcludeVolumeType(i: number, v: string): void {
		const list = [...(policyDetailsDraft.Exclusions?.ExcludeVolumeTypes ?? [])];
		list[i] = v;
		updateExclusions({ ExcludeVolumeTypes: list });
	}

	function addCrossRegionCopyTarget(): void {
		policyDetailsDraft.CrossRegionCopyTargets = [
			...(policyDetailsDraft.CrossRegionCopyTargets ?? []),
			{ TargetRegion: '' }
		];
	}
	function removeCrossRegionCopyTarget(i: number): void {
		policyDetailsDraft.CrossRegionCopyTargets = (policyDetailsDraft.CrossRegionCopyTargets ?? []).filter(
			(_, idx) => idx !== i
		);
	}
	function setCrossRegionCopyTargetRegion(i: number, v: string): void {
		const list = [...(policyDetailsDraft.CrossRegionCopyTargets ?? [])];
		list[i] = { TargetRegion: v };
		policyDetailsDraft.CrossRegionCopyTargets = list;
	}

	function addSchedule(): void {
		policyDetailsDraft.Schedules = [...(policyDetailsDraft.Schedules ?? []), { CreateRule: {}, RetainRule: {} }];
	}
	function removeSchedule(i: number): void {
		policyDetailsDraft.Schedules = (policyDetailsDraft.Schedules ?? []).filter((_, idx) => idx !== i);
	}
	function updateSchedule(i: number, patch: Partial<Schedule>): void {
		const list = [...(policyDetailsDraft.Schedules ?? [])];
		if (!list[i]) return;
		list[i] = { ...list[i], ...patch };
		policyDetailsDraft.Schedules = list;
	}
	function updateScheduleCreateRule(i: number, patch: Partial<Schedule['CreateRule']>): void {
		const sch = policyDetailsDraft.Schedules?.[i];
		updateSchedule(i, { CreateRule: { ...sch?.CreateRule, ...patch } });
	}
	function updateScheduleRetainRule(i: number, patch: Partial<Schedule['RetainRule']>): void {
		const sch = policyDetailsDraft.Schedules?.[i];
		updateSchedule(i, { RetainRule: { ...sch?.RetainRule, ...patch } });
	}

	function scheduleTagListOps(i: number, field: 'TagsToAdd' | 'VariableTags') {
		return tagListOps(
			() => policyDetailsDraft.Schedules?.[i]?.[field],
			(v) => updateSchedule(i, { [field]: v })
		);
	}

	function addScheduleTime(i: number): void {
		const sch = policyDetailsDraft.Schedules?.[i];
		updateScheduleCreateRule(i, { Times: [...(sch?.CreateRule?.Times ?? []), ''] });
	}
	function removeScheduleTime(i: number, timeIndex: number): void {
		const sch = policyDetailsDraft.Schedules?.[i];
		updateScheduleCreateRule(i, {
			Times: (sch?.CreateRule?.Times ?? []).filter((_, idx) => idx !== timeIndex)
		});
	}
	function setScheduleTime(i: number, timeIndex: number, v: string): void {
		const sch = policyDetailsDraft.Schedules?.[i];
		const list = [...(sch?.CreateRule?.Times ?? [])];
		list[timeIndex] = v;
		updateScheduleCreateRule(i, { Times: list });
	}

	// --- Create ---

	let createModal = $state<Modal | null>(null);
	let creating = $state(false);
	let createError = $state<string | null>(null);
	let newDescription = $state('');
	let newExecutionRoleArn = $state('');
	let newState = $state<'ENABLED' | 'DISABLED'>('ENABLED');
	let newTags = $state<{ key: string; value: string }[]>([]);

	function openCreateModal(): void {
		createError = null;
		newDescription = '';
		newExecutionRoleArn = '';
		newState = 'ENABLED';
		newTags = [];
		loadPolicyDetailsDraft();
		createModal?.open();
	}

	function addNewTagRow(): void {
		newTags = [...newTags, { key: '', value: '' }];
	}

	function removeNewTagRow(index: number): void {
		newTags = newTags.filter((_, i) => i !== index);
	}

	async function submitCreate(): Promise<void> {
		if (!newDescription || !newExecutionRoleArn) {
			createError = 'Description and execution role ARN are required.';
			return;
		}
		creating = true;
		createError = null;
		try {
			const tags: Record<string, string> = {};
			for (const t of newTags) {
				if (t.key) tags[t.key] = t.value;
			}
			await client().send(
				new CreateLifecyclePolicyCommand({
					Description: newDescription,
					ExecutionRoleArn: newExecutionRoleArn,
					State: newState,
					Tags: Object.keys(tags).length > 0 ? tags : undefined,
					PolicyDetails: buildPolicyDetailsPayload()
				})
			);
			toast.success('Lifecycle policy created');
			createModal?.close();
			await tabLoader.refresh('policies');
		} catch (e) {
			const msg = describeError(e);
			createError = msg;
			toast.error(msg);
		} finally {
			creating = false;
		}
	}

	// --- Delete ---

	async function handleDelete(p: LifecyclePolicySummary): Promise<void> {
		if (!p.PolicyId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete lifecycle policy',
			message: `Delete lifecycle policy ${p.PolicyId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteLifecyclePolicyCommand({ PolicyId: p.PolicyId }));
			toast.success('Lifecycle policy deleted');
			await tabLoader.refresh('policies');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Detail (with tag management) ---

	let detailModal = $state<Modal | null>(null);
	let viewedPolicy = $state<LifecyclePolicy | null>(null);
	let detailLoading = $state(false);
	let detailError = $state<string | null>(null);
	let tagActionError = $state<string | null>(null);
	let newTagKey = $state('');
	let newTagValue = $state('');

	async function openDetail(p: LifecyclePolicySummary): Promise<void> {
		viewedPolicy = null;
		detailError = null;
		tagActionError = null;
		newTagKey = '';
		newTagValue = '';
		detailModal?.open();
		if (!p.PolicyId) return;
		detailLoading = true;
		try {
			const resp = await client().send(new GetLifecyclePolicyCommand({ PolicyId: p.PolicyId }));
			viewedPolicy = resp.Policy ?? null;
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function refreshDetail(): Promise<void> {
		if (!viewedPolicy?.PolicyId) return;
		try {
			const resp = await client().send(
				new GetLifecyclePolicyCommand({ PolicyId: viewedPolicy.PolicyId })
			);
			viewedPolicy = resp.Policy ?? viewedPolicy;
		} catch (e) {
			detailError = describeError(e);
		}
	}

	async function addTag(): Promise<void> {
		if (!viewedPolicy?.PolicyArn || !newTagKey) return;
		tagActionError = null;
		try {
			await client().send(
				new TagResourceCommand({
					ResourceArn: viewedPolicy.PolicyArn,
					Tags: { [newTagKey]: newTagValue }
				})
			);
			newTagKey = '';
			newTagValue = '';
			toast.success('Tag added');
			await refreshDetail();
			await tabLoader.refresh('policies');
		} catch (e) {
			const msg = describeError(e);
			tagActionError = msg;
			toast.error(msg);
		}
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewedPolicy?.PolicyArn) return;
		tagActionError = null;
		try {
			await client().send(
				new UntagResourceCommand({ ResourceArn: viewedPolicy.PolicyArn, TagKeys: [key] })
			);
			toast.success('Tag removed');
			await refreshDetail();
			await tabLoader.refresh('policies');
		} catch (e) {
			const msg = describeError(e);
			tagActionError = msg;
			toast.error(msg);
		}
	}

	// --- Edit ---

	let editModal = $state<Modal | null>(null);
	let editing = $state(false);
	let editError = $state<string | null>(null);
	let editPolicyId = $state('');
	let editDescription = $state('');
	let editExecutionRoleArn = $state('');
	let editState = $state<'ENABLED' | 'DISABLED'>('ENABLED');

	function openEditModal(p: LifecyclePolicy): void {
		editError = null;
		editPolicyId = p.PolicyId ?? '';
		editDescription = p.Description ?? '';
		editExecutionRoleArn = p.ExecutionRoleArn ?? '';
		editState = p.State === 'DISABLED' ? 'DISABLED' : 'ENABLED';
		loadPolicyDetailsDraft(p.PolicyDetails);
		editModal?.open();
	}

	async function submitEdit(): Promise<void> {
		if (!editPolicyId) return;
		if (!editDescription || !editExecutionRoleArn) {
			editError = 'Description and execution role ARN are required.';
			return;
		}
		editing = true;
		editError = null;
		try {
			await client().send(
				new UpdateLifecyclePolicyCommand({
					PolicyId: editPolicyId,
					Description: editDescription,
					ExecutionRoleArn: editExecutionRoleArn,
					State: editState,
					PolicyDetails: buildPolicyDetailsPayload()
				})
			);
			toast.success('Lifecycle policy updated');
			editModal?.close();
			await tabLoader.refresh('policies');
			await refreshDetail();
		} catch (e) {
			const msg = describeError(e);
			editError = msg;
			toast.error(msg);
		} finally {
			editing = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={CalendarClock}
		title="Data Lifecycle Manager"
		description="Automate EBS snapshot & AMI lifecycle"
		onRefresh={handleRefresh}
		color="teal"
	>
		{#snippet actions()}
			<button
				onclick={openCreateModal}
				class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
			>
				<Plus class="w-4 h-4" /> Create policy
			</button>
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="teal" />
			<SearchInput bind:value={searchQuery} />
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

			{#if activeTab === 'policies'}
				{#snippet policyStateCell(p: LifecyclePolicySummary)}
					<span class="text-xs px-2 py-1 rounded-full {stateClass(p.State)}">{p.State ?? '—'}</span>
				{/snippet}
				{#snippet policyActionsCell(p: LifecyclePolicySummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openDetail(p)}
							title="View"
							aria-label="View policy {p.PolicyId}"
							class="text-gray-400 hover:text-teal-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDelete(p)}
							title="Delete"
							aria-label="Delete policy {p.PolicyId}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const policyColumns = defineColumns<LifecyclePolicySummary>([
					{ key: 'PolicyId', label: 'Policy ID' },
					{ key: 'Description', label: 'Description' },
					{ key: 'State', label: 'State', render: policyStateCell },
					{ key: 'PolicyType', label: 'Type' },
					{ key: 'actions', label: '', render: policyActionsCell }
				])}
				<DataTable
					rows={filteredPolicies}
					rowKey={(p) => p.PolicyId ?? ''}
					columns={policyColumns}
					loading={tabLoader.isLoading('policies')}
					emptyMessage="No lifecycle policies found"
				/>
			{/if}
		</div>
	</div>
</div>

{#snippet policyDetailsFields(idPrefix: string)}
	<div class="max-h-96 space-y-4 overflow-y-auto rounded-lg border border-slate-200 p-3 dark:border-slate-700">
		<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Policy content</h3>

		<div class="grid grid-cols-2 gap-3">
			<div>
				<label for={`${idPrefix}-policytype`} class="text-xs text-slate-500 dark:text-slate-400">Policy type</label>
				<select
					id={`${idPrefix}-policytype`}
					bind:value={policyDetailsDraft.PolicyType}
					class="mt-1 w-full rounded-lg border border-gray-200 bg-white px-2 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
				>
					<option value={undefined}>—</option>
					{#each Object.values(PolicyTypeValues) as v (v)}
						<option value={v}>{v}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for={`${idPrefix}-policylanguage`} class="text-xs text-slate-500 dark:text-slate-400"
					>Policy language</label
				>
				<select
					id={`${idPrefix}-policylanguage`}
					bind:value={policyDetailsDraft.PolicyLanguage}
					class="mt-1 w-full rounded-lg border border-gray-200 bg-white px-2 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
				>
					<option value={undefined}>—</option>
					{#each Object.values(PolicyLanguageValues) as v (v)}
						<option value={v}>{v}</option>
					{/each}
				</select>
			</div>
		</div>

		<div>
			<span class="text-xs text-slate-500 dark:text-slate-400">Resource types (custom policies)</span>
			<div class="mt-1 flex gap-3">
				{#each Object.values(ResourceTypeValues) as rt (rt)}
					<label class="flex items-center gap-1 text-sm text-slate-700 dark:text-slate-200">
						<input
							type="checkbox"
							checked={policyDetailsDraft.ResourceTypes?.includes(rt)}
							onchange={() => (policyDetailsDraft.ResourceTypes = toggleInList(policyDetailsDraft.ResourceTypes, rt))}
							class="rounded border-gray-300"
						/>
						{rt}
					</label>
				{/each}
			</div>
		</div>

		<div>
			<span class="text-xs text-slate-500 dark:text-slate-400">Resource locations</span>
			<div class="mt-1 flex gap-3">
				{#each Object.values(ResourceLocationValues) as rl (rl)}
					<label class="flex items-center gap-1 text-sm text-slate-700 dark:text-slate-200">
						<input
							type="checkbox"
							checked={policyDetailsDraft.ResourceLocations?.includes(rl)}
							onchange={() =>
								(policyDetailsDraft.ResourceLocations = toggleInList(policyDetailsDraft.ResourceLocations, rl))}
							class="rounded border-gray-300"
						/>
						{rl}
					</label>
				{/each}
			</div>
		</div>

		<div>
			<div class="flex items-center justify-between">
				<span class="text-xs text-slate-500 dark:text-slate-400">Target tags</span>
				<button
					type="button"
					onclick={targetTagsOps.add}
					aria-label="Add target tag"
					class="text-xs text-teal-600 dark:text-teal-400 hover:underline">Add tag</button
				>
			</div>
			{#each policyDetailsDraft.TargetTags ?? [] as tag, i (i)}
				<div class="mt-2 flex items-center gap-2">
					<input
						value={tag.Key ?? ''}
						oninput={(e) => targetTagsOps.setKey(i, e.currentTarget.value)}
						placeholder="Key"
						aria-label="Target tag key"
						class="w-1/2 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
					/>
					<input
						value={tag.Value ?? ''}
						oninput={(e) => targetTagsOps.setValue(i, e.currentTarget.value)}
						placeholder="Value"
						aria-label="Target tag value"
						class="w-1/2 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
					/>
					<button
						type="button"
						onclick={() => targetTagsOps.remove(i)}
						aria-label="Remove target tag row"
						class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
					>
				</div>
			{/each}
		</div>

		<div class="space-y-3 rounded-lg border border-slate-200 p-3 dark:border-slate-700">
			<p class="text-xs font-medium text-slate-500 dark:text-slate-400">
				Default policy settings (SIMPLIFIED policies only)
			</p>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for={`${idPrefix}-resourcetype`} class="text-xs text-slate-500 dark:text-slate-400"
						>Default policy resource type</label
					>
					<select
						id={`${idPrefix}-resourcetype`}
						bind:value={policyDetailsDraft.ResourceType}
						class="mt-1 w-full rounded-lg border border-gray-200 bg-white px-2 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
					>
						<option value={undefined}>—</option>
						{#each Object.values(ResourceTypeValues) as v (v)}
							<option value={v}>{v}</option>
						{/each}
					</select>
				</div>
				<div class="flex items-end gap-4 pb-1.5">
					<label class="flex items-center gap-1 text-sm text-slate-700 dark:text-slate-200">
						<input
							type="checkbox"
							checked={policyDetailsDraft.CopyTags ?? false}
							onchange={(e) => (policyDetailsDraft.CopyTags = e.currentTarget.checked)}
							class="rounded border-gray-300"
						/>
						Copy tags
					</label>
					<label class="flex items-center gap-1 text-sm text-slate-700 dark:text-slate-200">
						<input
							type="checkbox"
							checked={policyDetailsDraft.ExtendDeletion ?? false}
							onchange={(e) => (policyDetailsDraft.ExtendDeletion = e.currentTarget.checked)}
							class="rounded border-gray-300"
						/>
						Extend deletion
					</label>
				</div>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for={`${idPrefix}-createinterval`} class="text-xs text-slate-500 dark:text-slate-400"
						>Create interval (days)</label
					>
					<input
						id={`${idPrefix}-createinterval`}
						type="number"
						value={policyDetailsDraft.CreateInterval ?? ''}
						oninput={(e) => (policyDetailsDraft.CreateInterval = numOrUndef(e.currentTarget.value))}
						class="mt-1 w-full rounded-lg border border-gray-200 bg-white px-2 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
					/>
				</div>
				<div>
					<label for={`${idPrefix}-retaininterval`} class="text-xs text-slate-500 dark:text-slate-400"
						>Retain interval (days)</label
					>
					<input
						id={`${idPrefix}-retaininterval`}
						type="number"
						value={policyDetailsDraft.RetainInterval ?? ''}
						oninput={(e) => (policyDetailsDraft.RetainInterval = numOrUndef(e.currentTarget.value))}
						class="mt-1 w-full rounded-lg border border-gray-200 bg-white px-2 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
					/>
				</div>
			</div>

			<div>
				<div class="flex items-center justify-between">
					<span class="text-xs text-slate-500 dark:text-slate-400">Cross-Region copy targets</span>
					<button
						type="button"
						onclick={addCrossRegionCopyTarget}
						class="text-xs text-teal-600 dark:text-teal-400 hover:underline">Add region</button
					>
				</div>
				{#each policyDetailsDraft.CrossRegionCopyTargets ?? [] as target, i (i)}
					<div class="mt-2 flex items-center gap-2">
						<input
							value={target.TargetRegion ?? ''}
							oninput={(e) => setCrossRegionCopyTargetRegion(i, e.currentTarget.value)}
							placeholder="us-west-2"
							aria-label="Cross-Region copy target region"
							class="w-full rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						/>
						<button
							type="button"
							onclick={() => removeCrossRegionCopyTarget(i)}
							aria-label="Remove Cross-Region copy target"
							class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
						>
					</div>
				{/each}
			</div>

			<div class="space-y-2">
				<span class="text-xs text-slate-500 dark:text-slate-400">Exclusions</span>
				<label class="flex items-center gap-1 text-sm text-slate-700 dark:text-slate-200">
					<input
						type="checkbox"
						checked={policyDetailsDraft.Exclusions?.ExcludeBootVolumes ?? false}
						onchange={(e) => updateExclusions({ ExcludeBootVolumes: e.currentTarget.checked })}
						class="rounded border-gray-300"
					/>
					Exclude boot volumes
				</label>
				<div>
					<div class="flex items-center justify-between">
						<span class="text-xs text-slate-500 dark:text-slate-400">Exclude volume types</span>
						<button
							type="button"
							onclick={addExcludeVolumeType}
							class="text-xs text-teal-600 dark:text-teal-400 hover:underline">Add type</button
						>
					</div>
					{#each policyDetailsDraft.Exclusions?.ExcludeVolumeTypes ?? [] as vt, i (i)}
						<div class="mt-2 flex items-center gap-2">
							<input
								value={vt}
								oninput={(e) => setExcludeVolumeType(i, e.currentTarget.value)}
								placeholder="gp2"
								aria-label="Excluded volume type"
								class="w-full rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
							/>
							<button
								type="button"
								onclick={() => removeExcludeVolumeType(i)}
								aria-label="Remove excluded volume type"
								class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
							>
						</div>
					{/each}
				</div>
				<div>
					<div class="flex items-center justify-between">
						<span class="text-xs text-slate-500 dark:text-slate-400">Exclude tags</span>
						<button
							type="button"
							onclick={excludeTagsOps.add}
							aria-label="Add exclude tag"
							class="text-xs text-teal-600 dark:text-teal-400 hover:underline">Add tag</button
						>
					</div>
					{#each policyDetailsDraft.Exclusions?.ExcludeTags ?? [] as tag, i (i)}
						<div class="mt-2 flex items-center gap-2">
							<input
								value={tag.Key ?? ''}
								oninput={(e) => excludeTagsOps.setKey(i, e.currentTarget.value)}
								placeholder="Key"
								aria-label="Exclude tag key"
								class="w-1/2 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
							/>
							<input
								value={tag.Value ?? ''}
								oninput={(e) => excludeTagsOps.setValue(i, e.currentTarget.value)}
								placeholder="Value"
								aria-label="Exclude tag value"
								class="w-1/2 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
							/>
							<button
								type="button"
								onclick={() => excludeTagsOps.remove(i)}
								aria-label="Remove exclude tag row"
								class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
							>
						</div>
					{/each}
				</div>
			</div>
		</div>

		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<span class="text-xs font-medium text-slate-500 dark:text-slate-400"
					>Schedules (custom policies)</span
				>
				<button
					type="button"
					onclick={addSchedule}
					class="text-xs text-teal-600 dark:text-teal-400 hover:underline">Add schedule</button
				>
			</div>
			{#each policyDetailsDraft.Schedules ?? [] as sch, i (i)}
				{@const tagsToAddOps = scheduleTagListOps(i, 'TagsToAdd')}
				{@const variableTagsOps = scheduleTagListOps(i, 'VariableTags')}
				<div class="space-y-2 rounded-lg border border-slate-200 p-3 dark:border-slate-700">
					<div class="flex items-center justify-between">
						<input
							value={sch.Name ?? ''}
							oninput={(e) => updateSchedule(i, { Name: e.currentTarget.value })}
							placeholder="Schedule name"
							aria-label="Schedule name"
							class="w-2/3 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						/>
						<button
							type="button"
							onclick={() => removeSchedule(i)}
							aria-label="Remove schedule {i + 1}"
							class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
						>
					</div>
					<label class="flex items-center gap-1 text-sm text-slate-700 dark:text-slate-200">
						<input
							type="checkbox"
							checked={sch.CopyTags ?? false}
							onchange={(e) => updateSchedule(i, { CopyTags: e.currentTarget.checked })}
							class="rounded border-gray-300"
						/>
						Copy tags
					</label>

					<p class="text-xs font-medium text-slate-500 dark:text-slate-400">Create rule</p>
					<div class="grid grid-cols-2 gap-2">
						<select
							value={sch.CreateRule?.Location ?? ''}
							onchange={(e) =>
								updateScheduleCreateRule(i, {
									Location: (e.currentTarget.value || undefined) as LocationValues | undefined
								})}
							aria-label="Create rule location"
							class="rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						>
							<option value="">Location —</option>
							{#each Object.values(LocationValues) as v (v)}
								<option value={v}>{v}</option>
							{/each}
						</select>
						<input
							type="number"
							value={sch.CreateRule?.Interval ?? ''}
							oninput={(e) => updateScheduleCreateRule(i, { Interval: numOrUndef(e.currentTarget.value) })}
							placeholder="Interval (hours)"
							aria-label="Create rule interval"
							class="rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						/>
						<input
							value={sch.CreateRule?.CronExpression ?? ''}
							oninput={(e) =>
								updateScheduleCreateRule(i, { CronExpression: e.currentTarget.value || undefined })}
							placeholder="Cron expression (alternative to interval)"
							aria-label="Create rule cron expression"
							class="col-span-2 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						/>
					</div>
					<div>
						<div class="flex items-center justify-between">
							<span class="text-xs text-slate-500 dark:text-slate-400">Times (UTC, hh:mm)</span>
							<button
								type="button"
								onclick={() => addScheduleTime(i)}
								class="text-xs text-teal-600 dark:text-teal-400 hover:underline">Add time</button
							>
						</div>
						{#each sch.CreateRule?.Times ?? [] as time, timeIndex (timeIndex)}
							<div class="mt-2 flex items-center gap-2">
								<input
									value={time}
									oninput={(e) => setScheduleTime(i, timeIndex, e.currentTarget.value)}
									placeholder="09:00"
									aria-label="Create rule time"
									class="w-full rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
								/>
								<button
									type="button"
									onclick={() => removeScheduleTime(i, timeIndex)}
									aria-label="Remove create rule time"
									class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
								>
							</div>
						{/each}
					</div>

					<p class="text-xs font-medium text-slate-500 dark:text-slate-400">Retain rule</p>
					<div class="grid grid-cols-2 gap-2">
						<input
							type="number"
							value={sch.RetainRule?.Count ?? ''}
							oninput={(e) => updateScheduleRetainRule(i, { Count: numOrUndef(e.currentTarget.value) })}
							placeholder="Count"
							aria-label="Retain rule count"
							class="rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						/>
						<input
							type="number"
							value={sch.RetainRule?.Interval ?? ''}
							oninput={(e) => updateScheduleRetainRule(i, { Interval: numOrUndef(e.currentTarget.value) })}
							placeholder="Interval"
							aria-label="Retain rule interval"
							class="rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						/>
						<select
							value={sch.RetainRule?.IntervalUnit ?? ''}
							onchange={(e) =>
							updateScheduleRetainRule(i, {
								IntervalUnit: (e.currentTarget.value || undefined) as RetentionIntervalUnitValues | undefined
							})}
							aria-label="Retain rule interval unit"
							class="col-span-2 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						>
							<option value="">Interval unit —</option>
							{#each Object.values(RetentionIntervalUnitValues) as v (v)}
								<option value={v}>{v}</option>
							{/each}
						</select>
					</div>

					<div>
						<div class="flex items-center justify-between">
							<span class="text-xs text-slate-500 dark:text-slate-400">Tags to add</span>
							<button
								type="button"
								onclick={tagsToAddOps.add}
								aria-label="Add tags to add entry"
								class="text-xs text-teal-600 dark:text-teal-400 hover:underline">Add tag</button
							>
						</div>
						{#each sch.TagsToAdd ?? [] as tag, tagIndex (tagIndex)}
							<div class="mt-2 flex items-center gap-2">
								<input
									value={tag.Key ?? ''}
									oninput={(e) => tagsToAddOps.setKey(tagIndex, e.currentTarget.value)}
									placeholder="Key"
									aria-label="Tags to add key"
									class="w-1/2 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
								/>
								<input
									value={tag.Value ?? ''}
									oninput={(e) => tagsToAddOps.setValue(tagIndex, e.currentTarget.value)}
									placeholder="Value"
									aria-label="Tags to add value"
									class="w-1/2 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
								/>
								<button
									type="button"
									onclick={() => tagsToAddOps.remove(tagIndex)}
									aria-label="Remove tags to add row"
									class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
								>
							</div>
						{/each}
					</div>

					<div>
						<div class="flex items-center justify-between">
							<span class="text-xs text-slate-500 dark:text-slate-400"
								>Variable tags (instance policies)</span
							>
							<button
								type="button"
								onclick={variableTagsOps.add}
								aria-label="Add variable tag"
								class="text-xs text-teal-600 dark:text-teal-400 hover:underline">Add tag</button
							>
						</div>
						{#each sch.VariableTags ?? [] as tag, tagIndex (tagIndex)}
							<div class="mt-2 flex items-center gap-2">
								<input
									value={tag.Key ?? ''}
									oninput={(e) => variableTagsOps.setKey(tagIndex, e.currentTarget.value)}
									placeholder="Key"
									aria-label="Variable tag key"
									class="w-1/2 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
								/>
								<input
									value={tag.Value ?? ''}
									oninput={(e) => variableTagsOps.setValue(tagIndex, e.currentTarget.value)}
									placeholder="$(instance-id)"
									aria-label="Variable tag value"
									class="w-1/2 rounded-lg border border-gray-200 bg-white px-2 py-1 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
								/>
								<button
									type="button"
									onclick={() => variableTagsOps.remove(tagIndex)}
									aria-label="Remove variable tag row"
									class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
								>
							</div>
						{/each}
					</div>
				</div>
			{/each}
		</div>

		<div>
			<label for={`${idPrefix}-advanced-json`} class="text-xs text-slate-500 dark:text-slate-400"
				>Advanced (JSON): Actions, EventSource, Parameters -- merged over the fields above on save</label
			>
			<textarea
				id={`${idPrefix}-advanced-json`}
				bind:value={advancedDetailsJSON}
				rows="4"
				placeholder={'{"EventSource": {"Type": "MANAGED_CWE", ...}}'}
				class="mt-1 w-full rounded-lg border border-gray-200 bg-white px-2 py-1.5 font-mono text-xs text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
			></textarea>
			{#if advancedDetailsJSONError}
				<p class="mt-1 text-sm text-red-600 dark:text-red-400">{advancedDetailsJSONError}</p>
			{/if}
		</div>
	</div>
{/snippet}

<Modal bind:this={createModal} title="Create Lifecycle Policy">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dlm-new-description" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="dlm-new-description"
					bind:value={newDescription}
					placeholder="Daily EBS snapshots"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dlm-new-role-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>Execution role ARN</label
				>
				<input
					id="dlm-new-role-arn"
					bind:value={newExecutionRoleArn}
					placeholder="arn:aws:iam::123456789012:role/DLM-Role"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dlm-new-state" class="text-sm text-slate-600 dark:text-slate-300">State</label>
				<select
					id="dlm-new-state"
					bind:value={newState}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ENABLED">ENABLED</option>
					<option value="DISABLED">DISABLED</option>
				</select>
			</div>
			<div>
				<div class="flex items-center justify-between">
					<span class="text-sm text-slate-600 dark:text-slate-300">Tags</span>
					<button
						type="button"
						onclick={addNewTagRow}
						class="text-xs text-teal-600 dark:text-teal-400 hover:underline">Add tag</button
					>
				</div>
				{#each newTags as tag, index (index)}
					<div class="mt-2 flex items-center gap-2">
						<input
							bind:value={tag.key}
							placeholder="Key"
							aria-label="Tag key"
							class="w-1/2 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<input
							bind:value={tag.value}
							placeholder="Value"
							aria-label="Tag value"
							class="w-1/2 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<button
							type="button"
							onclick={() => removeNewTagRow(index)}
							aria-label="Remove tag row"
							class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
						>
					</div>
				{/each}
			</div>
			{@render policyDetailsFields('pd-create')}
			{#if createError}
				<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreate}
			disabled={creating}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50"
			>{creating ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editModal} title="Edit Lifecycle Policy">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dlm-edit-description" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="dlm-edit-description"
					bind:value={editDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dlm-edit-role-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>Execution role ARN</label
				>
				<input
					id="dlm-edit-role-arn"
					bind:value={editExecutionRoleArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dlm-edit-state" class="text-sm text-slate-600 dark:text-slate-300">State</label>
				<select
					id="dlm-edit-state"
					bind:value={editState}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ENABLED">ENABLED</option>
					<option value="DISABLED">DISABLED</option>
				</select>
			</div>
			{@render policyDetailsFields('pd-edit')}
			{#if editError}
				<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEdit}
			disabled={editing}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50"
			>{editing ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Lifecycle Policy">
	{#snippet children()}
		{#if detailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if detailError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>
		{:else if viewedPolicy}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Policy ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedPolicy.PolicyId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Policy ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedPolicy.PolicyArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Description</dt>
					<dd class="text-slate-900 dark:text-white">{viewedPolicy.Description ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Execution role ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">
						{viewedPolicy.ExecutionRoleArn ?? '—'}
					</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">State</dt>
					<dd class="text-slate-900 dark:text-white">{viewedPolicy.State ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status message</dt>
					<dd class="text-slate-900 dark:text-white">{viewedPolicy.StatusMessage ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Default policy</dt>
					<dd class="text-slate-900 dark:text-white">{viewedPolicy.DefaultPolicy ? 'Yes' : 'No'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedPolicy.DateCreated)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last modified</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedPolicy.DateModified)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Tags</dt>
					<dd class="text-slate-900 dark:text-white">
						{#if Object.keys(viewedPolicy.Tags ?? {}).length === 0}
							<span class="text-slate-500 dark:text-slate-400">No tags</span>
						{:else}
							<ul class="space-y-1">
								{#each Object.entries(viewedPolicy.Tags ?? {}) as [key, value] (key)}
									<li class="flex items-center gap-2">
										<span
											class="px-2 py-0.5 rounded-full bg-gray-100 dark:bg-slate-700 text-xs"
											>{key} = {value}</span
										>
										<button
											onclick={() => removeTag(key)}
											aria-label="Remove tag {key}"
											class="text-gray-400 hover:text-red-500"><X class="w-3 h-3" /></button
										>
									</li>
								{/each}
							</ul>
						{/if}
						<div class="mt-2 flex items-center gap-2">
							<input
								bind:value={newTagKey}
								placeholder="Key"
								aria-label="New tag key"
								class="w-1/3 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							/>
							<input
								bind:value={newTagValue}
								placeholder="Value"
								aria-label="New tag value"
								class="w-1/3 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							/>
							<button
								type="button"
								onclick={addTag}
								class="px-2 py-1 text-xs rounded-lg bg-teal-600 text-white hover:bg-teal-700">Add</button
							>
						</div>
						{#if tagActionError}
							<p class="mt-1 text-sm text-red-600 dark:text-red-400">{tagActionError}</p>
						{/if}
					</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Policy details (read-only)</dt>
					<dd class="text-slate-900 dark:text-white">
						<pre
							class="mt-1 max-h-48 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(
								viewedPolicy.PolicyDetails ?? {},
								null,
								2
							)}</pre>
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => detailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
		{#if viewedPolicy}
			<button
				type="button"
				onclick={() => viewedPolicy && openEditModal(viewedPolicy)}
				class="flex items-center gap-2 rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700"
				><Pencil class="w-4 h-4" /> Edit</button
			>
		{/if}
	{/snippet}
</Modal>
