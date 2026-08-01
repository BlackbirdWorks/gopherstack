<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getResourceGroupsClient } from '$lib/aws-client';
	import {
		ListGroupsCommand,
		CreateGroupCommand,
		GetGroupCommand,
		UpdateGroupCommand,
		DeleteGroupCommand,
		GetGroupQueryCommand,
		UpdateGroupQueryCommand,
		ListGroupResourcesCommand,
		GroupResourcesCommand,
		UngroupResourcesCommand,
		GetTagsCommand,
		TagCommand,
		UntagCommand,
		ListGroupingStatusesCommand,
		ListTagSyncTasksCommand,
		StartTagSyncTaskCommand,
		CancelTagSyncTaskCommand,
		GetAccountSettingsCommand,
		UpdateAccountSettingsCommand,
		type GroupIdentifier,
		type Group,
		type ResourceQuery,
		type QueryType,
		type GroupingStatusesItem,
		type TagSyncTaskItem,
		type GroupLifecycleEventsDesiredStatus,
		type AccountSettings
	} from '@aws-sdk/client-resource-groups';
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
	import { Layers, Plus, Trash2, Eye, Pencil, Settings, X } from 'lucide-svelte';

	const client = regionalClient(getResourceGroupsClient);

	type TabId = 'groups' | 'resources' | 'tags' | 'groupingStatuses' | 'syncTasks' | 'accountSettings';

	const tabs: TabDef[] = [
		{ id: 'groups', label: 'Groups' },
		{ id: 'resources', label: 'Resources' },
		{ id: 'tags', label: 'Tags' },
		{ id: 'groupingStatuses', label: 'Grouping Statuses' },
		{ id: 'syncTasks', label: 'Tag Sync Tasks' },
		{ id: 'accountSettings', label: 'Account Settings' }
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

	let activeTab = $state<TabId>('groups');
	let searchQuery = $state('');

	let groups = $state<GroupIdentifier[]>([]);
	let groupsNextToken = $state<string | undefined>();
	let loadingMoreGroups = $state(false);

	// Resources / Tags / Grouping Statuses are all scoped to one selected
	// group, the same shared-selector pattern accessanalyzer uses for its
	// analyzer-scoped tabs.
	let selectedGroupName = $state('');
	const selectedGroup = $derived(groups.find((g) => g.GroupName === selectedGroupName));
	const groupScopedTabs: TabId[] = ['resources', 'tags', 'groupingStatuses'];

	type ResourceItem = { ResourceArn?: string; ResourceType?: string };
	let groupResources = $state<ResourceItem[]>([]);
	let groupResourcesNextToken = $state<string | undefined>();
	let loadingMoreGroupResources = $state(false);

	type TagEntry = { key: string; value: string };
	let groupTags = $state<TagEntry[]>([]);

	let groupingStatuses = $state<GroupingStatusesItem[]>([]);
	let groupingStatusesNextToken = $state<string | undefined>();
	let loadingMoreGroupingStatuses = $state(false);

	let syncTasks = $state<TagSyncTaskItem[]>([]);
	let syncTasksNextToken = $state<string | undefined>();
	let loadingMoreSyncTasks = $state(false);

	let accountSettings = $state<AccountSettings | null>(null);

	async function fetchGroups(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListGroupsCommand({ NextToken: reset ? undefined : groupsNextToken })
		);
		groups = reset ? (resp.GroupIdentifiers ?? []) : [...groups, ...(resp.GroupIdentifiers ?? [])];
		groupsNextToken = resp.NextToken;
		if (!selectedGroupName && groups.length > 0) {
			selectedGroupName = groups[0].GroupName ?? '';
		}
	}

	async function fetchGroupResources(reset: boolean): Promise<void> {
		if (!selectedGroupName) {
			groupResources = [];
			groupResourcesNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListGroupResourcesCommand({
				Group: selectedGroupName,
				NextToken: reset ? undefined : groupResourcesNextToken
			})
		);
		const page = (resp.Resources ?? []).map((r) => ({
			ResourceArn: r.Identifier?.ResourceArn,
			ResourceType: r.Identifier?.ResourceType
		}));
		groupResources = reset ? page : [...groupResources, ...page];
		groupResourcesNextToken = resp.NextToken;
	}

	async function fetchGroupTags(): Promise<void> {
		if (!selectedGroup?.GroupArn) {
			groupTags = [];
			return;
		}
		const resp = await client().send(new GetTagsCommand({ Arn: selectedGroup.GroupArn }));
		groupTags = Object.entries(resp.Tags ?? {}).map(([key, value]) => ({ key, value }));
	}

	async function fetchGroupingStatuses(reset: boolean): Promise<void> {
		if (!selectedGroupName) {
			groupingStatuses = [];
			groupingStatusesNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListGroupingStatusesCommand({
				Group: selectedGroupName,
				NextToken: reset ? undefined : groupingStatusesNextToken
			})
		);
		groupingStatuses = reset
			? (resp.GroupingStatuses ?? [])
			: [...groupingStatuses, ...(resp.GroupingStatuses ?? [])];
		groupingStatusesNextToken = resp.NextToken;
	}

	async function fetchSyncTasks(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListTagSyncTasksCommand({ NextToken: reset ? undefined : syncTasksNextToken })
		);
		syncTasks = reset ? (resp.TagSyncTasks ?? []) : [...syncTasks, ...(resp.TagSyncTasks ?? [])];
		syncTasksNextToken = resp.NextToken;
	}

	async function fetchAccountSettings(): Promise<void> {
		const resp = await client().send(new GetAccountSettingsCommand({}));
		accountSettings = resp.AccountSettings ?? null;
	}

	const tabLoader = createTabLoader<TabId>({
		groups: () => fetchGroups(true).catch(rethrowDescribed),
		resources: () => fetchGroupResources(true).catch(rethrowDescribed),
		tags: () => fetchGroupTags().catch(rethrowDescribed),
		groupingStatuses: () => fetchGroupingStatuses(true).catch(rethrowDescribed),
		syncTasks: () => fetchSyncTasks(true).catch(rethrowDescribed),
		accountSettings: () => fetchAccountSettings().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	function onGroupSelect(name: string): void {
		selectedGroupName = name;
		if (groupScopedTabs.includes(activeTab)) {
			tabLoader.refresh(activeTab);
		}
	}

	// Groups is the parent resource for the group-scoped tabs: on a region
	// change the previously selected group belongs to the old region and must
	// not be reused, so reload groups first (which re-selects a group for the
	// new region) before reloading whichever tab is active.
	onRegionChange(() => {
		selectedGroupName = '';
		groups = [];
		groupsNextToken = undefined;
		void tabLoader.refresh('groups').then(() => {
			if (activeTab !== 'groups') {
				tabLoader.refresh(activeTab);
			}
		});
	});

	const filteredGroups = $derived(
		groups.filter((g) => (g.GroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredResources = $derived(
		groupResources.filter((r) => (r.ResourceArn ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredSyncTasks = $derived(
		syncTasks.filter((t) => (t.GroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

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

	async function loadMoreGroupResources(): Promise<void> {
		loadingMoreGroupResources = true;
		try {
			await fetchGroupResources(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGroupResources = false;
		}
	}

	async function loadMoreGroupingStatuses(): Promise<void> {
		loadingMoreGroupingStatuses = true;
		try {
			await fetchGroupingStatuses(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGroupingStatuses = false;
		}
	}

	async function loadMoreSyncTasks(): Promise<void> {
		loadingMoreSyncTasks = true;
		try {
			await fetchSyncTasks(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSyncTasks = false;
		}
	}

	// GetGroup's response type (Group) names the field "Name"; ListGroups'
	// GroupIdentifier names the same concept "GroupName" -- a real, if
	// mildly annoying, inconsistency in the SDK's own generated types. This
	// normalizes both for display.
	function groupDisplayName(g: Group | GroupIdentifier): string {
		return ('Name' in g ? g.Name : g.GroupName) ?? '—';
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// --- Groups: create / edit / delete / detail (with query) ---

	let createGroupModal = $state<Modal | null>(null);
	let creatingGroup = $state(false);
	let createGroupError = $state<string | null>(null);
	let newGroupName = $state('');
	let newGroupDescription = $state('');
	let newGroupQueryType = $state<QueryType>('TAG_FILTERS_1_0');
	let newGroupQuery = $state('{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[{"Key":"Stage","Values":["Test"]}]}');

	function openCreateGroupModal(): void {
		createGroupError = null;
		newGroupName = '';
		newGroupDescription = '';
		newGroupQueryType = 'TAG_FILTERS_1_0';
		newGroupQuery =
			'{"ResourceTypeFilters":["AWS::AllSupported"],"TagFilters":[{"Key":"Stage","Values":["Test"]}]}';
		createGroupModal?.open();
	}

	async function submitCreateGroup(): Promise<void> {
		if (!newGroupName) {
			createGroupError = 'Group name is required.';
			return;
		}
		let resourceQuery: ResourceQuery | undefined;
		if (newGroupQuery.trim()) {
			try {
				JSON.parse(newGroupQuery);
			} catch {
				createGroupError = 'Resource query must be valid JSON.';
				return;
			}
			resourceQuery = { Type: newGroupQueryType, Query: newGroupQuery };
		}
		creatingGroup = true;
		createGroupError = null;
		try {
			await client().send(
				new CreateGroupCommand({
					Name: newGroupName,
					Description: newGroupDescription || undefined,
					ResourceQuery: resourceQuery
				})
			);
			toast.success('Group created');
			createGroupModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			const msg = describeError(e);
			createGroupError = msg;
			toast.error(msg);
		} finally {
			creatingGroup = false;
		}
	}

	async function handleDeleteGroup(g: GroupIdentifier): Promise<void> {
		if (!g.GroupName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete group',
			message: `Delete group ${g.GroupName}? Its membership definition is removed; member resources themselves are not deleted.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteGroupCommand({ Group: g.GroupName }));
			toast.success('Group deleted');
			if (selectedGroupName === g.GroupName) {
				selectedGroupName = '';
			}
			await tabLoader.refresh('groups');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let groupDetailModal = $state<Modal | null>(null);
	let viewedGroup = $state<Group | GroupIdentifier | null>(null);
	let viewedGroupQuery = $state<ResourceQuery | null>(null);
	let groupDetailLoading = $state(false);
	let groupDetailError = $state<string | null>(null);

	async function openGroupDetail(g: GroupIdentifier): Promise<void> {
		viewedGroup = g;
		viewedGroupQuery = null;
		groupDetailError = null;
		groupDetailModal?.open();
		if (!g.GroupName) return;
		groupDetailLoading = true;
		try {
			const [detailResp, queryResp] = await Promise.all([
				client().send(new GetGroupCommand({ Group: g.GroupName })),
				client()
					.send(new GetGroupQueryCommand({ Group: g.GroupName }))
					.catch(() => {
						/* not every group has a resource query (e.g. configuration-based groups); best-effort */
					})
			]);
			viewedGroup = detailResp.Group ?? g;
			viewedGroupQuery = queryResp?.GroupQuery?.ResourceQuery ?? null;
		} catch (e) {
			groupDetailError = describeError(e);
		} finally {
			groupDetailLoading = false;
		}
	}

	let editGroupModal = $state<Modal | null>(null);
	let editingGroup = $state(false);
	let editGroupError = $state<string | null>(null);
	let editGroupTarget = $state('');
	let editGroupDescription = $state('');
	let editGroupDisplayName = $state('');
	let editGroupOwner = $state('');
	let editGroupCriticality = $state('');

	function openEditGroupModal(g: GroupIdentifier): void {
		if (!g.GroupName) return;
		editGroupError = null;
		editGroupTarget = g.GroupName;
		editGroupDescription = g.Description ?? '';
		editGroupDisplayName = g.DisplayName ?? '';
		editGroupOwner = g.Owner ?? '';
		editGroupCriticality = g.Criticality === undefined ? '' : String(g.Criticality);
		editGroupModal?.open();
	}

	async function submitEditGroup(): Promise<void> {
		if (!editGroupTarget) return;
		editingGroup = true;
		editGroupError = null;
		try {
			await client().send(
				new UpdateGroupCommand({
					Group: editGroupTarget,
					Description: editGroupDescription || undefined,
					DisplayName: editGroupDisplayName || undefined,
					Owner: editGroupOwner || undefined,
					Criticality: editGroupCriticality ? Number(editGroupCriticality) : undefined
				})
			);
			toast.success('Group updated');
			editGroupModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			const msg = describeError(e);
			editGroupError = msg;
			toast.error(msg);
		} finally {
			editingGroup = false;
		}
	}

	// Editing a group's ResourceQuery is a distinct real operation
	// (UpdateGroupQuery) from editing its descriptive fields (UpdateGroup).

	let editQueryModal = $state<Modal | null>(null);
	let editingQuery = $state(false);
	let editQueryError = $state<string | null>(null);
	let editQueryTarget = $state('');
	let editQueryType = $state<QueryType>('TAG_FILTERS_1_0');
	let editQueryValue = $state('{}');

	function openEditQueryModal(): void {
		if (!selectedGroupName) return;
		editQueryError = null;
		editQueryTarget = selectedGroupName;
		editQueryType = viewedGroupQuery?.Type ?? 'TAG_FILTERS_1_0';
		editQueryValue = viewedGroupQuery?.Query ?? '{}';
		editQueryModal?.open();
	}

	async function submitEditQuery(): Promise<void> {
		if (!editQueryTarget) return;
		try {
			JSON.parse(editQueryValue);
		} catch {
			editQueryError = 'Resource query must be valid JSON.';
			return;
		}
		editingQuery = true;
		editQueryError = null;
		try {
			await client().send(
				new UpdateGroupQueryCommand({
					Group: editQueryTarget,
					ResourceQuery: { Type: editQueryType, Query: editQueryValue }
				})
			);
			toast.success('Group query updated');
			editQueryModal?.close();
			// openEditQueryModal is only reachable from the group detail modal's
			// "Edit query" action, so refreshing that detail view here is safe.
			await openGroupDetail({ GroupName: editQueryTarget });
		} catch (e) {
			const msg = describeError(e);
			editQueryError = msg;
			toast.error(msg);
		} finally {
			editingQuery = false;
		}
	}

	// --- Resources: group / ungroup (this family's create/delete) ---

	let newResourceArn = $state('');

	async function addResource(): Promise<void> {
		if (!selectedGroupName || !newResourceArn.trim()) return;
		try {
			await client().send(
				new GroupResourcesCommand({ Group: selectedGroupName, ResourceArns: [newResourceArn.trim()] })
			);
			newResourceArn = '';
			toast.success('Resource added to group');
			await tabLoader.refresh('resources');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function ungroupResource(arn: string): Promise<void> {
		if (!selectedGroupName) return;
		const confirmed = await confirmDestructive({
			title: 'Ungroup resource',
			message: `Remove ${arn} from group ${selectedGroupName}? The resource itself is not deleted.`,
			confirmLabel: 'Ungroup'
		});
		if (!confirmed) return;
		try {
			await client().send(new UngroupResourcesCommand({ Group: selectedGroupName, ResourceArns: [arn] }));
			toast.success('Resource removed from group');
			await tabLoader.refresh('resources');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Tags: add / remove (Tag / Untag). GetTags has no create/delete of
	// its own -- Tag/Untag are the mutating ops for this family. ---

	let newTagKey = $state('');
	let newTagValue = $state('');

	async function addTag(): Promise<void> {
		if (!selectedGroup?.GroupArn || !newTagKey.trim()) return;
		try {
			await client().send(
				new TagCommand({ Arn: selectedGroup.GroupArn, Tags: { [newTagKey.trim()]: newTagValue } })
			);
			newTagKey = '';
			newTagValue = '';
			toast.success('Tag added');
			await tabLoader.refresh('tags');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removeTag(key: string): Promise<void> {
		if (!selectedGroup?.GroupArn) return;
		try {
			await client().send(new UntagCommand({ Arn: selectedGroup.GroupArn, Keys: [key] }));
			toast.success('Tag removed');
			await tabLoader.refresh('tags');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Tag Sync Tasks: start / cancel. CancelTagSyncTask deletes the task
	// outright per the real API (confirmed in PARITY.md), so it is treated as
	// this family's delete action. ---

	let createSyncTaskModal = $state<Modal | null>(null);
	let creatingSyncTask = $state(false);
	let createSyncTaskError = $state<string | null>(null);
	let newSyncTaskTagKey = $state('');
	let newSyncTaskTagValue = $state('');
	let newSyncTaskRoleArn = $state('');

	function openCreateSyncTaskModal(): void {
		createSyncTaskError = selectedGroupName ? null : 'Select a group from the Groups tab first.';
		newSyncTaskTagKey = '';
		newSyncTaskTagValue = '';
		newSyncTaskRoleArn = '';
		createSyncTaskModal?.open();
	}

	async function submitCreateSyncTask(): Promise<void> {
		if (!selectedGroupName) {
			createSyncTaskError = 'Select a group from the Groups tab first.';
			return;
		}
		if (!newSyncTaskTagKey || !newSyncTaskTagValue || !newSyncTaskRoleArn) {
			createSyncTaskError = 'Tag key, tag value, and role ARN are all required.';
			return;
		}
		creatingSyncTask = true;
		createSyncTaskError = null;
		try {
			await client().send(
				new StartTagSyncTaskCommand({
					Group: selectedGroupName,
					TagKey: newSyncTaskTagKey,
					TagValue: newSyncTaskTagValue,
					RoleArn: newSyncTaskRoleArn
				})
			);
			toast.success('Tag-sync task started');
			createSyncTaskModal?.close();
			await tabLoader.refresh('syncTasks');
		} catch (e) {
			const msg = describeError(e);
			createSyncTaskError = msg;
			toast.error(msg);
		} finally {
			creatingSyncTask = false;
		}
	}

	async function handleCancelSyncTask(t: TagSyncTaskItem): Promise<void> {
		if (!t.TaskArn) return;
		const confirmed = await confirmDestructive({
			title: 'Cancel tag-sync task',
			message: `Cancel and delete tag-sync task for group ${t.GroupName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new CancelTagSyncTaskCommand({ TaskArn: t.TaskArn }));
			toast.success('Tag-sync task cancelled');
			await tabLoader.refresh('syncTasks');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Account Settings: single account-wide form (Get/Update only, no list) ---

	let updatingAccountSettings = $state(false);

	async function toggleGroupLifecycleEvents(): Promise<void> {
		const desired: GroupLifecycleEventsDesiredStatus =
			accountSettings?.GroupLifecycleEventsStatus === 'ACTIVE' ? 'INACTIVE' : 'ACTIVE';
		updatingAccountSettings = true;
		try {
			const resp = await client().send(
				new UpdateAccountSettingsCommand({ GroupLifecycleEventsDesiredStatus: desired })
			);
			accountSettings = resp.AccountSettings ?? accountSettings;
			toast.success('Account settings updated');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			updatingAccountSettings = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Layers}
		title="Resource Groups"
		description="Group and manage AWS resources by tags or CloudFormation stack"
		onRefresh={handleRefresh}
		color="blue"
	>
		{#snippet actions()}
			{#if activeTab === 'groups'}
				<button
					onclick={openCreateGroupModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create group
				</button>
			{:else if activeTab === 'syncTasks'}
				<button
					onclick={openCreateSyncTaskModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Start tag-sync task
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
			{#if activeTab === 'groups' || activeTab === 'resources' || activeTab === 'syncTasks'}
				<SearchInput bind:value={searchQuery} />
			{/if}
		</div>

		<div class="p-4 space-y-4">
			{#if groupScopedTabs.includes(activeTab)}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="group-select" class="text-sm text-gray-500 dark:text-gray-400">Group</label>
					<select
						id="group-select"
						value={selectedGroupName}
						onchange={(e) => onGroupSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if groups.length === 0}
							<option value="">No groups</option>
						{/if}
						{#each groups as g (g.GroupName)}
							<option value={g.GroupName}>{g.GroupName}</option>
						{/each}
					</select>
				</div>
			{/if}

			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'groups'}
				{#snippet groupOwnerCell(g: GroupIdentifier)}
					{g.Owner ?? '—'}
				{/snippet}
				{#snippet groupCriticalityCell(g: GroupIdentifier)}
					{g.Criticality ?? '—'}
				{/snippet}
				{#snippet groupActionsCell(g: GroupIdentifier)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openGroupDetail(g)}
							title="View"
							aria-label="View group {g.GroupName}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditGroupModal(g)}
							title="Edit"
							aria-label="Edit group {g.GroupName}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteGroup(g)}
							title="Delete"
							aria-label="Delete group {g.GroupName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const groupColumns = defineColumns<GroupIdentifier>([
					{ key: 'GroupName', label: 'Name' },
					{ key: 'Description', label: 'Description' },
					{ key: 'Owner', label: 'Owner', render: groupOwnerCell },
					{ key: 'Criticality', label: 'Criticality', render: groupCriticalityCell },
					{ key: 'actions', label: '', render: groupActionsCell }
				])}
				<DataTable
					rows={filteredGroups}
					rowKey={(g) => g.GroupArn ?? ''}
					columns={groupColumns}
					loading={tabLoader.isLoading('groups')}
					emptyMessage="No groups found"
				/>
				<LoadMore hasMore={!!groupsNextToken} loading={loadingMoreGroups} onLoadMore={loadMoreGroups} />
			{:else if activeTab === 'resources'}
				{#if !selectedGroupName}
					<p class="text-sm text-slate-500 dark:text-slate-400">Select a group above first.</p>
				{:else}
					<div class="flex gap-2">
						<input
							type="text"
							placeholder="Resource ARN to add..."
							bind:value={newResourceArn}
							class="flex-1 rounded-lg border border-slate-200 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:border-slate-600 dark:bg-slate-700 dark:text-white"
						/>
						<button
							onclick={addResource}
							class="flex items-center gap-2 rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
						>
							<Plus class="w-4 h-4" /> Group resource
						</button>
					</div>
				{/if}
				{#snippet resourceActionsCell(r: ResourceItem)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => ungroupResource(r.ResourceArn ?? '')}
							title="Ungroup"
							aria-label="Ungroup resource {r.ResourceArn}"
							class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const resourceColumns = defineColumns<ResourceItem>([
					{ key: 'ResourceArn', label: 'Resource ARN' },
					{ key: 'ResourceType', label: 'Type' },
					{ key: 'actions', label: '', render: resourceActionsCell }
				])}
				<DataTable
					rows={filteredResources}
					rowKey={(r) => r.ResourceArn ?? ''}
					columns={resourceColumns}
					loading={tabLoader.isLoading('resources')}
					emptyMessage={selectedGroupName ? 'No resources in this group' : 'Select a group to see its resources'}
				/>
				<LoadMore
					hasMore={!!groupResourcesNextToken}
					loading={loadingMoreGroupResources}
					onLoadMore={loadMoreGroupResources}
				/>
			{:else if activeTab === 'tags'}
				{#if !selectedGroupName}
					<p class="text-sm text-slate-500 dark:text-slate-400">Select a group above first.</p>
				{:else}
					<div class="flex gap-2">
						<input
							type="text"
							placeholder="Tag key"
							bind:value={newTagKey}
							class="flex-1 rounded-lg border border-slate-200 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:border-slate-600 dark:bg-slate-700 dark:text-white"
						/>
						<input
							type="text"
							placeholder="Tag value"
							bind:value={newTagValue}
							class="flex-1 rounded-lg border border-slate-200 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:border-slate-600 dark:bg-slate-700 dark:text-white"
						/>
						<button
							onclick={addTag}
							class="flex items-center gap-2 rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
						>
							<Plus class="w-4 h-4" /> Add tag
						</button>
					</div>
				{/if}
				{#snippet tagActionsCell(t: TagEntry)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => removeTag(t.key)}
							title="Remove"
							aria-label="Remove tag {t.key}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const tagColumns = defineColumns<TagEntry>([
					{ key: 'key', label: 'Key' },
					{ key: 'value', label: 'Value' },
					{ key: 'actions', label: '', render: tagActionsCell }
				])}
				<DataTable
					rows={groupTags}
					rowKey={(t) => t.key}
					columns={tagColumns}
					loading={tabLoader.isLoading('tags')}
					emptyMessage={selectedGroupName ? 'No tags on this group' : 'Select a group to see its tags'}
				/>
			{:else if activeTab === 'groupingStatuses'}
				{#snippet statusCell(s: GroupingStatusesItem)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(s.Status === 'SUCCESS')}">{s.Status ?? '—'}</span>
				{/snippet}
				{#snippet statusUpdatedCell(s: GroupingStatusesItem)}
					{formatDate(s.UpdatedAt)}
				{/snippet}
				{@const statusColumns = defineColumns<GroupingStatusesItem>([
					{ key: 'ResourceArn', label: 'Resource ARN' },
					{ key: 'Action', label: 'Action' },
					{ key: 'Status', label: 'Status', render: statusCell },
					{ key: 'UpdatedAt', label: 'Updated', render: statusUpdatedCell }
				])}
				<DataTable
					rows={groupingStatuses}
					rowKey={(s) => `${s.ResourceArn ?? ''}/${s.Action ?? ''}`}
					columns={statusColumns}
					loading={tabLoader.isLoading('groupingStatuses')}
					emptyMessage={selectedGroupName
						? 'No grouping status entries'
						: 'Select a group to see its grouping statuses'}
				/>
				<LoadMore
					hasMore={!!groupingStatusesNextToken}
					loading={loadingMoreGroupingStatuses}
					onLoadMore={loadMoreGroupingStatuses}
				/>
			{:else if activeTab === 'syncTasks'}
				{#snippet syncStatusCell(t: TagSyncTaskItem)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(t.Status === 'ACTIVE')}">{t.Status ?? '—'}</span>
				{/snippet}
				{#snippet syncTagCell(t: TagSyncTaskItem)}
					{t.TagKey ? `${t.TagKey}${t.TagValue ? `=${t.TagValue}` : ''}` : '—'}
				{/snippet}
				{#snippet syncCreatedCell(t: TagSyncTaskItem)}
					{formatDate(t.CreatedAt)}
				{/snippet}
				{#snippet syncActionsCell(t: TagSyncTaskItem)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => handleCancelSyncTask(t)}
							title="Cancel"
							aria-label="Cancel tag-sync task for {t.GroupName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const syncColumns = defineColumns<TagSyncTaskItem>([
					{ key: 'GroupName', label: 'Group' },
					{ key: 'Tag', label: 'Tag', render: syncTagCell },
					{ key: 'Status', label: 'Status', render: syncStatusCell },
					{ key: 'CreatedAt', label: 'Created', render: syncCreatedCell },
					{ key: 'actions', label: '', render: syncActionsCell }
				])}
				<DataTable
					rows={filteredSyncTasks}
					rowKey={(t) => t.TaskArn ?? ''}
					columns={syncColumns}
					loading={tabLoader.isLoading('syncTasks')}
					emptyMessage="No tag-sync tasks found"
				/>
				<LoadMore
					hasMore={!!syncTasksNextToken}
					loading={loadingMoreSyncTasks}
					onLoadMore={loadMoreSyncTasks}
				/>
			{:else if activeTab === 'accountSettings'}
				<div class="max-w-lg space-y-3">
					<div class="flex items-center justify-between rounded-lg border border-slate-200 dark:border-slate-700 p-4">
						<div>
							<p class="text-sm font-medium text-slate-900 dark:text-white">Group lifecycle events</p>
							<p class="text-xs text-slate-500 dark:text-slate-400">
								Publishes group membership change events to EventBridge.
							</p>
							{#if accountSettings?.GroupLifecycleEventsStatusMessage}
								<p class="text-xs text-red-600 dark:text-red-400 mt-1">
									{accountSettings.GroupLifecycleEventsStatusMessage}
								</p>
							{/if}
						</div>
						<div class="flex items-center gap-3">
							<span class="text-xs px-2 py-1 rounded-full {statusClass(accountSettings?.GroupLifecycleEventsStatus === 'ACTIVE')}">
								{accountSettings?.GroupLifecycleEventsStatus ?? 'UNKNOWN'}
							</span>
							<button
								onclick={toggleGroupLifecycleEvents}
								disabled={updatingAccountSettings}
								class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm disabled:opacity-50"
							>
								<Settings class="w-4 h-4" />
								{accountSettings?.GroupLifecycleEventsStatus === 'ACTIVE' ? 'Turn off' : 'Turn on'}
							</button>
						</div>
					</div>
				</div>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createGroupModal} title="Create Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="group-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="group-name"
					bind:value={newGroupName}
					placeholder="my-group"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="group-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input
					id="group-description"
					bind:value={newGroupDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="group-query-type" class="text-sm text-slate-600 dark:text-slate-300">Query type</label>
				<select
					id="group-query-type"
					bind:value={newGroupQueryType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="TAG_FILTERS_1_0">Tag filters</option>
					<option value="CLOUDFORMATION_STACK_1_0">CloudFormation stack</option>
				</select>
			</div>
			<div>
				<label for="group-query" class="text-sm text-slate-600 dark:text-slate-300">Resource query (JSON)</label>
				<textarea
					id="group-query"
					bind:value={newGroupQuery}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createGroupError}
				<p class="text-sm text-red-600 dark:text-red-400">{createGroupError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createGroupModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateGroup}
			disabled={creatingGroup}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingGroup ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={groupDetailModal} title="Group">
	{#snippet children()}
		{#if groupDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedGroup}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{groupDisplayName(viewedGroup)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedGroup.GroupArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Description</dt>
					<dd class="text-slate-900 dark:text-white">{viewedGroup.Description ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Owner</dt>
					<dd class="text-slate-900 dark:text-white">{viewedGroup.Owner ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Criticality</dt>
					<dd class="text-slate-900 dark:text-white">{viewedGroup.Criticality ?? '—'}</dd>
				</div>
				<div class="flex items-center justify-between">
					<dt class="text-slate-500 dark:text-slate-400">Resource query</dt>
					<button
						type="button"
						onclick={openEditQueryModal}
						class="text-xs text-blue-600 hover:underline dark:text-blue-400">Edit query</button
					>
				</div>
				<dd class="font-mono text-xs break-all text-slate-900 dark:text-white">
					{#if viewedGroupQuery}
						{viewedGroupQuery.Type}: {viewedGroupQuery.Query}
					{:else}
						No resource query (configuration-based group)
					{/if}
				</dd>
			</dl>
			{#if groupDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{groupDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => groupDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editGroupModal} title="Edit Group">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editGroupTarget}</span>. Name and resource query are not
				editable here (query editing is a separate operation, available from the group's detail view).
			</p>
			<div>
				<label for="group-edit-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input
					id="group-edit-description"
					bind:value={editGroupDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="group-edit-displayname" class="text-sm text-slate-600 dark:text-slate-300">Display name</label>
				<input
					id="group-edit-displayname"
					bind:value={editGroupDisplayName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="group-edit-owner" class="text-sm text-slate-600 dark:text-slate-300">Owner</label>
				<input
					id="group-edit-owner"
					bind:value={editGroupOwner}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="group-edit-criticality" class="text-sm text-slate-600 dark:text-slate-300"
					>Criticality (1-10)</label
				>
				<input
					id="group-edit-criticality"
					type="number"
					min="1"
					max="10"
					bind:value={editGroupCriticality}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editGroupError}
				<p class="text-sm text-red-600 dark:text-red-400">{editGroupError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editGroupModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditGroup}
			disabled={editingGroup}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{editingGroup ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editQueryModal} title="Edit Group Query">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Updating the resource query for <span class="font-medium">{editQueryTarget}</span>.
			</p>
			<div>
				<label for="query-edit-type" class="text-sm text-slate-600 dark:text-slate-300">Query type</label>
				<select
					id="query-edit-type"
					bind:value={editQueryType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="TAG_FILTERS_1_0">Tag filters</option>
					<option value="CLOUDFORMATION_STACK_1_0">CloudFormation stack</option>
				</select>
			</div>
			<div>
				<label for="query-edit-value" class="text-sm text-slate-600 dark:text-slate-300">Query (JSON)</label>
				<textarea
					id="query-edit-value"
					bind:value={editQueryValue}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editQueryError}
				<p class="text-sm text-red-600 dark:text-red-400">{editQueryError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editQueryModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditQuery}
			disabled={editingQuery}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{editingQuery ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createSyncTaskModal} title="Start Tag-Sync Task">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				For group <span class="font-medium">{selectedGroupName || '(none selected)'}</span>. Resources
				tagged with this key/value are kept in sync as members of the group.
			</p>
			<div>
				<label for="synctask-tagkey" class="text-sm text-slate-600 dark:text-slate-300">Tag key</label>
				<input
					id="synctask-tagkey"
					bind:value={newSyncTaskTagKey}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="synctask-tagvalue" class="text-sm text-slate-600 dark:text-slate-300">Tag value</label>
				<input
					id="synctask-tagvalue"
					bind:value={newSyncTaskTagValue}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="synctask-rolearn" class="text-sm text-slate-600 dark:text-slate-300"
					>Role ARN (assumed to tag/untag resources)</label
				>
				<input
					id="synctask-rolearn"
					bind:value={newSyncTaskRoleArn}
					placeholder="arn:aws:iam::123456789012:role/TagSyncRole"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createSyncTaskError}
				<p class="text-sm text-red-600 dark:text-red-400">{createSyncTaskError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createSyncTaskModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateSyncTask}
			disabled={creatingSyncTask}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingSyncTask ? 'Starting…' : 'Start'}</button
		>
	{/snippet}
</Modal>
