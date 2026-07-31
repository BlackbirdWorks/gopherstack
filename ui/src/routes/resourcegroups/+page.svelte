<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getResourceGroupsClient } from '$lib/aws-client';
	import {
		ListGroupsCommand,
		ListGroupResourcesCommand,
		GetTagsCommand,
		ListTagSyncTasksCommand,
		GroupResourcesCommand,
		UngroupResourcesCommand,
		type GroupIdentifier,
		type ListGroupResourcesCommandOutput
	} from '@aws-sdk/client-resource-groups';
	import { toast } from 'svelte-sonner';
	import { Layers, RefreshCw, Tag, Link, Activity, Search } from 'lucide-svelte';

	const resourceGroups = regionalClient(getResourceGroupsClient);

	type TabName = 'groups' | 'resources' | 'tags' | 'synctasks';

	let activeTab = $state<TabName>('groups');
	let loading = $state(false);
	let searchQuery = $state('');

	// Groups tab
	let groups = $state<GroupIdentifier[]>([]);
	let selectedGroup = $state<string | null>(null);

	// Resources tab
	type ResourceItem = { ResourceArn?: string; ResourceType?: string };
	let groupResources = $state<ResourceItem[]>([]);
	let resourcesLoading = $state(false);
	let newResourceArn = $state('');
	let ungroupArn = $state('');

	// Tags tab
	type TagEntry = { key: string; value: string };
	let groupTags = $state<TagEntry[]>([]);
	let tagsLoading = $state(false);

	// Sync tasks tab
	type SyncTask = {
		TaskArn?: string;
		GroupName?: string;
		GroupArn?: string;
		RoleArn?: string;
		TagKey?: string;
		TagValue?: string;
		Status?: string;
	};
	let syncTasks = $state<SyncTask[]>([]);
	let syncTasksLoading = $state(false);

	const filteredGroups = $derived(
		groups.filter((g) => (g.GroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function loadGroups() {
		loading = true;
		try {
			const out = await resourceGroups().send(new ListGroupsCommand({}));
			groups = out.GroupIdentifiers ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load groups: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadGroupResources(groupName: string) {
		resourcesLoading = true;
		try {
			const out: ListGroupResourcesCommandOutput = await resourceGroups().send(
				new ListGroupResourcesCommand({ Group: groupName })
			);
			groupResources = (out.Resources ?? []).map((r) => ({
				ResourceArn: r.Identifier?.ResourceArn,
				ResourceType: r.Identifier?.ResourceType
			}));
		} catch (err: unknown) {
			toast.error(`Failed to load resources: ${(err as Error).message}`);
		} finally {
			resourcesLoading = false;
		}
	}

	async function loadGroupTags(groupName: string) {
		tagsLoading = true;
		try {
			const group = groups.find((g) => g.GroupName === groupName);
			if (!group?.GroupArn) {
				groupTags = [];
				return;
			}
			const out = await resourceGroups().send(new GetTagsCommand({ Arn: group.GroupArn }));
			groupTags = Object.entries(out.Tags ?? {}).map(([key, value]) => ({ key, value }));
		} catch (err: unknown) {
			toast.error(`Failed to load tags: ${(err as Error).message}`);
		} finally {
			tagsLoading = false;
		}
	}

	async function loadSyncTasks() {
		syncTasksLoading = true;
		try {
			const out = await resourceGroups().send(new ListTagSyncTasksCommand({}));
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			syncTasks = ((out as any).TagSyncTasks ?? []) as SyncTask[];
		} catch (err: unknown) {
			toast.error(`Failed to load sync tasks: ${(err as Error).message}`);
		} finally {
			syncTasksLoading = false;
		}
	}

	async function addResource() {
		if (!selectedGroup || !newResourceArn.trim()) return;
		try {
			await resourceGroups().send(
				new GroupResourcesCommand({ Group: selectedGroup, ResourceArns: [newResourceArn.trim()] })
			);
			newResourceArn = '';
			toast.success('Resource added to group');
			await loadGroupResources(selectedGroup);
		} catch (err: unknown) {
			toast.error(`Failed to add resource: ${(err as Error).message}`);
		}
	}

	async function ungroupResource(arn: string) {
		if (!selectedGroup) return;
		try {
			await resourceGroups().send(
				new UngroupResourcesCommand({ Group: selectedGroup, ResourceArns: [arn] })
			);
			toast.success('Resource removed from group');
			await loadGroupResources(selectedGroup);
		} catch (err: unknown) {
			toast.error(`Failed to ungroup resource: ${(err as Error).message}`);
		}
	}

	function selectGroup(name: string) {
		selectedGroup = name;
		if (activeTab === 'resources') {
			void loadGroupResources(name);
		} else if (activeTab === 'tags') {
			void loadGroupTags(name);
		}
	}

	function switchTab(tab: TabName) {
		activeTab = tab;
		if (tab === 'synctasks') {
			void loadSyncTasks();
		} else if (tab === 'resources' && selectedGroup) {
			void loadGroupResources(selectedGroup);
		} else if (tab === 'tags' && selectedGroup) {
			void loadGroupTags(selectedGroup);
		}
	}

	onRegionChange(loadGroups);
</script>

<div class="space-y-6 p-6">
	<!-- Header -->
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="flex items-center justify-between">
			<div class="flex items-center gap-3">
				<Layers class="h-8 w-8 text-blue-500" />
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Resource Groups</h1>
			</div>
			<button
				onclick={() => void loadGroups()}
				class="flex items-center gap-2 rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
			>
				<RefreshCw class="h-4 w-4" />
				Refresh
			</button>
		</div>
	</div>

	<!-- Tabs -->
	<div class="rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="border-b border-slate-200 dark:border-slate-700">
			<nav class="flex space-x-1 px-4 pt-2">
				{#each ([['groups', 'Groups', Layers], ['resources', 'Resources', Link], ['tags', 'Tags', Tag], ['synctasks', 'Sync Tasks', Activity]] as const) as [tab, label, Icon]}
					<button
						onclick={() => switchTab(tab as TabName)}
						class="flex items-center gap-2 rounded-t-lg px-4 py-2 text-sm font-medium transition-colors {activeTab === tab
							? 'border-b-2 border-blue-500 text-blue-600 dark:text-blue-400'
							: 'text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200'}"
					>
						<Icon class="h-4 w-4" />
						{label}
					</button>
				{/each}
			</nav>
		</div>

		<div class="p-6">
			<!-- Groups Tab -->
			{#if activeTab === 'groups'}
				<div class="mb-4 flex items-center gap-3">
					<div class="relative flex-1">
						<Search class="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
						<input
							type="text"
							placeholder="Search groups..."
							bind:value={searchQuery}
							class="w-full rounded-lg border border-slate-200 py-2 pl-10 pr-4 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:border-slate-600 dark:bg-slate-700 dark:text-white"
						/>
					</div>
				</div>

				{#if loading}
					<p class="text-sm text-slate-500 dark:text-slate-400">Loading groups...</p>
				{:else if filteredGroups.length === 0}
					<p class="text-sm text-slate-500 dark:text-slate-400">No groups found</p>
				{:else}
					<div class="space-y-2">
						{#each filteredGroups as group}
							<div
								class="cursor-pointer rounded-lg border p-3 transition-colors {selectedGroup === group.GroupName
									? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20'
									: 'border-slate-200 hover:border-slate-300 dark:border-slate-700'}"
								onclick={() => selectGroup(group.GroupName ?? '')}
								role="button"
								tabindex="0"
								onkeydown={(e) => e.key === 'Enter' && selectGroup(group.GroupName ?? '')}
							>
								<div class="font-medium text-slate-900 dark:text-white">{group.GroupName}</div>
								<div class="text-xs text-slate-500 dark:text-slate-400">{group.GroupArn}</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}

			<!-- Resources Tab -->
			{#if activeTab === 'resources'}
				{#if !selectedGroup}
					<p class="text-sm text-slate-500 dark:text-slate-400">Select a group from the Groups tab first.</p>
				{:else}
					<div class="mb-4">
						<p class="mb-2 text-sm font-medium text-slate-700 dark:text-slate-300">
							Group: <span class="font-bold">{selectedGroup}</span>
						</p>
						<div class="flex gap-2">
							<input
								type="text"
								placeholder="Resource ARN to add..."
								bind:value={newResourceArn}
								class="flex-1 rounded-lg border border-slate-200 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:border-slate-600 dark:bg-slate-700 dark:text-white"
							/>
							<button
								onclick={() => void addResource()}
								class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
							>
								Add Resource
							</button>
						</div>
					</div>

					{#if resourcesLoading}
						<p class="text-sm text-slate-500 dark:text-slate-400">Loading resources...</p>
					{:else if groupResources.length === 0}
						<p class="text-sm text-slate-500 dark:text-slate-400">No resources in this group.</p>
					{:else}
						<div class="space-y-2">
							{#each groupResources as res}
								<div class="flex items-center justify-between rounded-lg border border-slate-200 p-3 dark:border-slate-700">
									<div>
										<div class="text-sm font-mono text-slate-800 dark:text-slate-200">{res.ResourceArn}</div>
										{#if res.ResourceType}
											<div class="text-xs text-slate-500 dark:text-slate-400">{res.ResourceType}</div>
										{/if}
									</div>
									<button
										onclick={() => void ungroupResource(res.ResourceArn ?? '')}
										class="rounded bg-red-100 px-2 py-1 text-xs text-red-600 hover:bg-red-200 dark:bg-red-900/30 dark:text-red-400"
									>
										Ungroup
									</button>
								</div>
							{/each}
						</div>
					{/if}
				{/if}
			{/if}

			<!-- Tags Tab -->
			{#if activeTab === 'tags'}
				{#if !selectedGroup}
					<p class="text-sm text-slate-500 dark:text-slate-400">Select a group from the Groups tab first.</p>
				{:else}
					<p class="mb-3 text-sm font-medium text-slate-700 dark:text-slate-300">
						Tags for: <span class="font-bold">{selectedGroup}</span>
					</p>
					{#if tagsLoading}
						<p class="text-sm text-slate-500 dark:text-slate-400">Loading tags...</p>
					{:else if groupTags.length === 0}
						<p class="text-sm text-slate-500 dark:text-slate-400">No tags on this group.</p>
					{:else}
						<div class="overflow-hidden rounded-lg border border-slate-200 dark:border-slate-700">
							<table class="w-full text-sm">
								<thead class="bg-slate-50 dark:bg-slate-700/50">
									<tr>
										<th class="px-4 py-2 text-left font-medium text-slate-600 dark:text-slate-300">Key</th>
										<th class="px-4 py-2 text-left font-medium text-slate-600 dark:text-slate-300">Value</th>
									</tr>
								</thead>
								<tbody>
									{#each groupTags as tag}
										<tr class="border-t border-slate-200 dark:border-slate-700">
											<td class="px-4 py-2 font-mono text-slate-800 dark:text-slate-200">{tag.key}</td>
											<td class="px-4 py-2 text-slate-600 dark:text-slate-400">{tag.value}</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					{/if}
				{/if}
			{/if}

			<!-- Sync Tasks Tab -->
			{#if activeTab === 'synctasks'}
				<div class="mb-4 flex items-center justify-between">
					<p class="text-sm text-slate-600 dark:text-slate-400">Tag-sync task status (TTL-evicted after 24 h when not ACTIVE)</p>
					<button
						onclick={() => void loadSyncTasks()}
						class="flex items-center gap-1 rounded-lg bg-slate-100 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-200 dark:bg-slate-700 dark:text-slate-300"
					>
						<RefreshCw class="h-3 w-3" /> Refresh
					</button>
				</div>

				{#if syncTasksLoading}
					<p class="text-sm text-slate-500 dark:text-slate-400">Loading sync tasks...</p>
				{:else if syncTasks.length === 0}
					<p class="text-sm text-slate-500 dark:text-slate-400">No tag-sync tasks found.</p>
				{:else}
					<div class="space-y-2">
						{#each syncTasks as task}
							<div class="rounded-lg border border-slate-200 p-3 dark:border-slate-700">
								<div class="flex items-center justify-between">
									<div class="font-medium text-slate-900 dark:text-white">{task.GroupName}</div>
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {task.Status === 'ACTIVE' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-400'}">
										{task.Status ?? 'UNKNOWN'}
									</span>
								</div>
								{#if task.TagKey}
									<div class="mt-1 text-xs text-slate-500 dark:text-slate-400">
										Tag: {task.TagKey}{task.TagValue ? `=${task.TagValue}` : ''}
									</div>
								{/if}
								<div class="mt-1 truncate text-xs font-mono text-slate-400">{task.TaskArn}</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
