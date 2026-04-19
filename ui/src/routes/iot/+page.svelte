<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getIoTClient } from '$lib/aws-client';
	import {
		ListThingsCommand,
		DescribeThingCommand,
		CreateThingCommand,
		DeleteThingCommand,
		ListThingGroupsCommand,
		ListTopicRulesCommand,
		type ThingAttribute,
		type GroupNameAndArn,
		type TopicRuleListItem
	} from '@aws-sdk/client-iot';
	import { toast } from 'svelte-sonner';
	import {
		Wifi,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Eye,
		Tag,
		GitBranch,
		Radio
	} from 'lucide-svelte';

	const iot = getIoTClient();

	let loading = $state(false);
	let activeTab = $state<'things' | 'groups' | 'rules'>('things');
	let searchQuery = $state('');

	// Things
	let things = $state<ThingAttribute[]>([]);
	let selectedThing = $state<ThingAttribute | null>(null);
	let thingDetail = $state<object | null>(null);
	let loadingDetail = $state(false);
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newThingName = $state('');
	let newThingType = $state('');

	// Groups
	let groups = $state<GroupNameAndArn[]>([]);

	// Rules
	let rules = $state<TopicRuleListItem[]>([]);

	const filteredThings = $derived(
		things.filter(
			(t) =>
				(t.thingName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(t.thingTypeName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredGroups = $derived(
		groups.filter((g) => (g.groupName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredRules = $derived(
		rules.filter(
			(r) =>
				(r.ruleName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(r.topicPattern ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	async function loadThings() {
		loading = true;
		try {
			const res = await iot.send(new ListThingsCommand({ maxResults: 100 }));
			things = res.things ?? [];
		} catch (e) {
			toast.error(`Failed to load things: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadGroups() {
		loading = true;
		try {
			const res = await iot.send(new ListThingGroupsCommand({ maxResults: 100 }));
			groups = res.thingGroups ?? [];
		} catch (e) {
			toast.error(`Failed to load thing groups: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadRules() {
		loading = true;
		try {
			const res = await iot.send(new ListTopicRulesCommand({ maxResults: 100 }));
			rules = res.rules ?? [];
		} catch (e) {
			toast.error(`Failed to load topic rules: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewThing(thing: ThingAttribute) {
		selectedThing = thing;
		if (!thing.thingName) return;
		loadingDetail = true;
		try {
			const res = await iot.send(new DescribeThingCommand({ thingName: thing.thingName }));
			thingDetail = res;
		} catch (e) {
			toast.error(`Failed to load thing details: ${e}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function deleteThing(thing: ThingAttribute) {
		if (!thing.thingName || !await confirmDestructive(`Delete thing "${thing.thingName}"?`)) return;
		try {
			await iot.send(new DeleteThingCommand({ thingName: thing.thingName }));
			toast.success(`Thing "${thing.thingName}" deleted`);
			if (selectedThing?.thingName === thing.thingName) selectedThing = null;
			await loadThings();
		} catch (e) {
			toast.error(`Failed to delete thing: ${e}`);
		}
	}

	async function createThing() {
		if (!newThingName.trim()) return;
		creating = true;
		try {
			await iot.send(
				new CreateThingCommand({
					thingName: newThingName.trim(),
					thingTypeName: newThingType.trim() || undefined
				})
			);
			toast.success(`Thing "${newThingName}" created`);
			showCreateModal = false;
			newThingName = '';
			newThingType = '';
			await loadThings();
		} catch (e) {
			toast.error(`Failed to create thing: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		selectedThing = null;
		if (tab === 'things') await loadThings();
		else if (tab === 'groups') await loadGroups();
		else await loadRules();
	}

	onMount(() => loadThings());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Wifi class="h-8 w-8 text-teal-600" />
			<div>
				<h1 class="text-2xl font-bold">AWS IoT Core</h1>
				<p class="text-sm text-muted-foreground">Connect and manage IoT devices at scale</p>
			</div>
		</div>
		<button
			onclick={() => onTabChange(activeTab)}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'things', label: 'Things', icon: Radio }, { id: 'groups', label: 'Thing Groups', icon: Tag }, { id: 'rules', label: 'Topic Rules', icon: GitBranch }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				<tab.icon class="h-4 w-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Things Tab -->
	{#if activeTab === 'things'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search things..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Thing
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredThings.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Radio class="h-12 w-12 mb-3 opacity-30" />
				<p>No IoT things found</p>
				<p class="text-sm">Create a thing to register your device</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Thing Name</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-left font-medium">ARN</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredThings as thing}
							<tr class="hover:bg-muted/30 cursor-pointer" onclick={() => viewThing(thing)}>
								<td class="px-4 py-3 font-medium">{thing.thingName}</td>
								<td class="px-4 py-3 text-muted-foreground">{thing.thingTypeName ?? '—'}</td>
								<td class="px-4 py-3 text-xs text-muted-foreground truncate max-w-[250px]">
									{thing.thingArn ?? '—'}
								</td>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={(e) => { e.stopPropagation(); viewThing(thing); }}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View details"
									>
										<Eye class="h-4 w-4" />
									</button>
									<button
										onclick={(e) => { e.stopPropagation(); deleteThing(thing); }}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete thing"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- Thing Detail -->
			{#if selectedThing}
				<div class="rounded-lg border p-4 space-y-3">
					<div class="flex items-center justify-between">
						<h3 class="font-semibold">{selectedThing.thingName}</h3>
						<button onclick={() => (selectedThing = null)} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>
					{#if loadingDetail}
						<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
					{:else if selectedThing.attributes && Object.keys(selectedThing.attributes).length > 0}
						<div>
							<p class="text-sm font-medium mb-2">Attributes</p>
							<div class="flex flex-wrap gap-2">
								{#each Object.entries(selectedThing.attributes) as [key, value]}
									<span class="rounded bg-muted px-2 py-1 text-xs font-mono">
										{key}={value}
									</span>
								{/each}
							</div>
						</div>
					{:else}
						<p class="text-sm text-muted-foreground">No attributes</p>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- Groups Tab -->
	{#if activeTab === 'groups'}
		<div class="relative">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search groups..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredGroups.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Tag class="h-12 w-12 mb-3 opacity-30" />
				<p>No thing groups found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Group Name</th>
							<th class="px-4 py-3 text-left font-medium">ARN</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredGroups as group}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{group.groupName}</td>
								<td class="px-4 py-3 text-xs text-muted-foreground">{group.groupArn ?? '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Rules Tab -->
	{#if activeTab === 'rules'}
		<div class="relative">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search rules..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredRules.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<GitBranch class="h-12 w-12 mb-3 opacity-30" />
				<p>No topic rules found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Rule Name</th>
							<th class="px-4 py-3 text-left font-medium">Topic Pattern</th>
							<th class="px-4 py-3 text-left font-medium">Created</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredRules as rule}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{rule.ruleName}</td>
								<td class="px-4 py-3 font-mono text-xs text-muted-foreground">{rule.topicPattern ?? '—'}</td>
								<td class="px-4 py-3 text-xs text-muted-foreground">
									{rule.createdAt ? new Date(rule.createdAt).toLocaleDateString() : '—'}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Thing Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create IoT Thing</h2>
			<div class="space-y-3">
				<div>
					<label for="thing-name" class="block text-sm font-medium mb-1">Thing Name *</label>
					<input
						id="thing-name"
						type="text"
						bind:value={newThingName}
						placeholder="my-device"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="thing-type" class="block text-sm font-medium mb-1">Thing Type (optional)</label>
					<input
						id="thing-type"
						type="text"
						bind:value={newThingType}
						placeholder="LightBulb"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createThing}
					disabled={creating || !newThingName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create Thing'}
				</button>
			</div>
		</div>
	</div>
{/if}
