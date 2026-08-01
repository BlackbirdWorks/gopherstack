<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getIoTClient } from '$lib/aws-client';
	import {
		ListThingsCommand,
		DescribeThingCommand,
		CreateThingCommand,
		DeleteThingCommand,
		ListThingGroupsCommand,
		ListTopicRulesCommand,
		GetTopicRuleCommand,
		ListPoliciesCommand,
		CreatePolicyCommand,
		DeletePolicyCommand,
		GetPolicyCommand,
		UpdateThingCommand,
		AttachPolicyCommand,
		DetachPolicyCommand,
		ListAttachedPoliciesCommand,
		type ThingAttribute,
		type GroupNameAndArn,
		type TopicRuleListItem,
		type Policy
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
		Radio,
		ShieldCheck,
		Pencil,
		Link2
	} from 'lucide-svelte';

	const iot = regionalClient(getIoTClient);

	let loading = $state(false);
	let activeTab = $state<'things' | 'groups' | 'rules' | 'policies'>('things');
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
	let selectedRule = $state<TopicRuleListItem | null>(null);
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let ruleDetail = $state<any | null>(null);
	let loadingRuleDetail = $state(false);

	// Policies
	let policies = $state<Policy[]>([]);
	let selectedPolicy = $state<Policy | null>(null);
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let policyDetail = $state<any | null>(null);
	let loadingPolicyDetail = $state(false);
	let showCreatePolicyModal = $state(false);
	let creatingPolicy = $state(false);
	let newPolicyName = $state('');
	let newPolicyDocument = $state(
		JSON.stringify(
			{
				Version: '2012-10-17',
				Statement: [{ Effect: 'Allow', Action: 'iot:*', Resource: '*' }]
			},
			null,
			2
		)
	);

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

	const filteredPolicies = $derived(
		policies.filter((p) => (p.policyName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function loadThings() {
		loading = true;
		try {
			const res = await iot().send(new ListThingsCommand({ maxResults: 100 }));
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
			const res = await iot().send(new ListThingGroupsCommand({ maxResults: 100 }));
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
			const res = await iot().send(new ListTopicRulesCommand({ maxResults: 100 }));
			rules = res.rules ?? [];
		} catch (e) {
			toast.error(`Failed to load topic rules: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadPolicies() {
		loading = true;
		try {
			const res = await iot().send(new ListPoliciesCommand({ pageSize: 100 }));
			policies = res.policies ?? [];
		} catch (e) {
			toast.error(`Failed to load policies: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewThing(thing: ThingAttribute) {
		selectedThing = thing;
		if (!thing.thingName) return;
		loadingDetail = true;
		try {
			const res = await iot().send(new DescribeThingCommand({ thingName: thing.thingName }));
			thingDetail = res;
		} catch (e) {
			toast.error(`Failed to load thing details: ${e}`);
		} finally {
			loadingDetail = false;
		}
	}

	// Thing attribute editor
	let editingAttrs = $state(false);
	let attrRows = $state<{ key: string; value: string }[]>([]);
	let savingAttrs = $state(false);

	function startEditAttributes() {
		const attrs = selectedThing?.attributes ?? {};
		attrRows = Object.entries(attrs).map(([key, value]) => ({ key, value: String(value) }));
		if (attrRows.length === 0) attrRows = [{ key: '', value: '' }];
		editingAttrs = true;
	}

	function addAttrRow() {
		attrRows = [...attrRows, { key: '', value: '' }];
	}

	function removeAttrRow(i: number) {
		attrRows = attrRows.filter((_, idx) => idx !== i);
	}

	async function saveAttributes() {
		if (!selectedThing?.thingName) return;
		savingAttrs = true;
		try {
			const attributes: Record<string, string> = {};
			for (const r of attrRows) {
				if (r.key.trim()) attributes[r.key.trim()] = r.value;
			}
			await iot().send(
				new UpdateThingCommand({
					thingName: selectedThing.thingName,
					attributePayload: { attributes, merge: false }
				})
			);
			toast.success('Thing attributes updated');
			editingAttrs = false;
			// Reflect the change locally and re-fetch detail.
			selectedThing = { ...selectedThing, attributes };
			things = things.map((t) =>
				t.thingName === selectedThing?.thingName ? { ...t, attributes } : t
			);
			await viewThing(selectedThing);
		} catch (e) {
			toast.error(`Failed to update attributes: ${e}`);
		} finally {
			savingAttrs = false;
		}
	}

	// Policy attach/detach to a target (certificate ARN, thing-group ARN, or
	// Cognito identity), plus listing what is already attached.
	let showAttachModal = $state(false);
	let attachTarget = $state('');
	let attachPolicyName = $state('');
	let attaching = $state(false);
	let attachedPolicies = $state<{ policyName?: string; policyArn?: string }[]>([]);

	function openAttachModal(policy: Policy) {
		attachPolicyName = policy.policyName ?? '';
		attachTarget = '';
		attachedPolicies = [];
		showAttachModal = true;
	}

	async function listAttached() {
		if (!attachTarget.trim()) {
			attachedPolicies = [];
			return;
		}
		attaching = true;
		try {
			const res = await iot().send(
				new ListAttachedPoliciesCommand({ target: attachTarget.trim() })
			);
			attachedPolicies = res.policies ?? [];
		} catch (e) {
			toast.error(`Failed to list attached policies: ${e}`);
		} finally {
			attaching = false;
		}
	}

	async function attachOrDetach(detach = false) {
		if (!attachPolicyName.trim() || !attachTarget.trim()) return;
		attaching = true;
		try {
			if (detach) {
				await iot().send(
					new DetachPolicyCommand({ policyName: attachPolicyName.trim(), target: attachTarget.trim() })
				);
				toast.success(`Detached ${attachPolicyName} from target`);
			} else {
				await iot().send(
					new AttachPolicyCommand({ policyName: attachPolicyName.trim(), target: attachTarget.trim() })
				);
				toast.success(`Attached ${attachPolicyName} to target`);
			}
			await listAttached();
		} catch (e) {
			toast.error(`Failed to ${detach ? 'detach' : 'attach'} policy: ${e}`);
		} finally {
			attaching = false;
		}
	}

	async function deleteThing(thing: ThingAttribute) {
		if (
			!thing.thingName ||
			!(await confirmDestructive({
				title: 'Delete Thing',
				message: `Delete thing "${thing.thingName}"? All attached certificates and policies will be detached.`
			}))
		)
			return;
		try {
			await iot().send(new DeleteThingCommand({ thingName: thing.thingName }));
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
			await iot().send(
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

	async function viewRule(rule: TopicRuleListItem) {
		selectedRule = rule;
		if (!rule.ruleName) return;
		loadingRuleDetail = true;
		try {
			const res = await iot().send(new GetTopicRuleCommand({ ruleName: rule.ruleName }));
			ruleDetail = res;
		} catch (e) {
			toast.error(`Failed to load rule details: ${e}`);
		} finally {
			loadingRuleDetail = false;
		}
	}

	async function viewPolicy(policy: Policy) {
		selectedPolicy = policy;
		if (!policy.policyName) return;
		loadingPolicyDetail = true;
		try {
			const res = await iot().send(new GetPolicyCommand({ policyName: policy.policyName }));
			policyDetail = res;
		} catch (e) {
			toast.error(`Failed to load policy details: ${e}`);
		} finally {
			loadingPolicyDetail = false;
		}
	}

	async function deletePolicy(policy: Policy) {
		if (
			!policy.policyName ||
			!(await confirmDestructive({
				title: 'Delete Policy',
				message: `Delete policy "${policy.policyName}"?`
			}))
		)
			return;
		try {
			await iot().send(new DeletePolicyCommand({ policyName: policy.policyName }));
			toast.success(`Policy "${policy.policyName}" deleted`);
			if (selectedPolicy?.policyName === policy.policyName) selectedPolicy = null;
			await loadPolicies();
		} catch (e) {
			toast.error(`Failed to delete policy: ${e}`);
		}
	}

	async function createPolicy() {
		if (!newPolicyName.trim()) return;
		creatingPolicy = true;
		try {
			await iot().send(
				new CreatePolicyCommand({
					policyName: newPolicyName.trim(),
					policyDocument: newPolicyDocument.trim()
				})
			);
			toast.success(`Policy "${newPolicyName}" created`);
			showCreatePolicyModal = false;
			newPolicyName = '';
			await loadPolicies();
		} catch (e) {
			toast.error(`Failed to create policy: ${e}`);
		} finally {
			creatingPolicy = false;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		selectedThing = null;
		selectedRule = null;
		selectedPolicy = null;
		if (tab === 'things') await loadThings();
		else if (tab === 'groups') await loadGroups();
		else if (tab === 'rules') await loadRules();
		else await loadPolicies();
	}

	// Selected thing/rule/policy point at resources from whichever region
	// they were fetched in; clear them (matching onTabChange's behavior)
	// and reload whichever tab is active rather than only refetching
	// things. `activeTab` is read via untrack() because onTabChange also
	// writes it — without untrack, every tab switch would re-trigger this
	// region effect and double-fetch.
	onRegionChange(() => {
		selectedThing = null;
		selectedRule = null;
		selectedPolicy = null;
		editingAttrs = false;
		const tab = untrack(() => activeTab);
		if (tab === 'things') void loadThings();
		else if (tab === 'groups') void loadGroups();
		else if (tab === 'rules') void loadRules();
		else void loadPolicies();
	});
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
		{#each [{ id: 'things', label: 'Things', icon: Radio }, { id: 'groups', label: 'Thing Groups', icon: Tag }, { id: 'rules', label: 'Topic Rules', icon: GitBranch }, { id: 'policies', label: 'Policies', icon: ShieldCheck }] as tab}
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
										onclick={(e) => {
											e.stopPropagation();
											viewThing(thing);
										}}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View details"
									>
										<Eye class="h-4 w-4" />
									</button>
									<button
										onclick={(e) => {
											e.stopPropagation();
											deleteThing(thing);
										}}
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
						<div class="flex items-center gap-2">
							{#if !editingAttrs}
								<button
									onclick={startEditAttributes}
									class="flex items-center gap-1 rounded border px-2 py-1 text-xs hover:bg-accent"
								>
									<Pencil class="h-3.5 w-3.5" /> Edit Attributes
								</button>
							{/if}
							<button
								onclick={() => { selectedThing = null; editingAttrs = false; }}
								class="text-xs text-muted-foreground hover:text-foreground"
							>
								Close
							</button>
						</div>
					</div>
					{#if loadingDetail}
						<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
					{:else if editingAttrs}
						<div class="space-y-2">
							<p class="text-sm font-medium">Attributes</p>
							{#each attrRows as row, i}
								<div class="flex items-center gap-2">
									<input
										type="text"
										bind:value={row.key}
										placeholder="key"
										class="w-1/3 rounded-md border bg-background px-2 py-1 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
									/>
									<input
										type="text"
										bind:value={row.value}
										placeholder="value"
										class="flex-1 rounded-md border bg-background px-2 py-1 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
									/>
									<button onclick={() => removeAttrRow(i)} class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Remove">
										<Trash2 class="h-3.5 w-3.5" />
									</button>
								</div>
							{/each}
							<div class="flex items-center gap-2 pt-1">
								<button onclick={addAttrRow} class="flex items-center gap-1 rounded border px-2 py-1 text-xs hover:bg-accent">
									<Plus class="h-3.5 w-3.5" /> Add Attribute
								</button>
								<div class="flex-1"></div>
								<button onclick={() => (editingAttrs = false)} class="rounded border px-3 py-1 text-xs hover:bg-accent">Cancel</button>
								<button onclick={saveAttributes} disabled={savingAttrs} class="rounded bg-primary px-3 py-1 text-xs text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
									{savingAttrs ? 'Saving…' : 'Save'}
								</button>
							</div>
							<p class="text-xs text-muted-foreground">Saving replaces all attributes on the thing.</p>
						</div>
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
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredRules as rule}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => viewRule(rule)}
							>
								<td class="px-4 py-3 font-medium">{rule.ruleName}</td>
								<td class="px-4 py-3 font-mono text-xs text-muted-foreground"
									>{rule.topicPattern ?? '—'}</td
								>
								<td class="px-4 py-3 text-xs text-muted-foreground">
									{rule.createdAt ? new Date(rule.createdAt).toLocaleDateString() : '—'}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={(e) => {
											e.stopPropagation();
											viewRule(rule);
										}}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View rule details"
									>
										<Eye class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- Rule Detail Panel -->
			{#if selectedRule}
				<div class="rounded-lg border p-4 space-y-3">
					<div class="flex items-center justify-between">
						<h3 class="font-semibold">{selectedRule.ruleName}</h3>
						<button
							onclick={() => { selectedRule = null; ruleDetail = null; }}
							class="text-xs text-muted-foreground hover:text-foreground"
						>
							Close
						</button>
					</div>
					{#if loadingRuleDetail}
						<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
					{:else if ruleDetail?.rule}
						<div class="space-y-2 text-sm">
							<div>
								<span class="font-medium">SQL:</span>
								<code class="ml-2 rounded bg-muted px-2 py-0.5 text-xs font-mono"
									>{ruleDetail.rule.sql ?? '—'}</code
								>
							</div>
							{#if ruleDetail.rule.description}
								<div>
									<span class="font-medium">Description:</span>
									<span class="ml-2 text-muted-foreground">{ruleDetail.rule.description}</span>
								</div>
							{/if}
							{#if ruleDetail.rule.actions?.length}
								<div>
									<p class="font-medium mb-1">Actions</p>
									<div class="space-y-1">
										{#each ruleDetail.rule.actions as action}
											<div class="rounded bg-muted px-3 py-2 text-xs font-mono">
												{#if action.sqs}
													<span class="font-semibold text-orange-600">SQS</span> →
													{action.sqs.queueUrl}
												{:else if action.lambda}
													<span class="font-semibold text-purple-600">Lambda</span> →
													{action.lambda.functionArn}
												{:else}
													{JSON.stringify(action)}
												{/if}
											</div>
										{/each}
									</div>
								</div>
							{/if}
						</div>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- Policies Tab -->
	{#if activeTab === 'policies'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search policies..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreatePolicyModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Policy
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredPolicies.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<ShieldCheck class="h-12 w-12 mb-3 opacity-30" />
				<p>No IoT policies found</p>
				<p class="text-sm">Create a policy to control device permissions</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Policy Name</th>
							<th class="px-4 py-3 text-left font-medium">ARN</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredPolicies as policy}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => viewPolicy(policy)}
							>
								<td class="px-4 py-3 font-medium">{policy.policyName}</td>
								<td class="px-4 py-3 text-xs text-muted-foreground truncate max-w-[300px]"
									>{policy.policyArn ?? '—'}</td
								>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={(e) => {
											e.stopPropagation();
											viewPolicy(policy);
										}}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View policy"
									>
										<Eye class="h-4 w-4" />
									</button>
									<button
										onclick={(e) => {
											e.stopPropagation();
											openAttachModal(policy);
										}}
										class="rounded p-1 text-teal-600 hover:bg-teal-50 dark:hover:bg-teal-950"
										title="Attach / detach policy"
									>
										<Link2 class="h-4 w-4" />
									</button>
									<button
										onclick={(e) => {
											e.stopPropagation();
											deletePolicy(policy);
										}}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete policy"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- Policy Detail Panel -->
			{#if selectedPolicy}
				<div class="rounded-lg border p-4 space-y-3">
					<div class="flex items-center justify-between">
						<h3 class="font-semibold">{selectedPolicy.policyName}</h3>
						<button
							onclick={() => { selectedPolicy = null; policyDetail = null; }}
							class="text-xs text-muted-foreground hover:text-foreground"
						>
							Close
						</button>
					</div>
					{#if loadingPolicyDetail}
						<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
					{:else if policyDetail?.policyDocument}
						<div>
							<p class="text-sm font-medium mb-2">Policy Document</p>
							<pre class="rounded bg-muted px-3 py-2 text-xs font-mono overflow-auto max-h-64">{JSON.stringify(
									JSON.parse(policyDetail.policyDocument),
									null,
									2
								)}</pre>
						</div>
					{/if}
				</div>
			{/if}
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
					<label for="thing-type" class="block text-sm font-medium mb-1"
						>Thing Type (optional)</label
					>
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

<!-- Create Policy Modal -->
{#if showCreatePolicyModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-lg rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create IoT Policy</h2>
			<div class="space-y-3">
				<div>
					<label for="policy-name" class="block text-sm font-medium mb-1">Policy Name *</label>
					<input
						id="policy-name"
						type="text"
						bind:value={newPolicyName}
						placeholder="my-iot-policy"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="policy-doc" class="block text-sm font-medium mb-1"
						>Policy Document (JSON) *</label
					>
					<textarea
						id="policy-doc"
						bind:value={newPolicyDocument}
						rows={8}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					></textarea>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreatePolicyModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createPolicy}
					disabled={creatingPolicy || !newPolicyName.trim() || !newPolicyDocument.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingPolicy ? 'Creating...' : 'Create Policy'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Attach / Detach Policy Modal -->
{#if showAttachModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-lg rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-1">Attach / Detach Policy</h2>
			<p class="text-sm text-muted-foreground mb-4">Policy: <span class="font-mono">{attachPolicyName}</span></p>
			<div class="space-y-3">
				<div>
					<label for="attach-tgt" class="block text-sm font-medium mb-1">
						Target (certificate ARN, thing-group ARN, or Cognito identity)
					</label>
					<div class="flex gap-2">
						<input
							id="attach-tgt"
							type="text"
							bind:value={attachTarget}
							placeholder="arn:aws:iot:…:cert/… or us-east-1:…"
							class="flex-1 rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
						/>
						<button
							onclick={listAttached}
							disabled={attaching || !attachTarget.trim()}
							class="rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
						>
							List Attached
						</button>
					</div>
				</div>
				{#if attachedPolicies.length > 0}
					<div class="rounded border p-2 text-xs">
						<p class="font-medium mb-1">Policies on target:</p>
						<ul class="list-disc pl-4 space-y-0.5">
							{#each attachedPolicies as ap}
								<li class={ap.policyName === attachPolicyName ? 'font-semibold text-primary' : ''}>
									{ap.policyName}
								</li>
							{/each}
						</ul>
					</div>
				{/if}
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showAttachModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Close</button>
				<button
					onclick={() => attachOrDetach(true)}
					disabled={attaching || !attachTarget.trim()}
					class="inline-flex items-center gap-1 rounded-md border px-4 py-2 text-sm text-red-600 hover:bg-red-50 disabled:opacity-50 dark:hover:bg-red-950"
				>
					Detach
				</button>
				<button
					onclick={() => attachOrDetach(false)}
					disabled={attaching || !attachTarget.trim()}
					class="inline-flex items-center gap-1 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					<Link2 class="h-4 w-4" /> Attach
				</button>
			</div>
		</div>
	</div>
{/if}
