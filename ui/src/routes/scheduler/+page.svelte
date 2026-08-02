<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getSchedulerClient } from '$lib/aws-client';
	import {
		ListSchedulesCommand,
		GetScheduleCommand,
		CreateScheduleCommand,
		DeleteScheduleCommand,
		UpdateScheduleCommand,
		ListScheduleGroupsCommand,
		type ScheduleSummary,
		type ScheduleGroupSummary
	} from '@aws-sdk/client-scheduler';
	import { toast } from 'svelte-sonner';
	import {
		Clock,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Eye,
		Play,
		Pause,
		Layers,
		Pencil
	} from 'lucide-svelte';

	const scheduler = regionalClient(getSchedulerClient);

	let loading = $state(false);
	let activeTab = $state<'schedules' | 'groups'>('schedules');
	let searchQuery = $state('');

	// Schedules
	let schedules = $state<ScheduleSummary[]>([]);
	let selectedSchedule = $state<ScheduleSummary | null>(null);
	let showCreateModal = $state(false);
	let creating = $state(false);

	// Form
	let newScheduleName = $state('');
	let newScheduleExpression = $state('rate(1 hour)');
	let newScheduleTarget = $state('');
	let newScheduleTargetArn = $state('');
	let newScheduleRoleArn = $state('');
	let newScheduleState = $state<'ENABLED' | 'DISABLED'>('ENABLED');
	let newScheduleTimezone = $state('UTC');

	// Edit modal
	let showEditModal = $state(false);
	let editing = $state(false);
	let editScheduleName = $state('');
	let editScheduleGroupName = $state('');
	let editScheduleExpression = $state('');
	let editScheduleTargetArn = $state('');
	let editScheduleRoleArn = $state('');
	let editScheduleState = $state<'ENABLED' | 'DISABLED'>('ENABLED');
	let editScheduleTimezone = $state('UTC');

	// Groups
	let groups = $state<ScheduleGroupSummary[]>([]);
	let selectedGroup = $state('default');

	const filteredSchedules = $derived(
		schedules.filter(
			(s) =>
				(s.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(s.GroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredGroups = $derived(
		groups.filter((g) => (g.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	function stateBadge(state?: string) {
		if (state === 'ENABLED') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (state === 'DISABLED') return 'text-muted-foreground bg-muted';
		return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
	}

	function groupStateBadge(state?: string) {
		if (state === 'ACTIVE') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadSchedules() {
		loading = true;
		try {
			// `selectedGroup` is read with `untrack` so it never becomes a
			// dependency of the `onRegionChange` effect below -- the group
			// <select>'s onchange already writes selectedGroup and calls
			// loadSchedules() directly, so letting the effect also depend on
			// selectedGroup would double-fetch on every group change.
			const group = untrack(() => selectedGroup);
			const res = await scheduler().send(
				new ListSchedulesCommand({ GroupName: group === 'all' ? undefined : group, MaxResults: 100 })
			);
			schedules = res.Schedules ?? [];
		} catch (e) {
			toast.error(`Failed to load schedules: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadGroups() {
		loading = true;
		try {
			const res = await scheduler().send(new ListScheduleGroupsCommand({ MaxResults: 100 }));
			groups = res.ScheduleGroups ?? [];
		} catch (e) {
			toast.error(`Failed to load groups: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function deleteSchedule(schedule: ScheduleSummary) {
		if (!schedule.Name || !await confirmDestructive({ title: 'Delete Schedule', message: `Delete schedule "${schedule.Name}"? The associated target will no longer be invoked.` })) return;
		try {
			await scheduler().send(
				new DeleteScheduleCommand({
					Name: schedule.Name,
					GroupName: schedule.GroupName
				})
			);
			toast.success(`Schedule "${schedule.Name}" deleted`);
			await loadSchedules();
		} catch (e) {
			toast.error(`Failed to delete schedule: ${e}`);
		}
	}

	async function openEditModal(schedule: ScheduleSummary) {
		// Fetch full schedule details to populate the edit form
		try {
			const res = await scheduler().send(
				new GetScheduleCommand({ Name: schedule.Name, GroupName: schedule.GroupName })
			);
			editScheduleName = res.Name ?? '';
			editScheduleGroupName = schedule.GroupName ?? 'default';
			editScheduleExpression = res.ScheduleExpression ?? '';
			editScheduleTimezone = res.ScheduleExpressionTimezone ?? 'UTC';
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			editScheduleState = (res.State as any) ?? 'ENABLED';
			editScheduleTargetArn = res.Target?.Arn ?? '';
			editScheduleRoleArn = res.Target?.RoleArn ?? '';
			showEditModal = true;
		} catch (e) {
			toast.error(`Failed to load schedule details: ${e}`);
		}
	}

	async function updateSchedule() {
		if (!editScheduleExpression.trim() || !editScheduleTargetArn.trim() || !editScheduleRoleArn.trim()) {
			toast.error('Expression, target ARN, and role ARN are required');
			return;
		}
		editing = true;
		try {
			await scheduler().send(
				new UpdateScheduleCommand({
					Name: editScheduleName,
					GroupName: editScheduleGroupName === 'default' ? undefined : editScheduleGroupName,
					ScheduleExpression: editScheduleExpression.trim(),
					ScheduleExpressionTimezone: editScheduleTimezone,
					State: editScheduleState,
					Target: {
						Arn: editScheduleTargetArn.trim(),
						RoleArn: editScheduleRoleArn.trim()
					},
					FlexibleTimeWindow: { Mode: 'OFF' }
				})
			);
			toast.success(`Schedule "${editScheduleName}" updated`);
			showEditModal = false;
			await loadSchedules();
		} catch (e) {
			toast.error(`Failed to update schedule: ${e}`);
		} finally {
			editing = false;
		}
	}

	async function createSchedule() {
		if (!newScheduleName.trim() || !newScheduleExpression.trim() || !newScheduleTargetArn.trim() || !newScheduleRoleArn.trim()) {
			toast.error('Name, expression, target ARN, and role ARN are required');
			return;
		}
		creating = true;
		try {
			await scheduler().send(
				new CreateScheduleCommand({
					Name: newScheduleName.trim(),
					GroupName: selectedGroup === 'all' ? undefined : selectedGroup,
					ScheduleExpression: newScheduleExpression.trim(),
					ScheduleExpressionTimezone: newScheduleTimezone,
					State: newScheduleState,
					Target: {
						Arn: newScheduleTargetArn.trim(),
						RoleArn: newScheduleRoleArn.trim()
					},
					FlexibleTimeWindow: { Mode: 'OFF' }
				})
			);
			toast.success(`Schedule "${newScheduleName}" created`);
			showCreateModal = false;
			newScheduleName = '';
			newScheduleExpression = 'rate(1 hour)';
			newScheduleTargetArn = '';
			newScheduleRoleArn = '';
			await loadSchedules();
		} catch (e) {
			toast.error(`Failed to create schedule: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'schedules') await loadSchedules();
		else await loadGroups();
	}

	onRegionChange(async () => {
		await Promise.all([loadSchedules(), loadGroups()]);
	});
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Clock class="h-8 w-8 text-cyan-600" />
			<div>
				<h1 class="text-2xl font-bold">EventBridge Scheduler</h1>
				<p class="text-sm text-muted-foreground">Schedule tasks using cron or rate expressions</p>
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
		{#each [{ id: 'schedules', label: 'Schedules' }, { id: 'groups', label: 'Schedule Groups' }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Schedules Tab -->
	{#if activeTab === 'schedules'}
		<div class="flex items-center justify-between gap-4">
			<div class="flex gap-3 flex-1">
				<div class="relative flex-1">
					<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
					<input
						type="text"
						placeholder="Search schedules..."
						bind:value={searchQuery}
						class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<select
					bind:value={selectedGroup}
					onchange={loadSchedules}
					class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				>
					<option value="default">default</option>
					<option value="all">All Groups</option>
					{#each groups.filter((g) => g.Name !== 'default') as g}
						<option value={g.Name}>{g.Name}</option>
					{/each}
				</select>
			</div>
			<button
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Schedule
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredSchedules.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Clock class="h-12 w-12 mb-3 opacity-30" />
				<p>No schedules found</p>
				<p class="text-sm">Create a schedule to automate tasks</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Group</th>
							<th class="px-4 py-3 text-left font-medium">State</th>
							<th class="px-4 py-3 text-left font-medium">Last Modified</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredSchedules as schedule}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{schedule.Name}</td>
								<td class="px-4 py-3 text-muted-foreground">{schedule.GroupName ?? 'default'}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {stateBadge(schedule.State)}">
										{schedule.State ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{schedule.LastModificationDate ? new Date(schedule.LastModificationDate).toLocaleDateString() : '—'}
								</td>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={() => openEditModal(schedule)}
										class="rounded p-1 text-muted-foreground hover:bg-accent"
										title="Edit schedule"
									>
										<Pencil class="h-4 w-4" />
									</button>
									<button
										onclick={() => deleteSchedule(schedule)}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete schedule"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
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
				<Layers class="h-12 w-12 mb-3 opacity-30" />
				<p>No schedule groups found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">State</th>
							<th class="px-4 py-3 text-left font-medium">ARN</th>
							<th class="px-4 py-3 text-left font-medium">Created</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredGroups as group}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{group.Name}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {groupStateBadge(group.State)}">
										{group.State ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-xs text-muted-foreground truncate max-w-[300px]">{group.Arn ?? '—'}</td>
								<td class="px-4 py-3 text-xs text-muted-foreground">
									{group.CreationDate ? new Date(group.CreationDate).toLocaleDateString() : '—'}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Edit Schedule Modal -->
{#if showEditModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-lg rounded-lg bg-background p-6 shadow-xl overflow-y-auto max-h-[90vh]">
			<h2 class="text-lg font-semibold mb-4">Edit Schedule: {editScheduleName}</h2>
			<div class="space-y-3">
				<div>
					<label for="edit-sch-expr" class="block text-sm font-medium mb-1">Schedule Expression *</label>
					<input
						id="edit-sch-expr"
						type="text"
						bind:value={editScheduleExpression}
						placeholder="rate(1 hour) or cron(0 12 * * ? *)"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<p class="text-xs text-muted-foreground mt-1">
						Examples: rate(5 minutes), cron(0 9 ? * MON-FRI *)
					</p>
				</div>
				<div>
					<label for="edit-sch-tz" class="block text-sm font-medium mb-1">Timezone</label>
					<input
						id="edit-sch-tz"
						type="text"
						bind:value={editScheduleTimezone}
						placeholder="UTC"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="edit-sch-target" class="block text-sm font-medium mb-1">Target ARN *</label>
					<input
						id="edit-sch-target"
						type="text"
						bind:value={editScheduleTargetArn}
						placeholder="arn:aws:lambda:us-east-1:123456789012:function:my-function"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="edit-sch-role" class="block text-sm font-medium mb-1">Execution Role ARN *</label>
					<input
						id="edit-sch-role"
						type="text"
						bind:value={editScheduleRoleArn}
						placeholder="arn:aws:iam::123456789012:role/scheduler-role"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="edit-sch-state" class="block text-sm font-medium mb-1">State</label>
					<select
						id="edit-sch-state"
						bind:value={editScheduleState}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="ENABLED">Enabled</option>
						<option value="DISABLED">Disabled</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showEditModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={updateSchedule}
					disabled={editing || !editScheduleExpression.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{editing ? 'Saving...' : 'Save Changes'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Schedule Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-lg rounded-lg bg-background p-6 shadow-xl overflow-y-auto max-h-[90vh]">
			<h2 class="text-lg font-semibold mb-4">Create Schedule</h2>
			<div class="space-y-3">
				<div>
					<label for="sch-name" class="block text-sm font-medium mb-1">Schedule Name *</label>
					<input
						id="sch-name"
						type="text"
						bind:value={newScheduleName}
						placeholder="my-schedule"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="sch-expr" class="block text-sm font-medium mb-1">Schedule Expression *</label>
					<input
						id="sch-expr"
						type="text"
						bind:value={newScheduleExpression}
						placeholder="rate(1 hour) or cron(0 12 * * ? *)"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<p class="text-xs text-muted-foreground mt-1">
						Examples: rate(5 minutes), cron(0 9 ? * MON-FRI *)
					</p>
				</div>
				<div>
					<label for="sch-tz" class="block text-sm font-medium mb-1">Timezone</label>
					<input
						id="sch-tz"
						type="text"
						bind:value={newScheduleTimezone}
						placeholder="UTC"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="sch-target" class="block text-sm font-medium mb-1">Target ARN *</label>
					<input
						id="sch-target"
						type="text"
						bind:value={newScheduleTargetArn}
						placeholder="arn:aws:lambda:us-east-1:123456789012:function:my-function"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="sch-role" class="block text-sm font-medium mb-1">Execution Role ARN *</label>
					<input
						id="sch-role"
						type="text"
						bind:value={newScheduleRoleArn}
						placeholder="arn:aws:iam::123456789012:role/scheduler-role"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="sch-state" class="block text-sm font-medium mb-1">State</label>
					<select
						id="sch-state"
						bind:value={newScheduleState}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="ENABLED">Enabled</option>
						<option value="DISABLED">Disabled</option>
					</select>
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
					onclick={createSchedule}
					disabled={creating || !newScheduleName.trim() || !newScheduleExpression.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create Schedule'}
				</button>
			</div>
		</div>
	</div>
{/if}
