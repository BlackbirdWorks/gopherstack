<script lang="ts">
	// DataSync has three CRUD-able resource families: Agents, Locations, and
	// Tasks -- plus a fourth, Task Executions, which are *started* (from a
	// task) rather than created as an independent resource, and are stopped
	// via Cancel rather than deleted (there is no DeleteTaskExecution op in
	// the real API at all).
	//
	// Locations come in 11 real wire types (S3/NFS/SMB/EFS/HDFS/ObjectStorage/
	// AzureBlob/FsxLustre/FsxOntap/FsxOpenZfs/FsxWindows), all genuinely
	// implemented in the backend (services/datasync/PARITY.md: overall A).
	// This page's Create-Location form supports the four most common types
	// (S3, NFS, SMB, EFS) rather than all 11 near-identical forms -- a scope
	// line, not a backend gap. Delete/List are generic across every type
	// (DeleteLocation/ListLocations take no type-specific fields), so those
	// work for all 11 regardless. The detail view resolves a location's real
	// type from its LocationUri scheme prefix and calls the matching
	// Describe* command for the four supported types; other types show their
	// list-level summary with a note that this UI's detail view does not
	// have a per-type reader for them yet.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getDataSyncClient } from '$lib/aws-client';
	import {
		ListAgentsCommand,
		ListLocationsCommand,
		ListTasksCommand,
		ListTaskExecutionsCommand,
		DescribeAgentCommand,
		DescribeLocationS3Command,
		DescribeLocationNfsCommand,
		DescribeLocationSmbCommand,
		DescribeLocationEfsCommand,
		DescribeTaskCommand,
		DescribeTaskExecutionCommand,
		CreateAgentCommand,
		CreateLocationS3Command,
		CreateLocationNfsCommand,
		CreateLocationSmbCommand,
		CreateLocationEfsCommand,
		CreateTaskCommand,
		UpdateAgentCommand,
		UpdateTaskCommand,
		DeleteAgentCommand,
		DeleteLocationCommand,
		DeleteTaskCommand,
		StartTaskExecutionCommand,
		CancelTaskExecutionCommand,
		type AgentListEntry,
		type LocationListEntry,
		type TaskListEntry,
		type TaskExecutionListEntry,
		type DescribeAgentCommandOutput,
		type DescribeTaskCommandOutput,
		type DescribeTaskExecutionCommandOutput
	} from '@aws-sdk/client-datasync';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate, formatBytes } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { ArrowLeftRight, Plus, Trash2, Eye, Pencil, Play, Ban, RefreshCw } from 'lucide-svelte';

	const client = regionalClient(getDataSyncClient);

	type TabId = 'agents' | 'locations' | 'tasks' | 'executions';

	const tabs: TabDef[] = [
		{ id: 'agents', label: 'Agents' },
		{ id: 'locations', label: 'Locations' },
		{ id: 'tasks', label: 'Tasks' },
		{ id: 'executions', label: 'Task Executions' }
	];

	const activeStatuses = new Set(['AVAILABLE', 'ACTIVE', 'ONLINE', 'ENABLED', 'SUCCESS']);
	function statusClass(s: unknown): string {
		const value = String(s ?? '');
		if (activeStatuses.has(value)) return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (value.includes('ERROR') || value === 'OFFLINE') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		if (value.includes('LAUNCHING') || value.includes('PREPARING') || value.includes('TRANSFERRING') || value.includes('QUEUED') || value.includes('RUNNING')) {
			return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400';
		}
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

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

	// A location's real type is only observable from its LocationUri scheme
	// prefix (ListLocations/DescribeLocation* never echo a type field) --
	// see services/datasync/PARITY.md's LocationUri scheme notes.
	function locationType(uri: string | undefined): string {
		if (!uri) return 'unknown';
		const scheme = uri.split('://')[0];
		if (scheme === 's3') return 's3';
		if (scheme === 'nfs') return 'nfs';
		if (scheme === 'smb') return 'smb';
		if (scheme === 'efs') return 'efs';
		return scheme || 'unknown';
	}

	let activeTab = $state<TabId>('agents');
	let searchQuery = $state('');

	let agents = $state<AgentListEntry[]>([]);
	let locations = $state<LocationListEntry[]>([]);
	let tasksData = $state<TaskListEntry[]>([]);
	let executions = $state<TaskExecutionListEntry[]>([]);

	async function fetchAgents(): Promise<void> {
		const resp = await client().send(new ListAgentsCommand({}));
		agents = resp.Agents ?? [];
	}
	async function fetchLocations(): Promise<void> {
		const resp = await client().send(new ListLocationsCommand({}));
		locations = resp.Locations ?? [];
	}
	async function fetchTasks(): Promise<void> {
		const resp = await client().send(new ListTasksCommand({}));
		tasksData = resp.Tasks ?? [];
	}
	async function fetchExecutions(): Promise<void> {
		const resp = await client().send(new ListTaskExecutionsCommand({}));
		executions = resp.TaskExecutions ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		agents: () => fetchAgents().catch(rethrowDescribed),
		locations: () => fetchLocations().catch(rethrowDescribed),
		tasks: () => fetchTasks().catch(rethrowDescribed),
		executions: () => fetchExecutions().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// Every ARN here is only unique within a region.
	onRegionChange(() => {
		detailModal?.close();
		detailKind = null;
		tabLoader.refresh(untrack(() => activeTab));
	});

	function matches(q: string, ...fields: (string | undefined)[]): boolean {
		if (!q) return true;
		return fields.some((f) => (f ?? '').toLowerCase().includes(q));
	}

	const filteredAgents = $derived(agents.filter((a) => matches(searchQuery.toLowerCase(), a.Name, a.AgentArn, a.Status)));
	const filteredLocations = $derived(locations.filter((l) => matches(searchQuery.toLowerCase(), l.LocationUri, l.LocationArn)));
	const filteredTasks = $derived(tasksData.filter((t) => matches(searchQuery.toLowerCase(), t.Name, t.TaskArn, t.Status)));
	const filteredExecutions = $derived(executions.filter((e) => matches(searchQuery.toLowerCase(), e.TaskExecutionArn, e.Status)));

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Agent create ---

	let createAgentModal = $state<Modal | null>(null);
	let creatingAgent = $state(false);
	let createAgentError = $state<string | null>(null);
	let newAgentActivationKey = $state('');
	let newAgentName = $state('');

	function openCreateAgentModal(): void {
		createAgentError = null;
		newAgentActivationKey = '';
		newAgentName = '';
		createAgentModal?.open();
	}

	async function submitCreateAgent(): Promise<void> {
		if (!newAgentActivationKey) {
			createAgentError = 'Activation key is required.';
			return;
		}
		creatingAgent = true;
		createAgentError = null;
		try {
			await client().send(
				new CreateAgentCommand({ ActivationKey: newAgentActivationKey, AgentName: newAgentName || undefined })
			);
			toast.success('Agent created');
			createAgentModal?.close();
			await tabLoader.refresh('agents');
		} catch (e) {
			const msg = describeError(e);
			createAgentError = msg;
			toast.error(msg);
		} finally {
			creatingAgent = false;
		}
	}

	async function deleteAgent(a: AgentListEntry): Promise<void> {
		if (!a.AgentArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete agent',
			message: `Delete agent "${a.Name ?? a.AgentArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAgentCommand({ AgentArn: a.AgentArn }));
			toast.success('Agent deleted');
			await tabLoader.refresh('agents');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Location create ---

	let createLocationModal = $state<Modal | null>(null);
	let creatingLocation = $state(false);
	let createLocationError = $state<string | null>(null);
	let newLocationType = $state<'s3' | 'nfs' | 'smb' | 'efs'>('s3');
	let newLocationSubdirectory = $state('/');
	let newLocationS3BucketArn = $state('');
	let newLocationS3RoleArn = $state('');
	let newLocationServerHostname = $state('');
	let newLocationAgentArns = $state('');
	let newLocationSmbUser = $state('');
	let newLocationSmbPassword = $state('');
	let newLocationEfsFilesystemArn = $state('');
	let newLocationSubnetArn = $state('');
	let newLocationSecurityGroupArns = $state('');

	function openCreateLocationModal(): void {
		createLocationError = null;
		newLocationType = 's3';
		newLocationSubdirectory = '/';
		newLocationS3BucketArn = '';
		newLocationS3RoleArn = '';
		newLocationServerHostname = '';
		newLocationAgentArns = '';
		newLocationSmbUser = '';
		newLocationSmbPassword = '';
		newLocationEfsFilesystemArn = '';
		newLocationSubnetArn = '';
		newLocationSecurityGroupArns = '';
		createLocationModal?.open();
	}

	async function submitCreateLocation(): Promise<void> {
		creatingLocation = true;
		createLocationError = null;
		try {
			if (newLocationType === 's3') {
				if (!newLocationS3BucketArn || !newLocationS3RoleArn) {
					throw new Error('S3 bucket ARN and bucket access role ARN are required.');
				}
				await client().send(
					new CreateLocationS3Command({
						S3BucketArn: newLocationS3BucketArn,
						Subdirectory: newLocationSubdirectory,
						S3Config: { BucketAccessRoleArn: newLocationS3RoleArn }
					})
				);
			} else if (newLocationType === 'nfs') {
				const agentArns = newLocationAgentArns.split(',').map((s) => s.trim()).filter(Boolean);
				if (!newLocationServerHostname || agentArns.length === 0) {
					throw new Error('Server hostname and at least one agent ARN are required.');
				}
				await client().send(
					new CreateLocationNfsCommand({
						ServerHostname: newLocationServerHostname,
						Subdirectory: newLocationSubdirectory,
						OnPremConfig: { AgentArns: agentArns }
					})
				);
			} else if (newLocationType === 'smb') {
				const agentArns = newLocationAgentArns.split(',').map((s) => s.trim()).filter(Boolean);
				if (!newLocationServerHostname || agentArns.length === 0) {
					throw new Error('Server hostname and at least one agent ARN are required.');
				}
				await client().send(
					new CreateLocationSmbCommand({
						ServerHostname: newLocationServerHostname,
						Subdirectory: newLocationSubdirectory,
						AgentArns: agentArns,
						User: newLocationSmbUser || undefined,
						Password: newLocationSmbPassword || undefined
					})
				);
			} else {
				const securityGroupArns = newLocationSecurityGroupArns.split(',').map((s) => s.trim()).filter(Boolean);
				if (!newLocationEfsFilesystemArn || !newLocationSubnetArn || securityGroupArns.length === 0) {
					throw new Error('EFS filesystem ARN, subnet ARN, and at least one security group ARN are required.');
				}
				await client().send(
					new CreateLocationEfsCommand({
						EfsFilesystemArn: newLocationEfsFilesystemArn,
						Subdirectory: newLocationSubdirectory,
						Ec2Config: { SubnetArn: newLocationSubnetArn, SecurityGroupArns: securityGroupArns }
					})
				);
			}
			toast.success('Location created');
			createLocationModal?.close();
			await tabLoader.refresh('locations');
		} catch (e) {
			const msg = describeError(e);
			createLocationError = msg;
			toast.error(msg);
		} finally {
			creatingLocation = false;
		}
	}

	async function deleteLocation(l: LocationListEntry): Promise<void> {
		if (!l.LocationArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete location',
			message: `Delete location "${l.LocationUri ?? l.LocationArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteLocationCommand({ LocationArn: l.LocationArn }));
			toast.success('Location deleted');
			await tabLoader.refresh('locations');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Task create ---

	let createTaskModal = $state<Modal | null>(null);
	let creatingTask = $state(false);
	let createTaskError = $state<string | null>(null);
	let newTaskName = $state('');
	let newTaskSourceArn = $state('');
	let newTaskDestArn = $state('');

	function openCreateTaskModal(): void {
		createTaskError = null;
		newTaskName = '';
		newTaskSourceArn = '';
		newTaskDestArn = '';
		createTaskModal?.open();
	}

	async function submitCreateTask(): Promise<void> {
		if (!newTaskSourceArn || !newTaskDestArn) {
			createTaskError = 'Source and destination location ARNs are required.';
			return;
		}
		creatingTask = true;
		createTaskError = null;
		try {
			await client().send(
				new CreateTaskCommand({
					Name: newTaskName || undefined,
					SourceLocationArn: newTaskSourceArn,
					DestinationLocationArn: newTaskDestArn
				})
			);
			toast.success('Task created');
			createTaskModal?.close();
			await tabLoader.refresh('tasks');
		} catch (e) {
			const msg = describeError(e);
			createTaskError = msg;
			toast.error(msg);
		} finally {
			creatingTask = false;
		}
	}

	async function deleteTask(t: TaskListEntry): Promise<void> {
		if (!t.TaskArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete task',
			message: `Delete task "${t.Name ?? t.TaskArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteTaskCommand({ TaskArn: t.TaskArn }));
			toast.success('Task deleted');
			await tabLoader.refresh('tasks');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function startExecution(t: TaskListEntry): Promise<void> {
		if (!t.TaskArn) return;
		try {
			await client().send(new StartTaskExecutionCommand({ TaskArn: t.TaskArn }));
			toast.success('Task execution started');
			await tabLoader.refresh('tasks');
			if (tabLoader.isLoaded('executions')) await tabLoader.refresh('executions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Task execution cancel (not delete -- there is no DeleteTaskExecution op) ---

	async function cancelExecution(e: TaskExecutionListEntry): Promise<void> {
		if (!e.TaskExecutionArn) return;
		const confirmed = await confirmDestructive({
			title: 'Cancel task execution',
			message: `Cancel task execution "${e.TaskExecutionArn}"? This stops an in-progress transfer.`,
			confirmLabel: 'Cancel execution'
		});
		if (!confirmed) return;
		try {
			await client().send(new CancelTaskExecutionCommand({ TaskExecutionArn: e.TaskExecutionArn }));
			toast.success('Task execution cancelled');
			await tabLoader.refresh('executions');
		} catch (err) {
			toast.error(describeError(err));
		}
	}

	// --- Detail (per family) ---

	let detailModal = $state<Modal | null>(null);
	let detailKind = $state<'agent' | 'location' | 'task' | 'execution' | null>(null);
	let detailLoading = $state(false);
	let detailError = $state<string | null>(null);
	let viewedAgent = $state<DescribeAgentCommandOutput | null>(null);
	let viewedLocation = $state<{ LocationArn?: string; LocationUri?: string; type: string; raw: unknown } | null>(null);
	let viewedTask = $state<DescribeTaskCommandOutput | null>(null);
	let viewedExecution = $state<DescribeTaskExecutionCommandOutput | null>(null);
	let viewedAgentArn = $state('');
	let viewedTaskArn = $state('');
	let viewedExecutionArn = $state('');

	async function openAgentDetail(a: AgentListEntry): Promise<void> {
		detailKind = 'agent';
		viewedAgent = null;
		detailError = null;
		detailModal?.open();
		if (!a.AgentArn) return;
		viewedAgentArn = a.AgentArn;
		await refreshAgentDetail();
	}
	async function refreshAgentDetail(): Promise<void> {
		if (!viewedAgentArn) return;
		detailLoading = true;
		try {
			viewedAgent = await client().send(new DescribeAgentCommand({ AgentArn: viewedAgentArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openLocationDetail(l: LocationListEntry): Promise<void> {
		detailKind = 'location';
		viewedLocation = null;
		detailError = null;
		detailModal?.open();
		if (!l.LocationArn) return;
		const type = locationType(l.LocationUri);
		detailLoading = true;
		try {
			let raw: unknown = {};
			if (type === 's3') raw = await client().send(new DescribeLocationS3Command({ LocationArn: l.LocationArn }));
			else if (type === 'nfs') raw = await client().send(new DescribeLocationNfsCommand({ LocationArn: l.LocationArn }));
			else if (type === 'smb') raw = await client().send(new DescribeLocationSmbCommand({ LocationArn: l.LocationArn }));
			else if (type === 'efs') raw = await client().send(new DescribeLocationEfsCommand({ LocationArn: l.LocationArn }));
			viewedLocation = { LocationArn: l.LocationArn, LocationUri: l.LocationUri, type, raw };
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openTaskDetail(t: TaskListEntry): Promise<void> {
		detailKind = 'task';
		viewedTask = null;
		detailError = null;
		detailModal?.open();
		if (!t.TaskArn) return;
		viewedTaskArn = t.TaskArn;
		await refreshTaskDetail();
	}
	async function refreshTaskDetail(): Promise<void> {
		if (!viewedTaskArn) return;
		detailLoading = true;
		try {
			viewedTask = await client().send(new DescribeTaskCommand({ TaskArn: viewedTaskArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openExecutionDetail(e: TaskExecutionListEntry): Promise<void> {
		detailKind = 'execution';
		viewedExecution = null;
		detailError = null;
		detailModal?.open();
		if (!e.TaskExecutionArn) return;
		viewedExecutionArn = e.TaskExecutionArn;
		await refreshExecutionDetail();
	}
	async function refreshExecutionDetail(): Promise<void> {
		if (!viewedExecutionArn) return;
		detailLoading = true;
		try {
			viewedExecution = await client().send(
				new DescribeTaskExecutionCommand({ TaskExecutionArn: viewedExecutionArn })
			);
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	function refreshDetail(): void {
		if (detailKind === 'agent') void refreshAgentDetail();
		else if (detailKind === 'task') void refreshTaskDetail();
		else if (detailKind === 'execution') void refreshExecutionDetail();
	}

	// --- Edit (Agent name, Task name/log group) ---

	let editModal = $state<Modal | null>(null);
	let editing = $state(false);
	let editError = $state<string | null>(null);
	let editAgentArn = $state('');
	let editAgentName = $state('');
	let editTaskArn = $state('');
	let editTaskName = $state('');
	let editTaskLogGroupArn = $state('');

	function openEditAgent(): void {
		editError = null;
		editAgentArn = viewedAgentArn;
		editAgentName = viewedAgent?.Name ?? '';
		editModal?.open();
	}

	async function submitEditAgent(): Promise<void> {
		editing = true;
		editError = null;
		try {
			await client().send(new UpdateAgentCommand({ AgentArn: editAgentArn, Name: editAgentName }));
			toast.success('Agent updated');
			editModal?.close();
			await tabLoader.refresh('agents');
			await refreshAgentDetail();
		} catch (e) {
			const msg = describeError(e);
			editError = msg;
			toast.error(msg);
		} finally {
			editing = false;
		}
	}

	function openEditTask(): void {
		editError = null;
		editTaskArn = viewedTaskArn;
		editTaskName = viewedTask?.Name ?? '';
		editTaskLogGroupArn = viewedTask?.CloudWatchLogGroupArn ?? '';
		editModal?.open();
	}

	async function submitEditTask(): Promise<void> {
		editing = true;
		editError = null;
		try {
			await client().send(
				new UpdateTaskCommand({
					TaskArn: editTaskArn,
					Name: editTaskName,
					CloudWatchLogGroupArn: editTaskLogGroupArn || undefined
				})
			);
			toast.success('Task updated');
			editModal?.close();
			await tabLoader.refresh('tasks');
			await refreshTaskDetail();
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
	<PageHeader icon={ArrowLeftRight} title="AWS DataSync" description="Online data transfer service" onRefresh={handleRefresh} color="cyan">
		{#snippet actions()}
			{#if activeTab === 'agents'}
				<button onclick={openCreateAgentModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm">
					<Plus class="w-4 h-4" /> Create agent
				</button>
			{:else if activeTab === 'locations'}
				<button onclick={openCreateLocationModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm">
					<Plus class="w-4 h-4" /> Create location
				</button>
			{:else if activeTab === 'tasks'}
				<button onclick={openCreateTaskModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm">
					<Plus class="w-4 h-4" /> Create task
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="cyan" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'agents'}
				{#snippet agentStatusCell(a: AgentListEntry)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(a.Status)}">{a.Status ?? '—'}</span>
				{/snippet}
				{#snippet agentActionsCell(a: AgentListEntry)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openAgentDetail(a)} title="View" aria-label="View agent {a.Name}" class="text-gray-400 hover:text-cyan-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteAgent(a)} title="Delete" aria-label="Delete agent {a.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const agentColumns = defineColumns<AgentListEntry>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Status', label: 'Status', render: agentStatusCell },
					{ key: 'actions', label: '', render: agentActionsCell }
				])}
				<DataTable rows={filteredAgents} rowKey={(a) => a.AgentArn ?? ''} columns={agentColumns} loading={tabLoader.isLoading('agents')} emptyMessage="No agents found" />
			{:else if activeTab === 'locations'}
				{#snippet locationTypeCell(l: LocationListEntry)}
					<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-slate-700 text-gray-600 dark:text-gray-300 uppercase">{locationType(l.LocationUri)}</span>
				{/snippet}
				{#snippet locationActionsCell(l: LocationListEntry)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openLocationDetail(l)} title="View" aria-label="View location {l.LocationUri}" class="text-gray-400 hover:text-cyan-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteLocation(l)} title="Delete" aria-label="Delete location {l.LocationUri}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const locationColumns = defineColumns<LocationListEntry>([
					{ key: 'LocationUri', label: 'URI' },
					{ key: 'type', label: 'Type', render: locationTypeCell },
					{ key: 'actions', label: '', render: locationActionsCell }
				])}
				<DataTable rows={filteredLocations} rowKey={(l) => l.LocationArn ?? ''} columns={locationColumns} loading={tabLoader.isLoading('locations')} emptyMessage="No locations found" />
			{:else if activeTab === 'tasks'}
				{#snippet taskStatusCell(t: TaskListEntry)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(t.Status)}">{t.Status ?? '—'}</span>
				{/snippet}
				{#snippet taskActionsCell(t: TaskListEntry)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => startExecution(t)} title="Start execution" aria-label="Start execution for task {t.Name}" class="text-gray-400 hover:text-green-500"><Play class="w-4 h-4" /></button>
						<button onclick={() => openTaskDetail(t)} title="View" aria-label="View task {t.Name}" class="text-gray-400 hover:text-cyan-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteTask(t)} title="Delete" aria-label="Delete task {t.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const taskColumns = defineColumns<TaskListEntry>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Status', label: 'Status', render: taskStatusCell },
					{ key: 'actions', label: '', render: taskActionsCell }
				])}
				<DataTable rows={filteredTasks} rowKey={(t) => t.TaskArn ?? ''} columns={taskColumns} loading={tabLoader.isLoading('tasks')} emptyMessage="No tasks found" />
			{:else if activeTab === 'executions'}
				<p class="text-xs text-slate-500 dark:text-slate-400">
					Task executions are started from a task (see the Tasks tab's Start button), not created directly, and
					are stopped via Cancel rather than deleted -- there is no DeleteTaskExecution operation in the real API.
				</p>
				{#snippet executionStatusCell(e: TaskExecutionListEntry)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(e.Status)}">{e.Status ?? '—'}</span>
				{/snippet}
				{#snippet executionActionsCell(e: TaskExecutionListEntry)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openExecutionDetail(e)} title="View" aria-label="View execution {e.TaskExecutionArn}" class="text-gray-400 hover:text-cyan-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => cancelExecution(e)} title="Cancel" aria-label="Cancel execution {e.TaskExecutionArn}" class="text-gray-400 hover:text-red-500"><Ban class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const executionColumns = defineColumns<TaskExecutionListEntry>([
					{ key: 'TaskExecutionArn', label: 'Execution ARN' },
					{ key: 'Status', label: 'Status', render: executionStatusCell },
					{ key: 'actions', label: '', render: executionActionsCell }
				])}
				<DataTable rows={filteredExecutions} rowKey={(e) => e.TaskExecutionArn ?? ''} columns={executionColumns} loading={tabLoader.isLoading('executions')} emptyMessage="No task executions found" />
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createAgentModal} title="Create Agent">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ds-agent-key" class="text-sm text-slate-600 dark:text-slate-300">Activation key</label>
				<input id="ds-agent-key" bind:value={newAgentActivationKey} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ds-agent-name" class="text-sm text-slate-600 dark:text-slate-300">Name (optional)</label>
				<input id="ds-agent-name" bind:value={newAgentName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createAgentError}<p class="text-sm text-red-600 dark:text-red-400">{createAgentError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createAgentModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateAgent} disabled={creatingAgent} class="rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700 disabled:opacity-50">{creatingAgent ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createLocationModal} title="Create Location">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ds-loc-type" class="text-sm text-slate-600 dark:text-slate-300">Location type</label>
				<select id="ds-loc-type" bind:value={newLocationType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="s3">Amazon S3</option>
					<option value="nfs">NFS</option>
					<option value="smb">SMB</option>
					<option value="efs">Amazon EFS</option>
				</select>
			</div>
			<div>
				<label for="ds-loc-subdir" class="text-sm text-slate-600 dark:text-slate-300">Subdirectory</label>
				<input id="ds-loc-subdir" bind:value={newLocationSubdirectory} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if newLocationType === 's3'}
				<div>
					<label for="ds-loc-s3-bucket" class="text-sm text-slate-600 dark:text-slate-300">S3 bucket ARN</label>
					<input id="ds-loc-s3-bucket" bind:value={newLocationS3BucketArn} placeholder="arn:aws:s3:::my-bucket" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="ds-loc-s3-role" class="text-sm text-slate-600 dark:text-slate-300">Bucket access role ARN</label>
					<input id="ds-loc-s3-role" bind:value={newLocationS3RoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{:else if newLocationType === 'nfs' || newLocationType === 'smb'}
				<div>
					<label for="ds-loc-hostname" class="text-sm text-slate-600 dark:text-slate-300">Server hostname</label>
					<input id="ds-loc-hostname" bind:value={newLocationServerHostname} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="ds-loc-agentarns" class="text-sm text-slate-600 dark:text-slate-300">Agent ARNs (comma-separated)</label>
					<input id="ds-loc-agentarns" bind:value={newLocationAgentArns} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				{#if newLocationType === 'smb'}
					<div>
						<label for="ds-loc-smb-user" class="text-sm text-slate-600 dark:text-slate-300">User</label>
						<input id="ds-loc-smb-user" bind:value={newLocationSmbUser} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
					<div>
						<label for="ds-loc-smb-password" class="text-sm text-slate-600 dark:text-slate-300">Password</label>
						<input id="ds-loc-smb-password" type="password" bind:value={newLocationSmbPassword} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
				{/if}
			{:else}
				<div>
					<label for="ds-loc-efs-arn" class="text-sm text-slate-600 dark:text-slate-300">EFS filesystem ARN</label>
					<input id="ds-loc-efs-arn" bind:value={newLocationEfsFilesystemArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="ds-loc-subnet" class="text-sm text-slate-600 dark:text-slate-300">Subnet ARN</label>
					<input id="ds-loc-subnet" bind:value={newLocationSubnetArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="ds-loc-sgs" class="text-sm text-slate-600 dark:text-slate-300">Security group ARNs (comma-separated)</label>
					<input id="ds-loc-sgs" bind:value={newLocationSecurityGroupArns} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			<p class="text-xs text-slate-500 dark:text-slate-400">
				S3/NFS/SMB/EFS are supported here; the backend also implements HDFS/ObjectStorage/AzureBlob/FSx
				Lustre/ONTAP/OpenZFS/Windows locations, not exposed in this create form.
			</p>
			{#if createLocationError}<p class="text-sm text-red-600 dark:text-red-400">{createLocationError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createLocationModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateLocation} disabled={creatingLocation} class="rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700 disabled:opacity-50">{creatingLocation ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createTaskModal} title="Create Task">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ds-task-name" class="text-sm text-slate-600 dark:text-slate-300">Name (optional)</label>
				<input id="ds-task-name" bind:value={newTaskName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ds-task-source" class="text-sm text-slate-600 dark:text-slate-300">Source location</label>
				<select id="ds-task-source" bind:value={newTaskSourceArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a location…</option>
					{#each locations as l (l.LocationArn)}
						<option value={l.LocationArn}>{l.LocationUri}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="ds-task-dest" class="text-sm text-slate-600 dark:text-slate-300">Destination location</label>
				<select id="ds-task-dest" bind:value={newTaskDestArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a location…</option>
					{#each locations as l (l.LocationArn)}
						<option value={l.LocationArn}>{l.LocationUri}</option>
					{/each}
				</select>
			</div>
			{#if createTaskError}<p class="text-sm text-red-600 dark:text-red-400">{createTaskError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createTaskModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateTask} disabled={creatingTask} class="rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700 disabled:opacity-50">{creatingTask ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal
	bind:this={detailModal}
	title={detailKind === 'agent' ? 'Agent' : detailKind === 'location' ? 'Location' : detailKind === 'task' ? 'Task' : 'Task Execution'}
>
	{#snippet children()}
		{#if detailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if detailError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>
		{:else if detailKind === 'agent' && viewedAgent}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedAgent.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedAgent.AgentArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(viewedAgent.Status)}">{viewedAgent.Status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Platform</dt><dd class="text-slate-900 dark:text-white">{viewedAgent.Platform?.Version ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Endpoint type</dt><dd class="text-slate-900 dark:text-white">{viewedAgent.EndpointType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Last connection</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedAgent.LastConnectionTime)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedAgent.CreationTime)}</dd></div>
			</dl>
		{:else if detailKind === 'location' && viewedLocation}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">URI</dt><dd class="break-all text-slate-900 dark:text-white">{viewedLocation.LocationUri ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedLocation.LocationArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white uppercase">{viewedLocation.type}</dd></div>
				{#if viewedLocation.type === 's3' || viewedLocation.type === 'nfs' || viewedLocation.type === 'smb' || viewedLocation.type === 'efs'}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Details</dt>
						<dd class="text-slate-900 dark:text-white">
							<pre class="mt-1 max-h-48 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(viewedLocation.raw, null, 2)}</pre>
						</dd>
					</div>
				{:else}
					<p class="text-xs text-slate-500 dark:text-slate-400">
						This UI does not have a per-type detail reader for "{viewedLocation.type}" locations -- only the
						URI/ARN above (from ListLocations) are shown. The backend does implement Describe for this type.
					</p>
				{/if}
			</dl>
		{:else if detailKind === 'task' && viewedTask}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedTask.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedTask.TaskArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(viewedTask.Status)}">{viewedTask.Status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Source location</dt><dd class="break-all text-slate-900 dark:text-white">{viewedTask.SourceLocationArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Destination location</dt><dd class="break-all text-slate-900 dark:text-white">{viewedTask.DestinationLocationArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Current execution</dt><dd class="break-all text-slate-900 dark:text-white">{viewedTask.CurrentTaskExecutionArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">CloudWatch log group</dt><dd class="break-all text-slate-900 dark:text-white">{viewedTask.CloudWatchLogGroupArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedTask.CreationTime)}</dd></div>
			</dl>
		{:else if detailKind === 'execution' && viewedExecution}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedExecution.TaskExecutionArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(viewedExecution.Status)}">{viewedExecution.Status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Files transferred</dt><dd class="text-slate-900 dark:text-white">{viewedExecution.FilesTransferred ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Bytes transferred</dt><dd class="text-slate-900 dark:text-white">{formatBytes(viewedExecution.BytesTransferred)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Start time</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedExecution.StartTime)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">End time</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedExecution.EndTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if detailKind === 'agent' || detailKind === 'task' || detailKind === 'execution'}
			<button type="button" onclick={refreshDetail} class="flex items-center gap-2 rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"><RefreshCw class="w-4 h-4" /> Refresh</button>
		{/if}
		{#if detailKind === 'agent' && viewedAgent}
			<button type="button" onclick={openEditAgent} class="flex items-center gap-2 rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700"><Pencil class="w-4 h-4" /> Edit</button>
		{:else if detailKind === 'task' && viewedTask}
			<button type="button" onclick={openEditTask} class="flex items-center gap-2 rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700"><Pencil class="w-4 h-4" /> Edit</button>
		{/if}
	{/snippet}
</Modal>

<Modal bind:this={editModal} title={detailKind === 'agent' ? 'Edit Agent' : 'Edit Task'}>
	{#snippet children()}
		{#if detailKind === 'agent'}
			<div class="space-y-3">
				<div>
					<label for="ds-edit-agent-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
					<input id="ds-edit-agent-name" bind:value={editAgentName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
			</div>
		{:else if detailKind === 'task'}
			<div class="space-y-3">
				<div>
					<label for="ds-edit-task-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
					<input id="ds-edit-task-name" bind:value={editTaskName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="ds-edit-task-loggroup" class="text-sm text-slate-600 dark:text-slate-300">CloudWatch log group ARN</label>
					<input id="ds-edit-task-loggroup" bind:value={editTaskLogGroupArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		{#if detailKind === 'agent'}
			<button type="button" onclick={submitEditAgent} disabled={editing} class="rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
		{:else if detailKind === 'task'}
			<button type="button" onclick={submitEditTask} disabled={editing} class="rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
		{/if}
	{/snippet}
</Modal>
