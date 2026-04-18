<script lang="ts">
	import { onMount } from 'svelte';
	import { getCloudWatchLogsClient } from '$lib/aws-client';
	import {
		DescribeLogGroupsCommand,
		DescribeLogStreamsCommand,
		GetLogEventsCommand,
		CreateLogGroupCommand,
		DeleteLogGroupCommand,
		CreateLogStreamCommand,
		PutRetentionPolicyCommand,
		DeleteRetentionPolicyCommand,
		FilterLogEventsCommand,
		type LogGroup,
		type LogStream,
		type FilteredLogEvent
	} from '@aws-sdk/client-cloudwatch-logs';
	import { toast } from 'svelte-sonner';
	import { List, Search, RefreshCw, Plus, Trash2, ChevronRight, FileText, Clock, AlertCircle } from 'lucide-svelte';

	const cwl = getCloudWatchLogsClient();

	let loading = $state(false);
	let activeView = $state<'groups' | 'streams' | 'events'>('groups');
	let searchQuery = $state('');

	// Log Groups
	let logGroups = $state<LogGroup[]>([]);
	let selectedGroup = $state<LogGroup | null>(null);
	let showCreateGroup = $state(false);
	let creatingGroup = $state(false);
	let newGroupName = $state('');
	let newGroupRetention = $state(0);
	let deletingGroup = $state<string | null>(null);

	// Log Streams
	let logStreams = $state<LogStream[]>([]);
	let loadingStreams = $state(false);
	let selectedStream = $state<LogStream | null>(null);
	let showCreateStream = $state(false);
	let creatingStream = $state(false);
	let newStreamName = $state('');

	// Log Events
	let logEvents = $state<FilteredLogEvent[]>([]);
	let loadingEvents = $state(false);
	let filterPattern = $state('');
	let startTime = $state('');
	let endTime = $state('');

	// Retention options
	const retentionOptions = [
		{ label: 'Never expire', value: 0 },
		{ label: '1 day', value: 1 },
		{ label: '3 days', value: 3 },
		{ label: '7 days', value: 7 },
		{ label: '14 days', value: 14 },
		{ label: '30 days', value: 30 },
		{ label: '60 days', value: 60 },
		{ label: '90 days', value: 90 },
		{ label: '120 days', value: 120 },
		{ label: '180 days', value: 180 },
		{ label: '365 days', value: 365 }
	];

	const filteredGroups = $derived(
		logGroups.filter((g) => !searchQuery || (g.logGroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredStreams = $derived(
		logStreams.filter((s) => !searchQuery || (s.logStreamName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function loadLogGroups() {
		loading = true;
		try {
			const resp = await cwl.send(new DescribeLogGroupsCommand({ limit: 50 }));
			logGroups = resp.logGroups ?? [];
		} catch (e) {
			toast.error('Failed to load log groups: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectGroup(group: LogGroup) {
		selectedGroup = group;
		selectedStream = null;
		activeView = 'streams';
		searchQuery = '';
		await loadStreams(group.logGroupName ?? '');
	}

	async function loadStreams(groupName: string) {
		loadingStreams = true;
		try {
			const resp = await cwl.send(new DescribeLogStreamsCommand({
				logGroupName: groupName,
				orderBy: 'LastEventTime',
				descending: true,
				limit: 50
			}));
			logStreams = resp.logStreams ?? [];
		} catch (e) {
			toast.error('Failed to load streams: ' + String(e));
		} finally {
			loadingStreams = false;
		}
	}

	async function selectStream(stream: LogStream) {
		selectedStream = stream;
		activeView = 'events';
		await loadEvents();
	}

	async function loadEvents() {
		if (!selectedGroup || !selectedStream) return;
		loadingEvents = true;
		try {
			const params: { logGroupName: string; logStreamNames: string[]; filterPattern?: string; startTime?: number; endTime?: number } = {
				logGroupName: selectedGroup.logGroupName ?? '',
				logStreamNames: [selectedStream.logStreamName ?? '']
			};
			if (filterPattern) params.filterPattern = filterPattern;
			if (startTime) params.startTime = new Date(startTime).getTime();
			if (endTime) params.endTime = new Date(endTime).getTime();
			const resp = await cwl.send(new FilterLogEventsCommand(params));
			logEvents = resp.events ?? [];
		} catch (e) {
			toast.error('Failed to load events: ' + String(e));
		} finally {
			loadingEvents = false;
		}
	}

	async function createLogGroup() {
		if (!newGroupName.trim()) return;
		creatingGroup = true;
		try {
			await cwl.send(new CreateLogGroupCommand({ logGroupName: newGroupName.trim() }));
			if (newGroupRetention > 0) {
				await cwl.send(new PutRetentionPolicyCommand({
					logGroupName: newGroupName.trim(),
					retentionInDays: newGroupRetention
				}));
			}
			toast.success(`Log group "${newGroupName}" created`);
			showCreateGroup = false;
			newGroupName = '';
			newGroupRetention = 0;
			await loadLogGroups();
		} catch (e) {
			toast.error('Failed to create log group: ' + String(e));
		} finally {
			creatingGroup = false;
		}
	}

	async function deleteLogGroup(name: string) {
		if (!confirm(`Delete log group "${name}"?`)) return;
		deletingGroup = name;
		try {
			await cwl.send(new DeleteLogGroupCommand({ logGroupName: name }));
			toast.success(`Log group "${name}" deleted`);
			if (selectedGroup?.logGroupName === name) {
				selectedGroup = null;
				activeView = 'groups';
			}
			await loadLogGroups();
		} catch (e) {
			toast.error('Failed to delete log group: ' + String(e));
		} finally {
			deletingGroup = null;
		}
	}

	async function createLogStream() {
		if (!newStreamName.trim() || !selectedGroup) return;
		creatingStream = true;
		try {
			await cwl.send(new CreateLogStreamCommand({
				logGroupName: selectedGroup.logGroupName ?? '',
				logStreamName: newStreamName.trim()
			}));
			toast.success(`Log stream "${newStreamName}" created`);
			showCreateStream = false;
			newStreamName = '';
			await loadStreams(selectedGroup.logGroupName ?? '');
		} catch (e) {
			toast.error('Failed to create log stream: ' + String(e));
		} finally {
			creatingStream = false;
		}
	}

	async function updateRetention(groupName: string, days: number) {
		try {
			if (days === 0) {
				await cwl.send(new DeleteRetentionPolicyCommand({ logGroupName: groupName }));
			} else {
				await cwl.send(new PutRetentionPolicyCommand({ logGroupName: groupName, retentionInDays: days }));
			}
			toast.success('Retention policy updated');
			await loadLogGroups();
		} catch (e) {
			toast.error('Failed to update retention: ' + String(e));
		}
	}

	function formatBytes(bytes: number | undefined): string {
		if (!bytes) return '0 B';
		if (bytes < 1024) return `${bytes} B`;
		if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`;
		if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(1)} MB`;
		return `${(bytes / 1073741824).toFixed(1)} GB`;
	}

	function formatTimestamp(ms: number | undefined): string {
		if (!ms) return '-';
		return new Date(ms).toLocaleString();
	}

	onMount(loadLogGroups);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<List class="w-7 h-7 text-indigo-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">CloudWatch Logs</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Monitor and query log data</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={activeView === 'groups' ? loadLogGroups : () => {
					if (selectedGroup) loadStreams(selectedGroup.logGroupName ?? '');
				}}
				class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
			>
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			{#if activeView === 'groups'}
				<button
					onclick={() => (showCreateGroup = true)}
					class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium"
				>
					<Plus class="w-4 h-4" /> Create Log Group
				</button>
			{:else if activeView === 'streams' && selectedGroup}
				<button
					onclick={() => (showCreateStream = true)}
					class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium"
				>
					<Plus class="w-4 h-4" /> Create Stream
				</button>
			{/if}
		</div>
	</div>

	<!-- Breadcrumb -->
	{#if activeView !== 'groups'}
		<nav class="flex items-center gap-2 text-sm">
			<button onclick={() => { activeView = 'groups'; selectedGroup = null; selectedStream = null; searchQuery = ''; }} class="text-indigo-600 hover:underline">Log Groups</button>
			{#if selectedGroup}
				<ChevronRight class="w-4 h-4 text-gray-400" />
				<button onclick={() => { activeView = 'streams'; selectedStream = null; searchQuery = ''; }} class="text-indigo-600 hover:underline truncate max-w-xs">{selectedGroup.logGroupName}</button>
			{/if}
			{#if selectedStream && activeView === 'events'}
				<ChevronRight class="w-4 h-4 text-gray-400" />
				<span class="text-gray-600 dark:text-gray-300 truncate max-w-xs">{selectedStream.logStreamName}</span>
			{/if}
		</nav>
	{/if}

	<!-- Search -->
	{#if activeView !== 'events'}
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input
				bind:value={searchQuery}
				type="text"
				placeholder={activeView === 'groups' ? 'Search log groups...' : 'Search streams...'}
				class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white"
			/>
		</div>
	{/if}

	<!-- LOG GROUPS VIEW -->
	{#if activeView === 'groups'}
		{#if loading}
			<div class="flex justify-center py-12">
				<div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div>
			</div>
		{:else if filteredGroups.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<List class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No log groups found</p>
				<p class="text-sm mt-1">Create a log group to get started</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Name</th>
							<th class="px-4 py-3 text-left">Retention</th>
							<th class="px-4 py-3 text-left">Size</th>
							<th class="px-4 py-3 text-left">Created</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredGroups as group}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button
										onclick={() => selectGroup(group)}
										class="text-indigo-600 dark:text-indigo-400 hover:underline font-mono text-xs"
									>{group.logGroupName}</button>
								</td>
								<td class="px-4 py-3">
									<select
										value={group.retentionInDays ?? 0}
										onchange={(e) => updateRetention(group.logGroupName ?? '', parseInt((e.target as HTMLSelectElement, 10).value))}
										class="text-xs border border-gray-200 dark:border-gray-700 rounded px-2 py-1 bg-white dark:bg-gray-900 text-gray-700 dark:text-gray-300"
									>
										{#each retentionOptions as opt}
											<option value={opt.value}>{opt.label}</option>
										{/each}
									</select>
								</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{formatBytes(group.storedBytes)}</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{formatTimestamp(group.creationTime)}</td>
								<td class="px-4 py-3">
									<button
										onclick={() => deleteLogGroup(group.logGroupName ?? '')}
										disabled={deletingGroup === group.logGroupName}
										class="text-red-500 hover:text-red-700 p-1"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- LOG STREAMS VIEW -->
	{#if activeView === 'streams'}
		{#if loadingStreams}
			<div class="flex justify-center py-12">
				<div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div>
			</div>
		{:else if filteredStreams.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<FileText class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No log streams found</p>
				<p class="text-sm mt-1">Create a stream or check the group name</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Stream Name</th>
							<th class="px-4 py-3 text-left">Last Event</th>
							<th class="px-4 py-3 text-left">Last Ingestion</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredStreams as stream}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button
										onclick={() => selectStream(stream)}
										class="text-indigo-600 dark:text-indigo-400 hover:underline font-mono text-xs"
									>{stream.logStreamName}</button>
								</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-xs">{formatTimestamp(stream.lastEventTimestamp)}</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-xs">{formatTimestamp(stream.lastIngestionTime)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- LOG EVENTS VIEW -->
	{#if activeView === 'events'}
		<div class="flex flex-wrap gap-3 items-end">
			<div>
				<label for="filter-pattern" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Filter Pattern</label>
				<input
					id="filter-pattern"
					bind:value={filterPattern}
					type="text"
					placeholder="e.g. ERROR"
					class="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="start-time" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Start Time</label>
				<input
					id="start-time"
					bind:value={startTime}
					type="datetime-local"
					class="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="end-time" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">End Time</label>
				<input
					id="end-time"
					bind:value={endTime}
					type="datetime-local"
					class="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white"
				/>
			</div>
			<button
				onclick={loadEvents}
				class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium"
			>
				<Search class="w-4 h-4" /> Search Events
			</button>
		</div>

		{#if loadingEvents}
			<div class="flex justify-center py-12">
				<div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div>
			</div>
		{:else if logEvents.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<AlertCircle class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No events found</p>
				<p class="text-sm mt-1">Try adjusting the time range or filter pattern</p>
			</div>
		{:else}
			<div class="bg-gray-900 rounded-xl border border-gray-700 p-4 font-mono text-xs space-y-1 max-h-96 overflow-y-auto">
				{#each logEvents as event}
					<div class="flex gap-3">
						<span class="text-gray-500 shrink-0">{formatTimestamp(event.timestamp)}</span>
						<span class="text-green-400 whitespace-pre-wrap break-all">{event.message}</span>
					</div>
				{/each}
			</div>
			<p class="text-xs text-gray-500 dark:text-gray-400">{logEvents.length} events shown</p>
		{/if}
	{/if}
</div>

<!-- Create Log Group Modal -->
{#if showCreateGroup}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Log Group</h2>
			<div>
				<label for="group-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Group Name</label>
				<input
					id="group-name"
					bind:value={newGroupName}
					type="text"
					placeholder="/aws/lambda/my-function"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm"
				/>
			</div>
			<div>
				<label for="retention" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Retention Period</label>
				<select
					id="retention"
					bind:value={newGroupRetention}
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm"
				>
					{#each retentionOptions as opt}
						<option value={opt.value}>{opt.label}</option>
					{/each}
				</select>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateGroup = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button
					onclick={createLogGroup}
					disabled={creatingGroup || !newGroupName.trim()}
					class="flex-1 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 disabled:opacity-50"
				>
					{creatingGroup ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Log Stream Modal -->
{#if showCreateStream}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Log Stream</h2>
			<p class="text-sm text-gray-500 dark:text-gray-400">In: <span class="font-mono">{selectedGroup?.logGroupName}</span></p>
			<div>
				<label for="stream-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Stream Name</label>
				<input
					id="stream-name"
					bind:value={newStreamName}
					type="text"
					placeholder="my-stream"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm"
				/>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateStream = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button
					onclick={createLogStream}
					disabled={creatingStream || !newStreamName.trim()}
					class="flex-1 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 disabled:opacity-50"
				>
					{creatingStream ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}
