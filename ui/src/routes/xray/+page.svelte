<script lang="ts">
	import { onMount } from 'svelte';
	import { getXRayClient } from '$lib/aws-client';
	import {
		GetServiceGraphCommand,
		GetTraceSummariesCommand,
		GetGroupsCommand,
		BatchGetTracesCommand,
		type TraceSummary,
		type Group
	} from '@aws-sdk/client-xray';
	import { toast } from 'svelte-sonner';
	import {
		Activity,
		Search,
		RefreshCw,
		Eye,
		Clock,
		AlertCircle,
		Layers,
		Filter
	} from 'lucide-svelte';

	const xray = getXRayClient();

	let loading = $state(false);
	let activeTab = $state<'traces' | 'groups'>('traces');
	let searchQuery = $state('');

	// Traces
	let traceSummaries = $state<TraceSummary[]>([]);
	let errorFilter = $state<'all' | 'error' | 'fault' | 'throttle'>('all');
	let startTime = $state(new Date(Date.now() - 3600000).toISOString().slice(0, 16));
	let endTime = $state(new Date().toISOString().slice(0, 16));

	// Groups
	let groups = $state<Group[]>([]);

	const filteredTraces = $derived(
		traceSummaries.filter((t) => {
			const text =
				(t.Id ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(t.Http?.HttpURL ?? '').toLowerCase().includes(searchQuery.toLowerCase());
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

	async function loadTraces() {
		loading = true;
		try {
			const start = new Date(startTime).getTime() / 1000;
			const end = new Date(endTime).getTime() / 1000;
			const res = await xray.send(
				new GetTraceSummariesCommand({
					StartTime: new Date(startTime),
					EndTime: new Date(endTime),
					Sampling: false
				})
			);
			traceSummaries = res.TraceSummaries ?? [];
		} catch (e) {
			toast.error(`Failed to load traces: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadGroups() {
		loading = true;
		try {
			const res = await xray.send(new GetGroupsCommand({}));
			groups = res.Groups ?? [];
		} catch (e) {
			toast.error(`Failed to load groups: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'traces') await loadTraces();
		else await loadGroups();
	}

	// Stats
	const totalTraces = $derived(traceSummaries.length);
	const faultTraces = $derived(traceSummaries.filter((t) => t.HasFault).length);
	const errorTraces = $derived(traceSummaries.filter((t) => t.HasError).length);
	const avgDuration = $derived(
		traceSummaries.length > 0
			? (
					traceSummaries.reduce((sum, t) => sum + (t.Duration ?? 0), 0) / traceSummaries.length
				).toFixed(3)
			: '—'
	);

	onMount(() => loadTraces());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg">
				<Activity class="h-6 w-6 text-indigo-600 dark:text-indigo-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">AWS X-Ray</h1>
				<p class="text-slate-600 dark:text-slate-300">Distributed tracing for application analysis</p>
			</div>
		</div>
		<button
			onclick={() => onTabChange(activeTab)}
			class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300"
		>
			<RefreshCw class="h-4 w-4" />
		</button>
	</div>

	<!-- Summary Cards (always shown) -->
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
				<Filter class="w-5 h-5 text-orange-500 dark:text-orange-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-orange-500 dark:text-orange-400">{errorTraces}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Errors</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Clock class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-blue-600 dark:text-blue-400">{avgDuration}s</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Avg Duration</p>
			</div>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'traces', label: 'Traces' }, { id: 'groups', label: 'Groups' }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Traces Tab -->
	{#if activeTab === 'traces'}
		<div class="flex flex-wrap gap-3 rounded-lg border p-3 bg-muted/20">
			<div class="flex gap-2 items-center text-sm">
				<label for="start-time" class="font-medium">From:</label>
				<input
					id="start-time"
					type="datetime-local"
					bind:value={startTime}
					class="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<div class="flex gap-2 items-center text-sm">
				<label for="end-time" class="font-medium">To:</label>
				<input
					id="end-time"
					type="datetime-local"
					bind:value={endTime}
					class="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<select
				bind:value={errorFilter}
				class="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			>
				<option value="all">All Traces</option>
				<option value="error">Errors Only</option>
				<option value="fault">Faults Only</option>
				<option value="throttle">Throttles Only</option>
			</select>
			<button
				onclick={loadTraces}
				disabled={loading}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-1.5 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
			>
				<RefreshCw class="h-3.5 w-3.5 {loading ? 'animate-spin' : ''}" />
				Search
			</button>
		</div>

		<div class="relative">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Filter by trace ID or URL..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredTraces.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Activity class="h-12 w-12 mb-3 opacity-30" />
				<p>No traces found</p>
				<p class="text-sm">Adjust the time range or filters</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-left font-medium">Trace ID</th>
							<th class="px-4 py-3 text-left font-medium">Duration</th>
							<th class="px-4 py-3 text-left font-medium">HTTP</th>
							<th class="px-4 py-3 text-left font-medium">Time</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredTraces as trace}
							{@const status = statusIndicator(trace)}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3">
									<span class="text-xs font-medium {status.color}">{status.label}</span>
								</td>
								<td class="px-4 py-3 font-mono text-xs text-muted-foreground">{trace.Id}</td>
								<td class="px-4 py-3 text-muted-foreground">
									{trace.Duration != null ? trace.Duration.toFixed(3) + 's' : '—'}
								</td>
								<td class="px-4 py-3 text-xs text-muted-foreground">
									{#if trace.Http}
										<span class="font-medium">{trace.Http.HttpStatus ?? ''}</span>
										<span class="ml-1 truncate max-w-[200px] block">{trace.Http.HttpURL ?? ''}</span>
									{:else}
										—
									{/if}
								</td>
								<td class="px-4 py-3 text-xs text-muted-foreground">
									{trace.ResponseTime != null ? (trace.ResponseTime * 1000).toFixed(0) + 'ms' : '—'}
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
		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if groups.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Layers class="h-12 w-12 mb-3 opacity-30" />
				<p>No groups found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Group Name</th>
							<th class="px-4 py-3 text-left font-medium">Filter Expression</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each groups as group}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{group.GroupName}</td>
								<td class="px-4 py-3 font-mono text-xs text-muted-foreground">{group.FilterExpression ?? '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
