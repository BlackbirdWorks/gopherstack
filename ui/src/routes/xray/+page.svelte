<script lang="ts">
	import { onMount } from 'svelte';
	import { getXRayClient } from '$lib/aws-client';
	import {
		GetServiceGraphCommand,
		GetTraceSummariesCommand,
		GetGroupsCommand,
		BatchGetTracesCommand,
		type TraceSummary,
		type Group,
		type Service,
		type Trace
	} from '@aws-sdk/client-xray';
	import { toast } from 'svelte-sonner';
	import {
		Activity,
		Search,
		RefreshCw,
		Clock,
		AlertCircle,
		Layers,
		Filter,
		Share2,
		X,
		ChevronRight
	} from 'lucide-svelte';

	const xray = getXRayClient();

	let loading = $state(false);
	let activeTab = $state<'traces' | 'groups' | 'service-graph'>('traces');
	let searchQuery = $state('');

	// Traces
	let traceSummaries = $state<TraceSummary[]>([]);
	let errorFilter = $state<'all' | 'error' | 'fault' | 'throttle'>('all');
	let startTime = $state(new Date(Date.now() - 3600000).toISOString().slice(0, 16));
	let endTime = $state(new Date().toISOString().slice(0, 16));

	// Groups
	let groups = $state<Group[]>([]);

	// Service graph
	let serviceGraphNodes = $state<Service[]>([]);
	let serviceGraphStartTime = $state(new Date(Date.now() - 3600000).toISOString().slice(0, 16));
	let serviceGraphEndTime = $state(new Date().toISOString().slice(0, 16));

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

	// Trace detail
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
			const res = await xray.send(new BatchGetTracesCommand({ TraceIds: [traceId] }));
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
			toast.error(`Failed to load trace detail: ${e}`);
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

	async function loadServiceGraph() {
		loading = true;
		try {
			const res = await xray.send(
				new GetServiceGraphCommand({
					StartTime: new Date(serviceGraphStartTime),
					EndTime: new Date(serviceGraphEndTime)
				})
			);
			serviceGraphNodes = res.Services ?? [];
		} catch (e) {
			toast.error(`Failed to load service graph: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'traces') await loadTraces();
		else if (tab === 'groups') await loadGroups();
		else await loadServiceGraph();
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
		{#each [{ id: 'traces', label: 'Traces' }, { id: 'groups', label: 'Groups' }, { id: 'service-graph', label: 'Service Graph' }] as tab}
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
								<td class="px-4 py-3 font-mono text-xs">
									<button onclick={() => openTraceDetail(trace.Id)} class="text-indigo-600 dark:text-indigo-400 hover:underline">{trace.Id}</button>
								</td>
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

	<!-- Service Graph Tab -->
	{#if activeTab === 'service-graph'}
		<div class="flex flex-wrap gap-3 rounded-lg border p-3 bg-muted/20">
			<div class="flex gap-2 items-center text-sm">
				<label for="sg-start" class="font-medium">From:</label>
				<input id="sg-start" type="datetime-local" bind:value={serviceGraphStartTime} class="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
			</div>
			<div class="flex gap-2 items-center text-sm">
				<label for="sg-end" class="font-medium">To:</label>
				<input id="sg-end" type="datetime-local" bind:value={serviceGraphEndTime} class="rounded-md border bg-background px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
			</div>
			<button onclick={loadServiceGraph} disabled={loading} class="flex items-center gap-2 rounded-md bg-primary px-4 py-1.5 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
				<RefreshCw class="h-3.5 w-3.5 {loading ? 'animate-spin' : ''}" /> Load
			</button>
		</div>
		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if serviceGraphNodes.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Share2 class="h-12 w-12 mb-3 opacity-30" />
				<p>No service-graph data for this range</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Service</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-right font-medium">Requests</th>
							<th class="px-4 py-3 text-right font-medium">Faults</th>
							<th class="px-4 py-3 text-right font-medium">Errors</th>
							<th class="px-4 py-3 text-right font-medium">Avg Latency</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each serviceGraphNodes as svc}
							{@const stat = svc.SummaryStatistics}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{svc.Name ?? '—'}</td>
								<td class="px-4 py-3 text-xs text-muted-foreground">{svc.Type ?? '—'}</td>
								<td class="px-4 py-3 text-right text-muted-foreground">{stat?.TotalCount ?? 0}</td>
								<td class="px-4 py-3 text-right text-red-500">{stat?.FaultStatistics?.TotalCount ?? 0}</td>
								<td class="px-4 py-3 text-right text-orange-500">{stat?.ErrorStatistics?.TotalCount ?? 0}</td>
								<td class="px-4 py-3 text-right text-muted-foreground">{stat?.TotalResponseTime !== undefined && stat?.TotalResponseTime !== null ? (stat.TotalResponseTime * 1000).toFixed(0) + 'ms' : '—'}</td>
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

<!-- Trace Detail Modal: segment timeline -->
{#if selectedTrace || traceDetailLoading}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
		<div class="bg-white dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 shadow-xl w-full max-w-3xl max-h-[85vh] overflow-hidden flex flex-col">
			<div class="flex items-center justify-between px-5 py-3 border-b border-slate-100 dark:border-slate-800">
				<div class="flex items-center gap-2">
					<Share2 class="w-4 h-4 text-indigo-500" />
					<h2 class="text-sm font-semibold text-slate-900 dark:text-white">Trace {selectedTrace?.Id ?? ''}</h2>
					{#if selectedTrace?.Duration !== undefined && selectedTrace?.Duration !== null}<span class="text-xs text-slate-400">{selectedTrace.Duration.toFixed(3)}s</span>{/if}
				</div>
				<button onclick={closeTraceDetail} class="text-slate-400 hover:text-slate-600"><X class="w-5 h-5" /></button>
			</div>
			<div class="overflow-y-auto p-5">
				{#if traceDetailLoading}
					<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
				{:else if segmentRows.length === 0}
					<div class="text-center py-10 text-slate-500 text-sm">No segment documents for this trace.</div>
				{:else}
					<div class="space-y-1.5">
						{#each segmentRows as row, i}
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
												{#each row.annotations as [k, v]}
													<div class="flex gap-2"><span class="font-mono text-indigo-600 dark:text-indigo-400">{k}</span><span class="font-mono text-slate-700 dark:text-slate-200 break-all">{v}</span></div>
												{/each}
											</div>
										</div>
									{/if}
									{#if row.metadata.length > 0}
										<div>
											<div class="font-medium text-slate-500 dark:text-slate-400 mb-1">Metadata</div>
											<div class="space-y-0.5">
												{#each row.metadata as [k, v]}
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
