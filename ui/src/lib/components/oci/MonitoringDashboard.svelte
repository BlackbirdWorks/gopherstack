<script lang="ts">
	import { onMount } from 'svelte';
	import { Activity, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import OciSeriesChart from '#lib/components/oci/OciSeriesChart.svelte';
	import { GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		TIME_RANGES,
		getAlarmHistory,
		groupNamespaces,
		listAlarms,
		listCompartments,
		listMetrics,
		readStoredMonitoringCompartment,
		summarizeMetrics,
		writeStoredMonitoringCompartment,
		type AlarmInfo,
		type HistoryEntry,
		type MetricDef,
		type NamespaceInfo,
		type Series
	} from '#lib/ocimonitoring.ts';

	const TABS = [
		{ id: 'metrics', label: 'Metrics' },
		{ id: 'alarms', label: 'Alarms' }
	];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredMonitoringCompartment());
	let metrics = $state<MetricDef[]>([]);
	let alarms = $state<AlarmInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let tab = $state('metrics');
	let namespace = $state('');
	let query = $state('');
	let range = $state('1h');
	let series = $state<Series[] | null>(null);
	let querying = $state(false);
	let queryError = $state('');
	let selectedAlarm = $state('');
	let history = $state<HistoryEntry[]>([]);
	let historyError = $state('');

	const namespaces = $derived<NamespaceInfo[]>(groupNamespaces(metrics));
	const shown = $derived(namespaces.find((n) => n.namespace === namespace));

	async function loadData() {
		if (!compartment) {
			metrics = [];
			alarms = [];
			return;
		}
		try {
			[metrics, alarms] = await Promise.all([listMetrics(compartment), listAlarms(compartment)]);
			error = '';
			if (!namespaces.some((n) => n.namespace === namespace)) namespace = namespaces[0]?.namespace ?? '';
			if (!alarms.some((a) => a.id === selectedAlarm)) selectedAlarm = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await loadHistory();
	}

	async function load() {
		try {
			compartments = await listCompartments();
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await loadData();
		loading = false;
	}

	async function loadHistory() {
		historyError = '';
		if (!selectedAlarm) {
			history = [];
			return;
		}
		try {
			history = await getAlarmHistory(selectedAlarm);
		} catch (e) {
			historyError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredMonitoringCompartment(compartment);
		namespace = '';
		series = null;
		loading = true;
		void loadData();
	}

	function pickMetric(name: string) {
		query = `${name}[1m].mean()`;
	}

	function selectAlarm(a: AlarmInfo) {
		selectedAlarm = selectedAlarm === a.id ? '' : a.id;
		void loadHistory();
	}

	async function run() {
		querying = true;
		queryError = '';
		try {
			const seconds = TIME_RANGES.find((r) => r.id === range)?.seconds ?? 3600;
			series = await summarizeMetrics(compartment, namespace, query, seconds);
		} catch (e) {
			queryError = describeGcpError(e);
			series = null;
		} finally {
			querying = false;
		}
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'OK' || s === 'ACTIVE' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'FIRING' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="monitoring-dashboard">
	<PageHeader icon={Activity} title="Monitoring" description="Oracle Cloud Monitoring: metrics, MQL queries and alarms" color="red" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="monitoring-error">
			<p class="font-medium">Could not load metrics</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if compartments.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="monitoring-empty">No metrics or alarms yet. Post metric data or create an alarm with the OCI SDK or CLI.</p>
	{:else}
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Compartment
			<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="monitoring-compartment">
				{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
			</select>
		</label>
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="monitoring-list-error">{error}</p>{/if}

		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />

		{#if tab === 'metrics'}
			<div class="space-y-3">
				<div class="flex flex-wrap items-center gap-2">
					<label class="text-xs text-gray-500" for="monitoring-namespace">Namespace</label>
					<select id="monitoring-namespace" class="{GCP_INPUT} !w-auto !py-1 text-xs" bind:value={namespace} data-testid="monitoring-namespace">
						{#each namespaces as n (n.namespace)}<option value={n.namespace}>{n.namespace}</option>{/each}
					</select>
				</div>
				{#if shown}
					<div class="flex flex-wrap gap-2" data-testid="monitoring-metrics">
						{#each shown.metrics as m (m)}
							<button class={GCP_MINI} onclick={() => pickMetric(m)} data-testid="monitoring-metric">{m}</button>
						{/each}
					</div>
				{:else}
					<p class="text-sm text-gray-500" data-testid="monitoring-metrics-empty">No metrics in this compartment</p>
				{/if}

				<label class="block text-xs text-gray-500 dark:text-gray-400" for="monitoring-query">MQL query, for example cpu[1m].mean() or cpu[5m]&#123;host = "a"&#125;.groupBy(host).max()</label>
				<input id="monitoring-query" class="{GCP_INPUT} font-mono text-xs" type="text" bind:value={query} data-testid="monitoring-query" />
				<div class="flex flex-wrap items-center gap-2">
					<select class="{GCP_INPUT} !w-auto" bind:value={range} aria-label="Time range" data-testid="monitoring-range">
						{#each TIME_RANGES as r (r.id)}<option value={r.id}>{r.label}</option>{/each}
					</select>
					<button class={GCP_PRIMARY} onclick={run} disabled={querying || !namespace || !query.trim()} data-testid="monitoring-run">Run query</button>
					{#if querying}<RefreshCw class="h-4 w-4 animate-spin text-gray-400" />{/if}
				</div>
				{#if queryError}<p class="text-sm text-red-600" role="alert" data-testid="monitoring-query-error">{queryError}</p>{/if}
				{#if series}
					{#if series.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="monitoring-results-empty">No datapoints in this range</p>
					{:else}
						<OciSeriesChart {series} />
					{/if}
				{/if}
			</div>
		{:else}
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="monitoring-alarms">
					<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Alarm</th><th class={cell}>Status</th><th class={cell}>Severity</th><th class={cell}>Query</th><th class={cell}>Enabled</th><th class={cell}>Last triggered</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each alarms as a (a.id)}
							<tr class={selectedAlarm === a.id ? 'bg-red-50/60 dark:bg-red-950/20' : ''} data-testid="monitoring-alarm">
								<td class="{cell} font-mono text-xs" title={a.id}><button class="underline decoration-dotted" onclick={() => selectAlarm(a)} aria-expanded={selectedAlarm === a.id} data-testid="monitoring-alarm-select">{a.name}</button></td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(a.status || a.state)}" data-testid="monitoring-alarm-status">{a.status || a.state || '—'}</span></td>
								<td class="{cell} text-xs">{a.severity}</td>
								<td class="{cell} font-mono text-xs">{a.query}</td>
								<td class="{cell} text-xs">{a.enabled ? 'Yes' : 'No'}</td>
								<td class="{cell} text-xs">{a.triggered || '—'}</td>
							</tr>
						{:else}
							<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="monitoring-alarms-none">No alarms in this compartment</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
			{#if selectedAlarm}
				<div class="space-y-2 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="monitoring-history">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Alarm history</h3>
					{#if historyError}<p class="text-sm text-red-600" role="alert" data-testid="monitoring-history-error">{historyError}</p>{/if}
					{#if history.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="monitoring-history-empty">No history yet</p>
					{:else}
						<table class="w-full text-xs">
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each history as h (h.timestamp + h.summary)}
									<tr data-testid="monitoring-history-entry"><td class="{cell} font-mono whitespace-nowrap">{h.timestamp}</td><td class={cell}>{h.summary}</td></tr>
								{/each}
							</tbody>
						</table>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}
</div>
