<script lang="ts">
	import { onMount } from 'svelte';
	import { getCloudWatchClient } from '$lib/aws-client';
	import {
		DescribeAlarmsCommand,
		PutMetricAlarmCommand,
		DeleteAlarmsCommand,
		ListMetricsCommand,
		ListDashboardsCommand,
		PutDashboardCommand,
		DeleteDashboardsCommand,
		type MetricAlarm,
		type DashboardEntry,
		type Metric
	} from '@aws-sdk/client-cloudwatch';
	import { toast } from 'svelte-sonner';
	import { Activity, Search, RefreshCw, Plus, Trash2, Bell, BarChart2, Layout } from 'lucide-svelte';

	const cw = getCloudWatchClient();
	type PutMetricAlarmInput = ConstructorParameters<typeof PutMetricAlarmCommand>[0];

	let loading = $state(false);
	let activeTab = $state<'alarms' | 'metrics' | 'dashboards'>('alarms');
	let searchQuery = $state('');

	// Alarms
	let alarms = $state<MetricAlarm[]>([]);
	let showCreateAlarm = $state(false);
	let creatingAlarm = $state(false);
	let newAlarmName = $state('');
	let newMetricName = $state('');
	let newNamespace = $state('AWS/EC2');
	let newThreshold = $state(80);
	let newComparisonOperator = $state<NonNullable<PutMetricAlarmInput['ComparisonOperator']>>('GreaterThanThreshold');
	let newEvaluationPeriods = $state(1);
	let newPeriod = $state(300);
	let newStatistic = $state<NonNullable<PutMetricAlarmInput['Statistic']>>('Average');

	// Metrics
	let metrics = $state<Metric[]>([]);
	let metricsSearch = $state('');

	// Dashboards
	let dashboards = $state<DashboardEntry[]>([]);
	let showCreateDashboard = $state(false);
	let creatingDashboard = $state(false);
	let newDashboardName = $state('');

	const filteredAlarms = $derived(
		alarms.filter(
			(a) =>
				a.AlarmName?.toLowerCase().includes(searchQuery.toLowerCase()) ||
				a.Namespace?.toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const groupedMetrics = $derived(() => {
		const groups: Record<string, Metric[]> = {};
		for (const m of metrics) {
			if (!m.Namespace) continue;
			if (!groups[m.Namespace]) groups[m.Namespace] = [];
			if (
				!metricsSearch ||
				m.MetricName?.toLowerCase().includes(metricsSearch.toLowerCase()) ||
				m.Namespace?.toLowerCase().includes(metricsSearch.toLowerCase())
			) {
				groups[m.Namespace].push(m);
			}
		}
		return groups;
	});

	function stateColor(state: string | undefined): string {
		if (state === 'ALARM') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
		if (state === 'OK') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
	}

	async function loadData(tab: 'alarms' | 'metrics' | 'dashboards' = activeTab) {
		loading = true;
		try {
			if (tab === 'alarms') {
				const res = await cw.send(new DescribeAlarmsCommand({ MaxRecords: 100 }));
				alarms = res.MetricAlarms ?? [];
			} else if (tab === 'metrics') {
				const res = await cw.send(new ListMetricsCommand({}));
				metrics = res.Metrics ?? [];
			} else {
				const res = await cw.send(new ListDashboardsCommand({}));
				dashboards = res.DashboardEntries ?? [];
			}
		} catch (err: unknown) {
			toast.error(`Failed to load ${tab}: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadAlarms() {
		loading = true;
		try {
			const res = await cw.send(new DescribeAlarmsCommand({ MaxRecords: 100 }));
			alarms = res.MetricAlarms ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load alarms: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createAlarm() {
		if (!newAlarmName.trim() || !newMetricName.trim()) return;
		creatingAlarm = true;
		try {
			await cw.send(
				new PutMetricAlarmCommand({
					AlarmName: newAlarmName.trim(),
					MetricName: newMetricName.trim(),
					Namespace: newNamespace.trim(),
					Threshold: newThreshold,
					ComparisonOperator: newComparisonOperator,
					EvaluationPeriods: newEvaluationPeriods,
					Period: newPeriod,
					Statistic: newStatistic
				})
			);
			toast.success(`Alarm "${newAlarmName}" created`);
			showCreateAlarm = false;
			newAlarmName = '';
			newMetricName = '';
			await loadAlarms();
		} catch (err: unknown) {
			toast.error(`Create alarm failed: ${(err as Error).message}`);
		} finally {
			creatingAlarm = false;
		}
	}

	async function deleteAlarm(name: string) {
		if (!confirm(`Delete alarm "${name}"?`)) return;
		try {
			await cw.send(new DeleteAlarmsCommand({ AlarmNames: [name] }));
			toast.success(`Alarm "${name}" deleted`);
			await loadAlarms();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function createDashboard() {
		if (!newDashboardName.trim()) return;
		creatingDashboard = true;
		try {
			await cw.send(new PutDashboardCommand({
				DashboardName: newDashboardName.trim(),
				DashboardBody: JSON.stringify({ widgets: [] })
			}));
			toast.success(`Dashboard "${newDashboardName}" created`);
			showCreateDashboard = false;
			newDashboardName = '';
			const res = await cw.send(new ListDashboardsCommand({}));
			dashboards = res.DashboardEntries ?? [];
		} catch (err: unknown) {
			toast.error(`Create dashboard failed: ${(err as Error).message}`);
		} finally {
			creatingDashboard = false;
		}
	}

	async function deleteDashboard(name: string) {
		if (!confirm(`Delete dashboard "${name}"?`)) return;
		try {
			await cw.send(new DeleteDashboardsCommand({ DashboardNames: [name] }));
			toast.success(`Dashboard "${name}" deleted`);
			const res = await cw.send(new ListDashboardsCommand({}));
			dashboards = res.DashboardEntries ?? [];
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	$effect(() => {
		void loadData();
	});

	onMount(() => { loadAlarms(); });
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Activity class="w-6 h-6 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">CloudWatch</h1>
				<p class="text-slate-600 dark:text-slate-300">Monitoring and observability</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={() => loadData()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
			{#if activeTab === 'alarms'}
				<button onclick={() => { showCreateAlarm = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
					<Plus class="w-4 h-4" />Create Alarm
				</button>
			{:else if activeTab === 'dashboards'}
				<button onclick={() => { showCreateDashboard = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
					<Plus class="w-4 h-4" />Create Dashboard
				</button>
			{/if}
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex border-b border-slate-200 dark:border-slate-700">
		{#each [{ id: 'alarms', label: 'Alarms', icon: Bell }, { id: 'metrics', label: 'Metrics', icon: BarChart2 }, { id: 'dashboards', label: 'Dashboards', icon: Layout }] as tab}
			<button
				onclick={() => {
					const nextTab = tab.id as 'alarms' | 'metrics' | 'dashboards';
					activeTab = nextTab;
					void loadData(nextTab);
				}}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors {activeTab === tab.id ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
			>
				<tab.icon class="w-4 h-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Search (alarms only) -->
	{#if activeTab === 'alarms'}
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input type="text" bind:value={searchQuery} placeholder="Search alarms..." class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
		</div>
	{/if}

	{#if loading}
		<div class="text-center py-16">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
			<p class="text-slate-500 dark:text-slate-400">Loading...</p>
		</div>
	{:else if activeTab === 'alarms'}
		{#if filteredAlarms.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Bell class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
				<p class="text-slate-500 dark:text-slate-400">No alarms found</p>
			</div>
		{:else}
			<div class="space-y-2">
				{#each filteredAlarms as alarm}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-4">
						<Bell class="w-5 h-5 text-orange-500 flex-shrink-0" />
						<div class="flex-1 min-w-0">
							<p class="font-medium text-slate-900 dark:text-white">{alarm.AlarmName}</p>
							<p class="text-xs text-slate-500 dark:text-slate-400">
								{alarm.Namespace} / {alarm.MetricName} · {alarm.ComparisonOperator?.replace(/([A-Z])/g, ' $1').trim()} {alarm.Threshold}
							</p>
						</div>
						<span class="px-2 py-0.5 text-xs rounded-full {stateColor(alarm.StateValue)}">{alarm.StateValue}</span>
						<span class="text-sm text-slate-500 dark:text-slate-400">{alarm.Period}s / {alarm.EvaluationPeriods}p</span>
						<button onclick={() => deleteAlarm(alarm.AlarmName ?? '')} class="p-1 text-slate-400 hover:text-red-500">
							<Trash2 class="w-4 h-4" />
						</button>
					</div>
				{/each}
			</div>
		{/if}
	{:else if activeTab === 'metrics'}
		<div class="relative mb-4">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input type="text" bind:value={metricsSearch} placeholder="Filter metrics..." class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
		</div>
		{#if Object.keys(groupedMetrics()).length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<BarChart2 class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
				<p class="text-slate-500 dark:text-slate-400">No metrics found</p>
			</div>
		{:else}
			<div class="space-y-3">
				{#each Object.entries(groupedMetrics()) as [ns, nsMetrics]}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
						<h3 class="font-semibold text-slate-900 dark:text-white mb-2 flex items-center gap-2">
							<BarChart2 class="w-4 h-4 text-indigo-500" />
							{ns} <span class="text-sm font-normal text-slate-500 dark:text-slate-400">({nsMetrics.length})</span>
						</h3>
						<div class="flex flex-wrap gap-1">
							{#each nsMetrics.slice(0, 20) as m}
								<span class="px-2 py-1 text-xs bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded">{m.MetricName}</span>
							{/each}
							{#if nsMetrics.length > 20}
								<span class="px-2 py-1 text-xs text-slate-400">+{nsMetrics.length - 20} more</span>
							{/if}
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{:else}
		{#if dashboards.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Layout class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
				<p class="text-slate-500 dark:text-slate-400">No dashboards found</p>
			</div>
		{:else}
			<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each dashboards as dash}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center justify-between">
						<div>
							<p class="font-medium text-slate-900 dark:text-white">{dash.DashboardName}</p>
							<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
								{dash.LastModified ? new Date(dash.LastModified).toLocaleDateString() : 'N/A'}
							</p>
						</div>
						<button onclick={() => deleteDashboard(dash.DashboardName ?? '')} class="p-1.5 text-slate-400 hover:text-red-500">
							<Trash2 class="w-4 h-4" />
						</button>
					</div>
				{/each}
			</div>
		{/if}
	{/if}
</div>

<!-- Create Alarm Modal -->
{#if showCreateAlarm}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg overflow-y-auto max-h-screen">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Metric Alarm</h2>
			<form onsubmit={(e) => { e.preventDefault(); createAlarm(); }} class="space-y-4">
				<div>
					<label for="cw-alarm-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Alarm Name</label>
					<input id="cw-alarm-name" type="text" bind:value={newAlarmName} placeholder="e.g. high-cpu-alarm" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="cw-metric-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Metric Name</label>
						<input id="cw-metric-name" type="text" bind:value={newMetricName} placeholder="e.g. CPUUtilization" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
					</div>
					<div>
						<label for="cw-namespace" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Namespace</label>
						<input id="cw-namespace" type="text" bind:value={newNamespace} placeholder="e.g. AWS/EC2" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="cw-threshold" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Threshold</label>
						<input id="cw-threshold" type="number" bind:value={newThreshold} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
					<div>
						<label for="cw-statistic" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Statistic</label>
						<select id="cw-statistic" bind:value={newStatistic} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
							{#each ['Average', 'Sum', 'Maximum', 'Minimum', 'SampleCount'] as s}
								<option value={s}>{s}</option>
							{/each}
						</select>
					</div>
				</div>
				<div>
					<label for="cw-comparison-op" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Comparison Operator</label>
					<select id="cw-comparison-op" bind:value={newComparisonOperator} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
						{#each ['GreaterThanThreshold', 'GreaterThanOrEqualToThreshold', 'LessThanThreshold', 'LessThanOrEqualToThreshold'] as op}
							<option value={op}>{op.replace(/([A-Z])/g, ' $1').trim()}</option>
						{/each}
					</select>
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="cw-eval-periods" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Evaluation Periods</label>
						<input id="cw-eval-periods" type="number" bind:value={newEvaluationPeriods} min="1" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
					<div>
						<label for="cw-period" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Period (seconds)</label>
						<input id="cw-period" type="number" bind:value={newPeriod} min="60" step="60" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateAlarm = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingAlarm} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingAlarm ? 'Creating...' : 'Create Alarm'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Dashboard Modal -->
{#if showCreateDashboard}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Dashboard</h2>
			<form onsubmit={(e) => { e.preventDefault(); createDashboard(); }} class="space-y-4">
				<div>
					<label for="cw-dashboard-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Dashboard Name</label>
					<input id="cw-dashboard-name" type="text" bind:value={newDashboardName} placeholder="e.g. production-overview" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateDashboard = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingDashboard} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingDashboard ? 'Creating...' : 'Create Dashboard'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
