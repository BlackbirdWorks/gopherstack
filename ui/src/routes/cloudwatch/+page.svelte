<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { currentRegion, isAllRegions } from '$lib/region.svelte';
	import { multiRegionList } from '$lib/multi-region';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
	import { getCloudWatchClient, getCloudWatchLogsClient } from '$lib/aws-client';
	import {
		DescribeAlarmsCommand,
		DescribeAlarmHistoryCommand,
		PutMetricAlarmCommand,
		DeleteAlarmsCommand,
		ListMetricsCommand,
		ListDashboardsCommand,
		PutDashboardCommand,
		DeleteDashboardsCommand,
		ListMetricStreamsCommand,
		PutMetricStreamCommand,
		DeleteMetricStreamCommand,
		DescribeAnomalyDetectorsCommand,
		DeleteAnomalyDetectorCommand,
		PutAnomalyDetectorCommand,
		EnableAlarmActionsCommand,
		DisableAlarmActionsCommand,
		SetAlarmStateCommand,
		GetMetricStatisticsCommand,
		type MetricAlarm,
		type DashboardEntry,
		type Metric,
		type MetricStreamEntry,
		type AnomalyDetector,
		type AlarmHistoryItem,
		type Datapoint
	} from '@aws-sdk/client-cloudwatch';
	import {
		DescribeMetricFiltersCommand,
		DeleteMetricFilterCommand,
		PutMetricFilterCommand,
		type MetricFilter
	} from '@aws-sdk/client-cloudwatch-logs';
	import { toast } from 'svelte-sonner';
	import { Activity, Search, RefreshCw, Plus, Trash2, Bell, BarChart2, Layout, Filter, Radio, ScanLine, Edit2, ToggleLeft, ToggleRight, Clock, ChevronDown, ChevronUp } from 'lucide-svelte';

	const cw = regionalClient(getCloudWatchClient);
	const cwLogs = regionalClient(getCloudWatchLogsClient);
	type PutMetricAlarmInput = ConstructorParameters<typeof PutMetricAlarmCommand>[0];
	type TabId = 'alarms' | 'metrics' | 'dashboards' | 'streams' | 'anomaly' | 'filters';

	// Every row carries the region its List/Describe call was made against.
	// Row actions (delete/edit) must use THIS region, not the page's shared
	// `cw()`/`cwLogs()` clients -- in All mode the same resource name can
	// legitimately exist in two different regions.
	type Regioned<T> = T & { region: string };

	// Every load* function below goes through this: in single-region mode
	// that's exactly one call (unchanged behavior), and in All mode it fans
	// out across every region with data, tagging each row.
	async function loadRegioned<TResponse, TItem>(
		label: string,
		regionCall: (region: string) => Promise<TResponse>,
		extractItems: (r: TResponse) => TItem[]
	): Promise<Regioned<TItem>[]> {
		const result = await multiRegionList(regionCall, extractItems);
		if (result.errors.length > 0) toast.error(`Failed to load ${label} from ${result.errors.length} region(s)`);
		return result.items.map(({ region, item }) => ({ ...item, region }) as Regioned<TItem>);
	}

	let loading = $state(false);
	let activeTab = $state<TabId>('alarms');
	let searchQuery = $state('');

	// Alarms
	let alarms = $state<Regioned<MetricAlarm>[]>([]);
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
	let newDatapointsToAlarm = $state<number | undefined>();
	let newTreatMissingData = $state<string>('missing');
	let newAlarmDescription = $state('');
	let newAlarmActions = $state('');

	// Alarm history. Keyed by "region::name" -- expandedAlarms/historyAlarmKey
	// use the composite key so the same alarm name in two regions doesn't
	// collide.
	let historyAlarmKey = $state('');
	let alarmHistory = $state<AlarmHistoryItem[]>([]);
	let showHistory = $state(false);
	let loadingHistory = $state(false);
	let expandedAlarms = $state<Set<string>>(new Set());

	// Edit State modal
	let showEditState = $state(false);
	let editStateAlarmName = $state('');
	let editStateRegion = $state('');
	let editStateValue = $state<'ALARM' | 'OK' | 'INSUFFICIENT_DATA'>('OK');
	let editStateReason = $state('');
	let editingState = $state(false);

	// Metrics
	let metrics = $state<Regioned<Metric>[]>([]);
	let metricsSearch = $state('');

	// Metric chart (time-series)
	let showMetricChart = $state(false);
	let chartMetric = $state<Regioned<Metric> | null>(null);
	let chartStatistic = $state<'Average' | 'Sum' | 'Minimum' | 'Maximum' | 'SampleCount'>('Average');
	let chartRangeHours = $state(3);
	let chartPeriod = $state(300);
	let chartDatapoints = $state<Datapoint[]>([]);
	let loadingChart = $state(false);
	let chartError = $state('');

	function chartValue(d: Datapoint): number {
		switch (chartStatistic) {
			case 'Average':
				return d.Average ?? 0;
			case 'Sum':
				return d.Sum ?? 0;
			case 'Minimum':
				return d.Minimum ?? 0;
			case 'Maximum':
				return d.Maximum ?? 0;
			case 'SampleCount':
				return d.SampleCount ?? 0;
		}
	}

	async function openMetricChart(m: Regioned<Metric>) {
		chartMetric = m;
		showMetricChart = true;
		chartDatapoints = [];
		chartError = '';
		await loadMetricChart();
	}

	async function loadMetricChart() {
		if (!chartMetric?.Namespace || !chartMetric?.MetricName) return;
		loadingChart = true;
		chartError = '';
		try {
			const end = new Date();
			const start = new Date(end.getTime() - chartRangeHours * 3600 * 1000);
			const res = await getCloudWatchClient(chartMetric.region).send(
				new GetMetricStatisticsCommand({
					Namespace: chartMetric.Namespace,
					MetricName: chartMetric.MetricName,
					Dimensions: chartMetric.Dimensions,
					StartTime: start,
					EndTime: end,
					Period: chartPeriod,
					Statistics: [chartStatistic]
				})
			);
			chartDatapoints = (res.Datapoints ?? []).toSorted(
				(a, b) => (a.Timestamp?.getTime() ?? 0) - (b.Timestamp?.getTime() ?? 0)
			);
			if (chartDatapoints.length === 0) {
				chartError = 'No datapoints returned for this metric and time range.';
			}
		} catch (e) {
			chartError = e instanceof Error ? e.message : String(e);
		} finally {
			loadingChart = false;
		}
	}

	const chartGeometry = $derived(() => {
		const w = 640;
		const h = 200;
		const pad = 32;
		const vals = chartDatapoints.map((d) => chartValue(d));
		if (vals.length === 0) return { w, h, pad, path: '', min: 0, max: 0, points: [] as { x: number; y: number; v: number; t?: Date }[] };
		let min = Math.min(...vals);
		let max = Math.max(...vals);
		if (min === max) {
			min -= 1;
			max += 1;
		}
		const span = max - min;
		const n = chartDatapoints.length;
		const points = chartDatapoints.map((d, i) => {
			const x = n === 1 ? w / 2 : pad + (i / (n - 1)) * (w - pad * 2);
			const v = chartValue(d);
			const y = h - pad - ((v - min) / span) * (h - pad * 2);
			return { x, y, v, t: d.Timestamp };
		});
		const path = points.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(1)},${p.y.toFixed(1)}`).join(' ');
		return { w, h, pad, path, min, max, points };
	});

	// Dashboards
	let dashboards = $state<Regioned<DashboardEntry>[]>([]);
	let showCreateDashboard = $state(false);
	let creatingDashboard = $state(false);
	let newDashboardName = $state('');

	// Metric Streams
	let streams = $state<Regioned<MetricStreamEntry>[]>([]);
	let showCreateStream = $state(false);
	let creatingStream = $state(false);
	let newStreamName = $state('');
	let newStreamFirehoseArn = $state('');
	let newStreamOutputFormat = $state('json');

	// Anomaly Detectors
	let anomalyDetectors = $state<Regioned<AnomalyDetector>[]>([]);
	let showCreateAnomaly = $state(false);
	let creatingAnomaly = $state(false);
	let newAnomalyNamespace = $state('AWS/EC2');
	let newAnomalyMetric = $state('CPUUtilization');
	let newAnomalyStat = $state('Average');

	// Metric Filters
	let metricFilters = $state<Regioned<MetricFilter>[]>([]);

	const filteredAlarms = $derived(
		alarms.filter(
			(a) =>
				a.AlarmName?.toLowerCase().includes(searchQuery.toLowerCase()) ||
				a.Namespace?.toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	// Grouped by "namespace::region" (not namespace alone) -- under All mode
	// the same namespace legitimately appears in multiple regions, and a
	// namespace-only key would merge their metrics into one indistinguishable
	// group.
	const groupedMetrics = $derived(() => {
		const groups: Record<string, Regioned<Metric>[]> = {};
		for (const m of metrics) {
			if (!m.Namespace) continue;
			const key = `${m.Namespace}::${m.region}`;
			if (!groups[key]) groups[key] = [];
			if (
				!metricsSearch ||
				m.MetricName?.toLowerCase().includes(metricsSearch.toLowerCase()) ||
				m.Namespace?.toLowerCase().includes(metricsSearch.toLowerCase())
			) {
				groups[key].push(m);
			}
		}
		return groups;
	});

	function stateColor(state: string | undefined): string {
		if (state === 'ALARM') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
		if (state === 'OK') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
	}

	function anomalyStateColor(state: string | undefined): string {
		if (state === 'TRAINED') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		if (state === 'PENDING_TRAINING') return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
		return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	}

	// Demo-data fallback only applies in single-region mode: it seeds fake
	// local records tagged with the current region when the backend has
	// nothing yet, which doesn't make sense to inject into an aggregated
	// All-mode view.
	async function loadDemoData() {
		const region = currentRegion();
		// Demo alarms
		const demoAlarms: Regioned<MetricAlarm>[] = [
			{
				AlarmName: 'demo-high-cpu',
				StateValue: 'ALARM',
				Namespace: 'AWS/EC2',
				MetricName: 'CPUUtilization',
				Threshold: 80,
				ComparisonOperator: 'GreaterThanThreshold',
				EvaluationPeriods: 2,
				Period: 300,
				Statistic: 'Average',
				DatapointsToAlarm: 2,
				TreatMissingData: 'missing',
				ActionsEnabled: true,
				region
			},
			{
				AlarmName: 'demo-low-disk',
				StateValue: 'OK',
				Namespace: 'AWS/EBS',
				MetricName: 'VolumeReadOps',
				Threshold: 10,
				ComparisonOperator: 'LessThanThreshold',
				EvaluationPeriods: 1,
				Period: 60,
				Statistic: 'Sum',
				TreatMissingData: 'notBreaching',
				ActionsEnabled: false,
				region
			},
			{
				AlarmName: 'demo-network-in',
				StateValue: 'INSUFFICIENT_DATA',
				Namespace: 'AWS/EC2',
				MetricName: 'NetworkIn',
				Threshold: 5000000,
				ComparisonOperator: 'GreaterThanThreshold',
				EvaluationPeriods: 3,
				Period: 300,
				Statistic: 'Average',
				DatapointsToAlarm: 2,
				TreatMissingData: 'breaching',
				ActionsEnabled: true,
				region
			}
		];

		// Demo metric streams
		const demoStreams: Regioned<MetricStreamEntry>[] = [
			{ Name: 'demo-stream-firehose', FirehoseArn: 'arn:aws:firehose:us-east-1:123456789012:deliverystream/demo-stream', State: 'running', OutputFormat: 'json', CreationDate: new Date('2024-01-15'), region },
			{ Name: 'demo-stream-ops', FirehoseArn: 'arn:aws:firehose:us-east-1:123456789012:deliverystream/ops-stream', State: 'stopped', OutputFormat: 'opentelemetry1.0', CreationDate: new Date('2024-03-20'), region }
		];

		// Demo anomaly detectors
		const demoAnomalies: Regioned<AnomalyDetector>[] = [
			{ SingleMetricAnomalyDetector: { Namespace: 'AWS/EC2', MetricName: 'CPUUtilization', Stat: 'Average' }, StateValue: 'TRAINED', region },
			{ SingleMetricAnomalyDetector: { Namespace: 'AWS/RDS', MetricName: 'DatabaseConnections', Stat: 'Average' }, StateValue: 'PENDING_TRAINING', region }
		];

		// Demo metric filters
		const demoFilters: Regioned<MetricFilter>[] = [
			{ filterName: 'demo-error-filter', logGroupName: '/aws/lambda/my-function', filterPattern: '[ERROR]', metricTransformations: [{ metricName: 'ErrorCount', metricNamespace: 'CustomApp', metricValue: '1' }], region },
			{ filterName: 'demo-latency-filter', logGroupName: '/aws/apigateway/my-api', filterPattern: '[duration > 1000]', metricTransformations: [{ metricName: 'HighLatency', metricNamespace: 'CustomApp', metricValue: '1' }], region }
		];

		alarms = demoAlarms;
		streams = demoStreams;
		anomalyDetectors = demoAnomalies;
		metricFilters = demoFilters;

		// Attempt to push demo data via API (best-effort)
		try {
			await Promise.all([
				cw().send(new PutMetricAlarmCommand({
					AlarmName: 'demo-high-cpu',
					MetricName: 'CPUUtilization',
					Namespace: 'AWS/EC2',
					Threshold: 80,
					ComparisonOperator: 'GreaterThanThreshold',
					EvaluationPeriods: 2,
					Period: 300,
					Statistic: 'Average',
					DatapointsToAlarm: 2,
					TreatMissingData: 'missing'
				})),
				cwLogs().send(new PutMetricFilterCommand({
					logGroupName: '/aws/lambda/my-function',
					filterName: 'demo-error-filter',
					filterPattern: '[ERROR]',
					metricTransformations: [{ metricName: 'ErrorCount', metricNamespace: 'CustomApp', metricValue: '1' }]
				}))
			]);
		} catch {
			// Demo data is already set locally; ignore API errors
		}
	}

	async function loadAlarmsForTab() {
		alarms = await loadRegioned('alarms',
			(region) => getCloudWatchClient(region).send(new DescribeAlarmsCommand({ MaxRecords: 100 })),
			(r) => r.MetricAlarms ?? []);
		if (alarms.length === 0 && !isAllRegions()) await loadDemoData();
	}

	async function loadMetricsForTab() {
		metrics = await loadRegioned('metrics',
			(region) => getCloudWatchClient(region).send(new ListMetricsCommand({})),
			(r) => r.Metrics ?? []);
	}

	async function loadDashboardsForTab() {
		dashboards = await loadRegioned('dashboards',
			(region) => getCloudWatchClient(region).send(new ListDashboardsCommand({})),
			(r) => r.DashboardEntries ?? []);
	}

	async function loadStreamsForTab() {
		streams = await loadRegioned('metric streams',
			(region) => getCloudWatchClient(region).send(new ListMetricStreamsCommand({})),
			(r) => r.Entries ?? []);
		if (streams.length === 0 && !isAllRegions()) {
			const region = currentRegion();
			streams = [
				{ Name: 'demo-stream-firehose', FirehoseArn: 'arn:aws:firehose:us-east-1:123456789012:deliverystream/demo-stream', State: 'running', OutputFormat: 'json', CreationDate: new Date('2024-01-15'), region },
				{ Name: 'demo-stream-ops', FirehoseArn: 'arn:aws:firehose:us-east-1:123456789012:deliverystream/ops-stream', State: 'stopped', OutputFormat: 'opentelemetry1.0', CreationDate: new Date('2024-03-20'), region }
			];
		}
	}

	async function loadAnomalyForTab() {
		anomalyDetectors = await loadRegioned('anomaly detectors',
			(region) => getCloudWatchClient(region).send(new DescribeAnomalyDetectorsCommand({})),
			(r) => r.AnomalyDetectors ?? []);
		if (anomalyDetectors.length === 0 && !isAllRegions()) {
			const region = currentRegion();
			anomalyDetectors = [
				{ SingleMetricAnomalyDetector: { Namespace: 'AWS/EC2', MetricName: 'CPUUtilization', Stat: 'Average' }, StateValue: 'TRAINED', region },
				{ SingleMetricAnomalyDetector: { Namespace: 'AWS/RDS', MetricName: 'DatabaseConnections', Stat: 'Average' }, StateValue: 'PENDING_TRAINING', region }
			];
		}
	}

	async function loadFiltersForTab() {
		metricFilters = await loadRegioned('metric filters',
			(region) => getCloudWatchLogsClient(region).send(new DescribeMetricFiltersCommand({})),
			(r) => r.metricFilters ?? []);
		if (metricFilters.length === 0 && !isAllRegions()) {
			const region = currentRegion();
			metricFilters = [
				{ filterName: 'demo-error-filter', logGroupName: '/aws/lambda/my-function', filterPattern: '[ERROR]', metricTransformations: [{ metricName: 'ErrorCount', metricNamespace: 'CustomApp', metricValue: '1' }], region },
				{ filterName: 'demo-latency-filter', logGroupName: '/aws/apigateway/my-api', filterPattern: '[duration > 1000]', metricTransformations: [{ metricName: 'HighLatency', metricNamespace: 'CustomApp', metricValue: '1' }], region }
			];
		}
	}

	const tabDataLoaders: Record<TabId, () => Promise<void>> = {
		alarms: loadAlarmsForTab,
		metrics: loadMetricsForTab,
		dashboards: loadDashboardsForTab,
		streams: loadStreamsForTab,
		anomaly: loadAnomalyForTab,
		filters: loadFiltersForTab
	};

	async function loadData(tab: TabId = activeTab) {
		loading = true;
		try {
			await tabDataLoaders[tab]();
		} catch (err: unknown) {
			toast.error(`Failed to load ${tab}: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	// loadAlarms is loadAlarmsForTab's loading-flag-wrapped twin, used after
	// alarm mutations (create/delete/state-change) rather than a tab switch.
	async function loadAlarms() {
		loading = true;
		try {
			await loadAlarmsForTab();
		} catch (err: unknown) {
			toast.error(`Failed to load alarms: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function toggleAlarmHistory(alarmName: string, region: string) {
		const key = `${region}::${alarmName}`;
		const next = new Set(expandedAlarms);
		if (next.has(key)) {
			next.delete(key);
			expandedAlarms = next;
			return;
		}
		next.add(key);
		expandedAlarms = next;
		historyAlarmKey = key;
		loadingHistory = true;
		try {
			const res = await getCloudWatchClient(region).send(new DescribeAlarmHistoryCommand({ AlarmName: alarmName, MaxRecords: 20 }));
			alarmHistory = res.AlarmHistoryItems ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load history: ${(err as Error).message}`);
		} finally {
			loadingHistory = false;
		}
	}

	async function createAlarm() {
		if (!newAlarmName.trim() || !newMetricName.trim()) return;
		creatingAlarm = true;
		try {
			await cw().send(
				new PutMetricAlarmCommand({
					AlarmName: newAlarmName.trim(),
					MetricName: newMetricName.trim(),
					Namespace: newNamespace.trim(),
					Threshold: newThreshold,
					ComparisonOperator: newComparisonOperator,
					EvaluationPeriods: newEvaluationPeriods,
					Period: newPeriod,
					Statistic: newStatistic,
					...(newDatapointsToAlarm ? { DatapointsToAlarm: newDatapointsToAlarm } : {}),
					TreatMissingData: newTreatMissingData,
					...(newAlarmDescription.trim() ? { AlarmDescription: newAlarmDescription.trim() } : {}),
					...(newAlarmActions.trim() ? { AlarmActions: newAlarmActions.split(',').map(s => s.trim()).filter(Boolean) } : {})
				})
			);
			toast.success(`Alarm "${newAlarmName}" created`);
			showCreateAlarm = false;
			newAlarmName = '';
			newMetricName = '';
			newDatapointsToAlarm = undefined;
			newTreatMissingData = 'missing';
			newAlarmDescription = '';
			newAlarmActions = '';
			await loadAlarms();
		} catch (err: unknown) {
			toast.error(`Create alarm failed: ${(err as Error).message}`);
		} finally {
			creatingAlarm = false;
		}
	}

	async function deleteAlarm(name: string, region: string) {
		if (!await confirmDestructive({ title: 'Delete Alarm', message: `Delete alarm "${name}"? No further alerts will be triggered.` })) return;
		try {
			await getCloudWatchClient(region).send(new DeleteAlarmsCommand({ AlarmNames: [name] }));
			toast.success(`Alarm "${name}" deleted`);
			await loadAlarms();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function openEditState(alarmName: string, region: string) {
		editStateAlarmName = alarmName;
		editStateRegion = region;
		editStateValue = 'OK';
		editStateReason = '';
		showEditState = true;
	}

	async function setAlarmState() {
		if (!editStateAlarmName || !editStateReason.trim()) return;
		editingState = true;
		try {
			await getCloudWatchClient(editStateRegion).send(new SetAlarmStateCommand({
				AlarmName: editStateAlarmName,
				StateValue: editStateValue,
				StateReason: editStateReason.trim()
			}));
			toast.success(`Alarm "${editStateAlarmName}" state set to ${editStateValue}`);
			showEditState = false;
			await loadAlarms();
		} catch (err: unknown) {
			toast.error(`Set state failed: ${(err as Error).message}`);
		} finally {
			editingState = false;
		}
	}

	async function toggleAlarmActions(alarm: Regioned<MetricAlarm>) {
		const name = alarm.AlarmName ?? '';
		try {
			if (alarm.ActionsEnabled) {
				await getCloudWatchClient(alarm.region).send(new DisableAlarmActionsCommand({ AlarmNames: [name] }));
				toast.success(`Actions disabled for "${name}"`);
			} else {
				await getCloudWatchClient(alarm.region).send(new EnableAlarmActionsCommand({ AlarmNames: [name] }));
				toast.success(`Actions enabled for "${name}"`);
			}
			await loadAlarms();
		} catch (err: unknown) {
			toast.error(`Toggle actions failed: ${(err as Error).message}`);
		}
	}

	async function deleteStream(name: string, region: string) {
		if (!await confirmDestructive({ title: 'Delete Metric Stream', message: `Delete metric stream "${name}"?` })) return;
		try {
			await getCloudWatchClient(region).send(new DeleteMetricStreamCommand({ Name: name }));
			toast.success(`Metric stream "${name}" deleted`);
			await loadStreamsForTab();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function createStream() {
		if (!newStreamName.trim() || !newStreamFirehoseArn.trim()) return;
		creatingStream = true;
		try {
			await cw().send(new PutMetricStreamCommand({
				Name: newStreamName.trim(),
				FirehoseArn: newStreamFirehoseArn.trim(),
				OutputFormat: newStreamOutputFormat as 'json' | 'opentelemetry1.0' | 'opentelemetry0.7',
				RoleArn: 'arn:aws:iam::123456789012:role/CloudWatchStreamRole'
			}));
			toast.success(`Metric stream "${newStreamName}" created`);
			showCreateStream = false;
			newStreamName = '';
			newStreamFirehoseArn = '';
			newStreamOutputFormat = 'json';
			await loadStreamsForTab();
		} catch (err: unknown) {
			toast.error(`Create stream failed: ${(err as Error).message}`);
		} finally {
			creatingStream = false;
		}
	}

	async function deleteAnomalyDetector(detector: Regioned<AnomalyDetector>) {
		const label = detector.SingleMetricAnomalyDetector?.MetricName ?? 'detector';
		if (!await confirmDestructive({ title: 'Delete Anomaly Detector', message: `Delete anomaly detector for "${label}"?` })) return;
		try {
			await getCloudWatchClient(detector.region).send(new DeleteAnomalyDetectorCommand({
				SingleMetricAnomalyDetector: detector.SingleMetricAnomalyDetector
			}));
			toast.success(`Anomaly detector for "${label}" deleted`);
			await loadAnomalyForTab();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function createAnomalyDetector() {
		if (!newAnomalyNamespace.trim() || !newAnomalyMetric.trim()) return;
		creatingAnomaly = true;
		try {
			await cw().send(new PutAnomalyDetectorCommand({
				SingleMetricAnomalyDetector: {
					Namespace: newAnomalyNamespace.trim(),
					MetricName: newAnomalyMetric.trim(),
					Stat: newAnomalyStat.trim()
				}
			}));
			toast.success(`Anomaly detector created for ${newAnomalyMetric}`);
			showCreateAnomaly = false;
			newAnomalyNamespace = 'AWS/EC2';
			newAnomalyMetric = 'CPUUtilization';
			newAnomalyStat = 'Average';
			await loadAnomalyForTab();
		} catch (err: unknown) {
			toast.error(`Create anomaly detector failed: ${(err as Error).message}`);
		} finally {
			creatingAnomaly = false;
		}
	}

	async function deleteMetricFilter(filter: Regioned<MetricFilter>) {
		const name = filter.filterName ?? '';
		const logGroup = filter.logGroupName ?? '';
		if (!await confirmDestructive({ title: 'Delete Metric Filter', message: `Delete metric filter "${name}"?` })) return;
		try {
			await getCloudWatchLogsClient(filter.region).send(new DeleteMetricFilterCommand({ filterName: name, logGroupName: logGroup }));
			toast.success(`Metric filter "${name}" deleted`);
			await loadFiltersForTab();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function createDashboard() {
		if (!newDashboardName.trim()) return;
		creatingDashboard = true;
		try {
			await cw().send(new PutDashboardCommand({
				DashboardName: newDashboardName.trim(),
				DashboardBody: JSON.stringify({ widgets: [] })
			}));
			toast.success(`Dashboard "${newDashboardName}" created`);
			showCreateDashboard = false;
			newDashboardName = '';
			await loadDashboardsForTab();
		} catch (err: unknown) {
			toast.error(`Create dashboard failed: ${(err as Error).message}`);
		} finally {
			creatingDashboard = false;
		}
	}

	async function deleteDashboard(name: string, region: string) {
		if (!await confirmDestructive({ title: 'Delete Dashboard', message: `Delete dashboard "${name}"? All widgets and layout settings will be lost.` })) return;
		try {
			await getCloudWatchClient(region).send(new DeleteDashboardsCommand({ DashboardNames: [name] }));
			toast.success(`Dashboard "${name}" deleted`);
			await loadDashboardsForTab();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	$effect(() => {
		void loadData();
	});

	// Every tab's list is region-scoped, as is the alarm-history/chart detail
	// state (an alarm or metric name from the old region is meaningless in
	// the new one). `activeTab` is read via untrack() because the tab
	// buttons' onclick also writes it: without untrack, every tab switch
	// would re-trigger this region effect and double-fetch.
	onRegionChange(() => {
		alarms = [];
		metrics = [];
		dashboards = [];
		streams = [];
		anomalyDetectors = [];
		metricFilters = [];
		alarmHistory = [];
		historyAlarmKey = '';
		expandedAlarms = new Set();
		showHistory = false;
		chartMetric = null;
		chartDatapoints = [];
		showMetricChart = false;
		const tab = untrack(() => activeTab);
		void loadData(tab);
	});
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
				<WriteRegionHint />
				<button onclick={() => { showCreateAlarm = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
					<Plus class="w-4 h-4" />Create Alarm
				</button>
			{:else if activeTab === 'dashboards'}
				<WriteRegionHint />
				<button onclick={() => { showCreateDashboard = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
					<Plus class="w-4 h-4" />Create Dashboard
				</button>
			{/if}
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex flex-wrap border-b border-slate-200 dark:border-slate-700">
		{#each [
			{ id: 'alarms', label: 'Alarms', icon: Bell },
			{ id: 'metrics', label: 'Metrics', icon: BarChart2 },
			{ id: 'dashboards', label: 'Dashboards', icon: Layout },
			{ id: 'streams', label: 'Metric Streams', icon: Radio },
			{ id: 'anomaly', label: 'Anomaly Detectors', icon: ScanLine },
			{ id: 'filters', label: 'Metric Filters', icon: Filter }
		] as tab}
			<button
				onclick={() => {
					const nextTab = tab.id as TabId;
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
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
						<div class="p-4 flex items-start gap-4">
							<Bell class="w-5 h-5 text-orange-500 flex-shrink-0 mt-0.5" />
							<div class="flex-1 min-w-0">
								<div class="flex items-center gap-2">
									<p class="font-medium text-slate-900 dark:text-white">{alarm.AlarmName}</p>
									<RegionChip region={alarm.region} />
								</div>
								<p class="text-xs text-slate-500 dark:text-slate-400">
									{alarm.Namespace} / {alarm.MetricName} · {alarm.ComparisonOperator?.replace(/([A-Z])/g, ' $1').trim()} {alarm.Threshold}
								</p>
								{#if alarm.DatapointsToAlarm || alarm.TreatMissingData}
									<p class="text-xs text-slate-400 dark:text-slate-500 mt-0.5">
										{#if alarm.DatapointsToAlarm}Datapoints: {alarm.DatapointsToAlarm} · {/if}
										{#if alarm.TreatMissingData}Missing: {alarm.TreatMissingData}{/if}
									</p>
								{/if}
								{#if alarm.AlarmDescription}
									<p class="text-xs text-slate-400 dark:text-slate-500 mt-0.5 italic">{alarm.AlarmDescription}</p>
								{/if}
								{#if alarm.StateReason}
									<p class="text-xs text-slate-400 dark:text-slate-500 mt-0.5">Reason: {alarm.StateReason}</p>
								{/if}
							</div>
							<span class="px-2 py-0.5 text-xs rounded-full {stateColor(alarm.StateValue)}">{alarm.StateValue}</span>
							<span class="text-sm text-slate-500 dark:text-slate-400 whitespace-nowrap">{alarm.Period}s / {alarm.EvaluationPeriods}p</span>
							<button
								onclick={() => toggleAlarmActions(alarm)}
								class="p-1 text-slate-400 hover:text-indigo-500"
								title={alarm.ActionsEnabled ? 'Disable actions' : 'Enable actions'}
							>
								{#if alarm.ActionsEnabled}
									<ToggleRight class="w-5 h-5 text-indigo-500" />
								{:else}
									<ToggleLeft class="w-5 h-5" />
								{/if}
							</button>
							<button
								onclick={() => openEditState(alarm.AlarmName ?? '', alarm.region)}
								class="p-1 text-slate-400 hover:text-indigo-500"
								title="Edit state"
								data-testid="edit-state-btn"
							>
								<Edit2 class="w-4 h-4" />
							</button>
							<button
								onclick={() => toggleAlarmHistory(alarm.AlarmName ?? '', alarm.region)}
								class="p-1 text-slate-400 hover:text-indigo-500"
								title="View history"
							>
								{#if expandedAlarms.has(`${alarm.region}::${alarm.AlarmName ?? ''}`)}
									<ChevronUp class="w-4 h-4" />
								{:else}
									<ChevronDown class="w-4 h-4" />
								{/if}
							</button>
							<button onclick={() => deleteAlarm(alarm.AlarmName ?? '', alarm.region)} class="p-1 text-slate-400 hover:text-red-500">
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
						{#if expandedAlarms.has(`${alarm.region}::${alarm.AlarmName ?? ''}`)}
							<div class="border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 px-4 py-3">
								<div class="flex items-center gap-2 mb-2">
									<Clock class="w-4 h-4 text-slate-400" />
									<span class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide">Alarm History</span>
								</div>
								{#if loadingHistory && historyAlarmKey === `${alarm.region}::${alarm.AlarmName ?? ''}`}
									<p class="text-xs text-slate-400">Loading history...</p>
								{:else if alarmHistory.length === 0}
									<p class="text-xs text-slate-400">No history entries</p>
								{:else}
									<div class="space-y-1 max-h-48 overflow-y-auto">
										{#each alarmHistory as item}
											<div class="flex items-start gap-2 text-xs">
												<span class="text-slate-400 whitespace-nowrap">{item.Timestamp ? new Date(item.Timestamp).toLocaleString() : ''}</span>
												<span class="text-slate-500 dark:text-slate-400">{item.HistoryItemType}</span>
												<span class="text-slate-700 dark:text-slate-300">{item.HistorySummary}</span>
											</div>
										{/each}
									</div>
								{/if}
							</div>
						{/if}
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
				{#each Object.entries(groupedMetrics()) as [key, nsMetrics]}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
						<h3 class="font-semibold text-slate-900 dark:text-white mb-2 flex items-center gap-2">
							<BarChart2 class="w-4 h-4 text-indigo-500" />
							{nsMetrics[0]?.Namespace ?? key} <RegionChip region={nsMetrics[0]?.region} /> <span class="text-sm font-normal text-slate-500 dark:text-slate-400">({nsMetrics.length} metrics)</span>
						</h3>
						<div class="flex flex-wrap gap-1">
							{#each nsMetrics.slice(0, 20) as m}
								<button
									type="button"
									onclick={() => openMetricChart(m)}
									title="Graph this metric"
									class="px-2 py-1 text-xs bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded hover:bg-indigo-100 dark:hover:bg-indigo-900 hover:text-indigo-700 dark:hover:text-indigo-300 transition-colors"
								>{m.MetricName}</button>
							{/each}
							{#if nsMetrics.length > 20}
								<span class="px-2 py-1 text-xs text-slate-400">+{nsMetrics.length - 20} more</span>
							{/if}
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{:else if activeTab === 'dashboards'}
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
							<div class="flex items-center gap-2">
								<p class="font-medium text-slate-900 dark:text-white">{dash.DashboardName}</p>
								<RegionChip region={dash.region} />
							</div>
							<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
								Modified: {dash.LastModified ? new Date(dash.LastModified).toLocaleDateString() : 'N/A'}
							</p>
							{#if dash.Size}
								<p class="text-xs text-slate-400 dark:text-slate-500">{dash.Size} bytes</p>
							{/if}
						</div>
						<button onclick={() => deleteDashboard(dash.DashboardName ?? '', dash.region)} class="p-1.5 text-slate-400 hover:text-red-500">
							<Trash2 class="w-4 h-4" />
						</button>
					</div>
				{/each}
			</div>
		{/if}
	{:else if activeTab === 'streams'}
		<div class="flex justify-end mb-3 items-center gap-3">
			<WriteRegionHint />
			<button onclick={() => { showCreateStream = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2 text-sm">
				<Plus class="w-4 h-4" />Create Stream
			</button>
		</div>
		{#if streams.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Radio class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
				<p class="text-slate-500 dark:text-slate-400">No metric streams found</p>
			</div>
		{:else}
			<div class="space-y-2">
				{#each streams as stream}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-4">
						<Radio class="w-5 h-5 text-indigo-500 flex-shrink-0" />
						<div class="flex-1 min-w-0">
							<div class="flex items-center gap-2">
								<p class="font-medium text-slate-900 dark:text-white">{stream.Name}</p>
								<RegionChip region={stream.region} />
							</div>
							<p class="text-xs text-slate-500 dark:text-slate-400">{stream.FirehoseArn ?? ''}</p>
							{#if stream.CreationDate}
								<p class="text-xs text-slate-400">Created {new Date(stream.CreationDate).toLocaleDateString()}</p>
							{/if}
						</div>
						<span class="px-2 py-0.5 text-xs rounded-full {stream.State === 'running' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400'}">{stream.State}</span>
						<span class="px-2 py-0.5 text-xs bg-blue-50 dark:bg-blue-900/20 text-blue-600 dark:text-blue-400 rounded">{stream.OutputFormat}</span>
						<button onclick={() => deleteStream(stream.Name ?? '', stream.region)} class="p-1 text-slate-400 hover:text-red-500">
							<Trash2 class="w-4 h-4" />
						</button>
					</div>
				{/each}
			</div>
		{/if}
	{:else if activeTab === 'anomaly'}
		<div class="flex justify-end mb-3 items-center gap-3">
			<WriteRegionHint />
			<button onclick={() => { showCreateAnomaly = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2 text-sm">
				<Plus class="w-4 h-4" />Create Detector
			</button>
		</div>
		{#if anomalyDetectors.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<ScanLine class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
				<p class="text-slate-500 dark:text-slate-400">No anomaly detectors found</p>
			</div>
		{:else}
			<div class="space-y-2">
				{#each anomalyDetectors as detector}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-4">
						<ScanLine class="w-5 h-5 text-purple-500 flex-shrink-0" />
						<div class="flex-1 min-w-0">
							<div class="flex items-center gap-2">
								<p class="font-medium text-slate-900 dark:text-white">
									{detector.SingleMetricAnomalyDetector?.MetricName ?? 'Unknown'}
								</p>
								<RegionChip region={detector.region} />
							</div>
							<p class="text-xs text-slate-500 dark:text-slate-400">
								{detector.SingleMetricAnomalyDetector?.Namespace ?? ''} · {detector.SingleMetricAnomalyDetector?.Stat ?? ''}
							</p>
						</div>
						<span class="px-2 py-0.5 text-xs rounded-full {anomalyStateColor(detector.StateValue)}">{detector.StateValue}</span>
						<button onclick={() => deleteAnomalyDetector(detector)} class="p-1 text-slate-400 hover:text-red-500">
							<Trash2 class="w-4 h-4" />
						</button>
					</div>
				{/each}
			</div>
		{/if}
	{:else if activeTab === 'filters'}
		{#if metricFilters.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Filter class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
				<p class="text-slate-500 dark:text-slate-400">No metric filters found</p>
			</div>
		{:else}
			<div class="space-y-2">
				{#each metricFilters as filter}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-4">
						<Filter class="w-5 h-5 text-teal-500 flex-shrink-0" />
						<div class="flex-1 min-w-0">
							<div class="flex items-center gap-2">
								<p class="font-medium text-slate-900 dark:text-white">{filter.filterName}</p>
								<RegionChip region={filter.region} />
							</div>
							<p class="text-xs text-slate-500 dark:text-slate-400">
								{filter.logGroupName} · <span class="font-mono">{filter.filterPattern}</span>
							</p>
							{#if filter.metricTransformations?.length}
								<p class="text-xs text-slate-400 dark:text-slate-500 mt-0.5">
									→ {filter.metricTransformations[0].metricNamespace}/{filter.metricTransformations[0].metricName}
								</p>
							{/if}
						</div>
						<button onclick={() => deleteMetricFilter(filter)} class="p-1 text-slate-400 hover:text-red-500">
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
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="cw-datapoints" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Datapoints to Alarm <span class="text-slate-400">(optional)</span></label>
						<input id="cw-datapoints" type="number" bind:value={newDatapointsToAlarm} min="1" placeholder="e.g. 2" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
					<div>
						<label for="cw-treat-missing" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Treat Missing Data</label>
						<select id="cw-treat-missing" bind:value={newTreatMissingData} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
							{#each ['missing', 'notBreaching', 'breaching', 'ignore'] as opt}
								<option value={opt}>{opt}</option>
							{/each}
						</select>
					</div>
				</div>
				<div>
					<label for="cw-alarm-desc" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Description <span class="text-slate-400">(optional)</span></label>
					<input id="cw-alarm-desc" type="text" bind:value={newAlarmDescription} placeholder="Human-readable description" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
				</div>
				<div>
					<label for="cw-alarm-actions" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Alarm Actions <span class="text-slate-400">(comma-separated SNS ARNs, optional)</span></label>
					<input id="cw-alarm-actions" type="text" bind:value={newAlarmActions} placeholder="arn:aws:sns:us-east-1:123456789012:my-topic" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
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

<!-- Edit Alarm State Modal -->
{#if showEditState}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-1">Edit Alarm State</h2>
			<p class="text-sm text-slate-500 dark:text-slate-400 mb-4">{editStateAlarmName}</p>
			<form onsubmit={(e) => { e.preventDefault(); setAlarmState(); }} class="space-y-4">
				<div>
					<label for="cw-state-value" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">State</label>
					<select id="cw-state-value" bind:value={editStateValue} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
						{#each ['ALARM', 'OK', 'INSUFFICIENT_DATA'] as v}
							<option value={v}>{v}</option>
						{/each}
					</select>
				</div>
				<div>
					<label for="cw-state-reason" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Reason</label>
					<input id="cw-state-reason" type="text" bind:value={editStateReason} placeholder="Reason for state change" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showEditState = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={editingState} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{editingState ? 'Saving...' : 'Set State'}
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

<!-- Metric Chart Modal -->
{#if showMetricChart && chartMetric}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-3xl">
			<div class="flex items-start justify-between mb-4">
				<div>
					<h2 class="text-xl font-bold text-slate-900 dark:text-white flex items-center gap-2">
						<BarChart2 class="w-5 h-5 text-indigo-500" />
						{chartMetric.MetricName}
					</h2>
					<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{chartMetric.Namespace}</p>
					{#if chartMetric.Dimensions && chartMetric.Dimensions.length > 0}
						<p class="text-xs text-slate-400 dark:text-slate-500 mt-0.5">
							{chartMetric.Dimensions.map((d) => `${d.Name}=${d.Value}`).join(', ')}
						</p>
					{/if}
				</div>
				<button type="button" onclick={() => { showMetricChart = false; chartMetric = null; }} class="p-1.5 text-slate-400 hover:text-slate-700 dark:hover:text-white">✕</button>
			</div>

			<div class="flex flex-wrap items-end gap-3 mb-4">
				<div>
					<label for="cw-chart-stat" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Statistic</label>
					<select id="cw-chart-stat" bind:value={chartStatistic} onchange={loadMetricChart} class="px-3 py-1.5 text-sm bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white">
						<option value="Average">Average</option>
						<option value="Sum">Sum</option>
						<option value="Minimum">Minimum</option>
						<option value="Maximum">Maximum</option>
						<option value="SampleCount">SampleCount</option>
					</select>
				</div>
				<div>
					<label for="cw-chart-range" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Time Range</label>
					<select id="cw-chart-range" bind:value={chartRangeHours} onchange={loadMetricChart} class="px-3 py-1.5 text-sm bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white">
						<option value={1}>Last 1h</option>
						<option value={3}>Last 3h</option>
						<option value={12}>Last 12h</option>
						<option value={24}>Last 24h</option>
						<option value={168}>Last 7d</option>
					</select>
				</div>
				<div>
					<label for="cw-chart-period" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Period (s)</label>
					<select id="cw-chart-period" bind:value={chartPeriod} onchange={loadMetricChart} class="px-3 py-1.5 text-sm bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white">
						<option value={60}>60</option>
						<option value={300}>300</option>
						<option value={900}>900</option>
						<option value={3600}>3600</option>
					</select>
				</div>
				<button type="button" onclick={loadMetricChart} class="px-3 py-1.5 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5">
					<RefreshCw class="w-3.5 h-3.5" />Refresh
				</button>
			</div>

			{#if loadingChart}
				<div class="py-16 text-center text-slate-500 dark:text-slate-400">Loading datapoints...</div>
			{:else if chartError}
				<div class="py-16 text-center text-amber-600 dark:text-amber-400 text-sm">{chartError}</div>
			{:else}
				{@const g = chartGeometry()}
				<div class="bg-slate-50 dark:bg-slate-900 rounded-lg border border-slate-200 dark:border-slate-700 p-2">
					<svg viewBox={`0 0 ${g.w} ${g.h}`} class="w-full" role="img" aria-label="Metric time series chart">
						<line x1={g.pad} y1={g.h - g.pad} x2={g.w - g.pad} y2={g.h - g.pad} class="stroke-slate-300 dark:stroke-slate-600" stroke-width="1" />
						<line x1={g.pad} y1={g.pad} x2={g.pad} y2={g.h - g.pad} class="stroke-slate-300 dark:stroke-slate-600" stroke-width="1" />
						<text x={g.pad - 4} y={g.pad + 4} text-anchor="end" class="fill-slate-500 text-[10px]">{g.max.toFixed(2)}</text>
						<text x={g.pad - 4} y={g.h - g.pad} text-anchor="end" class="fill-slate-500 text-[10px]">{g.min.toFixed(2)}</text>
						<path d={g.path} fill="none" class="stroke-indigo-500" stroke-width="2" />
						{#each g.points as p}
							<circle cx={p.x} cy={p.y} r="2.5" class="fill-indigo-500">
								<title>{p.t ? new Date(p.t).toLocaleString() : ''}: {p.v.toFixed(4)}</title>
							</circle>
						{/each}
					</svg>
				</div>
				<p class="text-xs text-slate-500 dark:text-slate-400 mt-2">{chartDatapoints.length} datapoints · {chartStatistic}</p>
			{/if}
		</div>
	</div>
{/if}

<!-- Create Metric Stream Modal -->
{#if showCreateStream}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Metric Stream</h2>
<form onsubmit={(e) => { e.preventDefault(); createStream(); }} class="space-y-4">
<div>
<label for="cw-stream-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Stream Name</label>
<input id="cw-stream-name" type="text" bind:value={newStreamName} placeholder="e.g. my-metric-stream" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
</div>
<div>
<label for="cw-stream-firehose" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Firehose ARN</label>
<input id="cw-stream-firehose" type="text" bind:value={newStreamFirehoseArn} placeholder="arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
</div>
<div>
<label for="cw-stream-format" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Output Format</label>
<select id="cw-stream-format" bind:value={newStreamOutputFormat} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
{#each ['json', 'opentelemetry1.0', 'opentelemetry0.7'] as fmt}
<option value={fmt}>{fmt}</option>
{/each}
</select>
</div>
<div class="flex justify-end gap-3">
<button type="button" onclick={() => { showCreateStream = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
<button type="submit" disabled={creatingStream} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
{creatingStream ? 'Creating...' : 'Create Stream'}
</button>
</div>
</form>
</div>
</div>
{/if}

<!-- Create Anomaly Detector Modal -->
{#if showCreateAnomaly}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Anomaly Detector</h2>
<form onsubmit={(e) => { e.preventDefault(); createAnomalyDetector(); }} class="space-y-4">
<div>
<label for="cw-anomaly-namespace" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Namespace</label>
<input id="cw-anomaly-namespace" type="text" bind:value={newAnomalyNamespace} placeholder="e.g. AWS/EC2" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
</div>
<div>
<label for="cw-anomaly-metric" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Metric Name</label>
<input id="cw-anomaly-metric" type="text" bind:value={newAnomalyMetric} placeholder="e.g. CPUUtilization" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
</div>
<div>
<label for="cw-anomaly-stat" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Statistic</label>
<select id="cw-anomaly-stat" bind:value={newAnomalyStat} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
{#each ['Average', 'Sum', 'Maximum', 'Minimum', 'SampleCount'] as s}
<option value={s}>{s}</option>
{/each}
</select>
</div>
<div class="flex justify-end gap-3">
<button type="button" onclick={() => { showCreateAnomaly = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
<button type="submit" disabled={creatingAnomaly} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
{creatingAnomaly ? 'Creating...' : 'Create Detector'}
</button>
</div>
</form>
</div>
</div>
{/if}
