<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onDestroy, untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { urlState } from '$lib/url-state.svelte';
	import LiveDot from '$lib/components/LiveDot.svelte';
	import { getCloudWatchLogsClient } from '$lib/aws-client';
	import {
		DescribeLogGroupsCommand,
		DescribeLogStreamsCommand,
		FilterLogEventsCommand,
		CreateLogGroupCommand,
		DeleteLogGroupCommand,
		CreateLogStreamCommand,
		DeleteLogStreamCommand,
		PutRetentionPolicyCommand,
		DeleteRetentionPolicyCommand,
		PutSubscriptionFilterCommand,
		DescribeSubscriptionFiltersCommand,
		DeleteSubscriptionFilterCommand,
		PutMetricFilterCommand,
		DescribeMetricFiltersCommand,
		DeleteMetricFilterCommand,
		TestMetricFilterCommand,
		StartQueryCommand,
		GetQueryResultsCommand,
		StopQueryCommand,
		PutQueryDefinitionCommand,
		DescribeQueryDefinitionsCommand,
		DeleteQueryDefinitionCommand,
		CreateExportTaskCommand,
		DescribeExportTasksCommand,
		CancelExportTaskCommand,
		type LogGroup,
		type LogStream,
		type FilteredLogEvent,
		type MetricFilter,
		type SubscriptionFilter,
		type ExportTask,
		type QueryDefinition
	} from '@aws-sdk/client-cloudwatch-logs';
	import { toast } from 'svelte-sonner';
	import {
		List,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		ChevronRight,
		FileText,
		Clock,
		AlertCircle,
		Copy,
		Download,
		Play,
		Filter,
		Activity,
		Settings2,
		ChevronDown,
		ChevronUp,
		Database,
		Zap,
		BookOpen
	} from 'lucide-svelte';

	const cwl = regionalClient(getCloudWatchLogsClient);

	// ─── Top-level page tabs ──────────────────────────────────────────────────
	type PageTab = 'groups' | 'insights' | 'metric-filters' | 'subscription-filters' | 'export-tasks';
	// URL-backed (?tab=...); see url-state.svelte.ts. Read via untrack() inside
	// the onRegionChange effect below (switchTab() also writes it): without
	// untrack, every tab switch would re-trigger the region effect and
	// double-fetch — the same reasoning that already applied to this
	// variable before it moved into the URL, now widened to the whole shared
	// page.url (see url-state.svelte.ts's header comment on the
	// region-effect hazard).
	const pageTabParam = urlState<PageTab>('tab', 'groups');
	let pageTab = $derived(pageTabParam.get());

	// ─── Log Groups / Streams / Events drill-down ─────────────────────────────
	let activeView = $state<'groups' | 'streams' | 'events'>('groups');
	// URL-backed (?q=...); see url-state.svelte.ts. Not read inside the
	// onRegionChange effect below, so no untrack() is needed at that call
	// site.
	const searchQueryParam = urlState<string>('q', '');
	let searchQuery = $derived(searchQueryParam.get());

	// Log Groups
	let logGroups = $state<LogGroup[]>([]);
	let loading = $state(false);
	let selectedGroup = $state<LogGroup | null>(null);
	let showCreateGroup = $state(false);
	let creatingGroup = $state(false);
	let newGroupName = $state('');
	let newGroupRetention = $state(0);
	let deletingGroup = $state<string | null>(null);
	let groupSortField = $state<'name' | 'size' | 'created'>('name');
	let groupSortDesc = $state(false);
	let groupNextToken = $state<string | undefined>();

	// Log Streams
	let logStreams = $state<LogStream[]>([]);
	let loadingStreams = $state(false);
	let selectedStream = $state<LogStream | null>(null);
	let showCreateStream = $state(false);
	let creatingStream = $state(false);
	let newStreamName = $state('');
	let deletingStream = $state<string | null>(null);
	let streamSortField = $state<'name' | 'lastEventTime'>('lastEventTime');
	let streamSortDesc = $state(true);

	// Log Events
	let logEvents = $state<FilteredLogEvent[]>([]);
	let loadingEvents = $state(false);
	let filterPattern = $state('');
	let startTime = $state('');
	let endTime = $state('');
	let autoRefresh = $state(false);
	let autoRefreshInterval: ReturnType<typeof setInterval> | null = null;

	// ─── Insights ────────────────────────────────────────────────────────────
	let insightsQuery = $state("fields @timestamp, @message | sort @timestamp desc | limit 20");
	let insightsGroups = $state<string[]>([]);
	let insightsStartTime = $state('');
	let insightsEndTime = $state('');
	let insightsQueryId = $state<string | null>(null);
	let insightsStatus = $state<string>('');
	let insightsResults = $state<Array<Array<{field: string; value: string}>>>([]);
	let insightsRunning = $state(false);
	let insightsPollInterval: ReturnType<typeof setInterval> | null = null;
	let showSaveQueryDef = $state(false);
	let newQueryDefName = $state('');
	let queryDefinitions = $state<QueryDefinition[]>([]);
	let loadingQueryDefs = $state(false);

	// ─── Metric Filters ───────────────────────────────────────────────────────
	let mfGroup = $state('');
	let metricFilters = $state<MetricFilter[]>([]);
	let loadingMf = $state(false);
	let showCreateMf = $state(false);
	let mfFilterName = $state('');
	let mfFilterPattern = $state('');
	let mfMetricNamespace = $state('');
	let mfMetricName = $state('');
	let mfMetricValue = $state('1');
	let creatingMf = $state(false);
	let deletingMf = $state<string | null>(null);
	let mfTestEvents = $state('');
	let mfTestPattern = $state('');
	let mfTestResults = $state<Array<{eventMessage: string; eventNumber: number}>>([]);
	let testingMf = $state(false);

	// ─── Subscription Filters ─────────────────────────────────────────────────
	let sfGroup = $state('');
	let subFilters = $state<SubscriptionFilter[]>([]);
	let loadingSf = $state(false);
	let showCreateSf = $state(false);
	let sfFilterName = $state('');
	let sfFilterPattern = $state('');
	let sfDestinationArn = $state('');
	let creatingSf = $state(false);
	let deletingSf = $state<string | null>(null);

	// ─── Export Tasks ─────────────────────────────────────────────────────────
	let exportTasks = $state<ExportTask[]>([]);
	let loadingExport = $state(false);
	let showCreateExport = $state(false);
	let etTaskName = $state('');
	let etLogGroupName = $state('');
	let etDestination = $state('');
	let etDestinationPrefix = $state('');
	let etFrom = $state('');
	let etTo = $state('');
	let creatingExport = $state(false);
	let cancellingExport = $state<string | null>(null);

	// ─── Shared retention options (full AWS set) ──────────────────────────────
	const retentionOptions = [
		{ label: 'Never expire', value: 0 },
		{ label: '1 day', value: 1 },
		{ label: '3 days', value: 3 },
		{ label: '5 days', value: 5 },
		{ label: '7 days', value: 7 },
		{ label: '14 days', value: 14 },
		{ label: '30 days', value: 30 },
		{ label: '60 days', value: 60 },
		{ label: '90 days', value: 90 },
		{ label: '120 days', value: 120 },
		{ label: '150 days', value: 150 },
		{ label: '180 days', value: 180 },
		{ label: '365 days', value: 365 },
		{ label: '400 days', value: 400 },
		{ label: '545 days', value: 545 },
		{ label: '731 days', value: 731 },
		{ label: '1096 days', value: 1096 },
		{ label: '1827 days', value: 1827 },
		{ label: '2192 days', value: 2192 },
		{ label: '2557 days', value: 2557 },
		{ label: '2922 days', value: 2922 },
		{ label: '3288 days', value: 3288 },
		{ label: '3653 days', value: 3653 }
	];

	// ─── Derived ──────────────────────────────────────────────────────────────
	const filteredGroups = $derived(() => {
		let list = logGroups.filter((g) =>
			!searchQuery || (g.logGroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		);
		list = list.toSorted((a, b) => {
			let cmp = 0;
			if (groupSortField === 'name') cmp = (a.logGroupName ?? '').localeCompare(b.logGroupName ?? '');
			else if (groupSortField === 'size') cmp = (a.storedBytes ?? 0) - (b.storedBytes ?? 0);
			else if (groupSortField === 'created') cmp = (a.creationTime ?? 0) - (b.creationTime ?? 0);
			return groupSortDesc ? -cmp : cmp;
		});
		return list;
	});

	const filteredStreams = $derived(
		logStreams.filter((s) => !searchQuery || (s.logStreamName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const insightsResultFields = $derived(
		insightsResults.length > 0 ? insightsResults[0].map((f) => f.field) : []
	);

	function csvEsc(v: string): string {
		return /[",\n]/.test(v) ? `"${v.replaceAll('"', '""')}"` : v;
	}

	function exportInsightsCsv() {
		if (insightsResults.length === 0) return;
		const header = insightsResultFields.map((f) => csvEsc(f)).join(',');
		const rows = insightsResults.map((row) => {
			const lookup = new Map(row.map((f) => [f.field, f.value]));
			return insightsResultFields.map((field) => csvEsc(lookup.get(field) ?? '')).join(',');
		});
		const csv = [header, ...rows].join('\n');
		const blob = new Blob([csv], { type: 'text/csv' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = 'insights-results.csv';
		a.click();
		URL.revokeObjectURL(url);
	}

	// ─── Utility ──────────────────────────────────────────────────────────────
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

	async function copyToClipboard(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Failed to copy');
		}
	}

	function sortIcon(field: string, current: string, desc: boolean) {
		if (field !== current) return null;
		return desc ? '↓' : '↑';
	}

	// ─── Log Groups ───────────────────────────────────────────────────────────
	async function loadLogGroups(append = false) {
		loading = true;
		try {
			const resp = await cwl().send(new DescribeLogGroupsCommand({
				limit: 50,
				nextToken: append ? groupNextToken : undefined
			}));
			if (append) {
				logGroups = [...logGroups, ...(resp.logGroups ?? [])];
			} else {
				logGroups = resp.logGroups ?? [];
			}
			groupNextToken = resp.nextToken;
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
			const resp = await cwl().send(new DescribeLogStreamsCommand({
				logGroupName: groupName,
				orderBy: streamSortField === 'lastEventTime' ? 'LastEventTime' : 'LogStreamName',
				descending: streamSortDesc,
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
			const params: {
				logGroupName: string;
				logStreamNames: string[];
				filterPattern?: string;
				startTime?: number;
				endTime?: number;
			} = {
				logGroupName: selectedGroup.logGroupName ?? '',
				logStreamNames: [selectedStream.logStreamName ?? '']
			};
			if (filterPattern) params.filterPattern = filterPattern;
			if (startTime) params.startTime = new Date(startTime).getTime();
			if (endTime) params.endTime = new Date(endTime).getTime();
			const resp = await cwl().send(new FilterLogEventsCommand(params));
			logEvents = resp.events ?? [];
		} catch (e) {
			toast.error('Failed to load events: ' + String(e));
		} finally {
			loadingEvents = false;
		}
	}

	function toggleAutoRefresh() {
		autoRefresh = !autoRefresh;
		if (autoRefresh) {
			autoRefreshInterval = setInterval(() => loadEvents(), 5000);
		} else if (autoRefreshInterval !== null) {
			clearInterval(autoRefreshInterval);
			autoRefreshInterval = null;
		}
	}

	function downloadEventsJSON() {
		const blob = new Blob([JSON.stringify(logEvents, null, 2)], { type: 'application/json' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `${selectedStream?.logStreamName ?? 'events'}.json`;
		a.click();
		URL.revokeObjectURL(url);
	}

	async function createLogGroup() {
		if (!newGroupName.trim()) return;
		creatingGroup = true;
		try {
			await cwl().send(new CreateLogGroupCommand({ logGroupName: newGroupName.trim() }));
			if (newGroupRetention > 0) {
				await cwl().send(new PutRetentionPolicyCommand({
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
		if (!await confirmDestructive({ title: 'Delete Log Group', message: `Delete log group "${name}"? All log streams and retained data will be permanently removed.` })) return;
		deletingGroup = name;
		try {
			await cwl().send(new DeleteLogGroupCommand({ logGroupName: name }));
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

	async function updateRetention(groupName: string, days: number) {
		try {
			if (days === 0) {
				await cwl().send(new DeleteRetentionPolicyCommand({ logGroupName: groupName }));
			} else {
				await cwl().send(new PutRetentionPolicyCommand({ logGroupName: groupName, retentionInDays: days }));
			}
			toast.success('Retention policy updated');
			await loadLogGroups();
		} catch (e) {
			toast.error('Failed to update retention: ' + String(e));
		}
	}

	// ─── Log Streams ──────────────────────────────────────────────────────────
	async function createLogStream() {
		if (!newStreamName.trim() || !selectedGroup) return;
		creatingStream = true;
		try {
			await cwl().send(new CreateLogStreamCommand({
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

	async function deleteLogStream(streamName: string) {
		if (!selectedGroup) return;
		if (!await confirmDestructive({ title: 'Delete Log Stream', message: `Delete stream "${streamName}"? All retained log events will be removed.` })) return;
		deletingStream = streamName;
		try {
			await cwl().send(new DeleteLogStreamCommand({
				logGroupName: selectedGroup.logGroupName ?? '',
				logStreamName: streamName
			}));
			toast.success(`Stream "${streamName}" deleted`);
			if (selectedStream?.logStreamName === streamName) {
				selectedStream = null;
				activeView = 'streams';
			}
			await loadStreams(selectedGroup.logGroupName ?? '');
		} catch (e) {
			toast.error('Failed to delete stream: ' + String(e));
		} finally {
			deletingStream = null;
		}
	}

	async function toggleStreamSort(field: 'name' | 'lastEventTime') {
		if (streamSortField === field) {
			streamSortDesc = !streamSortDesc;
		} else {
			streamSortField = field;
			streamSortDesc = field === 'lastEventTime';
		}
		if (selectedGroup) {
			await loadStreams(selectedGroup.logGroupName ?? '');
		}
	}

	// ─── Insights ─────────────────────────────────────────────────────────────
	async function runInsightsQuery() {
		if (!insightsQuery.trim()) { toast.error('Enter a query string'); return; }
		if (insightsGroups.length === 0) { toast.error('Select at least one log group'); return; }
		insightsRunning = true;
		insightsStatus = 'Scheduled';
		insightsResults = [];
		try {
			const startMs = insightsStartTime ? new Date(insightsStartTime).getTime() : Date.now() - 3600000;
			const endMs = insightsEndTime ? new Date(insightsEndTime).getTime() : Date.now();
			const resp = await cwl().send(new StartQueryCommand({
				logGroupNames: insightsGroups,
				queryString: insightsQuery,
				startTime: Math.floor(startMs / 1000),
				endTime: Math.floor(endMs / 1000)
			}));
			insightsQueryId = resp.queryId ?? null;
			if (insightsPollInterval) clearInterval(insightsPollInterval);
			insightsPollInterval = setInterval(() => pollInsightsResults(), 2000);
		} catch (e) {
			toast.error('Failed to start query: ' + String(e));
			insightsRunning = false;
		}
	}

	async function pollInsightsResults() {
		if (!insightsQueryId) return;
		try {
			const resp = await cwl().send(new GetQueryResultsCommand({ queryId: insightsQueryId }));
			insightsStatus = resp.status ?? '';
			if (resp.status === 'Complete' || resp.status === 'Failed' || resp.status === 'Cancelled') {
				if (insightsPollInterval) { clearInterval(insightsPollInterval); insightsPollInterval = null; }
				insightsRunning = false;
				if (resp.status === 'Complete') {
					insightsResults = (resp.results ?? []).map((row) =>
						(row ?? []).map((f) => ({ field: f.field ?? '', value: f.value ?? '' }))
					);
				} else {
					toast.error(`Query ${resp.status}`);
				}
			}
		} catch (e) {
			if (insightsPollInterval) { clearInterval(insightsPollInterval); insightsPollInterval = null; }
			insightsRunning = false;
			toast.error('Failed to get query results: ' + String(e));
		}
	}

	async function stopInsightsQuery() {
		if (!insightsQueryId) return;
		try {
			await cwl().send(new StopQueryCommand({ queryId: insightsQueryId }));
			if (insightsPollInterval) { clearInterval(insightsPollInterval); insightsPollInterval = null; }
			insightsRunning = false;
			insightsStatus = 'Cancelled';
		} catch (e) {
			toast.error('Failed to stop query: ' + String(e));
		}
	}

	async function loadQueryDefinitions() {
		loadingQueryDefs = true;
		try {
			const resp = await cwl().send(new DescribeQueryDefinitionsCommand({}));
			queryDefinitions = resp.queryDefinitions ?? [];
		} catch (e) {
			toast.error('Failed to load query definitions: ' + String(e));
		} finally {
			loadingQueryDefs = false;
		}
	}

	async function saveQueryDefinition() {
		if (!newQueryDefName.trim()) { toast.error('Enter a name'); return; }
		try {
			await cwl().send(new PutQueryDefinitionCommand({
				name: newQueryDefName.trim(),
				queryString: insightsQuery,
				logGroupNames: insightsGroups.length > 0 ? insightsGroups : undefined
			}));
			toast.success('Query definition saved');
			showSaveQueryDef = false;
			newQueryDefName = '';
			await loadQueryDefinitions();
		} catch (e) {
			toast.error('Failed to save query definition: ' + String(e));
		}
	}

	async function deleteQueryDefinition(id: string) {
		if (!await confirmDestructive({ title: 'Delete Query Definition', message: 'Delete this saved query definition?' })) return;
		try {
			await cwl().send(new DeleteQueryDefinitionCommand({ queryDefinitionId: id }));
			toast.success('Query definition deleted');
			await loadQueryDefinitions();
		} catch (e) {
			toast.error('Failed to delete query definition: ' + String(e));
		}
	}

	function toggleInsightsGroup(groupName: string) {
		if (insightsGroups.includes(groupName)) {
			insightsGroups = insightsGroups.filter((g) => g !== groupName);
		} else {
			insightsGroups = [...insightsGroups, groupName];
		}
	}

	// ─── Metric Filters ───────────────────────────────────────────────────────
	async function loadMetricFilters() {
		if (!mfGroup) { metricFilters = []; return; }
		loadingMf = true;
		try {
			const resp = await cwl().send(new DescribeMetricFiltersCommand({ logGroupName: mfGroup }));
			metricFilters = resp.metricFilters ?? [];
		} catch (e) {
			toast.error('Failed to load metric filters: ' + String(e));
		} finally {
			loadingMf = false;
		}
	}

	async function createMetricFilter() {
		if (!mfFilterName.trim() || !mfGroup || !mfMetricNamespace.trim() || !mfMetricName.trim()) {
			toast.error('Fill in all required fields');
			return;
		}
		creatingMf = true;
		try {
			await cwl().send(new PutMetricFilterCommand({
				logGroupName: mfGroup,
				filterName: mfFilterName.trim(),
				filterPattern: mfFilterPattern,
				metricTransformations: [{
					metricNamespace: mfMetricNamespace.trim(),
					metricName: mfMetricName.trim(),
					metricValue: mfMetricValue || '1'
				}]
			}));
			toast.success(`Metric filter "${mfFilterName}" created`);
			showCreateMf = false;
			mfFilterName = ''; mfFilterPattern = ''; mfMetricNamespace = ''; mfMetricName = ''; mfMetricValue = '1';
			await loadMetricFilters();
		} catch (e) {
			toast.error('Failed to create metric filter: ' + String(e));
		} finally {
			creatingMf = false;
		}
	}

	async function deleteMetricFilter(filterName: string) {
		if (!await confirmDestructive({ title: 'Delete Metric Filter', message: `Delete metric filter "${filterName}"?` })) return;
		deletingMf = filterName;
		try {
			await cwl().send(new DeleteMetricFilterCommand({ logGroupName: mfGroup, filterName }));
			toast.success(`Metric filter "${filterName}" deleted`);
			await loadMetricFilters();
		} catch (e) {
			toast.error('Failed to delete metric filter: ' + String(e));
		} finally {
			deletingMf = null;
		}
	}

	async function testMetricFilter() {
		if (!mfTestPattern.trim()) { toast.error('Enter a filter pattern'); return; }
		const messages = mfTestEvents.split('\n').map((s) => s.trim()).filter(Boolean);
		if (messages.length === 0) { toast.error('Enter at least one test event'); return; }
		testingMf = true;
		try {
			const resp = await cwl().send(new TestMetricFilterCommand({
				filterPattern: mfTestPattern,
				logEventMessages: messages
			}));
			mfTestResults = (resp.matches ?? []).map((m) => ({
				eventMessage: m.eventMessage ?? '',
				eventNumber: Number(m.eventNumber ?? 0)
			}));
			if (mfTestResults.length === 0) {
				toast.info('No events matched the filter pattern');
			} else {
				toast.success(`${mfTestResults.length} event(s) matched`);
			}
		} catch (e) {
			toast.error('Test failed: ' + String(e));
		} finally {
			testingMf = false;
		}
	}

	// ─── Subscription Filters ─────────────────────────────────────────────────
	async function loadSubFilters() {
		if (!sfGroup) { subFilters = []; return; }
		loadingSf = true;
		try {
			const resp = await cwl().send(new DescribeSubscriptionFiltersCommand({ logGroupName: sfGroup }));
			subFilters = resp.subscriptionFilters ?? [];
		} catch (e) {
			toast.error('Failed to load subscription filters: ' + String(e));
		} finally {
			loadingSf = false;
		}
	}

	async function createSubFilter() {
		if (!sfFilterName.trim() || !sfGroup || !sfDestinationArn.trim()) {
			toast.error('Fill in all required fields');
			return;
		}
		creatingSf = true;
		try {
			await cwl().send(new PutSubscriptionFilterCommand({
				logGroupName: sfGroup,
				filterName: sfFilterName.trim(),
				filterPattern: sfFilterPattern,
				destinationArn: sfDestinationArn.trim()
			}));
			toast.success(`Subscription filter "${sfFilterName}" created`);
			showCreateSf = false;
			sfFilterName = ''; sfFilterPattern = ''; sfDestinationArn = '';
			await loadSubFilters();
		} catch (e) {
			toast.error('Failed to create subscription filter: ' + String(e));
		} finally {
			creatingSf = false;
		}
	}

	async function deleteSubFilter(filterName: string) {
		if (!await confirmDestructive({ title: 'Delete Subscription Filter', message: `Delete filter "${filterName}"?` })) return;
		deletingSf = filterName;
		try {
			await cwl().send(new DeleteSubscriptionFilterCommand({ logGroupName: sfGroup, filterName }));
			toast.success(`Filter "${filterName}" deleted`);
			await loadSubFilters();
		} catch (e) {
			toast.error('Failed to delete subscription filter: ' + String(e));
		} finally {
			deletingSf = null;
		}
	}

	// ─── Export Tasks ─────────────────────────────────────────────────────────
	async function loadExportTasks() {
		loadingExport = true;
		try {
			const resp = await cwl().send(new DescribeExportTasksCommand({}));
			exportTasks = resp.exportTasks ?? [];
		} catch (e) {
			toast.error('Failed to load export tasks: ' + String(e));
		} finally {
			loadingExport = false;
		}
	}

	async function createExportTask() {
		if (!etLogGroupName.trim() || !etDestination.trim() || !etFrom || !etTo) {
			toast.error('Fill in all required fields');
			return;
		}
		creatingExport = true;
		try {
			await cwl().send(new CreateExportTaskCommand({
				taskName: etTaskName.trim() || undefined,
				logGroupName: etLogGroupName.trim(),
				destination: etDestination.trim(),
				destinationPrefix: etDestinationPrefix.trim() || undefined,
				from: new Date(etFrom).getTime(),
				to: new Date(etTo).getTime()
			}));
			toast.success('Export task created');
			showCreateExport = false;
			etTaskName = ''; etLogGroupName = ''; etDestination = ''; etDestinationPrefix = ''; etFrom = ''; etTo = '';
			await loadExportTasks();
		} catch (e) {
			toast.error('Failed to create export task: ' + String(e));
		} finally {
			creatingExport = false;
		}
	}

	async function cancelExportTask(taskId: string) {
		cancellingExport = taskId;
		try {
			await cwl().send(new CancelExportTaskCommand({ taskId }));
			toast.success('Export task cancelled');
			await loadExportTasks();
		} catch (e) {
			toast.error('Failed to cancel export task: ' + String(e));
		} finally {
			cancellingExport = null;
		}
	}

	function exportStatusColor(status: string | undefined): string {
		switch (status) {
			case 'PENDING': return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/30 dark:text-yellow-300';
			case 'RUNNING': return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300';
			case 'COMPLETED': return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300';
			case 'CANCELLED': return 'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400';
			default: return 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300';
		}
	}

	// ─── Tab change ───────────────────────────────────────────────────────────
	function switchTab(tab: PageTab) {
		pageTabParam.set(tab);
		if (tab === 'insights') { loadQueryDefinitions(); }
		else if (tab === 'export-tasks') { loadExportTasks(); }
	}

	// ─── Lifecycle ────────────────────────────────────────────────────────────
	// Log groups/streams/events, insights results, metric/subscription filters,
	// and export tasks are all region-scoped, as are the two in-flight timers
	// (events auto-refresh, insights polling) — both close over state that
	// belongs to the old region, so stop them before resetting. `pageTab` is
	// read via untrack() because switchTab() also writes it: without untrack,
	// every tab switch would re-trigger this region effect and double-fetch.
	onRegionChange(() => {
		if (autoRefreshInterval !== null) {
			clearInterval(autoRefreshInterval);
			autoRefreshInterval = null;
		}
		autoRefresh = false;
		if (insightsPollInterval !== null) {
			clearInterval(insightsPollInterval);
			insightsPollInterval = null;
		}
		insightsRunning = false;
		insightsQueryId = null;
		insightsStatus = '';
		insightsResults = [];
		queryDefinitions = [];
		logGroups = [];
		groupNextToken = undefined;
		selectedGroup = null;
		selectedStream = null;
		logStreams = [];
		logEvents = [];
		activeView = 'groups';
		mfGroup = '';
		metricFilters = [];
		sfGroup = '';
		subFilters = [];
		exportTasks = [];
		const tab = untrack(() => pageTab);
		void loadLogGroups();
		if (tab === 'insights') void loadQueryDefinitions();
		else if (tab === 'export-tasks') void loadExportTasks();
	});

	onDestroy(() => {
		if (autoRefreshInterval !== null) clearInterval(autoRefreshInterval);
		if (insightsPollInterval !== null) clearInterval(insightsPollInterval);
	});
</script>

<div class="p-6 space-y-6">
	<!-- Header + top tabs -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<List class="w-7 h-7 text-indigo-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
					CloudWatch Logs
					<LiveDot service="cloudwatchlogs" />
				</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Monitor, query, and manage log data</p>
			</div>
		</div>
		<button
			onclick={() => { if (pageTab === 'groups') loadLogGroups(); else if (pageTab === 'export-tasks') loadExportTasks(); }}
			class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
		>
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<!-- Page Tabs -->
	<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
		{#each [
			{ key: 'groups', label: 'Log Groups', icon: Database },
			{ key: 'insights', label: 'Logs Insights', icon: Search },
			{ key: 'metric-filters', label: 'Metric Filters', icon: Filter },
			{ key: 'subscription-filters', label: 'Subscription Filters', icon: Zap },
			{ key: 'export-tasks', label: 'Export Tasks', icon: Download }
		] as tab}
			{@const TabIcon = tab.icon}
			<button
				onclick={() => switchTab(tab.key as typeof pageTab)}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors {pageTab === tab.key
					? 'border-indigo-600 text-indigo-600 dark:text-indigo-400'
					: 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'}"
			>
			<TabIcon class="w-4 h-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- ════════════════════════════════════════════════════════════════════════
	     TAB 1: LOG GROUPS
	     ════════════════════════════════════════════════════════════════════════ -->
	{#if pageTab === 'groups'}

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

		<!-- ── LOG GROUPS VIEW ── -->
		{#if activeView === 'groups'}
			<div class="flex flex-wrap items-center gap-3">
				<div class="flex-1 relative min-w-48">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input value={searchQuery} oninput={(e) => searchQueryParam.set(e.currentTarget.value)} type="text" placeholder="Search log groups..."
						class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white" />
				</div>
				<button onclick={() => (showCreateGroup = true)}
					class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium">
					<Plus class="w-4 h-4" /> Create Log Group
				</button>
			</div>

			{#if loading && logGroups.length === 0}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div></div>
			{:else if filteredGroups().length === 0}
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
								<th class="px-4 py-3 text-left">
									<button onclick={() => { groupSortField === 'name' ? (groupSortDesc = !groupSortDesc) : (groupSortField = 'name', groupSortDesc = false); }} class="flex items-center gap-1 hover:text-gray-900 dark:hover:text-white">
										Name {#if groupSortField === 'name'}{groupSortDesc ? '↓' : '↑'}{/if}
									</button>
								</th>
								<th class="px-4 py-3 text-left">Retention</th>
								<th class="px-4 py-3 text-left">
									<button onclick={() => { groupSortField === 'size' ? (groupSortDesc = !groupSortDesc) : (groupSortField = 'size', groupSortDesc = false); }} class="flex items-center gap-1 hover:text-gray-900 dark:hover:text-white">
										Size {#if groupSortField === 'size'}{groupSortDesc ? '↓' : '↑'}{/if}
									</button>
								</th>
								<th class="px-4 py-3 text-left">Filters</th>
								<th class="px-4 py-3 text-left">
									<button onclick={() => { groupSortField === 'created' ? (groupSortDesc = !groupSortDesc) : (groupSortField = 'created', groupSortDesc = false); }} class="flex items-center gap-1 hover:text-gray-900 dark:hover:text-white">
										Created {#if groupSortField === 'created'}{groupSortDesc ? '↓' : '↑'}{/if}
									</button>
								</th>
								<th class="px-4 py-3 text-left">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredGroups() as group}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3">
										<div class="flex items-center gap-2">
											<button onclick={() => selectGroup(group)}
												class="text-indigo-600 dark:text-indigo-400 hover:underline font-mono text-xs">{group.logGroupName}</button>
											<button onclick={() => copyToClipboard(group.arn ?? '')} title="Copy ARN"
												class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200">
												<Copy class="w-3 h-3" />
											</button>
										</div>
									</td>
									<td class="px-4 py-3">
										<select value={group.retentionInDays ?? 0}
											onchange={(e) => updateRetention(group.logGroupName ?? '', parseInt((e.target as HTMLSelectElement).value, 10))}
											class="text-xs border border-gray-200 dark:border-gray-700 rounded px-2 py-1 bg-white dark:bg-gray-900 text-gray-700 dark:text-gray-300">
											{#each retentionOptions as opt}
												<option value={opt.value}>{opt.label}</option>
											{/each}
										</select>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{formatBytes(group.storedBytes)}</td>
									<td class="px-4 py-3">
										{#if (group.metricFilterCount ?? 0) > 0}
											<span class="px-2 py-0.5 rounded-full bg-indigo-100 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300 text-xs">{group.metricFilterCount}</span>
										{:else}
											<span class="text-gray-400 text-xs">—</span>
										{/if}
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-xs">{formatTimestamp(group.creationTime)}</td>
									<td class="px-4 py-3">
										<button onclick={() => deleteLogGroup(group.logGroupName ?? '')} disabled={deletingGroup === group.logGroupName}
											class="text-red-500 hover:text-red-700 p-1 disabled:opacity-40">
											<Trash2 class="w-4 h-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
				{#if groupNextToken}
					<div class="flex justify-center">
						<button onclick={() => loadLogGroups(true)} disabled={loading}
							class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-600 dark:text-gray-300 disabled:opacity-50">
							{loading ? 'Loading...' : 'Load More'}
						</button>
					</div>
				{/if}
			{/if}
		{/if}

		<!-- ── LOG STREAMS VIEW ── -->
		{#if activeView === 'streams'}
			<div class="flex flex-wrap items-center gap-3">
				<div class="flex-1 relative min-w-48">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input value={searchQuery} oninput={(e) => searchQueryParam.set(e.currentTarget.value)} type="text" placeholder="Search streams..."
						class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white" />
				</div>
				<button onclick={() => (showCreateStream = true)}
					class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium">
					<Plus class="w-4 h-4" /> Create Stream
				</button>
			</div>

			{#if loadingStreams}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div></div>
			{:else if filteredStreams.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<FileText class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No log streams found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">
									<button onclick={() => toggleStreamSort('name')} class="flex items-center gap-1 hover:text-gray-900 dark:hover:text-white">
										Stream Name {#if streamSortField === 'name'}{streamSortDesc ? '↓' : '↑'}{/if}
									</button>
								</th>
								<th class="px-4 py-3 text-left">
									<button onclick={() => toggleStreamSort('lastEventTime')} class="flex items-center gap-1 hover:text-gray-900 dark:hover:text-white">
										Last Event {#if streamSortField === 'lastEventTime'}{streamSortDesc ? '↓' : '↑'}{/if}
									</button>
								</th>
								<th class="px-4 py-3 text-left">Last Ingestion</th>
								<th class="px-4 py-3 text-left">Size</th>
								<th class="px-4 py-3 text-left">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredStreams as stream}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3">
										<button onclick={() => selectStream(stream)}
											class="text-indigo-600 dark:text-indigo-400 hover:underline font-mono text-xs">{stream.logStreamName}</button>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-xs">{formatTimestamp(stream.lastEventTimestamp)}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-xs">{formatTimestamp(stream.lastIngestionTime)}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-xs">{formatBytes(stream.storedBytes)}</td>
									<td class="px-4 py-3">
										<button onclick={() => deleteLogStream(stream.logStreamName ?? '')} disabled={deletingStream === stream.logStreamName}
											class="text-red-500 hover:text-red-700 p-1 disabled:opacity-40">
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

		<!-- ── LOG EVENTS VIEW ── -->
		{#if activeView === 'events'}
			<div class="flex flex-wrap gap-3 items-end">
				<div>
					<label for="filter-pattern" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Filter Pattern</label>
					<input id="filter-pattern" bind:value={filterPattern} type="text" placeholder="e.g. ERROR"
						class="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="start-time" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Start Time</label>
					<input id="start-time" bind:value={startTime} type="datetime-local"
						class="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="end-time" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">End Time</label>
					<input id="end-time" bind:value={endTime} type="datetime-local"
						class="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white" />
				</div>
				<button onclick={loadEvents}
					class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium">
					<Search class="w-4 h-4" /> Search
				</button>
				<button onclick={toggleAutoRefresh}
					class="flex items-center gap-2 px-3 py-2 rounded-lg border text-sm {autoRefresh ? 'border-green-500 text-green-600 dark:text-green-400 bg-green-50 dark:bg-green-900/20' : 'border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300'}">
					<Clock class="w-4 h-4" /> {autoRefresh ? 'Stop refresh' : 'Auto-refresh (5s)'}
				</button>
				<button onclick={downloadEventsJSON}
					class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 text-sm">
					<Download class="w-4 h-4" /> Download JSON
				</button>
			</div>

			{#if loadingEvents}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div></div>
			{:else if logEvents.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<AlertCircle class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No events found</p>
					<p class="text-sm mt-1">Try adjusting the time range or filter pattern</p>
				</div>
			{:else}
				<p class="text-xs text-gray-500 dark:text-gray-400">{logEvents.length} events shown</p>
				<div class="bg-gray-900 rounded-xl border border-gray-700 p-4 font-mono text-xs space-y-1 max-h-96 overflow-y-auto">
					{#each logEvents as event}
						<div class="flex gap-3 group">
							<span class="text-gray-500 shrink-0">{formatTimestamp(event.timestamp)}</span>
							<button onclick={() => copyToClipboard(event.message ?? '')} title="Copy"
								class="text-gray-600 hover:text-gray-300 opacity-0 group-hover:opacity-100 shrink-0">
								<Copy class="w-3 h-3" />
							</button>
							<span class="text-green-400 whitespace-pre-wrap break-all">{event.message}</span>
						</div>
					{/each}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- ════════════════════════════════════════════════════════════════════════
	     TAB 2: LOGS INSIGHTS
	     ════════════════════════════════════════════════════════════════════════ -->
	{#if pageTab === 'insights'}
		<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
			<!-- Query panel -->
			<div class="lg:col-span-2 space-y-4">
				<div>
					<label for="insights-query" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Query</label>
					<textarea id="insights-query" bind:value={insightsQuery} rows="5"
						placeholder="fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc | limit 20"
						class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono text-gray-900 dark:text-white resize-none"></textarea>
				</div>
				<div class="flex gap-3 flex-wrap">
					<div class="flex-1">
						<label for="insights-start-time" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Start Time</label>
						<input id="insights-start-time" bind:value={insightsStartTime} type="datetime-local"
							class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white" />
					</div>
					<div class="flex-1">
						<label for="insights-end-time" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">End Time</label>
						<input id="insights-end-time" bind:value={insightsEndTime} type="datetime-local"
							class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white" />
					</div>
				</div>
				<div class="flex gap-3">
					{#if !insightsRunning}
						<button onclick={runInsightsQuery}
							class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium">
							<Play class="w-4 h-4" /> Run Query
						</button>
					{:else}
						<button onclick={stopInsightsQuery}
							class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm font-medium">
							Stop
						</button>
					{/if}
					<button onclick={() => (showSaveQueryDef = true)}
						class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
						<BookOpen class="w-4 h-4" /> Save Definition
					</button>
					{#if insightsStatus}
						<span class="flex items-center px-3 py-1 rounded-full text-xs font-medium {insightsStatus === 'Complete' ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300' : insightsStatus === 'Running' ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300' : 'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400'}">
							{insightsStatus}
						</span>
					{/if}
				</div>

				<!-- Results -->
				{#if insightsResults.length > 0}
					<div>
						<div class="flex items-center justify-between mb-2">
							<p class="text-xs text-gray-500 dark:text-gray-400">{insightsResults.length} rows</p>
							<button onclick={exportInsightsCsv} class="px-2.5 py-1 text-xs rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-700">Export CSV</button>
						</div>
						<div class="overflow-x-auto rounded-xl border border-gray-200 dark:border-gray-700">
							<table class="w-full text-xs">
								<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400">
									<tr>
										{#each insightsResultFields as field}
											<th class="px-3 py-2 text-left font-medium">{field}</th>
										{/each}
									</tr>
								</thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800 font-mono">
									{#each insightsResults as row}
										<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
											{#each row as cell}
												<td class="px-3 py-2 text-gray-700 dark:text-gray-300 max-w-xs truncate">{cell.value}</td>
											{/each}
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					</div>
				{:else if insightsStatus === 'Complete'}
					<p class="text-gray-500 dark:text-gray-400 text-sm">No results returned</p>
				{/if}
			</div>

			<!-- Sidebar: Log Group selector + saved queries -->
			<div class="space-y-4">
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">Log Groups</h3>
					<div class="space-y-1 max-h-48 overflow-y-auto">
						{#each logGroups as group}
							<label class="flex items-center gap-2 text-xs cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-800 px-2 py-1 rounded">
								<input type="checkbox" checked={insightsGroups.includes(group.logGroupName ?? '')}
									onchange={() => toggleInsightsGroup(group.logGroupName ?? '')}
									class="rounded border-gray-300 text-indigo-600" />
								<span class="font-mono truncate text-gray-700 dark:text-gray-300">{group.logGroupName}</span>
							</label>
						{/each}
					</div>
				</div>

				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">Saved Queries</h3>
					{#if loadingQueryDefs}
						<div class="text-xs text-gray-400">Loading...</div>
					{:else if queryDefinitions.length === 0}
						<p class="text-xs text-gray-400">No saved queries</p>
					{:else}
						<div class="space-y-2">
							{#each queryDefinitions as qd}
								<div class="flex items-center justify-between gap-2">
									<button onclick={() => (insightsQuery = qd.queryString ?? '')}
										class="text-xs text-indigo-600 dark:text-indigo-400 hover:underline truncate text-left">{qd.name}</button>
									<button onclick={() => deleteQueryDefinition(qd.queryDefinitionId ?? '')}
										class="text-red-400 hover:text-red-600 shrink-0">
										<Trash2 class="w-3 h-3" />
									</button>
								</div>
							{/each}
						</div>
					{/if}
				</div>
			</div>
		</div>
	{/if}

	<!-- ════════════════════════════════════════════════════════════════════════
	     TAB 3: METRIC FILTERS
	     ════════════════════════════════════════════════════════════════════════ -->
	{#if pageTab === 'metric-filters'}
		<div class="space-y-4">
			<div class="flex flex-wrap gap-3 items-end">
				<div>
					<label for="mf-group" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Log Group</label>
					<select id="mf-group" bind:value={mfGroup} onchange={loadMetricFilters}
						class="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white min-w-48">
						<option value="">Select a log group...</option>
						{#each logGroups as group}
							<option value={group.logGroupName}>{group.logGroupName}</option>
						{/each}
					</select>
				</div>
				{#if mfGroup}
					<button onclick={() => (showCreateMf = true)}
						class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium">
						<Plus class="w-4 h-4" /> Create Metric Filter
					</button>
				{/if}
			</div>

			{#if mfGroup}
				{#if loadingMf}
					<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div></div>
				{:else if metricFilters.length === 0}
					<div class="text-center py-12 text-gray-500 dark:text-gray-400">
						<Filter class="w-10 h-10 mx-auto mb-2 opacity-40" />
						<p class="font-medium">No metric filters</p>
					</div>
				{:else}
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
								<tr>
									<th class="px-4 py-3 text-left">Filter Name</th>
									<th class="px-4 py-3 text-left">Pattern</th>
									<th class="px-4 py-3 text-left">Metric</th>
									<th class="px-4 py-3 text-left">Created</th>
									<th class="px-4 py-3 text-left">Actions</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each metricFilters as mf}
									<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
										<td class="px-4 py-3 font-mono text-xs text-gray-900 dark:text-white">{mf.filterName}</td>
										<td class="px-4 py-3 font-mono text-xs text-gray-600 dark:text-gray-400">{mf.filterPattern || '(all events)'}</td>
										<td class="px-4 py-3 text-xs text-gray-600 dark:text-gray-400">
											{#each (mf.metricTransformations ?? []) as t}
												<span class="font-mono">{t.metricNamespace}/{t.metricName}</span>
											{/each}
										</td>
										<td class="px-4 py-3 text-xs text-gray-600 dark:text-gray-400">{formatTimestamp(mf.creationTime)}</td>
										<td class="px-4 py-3">
											<button onclick={() => deleteMetricFilter(mf.filterName ?? '')} disabled={deletingMf === mf.filterName}
												class="text-red-500 hover:text-red-700 p-1 disabled:opacity-40">
												<Trash2 class="w-4 h-4" />
											</button>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}

				<!-- Test metric filter -->
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4 space-y-3">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Test Metric Filter Pattern</h3>
					<div class="flex gap-3">
						<input bind:value={mfTestPattern} type="text" placeholder="Filter pattern (e.g. ERROR)"
							class="flex-1 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white" />
						<button onclick={testMetricFilter} disabled={testingMf}
							class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium disabled:opacity-50">
							<Activity class="w-4 h-4" /> Test
						</button>
					</div>
					<textarea bind:value={mfTestEvents} rows="4" placeholder="Enter test log events (one per line)&#10;[ERROR] Connection failed&#10;[INFO] Request completed"
						class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono text-gray-900 dark:text-white resize-none"></textarea>
					{#if mfTestResults.length > 0}
						<div class="space-y-1">
							<p class="text-xs text-green-600 dark:text-green-400 font-medium">{mfTestResults.length} event(s) matched:</p>
							{#each mfTestResults as r}
								<div class="text-xs font-mono bg-green-50 dark:bg-green-900/20 text-green-800 dark:text-green-200 px-3 py-1 rounded">#{r.eventNumber}: {r.eventMessage}</div>
							{/each}
						</div>
					{/if}
				</div>
			{/if}
		</div>
	{/if}

	<!-- ════════════════════════════════════════════════════════════════════════
	     TAB 4: SUBSCRIPTION FILTERS
	     ════════════════════════════════════════════════════════════════════════ -->
	{#if pageTab === 'subscription-filters'}
		<div class="space-y-4">
			<div class="flex flex-wrap gap-3 items-end">
				<div>
					<label for="sf-group" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Log Group</label>
					<select id="sf-group" bind:value={sfGroup} onchange={loadSubFilters}
						class="px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white min-w-48">
						<option value="">Select a log group...</option>
						{#each logGroups as group}
							<option value={group.logGroupName}>{group.logGroupName}</option>
						{/each}
					</select>
				</div>
				{#if sfGroup}
					<button onclick={() => (showCreateSf = true)}
						class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium">
						<Plus class="w-4 h-4" /> Create Subscription Filter
					</button>
				{/if}
			</div>

			{#if sfGroup}
				{#if loadingSf}
					<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div></div>
				{:else if subFilters.length === 0}
					<div class="text-center py-12 text-gray-500 dark:text-gray-400">
						<Zap class="w-10 h-10 mx-auto mb-2 opacity-40" />
						<p class="font-medium">No subscription filters</p>
					</div>
				{:else}
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
								<tr>
									<th class="px-4 py-3 text-left">Filter Name</th>
									<th class="px-4 py-3 text-left">Pattern</th>
									<th class="px-4 py-3 text-left">Destination ARN</th>
									<th class="px-4 py-3 text-left">Created</th>
									<th class="px-4 py-3 text-left">Actions</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each subFilters as sf}
									<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
										<td class="px-4 py-3 font-mono text-xs text-gray-900 dark:text-white">{sf.filterName}</td>
										<td class="px-4 py-3 font-mono text-xs text-gray-600 dark:text-gray-400">{sf.filterPattern || '(all events)'}</td>
										<td class="px-4 py-3 font-mono text-xs text-gray-600 dark:text-gray-400 max-w-xs truncate">{sf.destinationArn}</td>
										<td class="px-4 py-3 text-xs text-gray-600 dark:text-gray-400">{formatTimestamp(sf.creationTime)}</td>
										<td class="px-4 py-3">
											<button onclick={() => deleteSubFilter(sf.filterName ?? '')} disabled={deletingSf === sf.filterName}
												class="text-red-500 hover:text-red-700 p-1 disabled:opacity-40">
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
		</div>
	{/if}

	<!-- ════════════════════════════════════════════════════════════════════════
	     TAB 5: EXPORT TASKS
	     ════════════════════════════════════════════════════════════════════════ -->
	{#if pageTab === 'export-tasks'}
		<div class="space-y-4">
			<div class="flex justify-between items-center">
				<h2 class="text-base font-semibold text-gray-700 dark:text-gray-300">Export Tasks</h2>
				<button onclick={() => (showCreateExport = true)}
					class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium">
					<Plus class="w-4 h-4" /> Create Export Task
				</button>
			</div>

			{#if loadingExport}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-indigo-600 border-t-transparent rounded-full"></div></div>
			{:else if exportTasks.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Download class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No export tasks</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Task Name / ID</th>
								<th class="px-4 py-3 text-left">Log Group</th>
								<th class="px-4 py-3 text-left">Destination</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Created</th>
								<th class="px-4 py-3 text-left">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each exportTasks as task}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
									<td class="px-4 py-3">
										<div class="text-xs font-medium text-gray-900 dark:text-white">{task.taskName || '—'}</div>
										<div class="text-xs font-mono text-gray-400">{task.taskId}</div>
									</td>
									<td class="px-4 py-3 font-mono text-xs text-gray-600 dark:text-gray-400">{task.logGroupName}</td>
									<td class="px-4 py-3 font-mono text-xs text-gray-600 dark:text-gray-400 max-w-xs truncate">{task.destination}</td>
									<td class="px-4 py-3">
										<span class="px-2 py-0.5 rounded-full text-xs font-medium {exportStatusColor(task.status?.code)}">{task.status?.code ?? '—'}</span>
									</td>
									<td class="px-4 py-3 text-xs text-gray-600 dark:text-gray-400">{formatTimestamp(task.executionInfo?.creationTime)}</td>
									<td class="px-4 py-3">
										{#if task.status?.code === 'PENDING' || task.status?.code === 'RUNNING'}
											<button onclick={() => cancelExportTask(task.taskId ?? '')} disabled={cancellingExport === task.taskId}
												class="text-red-500 hover:text-red-700 text-xs disabled:opacity-40">Cancel</button>
										{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}
</div>

<!-- ════════════════════════════════════════════════════════════════════════════
     MODALS
     ════════════════════════════════════════════════════════════════════════════ -->

<!-- Create Log Group -->
{#if showCreateGroup}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Log Group</h2>
			<div>
				<label for="group-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Group Name</label>
				<input id="group-name" bind:value={newGroupName} type="text" placeholder="/aws/lambda/my-function"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="retention" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Retention Period</label>
				<select id="retention" bind:value={newGroupRetention}
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white">
					{#each retentionOptions as opt}
						<option value={opt.value}>{opt.label}</option>
					{/each}
				</select>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateGroup = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-700 dark:text-gray-300">Cancel</button>
				<button onclick={createLogGroup} disabled={creatingGroup || !newGroupName.trim()}
					class="flex-1 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 disabled:opacity-50">
					{creatingGroup ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Log Stream -->
{#if showCreateStream}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Log Stream</h2>
			<p class="text-sm text-gray-500 dark:text-gray-400">In: <span class="font-mono">{selectedGroup?.logGroupName}</span></p>
			<div>
				<label for="stream-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Stream Name</label>
				<input id="stream-name" bind:value={newStreamName} type="text" placeholder="my-stream"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateStream = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-700 dark:text-gray-300">Cancel</button>
				<button onclick={createLogStream} disabled={creatingStream || !newStreamName.trim()}
					class="flex-1 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 disabled:opacity-50">
					{creatingStream ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Metric Filter -->
{#if showCreateMf}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Metric Filter</h2>
			<div>
				<label for="mf-filter-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Filter Name *</label>
				<input id="mf-filter-name" bind:value={mfFilterName} type="text" placeholder="my-error-filter"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="mf-filter-pattern" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Filter Pattern</label>
				<input id="mf-filter-pattern" bind:value={mfFilterPattern} type="text" placeholder="ERROR"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="mf-metric-namespace" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Metric Namespace *</label>
					<input id="mf-metric-namespace" bind:value={mfMetricNamespace} type="text" placeholder="MyApp"
						class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="mf-metric-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Metric Name *</label>
					<input id="mf-metric-name" bind:value={mfMetricName} type="text" placeholder="ErrorCount"
						class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
				</div>
			</div>
			<div>
				<label for="mf-metric-value" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Metric Value</label>
				<input id="mf-metric-value" bind:value={mfMetricValue} type="text" placeholder="1"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateMf = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-700 dark:text-gray-300">Cancel</button>
				<button onclick={createMetricFilter} disabled={creatingMf}
					class="flex-1 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 disabled:opacity-50">
					{creatingMf ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Subscription Filter -->
{#if showCreateSf}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Subscription Filter</h2>
			<div>
				<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Filter Name *</label>
				<input bind:value={sfFilterName} type="text" placeholder="my-filter"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
			</div>
			<div>
				<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Filter Pattern</label>
				<input bind:value={sfFilterPattern} type="text" placeholder="ERROR (empty matches all)"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono text-gray-900 dark:text-white" />
			</div>
			<div>
				<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Destination ARN *</label>
				<input bind:value={sfDestinationArn} type="text" placeholder="arn:aws:lambda:..."
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono text-gray-900 dark:text-white" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateSf = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-700 dark:text-gray-300">Cancel</button>
				<button onclick={createSubFilter} disabled={creatingSf}
					class="flex-1 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 disabled:opacity-50">
					{creatingSf ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Export Task -->
{#if showCreateExport}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Export Task</h2>
			<div>
				<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Task Name (optional)</label>
				<input bind:value={etTaskName} type="text" placeholder="my-export"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
			</div>
			<div>
				<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Log Group Name *</label>
				<select bind:value={etLogGroupName}
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white">
					<option value="">Select...</option>
					{#each logGroups as group}
						<option value={group.logGroupName}>{group.logGroupName}</option>
					{/each}
				</select>
			</div>
			<div>
				<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">S3 Bucket *</label>
				<input bind:value={etDestination} type="text" placeholder="my-bucket"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
			</div>
			<div>
				<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Prefix (optional)</label>
				<input bind:value={etDestinationPrefix} type="text" placeholder="exports/my-group"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">From *</label>
					<input bind:value={etFrom} type="datetime-local"
						class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
				</div>
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">To *</label>
					<input bind:value={etTo} type="datetime-local"
						class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
				</div>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateExport = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-700 dark:text-gray-300">Cancel</button>
				<button onclick={createExportTask} disabled={creatingExport}
					class="flex-1 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 disabled:opacity-50">
					{creatingExport ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Save Query Definition -->
{#if showSaveQueryDef}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Save Query Definition</h2>
			<div>
				<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name *</label>
				<input bind:value={newQueryDefName} type="text" placeholder="My error query"
					class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showSaveQueryDef = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-700 dark:text-gray-300">Cancel</button>
				<button onclick={saveQueryDefinition} disabled={!newQueryDefName.trim()}
					class="flex-1 px-4 py-2 rounded-lg bg-indigo-600 text-white text-sm font-medium hover:bg-indigo-700 disabled:opacity-50">Save</button>
			</div>
		</div>
	</div>
{/if}
