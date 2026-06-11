<script lang="ts">
	import { onMount } from 'svelte';
	import { getSWFClient } from '$lib/aws-client';
	import {
		ListDomainsCommand,
		ListWorkflowTypesCommand,
		ListActivityTypesCommand,
		ListOpenWorkflowExecutionsCommand,
		ListClosedWorkflowExecutionsCommand,
		GetWorkflowExecutionHistoryCommand,
		RegisterDomainCommand,
		StartWorkflowExecutionCommand,
		TerminateWorkflowExecutionCommand,
		RequestCancelWorkflowExecutionCommand,
		SignalWorkflowExecutionCommand,
		TagResourceCommand,
		ListTagsForResourceCommand,
		PollForActivityTaskCommand,
		PollForDecisionTaskCommand,
		RespondActivityTaskCompletedCommand,
		RespondActivityTaskFailedCommand,
		RespondDecisionTaskCompletedCommand,
		DescribeWorkflowExecutionCommand,
		DescribeActivityTypeCommand,
		type SWFClient,
		type DomainInfo,
		type WorkflowTypeInfo,
		type ActivityTypeInfo,
		type WorkflowExecutionInfo,
		type HistoryEvent,
		type WorkflowExecutionDetail,
		type ActivityTypeDetail
	} from '@aws-sdk/client-swf';
	import { toast } from 'svelte-sonner';
	import {
		Workflow,
		RefreshCw,
		Search,
		Globe,
		Play,
		XCircle,
		Clock,
		CheckCircle,
		AlertCircle,
		Plus,
		List,
		Activity,
		Bell,
		Tag,
		ChevronRight,
		ChevronDown,
		History
	} from 'lucide-svelte';

	let swfClient: SWFClient | undefined;
	function swf(): SWFClient {
		return (swfClient ??= getSWFClient());
	}

	type Tab = 'domains' | 'workflows' | 'executions' | 'history' | 'polling';

	let loading = $state(false);
	let activeTab = $state<Tab>('domains');
	let searchQuery = $state('');

	// Data
	let domains = $state<DomainInfo[]>([]);
	let workflowTypes = $state<WorkflowTypeInfo[]>([]);
	let activityTypes = $state<ActivityTypeInfo[]>([]);
	let openExecs = $state<WorkflowExecutionInfo[]>([]);
	let closedExecs = $state<WorkflowExecutionInfo[]>([]);
	let historyEvents = $state<HistoryEvent[]>([]);
	let historyWorkflowId = $state('');
	let historyDomain = $state('');
	let selectedDomain = $state('');

	// Polling state
	let pollDomain = $state('');
	let pollTaskList = $state('default');
	let pollMode = $state<'activity' | 'decision'>('activity');
	let polledTask = $state<Record<string, unknown> | null>(null);
	let polling = $state(false);
	let pollResult = $state('');
	let pollReason = $state('');
	let respondingTask = $state(false);

	// Signal modal
	let showSignal = $state(false);
	let signalDomain = $state('');
	let signalWorkflowId = $state('');
	let signalName = $state('');
	let signalInput = $state('');
	let sendingSignal = $state(false);

	// Start Execution modal
	let showStartExec = $state(false);
	let startDomain = $state('');
	let startWorkflowId = $state('');
	let startWorkflowName = $state('');
	let startWorkflowVersion = $state('');
	let startingExec = $state(false);

	// Register Domain modal
	let showRegisterDomain = $state(false);
	let newDomainName = $state('');
	let newDomainDesc = $state('');
	let registeringDomain = $state(false);

	// Tags modal
	let showTags = $state(false);
	let tagsArn = $state('');
	let tagsList = $state<Array<{ key: string; value: string }>>([]);
	let newTagKey = $state('');
	let newTagValue = $state('');
	let loadingTags = $state(false);

	const filteredDomains = $derived(
		domains.filter((d) => (d.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredWfTypes = $derived(
		workflowTypes.filter((w) =>
			(w.workflowType?.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredOpenExecs = $derived(
		openExecs.filter((e) =>
			(e.execution?.workflowId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredClosedExecs = $derived(
		closedExecs.filter((e) =>
			(e.execution?.workflowId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	async function loadDomains() {
		const out = await swf().send(
			new ListDomainsCommand({ registrationStatus: 'REGISTERED', maximumPageSize: 100 })
		);
		domains = out.domainInfos ?? [];
		// also fetch deprecated
		const dep = await swf().send(
			new ListDomainsCommand({ registrationStatus: 'DEPRECATED', maximumPageSize: 100 })
		);
		domains = [...domains, ...(dep.domainInfos ?? [])];
	}

	async function loadWorkflowTypes() {
		if (!selectedDomain) {
			workflowTypes = [];
			activityTypes = [];
			return;
		}
		const [wfResp, atResp] = await Promise.all([
			swf().send(
				new ListWorkflowTypesCommand({
					domain: selectedDomain,
					registrationStatus: 'REGISTERED',
					maximumPageSize: 100
				})
			),
			swf().send(
				new ListActivityTypesCommand({
					domain: selectedDomain,
					registrationStatus: 'REGISTERED',
					maximumPageSize: 100
				})
			)
		]);
		workflowTypes = wfResp.typeInfos ?? [];
		activityTypes = atResp.typeInfos ?? [];
	}

	async function loadExecutions() {
		if (!selectedDomain) {
			openExecs = [];
			closedExecs = [];
			return;
		}
		const now = new Date();
		const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
		const [openResp, closedResp] = await Promise.all([
			swf().send(
				new ListOpenWorkflowExecutionsCommand({
					domain: selectedDomain,
					startTimeFilter: { oldestDate: oneWeekAgo, latestDate: now },
					maximumPageSize: 100
				})
			),
			swf().send(
				new ListClosedWorkflowExecutionsCommand({
					domain: selectedDomain,
					startTimeFilter: { oldestDate: oneWeekAgo, latestDate: now },
					maximumPageSize: 100
				})
			)
		]);
		openExecs = openResp.executionInfos ?? [];
		closedExecs = closedResp.executionInfos ?? [];
	}

	async function loadAll() {
		loading = true;
		try {
			await loadDomains();
			await loadWorkflowTypes();
			await loadExecutions();
		} catch (e) {
			toast.error('Failed to load SWF data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	let historyRunId = $state('');
	let historyEventFilter = $state('');
	let expandedEventId = $state<number | null>(null);
	let execDetail = $state<WorkflowExecutionDetail | null>(null);

	const filteredHistoryEvents = $derived(
		historyEventFilter.trim()
			? historyEvents.filter((e) =>
					(e.eventType ?? '').toLowerCase().includes(historyEventFilter.trim().toLowerCase())
				)
			: historyEvents
	);

	// Extract the input/result/details payload attributes attached to a history
	// event so the operator can inspect the workflow/activity I/O.
	function eventPayload(event: HistoryEvent): Record<string, unknown> | null {
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		const ev = event as unknown as Record<string, any>;
		const attrKey = Object.keys(ev).find((k) => k.endsWith('EventAttributes'));
		if (!attrKey) return null;
		const attrs = ev[attrKey] as Record<string, unknown>;
		if (!attrs) return null;
		const picked: Record<string, unknown> = {};
		for (const key of ['input', 'result', 'details', 'reason', 'control', 'signalName', 'cause']) {
			if (attrs[key] !== undefined) picked[key] = attrs[key];
		}
		return Object.keys(picked).length > 0 ? picked : attrs;
	}

	function toggleEvent(id: number | undefined) {
		if (id === undefined) return;
		expandedEventId = expandedEventId === id ? null : id;
	}

	async function loadHistory() {
		if (!historyDomain || !historyWorkflowId) return;
		try {
			const resp = await swf().send(
				new GetWorkflowExecutionHistoryCommand({
					domain: historyDomain,
					execution: { workflowId: historyWorkflowId, runId: historyRunId || '' }
				})
			);
			historyEvents = resp.events ?? [];
			// Also fetch the execution detail for the open-execution input/output.
			execDetail = null;
			if (historyRunId) {
				try {
					const det = await swf().send(
						new DescribeWorkflowExecutionCommand({
							domain: historyDomain,
							execution: { workflowId: historyWorkflowId, runId: historyRunId }
						})
					);
					execDetail = det ?? null;
				} catch {
					execDetail = null;
				}
			}
		} catch (e) {
			toast.error('Failed to load history: ' + String(e));
		}
	}

	async function openHistory(domain: string, workflowId: string, runId = '') {
		historyDomain = domain;
		historyWorkflowId = workflowId;
		historyRunId = runId;
		historyEvents = [];
		expandedEventId = null;
		execDetail = null;
		activeTab = 'history';
		await loadHistory();
	}

	// Activity-type detail (timeouts / heartbeat config).
	let showActivityDetail = $state(false);
	let activityDetail = $state<ActivityTypeDetail | null>(null);
	let loadingActivityDetail = $state(false);

	async function viewActivityType(at: ActivityTypeInfo) {
		if (!selectedDomain || !at.activityType?.name || !at.activityType?.version) return;
		showActivityDetail = true;
		activityDetail = null;
		loadingActivityDetail = true;
		try {
			const resp = await swf().send(
				new DescribeActivityTypeCommand({
					domain: selectedDomain,
					activityType: { name: at.activityType.name, version: at.activityType.version }
				})
			);
			activityDetail = resp ?? null;
		} catch (e) {
			toast.error('Failed to load activity type: ' + String(e));
		} finally {
			loadingActivityDetail = false;
		}
	}

	async function registerDomain() {
		if (!newDomainName.trim()) {
			toast.error('Domain name is required');
			return;
		}
		registeringDomain = true;
		try {
			await swf().send(
				new RegisterDomainCommand({
					name: newDomainName.trim(),
					description: newDomainDesc || undefined,
					workflowExecutionRetentionPeriodInDays: '30'
				})
			);
			toast.success(`Domain "${newDomainName}" registered`);
			showRegisterDomain = false;
			newDomainName = '';
			newDomainDesc = '';
			await loadDomains();
		} catch (e) {
			toast.error('Failed to register domain: ' + String(e));
		} finally {
			registeringDomain = false;
		}
	}

	async function startExecution() {
		if (!startDomain || !startWorkflowId.trim()) {
			toast.error('Domain and Workflow ID are required');
			return;
		}
		startingExec = true;
		try {
			await swf().send(new StartWorkflowExecutionCommand({
				domain: startDomain,
				workflowId: startWorkflowId.trim(),
				workflowType: {
					name: startWorkflowName || 'default',
					version: startWorkflowVersion || '1.0'
				}
			}));
			toast.success(`Workflow execution started`);
			showStartExec = false;
			startWorkflowId = '';
			await loadExecutions();
		} catch (e) {
			toast.error('Failed to start execution: ' + String(e));
		} finally {
			startingExec = false;
		}
	}

	async function terminateExecution(domain: string, workflowId: string) {
		try {
			await swf().send(
				new TerminateWorkflowExecutionCommand({
					domain,
					workflowId,
					reason: 'Terminated via gopherstack UI'
				})
			);
			toast.success(`Execution ${workflowId} terminated`);
			await loadExecutions();
		} catch (e) {
			toast.error('Failed to terminate: ' + String(e));
		}
	}

	async function cancelExecution(domain: string, workflowId: string) {
		try {
			await swf().send(
				new RequestCancelWorkflowExecutionCommand({ domain, workflowId })
			);
			toast.success(`Cancel requested for ${workflowId}`);
			await loadExecutions();
		} catch (e) {
			toast.error('Failed to request cancel: ' + String(e));
		}
	}

	async function sendSignal() {
		if (!signalDomain || !signalWorkflowId || !signalName.trim()) {
			toast.error('Domain, Workflow ID, and Signal Name are required');
			return;
		}
		sendingSignal = true;
		try {
			await swf().send(
				new SignalWorkflowExecutionCommand({
					domain: signalDomain,
					workflowId: signalWorkflowId,
					signalName: signalName.trim(),
					input: signalInput || undefined
				})
			);
			toast.success(`Signal "${signalName}" sent`);
			showSignal = false;
			signalName = '';
			signalInput = '';
		} catch (e) {
			toast.error('Failed to send signal: ' + String(e));
		} finally {
			sendingSignal = false;
		}
	}

	function openSignalModal(domain: string, workflowId: string) {
		signalDomain = domain;
		signalWorkflowId = workflowId;
		signalName = '';
		signalInput = '';
		showSignal = true;
	}

	async function pollTask() {
		if (!pollDomain || !pollTaskList) {
			toast.error('Domain and task list are required');
			return;
		}
		polling = true;
		polledTask = null;
		try {
			if (pollMode === 'activity') {
				const resp = await swf().send(
					new PollForActivityTaskCommand({ domain: pollDomain, taskList: { name: pollTaskList } })
				);
				polledTask = resp.taskToken ? (resp as unknown as Record<string, unknown>) : null;
			} else {
				const resp = await swf().send(
					new PollForDecisionTaskCommand({ domain: pollDomain, taskList: { name: pollTaskList } })
				);
				polledTask = resp.taskToken ? (resp as unknown as Record<string, unknown>) : null;
			}
			if (!polledTask) toast.info('No tasks in queue');
		} catch (e) {
			toast.error('Poll failed: ' + String(e));
		} finally {
			polling = false;
		}
	}

	async function respondTask(outcome: 'completed' | 'failed') {
		const token = String((polledTask as Record<string, unknown>)?.taskToken ?? '');
		if (!token) return;
		respondingTask = true;
		try {
			if (pollMode === 'activity') {
				if (outcome === 'completed') {
					await swf().send(
						new RespondActivityTaskCompletedCommand({ taskToken: token, result: pollResult })
					);
				} else {
					await swf().send(
						new RespondActivityTaskFailedCommand({ taskToken: token, reason: pollReason })
					);
				}
			} else {
				await swf().send(new RespondDecisionTaskCompletedCommand({ taskToken: token }));
			}
			toast.success(`Task responded as ${outcome}`);
			polledTask = null;
			pollResult = '';
			pollReason = '';
		} catch (e) {
			toast.error('Failed to respond: ' + String(e));
		} finally {
			respondingTask = false;
		}
	}

	async function loadTags(arn: string) {
		tagsArn = arn;
		loadingTags = true;
		showTags = true;
		tagsList = [];
		try {
			const resp = await swf().send(new ListTagsForResourceCommand({ resourceArn: arn }));
			tagsList = (resp.tags ?? []).map((t) => ({ key: t.key ?? '', value: t.value ?? '' }));
		} catch (e) {
			toast.error('Failed to load tags: ' + String(e));
		} finally {
			loadingTags = false;
		}
	}

	async function addTag() {
		if (!newTagKey.trim()) {
			toast.error('Tag key is required');
			return;
		}
		try {
			await swf().send(
				new TagResourceCommand({
					resourceArn: tagsArn,
					tags: [{ key: newTagKey.trim(), value: newTagValue }]
				})
			);
			tagsList = [...tagsList, { key: newTagKey.trim(), value: newTagValue }];
			newTagKey = '';
			newTagValue = '';
			toast.success('Tag added');
		} catch (e) {
			toast.error('Failed to add tag: ' + String(e));
		}
	}

	function statusColor(status?: string) {
		switch (status) {
			case 'RUNNING':
				return 'text-green-600 dark:text-green-400 bg-green-100 dark:bg-green-900/30';
			case 'COMPLETED':
				return 'text-blue-600 dark:text-blue-400 bg-blue-100 dark:bg-blue-900/30';
			case 'TERMINATED':
				return 'text-red-600 dark:text-red-400 bg-red-100 dark:bg-red-900/30';
			case 'CANCELED':
				return 'text-orange-600 dark:text-orange-400 bg-orange-100 dark:bg-orange-900/30';
			case 'TIMED_OUT':
				return 'text-yellow-600 dark:text-yellow-400 bg-yellow-100 dark:bg-yellow-900/30';
			case 'FAILED':
				return 'text-red-700 dark:text-red-500 bg-red-100 dark:bg-red-900/30';
			case 'REGISTERED':
				return 'text-green-600 dark:text-green-400 bg-green-100 dark:bg-green-900/30';
			case 'DEPRECATED':
				return 'text-gray-500 dark:text-gray-400 bg-gray-100 dark:bg-gray-700';
			default:
				return 'text-gray-600 dark:text-gray-400 bg-gray-100 dark:bg-gray-700';
		}
	}

	function eventTypeColor(type?: string) {
		if (!type) return 'text-gray-400';
		if (type.includes('Started') || type.includes('Initiated'))
			return 'text-green-600 dark:text-green-400';
		if (type.includes('Completed') || type.includes('Success'))
			return 'text-blue-600 dark:text-blue-400';
		if (type.includes('Failed') || type.includes('Terminated') || type.includes('Timed'))
			return 'text-red-600 dark:text-red-400';
		if (type.includes('Signaled') || type.includes('Cancel'))
			return 'text-orange-600 dark:text-orange-400';
		return 'text-slate-600 dark:text-slate-400';
	}

	function fmtTs(ts?: Date | number) {
		if (!ts) return '—';
		const d = ts instanceof Date ? ts : new Date(Number(ts) * 1000);
		return d.toLocaleString();
	}

	onMount(loadAll);
</script>

<div class="space-y-6 p-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Workflow class="h-7 w-7 text-violet-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Simple Workflow Service</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Manage SWF domains, workflow types, and executions</p>
			</div>
		</div>
		<button
			onclick={loadAll}
			title="Refresh"
			class="flex items-center gap-2 rounded-lg border border-gray-200 px-3 py-2 text-sm text-gray-600 hover:bg-gray-50 dark:border-gray-700 dark:text-gray-300 dark:hover:bg-gray-800"
		>
			<RefreshCw class="h-4 w-4" /> Refresh
		</button>
	</div>

	<!-- Stats -->
	<div class="grid grid-cols-2 gap-4 sm:grid-cols-4">
		<div class="flex items-center gap-3 rounded-lg border border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-800">
			<div class="rounded-lg bg-violet-100 p-2 dark:bg-violet-900/30">
				<Globe class="h-5 w-5 text-violet-600 dark:text-violet-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{domains.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Domains</p>
			</div>
		</div>
		<div class="flex items-center gap-3 rounded-lg border border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-800">
			<div class="rounded-lg bg-blue-100 p-2 dark:bg-blue-900/30">
				<Workflow class="h-5 w-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{workflowTypes.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Workflow Types</p>
			</div>
		</div>
		<div class="flex items-center gap-3 rounded-lg border border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-800">
			<div class="rounded-lg bg-green-100 p-2 dark:bg-green-900/30">
				<Play class="h-5 w-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{openExecs.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Open Executions</p>
			</div>
		</div>
		<div class="flex items-center gap-3 rounded-lg border border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-800">
			<div class="rounded-lg bg-gray-100 p-2 dark:bg-gray-700">
				<CheckCircle class="h-5 w-5 text-gray-600 dark:text-gray-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{closedExecs.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Closed Executions</p>
			</div>
		</div>
	</div>

	<!-- Main panel -->
	<div class="rounded-lg border border-slate-200 bg-white dark:border-slate-700 dark:bg-slate-800">
		<!-- Tab bar -->
		<div class="flex flex-col gap-3 border-b border-slate-200 p-4 dark:border-slate-700 sm:flex-row sm:items-center sm:justify-between">
			<div class="flex flex-wrap gap-2">
				{#each (['domains', 'workflows', 'executions', 'history', 'polling'] as Tab[]) as tab}
					<button
						onclick={() => { activeTab = tab; searchQuery = ''; }}
						class="flex items-center gap-1.5 rounded-lg px-3 py-2 text-sm font-medium {activeTab === tab ? 'bg-violet-600 text-white' : 'bg-gray-100 text-gray-700 hover:bg-gray-200 dark:bg-slate-700 dark:text-gray-300 dark:hover:bg-slate-600'}"
					>
						{tab === 'domains' ? 'Domains' : tab === 'workflows' ? 'Workflow Types' : tab === 'executions' ? 'Executions' : tab === 'history' ? 'Exec History' : 'Polling'}
					</button>
				{/each}
			</div>
			<div class="flex items-center gap-2">
				{#if activeTab === 'domains'}
					<button
						onclick={() => { newDomainName = ''; newDomainDesc = ''; showRegisterDomain = true; }}
						class="flex items-center gap-1.5 rounded-lg bg-violet-600 px-3 py-2 text-sm font-medium text-white hover:bg-violet-700"
					>
						<Plus class="h-4 w-4" /> Register Domain
					</button>
				{:else if activeTab === 'executions'}
					<div class="flex gap-2">
						<select
							bind:value={selectedDomain}
							onchange={loadExecutions}
							class="rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						>
							<option value="">— select domain —</option>
							{#each domains as d}<option value={d.name}>{d.name}</option>{/each}
						</select>
						<button
							onclick={() => { startDomain = selectedDomain; startWorkflowId = ''; showStartExec = true; }}
							class="flex items-center gap-1.5 rounded-lg bg-green-600 px-3 py-2 text-sm font-medium text-white hover:bg-green-700"
						>
							<Plus class="h-4 w-4" /> Start Execution
						</button>
					</div>
				{:else if activeTab === 'workflows'}
					<select
						bind:value={selectedDomain}
						onchange={loadWorkflowTypes}
						class="rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
					>
						<option value="">— select domain —</option>
						{#each domains as d}<option value={d.name}>{d.name}</option>{/each}
					</select>
				{/if}
				{#if activeTab !== 'polling' && activeTab !== 'history'}
					<div class="relative">
						<Search class="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
						<input
							bind:value={searchQuery}
							placeholder="Search..."
							class="w-full rounded-lg border border-gray-200 bg-white py-2 pl-9 pr-4 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white sm:w-48"
						/>
					</div>
				{/if}
			</div>
		</div>

		<div class="p-4">
			{#if loading}
				<div class="py-8 text-center text-gray-500 dark:text-gray-400">Loading…</div>

			<!-- DOMAINS TAB -->
			{:else if activeTab === 'domains'}
				{#if filteredDomains.length === 0}
					<div class="py-8 text-center text-gray-500 dark:text-gray-400">No domains found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredDomains as domain}
							<div class="flex items-center justify-between rounded-lg bg-gray-50 p-3 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Globe class="h-5 w-5 shrink-0 text-violet-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{domain.name}</p>
										{#if domain.description}
											<p class="text-xs text-gray-500 dark:text-gray-400">{domain.description}</p>
										{/if}
									</div>
								</div>
								<div class="flex items-center gap-2">
									<span class="rounded-full px-2 py-1 text-xs font-medium {statusColor(domain.status)}">{domain.status}</span>
									<button
										onclick={() => loadTags(`arn:aws:swf:us-east-1:123456789012:/domain/${domain.name}`)}
										class="rounded px-2 py-1 text-xs text-violet-600 hover:bg-violet-50 dark:text-violet-400 dark:hover:bg-violet-900/20"
									>
										<Tag class="inline h-3.5 w-3.5" /> Tags
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}

			<!-- WORKFLOW TYPES TAB -->
			{:else if activeTab === 'workflows'}
				{#if !selectedDomain}
					<div class="py-8 text-center text-gray-500 dark:text-gray-400">Select a domain to view workflow types</div>
				{:else}
					<div class="mb-4">
						<h3 class="mb-2 text-sm font-medium text-gray-700 dark:text-gray-300">Workflow Types</h3>
						{#if filteredWfTypes.length === 0}
							<div class="py-4 text-center text-gray-400 text-sm">No workflow types registered</div>
						{:else}
							<div class="space-y-2">
								{#each filteredWfTypes as wt}
									<div class="flex items-center justify-between rounded-lg bg-gray-50 p-3 dark:bg-slate-700/50">
										<div class="flex items-center gap-3">
											<Workflow class="h-5 w-5 shrink-0 text-blue-500" />
											<div>
												<p class="font-medium text-gray-900 dark:text-white">
													{wt.workflowType?.name}
													<span class="ml-1 text-xs text-gray-400">v{wt.workflowType?.version}</span>
												</p>
												{#if wt.description}<p class="text-xs text-gray-500 dark:text-gray-400">{wt.description}</p>{/if}
											</div>
										</div>
										<span class="rounded-full px-2 py-1 text-xs font-medium {statusColor(wt.status)}">{wt.status}</span>
									</div>
								{/each}
							</div>
						{/if}
					</div>
					<div>
						<h3 class="mb-2 text-sm font-medium text-gray-700 dark:text-gray-300">Activity Types</h3>
						{#if activityTypes.length === 0}
							<div class="py-4 text-center text-gray-400 text-sm">No activity types registered</div>
						{:else}
							<div class="space-y-2">
								{#each activityTypes as at}
									<div class="flex items-center justify-between rounded-lg bg-gray-50 p-3 dark:bg-slate-700/50">
										<div class="flex items-center gap-3">
											<Activity class="h-5 w-5 shrink-0 text-indigo-500" />
											<div>
												<p class="font-medium text-gray-900 dark:text-white">
													{at.activityType?.name}
													<span class="ml-1 text-xs text-gray-400">v{at.activityType?.version}</span>
												</p>
												{#if at.description}<p class="text-xs text-gray-500 dark:text-gray-400">{at.description}</p>{/if}
											</div>
										</div>
										<div class="flex items-center gap-2">
											<button
												onclick={() => viewActivityType(at)}
												class="rounded px-2 py-1 text-xs text-blue-600 hover:bg-blue-50 dark:text-blue-400 dark:hover:bg-blue-900/20"
											>
												Detail
											</button>
											<span class="rounded-full px-2 py-1 text-xs font-medium {statusColor(at.status)}">{at.status}</span>
										</div>
									</div>
								{/each}
							</div>
						{/if}
					</div>
				{/if}

			<!-- EXECUTIONS TAB -->
			{:else if activeTab === 'executions'}
				{#if !selectedDomain}
					<div class="py-8 text-center text-gray-500 dark:text-gray-400">Select a domain to view executions</div>
				{:else}
					<!-- Open executions -->
					<div class="mb-6">
						<h3 class="mb-2 flex items-center gap-2 text-sm font-semibold text-gray-700 dark:text-gray-300">
							<Play class="h-4 w-4 text-green-500" /> Open Executions
							<span class="rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700 dark:bg-green-900/30 dark:text-green-400">{filteredOpenExecs.length}</span>
						</h3>
						{#if filteredOpenExecs.length === 0}
							<div class="py-3 text-center text-sm text-gray-400">No open executions</div>
						{:else}
							<div class="space-y-2">
								{#each filteredOpenExecs as exec}
									<div class="rounded-lg bg-gray-50 p-3 dark:bg-slate-700/50">
										<div class="flex items-center justify-between">
											<div class="flex items-center gap-3">
												<CheckCircle class="h-5 w-5 shrink-0 text-green-500" />
												<div>
													<p class="font-medium font-mono text-sm text-gray-900 dark:text-white">{exec.execution?.workflowId}</p>
													<p class="text-xs text-gray-400">Run: {exec.execution?.runId?.slice(0, 8)}…</p>
												</div>
											</div>
											<div class="flex items-center gap-2">
												<span class="rounded-full px-2 py-1 text-xs font-medium {statusColor('RUNNING')}">RUNNING</span>
												<button onclick={() => openHistory(selectedDomain, exec.execution?.workflowId ?? '', exec.execution?.runId ?? '')}
													class="rounded px-2 py-1 text-xs text-blue-600 hover:bg-blue-50 dark:text-blue-400 dark:hover:bg-blue-900/20">
													<History class="inline h-3.5 w-3.5" /> History
												</button>
												<button onclick={() => openSignalModal(selectedDomain, exec.execution?.workflowId ?? '')}
													class="rounded px-2 py-1 text-xs text-amber-600 hover:bg-amber-50 dark:text-amber-400 dark:hover:bg-amber-900/20">
													<Bell class="inline h-3.5 w-3.5" /> Signal
												</button>
												<button onclick={() => cancelExecution(selectedDomain, exec.execution?.workflowId ?? '')}
													class="rounded px-2 py-1 text-xs text-orange-600 hover:bg-orange-50 dark:text-orange-400 dark:hover:bg-orange-900/20">
													Cancel
												</button>
												<button onclick={() => terminateExecution(selectedDomain, exec.execution?.workflowId ?? '')}
													class="rounded px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-900/20">
													<XCircle class="inline h-3.5 w-3.5" /> Terminate
												</button>
											</div>
										</div>
										{#if exec.startTimestamp}
											<p class="mt-1 pl-8 text-xs text-gray-400">Started: {fmtTs(exec.startTimestamp)}</p>
										{/if}
									</div>
								{/each}
							</div>
						{/if}
					</div>

					<!-- Closed executions -->
					<div>
						<h3 class="mb-2 flex items-center gap-2 text-sm font-semibold text-gray-700 dark:text-gray-300">
							<Clock class="h-4 w-4 text-gray-500" /> Closed Executions
							<span class="rounded-full bg-gray-100 px-2 py-0.5 text-xs text-gray-600 dark:bg-gray-700 dark:text-gray-300">{filteredClosedExecs.length}</span>
						</h3>
						{#if filteredClosedExecs.length === 0}
							<div class="py-3 text-center text-sm text-gray-400">No closed executions</div>
						{:else}
							<div class="space-y-2">
								{#each filteredClosedExecs as exec}
									<div class="rounded-lg bg-gray-50 p-3 dark:bg-slate-700/50">
										<div class="flex items-center justify-between">
											<div class="flex items-center gap-3">
												<Clock class="h-5 w-5 shrink-0 text-gray-400" />
												<div>
													<p class="font-medium font-mono text-sm text-gray-900 dark:text-white">{exec.execution?.workflowId}</p>
													{#if exec.closeStatus}<p class="text-xs text-gray-400">Close reason: {exec.closeStatus}</p>{/if}
												</div>
											</div>
											<div class="flex items-center gap-2">
												<span class="rounded-full px-2 py-1 text-xs font-medium {statusColor(exec.closeStatus ?? exec.executionStatus)}">{exec.closeStatus ?? exec.executionStatus ?? '—'}</span>
												<button onclick={() => openHistory(selectedDomain, exec.execution?.workflowId ?? '', exec.execution?.runId ?? '')}
													class="rounded px-2 py-1 text-xs text-blue-600 hover:bg-blue-50 dark:text-blue-400 dark:hover:bg-blue-900/20">
													<History class="inline h-3.5 w-3.5" /> History
												</button>
											</div>
										</div>
									</div>
								{/each}
							</div>
						{/if}
					</div>
				{/if}

			<!-- HISTORY TAB -->
			{:else if activeTab === 'history'}
				<div class="mb-4 flex items-end gap-3">
					<div class="flex-1">
						<label class="mb-1 block text-xs text-gray-500 dark:text-gray-400">Domain</label>
						<select
							bind:value={historyDomain}
							class="w-full rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						>
							<option value="">— select domain —</option>
							{#each domains as d}<option value={d.name}>{d.name}</option>{/each}
						</select>
					</div>
					<div class="flex-1">
						<label class="mb-1 block text-xs text-gray-500 dark:text-gray-400">Workflow ID</label>
						<input
							bind:value={historyWorkflowId}
							placeholder="wf-12345"
							class="w-full rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						/>
					</div>
					<div class="flex-1">
						<label class="mb-1 block text-xs text-gray-500 dark:text-gray-400">Run ID (optional)</label>
						<input
							bind:value={historyRunId}
							placeholder="run id"
							class="w-full rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
						/>
					</div>
					<button
						onclick={loadHistory}
						class="flex items-center gap-1.5 rounded-lg bg-violet-600 px-4 py-2 text-sm font-medium text-white hover:bg-violet-700"
					>
						<History class="h-4 w-4" /> Load History
					</button>
				</div>

				{#if execDetail}
					<div class="mb-4 grid grid-cols-1 gap-3 rounded-lg border border-gray-200 p-3 dark:border-gray-600 sm:grid-cols-2">
						<div>
							<p class="text-xs font-semibold text-gray-500 dark:text-gray-400">Execution Status</p>
							<p class="text-sm">{execDetail.executionInfo?.executionStatus ?? '—'} {execDetail.executionInfo?.closeStatus ? `(${execDetail.executionInfo.closeStatus})` : ''}</p>
						</div>
						<div>
							<p class="text-xs font-semibold text-gray-500 dark:text-gray-400">Open Counts</p>
							<p class="text-sm font-mono">act:{execDetail.openCounts?.openActivityTasks ?? 0} dec:{execDetail.openCounts?.openDecisionTasks ?? 0} timers:{execDetail.openCounts?.openTimers ?? 0}</p>
						</div>
					</div>
				{/if}

				{#if historyEvents.length === 0}
					<div class="py-8 text-center text-gray-500 dark:text-gray-400">
						{historyWorkflowId ? 'No history events found' : 'Enter a Workflow ID to view history'}
					</div>
				{:else}
					<div class="mb-3">
						<div class="relative">
							<Search class="absolute left-3 top-2.5 h-4 w-4 text-gray-400" />
							<input
								bind:value={historyEventFilter}
								placeholder="Filter by event type (e.g. ActivityTask)…"
								class="w-full rounded-lg border border-gray-200 bg-white py-1.5 pl-9 pr-4 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
							/>
						</div>
					</div>
					<div class="space-y-1">
						{#each filteredHistoryEvents as event}
							{@const payload = eventPayload(event)}
							<div class="rounded-lg hover:bg-gray-50 dark:hover:bg-slate-700/50">
								<button
									type="button"
									onclick={() => toggleEvent(event.eventId)}
									class="flex w-full items-start gap-3 p-2 text-left"
								>
									<span class="mt-0.5 min-w-8 rounded bg-gray-100 px-1.5 py-0.5 text-center text-xs font-mono text-gray-500 dark:bg-gray-700 dark:text-gray-400">
										{event.eventId}
									</span>
									<div class="flex-1">
										<span class="text-sm font-medium {eventTypeColor(event.eventType)}">{event.eventType}</span>
										{#if event.eventTimestamp}
											<span class="ml-3 text-xs text-gray-400">{fmtTs(event.eventTimestamp)}</span>
										{/if}
									</div>
									{#if payload}
										{#if expandedEventId === event.eventId}
											<ChevronDown class="h-4 w-4 text-gray-400" />
										{:else}
											<ChevronRight class="h-4 w-4 text-gray-400" />
										{/if}
									{/if}
								</button>
								{#if expandedEventId === event.eventId && payload}
									<pre class="mx-2 mb-2 overflow-auto rounded bg-gray-900 px-3 py-2 text-xs font-mono text-gray-100">{JSON.stringify(payload, null, 2)}</pre>
								{/if}
							</div>
						{/each}
					</div>
				{/if}

			<!-- POLLING TAB -->
			{:else if activeTab === 'polling'}
				<div class="space-y-4">
					<div class="grid grid-cols-1 gap-4 sm:grid-cols-3">
						<div>
							<label class="mb-1 block text-xs text-gray-500 dark:text-gray-400">Domain</label>
							<select
								bind:value={pollDomain}
								class="w-full rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
							>
								<option value="">— select domain —</option>
								{#each domains as d}<option value={d.name}>{d.name}</option>{/each}
							</select>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-500 dark:text-gray-400">Task List</label>
							<input
								bind:value={pollTaskList}
								placeholder="default"
								class="w-full rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
							/>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-500 dark:text-gray-400">Task Type</label>
							<select
								bind:value={pollMode}
								class="w-full rounded-lg border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
							>
								<option value="activity">Activity Task</option>
								<option value="decision">Decision Task</option>
							</select>
						</div>
					</div>

					<button
						onclick={pollTask}
						disabled={polling}
						class="flex items-center gap-2 rounded-lg bg-violet-600 px-4 py-2 text-sm font-medium text-white hover:bg-violet-700 disabled:opacity-50"
					>
						<Bell class="h-4 w-4" />
						{polling ? 'Polling…' : `Poll for ${pollMode === 'activity' ? 'Activity' : 'Decision'} Task`}
					</button>

					{#if polledTask}
						<div class="rounded-lg border border-violet-200 bg-violet-50 p-4 dark:border-violet-700 dark:bg-violet-900/20">
							<h4 class="mb-2 font-semibold text-violet-900 dark:text-violet-200">Task Received</h4>
							<pre class="overflow-auto rounded bg-white p-3 text-xs text-gray-800 dark:bg-slate-900 dark:text-gray-200">{JSON.stringify(polledTask, null, 2)}</pre>
							<div class="mt-3 space-y-2">
								{#if pollMode === 'activity'}
									<div>
										<label class="mb-1 block text-xs text-gray-500">Result (for complete)</label>
										<input
											bind:value={pollResult}
											placeholder="status:ok"
											class="w-full rounded border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
										/>
									</div>
									<div>
										<label class="mb-1 block text-xs text-gray-500">Reason (for fail)</label>
										<input
											bind:value={pollReason}
											placeholder="Task timed out"
											class="w-full rounded border border-gray-200 bg-white px-3 py-1.5 text-sm text-gray-900 dark:border-gray-600 dark:bg-slate-700 dark:text-white"
										/>
									</div>
									<div class="flex gap-2">
										<button
											onclick={() => respondTask('completed')}
											disabled={respondingTask}
											class="rounded bg-green-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-green-700 disabled:opacity-50"
										>
											{respondingTask ? '…' : 'Respond Completed'}
										</button>
										<button
											onclick={() => respondTask('failed')}
											disabled={respondingTask}
											class="rounded bg-red-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-red-700 disabled:opacity-50"
										>
											{respondingTask ? '…' : 'Respond Failed'}
										</button>
									</div>
								{:else}
									<button
										onclick={() => respondTask('completed')}
										disabled={respondingTask}
										class="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
									>
										{respondingTask ? '…' : 'Respond Decision Completed'}
									</button>
								{/if}
							</div>
						</div>
					{/if}
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Register Domain Modal -->
{#if showRegisterDomain}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
		role="presentation"
		onclick={() => (showRegisterDomain = false)}
		onkeydown={(e) => e.key === 'Escape' && (showRegisterDomain = false)}>
		<div class="w-96 rounded-lg border border-gray-700 bg-gray-800 p-6"
			role="dialog" aria-modal="true" aria-labelledby="reg-dom-title" tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => { if (e.key === 'Escape') showRegisterDomain = false; }}>
			<h3 id="reg-dom-title" class="mb-4 font-semibold text-white">Register Domain</h3>
			<div class="space-y-3">
				<div>
					<label class="mb-1 block text-xs text-gray-400">Name *</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={newDomainName} placeholder="my-domain" />
				</div>
				<div>
					<label class="mb-1 block text-xs text-gray-400">Description</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={newDomainDesc} placeholder="Optional" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200" onclick={() => (showRegisterDomain = false)}>Cancel</button>
				<button class="rounded bg-violet-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-violet-700 disabled:opacity-50"
					onclick={registerDomain} disabled={registeringDomain}>
					{registeringDomain ? 'Registering…' : 'Register'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Start Execution Modal -->
{#if showStartExec}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
		role="presentation"
		onclick={() => (showStartExec = false)}
		onkeydown={(e) => e.key === 'Escape' && (showStartExec = false)}>
		<div class="w-96 rounded-lg border border-gray-700 bg-gray-800 p-6"
			role="dialog" aria-modal="true" aria-labelledby="start-exec-title" tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => { if (e.key === 'Escape') showStartExec = false; }}>
			<h3 id="start-exec-title" class="mb-4 font-semibold text-white">Start Workflow Execution</h3>
			<div class="space-y-3">
				<div>
					<label class="mb-1 block text-xs text-gray-400">Domain *</label>
					<select bind:value={startDomain} class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white">
						<option value="">— select domain —</option>
						{#each domains as d}<option value={d.name}>{d.name}</option>{/each}
					</select>
				</div>
				<div>
					<label class="mb-1 block text-xs text-gray-400">Workflow ID *</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={startWorkflowId} placeholder="my-workflow-001" />
				</div>
				<div class="grid grid-cols-2 gap-2">
					<div>
						<label class="mb-1 block text-xs text-gray-400">Workflow Type Name</label>
						<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
							bind:value={startWorkflowName} placeholder="my-workflow" />
					</div>
					<div>
						<label class="mb-1 block text-xs text-gray-400">Version</label>
						<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
							bind:value={startWorkflowVersion} placeholder="1.0" />
					</div>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200" onclick={() => (showStartExec = false)}>Cancel</button>
				<button class="rounded bg-green-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-green-700 disabled:opacity-50"
					onclick={startExecution} disabled={startingExec}>
					{startingExec ? 'Starting…' : 'Start'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Signal Modal -->
{#if showSignal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
		role="presentation"
		onclick={() => (showSignal = false)}
		onkeydown={(e) => e.key === 'Escape' && (showSignal = false)}>
		<div class="w-96 rounded-lg border border-gray-700 bg-gray-800 p-6"
			role="dialog" aria-modal="true" aria-labelledby="signal-title" tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => { if (e.key === 'Escape') showSignal = false; }}>
			<h3 id="signal-title" class="mb-1 font-semibold text-white">Signal Workflow</h3>
			<p class="mb-4 text-xs text-gray-400 font-mono">{signalWorkflowId}</p>
			<div class="space-y-3">
				<div>
					<label class="mb-1 block text-xs text-gray-400">Signal Name *</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={signalName} placeholder="my-signal" />
				</div>
				<div>
					<label class="mb-1 block text-xs text-gray-400">Input (optional)</label>
					<textarea class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={signalInput} placeholder="key:value" rows="3"></textarea>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200" onclick={() => (showSignal = false)}>Cancel</button>
				<button class="rounded bg-amber-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50"
					onclick={sendSignal} disabled={sendingSignal}>
					{sendingSignal ? 'Sending…' : 'Send Signal'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Tags Modal -->
{#if showTags}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
		role="presentation"
		onclick={() => (showTags = false)}
		onkeydown={(e) => e.key === 'Escape' && (showTags = false)}>
		<div class="w-[28rem] rounded-lg border border-gray-700 bg-gray-800 p-6"
			role="dialog" aria-modal="true" aria-labelledby="tags-title" tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => { if (e.key === 'Escape') showTags = false; }}>
			<h3 id="tags-title" class="mb-1 font-semibold text-white">Resource Tags</h3>
			<p class="mb-4 truncate text-xs text-gray-400 font-mono">{tagsArn}</p>
			{#if loadingTags}
				<div class="py-4 text-center text-sm text-gray-400">Loading…</div>
			{:else if tagsList.length === 0}
				<div class="mb-3 py-2 text-center text-sm text-gray-400">No tags</div>
			{:else}
				<div class="mb-3 space-y-1 max-h-40 overflow-y-auto">
					{#each tagsList as tag}
						<div class="flex items-center gap-2 rounded bg-gray-900 px-3 py-1.5">
							<span class="text-xs text-violet-400 font-medium">{tag.key}</span>
							<span class="text-gray-500">=</span>
							<span class="text-xs text-gray-300">{tag.value}</span>
						</div>
					{/each}
				</div>
			{/if}
			<div class="space-y-2 border-t border-gray-700 pt-3">
				<p class="text-xs text-gray-400">Add Tag</p>
				<div class="flex gap-2">
					<input bind:value={newTagKey} placeholder="Key" class="flex-1 rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white" />
					<input bind:value={newTagValue} placeholder="Value" class="flex-1 rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white" />
					<button onclick={addTag} class="rounded bg-violet-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-violet-700">Add</button>
				</div>
			</div>
			<div class="mt-4 flex justify-end">
				<button class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200" onclick={() => (showTags = false)}>Close</button>
			</div>
		</div>
	</div>
{/if}

<!-- Activity Type Detail Modal -->
{#if showActivityDetail}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-lg rounded-lg bg-white p-6 shadow-xl dark:bg-slate-800">
			<div class="mb-4 flex items-center justify-between">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Activity Type Detail</h2>
				<button class="text-sm text-gray-400 hover:text-gray-600" onclick={() => (showActivityDetail = false)}>Close</button>
			</div>
			{#if loadingActivityDetail}
				<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
			{:else if activityDetail}
				<div class="space-y-3 text-sm">
					<div>
						<span class="font-semibold">{activityDetail.typeInfo?.activityType?.name}</span>
						<span class="ml-1 text-xs text-gray-400">v{activityDetail.typeInfo?.activityType?.version}</span>
					</div>
					{#if activityDetail.typeInfo?.description}<p class="text-gray-500 dark:text-gray-400">{activityDetail.typeInfo.description}</p>{/if}
					<div class="grid grid-cols-2 gap-3">
						<div><p class="text-xs font-semibold text-gray-500">Default Task Start-to-Close</p><p class="font-mono">{activityDetail.configuration?.defaultTaskStartToCloseTimeout ?? "\u2014"}</p></div>
						<div><p class="text-xs font-semibold text-gray-500">Schedule-to-Start</p><p class="font-mono">{activityDetail.configuration?.defaultTaskScheduleToStartTimeout ?? "\u2014"}</p></div>
						<div><p class="text-xs font-semibold text-gray-500">Schedule-to-Close</p><p class="font-mono">{activityDetail.configuration?.defaultTaskScheduleToCloseTimeout ?? "\u2014"}</p></div>
						<div><p class="text-xs font-semibold text-gray-500">Heartbeat Timeout</p><p class="font-mono">{activityDetail.configuration?.defaultTaskHeartbeatTimeout ?? "\u2014"}</p></div>
						<div><p class="text-xs font-semibold text-gray-500">Default Task List</p><p class="font-mono">{activityDetail.configuration?.defaultTaskList?.name ?? "\u2014"}</p></div>
						<div><p class="text-xs font-semibold text-gray-500">Default Priority</p><p class="font-mono">{activityDetail.configuration?.defaultTaskPriority ?? "\u2014"}</p></div>
					</div>
				</div>
			{:else}
				<p class="py-6 text-center text-sm text-gray-400">No detail available</p>
			{/if}
		</div>
	</div>
{/if}
