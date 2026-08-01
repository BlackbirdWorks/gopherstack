<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getEMRServerlessClient } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		CreateApplicationCommand,
		GetApplicationCommand,
		UpdateApplicationCommand,
		DeleteApplicationCommand,
		StartApplicationCommand,
		StopApplicationCommand,
		ListJobRunsCommand,
		StartJobRunCommand,
		GetJobRunCommand,
		CancelJobRunCommand,
		GetDashboardForJobRunCommand,
		ListSessionsCommand,
		StartSessionCommand,
		GetSessionCommand,
		TerminateSessionCommand,
		type ApplicationSummary,
		type Application,
		type JobRunSummary,
		type JobRun,
		type SessionSummary,
		type Session
	} from '@aws-sdk/client-emr-serverless';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { Zap, Plus, Trash2, Eye, Pencil, Play, Square, Ban, ExternalLink } from 'lucide-svelte';

	const client = regionalClient(getEMRServerlessClient);

	type TabId = 'applications' | 'jobRuns' | 'sessions';

	const tabs: TabDef[] = [
		{ id: 'applications', label: 'Applications' },
		{ id: 'jobRuns', label: 'Job Runs' },
		{ id: 'sessions', label: 'Sessions' }
	];

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

	let activeTab = $state<TabId>('applications');
	let searchQuery = $state('');

	let applications = $state<ApplicationSummary[]>([]);
	let applicationsNextToken = $state<string | undefined>();
	let loadingMoreApplications = $state(false);

	// Job Runs / Sessions are both scoped to one selected application, the
	// same shared-selector pattern accessanalyzer uses for its
	// analyzer-scoped tabs.
	let selectedAppId = $state('');
	const selectedApp = $derived(applications.find((a) => a.id === selectedAppId));
	const appScopedTabs: TabId[] = ['jobRuns', 'sessions'];

	let jobRuns = $state<JobRunSummary[]>([]);
	let jobRunsNextToken = $state<string | undefined>();
	let loadingMoreJobRuns = $state(false);

	let sessions = $state<SessionSummary[]>([]);
	let sessionsNextToken = $state<string | undefined>();
	let loadingMoreSessions = $state(false);

	async function fetchApplications(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListApplicationsCommand({ nextToken: reset ? undefined : applicationsNextToken })
		);
		applications = reset ? (resp.applications ?? []) : [...applications, ...(resp.applications ?? [])];
		applicationsNextToken = resp.nextToken;
		if (!selectedAppId && applications.length > 0) {
			selectedAppId = applications[0].id ?? '';
		}
	}

	async function fetchJobRuns(reset: boolean): Promise<void> {
		if (!selectedAppId) {
			jobRuns = [];
			jobRunsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListJobRunsCommand({
				applicationId: selectedAppId,
				nextToken: reset ? undefined : jobRunsNextToken
			})
		);
		jobRuns = reset ? (resp.jobRuns ?? []) : [...jobRuns, ...(resp.jobRuns ?? [])];
		jobRunsNextToken = resp.nextToken;
	}

	async function fetchSessions(reset: boolean): Promise<void> {
		if (!selectedAppId) {
			sessions = [];
			sessionsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListSessionsCommand({
				applicationId: selectedAppId,
				nextToken: reset ? undefined : sessionsNextToken
			})
		);
		sessions = reset ? (resp.sessions ?? []) : [...sessions, ...(resp.sessions ?? [])];
		sessionsNextToken = resp.nextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		applications: () => fetchApplications(true).catch(rethrowDescribed),
		jobRuns: () => fetchJobRuns(true).catch(rethrowDescribed),
		sessions: () => fetchSessions(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	function onAppSelect(id: string): void {
		selectedAppId = id;
		if (appScopedTabs.includes(activeTab)) {
			tabLoader.refresh(activeTab);
		}
	}

	// Applications is the parent resource for the app-scoped tabs: on a
	// region change the previously selected application belongs to the old
	// region and must not be reused, so reload applications first (which
	// re-selects one for the new region) before reloading whichever tab is
	// active.
	onRegionChange(() => {
		selectedAppId = '';
		applications = [];
		applicationsNextToken = undefined;
		void tabLoader.refresh('applications').then(() => {
			if (activeTab !== 'applications') {
				tabLoader.refresh(activeTab);
			}
		});
	});

	const filteredApplications = $derived(
		applications.filter((a) => (a.name ?? a.id ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredJobRuns = $derived(
		jobRuns.filter((j) => (j.name ?? j.id ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredSessions = $derived(
		sessions.filter((s) => (s.name ?? s.sessionId ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreApplications(): Promise<void> {
		loadingMoreApplications = true;
		try {
			await fetchApplications(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreApplications = false;
		}
	}

	async function loadMoreJobRuns(): Promise<void> {
		loadingMoreJobRuns = true;
		try {
			await fetchJobRuns(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreJobRuns = false;
		}
	}

	async function loadMoreSessions(): Promise<void> {
		loadingMoreSessions = true;
		try {
			await fetchSessions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSessions = false;
		}
	}

	const APP_STATE_BADGE: Record<string, string> = {
		STARTED: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400',
		STARTING: 'bg-cyan-100 dark:bg-cyan-900/30 text-cyan-700 dark:text-cyan-400',
		CREATING: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400',
		STOPPING: 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400',
		STOPPED: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400',
		TERMINATED: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
	};
	const JOB_STATE_BADGE: Record<string, string> = {
		SUCCESS: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400',
		FAILED: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400',
		RUNNING: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400',
		SCHEDULED: 'bg-cyan-100 dark:bg-cyan-900/30 text-cyan-700 dark:text-cyan-400',
		PENDING: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400',
		QUEUED: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400',
		SUBMITTED: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400',
		CANCELLING: 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400',
		CANCELLED: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
	};
	const SESSION_STATE_BADGE: Record<string, string> = {
		STARTED: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400',
		IDLE: 'bg-cyan-100 dark:bg-cyan-900/30 text-cyan-700 dark:text-cyan-400',
		BUSY: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400',
		STARTING: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400',
		SUBMITTED: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400',
		TERMINATING: 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400',
		TERMINATED: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400',
		FAILED: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
	};

	function badgeClass(map: Record<string, string>, state?: string): string {
		return map[state ?? ''] ?? 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// GetJobRun's response type (JobRun) names the field "jobRunId";
	// ListJobRuns' JobRunSummary names the same concept "id" -- another real
	// AWS API inconsistency in the generated types, same shape as
	// GroupIdentifier/Group in resourcegroups.
	function jobRunIdOf(j: JobRun | JobRunSummary): string | undefined {
		return 'jobRunId' in j ? j.jobRunId : j.id;
	}

	// --- Applications: create / edit / delete / start / stop / detail ---

	let createAppModal = $state<Modal | null>(null);
	let creatingApp = $state(false);
	let createAppError = $state<string | null>(null);
	let newAppName = $state('');
	let newAppReleaseLabel = $state('emr-7.1.0');
	let newAppType = $state('SPARK');

	function openCreateAppModal(): void {
		createAppError = null;
		newAppName = '';
		newAppReleaseLabel = 'emr-7.1.0';
		newAppType = 'SPARK';
		createAppModal?.open();
	}

	async function submitCreateApp(): Promise<void> {
		if (!newAppReleaseLabel) {
			createAppError = 'Release label is required.';
			return;
		}
		creatingApp = true;
		createAppError = null;
		try {
			await client().send(
				new CreateApplicationCommand({
					name: newAppName || undefined,
					releaseLabel: newAppReleaseLabel,
					type: newAppType
				})
			);
			toast.success('Application created');
			createAppModal?.close();
			await tabLoader.refresh('applications');
		} catch (e) {
			const msg = describeError(e);
			createAppError = msg;
			toast.error(msg);
		} finally {
			creatingApp = false;
		}
	}

	async function handleDeleteApp(a: ApplicationSummary): Promise<void> {
		if (!a.id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete application',
			message: `Delete application ${a.name ?? a.id}? Only STOPPED or CREATED applications can be deleted.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteApplicationCommand({ applicationId: a.id }));
			toast.success('Application deleted');
			if (selectedAppId === a.id) selectedAppId = '';
			await tabLoader.refresh('applications');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleStartApp(a: ApplicationSummary): Promise<void> {
		if (!a.id) return;
		try {
			await client().send(new StartApplicationCommand({ applicationId: a.id }));
			toast.success('Application starting');
			await tabLoader.refresh('applications');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleStopApp(a: ApplicationSummary): Promise<void> {
		if (!a.id) return;
		try {
			await client().send(new StopApplicationCommand({ applicationId: a.id }));
			toast.success('Application stopping');
			await tabLoader.refresh('applications');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let appDetailModal = $state<Modal | null>(null);
	let viewedApp = $state<Application | ApplicationSummary | null>(null);
	let appDetailLoading = $state(false);
	let appDetailError = $state<string | null>(null);

	async function openAppDetail(a: ApplicationSummary): Promise<void> {
		viewedApp = a;
		appDetailError = null;
		appDetailModal?.open();
		if (!a.id) return;
		appDetailLoading = true;
		try {
			const resp = await client().send(new GetApplicationCommand({ applicationId: a.id }));
			viewedApp = resp.application ?? a;
		} catch (e) {
			appDetailError = describeError(e);
		} finally {
			appDetailLoading = false;
		}
	}

	let editAppModal = $state<Modal | null>(null);
	let editingApp = $state(false);
	let editAppError = $state<string | null>(null);
	let editAppId = $state('');
	let editAppReleaseLabel = $state('');

	function openEditAppModal(a: ApplicationSummary): void {
		if (!a.id) return;
		editAppError = null;
		editAppId = a.id;
		editAppReleaseLabel = a.releaseLabel ?? '';
		editAppModal?.open();
	}

	async function submitEditApp(): Promise<void> {
		if (!editAppId || !editAppReleaseLabel) {
			editAppError = 'Release label is required.';
			return;
		}
		editingApp = true;
		editAppError = null;
		try {
			await client().send(
				new UpdateApplicationCommand({ applicationId: editAppId, releaseLabel: editAppReleaseLabel })
			);
			toast.success('Application updated');
			editAppModal?.close();
			await tabLoader.refresh('applications');
		} catch (e) {
			const msg = describeError(e);
			editAppError = msg;
			toast.error(msg);
		} finally {
			editingApp = false;
		}
	}

	// --- Job Runs: submit / cancel / detail. Job runs are submitted and
	// cancelled, not created and deleted -- CancelJobRun is the real API's
	// only mutation on an in-flight run, so the "delete"-shaped action here
	// is deliberately labeled and implemented as Cancel, not Delete. There is
	// no DeleteJobRun operation in the real API. ---

	let startJobRunModal = $state<Modal | null>(null);
	let startingJobRun = $state(false);
	let startJobRunError = $state<string | null>(null);
	let newJobRunName = $state('');
	let newJobRunExecutionRoleArn = $state('');
	let newJobRunEntryPoint = $state('');
	let newJobRunArguments = $state('');
	let newJobRunSparkParams = $state('');

	function openStartJobRunModal(): void {
		startJobRunError = selectedAppId ? null : 'Select an application first.';
		newJobRunName = '';
		newJobRunExecutionRoleArn = '';
		newJobRunEntryPoint = '';
		newJobRunArguments = '';
		newJobRunSparkParams = '';
		startJobRunModal?.open();
	}

	async function submitStartJobRun(): Promise<void> {
		if (!selectedAppId) {
			startJobRunError = 'Select an application first.';
			return;
		}
		if (!newJobRunExecutionRoleArn || !newJobRunEntryPoint) {
			startJobRunError = 'Execution role ARN and entry point are required.';
			return;
		}
		startingJobRun = true;
		startJobRunError = null;
		try {
			await client().send(
				new StartJobRunCommand({
					applicationId: selectedAppId,
					name: newJobRunName || undefined,
					executionRoleArn: newJobRunExecutionRoleArn,
					jobDriver: {
						sparkSubmit: {
							entryPoint: newJobRunEntryPoint,
							entryPointArguments: newJobRunArguments
								? newJobRunArguments.split(',').map((s) => s.trim())
								: undefined,
							sparkSubmitParameters: newJobRunSparkParams || undefined
						}
					}
				})
			);
			toast.success('Job run submitted');
			startJobRunModal?.close();
			await tabLoader.refresh('jobRuns');
		} catch (e) {
			const msg = describeError(e);
			startJobRunError = msg;
			toast.error(msg);
		} finally {
			startingJobRun = false;
		}
	}

	async function handleCancelJobRun(j: JobRunSummary): Promise<void> {
		if (!j.id || !selectedAppId) return;
		const confirmed = await confirmDestructive({
			title: 'Cancel job run',
			message: `Cancel job run ${j.name ?? j.id}?`,
			confirmLabel: 'Cancel run'
		});
		if (!confirmed) return;
		try {
			await client().send(new CancelJobRunCommand({ applicationId: selectedAppId, jobRunId: j.id }));
			toast.success('Job run cancelled');
			await tabLoader.refresh('jobRuns');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let jobRunDetailModal = $state<Modal | null>(null);
	let viewedJobRun = $state<JobRun | JobRunSummary | null>(null);
	let viewedJobRunDashboardUrl = $state<string | null>(null);
	let jobRunDetailLoading = $state(false);
	let jobRunDetailError = $state<string | null>(null);

	async function openJobRunDetail(j: JobRunSummary): Promise<void> {
		viewedJobRun = j;
		viewedJobRunDashboardUrl = null;
		jobRunDetailError = null;
		jobRunDetailModal?.open();
		if (!j.id || !selectedAppId) return;
		jobRunDetailLoading = true;
		try {
			const [detailResp, dashboardResp] = await Promise.all([
				client().send(new GetJobRunCommand({ applicationId: selectedAppId, jobRunId: j.id })),
				client()
					.send(new GetDashboardForJobRunCommand({ applicationId: selectedAppId, jobRunId: j.id }))
					.catch(() => {
						/* dashboard is best-effort; job run detail still renders without it */
					})
			]);
			viewedJobRun = detailResp.jobRun ?? j;
			viewedJobRunDashboardUrl = dashboardResp?.url ?? null;
		} catch (e) {
			jobRunDetailError = describeError(e);
		} finally {
			jobRunDetailLoading = false;
		}
	}

	// --- Sessions: start / terminate / detail. TerminateSession is the real
	// API's own name for ending a session -- used verbatim here rather than
	// relabeled as "delete". ---

	let startSessionModal = $state<Modal | null>(null);
	let startingSession = $state(false);
	let startSessionError = $state<string | null>(null);
	let newSessionName = $state('');
	let newSessionExecutionRoleArn = $state('');

	function openStartSessionModal(): void {
		startSessionError = selectedAppId ? null : 'Select an application first.';
		newSessionName = '';
		newSessionExecutionRoleArn = '';
		startSessionModal?.open();
	}

	async function submitStartSession(): Promise<void> {
		if (!selectedAppId) {
			startSessionError = 'Select an application first.';
			return;
		}
		if (!newSessionExecutionRoleArn) {
			startSessionError = 'Execution role ARN is required.';
			return;
		}
		startingSession = true;
		startSessionError = null;
		try {
			await client().send(
				new StartSessionCommand({
					applicationId: selectedAppId,
					name: newSessionName || undefined,
					executionRoleArn: newSessionExecutionRoleArn
				})
			);
			toast.success('Session starting');
			startSessionModal?.close();
			await tabLoader.refresh('sessions');
		} catch (e) {
			const msg = describeError(e);
			startSessionError = msg;
			toast.error(msg);
		} finally {
			startingSession = false;
		}
	}

	async function handleTerminateSession(s: SessionSummary): Promise<void> {
		if (!s.sessionId || !selectedAppId) return;
		const confirmed = await confirmDestructive({
			title: 'Terminate session',
			message: `Terminate session ${s.name ?? s.sessionId}?`,
			confirmLabel: 'Terminate'
		});
		if (!confirmed) return;
		try {
			await client().send(
				new TerminateSessionCommand({ applicationId: selectedAppId, sessionId: s.sessionId })
			);
			toast.success('Session terminating');
			await tabLoader.refresh('sessions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let sessionDetailModal = $state<Modal | null>(null);
	let viewedSession = $state<Session | SessionSummary | null>(null);
	let sessionDetailLoading = $state(false);
	let sessionDetailError = $state<string | null>(null);

	async function openSessionDetail(s: SessionSummary): Promise<void> {
		viewedSession = s;
		sessionDetailError = null;
		sessionDetailModal?.open();
		if (!s.sessionId || !selectedAppId) return;
		sessionDetailLoading = true;
		try {
			const resp = await client().send(
				new GetSessionCommand({ applicationId: selectedAppId, sessionId: s.sessionId })
			);
			viewedSession = resp.session ?? s;
		} catch (e) {
			sessionDetailError = describeError(e);
		} finally {
			sessionDetailLoading = false;
		}
	}

</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Zap}
		title="Amazon EMR Serverless"
		description="Run big data frameworks without managing clusters"
		onRefresh={handleRefresh}
		color="amber"
	>
		{#snippet actions()}
			{#if activeTab === 'applications'}
				<button
					onclick={openCreateAppModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create application
				</button>
			{:else if activeTab === 'jobRuns'}
				<button
					onclick={openStartJobRunModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Submit job run
				</button>
			{:else if activeTab === 'sessions'}
				<button
					onclick={openStartSessionModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Start session
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="amber" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if appScopedTabs.includes(activeTab)}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="app-select" class="text-sm text-gray-500 dark:text-gray-400">Application</label>
					<select
						id="app-select"
						value={selectedAppId}
						onchange={(e) => onAppSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if applications.length === 0}
							<option value="">No applications</option>
						{/if}
						{#each applications as a (a.id)}
							<option value={a.id}>{a.name ?? a.id} ({a.state})</option>
						{/each}
					</select>
				</div>
			{/if}

			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'applications'}
				{#snippet appStateCell(a: ApplicationSummary)}
					<span class="text-xs px-2 py-1 rounded-full {badgeClass(APP_STATE_BADGE, a.state)}">{a.state ?? '—'}</span>
				{/snippet}
				{#snippet appCreatedCell(a: ApplicationSummary)}
					{formatDate(a.createdAt)}
				{/snippet}
				{#snippet appActionsCell(a: ApplicationSummary)}
					<div class="flex items-center gap-2 justify-end">
						{#if a.state === 'STOPPED' || a.state === 'CREATED'}
							<button
								onclick={() => handleStartApp(a)}
								title="Start"
								aria-label="Start application {a.name}"
								class="text-gray-400 hover:text-green-500"><Play class="w-4 h-4" /></button
							>
						{:else if a.state === 'STARTED'}
							<button
								onclick={() => handleStopApp(a)}
								title="Stop"
								aria-label="Stop application {a.name}"
								class="text-gray-400 hover:text-orange-500"><Square class="w-4 h-4" /></button
							>
						{/if}
						<button
							onclick={() => openAppDetail(a)}
							title="View"
							aria-label="View application {a.name}"
							class="text-gray-400 hover:text-amber-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditAppModal(a)}
							title="Edit"
							aria-label="Edit application {a.name}"
							class="text-gray-400 hover:text-amber-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteApp(a)}
							title="Delete"
							aria-label="Delete application {a.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const appColumns = defineColumns<ApplicationSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'type', label: 'Type' },
					{ key: 'releaseLabel', label: 'Release' },
					{ key: 'state', label: 'State', render: appStateCell },
					{ key: 'createdAt', label: 'Created', render: appCreatedCell },
					{ key: 'actions', label: '', render: appActionsCell }
				])}
				<DataTable
					rows={filteredApplications}
					rowKey={(a) => a.id ?? ''}
					columns={appColumns}
					loading={tabLoader.isLoading('applications')}
					emptyMessage="No applications found"
				/>
				<LoadMore
					hasMore={!!applicationsNextToken}
					loading={loadingMoreApplications}
					onLoadMore={loadMoreApplications}
				/>
			{:else if activeTab === 'jobRuns'}
				{#snippet jobStateCell(j: JobRunSummary)}
					<span class="text-xs px-2 py-1 rounded-full {badgeClass(JOB_STATE_BADGE, j.state)}">{j.state ?? '—'}</span>
				{/snippet}
				{#snippet jobCreatedCell(j: JobRunSummary)}
					{formatDate(j.createdAt)}
				{/snippet}
				{#snippet jobActionsCell(j: JobRunSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openJobRunDetail(j)}
							title="View"
							aria-label="View job run {j.name ?? j.id}"
							class="text-gray-400 hover:text-amber-500"><Eye class="w-4 h-4" /></button
						>
						{#if j.state !== 'SUCCESS' && j.state !== 'FAILED' && j.state !== 'CANCELLED'}
							<button
								onclick={() => handleCancelJobRun(j)}
								title="Cancel"
								aria-label="Cancel job run {j.name ?? j.id}"
								class="text-gray-400 hover:text-red-500"><Ban class="w-4 h-4" /></button
							>
						{/if}
					</div>
				{/snippet}
				{@const jobColumns = defineColumns<JobRunSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'id', label: 'ID' },
					{ key: 'state', label: 'State', render: jobStateCell },
					{ key: 'createdAt', label: 'Created', render: jobCreatedCell },
					{ key: 'actions', label: '', render: jobActionsCell }
				])}
				<DataTable
					rows={filteredJobRuns}
					rowKey={(j) => j.id ?? ''}
					columns={jobColumns}
					loading={tabLoader.isLoading('jobRuns')}
					emptyMessage={selectedAppId ? 'No job runs found for this application' : 'Select an application first'}
				/>
				<LoadMore hasMore={!!jobRunsNextToken} loading={loadingMoreJobRuns} onLoadMore={loadMoreJobRuns} />
			{:else if activeTab === 'sessions'}
				{#snippet sessionStateCell(s: SessionSummary)}
					<span class="text-xs px-2 py-1 rounded-full {badgeClass(SESSION_STATE_BADGE, s.state)}">{s.state ?? '—'}</span>
				{/snippet}
				{#snippet sessionCreatedCell(s: SessionSummary)}
					{formatDate(s.createdAt)}
				{/snippet}
				{#snippet sessionActionsCell(s: SessionSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openSessionDetail(s)}
							title="View"
							aria-label="View session {s.name ?? s.sessionId}"
							class="text-gray-400 hover:text-amber-500"><Eye class="w-4 h-4" /></button
						>
						{#if s.state !== 'TERMINATED' && s.state !== 'TERMINATING'}
							<button
								onclick={() => handleTerminateSession(s)}
								title="Terminate"
								aria-label="Terminate session {s.name ?? s.sessionId}"
								class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
							>
						{/if}
					</div>
				{/snippet}
				{@const sessionColumns = defineColumns<SessionSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'sessionId', label: 'ID' },
					{ key: 'state', label: 'State', render: sessionStateCell },
					{ key: 'createdAt', label: 'Created', render: sessionCreatedCell },
					{ key: 'actions', label: '', render: sessionActionsCell }
				])}
				<DataTable
					rows={filteredSessions}
					rowKey={(s) => s.sessionId ?? ''}
					columns={sessionColumns}
					loading={tabLoader.isLoading('sessions')}
					emptyMessage={selectedAppId ? 'No sessions found for this application' : 'Select an application first'}
				/>
				<LoadMore hasMore={!!sessionsNextToken} loading={loadingMoreSessions} onLoadMore={loadMoreSessions} />
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createAppModal} title="Create Application">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="app-name" class="text-sm text-slate-600 dark:text-slate-300">Name (optional)</label>
				<input
					id="app-name"
					bind:value={newAppName}
					placeholder="my-application"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="app-release" class="text-sm text-slate-600 dark:text-slate-300">Release label</label>
				<input
					id="app-release"
					bind:value={newAppReleaseLabel}
					placeholder="emr-7.1.0"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="app-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select
					id="app-type"
					bind:value={newAppType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SPARK">Spark</option>
					<option value="HIVE">Hive</option>
				</select>
			</div>
			{#if createAppError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAppError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createAppModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateApp}
			disabled={creatingApp}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{creatingApp ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={appDetailModal} title="Application">
	{#snippet children()}
		{#if appDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedApp}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedApp.name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedApp.arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Type / Release</dt>
					<dd class="text-slate-900 dark:text-white">{viewedApp.type} / {viewedApp.releaseLabel}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">State</dt>
					<dd class="text-slate-900 dark:text-white">{viewedApp.state ?? '—'} {viewedApp.stateDetails ? `(${viewedApp.stateDetails})` : ''}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedApp.createdAt)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedApp.updatedAt)}</dd>
				</div>
			</dl>
			{#if appDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{appDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => appDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editAppModal} title="Edit Application">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editAppId}</span>. Only the release label can be changed
				here; capacity/network/monitoring configuration is set at creation time.
			</p>
			<div>
				<label for="app-edit-release" class="text-sm text-slate-600 dark:text-slate-300">Release label</label>
				<input
					id="app-edit-release"
					bind:value={editAppReleaseLabel}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editAppError}
				<p class="text-sm text-red-600 dark:text-red-400">{editAppError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editAppModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditApp}
			disabled={editingApp}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{editingApp ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={startJobRunModal} title="Submit Job Run">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				For application <span class="font-medium">{selectedApp?.name ?? (selectedAppId || '(none selected)')}</span>.
				Spark submit job driver only; use the real console/API for Hive job runs.
			</p>
			<div>
				<label for="jobrun-name" class="text-sm text-slate-600 dark:text-slate-300">Name (optional)</label>
				<input
					id="jobrun-name"
					bind:value={newJobRunName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="jobrun-role" class="text-sm text-slate-600 dark:text-slate-300">Execution role ARN</label>
				<input
					id="jobrun-role"
					bind:value={newJobRunExecutionRoleArn}
					placeholder="arn:aws:iam::123456789012:role/EMRServerlessExecutionRole"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="jobrun-entrypoint" class="text-sm text-slate-600 dark:text-slate-300"
					>Entry point (S3 URI)</label
				>
				<input
					id="jobrun-entrypoint"
					bind:value={newJobRunEntryPoint}
					placeholder="s3://my-bucket/scripts/job.py"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="jobrun-args" class="text-sm text-slate-600 dark:text-slate-300"
					>Entry point arguments (comma-separated, optional)</label
				>
				<input
					id="jobrun-args"
					bind:value={newJobRunArguments}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="jobrun-sparkparams" class="text-sm text-slate-600 dark:text-slate-300"
					>Spark submit parameters (optional)</label
				>
				<input
					id="jobrun-sparkparams"
					bind:value={newJobRunSparkParams}
					placeholder="--conf spark.executor.cores=4"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if startJobRunError}
				<p class="text-sm text-red-600 dark:text-red-400">{startJobRunError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => startJobRunModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitStartJobRun}
			disabled={startingJobRun}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{startingJobRun ? 'Submitting…' : 'Submit'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={jobRunDetailModal} title="Job Run">
	{#snippet children()}
		{#if jobRunDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedJobRun}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedJobRun.name ?? jobRunIdOf(viewedJobRun) ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedJobRun.arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">State</dt>
					<dd class="text-slate-900 dark:text-white">{viewedJobRun.state ?? '—'} {viewedJobRun.stateDetails ? `(${viewedJobRun.stateDetails})` : ''}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Execution role</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedJobRun.executionRole ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedJobRun.createdAt)}</dd>
				</div>
				{#if viewedJobRunDashboardUrl}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Dashboard</dt>
						<dd>
							<a
								href={viewedJobRunDashboardUrl}
								target="_blank"
								rel="noopener noreferrer"
								class="inline-flex items-center gap-1 text-amber-600 hover:underline dark:text-amber-400"
							>
								Open dashboard <ExternalLink class="w-3 h-3" />
							</a>
						</dd>
					</div>
				{/if}
			</dl>
			{#if jobRunDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{jobRunDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => jobRunDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={startSessionModal} title="Start Session">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				For application <span class="font-medium">{selectedApp?.name ?? (selectedAppId || '(none selected)')}</span>.
				Interactive sessions require a STARTED application.
			</p>
			<div>
				<label for="session-name" class="text-sm text-slate-600 dark:text-slate-300">Name (optional)</label>
				<input
					id="session-name"
					bind:value={newSessionName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="session-role" class="text-sm text-slate-600 dark:text-slate-300">Execution role ARN</label>
				<input
					id="session-role"
					bind:value={newSessionExecutionRoleArn}
					placeholder="arn:aws:iam::123456789012:role/EMRServerlessExecutionRole"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if startSessionError}
				<p class="text-sm text-red-600 dark:text-red-400">{startSessionError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => startSessionModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitStartSession}
			disabled={startingSession}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{startingSession ? 'Starting…' : 'Start'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={sessionDetailModal} title="Session">
	{#snippet children()}
		{#if sessionDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedSession}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedSession.name ?? viewedSession.sessionId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedSession.arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">State</dt>
					<dd class="text-slate-900 dark:text-white">{viewedSession.state ?? '—'} {viewedSession.stateDetails ? `(${viewedSession.stateDetails})` : ''}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Execution role</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedSession.executionRoleArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedSession.createdAt)}</dd>
				</div>
			</dl>
			{#if sessionDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{sessionDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => sessionDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>
