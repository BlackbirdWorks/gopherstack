<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getEMRServerlessClient } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		ListJobRunsCommand,
		type ApplicationSummary,
		type JobRunSummary
	} from '@aws-sdk/client-emr-serverless';
	import { toast } from 'svelte-sonner';
	import { Zap, RefreshCw, Search, Activity, Server, CheckCircle, XCircle, ArrowUpDown, Info } from 'lucide-svelte';

	const emr = regionalClient(getEMRServerlessClient);

	const JOB_STATE_BADGE: Record<string, string> = {
		SUCCESS:   'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400',
		FAILED:    'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400',
		RUNNING:   'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400',
		SCHEDULED: 'bg-cyan-100 dark:bg-cyan-900/30 text-cyan-700 dark:text-cyan-400',
		PENDING:   'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400',
		SUBMITTED: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400',
		CANCELLING:'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400',
		CANCELLED: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
	};

	// Single source of truth for job run states used in filter pills and the JobStateFilter type.
	const JOB_STATES = ['ALL', 'SUBMITTED', 'PENDING', 'SCHEDULED', 'RUNNING', 'SUCCESS', 'FAILED', 'CANCELLING', 'CANCELLED'] as const;

	const APP_STATE_BADGE: Record<string, string> = {
		STARTED:                'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400',
		STARTING:               'bg-cyan-100 dark:bg-cyan-900/30 text-cyan-700 dark:text-cyan-400',
		CREATING:               'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400',
		STOPPING:               'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400',
		STOPPED:                'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400',
		TERMINATED:             'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400',
		TERMINATED_WITH_ERRORS: 'bg-red-200 dark:bg-red-900/50 text-red-800 dark:text-red-300'
	};

	// Single source of truth for app states used in filter pills and the AppStateFilter type.
	const APP_STATES = ['ALL', 'CREATING', 'STARTING', 'STARTED', 'STOPPING', 'STOPPED', 'TERMINATED', 'TERMINATED_WITH_ERRORS'] as const;

	type JobSortKey = 'newest' | 'oldest' | 'name-asc' | 'name-desc';
	type JobStateFilter = (typeof JOB_STATES)[number];
	type AppSortKey = 'newest' | 'oldest' | 'name-asc' | 'name-desc';
	type AppStateFilter = (typeof APP_STATES)[number];
	type MainTab = 'applications' | 'jobs' | 'docs';

	let loading = $state(false);
	let jobsLoading = $state(false);
	let activeTab = $state<MainTab>('applications');
	let searchQuery = $state('');
	let jobStateFilter = $state<JobStateFilter>('ALL');
	let jobSort = $state<JobSortKey>('newest');
	let appStateFilter = $state<AppStateFilter>('ALL');
	let appSort = $state<AppSortKey>('newest');
	let applications = $state<ApplicationSummary[]>([]);
	let jobRuns = $state<JobRunSummary[]>([]);
	let selectedAppId = $state<string | null>(null);

	const selectedApp = $derived(applications.find((a) => a.id === selectedAppId) ?? null);

	const filteredApps = $derived((() => {
		const result = applications.filter((a) => {
			const search = searchQuery.toLowerCase();
			const matchesSearch = (a.name ?? a.id ?? '').toLowerCase().includes(search);
			const matchesState = appStateFilter === 'ALL' || a.state === appStateFilter;
			return matchesSearch && matchesState;
		});
		if (appSort === 'newest') {
			result.sort((a, b) => (b.createdAt?.getTime() ?? 0) - (a.createdAt?.getTime() ?? 0));
		} else if (appSort === 'oldest') {
			result.sort((a, b) => (a.createdAt?.getTime() ?? 0) - (b.createdAt?.getTime() ?? 0));
		} else if (appSort === 'name-asc') {
			result.sort((a, b) => (a.name ?? a.id ?? '').localeCompare(b.name ?? b.id ?? ''));
		} else if (appSort === 'name-desc') {
			result.sort((a, b) => (b.name ?? b.id ?? '').localeCompare(a.name ?? a.id ?? ''));
		}
		return result;
	})());

	const filteredJobs = $derived((() => {
		const result = jobRuns.filter((j) => {
			const matchesSearch = (j.name ?? j.id ?? '').toLowerCase().includes(searchQuery.toLowerCase());
			const matchesState = jobStateFilter === 'ALL' || j.state === jobStateFilter;
			return matchesSearch && matchesState;
		});
		if (jobSort === 'newest') {
			result.sort((a, b) => (b.createdAt?.getTime() ?? 0) - (a.createdAt?.getTime() ?? 0));
		} else if (jobSort === 'oldest') {
			result.sort((a, b) => (a.createdAt?.getTime() ?? 0) - (b.createdAt?.getTime() ?? 0));
		} else if (jobSort === 'name-asc') {
			result.sort((a, b) => (a.name ?? a.id ?? '').localeCompare(b.name ?? b.id ?? ''));
		} else if (jobSort === 'name-desc') {
			result.sort((a, b) => (b.name ?? b.id ?? '').localeCompare(a.name ?? a.id ?? ''));
		}
		return result;
	})());

	const startedApps    = $derived(applications.filter((a) => a.state === 'STARTED').length);
	const runningJobs    = $derived(jobRuns.filter((j) => j.state === 'RUNNING' || j.state === 'SCHEDULED').length);
	const successfulJobs = $derived(jobRuns.filter((j) => j.state === 'SUCCESS').length);
	const failedJobs     = $derived(jobRuns.filter((j) => j.state === 'FAILED').length);

	function jobStateBadge(state?: string): string {
		return JOB_STATE_BADGE[state ?? ''] ?? 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	function appStateBadge(state?: string): string {
		return APP_STATE_BADGE[state ?? ''] ?? 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	function formatDate(d?: Date): string {
		if (!d) return '—';
		return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
	}

	async function loadData() {
		loading = true;
		try {
			const resp = await emr().send(new ListApplicationsCommand({}));
			applications = resp.applications ?? [];
		} catch (e) {
			toast.error('Failed to load EMR Serverless applications: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadJobs(appId: string) {
		selectedAppId = appId;
		jobStateFilter = 'ALL';
		jobSort = 'newest';
		searchQuery = '';
		jobsLoading = true;
		try {
			const resp = await emr().send(new ListJobRunsCommand({ applicationId: appId }));
			jobRuns = resp.jobRuns ?? [];
			activeTab = 'jobs';
		} catch (e) {
			toast.error('Failed to load job runs: ' + String(e));
		} finally {
			jobsLoading = false;
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<!-- Page header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Zap class="w-7 h-7 text-yellow-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon EMR Serverless</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Run big data frameworks without managing clusters</p>
			</div>
		</div>
		<button
			onclick={loadData}
			title="Refresh"
			class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
		>
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<!-- Metrics cards: Applications + Started + Running Jobs + Successful Jobs + Failed Jobs -->
	<div class="grid grid-cols-2 sm:grid-cols-5 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-yellow-100 dark:bg-yellow-900/30 rounded-lg"><Server class="w-5 h-5 text-yellow-600 dark:text-yellow-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{applications.length}</p><p class="text-xs text-gray-500 dark:text-gray-400">Applications</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{startedApps}</p><p class="text-xs text-gray-500 dark:text-gray-400">Started</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Activity class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{runningJobs}</p><p class="text-xs text-gray-500 dark:text-gray-400">Running Jobs</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Zap class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{successfulJobs}</p><p class="text-xs text-gray-500 dark:text-gray-400">Successful</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg"><XCircle class="w-5 h-5 text-red-600 dark:text-red-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{failedJobs}</p><p class="text-xs text-gray-500 dark:text-gray-400">Failed</p></div>
		</div>
	</div>

	<!-- Main card -->
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<!-- Tab bar + search + filters -->
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col gap-3">
			<div class="flex flex-col sm:flex-row gap-3 justify-between">
				<div class="flex gap-2">
					{#each ([['applications', 'Applications'], ['jobs', 'Job Runs'], ['docs', 'Docs']] as const) as [tab, label]}
						<button
							onclick={() => { activeTab = tab; searchQuery = ''; }}
							class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-yellow-500 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}"
						>
							{label}
						</button>
					{/each}
				</div>
				{#if activeTab !== 'docs'}
					<div class="relative">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
						<input
							bind:value={searchQuery}
							placeholder="Search..."
							class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64"
						/>
					</div>
				{/if}
			</div>

			{#if activeTab === 'applications'}
				<!-- State filter + sort row -->
				<div class="flex flex-col sm:flex-row gap-2 items-start sm:items-center justify-between">
					<div class="flex flex-wrap gap-1" role="group" aria-label="Filter applications by state">
						{#each APP_STATES as s}
							<button
								type="button"
								aria-pressed={appStateFilter === s}
								onclick={() => (appStateFilter = s)}
								class="px-2 py-1 rounded text-xs font-medium transition-colors {appStateFilter === s ? 'bg-yellow-500 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-slate-600'}"
							>
								{s}
							</button>
						{/each}
					</div>
					<div class="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400 shrink-0">
						<ArrowUpDown class="w-4 h-4" />
						<select
							bind:value={appSort}
							class="py-1 px-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"
						>
							<option value="newest">Newest first</option>
							<option value="oldest">Oldest first</option>
							<option value="name-asc">Name A→Z</option>
							<option value="name-desc">Name Z→A</option>
						</select>
					</div>
				</div>
			{:else if activeTab === 'jobs'}
				<!-- Selected application context line -->
				{#if selectedApp}
					<p class="text-xs text-gray-500 dark:text-gray-400">
						Showing job runs for <span class="font-semibold text-gray-700 dark:text-gray-300">{selectedApp.name}</span>
						<span class="ml-1 px-1.5 py-0.5 rounded {appStateBadge(selectedApp.state)} text-xs">{selectedApp.state}</span>
					</p>
				{/if}
				<!-- State filter + sort row -->
				<div class="flex flex-col sm:flex-row gap-2 items-start sm:items-center justify-between">
					<div class="flex flex-wrap gap-1" role="group" aria-label="Filter job runs by state">
						{#each JOB_STATES as s}
							<button
								type="button"
								aria-pressed={jobStateFilter === s}
								onclick={() => (jobStateFilter = s)}
								class="px-2 py-1 rounded text-xs font-medium transition-colors {jobStateFilter === s ? 'bg-yellow-500 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-600 dark:text-gray-400 hover:bg-gray-200 dark:hover:bg-slate-600'}"
							>
								{s}
							</button>
						{/each}
					</div>
					<div class="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400 shrink-0">
						<ArrowUpDown class="w-4 h-4" />
						<select
							bind:value={jobSort}
							class="py-1 px-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"
						>
							<option value="newest">Newest first</option>
							<option value="oldest">Oldest first</option>
							<option value="name-asc">Name A→Z</option>
							<option value="name-desc">Name Z→A</option>
						</select>
					</div>
				</div>
			{/if}
		</div>

		<!-- Content -->
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading…</div>
			{:else if activeTab === 'applications'}
				{#if filteredApps.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No applications found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredApps as app}
							<button
								onclick={() => loadJobs(app.id ?? '')}
								class="w-full flex items-center justify-between p-3 rounded-lg border {selectedAppId === app.id ? 'border-yellow-400 bg-yellow-50 dark:bg-yellow-900/10' : 'border-transparent bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700'} text-left transition-colors"
							>
								<div class="flex items-center gap-3 min-w-0">
									<Server class="w-5 h-5 text-yellow-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{app.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{app.type} · {app.releaseLabel} · Created {formatDate(app.createdAt)}</p>
									</div>
								</div>
								<div class="flex items-center gap-2 shrink-0 ml-3">
									<span class="text-xs text-gray-500 dark:text-gray-400">View Jobs →</span>
									<span class="text-xs px-2 py-1 rounded-full {appStateBadge(app.state)}">{app.state}</span>
								</div>
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'jobs'}
				{#if jobsLoading}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading job runs…</div>
				{:else if !selectedAppId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						<Activity class="w-10 h-10 mx-auto mb-2 opacity-30" />
						<p>Select an application from the <button onclick={() => (activeTab = 'applications')} class="underline text-yellow-600 dark:text-yellow-400">Applications</button> tab to view its job runs.</p>
					</div>
				{:else if filteredJobs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						{jobRuns.length === 0 ? 'No job runs found for this application.' : 'No job runs match the current filter.'}
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredJobs as job}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 border border-transparent hover:border-gray-200 dark:hover:border-slate-600 transition-colors">
								<div class="flex items-center gap-3 min-w-0">
									<Activity class="w-5 h-5 text-blue-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{job.name ?? job.id}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate font-mono">{job.id} · {formatDate(job.createdAt)}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full shrink-0 ml-3 {jobStateBadge(job.state)}">{job.state}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'docs'}
				<div class="prose prose-sm dark:prose-invert max-w-none space-y-4 text-gray-700 dark:text-gray-300">
					<div class="flex items-center gap-2 mb-3">
						<Info class="w-5 h-5 text-yellow-500" />
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white m-0">EMR Serverless Operations</h2>
					</div>
					<div class="grid sm:grid-cols-2 gap-4">
						{#each [
							{ op: 'CreateApplication', desc: 'Creates a serverless application (SPARK, HIVE, PRESTO, FLINK).' },
							{ op: 'GetApplication', desc: 'Retrieves configuration and state of an application.' },
							{ op: 'ListApplications', desc: 'Lists applications with optional state filter.' },
							{ op: 'UpdateApplication', desc: 'Updates release label or other mutable configuration.' },
							{ op: 'DeleteApplication', desc: 'Deletes a STOPPED or CREATED application.' },
							{ op: 'StartApplication', desc: 'Starts an application so it can accept job runs.' },
							{ op: 'StopApplication', desc: 'Stops a running application.' },
							{ op: 'StartJobRun', desc: 'Submits a job run to a STARTED application.' },
							{ op: 'GetJobRun', desc: 'Retrieves state and details of a specific job run.' },
							{ op: 'ListJobRuns', desc: 'Lists job runs with optional state filter and pagination.' },
							{ op: 'CancelJobRun', desc: 'Cancels a job run that is not yet in a terminal state.' },
							{ op: 'TagResource', desc: 'Adds or overwrites tags on an application or job run ARN.' },
							{ op: 'UntagResource', desc: 'Removes specific tag keys from a resource.' },
							{ op: 'ListTagsForResource', desc: 'Lists all tags attached to a resource ARN.' },
							{ op: 'GetDashboardForJobRun', desc: 'Returns a console URL for a job run dashboard.' },
							{ op: 'ListJobRunAttempts', desc: 'Returns attempt summaries for a job run (retries).' },
						] as entry}
							<div class="p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<p class="font-mono font-semibold text-sm text-yellow-700 dark:text-yellow-400">{entry.op}</p>
								<p class="text-xs text-gray-500 dark:text-gray-400 mt-1">{entry.desc}</p>
							</div>
						{/each}
					</div>
				</div>
			{/if}
		</div>
	</div>
</div>

