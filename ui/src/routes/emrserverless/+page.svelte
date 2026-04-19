<script lang="ts">
	import { onMount } from 'svelte';
	import { getEMRServerlessClient } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		ListJobRunsCommand,
		type ApplicationSummary,
		type JobRunSummary
	} from '@aws-sdk/client-emr-serverless';
	import { toast } from 'svelte-sonner';
	import { Zap, RefreshCw, Search, Activity, Server, CheckCircle } from 'lucide-svelte';

	const emr = getEMRServerlessClient();

	let loading = $state(false);
	let activeTab = $state<'applications' | 'jobs'>('applications');
	let searchQuery = $state('');
	let applications = $state<ApplicationSummary[]>([]);
	let jobRuns = $state<JobRunSummary[]>([]);
	let selectedAppId = $state<string | null>(null);

	const filteredApps = $derived(applications.filter((a) => (a.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredJobs = $derived(jobRuns.filter((j) => (j.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const runningApps = $derived(applications.filter((a) => a.state === 'STARTED').length);
	const successfulJobs = $derived(jobRuns.filter((j) => j.state === 'SUCCESS').length);

	async function loadData() {
		loading = true;
		try {
			const resp = await emr.send(new ListApplicationsCommand({}));
			applications = resp.applications ?? [];
		} catch (e) {
			toast.error('Failed to load EMR Serverless data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadJobs(appId: string) {
		selectedAppId = appId;
		try {
			const resp = await emr.send(new ListJobRunsCommand({ applicationId: appId }));
			jobRuns = resp.jobRuns ?? [];
			activeTab = 'jobs';
		} catch (e) {
			toast.error('Failed to load job runs: ' + String(e));
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Zap class="w-7 h-7 text-yellow-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon EMR Serverless</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Run big data frameworks without managing clusters</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-yellow-100 dark:bg-yellow-900/30 rounded-lg"><Server class="w-5 h-5 text-yellow-600 dark:text-yellow-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{applications.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Applications</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{runningApps}</p><p class="text-sm text-gray-500 dark:text-gray-400">Started</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Activity class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{jobRuns.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Job Runs</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Zap class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{successfulJobs}</p><p class="text-sm text-gray-500 dark:text-gray-400">Successful Jobs</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['applications', 'Applications'], ['jobs', 'Job Runs']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-yellow-500 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'applications'}
				{#if filteredApps.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No applications found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredApps as app}
							<button onclick={() => loadJobs(app.id ?? '')}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left">
								<div class="flex items-center gap-3">
									<Server class="w-5 h-5 text-yellow-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{app.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{app.type} · {app.releaseLabel}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {app.state === 'STARTED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{app.state}</span>
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'jobs'}
				{#if filteredJobs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No job runs found. Select an application to view its job runs.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredJobs as job}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{job.name ?? job.id}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{job.id}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {job.state === 'SUCCESS' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : job.state === 'FAILED' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400' : 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400'}">{job.state}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
