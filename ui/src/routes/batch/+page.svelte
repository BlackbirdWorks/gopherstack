<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getBatchClient } from '$lib/aws-client';
	import {
		DescribeJobQueuesCommand,
		DescribeJobDefinitionsCommand,
		ListJobsCommand,
		SubmitJobCommand,
		CancelJobCommand,
		TerminateJobCommand,
		CreateJobQueueCommand,
		DeleteJobQueueCommand,
		type JobQueueDetail,
		type JobDefinition,
		type JobSummary,
		type ComputeEnvironmentOrder
	} from '@aws-sdk/client-batch';
	import { toast } from 'svelte-sonner';
	import { Box, Search, RefreshCw, Plus, Trash2, Play, XCircle, ChevronRight, Layers, FileCode } from 'lucide-svelte';

	const batch = getBatchClient();

	let loading = $state(false);
	let activeTab = $state<'queues' | 'jobs' | 'definitions'>('queues');
	let searchQuery = $state('');

	// Job Queues
	let queues = $state<JobQueueDetail[]>([]);
	let selectedQueue = $state<JobQueueDetail | null>(null);

	// Job Definitions
	let definitions = $state<JobDefinition[]>([]);
	let loadingDefinitions = $state(false);

	// Jobs
	let jobs = $state<JobSummary[]>([]);
	let loadingJobs = $state(false);
	let jobStatusFilter = $state<'SUBMITTED' | 'PENDING' | 'RUNNABLE' | 'STARTING' | 'RUNNING' | 'SUCCEEDED' | 'FAILED'>('RUNNING');

	// Submit Job
	let showSubmitJob = $state(false);
	let submittingJob = $state(false);
	let submitJobName = $state('');
	let submitJobQueue = $state('');
	let submitJobDef = $state('');
	let submitJobContainerOverrides = $state('{}');

	// Create Queue
	let showCreateQueue = $state(false);
	let creatingQueue = $state(false);
	let newQueueName = $state('');
	let newQueuePriority = $state(100);
	let newComputeEnvArn = $state('');

	const statusColors: Record<string, string> = {
		SUCCEEDED: 'green',
		FAILED: 'red',
		RUNNING: 'blue',
		STARTING: 'yellow',
		RUNNABLE: 'yellow',
		PENDING: 'gray',
		SUBMITTED: 'gray'
	};

	const filteredQueues = $derived(
		queues.filter((q) => !searchQuery || (q.jobQueueName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredDefinitions = $derived(
		definitions.filter((d) => !searchQuery || (d.jobDefinitionName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredJobs = $derived(
		jobs.filter((j) => !searchQuery || (j.jobName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function loadQueues() {
		loading = true;
		try {
			const resp = await batch.send(new DescribeJobQueuesCommand({ maxResults: 50 }));
			queues = resp.jobQueues ?? [];
		} catch (e) {
			toast.error('Failed to load queues: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadDefinitions() {
		loadingDefinitions = true;
		try {
			const resp = await batch.send(new DescribeJobDefinitionsCommand({ maxResults: 50, status: 'ACTIVE' }));
			definitions = resp.jobDefinitions ?? [];
		} catch (e) {
			toast.error('Failed to load definitions: ' + String(e));
		} finally {
			loadingDefinitions = false;
		}
	}

	async function loadJobs(queueArn?: string) {
		loadingJobs = true;
		try {
			const params: { jobQueue?: string; jobStatus: typeof jobStatusFilter; maxResults: number } = {
				jobStatus: jobStatusFilter,
				maxResults: 50
			};
			if (queueArn) params.jobQueue = queueArn;
			const resp = await batch.send(new ListJobsCommand(params));
			jobs = resp.jobSummaryList ?? [];
		} catch (e) {
			toast.error('Failed to load jobs: ' + String(e));
		} finally {
			loadingJobs = false;
		}
	}

	async function handleTabChange(tab: 'queues' | 'jobs' | 'definitions') {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'definitions' && definitions.length === 0) await loadDefinitions();
		if (tab === 'jobs') await loadJobs();
	}

	async function createQueue() {
		if (!newQueueName.trim()) return;
		creatingQueue = true;
		try {
			const computeEnvOrder: ComputeEnvironmentOrder[] = newComputeEnvArn.trim()
				? [{ order: 1, computeEnvironment: newComputeEnvArn.trim() }]
				: [];
			await batch.send(new CreateJobQueueCommand({
				jobQueueName: newQueueName.trim(),
				priority: newQueuePriority,
				computeEnvironmentOrder: computeEnvOrder,
				state: 'ENABLED'
			}));
			toast.success(`Queue "${newQueueName}" created`);
			showCreateQueue = false;
			newQueueName = '';
			newQueuePriority = 100;
			newComputeEnvArn = '';
			await loadQueues();
		} catch (e) {
			toast.error('Failed to create queue: ' + String(e));
		} finally {
			creatingQueue = false;
		}
	}

	async function deleteQueue(name: string) {
		if (!await confirmDestructive(`Delete job queue "${name}"?`)) return;
		try {
			await batch.send(new DeleteJobQueueCommand({ jobQueue: name }));
			toast.success(`Queue "${name}" deleted`);
			await loadQueues();
		} catch (e) {
			toast.error('Failed to delete queue: ' + String(e));
		}
	}

	async function submitJob() {
		if (!submitJobName.trim() || !submitJobQueue.trim() || !submitJobDef.trim()) return;
		submittingJob = true;
		try {
			let containerOverrides;
			try {
				const parsed = JSON.parse(submitJobContainerOverrides);
				if (Object.keys(parsed).length > 0) containerOverrides = parsed;
			} catch {
				// Ignore invalid overrides and submit without them.
			}
			const resp = await batch.send(new SubmitJobCommand({
				jobName: submitJobName.trim(),
				jobQueue: submitJobQueue.trim(),
				jobDefinition: submitJobDef.trim(),
				containerOverrides
			}));
			toast.success(`Job "${resp.jobName}" submitted (ID: ${resp.jobId})`);
			showSubmitJob = false;
			submitJobName = '';
			submitJobContainerOverrides = '{}';
			await loadJobs();
		} catch (e) {
			toast.error('Failed to submit job: ' + String(e));
		} finally {
			submittingJob = false;
		}
	}

	async function cancelJob(jobId: string) {
		try {
			await batch.send(new CancelJobCommand({ jobId, reason: 'Cancelled by user' }));
			toast.success('Job cancelled');
			await loadJobs();
		} catch (e) {
			toast.error('Failed to cancel job: ' + String(e));
		}
	}

	async function terminateJob(jobId: string) {
		if (!await confirmDestructive('Terminate this job?')) return;
		try {
			await batch.send(new TerminateJobCommand({ jobId, reason: 'Terminated by user' }));
			toast.success('Job terminated');
			await loadJobs();
		} catch (e) {
			toast.error('Failed to terminate job: ' + String(e));
		}
	}

	function formatDate(d: number | undefined): string {
		if (!d) return '-';
		return new Date(d).toLocaleString();
	}

	onMount(loadQueues);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Box class="w-7 h-7 text-purple-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Batch</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Fully managed batch computing</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={() => { if (activeTab === 'queues') loadQueues(); else if (activeTab === 'jobs') loadJobs(); else loadDefinitions(); }} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			{#if activeTab === 'queues'}
				<button onclick={() => (showCreateQueue = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm font-medium">
					<Plus class="w-4 h-4" /> Create Queue
				</button>
			{:else if activeTab === 'jobs'}
				<button onclick={() => (showSubmitJob = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm font-medium">
					<Play class="w-4 h-4" /> Submit Job
				</button>
			{/if}
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
		{#each [['queues', 'Job Queues', Layers], ['jobs', 'Jobs', Play], ['definitions', 'Job Definitions', FileCode]] as [tab, label, _icon]}
			<button
				onclick={() => handleTabChange(tab as 'queues' | 'jobs' | 'definitions')}
				class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
			>
				{label}
			</button>
		{/each}
	</div>

	<!-- Search -->
	<div class="relative">
		<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
		<input bind:value={searchQuery} type="text" placeholder="Search..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
	</div>

	<!-- QUEUES TAB -->
	{#if activeTab === 'queues'}
		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-purple-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredQueues.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Layers class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No job queues found</p>
				<p class="text-sm mt-1">Create a queue to run batch jobs</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Queue Name</th>
							<th class="px-4 py-3 text-left">State</th>
							<th class="px-4 py-3 text-left">Status</th>
							<th class="px-4 py-3 text-left">Priority</th>
							<th class="px-4 py-3 text-left">Compute Envs</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredQueues as queue}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3 font-medium text-purple-600 dark:text-purple-400">{queue.jobQueueName}</td>
								<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${queue.state === 'ENABLED' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{queue.state}</span></td>
								<td class="px-4 py-3 text-xs text-gray-500">{queue.status}</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{queue.priority}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{(queue.computeEnvironmentOrder ?? []).length} env(s)</td>
								<td class="px-4 py-3">
									<button onclick={() => deleteQueue(queue.jobQueueName ?? '')} class="text-red-500 hover:text-red-700 p-1">
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

	<!-- JOBS TAB -->
	{#if activeTab === 'jobs'}
		<div class="flex flex-wrap gap-2 items-center">
			<span class="text-sm text-gray-600 dark:text-gray-400">Status:</span>
			{#each ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING', 'SUCCEEDED', 'FAILED'] as status}
				<button
					onclick={() => { jobStatusFilter = status as typeof jobStatusFilter; loadJobs(); }}
					class={`px-3 py-1 rounded-full text-xs font-medium ${jobStatusFilter === status ? `bg-${statusColors[status]}-100 text-${statusColors[status]}-700 dark:bg-${statusColors[status]}-900/30 dark:text-${statusColors[status]}-400 ring-1 ring-${statusColors[status]}-400` : 'bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400'}`}
				>
					{status}
				</button>
			{/each}
		</div>

		{#if loadingJobs}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-purple-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredJobs.length === 0}
			<div class="text-center py-12 text-gray-500 dark:text-gray-400">
				<Play class="w-10 h-10 mx-auto mb-2 opacity-40" />
				<p>No {jobStatusFilter.toLowerCase()} jobs found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Job Name</th>
							<th class="px-4 py-3 text-left">Job ID</th>
							<th class="px-4 py-3 text-left">Status</th>
							<th class="px-4 py-3 text-left">Created</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredJobs as job}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3 font-medium">{job.jobName}</td>
								<td class="px-4 py-3 font-mono text-xs text-gray-500">{job.jobId}</td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColors[job.status ?? ''] ?? 'gray'}-100 text-${statusColors[job.status ?? ''] ?? 'gray'}-700`}>
										{job.status}
									</span>
								</td>
								<td class="px-4 py-3 text-xs text-gray-500">{formatDate(job.createdAt)}</td>
								<td class="px-4 py-3">
									{#if job.status === 'SUBMITTED' || job.status === 'PENDING' || job.status === 'RUNNABLE'}
										<button onclick={() => cancelJob(job.jobId ?? '')} class="text-yellow-500 hover:text-yellow-700 p-1" title="Cancel">
											<XCircle class="w-4 h-4" />
										</button>
									{:else if job.status === 'STARTING' || job.status === 'RUNNING'}
										<button onclick={() => terminateJob(job.jobId ?? '')} class="text-red-500 hover:text-red-700 p-1" title="Terminate">
											<XCircle class="w-4 h-4" />
										</button>
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- DEFINITIONS TAB -->
	{#if activeTab === 'definitions'}
		{#if loadingDefinitions}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-purple-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredDefinitions.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<FileCode class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No active job definitions</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Definition Name</th>
							<th class="px-4 py-3 text-left">Revision</th>
							<th class="px-4 py-3 text-left">Type</th>
							<th class="px-4 py-3 text-left">Status</th>
							<th class="px-4 py-3 text-left">Platform</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredDefinitions as def}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3 font-medium text-purple-600 dark:text-purple-400">{def.jobDefinitionName}</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{def.revision}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{def.type}</td>
								<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${def.status === 'ACTIVE' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{def.status}</span></td>
								<td class="px-4 py-3 text-xs text-gray-500">{def.platformCapabilities?.join(', ') ?? 'EC2'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Queue Modal -->
{#if showCreateQueue}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Job Queue</h2>
			<div>
				<label for="queue-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Queue Name</label>
				<input id="queue-name" bind:value={newQueueName} type="text" placeholder="my-batch-queue" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="queue-priority" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Priority (higher = more precedence)</label>
				<input id="queue-priority" bind:value={newQueuePriority} type="number" min="1" max="1000" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="compute-env-arn" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Compute Environment ARN</label>
				<input id="compute-env-arn" bind:value={newComputeEnvArn} type="text" placeholder="arn:aws:batch:..." class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateQueue = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createQueue} disabled={creatingQueue || !newQueueName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-purple-600 text-white text-sm font-medium hover:bg-purple-700 disabled:opacity-50">
					{creatingQueue ? 'Creating...' : 'Create Queue'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Submit Job Modal -->
{#if showSubmitJob}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Submit Job</h2>
			<div>
				<label for="job-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Job Name</label>
				<input id="job-name" bind:value={submitJobName} type="text" placeholder="my-batch-job" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="submit-queue" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Job Queue</label>
				{#if queues.length > 0}
					<select id="submit-queue" bind:value={submitJobQueue} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="">Select queue...</option>
						{#each queues as q}
							<option value={q.jobQueueName}>{q.jobQueueName}</option>
						{/each}
					</select>
				{:else}
					<input id="submit-queue" bind:value={submitJobQueue} type="text" placeholder="my-queue" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				{/if}
			</div>
			<div>
				<label for="job-def" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Job Definition</label>
				{#if definitions.length > 0}
					<select id="job-def" bind:value={submitJobDef} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="">Select definition...</option>
						{#each definitions as d}
							<option value={`${d.jobDefinitionName}:${d.revision}`}>{d.jobDefinitionName}:{d.revision}</option>
						{/each}
					</select>
				{:else}
					<input id="job-def" bind:value={submitJobDef} type="text" placeholder="my-job-def:1" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				{/if}
			</div>
			<div>
				<label for="container-overrides" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Container Overrides (JSON)</label>
				<textarea id="container-overrides" bind:value={submitJobContainerOverrides} rows={4} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showSubmitJob = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={submitJob} disabled={submittingJob || !submitJobName.trim() || !submitJobQueue.trim() || !submitJobDef.trim()} class="flex-1 px-4 py-2 rounded-lg bg-purple-600 text-white text-sm font-medium hover:bg-purple-700 disabled:opacity-50">
					{submittingJob ? 'Submitting...' : 'Submit Job'}
				</button>
			</div>
		</div>
	</div>
{/if}
