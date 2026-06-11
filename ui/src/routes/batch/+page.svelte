<script lang="ts">
import { confirmDestructive } from '$lib/confirm-dialog';
import { onMount } from 'svelte';
import { getBatchClient, getCloudWatchLogsClient } from '$lib/aws-client';
import { GetLogEventsCommand } from '@aws-sdk/client-cloudwatch-logs';
import {
DescribeComputeEnvironmentsCommand,
DescribeJobQueuesCommand,
DescribeJobDefinitionsCommand,
ListJobsCommand,
SubmitJobCommand,
CancelJobCommand,
TerminateJobCommand,
CreateJobQueueCommand,
DeleteJobQueueCommand,
CreateComputeEnvironmentCommand,
DeleteComputeEnvironmentCommand,
UpdateComputeEnvironmentCommand,
UpdateJobQueueCommand,
DescribeJobsCommand,
DescribeServiceEnvironmentsCommand,
type ComputeEnvironmentDetail,
type JobQueueDetail,
type JobDefinition,
type JobSummary,
type JobDetail,
type ComputeEnvironmentOrder,
type ServiceEnvironmentDetail,
type DescribeServiceEnvironmentsCommandOutput
} from '@aws-sdk/client-batch';
import { toast } from 'svelte-sonner';
import { Box, Search, RefreshCw, Plus, Trash2, Play, XCircle, Layers, FileCode, Server, BookOpen, Terminal } from 'lucide-svelte';

const batch = getBatchClient();
const cwl = getCloudWatchLogsClient();

type ActiveTab = 'queues' | 'compute-environments' | 'service-environments' | 'jobs' | 'definitions' | 'metrics' | 'docs';

let loading = $state(false);
let activeTab = $state<ActiveTab>('queues');
let searchQuery = $state('');

// Compute Environments
let computeEnvironments = $state<ComputeEnvironmentDetail[]>([]);
let loadingCEs = $state(false);

// Service Environments
let serviceEnvironments = $state<ServiceEnvironmentDetail[]>([]);
let loadingSEs = $state(false);

// Jobs: per-queue counts keyed by queue name
let jobCountByQueue = $state<Record<string, number>>({});

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

// Create Compute Environment
let showCreateCE = $state(false);
let creatingCE = $state(false);
let newCEName = $state('');
let newCEType = $state('UNMANAGED');
let newCEState = $state('ENABLED');

// Job JSON viewer
let selectedJob = $state<JobDetail | null>(null);
let loadingJobDetail = $state(false);

// Create Queue
let showCreateQueue = $state(false);
let creatingQueue = $state(false);
let newQueueName = $state('');
let newQueuePriority = $state(100);
let newComputeEnvArn = $state('');

const statusBadgeClass: Record<string, string> = {
SUCCEEDED: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400',
FAILED: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
RUNNING: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400',
STARTING: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400',
RUNNABLE: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400',
PENDING: 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-400',
SUBMITTED: 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-400',
ENABLED: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400',
DISABLED: 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-400',
ACTIVE: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400',
INACTIVE: 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-400',
VALID: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400',
INVALID: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
};

function badgeClass(status: string | undefined): string {
return statusBadgeClass[status ?? ''] ?? 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-400';
}

const filteredQueues = $derived(
queues.filter((q) => !searchQuery || (q.jobQueueName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
);
const filteredCEs = $derived(
computeEnvironments.filter((c) => !searchQuery || (c.computeEnvironmentName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
);
const filteredSEs = $derived(
serviceEnvironments.filter((s) => !searchQuery || (s.serviceEnvironmentName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
);
const filteredDefinitions = $derived(
definitions.filter((d) => !searchQuery || (d.jobDefinitionName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
);
const filteredJobs = $derived(
jobs.filter((j) => !searchQuery || (j.jobName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
);

async function loadComputeEnvironments() {
loadingCEs = true;
try {
const resp = await batch.send(new DescribeComputeEnvironmentsCommand({ maxResults: 50 }));
computeEnvironments = resp.computeEnvironments ?? [];
} catch (e) {
toast.error('Failed to load compute environments: ' + String(e));
} finally {
loadingCEs = false;
}
}

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

async function loadMetrics() {
await Promise.all([
computeEnvironments.length === 0 ? loadComputeEnvironments() : Promise.resolve(),
queues.length === 0 ? loadQueues() : Promise.resolve(),
definitions.length === 0 ? loadDefinitions() : Promise.resolve(),
]);
if (jobs.length === 0) await loadJobs();
}

async function handleTabChange(tab: ActiveTab) {
activeTab = tab;
searchQuery = '';
if (tab === 'compute-environments' && computeEnvironments.length === 0) await loadComputeEnvironments();
if (tab === 'service-environments' && serviceEnvironments.length === 0) await loadServiceEnvironments();
if (tab === 'definitions' && definitions.length === 0) await loadDefinitions();
if (tab === 'jobs') await loadJobs();
if (tab === 'metrics') await loadMetrics();
if (tab === 'queues') await loadJobCounts();
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
if (!await confirmDestructive({ title: 'Delete Job Queue', message: `Delete job queue "${name}"? All pending jobs will be removed.` })) return;
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
if (!await confirmDestructive({ title: 'Terminate Job', message: 'Terminate this job? This cannot be undone.', confirmLabel: 'Terminate' })) return;
try {
await batch.send(new TerminateJobCommand({ jobId, reason: 'Terminated by user' }));
toast.success('Job terminated');
await loadJobs();
} catch (e) {
toast.error('Failed to terminate job: ' + String(e));
}
}

async function loadServiceEnvironments() {
loadingSEs = true;
try {
const resp = await batch.send(new DescribeServiceEnvironmentsCommand({ maxResults: 50 }));
serviceEnvironments = (resp as DescribeServiceEnvironmentsCommandOutput).serviceEnvironments ?? [];
} catch (e) {
toast.error('Failed to load service environments: ' + String(e));
} finally {
loadingSEs = false;
}
}

async function createComputeEnvironment() {
if (!newCEName.trim()) return;
creatingCE = true;
try {
await batch.send(new CreateComputeEnvironmentCommand({
computeEnvironmentName: newCEName.trim(),
type: newCEType as 'MANAGED' | 'UNMANAGED',
state: newCEState as 'ENABLED' | 'DISABLED'
}));
toast.success(`Compute environment "${newCEName}" created`);
showCreateCE = false;
newCEName = '';
newCEType = 'UNMANAGED';
newCEState = 'ENABLED';
await loadComputeEnvironments();
} catch (e) {
toast.error('Failed to create compute environment: ' + String(e));
} finally {
creatingCE = false;
}
}

async function deleteCE(name: string) {
if (!await confirmDestructive({ title: 'Delete Compute Environment', message: `Delete compute environment "${name}"?` })) return;
try {
await batch.send(new UpdateComputeEnvironmentCommand({ computeEnvironment: name, state: 'DISABLED' }));
await batch.send(new DeleteComputeEnvironmentCommand({ computeEnvironment: name }));
toast.success(`Compute environment "${name}" deleted`);
await loadComputeEnvironments();
} catch (e) {
toast.error('Failed to delete compute environment: ' + String(e));
}
}

async function toggleQueueState(queue: JobQueueDetail) {
const newState = queue.state === 'ENABLED' ? 'DISABLED' : 'ENABLED';
try {
await batch.send(new UpdateJobQueueCommand({ jobQueue: queue.jobQueueName!, state: newState as 'ENABLED' | 'DISABLED' }));
toast.success(`Queue "${queue.jobQueueName}" ${newState.toLowerCase()}`);
await loadQueues();
} catch (e) {
toast.error('Failed to update queue: ' + String(e));
}
}

async function loadJobDetail(job: JobSummary) {
loadingJobDetail = true;
jobLogEvents = [];
jobLogError = '';
try {
const resp = await batch.send(new DescribeJobsCommand({ jobs: [job.jobId!] }));
selectedJob = (resp.jobs ?? [])[0] ?? null;
} catch (e) {
toast.error('Failed to load job details: ' + String(e));
} finally {
loadingJobDetail = false;
}
}

let jobLogEvents = $state<{ timestamp?: number; message?: string }[]>([]);
let loadingJobLogs = $state(false);
let jobLogError = $state('');

// AWS Batch container logs land in the /aws/batch/job CloudWatch log group,
// keyed by the container's logStreamName.
async function loadJobLogs() {
const stream = selectedJob?.container?.logStreamName;
if (!stream) {
jobLogError = 'No log stream is associated with this job yet.';
return;
}
loadingJobLogs = true;
jobLogError = '';
try {
const resp = await cwl.send(
new GetLogEventsCommand({
logGroupName: '/aws/batch/job',
logStreamName: stream,
startFromHead: true,
limit: 500
})
);
jobLogEvents = (resp.events ?? []).map((e) => ({ timestamp: e.timestamp, message: e.message }));
if (jobLogEvents.length === 0) jobLogError = 'No log events found for this stream.';
} catch (e) {
jobLogError = 'Failed to load logs: ' + String(e);
} finally {
loadingJobLogs = false;
}
}

async function loadJobCounts() {
const counts: Record<string, number> = {};
for (const q of queues) {
if (!q.jobQueueName) continue;
try {
const resp = await batch.send(new ListJobsCommand({ jobQueue: q.jobQueueName, maxResults: 100 }));
counts[q.jobQueueName] = (resp.jobSummaryList ?? []).length;
} catch {
counts[q.jobQueueName] = 0;
}
}
jobCountByQueue = counts;
}

function formatDate(d: number | undefined): string {
if (!d) return '-';
return new Date(d).toLocaleString();
}

onMount(async () => { await Promise.all([loadQueues(), loadJobCounts()]); });
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
<button onclick={() => {
if (activeTab === 'queues') { loadQueues(); loadJobCounts(); }
else if (activeTab === 'compute-environments') loadComputeEnvironments();
else if (activeTab === 'service-environments') loadServiceEnvironments();
else if (activeTab === 'jobs') loadJobs();
else if (activeTab === 'definitions') loadDefinitions();
else if (activeTab === 'metrics') loadMetrics();
}} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
<RefreshCw class="w-4 h-4" /> Refresh
</button>
{#if activeTab === 'queues'}
<button onclick={() => (showCreateQueue = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm font-medium">
<Plus class="w-4 h-4" /> Create Queue
</button>
{:else if activeTab === 'compute-environments'}
<button onclick={() => (showCreateCE = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm font-medium">
<Plus class="w-4 h-4" /> Create CE
</button>
{:else if activeTab === 'jobs'}
<button onclick={() => (showSubmitJob = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm font-medium">
<Play class="w-4 h-4" /> Submit Job
</button>
{/if}
</div>
</div>

<!-- Tabs -->
<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700 flex-wrap">
{#each [['queues', 'Job Queues'], ['compute-environments', 'Compute Envs'], ['service-environments', 'Service Envs'], ['jobs', 'Jobs'], ['definitions', 'Job Definitions'], ['metrics', 'Metrics'], ['docs', 'Docs']] as [tab, label]}
<button
onclick={() => handleTabChange(tab as ActiveTab)}
class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
>
{label}
</button>
{/each}
</div>

<!-- Search (not on metrics/docs) -->
{#if activeTab !== 'metrics' && activeTab !== 'docs'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
<input bind:value={searchQuery} type="text" placeholder="Search..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
</div>
{/if}

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
<th class="px-4 py-3 text-left">Jobs</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each filteredQueues as queue}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
<td class="px-4 py-3 font-medium text-purple-600 dark:text-purple-400" title={queue.jobQueueArn}>{queue.jobQueueName}</td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${badgeClass(queue.state)}`}>{queue.state}</span></td>
<td class="px-4 py-3 text-xs text-gray-500">{queue.status}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{queue.priority}</td>
<td class="px-4 py-3 text-xs text-gray-500">{(queue.computeEnvironmentOrder ?? []).length} env(s)</td>
<td class="px-4 py-3 text-xs text-gray-500">{jobCountByQueue[queue.jobQueueName ?? ''] ?? 0}</td>
<td class="px-4 py-3 flex items-center gap-1">
<button onclick={() => toggleQueueState(queue)} class={`p-1 ${queue.state === 'ENABLED' ? 'text-yellow-500 hover:text-yellow-700' : 'text-green-500 hover:text-green-700'}`} title={queue.state === 'ENABLED' ? 'Disable' : 'Enable'}>
{#if queue.state === 'ENABLED'}
<svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636" /></svg>
{:else}
<svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7" /></svg>
{/if}
</button>
<button onclick={() => deleteQueue(queue.jobQueueName ?? '')} class="text-red-500 hover:text-red-700 p-1" title="Delete">
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

<!-- SERVICE ENVIRONMENTS TAB -->
{#if activeTab === 'service-environments'}
{#if loadingSEs}
<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-purple-600 border-t-transparent rounded-full"></div></div>
{:else if filteredSEs.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
<Server class="w-12 h-12 mx-auto mb-3 opacity-40" />
<p class="font-medium">No service environments found</p>
<p class="text-sm mt-1">Service environments group compute resources for specific workloads</p>
</div>
{:else}
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
<tr>
<th class="px-4 py-3 text-left">Name</th>
<th class="px-4 py-3 text-left">Type</th>
<th class="px-4 py-3 text-left">State</th>
<th class="px-4 py-3 text-left">Status</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each filteredSEs as se}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
<td class="px-4 py-3 font-medium text-purple-600 dark:text-purple-400" title={se.serviceEnvironmentArn}>{se.serviceEnvironmentName}</td>
<td class="px-4 py-3 text-xs text-gray-500">{se.serviceEnvironmentType}</td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${badgeClass(se.state)}`}>{se.state}</span></td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${badgeClass(se.status)}`}>{se.status}</span></td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- COMPUTE ENVIRONMENTS TAB -->
{#if activeTab === 'compute-environments'}
{#if loadingCEs}
<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-purple-600 border-t-transparent rounded-full"></div></div>
{:else if filteredCEs.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
<Server class="w-12 h-12 mx-auto mb-3 opacity-40" />
<p class="font-medium">No compute environments found</p>
<p class="text-sm mt-1">Compute environments provide the infrastructure to run batch jobs</p>
</div>
{:else}
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
<tr>
<th class="px-4 py-3 text-left">Name</th>
<th class="px-4 py-3 text-left">Type</th>
<th class="px-4 py-3 text-left">State</th>
<th class="px-4 py-3 text-left">Status</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each filteredCEs as ce}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
<td class="px-4 py-3 font-medium text-purple-600 dark:text-purple-400" title={ce.computeEnvironmentArn}>{ce.computeEnvironmentName}</td>
<td class="px-4 py-3 text-xs text-gray-500">{ce.type}</td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${badgeClass(ce.state)}`}>{ce.state}</span></td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${badgeClass(ce.status)}`}>{ce.status}</span></td>
<td class="px-4 py-3">
<button onclick={() => deleteCE(ce.computeEnvironmentName ?? '')} class="text-red-500 hover:text-red-700 p-1" title="Delete">
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
class={`px-3 py-1 rounded-full text-xs font-medium ${jobStatusFilter === status ? badgeClass(status) + ' ring-1 ring-current' : 'bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400'}`}
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
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors cursor-pointer" onclick={() => loadJobDetail(job)}>
<td class="px-4 py-3 font-medium">{job.jobName}</td>
<td class="px-4 py-3 font-mono text-xs text-gray-500">{job.jobId}</td>
<td class="px-4 py-3">
<span class={`px-2 py-0.5 rounded text-xs font-medium ${badgeClass(job.status)}`}>
{job.status}
</span>
</td>
<td class="px-4 py-3 text-xs text-gray-500">{formatDate(job.createdAt)}</td>
<td class="px-4 py-3" onclick={(e) => e.stopPropagation()}>
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
<td class="px-4 py-3 font-medium text-purple-600 dark:text-purple-400" title={def.jobDefinitionArn}>{def.jobDefinitionName}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{def.revision}</td>
<td class="px-4 py-3 text-xs text-gray-500">{def.type}</td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${badgeClass(def.status)}`}>{def.status}</span></td>
<td class="px-4 py-3 text-xs text-gray-500">{def.platformCapabilities?.join(', ') ?? 'EC2'}</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- METRICS TAB -->
{#if activeTab === 'metrics'}
<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5 flex flex-col gap-1">
<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">Compute Environments</span>
<span class="text-3xl font-bold text-gray-900 dark:text-white">{computeEnvironments.length}</span>
</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5 flex flex-col gap-1">
<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">Job Queues</span>
<span class="text-3xl font-bold text-gray-900 dark:text-white">{queues.length}</span>
</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5 flex flex-col gap-1">
<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">Jobs</span>
<span class="text-3xl font-bold text-gray-900 dark:text-white">{jobs.length}</span>
</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5 flex flex-col gap-1">
<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">Job Definitions</span>
<span class="text-3xl font-bold text-gray-900 dark:text-white">{definitions.length}</span>
</div>
</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5 space-y-3">
<h3 class="font-semibold text-gray-900 dark:text-white">Queue States</h3>
<div class="flex flex-wrap gap-4 text-sm">
<div class="flex items-center gap-2">
<span class="w-2 h-2 rounded-full bg-green-500"></span>
<span class="text-gray-600 dark:text-gray-400">Enabled: {queues.filter(q => q.state === 'ENABLED').length}</span>
</div>
<div class="flex items-center gap-2">
<span class="w-2 h-2 rounded-full bg-gray-400"></span>
<span class="text-gray-600 dark:text-gray-400">Disabled: {queues.filter(q => q.state === 'DISABLED').length}</span>
</div>
</div>
</div>
{/if}

<!-- DOCS TAB -->
{#if activeTab === 'docs'}
<div class="space-y-4">
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6 space-y-4">
<h3 class="font-semibold text-gray-900 dark:text-white flex items-center gap-2"><BookOpen class="w-4 h-4" /> Compute Environment Operations</h3>
<dl class="space-y-3 text-sm">
<div><dt class="font-medium text-gray-800 dark:text-gray-200">CreateComputeEnvironment</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Creates a managed or unmanaged compute environment that provides compute resources for batch jobs.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">DescribeComputeEnvironments</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Returns details about one or more compute environments.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">UpdateComputeEnvironment</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Updates the state (ENABLED/DISABLED) of a compute environment.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">DeleteComputeEnvironment</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Deletes a compute environment. Must be in DISABLED state first.</dd></div>
</dl>
</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6 space-y-4">
<h3 class="font-semibold text-gray-900 dark:text-white flex items-center gap-2"><Layers class="w-4 h-4" /> Job Queue Operations</h3>
<dl class="space-y-3 text-sm">
<div><dt class="font-medium text-gray-800 dark:text-gray-200">CreateJobQueue</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Creates a job queue that maps to one or more compute environments with ordering.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">DescribeJobQueues</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Returns details about one or more job queues.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">UpdateJobQueue</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Updates the state or priority of a job queue.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">DeleteJobQueue</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Deletes a job queue. Must be in DISABLED state first.</dd></div>
</dl>
</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6 space-y-4">
<h3 class="font-semibold text-gray-900 dark:text-white flex items-center gap-2"><Play class="w-4 h-4" /> Job Operations</h3>
<dl class="space-y-3 text-sm">
<div><dt class="font-medium text-gray-800 dark:text-gray-200">SubmitJob</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Submits a batch job to a job queue using a job definition. Job name must be 1-128 characters.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">ListJobs</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Lists jobs in a queue filtered by status. Omit jobQueue to list all jobs.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">DescribeJobs</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Returns full details for specified job IDs or ARNs.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">CancelJob</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Cancels a job in SUBMITTED, PENDING, or RUNNABLE state.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">TerminateJob</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Terminates a running job. Cannot terminate jobs already in SUCCEEDED or FAILED state.</dd></div>
</dl>
</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6 space-y-4">
<h3 class="font-semibold text-gray-900 dark:text-white flex items-center gap-2"><FileCode class="w-4 h-4" /> Job Definition Operations</h3>
<dl class="space-y-3 text-sm">
<div><dt class="font-medium text-gray-800 dark:text-gray-200">RegisterJobDefinition</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Registers a new job definition or a new revision of an existing definition.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">DescribeJobDefinitions</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Returns details about job definitions, sorted by revision descending.</dd></div>
<div><dt class="font-medium text-gray-800 dark:text-gray-200">DeregisterJobDefinition</dt><dd class="text-gray-500 dark:text-gray-400 mt-0.5">Marks a job definition as INACTIVE. It remains visible until the janitor TTL expires.</dd></div>
</dl>
</div>
</div>
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

<!-- Create Compute Environment Modal -->
{#if showCreateCE}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Compute Environment</h2>
<div>
<label for="ce-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name</label>
<input id="ce-name" bind:value={newCEName} type="text" placeholder="my-compute-env" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<div>
<label for="ce-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Type</label>
<select id="ce-type" bind:value={newCEType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
<option value="MANAGED">MANAGED</option>
<option value="UNMANAGED">UNMANAGED</option>
</select>
</div>
<div>
<label for="ce-state" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">State</label>
<select id="ce-state" bind:value={newCEState} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
<option value="ENABLED">ENABLED</option>
<option value="DISABLED">DISABLED</option>
</select>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => (showCreateCE = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createComputeEnvironment} disabled={creatingCE || !newCEName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-purple-600 text-white text-sm font-medium hover:bg-purple-700 disabled:opacity-50">
{creatingCE ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- Job Detail Modal -->
{#if selectedJob || loadingJobDetail}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-2xl p-6 space-y-4">
<div class="flex items-center justify-between">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Job Details</h2>
<button onclick={() => { selectedJob = null; jobLogEvents = []; jobLogError = ''; }} aria-label="Close" class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200">
<svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
</button>
</div>
{#if loadingJobDetail}
<div class="flex justify-center py-8"><div class="animate-spin w-8 h-8 border-4 border-purple-600 border-t-transparent rounded-full"></div></div>
{:else if selectedJob}
<pre class="bg-gray-50 dark:bg-gray-800 rounded-lg p-4 text-xs font-mono overflow-auto max-h-72 text-gray-800 dark:text-gray-200">{JSON.stringify(selectedJob, null, 2)}</pre>

<!-- Container log streaming (CloudWatch /aws/batch/job) -->
<div class="border border-gray-200 dark:border-gray-700 rounded-lg">
<div class="flex items-center justify-between px-3 py-2 border-b border-gray-100 dark:border-gray-800">
<div class="flex items-center gap-2">
<Terminal class="w-4 h-4 text-purple-500" />
<span class="text-sm font-semibold text-gray-700 dark:text-gray-300">Container Logs</span>
{#if selectedJob.container?.logStreamName}
<span class="text-xs font-mono text-gray-400 truncate max-w-[200px]">{selectedJob.container.logStreamName}</span>
{/if}
</div>
<button
onclick={loadJobLogs}
disabled={loadingJobLogs || !selectedJob.container?.logStreamName}
class="flex items-center gap-1 px-2.5 py-1 text-xs rounded-md bg-purple-600 text-white hover:bg-purple-700 disabled:opacity-50"
>
{#if loadingJobLogs}<RefreshCw class="w-3 h-3 animate-spin" />{:else}<RefreshCw class="w-3 h-3" />{/if}
Fetch Logs
</button>
</div>
{#if jobLogError}
<p class="px-3 py-3 text-xs text-gray-500">{jobLogError}</p>
{:else if jobLogEvents.length > 0}
<div class="bg-gray-950 text-gray-100 rounded-b-lg p-3 text-xs font-mono overflow-auto max-h-72 space-y-0.5">
{#each jobLogEvents as ev}
<div class="flex gap-3">
<span class="text-gray-500 shrink-0">{ev.timestamp ? new Date(ev.timestamp).toLocaleTimeString() : ''}</span>
<span class="whitespace-pre-wrap break-all">{ev.message}</span>
</div>
{/each}
</div>
{:else}
<p class="px-3 py-3 text-xs text-gray-500">Click "Fetch Logs" to load container output from CloudWatch.</p>
{/if}
</div>
{/if}
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
