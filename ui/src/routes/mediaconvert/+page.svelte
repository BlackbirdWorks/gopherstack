<script lang="ts">
	import { onMount } from 'svelte';
	import { getMediaConvertClient } from '$lib/aws-client';
	import {
		ListJobsCommand,
		ListQueuesCommand,
		ListJobTemplatesCommand,
		CreateJobCommand,
		CreateJobTemplateCommand,
		UpdateJobTemplateCommand,
		type Job,
		type Queue,
		type JobTemplate
	} from '@aws-sdk/client-mediaconvert';
	import { toast } from 'svelte-sonner';
	import {
		Film,
		Search,
		RefreshCw,
		List,
		Layers,
		FileText,
		CheckCircle,
		Clock,
		XCircle,
		ChevronRight,
		Play,
		Pause,
		Plus,
		X,
		Edit
	} from 'lucide-svelte';

	const mediaConvert = getMediaConvertClient();

	let loading = $state(false);
	let activeTab = $state<'jobs' | 'queues' | 'templates'>('queues');
	let searchQuery = $state('');
	let jobs = $state<Job[]>([]);
	let queues = $state<Queue[]>([]);
	let templates = $state<JobTemplate[]>([]);
	let selectedJob = $state<Job | null>(null);
	let selectedQueue = $state<Queue | null>(null);
	let selectedTemplate = $state<JobTemplate | null>(null);

	// Job creation modal state
	let showCreateJob = $state(false);
	let createJobRole = $state('');
	let createJobQueue = $state('');
	let createJobTemplate = $state('');
	let createJobSubmitting = $state(false);

	// Template create/edit modal state
	let showTemplateModal = $state(false);
	let templateModalMode = $state<'create' | 'edit'>('create');
	let templateName = $state('');
	let templateDescription = $state('');
	let templateCategory = $state('');
	let templateQueue = $state('');
	let templatePriority = $state(0);
	let templateSubmitting = $state(false);

	const filteredJobs = $derived(
		jobs.filter(
			(j) =>
				(j.Id ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(j.Status ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(j.Queue ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredQueues = $derived(
		queues.filter(
			(q) =>
				(q.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(q.Status ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredTemplates = $derived(
		templates.filter(
			(t) =>
				(t.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(t.Description ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const submittedCount = $derived(jobs.filter((j) => j.Status === 'SUBMITTED').length);
	const progressingCount = $derived(jobs.filter((j) => j.Status === 'PROGRESSING').length);

	function statusColor(status: string | undefined): string {
		if (status === 'COMPLETE' || status === 'ACTIVE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		if (status === 'PROGRESSING' || status === 'SUBMITTED')
			return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
		if (status === 'ERROR' || status === 'CANCELED')
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
		return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	}

	async function loadJobs() {
		loading = true;
		try {
			const res = await mediaConvert.send(new ListJobsCommand({ MaxResults: 100 }));
			jobs = res.Jobs ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load jobs: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadQueues() {
		loading = true;
		try {
			const res = await mediaConvert.send(new ListQueuesCommand({}));
			queues = res.Queues ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load queues: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadTemplates() {
		loading = true;
		try {
			const res = await mediaConvert.send(new ListJobTemplatesCommand({ MaxResults: 100 }));
			templates = res.JobTemplates ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load templates: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		selectedJob = null;
		selectedQueue = null;
		selectedTemplate = null;
		if (tab === 'jobs' && jobs.length === 0) await loadJobs();
		else if (tab === 'queues' && queues.length === 0) await loadQueues();
		else if (tab === 'templates' && templates.length === 0) await loadTemplates();
	}

	async function refresh() {
		if (activeTab === 'jobs') { jobs = []; await loadJobs(); }
		else if (activeTab === 'queues') { queues = []; await loadQueues(); }
		else { templates = []; await loadTemplates(); }
	}

	function openCreateJob() {
		createJobRole = '';
		createJobQueue = '';
		createJobTemplate = '';
		showCreateJob = true;
	}

	async function submitCreateJob() {
		if (!createJobRole.trim()) {
			toast.error('Role ARN is required');
			return;
		}
		createJobSubmitting = true;
		try {
			await mediaConvert.send(new CreateJobCommand({
				Role: createJobRole.trim(),
				Queue: createJobQueue.trim() || undefined,
				JobTemplate: createJobTemplate.trim() || undefined,
				Settings: { Inputs: [] }
			}));
			toast.success('Job created');
			showCreateJob = false;
			jobs = [];
			await loadJobs();
		} catch (err: unknown) {
			toast.error(`Failed to create job: ${(err as Error).message}`);
		} finally {
			createJobSubmitting = false;
		}
	}

	function openCreateTemplate() {
		templateModalMode = 'create';
		templateName = '';
		templateDescription = '';
		templateCategory = '';
		templateQueue = '';
		templatePriority = 0;
		showTemplateModal = true;
	}

	function openEditTemplate(t: JobTemplate) {
		templateModalMode = 'edit';
		templateName = t.Name ?? '';
		templateDescription = t.Description ?? '';
		templateCategory = t.Category ?? '';
		templateQueue = t.Queue ?? '';
		templatePriority = t.Priority ?? 0;
		showTemplateModal = true;
	}

	async function submitTemplateModal() {
		if (templateModalMode === 'create' && !templateName.trim()) {
			toast.error('Template name is required');
			return;
		}
		templateSubmitting = true;
		try {
			if (templateModalMode === 'create') {
				await mediaConvert.send(new CreateJobTemplateCommand({
					Name: templateName.trim(),
					Description: templateDescription.trim() || undefined,
					Category: templateCategory.trim() || undefined,
					Queue: templateQueue.trim() || undefined,
					Priority: templatePriority,
					Settings: {}
				}));
				toast.success('Template created');
			} else {
				await mediaConvert.send(new UpdateJobTemplateCommand({
					Name: selectedTemplate?.Name ?? templateName,
					Description: templateDescription.trim() || undefined,
					Category: templateCategory.trim() || undefined,
					Queue: templateQueue.trim() || undefined,
					Priority: templatePriority,
					Settings: selectedTemplate?.Settings ?? {}
				}));
				toast.success('Template updated');
			}
			showTemplateModal = false;
			selectedTemplate = null;
			templates = [];
			await loadTemplates();
		} catch (err: unknown) {
			toast.error(`Failed to save template: ${(err as Error).message}`);
		} finally {
			templateSubmitting = false;
		}
	}

	onMount(() => {
		loadQueues();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-rose-100 dark:bg-rose-900/30 rounded-lg">
				<Film class="w-6 h-6 text-rose-600 dark:text-rose-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">AWS MediaConvert</h1>
				<p class="text-slate-600 dark:text-slate-300">File-based video transcoding service</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={() => refresh()}
				class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white"
				title="Refresh"
			>
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
		</div>
	</div>

	<!-- Stat cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-rose-100 dark:bg-rose-900/30 rounded-lg">
				<Film class="w-5 h-5 text-rose-600 dark:text-rose-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{jobs.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Total Jobs</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Layers class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{queues.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Queues</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-lg">
				<Clock class="w-5 h-5 text-amber-600 dark:text-amber-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{submittedCount}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Submitted</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<Play class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{progressingCount}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Progressing</p>
			</div>
		</div>
	</div>

	<!-- Tab navigation -->
	<div class="flex items-center gap-1 border-b border-slate-200 dark:border-slate-700">
		<button
			onclick={() => selectTab('jobs')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'jobs' ? 'border-rose-500 text-rose-600 dark:text-rose-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><Film class="w-4 h-4" />Jobs</span>
		</button>
		<button
			onclick={() => selectTab('queues')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'queues' ? 'border-rose-500 text-rose-600 dark:text-rose-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><Layers class="w-4 h-4" />Queues</span>
		</button>
		<button
			onclick={() => selectTab('templates')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'templates' ? 'border-rose-500 text-rose-600 dark:text-rose-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><FileText class="w-4 h-4" />Templates</span>
		</button>
	</div>

	<!-- Search + action bar -->
	<div class="flex items-center gap-3">
		<div class="relative flex-1">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input
				type="text"
				bind:value={searchQuery}
				placeholder="Search {activeTab}..."
				class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-rose-500"
			/>
		</div>
		{#if activeTab === 'jobs'}
			<button
				onclick={openCreateJob}
				class="flex items-center gap-1.5 px-3 py-2 bg-rose-600 hover:bg-rose-700 text-white text-sm font-medium rounded-lg transition-colors"
			>
				<Plus class="w-4 h-4" />Create Job
			</button>
		{:else if activeTab === 'templates'}
			<button
				onclick={openCreateTemplate}
				class="flex items-center gap-1.5 px-3 py-2 bg-rose-600 hover:bg-rose-700 text-white text-sm font-medium rounded-lg transition-colors"
			>
				<Plus class="w-4 h-4" />Create Template
			</button>
		{/if}
	</div>

	<!-- Content -->
	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- List -->
		<div class="lg:col-span-1 space-y-2">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-rose-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading {activeTab}...</p>
				</div>
			{:else if activeTab === 'jobs'}
				{#if filteredJobs.length === 0}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
						<Film class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">No jobs found</p>
						<button onclick={openCreateJob} class="mt-3 text-sm text-rose-600 dark:text-rose-400 hover:underline">Create a job</button>
					</div>
				{:else}
					{#each filteredJobs as job}
						<div
							role="button"
							tabindex="0"
							onclick={() => { selectedJob = job; selectedQueue = null; selectedTemplate = null; }}
							onkeypress={(e) => { if (e.key === 'Enter') { selectedJob = job; selectedQueue = null; selectedTemplate = null; } }}
							class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-rose-400 transition-colors cursor-pointer {selectedJob?.Id === job.Id ? 'border-rose-500 ring-1 ring-rose-500' : 'border-slate-200 dark:border-slate-700'}"
						>
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-mono text-xs font-medium text-slate-900 dark:text-white truncate">{job.Id}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5 truncate">{job.Queue?.split('/').pop() ?? 'Default'}</p>
								</div>
								<div class="flex items-center gap-1.5 ml-2 flex-shrink-0">
									<span class="px-2 py-0.5 text-xs rounded-full {statusColor(job.Status)}">{job.Status}</span>
									<ChevronRight class="w-4 h-4 text-slate-400" />
								</div>
							</div>
						</div>
					{/each}
				{/if}
			{:else if activeTab === 'queues'}
				{#if filteredQueues.length === 0}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
						<Layers class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">No queues found</p>
					</div>
				{:else}
					{#each filteredQueues as queue}
						<div
							role="button"
							tabindex="0"
							onclick={() => { selectedQueue = queue; selectedJob = null; selectedTemplate = null; }}
							onkeypress={(e) => { if (e.key === 'Enter') { selectedQueue = queue; selectedJob = null; selectedTemplate = null; } }}
							class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-rose-400 transition-colors cursor-pointer {selectedQueue?.Name === queue.Name ? 'border-rose-500 ring-1 ring-rose-500' : 'border-slate-200 dark:border-slate-700'}"
						>
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white truncate">{queue.Name}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{queue.Type ?? 'SYSTEM'}</p>
								</div>
								<div class="flex items-center gap-1.5 ml-2 flex-shrink-0">
									<span class="px-2 py-0.5 text-xs rounded-full {statusColor(queue.Status)}">{queue.Status}</span>
									<ChevronRight class="w-4 h-4 text-slate-400" />
								</div>
							</div>
						</div>
					{/each}
				{/if}
			{:else}
				{#if filteredTemplates.length === 0}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
						<FileText class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">No templates found</p>
						<button onclick={openCreateTemplate} class="mt-3 text-sm text-rose-600 dark:text-rose-400 hover:underline">Create a template</button>
					</div>
				{:else}
					{#each filteredTemplates as template}
						<div
							role="button"
							tabindex="0"
							onclick={() => { selectedTemplate = template; selectedJob = null; selectedQueue = null; }}
							onkeypress={(e) => { if (e.key === 'Enter') { selectedTemplate = template; selectedJob = null; selectedQueue = null; } }}
							class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-rose-400 transition-colors cursor-pointer {selectedTemplate?.Name === template.Name ? 'border-rose-500 ring-1 ring-rose-500' : 'border-slate-200 dark:border-slate-700'}"
						>
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white truncate">{template.Name}</p>
									{#if template.Description}
										<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5 truncate">{template.Description}</p>
									{/if}
									<p class="text-xs text-slate-400 dark:text-slate-500 mt-1">
										{template.LastUpdated ? new Date(template.LastUpdated).toLocaleDateString() : 'N/A'}
									</p>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-400 flex-shrink-0 ml-2" />
							</div>
						</div>
					{/each}
				{/if}
			{/if}
		</div>

		<!-- Detail panel -->
		<div class="lg:col-span-2">
			{#if selectedJob}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
					<div class="flex items-start justify-between">
						<div>
							<h2 class="text-lg font-bold text-slate-900 dark:text-white font-mono">{selectedJob.Id}</h2>
							<span class="mt-1 inline-block px-2 py-0.5 text-xs rounded-full {statusColor(selectedJob.Status)}">
								{selectedJob.Status}
							</span>
						</div>
						<div class="p-2 bg-rose-100 dark:bg-rose-900/30 rounded-lg">
							<Film class="w-5 h-5 text-rose-600 dark:text-rose-400" />
						</div>
					</div>

					<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
						{#each [
							['Queue', selectedJob.Queue?.split('/').pop() ?? 'Default'],
							['Role ARN', selectedJob.Role?.split('/').pop() ?? 'N/A'],
							['Created', selectedJob.CreatedAt ? new Date(selectedJob.CreatedAt).toLocaleString() : 'N/A'],
							['Started', selectedJob.Timing?.StartTime ? new Date(selectedJob.Timing.StartTime).toLocaleString() : 'N/A'],
							['Finished', selectedJob.Timing?.FinishTime ? new Date(selectedJob.Timing.FinishTime).toLocaleString() : 'N/A'],
							['Billing Tags', selectedJob.BillingTagsSource ?? 'N/A'],
							['Simulated Reserved', String(selectedJob.SimulateReservedQueue ?? false)],
							['Status Update Interval', selectedJob.StatusUpdateInterval ?? 'N/A'],
							['Current Phase', selectedJob.CurrentPhase ?? 'N/A']
						] as [label, value]}
							<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
								<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
								<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate" title={value}>{value}</p>
							</div>
						{/each}
					</div>

					{#if selectedJob.ErrorMessage}
						<div class="bg-red-50 dark:bg-red-900/20 rounded-lg p-3">
							<p class="text-xs font-medium text-red-700 dark:text-red-300 mb-1">Error</p>
							<p class="text-sm text-red-600 dark:text-red-400">{selectedJob.ErrorMessage}</p>
						</div>
					{/if}

					{#if (selectedJob.OutputGroupDetails ?? []).length > 0}
						<div>
							<h3 class="font-semibold text-slate-900 dark:text-white mb-2">Output Groups</h3>
							<div class="space-y-2">
								{#each selectedJob.OutputGroupDetails ?? [] as group, i}
									<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg px-3 py-2">
										<p class="text-sm font-medium text-slate-900 dark:text-white">Group {i + 1}</p>
										<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
											{(group.OutputDetails ?? []).length} output(s)
										</p>
									</div>
								{/each}
							</div>
						</div>
					{/if}
				</div>
			{:else if selectedQueue}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
					<div class="flex items-start justify-between">
						<div>
							<h2 class="text-xl font-bold text-slate-900 dark:text-white">{selectedQueue.Name}</h2>
							<span class="mt-1 inline-block px-2 py-0.5 text-xs rounded-full {statusColor(selectedQueue.Status)}">
								{selectedQueue.Status}
							</span>
						</div>
						<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
							<Layers class="w-5 h-5 text-blue-600 dark:text-blue-400" />
						</div>
					</div>
					<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
						{#each [
							['Queue ARN', selectedQueue.Arn ?? 'N/A'],
							['Type', selectedQueue.Type ?? 'N/A'],
							['Pricing Plan', selectedQueue.PricingPlan ?? 'N/A'],
							['Jobs Running', String(selectedQueue.ProgressingJobsCount ?? 0)],
							['Jobs Submitted', String(selectedQueue.SubmittedJobsCount ?? 0)],
							['Created', selectedQueue.CreatedAt ? new Date(selectedQueue.CreatedAt).toLocaleDateString() : 'N/A']
						] as [label, value]}
							<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
								<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
								<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate" title={value}>{value}</p>
							</div>
						{/each}
					</div>
					{#if selectedQueue.Description}
						<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-3">
							<p class="text-xs text-slate-500 dark:text-slate-400 mb-1">Description</p>
							<p class="text-sm text-slate-700 dark:text-slate-300">{selectedQueue.Description}</p>
						</div>
					{/if}
				</div>
			{:else if selectedTemplate}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
					<div class="flex items-start justify-between">
						<div>
							<h2 class="text-xl font-bold text-slate-900 dark:text-white">{selectedTemplate.Name}</h2>
							{#if selectedTemplate.Description}
								<p class="text-sm text-slate-500 dark:text-slate-400 mt-1">{selectedTemplate.Description}</p>
							{/if}
						</div>
						<div class="flex items-center gap-2">
							<button
								onclick={() => openEditTemplate(selectedTemplate!)}
								class="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-slate-100 dark:bg-slate-700 hover:bg-slate-200 dark:hover:bg-slate-600 text-slate-700 dark:text-slate-200 rounded-lg transition-colors"
							>
								<Edit class="w-3.5 h-3.5" />Edit
							</button>
							<div class="p-2 bg-rose-100 dark:bg-rose-900/30 rounded-lg">
								<FileText class="w-5 h-5 text-rose-600 dark:text-rose-400" />
							</div>
						</div>
					</div>
					<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
						{#each [
							['Category', selectedTemplate.Category ?? 'N/A'],
							['Queue', selectedTemplate.Queue?.split('/').pop() ?? 'N/A'],
							['Priority', String(selectedTemplate.Priority ?? 0)],
							['ARN', selectedTemplate.Arn ?? 'N/A'],
							['Last Updated', selectedTemplate.LastUpdated ? new Date(selectedTemplate.LastUpdated).toLocaleString() : 'N/A']
						] as [label, value]}
							<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
								<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
								<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate" title={value}>{value}</p>
							</div>
						{/each}
					</div>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Film class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a {activeTab === 'templates' ? 'template' : activeTab.slice(0, -1)} to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Job Modal -->
{#if showCreateJob}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-xl w-full max-w-md mx-4 p-6 space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-bold text-slate-900 dark:text-white">Create Job</h2>
				<button onclick={() => showCreateJob = false} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">
					<X class="w-5 h-5" />
				</button>
			</div>

			<div class="space-y-3">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
						Role ARN <span class="text-red-500">*</span>
					</label>
					<input
						type="text"
						bind:value={createJobRole}
						placeholder="arn:aws:iam::123456789012:role/MediaConvertRole"
						class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-rose-500"
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Queue (optional)</label>
					<input
						type="text"
						bind:value={createJobQueue}
						placeholder="Default or queue ARN"
						class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-rose-500"
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Job Template (optional)</label>
					<input
						type="text"
						bind:value={createJobTemplate}
						placeholder="Template name"
						class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-rose-500"
					/>
				</div>
			</div>

			<div class="flex items-center justify-end gap-3 pt-2">
				<button
					onclick={() => showCreateJob = false}
					class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white transition-colors"
				>
					Cancel
				</button>
				<button
					onclick={submitCreateJob}
					disabled={createJobSubmitting}
					class="px-4 py-2 bg-rose-600 hover:bg-rose-700 disabled:opacity-50 text-white text-sm font-medium rounded-lg transition-colors"
				>
					{createJobSubmitting ? 'Creating...' : 'Create Job'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create/Edit Template Modal -->
{#if showTemplateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-xl w-full max-w-md mx-4 p-6 space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-bold text-slate-900 dark:text-white">
					{templateModalMode === 'create' ? 'Create Template' : 'Edit Template'}
				</h2>
				<button onclick={() => showTemplateModal = false} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">
					<X class="w-5 h-5" />
				</button>
			</div>

			<div class="space-y-3">
				{#if templateModalMode === 'create'}
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
							Name <span class="text-red-500">*</span>
						</label>
						<input
							type="text"
							bind:value={templateName}
							placeholder="my-template"
							class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-rose-500"
						/>
					</div>
				{/if}
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Description</label>
					<input
						type="text"
						bind:value={templateDescription}
						placeholder="Optional description"
						class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-rose-500"
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Category</label>
					<input
						type="text"
						bind:value={templateCategory}
						placeholder="Optional category"
						class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-rose-500"
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Queue</label>
					<input
						type="text"
						bind:value={templateQueue}
						placeholder="Queue ARN (optional)"
						class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-rose-500"
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Priority</label>
					<input
						type="number"
						bind:value={templatePriority}
						min="-50"
						max="50"
						class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-rose-500"
					/>
				</div>
			</div>

			<div class="flex items-center justify-end gap-3 pt-2">
				<button
					onclick={() => showTemplateModal = false}
					class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white transition-colors"
				>
					Cancel
				</button>
				<button
					onclick={submitTemplateModal}
					disabled={templateSubmitting}
					class="px-4 py-2 bg-rose-600 hover:bg-rose-700 disabled:opacity-50 text-white text-sm font-medium rounded-lg transition-colors"
				>
					{templateSubmitting ? 'Saving...' : templateModalMode === 'create' ? 'Create Template' : 'Save Changes'}
				</button>
			</div>
		</div>
	</div>
{/if}
