<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { multiRegionList } from '$lib/multi-region';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
	import { getSageMakerClient } from '$lib/aws-client';
	import {
		ListNotebookInstancesCommand,
		ListTrainingJobsCommand,
		ListModelsCommand,
		ListEndpointsCommand,
		ListPipelinesCommand,
		CreateEndpointCommand,
		CreateTrainingJobCommand,
		DescribeEndpointCommand,
		DescribeTrainingJobCommand,
		type NotebookInstanceSummary,
		type TrainingJobSummary,
		type ModelSummary,
		type EndpointSummary,
		type PipelineSummary,
		type DescribeEndpointOutput,
		type DescribeTrainingJobResponse
	} from '@aws-sdk/client-sagemaker';
	import { toast } from 'svelte-sonner';
	import { Brain, RefreshCw, Search, Server, Activity, Box, BookOpen, Plus, X, GitBranch, ChevronRight } from 'lucide-svelte';

	const sm = regionalClient(getSageMakerClient);

	// Every row carries the region its List call was made against. Detail
	// calls must build a client for THAT region -- in All mode the same
	// resource name can legitimately exist in two different regions.
	type Regioned<T> = T & { region: string };

	let loading = $state(false);
	let activeTab = $state<'notebooks' | 'training' | 'models' | 'endpoints' | 'pipelines'>('notebooks');
	let searchQuery = $state('');
	let notebooks = $state<Regioned<NotebookInstanceSummary>[]>([]);
	let trainingJobs = $state<Regioned<TrainingJobSummary>[]>([]);
	let models = $state<Regioned<ModelSummary>[]>([]);
	let endpoints = $state<Regioned<EndpointSummary>[]>([]);
	let pipelines = $state<Regioned<PipelineSummary>[]>([]);

	// Detail panels
	let selectedEndpoint = $state<(DescribeEndpointOutput & { region: string }) | null>(null);
	let loadingEndpointDetail = $state(false);
	let selectedTrainingJob = $state<(DescribeTrainingJobResponse & { region: string }) | null>(null);
	let loadingTrainingDetail = $state(false);

	// Create Endpoint dialog
	let showCreateEndpoint = $state(false);
	let newEndpointName = $state('');
	let newEndpointConfigName = $state('');
	let creatingEndpoint = $state(false);

	// Create Training Job dialog
	let showCreateTraining = $state(false);
	let newTrainingJobName = $state('');
	let newTrainingImage = $state('');
	let newTrainingRoleArn = $state('');
	let creatingTraining = $state(false);

	const filteredNotebooks = $derived(
		notebooks.filter((n) =>
			(n.NotebookInstanceName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredTraining = $derived(
		trainingJobs.filter((j) =>
			(j.TrainingJobName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredModels = $derived(
		models.filter((m) => (m.ModelName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredEndpoints = $derived(
		endpoints.filter((e) =>
			(e.EndpointName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredPipelines = $derived(
		pipelines.filter((p) =>
			(p.PipelineName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const inServiceEndpoints = $derived(
		endpoints.filter((e) => e.EndpointStatus === 'InService').length
	);
	const completedJobs = $derived(
		trainingJobs.filter((j) => j.TrainingJobStatus === 'Completed').length
	);

	async function loadData() {
		loading = true;
		try {
			const [nb, tj, mo, ep, pl] = await Promise.all([
				multiRegionList((region) => getSageMakerClient(region).send(new ListNotebookInstancesCommand({})), (r) => r.NotebookInstances ?? []),
				multiRegionList((region) => getSageMakerClient(region).send(new ListTrainingJobsCommand({})), (r) => r.TrainingJobSummaries ?? []),
				multiRegionList((region) => getSageMakerClient(region).send(new ListModelsCommand({})), (r) => r.Models ?? []),
				multiRegionList((region) => getSageMakerClient(region).send(new ListEndpointsCommand({})), (r) => r.Endpoints ?? []),
				multiRegionList((region) => getSageMakerClient(region).send(new ListPipelinesCommand({})), (r) => r.PipelineSummaries ?? [])
			]);
			notebooks = nb.items.map(({ region, item }) => ({ ...item, region }));
			trainingJobs = tj.items.map(({ region, item }) => ({ ...item, region }));
			models = mo.items.map(({ region, item }) => ({ ...item, region }));
			endpoints = ep.items.map(({ region, item }) => ({ ...item, region }));
			pipelines = pl.items.map(({ region, item }) => ({ ...item, region }));
			const errCount = nb.errors.length + tj.errors.length + mo.errors.length + ep.errors.length + pl.errors.length;
			if (errCount > 0) toast.error(`Failed to load some SageMaker data from ${errCount} region call(s)`);
		} catch (e) {
			toast.error('Failed to load SageMaker data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function createEndpoint() {
		if (!newEndpointName.trim() || !newEndpointConfigName.trim()) {
			toast.error('Endpoint name and config name are required');
			return;
		}
		creatingEndpoint = true;
		try {
			await sm().send(
				new CreateEndpointCommand({
					EndpointName: newEndpointName.trim(),
					EndpointConfigName: newEndpointConfigName.trim()
				})
			);
			toast.success(`Endpoint "${newEndpointName}" created`);
			showCreateEndpoint = false;
			newEndpointName = '';
			newEndpointConfigName = '';
			await loadData();
		} catch (e) {
			toast.error('Failed to create endpoint: ' + String(e));
		} finally {
			creatingEndpoint = false;
		}
	}

	async function createTrainingJob() {
		if (!newTrainingJobName.trim()) {
			toast.error('Training job name is required');
			return;
		}
		creatingTraining = true;
		try {
			await sm().send(
				new CreateTrainingJobCommand({
					TrainingJobName: newTrainingJobName.trim(),
					RoleArn: newTrainingRoleArn.trim() || undefined,
					AlgorithmSpecification: {
						TrainingImage: newTrainingImage.trim() || undefined,
						TrainingInputMode: 'File'
					},
					OutputDataConfig: { S3OutputPath: 's3://placeholder/output' },
					ResourceConfig: {
						InstanceType: 'ml.m5.large',
						InstanceCount: 1,
						VolumeSizeInGB: 30
					},
					StoppingCondition: { MaxRuntimeInSeconds: 3600 }
				})
			);
			toast.success(`Training job "${newTrainingJobName}" created`);
			showCreateTraining = false;
			newTrainingJobName = '';
			newTrainingImage = '';
			newTrainingRoleArn = '';
			await loadData();
		} catch (e) {
			toast.error('Failed to create training job: ' + String(e));
		} finally {
			creatingTraining = false;
		}
	}

	async function selectEndpoint(ep: Regioned<EndpointSummary>) {
		loadingEndpointDetail = true;
		selectedEndpoint = null;
		try {
			const resp = await getSageMakerClient(ep.region).send(new DescribeEndpointCommand({ EndpointName: ep.EndpointName }));
			selectedEndpoint = { ...resp, region: ep.region };
		} catch (e) {
			toast.error('Failed to load endpoint detail: ' + String(e));
		} finally {
			loadingEndpointDetail = false;
		}
	}

	async function selectTrainingJob(job: Regioned<TrainingJobSummary>) {
		loadingTrainingDetail = true;
		selectedTrainingJob = null;
		try {
			const resp = await getSageMakerClient(job.region).send(new DescribeTrainingJobCommand({ TrainingJobName: job.TrainingJobName }));
			selectedTrainingJob = { ...resp, region: job.region };
		} catch (e) {
			toast.error('Failed to load training job detail: ' + String(e));
		} finally {
			loadingTrainingDetail = false;
		}
	}

	function formatDate(d: Date | undefined): string {
		if (!d) return '-';
		return d.toLocaleString();
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Brain class="w-7 h-7 text-teal-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon SageMaker</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">
					Build, train, and deploy machine learning models
				</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<WriteRegionHint />
			<button
				onclick={loadData}
				title="Refresh"
				class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
			>
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg">
				<BookOpen class="w-5 h-5 text-teal-600 dark:text-teal-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{notebooks.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Notebooks</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Activity class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{trainingJobs.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Training Jobs</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Box class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{models.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Models</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<Server class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{inServiceEndpoints}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Active Endpoints</p>
			</div>
		</div>
	</div>

	<!-- Create Endpoint Dialog -->
	{#if showCreateEndpoint}
		<div
			class="fixed inset-0 bg-black/50 flex items-center justify-center z-50"
			role="dialog"
			aria-modal="true"
			aria-label="Create Endpoint"
		>
			<div
				class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md mx-4 space-y-4"
			>
				<div class="flex items-center justify-between">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Endpoint</h2>
					<button
						onclick={() => (showCreateEndpoint = false)}
						class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
				<div class="space-y-3">
					<div>
						<label
							class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							for="endpoint-name">Endpoint Name</label
						>
						<input
							id="endpoint-name"
							bind:value={newEndpointName}
							placeholder="my-endpoint"
							class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"
						/>
					</div>
					<div>
						<label
							class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							for="endpoint-config">Endpoint Config Name</label
						>
						<input
							id="endpoint-config"
							bind:value={newEndpointConfigName}
							placeholder="my-endpoint-config"
							class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"
						/>
					</div>
				</div>
				<div class="flex gap-3 justify-end">
					<button
						onclick={() => (showCreateEndpoint = false)}
						class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
					>
						Cancel
					</button>
					<button
						onclick={createEndpoint}
						disabled={creatingEndpoint}
						class="px-4 py-2 text-sm rounded-lg bg-teal-600 text-white hover:bg-teal-700 disabled:opacity-50"
					>
						{creatingEndpoint ? 'Creating...' : 'Create'}
					</button>
				</div>
			</div>
		</div>
	{/if}

	<!-- Create Training Job Dialog -->
	{#if showCreateTraining}
		<div
			class="fixed inset-0 bg-black/50 flex items-center justify-center z-50"
			role="dialog"
			aria-modal="true"
			aria-label="Create Training Job"
		>
			<div
				class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md mx-4 space-y-4"
			>
				<div class="flex items-center justify-between">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Training Job</h2>
					<button
						onclick={() => (showCreateTraining = false)}
						class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
					>
						<X class="w-5 h-5" />
					</button>
				</div>
				<div class="space-y-3">
					<div>
						<label
							class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							for="training-job-name">Job Name</label
						>
						<input
							id="training-job-name"
							bind:value={newTrainingJobName}
							placeholder="my-training-job"
							class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"
						/>
					</div>
					<div>
						<label
							class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							for="training-image">Training Image URI (optional)</label
						>
						<input
							id="training-image"
							bind:value={newTrainingImage}
							placeholder="123456789.dkr.ecr.us-east-1.amazonaws.com/my-image:latest"
							class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"
						/>
					</div>
					<div>
						<label
							class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							for="training-role">IAM Role ARN (optional)</label
						>
						<input
							id="training-role"
							bind:value={newTrainingRoleArn}
							placeholder="arn:aws:iam::123456789:role/SageMakerRole"
							class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"
						/>
					</div>
				</div>
				<div class="flex gap-3 justify-end">
					<button
						onclick={() => (showCreateTraining = false)}
						class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
					>
						Cancel
					</button>
					<button
						onclick={createTrainingJob}
						disabled={creatingTraining}
						class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
					>
						{creatingTraining ? 'Creating...' : 'Create'}
					</button>
				</div>
			</div>
		</div>
	{/if}

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<div class="flex gap-2 flex-wrap">
				{#each ['notebooks', 'training', 'models', 'endpoints', 'pipelines'] as tab}
					<button
						onclick={() => {
							activeTab = tab as typeof activeTab;
							searchQuery = '';
						}}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab
							? 'bg-teal-600 text-white'
							: 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}"
					>
						{tab.charAt(0).toUpperCase() + tab.slice(1)}
					</button>
				{/each}
			</div>
			<div class="flex gap-2 items-center">
				{#if activeTab === 'endpoints'}
					<button
						onclick={() => (showCreateEndpoint = true)}
						class="flex items-center gap-1 px-3 py-2 text-sm rounded-lg bg-teal-600 text-white hover:bg-teal-700"
					>
						<Plus class="w-4 h-4" /> Create Endpoint
					</button>
				{:else if activeTab === 'training'}
					<button
						onclick={() => (showCreateTraining = true)}
						class="flex items-center gap-1 px-3 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700"
					>
						<Plus class="w-4 h-4" /> Launch Training Job
					</button>
				{/if}
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input
						bind:value={searchQuery}
						placeholder="Search..."
						class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64"
					/>
				</div>
			</div>
		</div>

		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'notebooks'}
				{#if filteredNotebooks.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No notebook instances found
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredNotebooks as nb}
							<div
								class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50"
							>
								<div class="flex items-center gap-3">
									<BookOpen class="w-5 h-5 text-teal-500" />
									<div>
										<div class="flex items-center gap-2">
											<p class="font-medium text-gray-900 dark:text-white">
												{nb.NotebookInstanceName}
											</p>
											<RegionChip region={nb.region} />
										</div>
										<p class="text-xs text-gray-500 dark:text-gray-400">
											{nb.InstanceType ?? 'Unknown'}
										</p>
									</div>
								</div>
								<span
									class="text-xs px-2 py-1 rounded-full {nb.NotebookInstanceStatus === 'InService'
										? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
										: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}"
								>
									{nb.NotebookInstanceStatus}
								</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'training'}
				{#if selectedTrainingJob}
					<div class="space-y-4">
						<div class="flex items-center gap-2 text-sm">
							<button onclick={() => (selectedTrainingJob = null)} class="text-teal-600 hover:underline">Training Jobs</button>
							<ChevronRight class="w-4 h-4 text-gray-400" />
							<span class="font-medium text-gray-700 dark:text-gray-300">{selectedTrainingJob.TrainingJobName}</span>
							<RegionChip region={selectedTrainingJob.region} />
						</div>
						<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
							{#each [
								{ label: 'Status', value: selectedTrainingJob.TrainingJobStatus ?? '-' },
								{ label: 'Secondary Status', value: selectedTrainingJob.SecondaryStatus ?? '-' },
								{ label: 'Algorithm', value: selectedTrainingJob.AlgorithmSpecification?.AlgorithmName ?? selectedTrainingJob.AlgorithmSpecification?.TrainingImage ?? '-' },
								{ label: 'Instance Type', value: selectedTrainingJob.ResourceConfig?.InstanceType ?? '-' },
								{ label: 'Instance Count', value: String(selectedTrainingJob.ResourceConfig?.InstanceCount ?? '-') },
								{ label: 'Created', value: formatDate(selectedTrainingJob.CreationTime) },
								{ label: 'Started', value: formatDate(selectedTrainingJob.TrainingStartTime) },
								{ label: 'Ended', value: formatDate(selectedTrainingJob.TrainingEndTime) },
								{ label: 'Billable Seconds', value: String(selectedTrainingJob.BillableTimeInSeconds ?? '-') }
							] as card}
								<div class="bg-gray-50 dark:bg-slate-700/50 rounded-lg p-3">
									<p class="text-xs text-gray-500 dark:text-gray-400 font-medium">{card.label}</p>
									<p class="text-sm font-mono text-gray-900 dark:text-white mt-1 truncate">{card.value}</p>
								</div>
							{/each}
						</div>
						{#if selectedTrainingJob.FailureReason}
							<div class="p-3 rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800">
								<p class="text-xs font-medium text-red-700 dark:text-red-400 mb-1">Failure Reason</p>
								<p class="text-sm text-red-600 dark:text-red-300">{selectedTrainingJob.FailureReason}</p>
							</div>
						{/if}
						{#if selectedTrainingJob.HyperParameters && Object.keys(selectedTrainingJob.HyperParameters).length > 0}
							<div>
								<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Hyperparameters</h3>
								<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
									<table class="w-full text-sm">
										<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
											<tr>
												<th class="px-4 py-2 text-left">Key</th>
												<th class="px-4 py-2 text-left">Value</th>
											</tr>
										</thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each Object.entries(selectedTrainingJob.HyperParameters) as [k, v]}
												<tr>
													<td class="px-4 py-2 font-mono text-xs text-gray-700 dark:text-gray-300">{k}</td>
													<td class="px-4 py-2 font-mono text-xs text-gray-600 dark:text-gray-400">{v}</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							</div>
						{/if}
					</div>
				{:else if loadingTrainingDetail}
					<div class="flex justify-center py-8"><div class="animate-spin w-6 h-6 border-4 border-teal-600 border-t-transparent rounded-full"></div></div>
				{:else if filteredTraining.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No training jobs found
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTraining as job}
							<button
								onclick={() => selectTrainingJob(job)}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left"
							>
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-blue-500" />
									<div>
										<div class="flex items-center gap-2">
											<p class="font-medium text-gray-900 dark:text-white">{job.TrainingJobName}</p>
											<RegionChip region={job.region} />
										</div>
										<p class="text-xs text-gray-500 dark:text-gray-400 font-mono">{job.TrainingJobArn}</p>
									</div>
								</div>
								<span
									class="text-xs px-2 py-1 rounded-full {job.TrainingJobStatus === 'Completed'
										? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
										: job.TrainingJobStatus === 'Failed'
											? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
											: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400'}"
								>
									{job.TrainingJobStatus}
								</span>
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'models'}
				{#if filteredModels.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No models found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredModels as model}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Box class="w-5 h-5 text-purple-500" />
								<div>
									<div class="flex items-center gap-2">
										<p class="font-medium text-gray-900 dark:text-white">{model.ModelName}</p>
										<RegionChip region={model.region} />
									</div>
									<p class="text-xs text-gray-500 dark:text-gray-400">{model.ModelArn}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'endpoints'}
				{#if selectedEndpoint}
					<div class="space-y-4">
						<div class="flex items-center gap-2 text-sm">
							<button onclick={() => (selectedEndpoint = null)} class="text-teal-600 hover:underline">Endpoints</button>
							<ChevronRight class="w-4 h-4 text-gray-400" />
							<span class="font-medium text-gray-700 dark:text-gray-300">{selectedEndpoint.EndpointName}</span>
							<RegionChip region={selectedEndpoint.region} />
						</div>
						<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
							{#each [
								{ label: 'Status', value: selectedEndpoint.EndpointStatus ?? '-' },
								{ label: 'Config Name', value: selectedEndpoint.EndpointConfigName ?? '-' },
								{ label: 'Created', value: formatDate(selectedEndpoint.CreationTime) },
								{ label: 'Last Modified', value: formatDate(selectedEndpoint.LastModifiedTime) }
							] as card}
								<div class="bg-gray-50 dark:bg-slate-700/50 rounded-lg p-3">
									<p class="text-xs text-gray-500 dark:text-gray-400 font-medium">{card.label}</p>
									<p class="text-sm font-mono text-gray-900 dark:text-white mt-1 truncate">{card.value}</p>
								</div>
							{/each}
						</div>
						<div class="bg-gray-50 dark:bg-slate-700/30 rounded-lg p-3">
							<p class="text-xs text-gray-500 dark:text-gray-400 font-medium mb-1">ARN</p>
							<p class="text-xs font-mono text-gray-700 dark:text-gray-300 break-all">{selectedEndpoint.EndpointArn}</p>
						</div>
						{#if selectedEndpoint.FailureReason}
							<div class="p-3 rounded-lg bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800">
								<p class="text-xs font-medium text-red-700 dark:text-red-400 mb-1">Failure Reason</p>
								<p class="text-sm text-red-600 dark:text-red-300">{selectedEndpoint.FailureReason}</p>
							</div>
						{/if}
						{#if (selectedEndpoint.ProductionVariants ?? []).length > 0}
							<div>
								<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Production Variants</h3>
								<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
									<table class="w-full text-sm">
										<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
											<tr>
												<th class="px-4 py-2 text-left">Variant</th>
												<th class="px-4 py-2 text-right">Current Instances</th>
												<th class="px-4 py-2 text-right">Desired Instances</th>
												<th class="px-4 py-2 text-right">Current Weight</th>
											</tr>
										</thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each selectedEndpoint.ProductionVariants ?? [] as v}
												<tr>
													<td class="px-4 py-2 font-medium">{v.VariantName}</td>
													<td class="px-4 py-2 text-right text-gray-600 dark:text-gray-400">{v.CurrentInstanceCount ?? '-'}</td>
													<td class="px-4 py-2 text-right text-gray-600 dark:text-gray-400">{v.DesiredInstanceCount ?? '-'}</td>
													<td class="px-4 py-2 text-right text-gray-600 dark:text-gray-400">{v.CurrentWeight ?? '-'}</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							</div>
						{/if}
					</div>
				{:else if loadingEndpointDetail}
					<div class="flex justify-center py-8"><div class="animate-spin w-6 h-6 border-4 border-teal-600 border-t-transparent rounded-full"></div></div>
				{:else if filteredEndpoints.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No endpoints found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredEndpoints as ep}
							<button
								onclick={() => selectEndpoint(ep)}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left"
							>
								<div class="flex items-center gap-3">
									<Server class="w-5 h-5 text-green-500" />
									<div>
										<div class="flex items-center gap-2">
											<p class="font-medium text-gray-900 dark:text-white">{ep.EndpointName}</p>
											<RegionChip region={ep.region} />
										</div>
										<p class="text-xs text-gray-500 dark:text-gray-400 font-mono">{ep.EndpointArn}</p>
									</div>
								</div>
								<span
									class="text-xs px-2 py-1 rounded-full {ep.EndpointStatus === 'InService'
										? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
										: ep.EndpointStatus === 'Failed'
											? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
											: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}"
								>
									{ep.EndpointStatus}
								</span>
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'pipelines'}
				{#if filteredPipelines.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No pipelines found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredPipelines as pipeline}
							<div
								class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50"
							>
								<div class="flex items-center gap-3">
									<GitBranch class="w-5 h-5 text-indigo-500" />
									<div>
										<div class="flex items-center gap-2">
											<p class="font-medium text-gray-900 dark:text-white">
												{pipeline.PipelineName}
											</p>
											<RegionChip region={pipeline.region} />
										</div>
										<p class="text-xs text-gray-500 dark:text-gray-400">
											{pipeline.PipelineArn ?? ''}
										</p>
									</div>
								</div>
								<span
									class="text-xs px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400"
								>
									Active
								</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
