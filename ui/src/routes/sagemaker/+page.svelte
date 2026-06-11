<script lang="ts">
	import { onMount } from 'svelte';
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
		UpdateEndpointWeightsAndCapacitiesCommand,
		type SageMakerClient,
		type NotebookInstanceSummary,
		type TrainingJobSummary,
		type ModelSummary,
		type EndpointSummary,
		type PipelineSummary,
		type ProductionVariantSummary
	} from '@aws-sdk/client-sagemaker';
	import { toast } from 'svelte-sonner';
	import { Brain, RefreshCw, Search, Server, Activity, Box, BookOpen, Plus, X, GitBranch, ChevronDown, ChevronRight, Save } from 'lucide-svelte';

	let sm: SageMakerClient | undefined;
	function client(): SageMakerClient {
		return (sm ??= getSageMakerClient());
	}

	// Endpoint A/B traffic-split: variant weight editor.
	let expandedEndpoint = $state<string | null>(null);
	let variants = $state<ProductionVariantSummary[]>([]);
	let variantWeights = $state<Record<string, number>>({});
	let loadingVariants = $state(false);
	let savingWeights = $state(false);

	async function toggleEndpointVariants(name: string) {
		if (expandedEndpoint === name) {
			expandedEndpoint = null;
			return;
		}
		expandedEndpoint = name;
		variants = [];
		variantWeights = {};
		loadingVariants = true;
		try {
			const resp = await client().send(new DescribeEndpointCommand({ EndpointName: name }));
			variants = resp.ProductionVariants ?? [];
			const w: Record<string, number> = {};
			for (const v of variants) {
				if (v.VariantName) w[v.VariantName] = v.CurrentWeight ?? 1;
			}
			variantWeights = w;
		} catch (e) {
			toast.error('Failed to load endpoint variants: ' + String(e));
		} finally {
			loadingVariants = false;
		}
	}

	async function saveVariantWeights(name: string) {
		savingWeights = true;
		try {
			await client().send(
				new UpdateEndpointWeightsAndCapacitiesCommand({
					EndpointName: name,
					DesiredWeightsAndCapacities: variants.map((v) => ({
						VariantName: v.VariantName ?? '',
						DesiredWeight: variantWeights[v.VariantName ?? ''] ?? v.CurrentWeight ?? 1
					}))
				})
			);
			toast.success('Variant weights updated');
			await toggleEndpointVariants(name);
			expandedEndpoint = name;
		} catch (e) {
			toast.error('Failed to update weights: ' + String(e));
		} finally {
			savingWeights = false;
		}
	}

	const totalWeight = $derived(Object.values(variantWeights).reduce((a, b) => a + (Number(b) || 0), 0));

	let loading = $state(false);
	let activeTab = $state<'notebooks' | 'training' | 'models' | 'endpoints' | 'pipelines'>('notebooks');
	let searchQuery = $state('');
	let notebooks = $state<NotebookInstanceSummary[]>([]);
	let trainingJobs = $state<TrainingJobSummary[]>([]);
	let models = $state<ModelSummary[]>([]);
	let endpoints = $state<EndpointSummary[]>([]);
	let pipelines = $state<PipelineSummary[]>([]);

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
				client().send(new ListNotebookInstancesCommand({})),
				client().send(new ListTrainingJobsCommand({})),
				client().send(new ListModelsCommand({})),
				client().send(new ListEndpointsCommand({})),
				client().send(new ListPipelinesCommand({}))
			]);
			notebooks = nb.NotebookInstances ?? [];
			trainingJobs = tj.TrainingJobSummaries ?? [];
			models = mo.Models ?? [];
			endpoints = ep.Endpoints ?? [];
			pipelines = pl.PipelineSummaries ?? [];
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
			await client().send(
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
			await client().send(
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

	onMount(loadData);
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
		<button
			onclick={loadData}
			title="Refresh"
			class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
		>
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
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
										<p class="font-medium text-gray-900 dark:text-white">
											{nb.NotebookInstanceName}
										</p>
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
				{#if filteredTraining.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No training jobs found
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTraining as job}
							<div
								class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50"
							>
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{job.TrainingJobName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{job.TrainingJobArn}</p>
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
							</div>
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
									<p class="font-medium text-gray-900 dark:text-white">{model.ModelName}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{model.ModelArn}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'endpoints'}
				{#if filteredEndpoints.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No endpoints found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredEndpoints as ep}
							{@const epn = ep.EndpointName ?? ''}
							<div class="rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center justify-between p-3">
									<button onclick={() => toggleEndpointVariants(epn)} class="flex items-center gap-3 min-w-0 text-left">
										{#if expandedEndpoint === epn}<ChevronDown class="w-4 h-4 text-gray-400 flex-shrink-0" />{:else}<ChevronRight class="w-4 h-4 text-gray-400 flex-shrink-0" />{/if}
										<Server class="w-5 h-5 text-green-500 flex-shrink-0" />
										<div class="min-w-0">
											<p class="font-medium text-gray-900 dark:text-white truncate">{ep.EndpointName}</p>
											<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{ep.EndpointArn}</p>
										</div>
									</button>
									<span
										class="text-xs px-2 py-1 rounded-full flex-shrink-0 {ep.EndpointStatus === 'InService'
											? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
											: ep.EndpointStatus === 'Failed'
												? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
												: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}"
									>
										{ep.EndpointStatus}
									</span>
								</div>
								{#if expandedEndpoint === epn}
									<div class="px-3 pb-3 border-t border-slate-200 dark:border-slate-600 pt-3">
										<p class="text-sm font-semibold text-gray-900 dark:text-white mb-2">A/B Traffic Split — Variant Weights</p>
										{#if loadingVariants}
											<p class="text-sm text-gray-500 dark:text-gray-400">Loading variants…</p>
										{:else if variants.length === 0}
											<p class="text-sm text-gray-500 dark:text-gray-400">No production variants found.</p>
										{:else}
											<div class="space-y-2">
												{#each variants as v}
													{@const vn = v.VariantName ?? ''}
													<div class="flex items-center gap-3">
														<span class="text-sm font-medium text-gray-900 dark:text-white w-40 truncate" title={vn}>{vn}</span>
														<input type="number" min="0" step="0.1" bind:value={variantWeights[vn]} class="w-24 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
														<div class="flex-1 h-2 bg-gray-200 dark:bg-slate-600 rounded-full overflow-hidden">
															<div class="h-full bg-teal-500" style="width: {totalWeight > 0 ? ((Number(variantWeights[vn]) || 0) / totalWeight) * 100 : 0}%"></div>
														</div>
														<span class="text-xs font-mono text-gray-500 dark:text-gray-400 w-14 text-right">{totalWeight > 0 ? (((Number(variantWeights[vn]) || 0) / totalWeight) * 100).toFixed(1) : '0.0'}%</span>
														<span class="text-xs text-gray-400 w-24 truncate" title={v.CurrentInstanceCount != null ? v.CurrentInstanceCount + ' inst' : ''}>{v.CurrentInstanceCount != null ? v.CurrentInstanceCount + ' inst' : ''}</span>
													</div>
												{/each}
											</div>
											<button onclick={() => saveVariantWeights(epn)} disabled={savingWeights}
												class="mt-3 flex items-center gap-2 px-3 py-1.5 text-sm rounded-lg bg-teal-600 text-white hover:bg-teal-700 disabled:opacity-50">
												<Save class="w-4 h-4" /> {savingWeights ? 'Saving…' : 'Update Weights'}
											</button>
										{/if}
									</div>
								{/if}
							</div>
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
										<p class="font-medium text-gray-900 dark:text-white">
											{pipeline.PipelineName}
										</p>
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
