<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getEMRClient } from '$lib/aws-client';
	import {
		ListClustersCommand,
		DescribeClusterCommand,
		ListInstancesCommand,
		ListInstanceFleetsCommand,
		ListInstanceGroupsCommand,
		ListBootstrapActionsCommand,
		ListNotebookExecutionsCommand,
		ListStudiosCommand,
		ModifyClusterCommand,
		PutAutoScalingPolicyCommand,
		PutManagedScalingPolicyCommand,
		RemoveAutoScalingPolicyCommand,
		RemoveManagedScalingPolicyCommand,
		AddJobFlowStepsCommand,
		ListStepsCommand,
		TerminateJobFlowsCommand,
		type ClusterSummary,
		type Cluster,
		type StepSummary,
		type Instance,
		type InstanceFleet,
		type InstanceGroup,
		type StudioSummary,
		type NotebookExecutionSummary
	} from '@aws-sdk/client-emr';
	import { toast } from 'svelte-sonner';
	import { Database, Search, RefreshCw, Plus, XCircle, ChevronRight, Play, Server, Settings, BookOpen, Layers } from 'lucide-svelte';

	const emr = getEMRClient();

	let loading = $state(false);
	let clusters = $state<ClusterSummary[]>([]);
	let selectedCluster = $state<Cluster | null>(null);
	let activeTab = $state<'overview' | 'steps' | 'instances' | 'instance-groups' | 'modifications' | 'autoscaling' | 'notebooks' | 'studios' | 'bootstrap-actions' | 'instance-fleets'>('overview');
	let searchQuery = $state('');

	// Steps
	let steps = $state<StepSummary[]>([]);
	let loadingSteps = $state(false);

	// Instances
	let instances = $state<Instance[]>([]);
	let loadingInstances = $state(false);

	// Instance Fleets
	let instanceFleets = $state<InstanceFleet[]>([]);
	let loadingFleets = $state(false);

	// Instance Groups
	let instanceGroups = $state<InstanceGroup[]>([]);
	let loadingGroups = $state(false);

	// Bootstrap Actions
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let bootstrapActions = $state<any[]>([]);
	let loadingBootstrap = $state(false);

	// Notebooks
	let notebookExecutions = $state<NotebookExecutionSummary[]>([]);
	let loadingNotebooks = $state(false);

	// Studios
	let studios = $state<StudioSummary[]>([]);
	let loadingStudios = $state(false);

	// Modify Cluster
	let modifyStepConcurrency = $state(1);
	let modifying = $state(false);

	// Autoscaling
	let autoScalingMaxCapacity = $state(10);
	let autoScalingMinCapacity = $state(2);
	let managedMaxOnDemand = $state(5);

	// Add Step
	let showAddStep = $state(false);
	let addingStep = $state(false);
	let stepName = $state('');
	let stepJar = $state('s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar');
	let stepArgs = $state('');
	let stepActionOnFailure = $state<'CONTINUE' | 'TERMINATE_CLUSTER'>('CONTINUE');

	const filteredClusters = $derived(
		clusters.filter((c) => !searchQuery || (c.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const clusterStatusColor = (state: string | undefined) => {
		if (!state) return 'gray';
		if (state === 'RUNNING' || state === 'WAITING') return 'green';
		if (state === 'BOOTSTRAPPING' || state === 'STARTING') return 'blue';
		if (state === 'TERMINATING') return 'yellow';
		if (state === 'TERMINATED' || state === 'TERMINATED_WITH_ERRORS') return 'red';
		return 'gray';
	};

	const stepStatusColor = (state: string | undefined) => {
		if (!state) return 'gray';
		if (state === 'COMPLETED') return 'green';
		if (state === 'FAILED' || state === 'CANCELLED') return 'red';
		if (state === 'RUNNING') return 'blue';
		if (state === 'PENDING') return 'yellow';
		return 'gray';
	};

	async function loadClusters() {
		loading = true;
		try {
			const resp = await emr.send(new ListClustersCommand({
				ClusterStates: ['RUNNING', 'WAITING', 'BOOTSTRAPPING', 'STARTING']
			}));
			clusters = resp.Clusters ?? [];
		} catch (e) {
			toast.error('Failed to load clusters: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectCluster(clusterId: string) {
		activeTab = 'overview';
		try {
			const resp = await emr.send(new DescribeClusterCommand({ ClusterId: clusterId }));
			selectedCluster = resp.Cluster ?? null;
		} catch (e) {
			toast.error('Failed to load cluster details: ' + String(e));
		}
	}

	async function loadSteps() {
		if (steps.length > 0) return;
		loadingSteps = true;
		try {
			const resp = await emr.send(new ListStepsCommand({ ClusterId: selectedCluster?.Id ?? '' }));
			steps = resp.Steps ?? [];
		} catch (e) {
			toast.error('Failed to load steps: ' + String(e));
		} finally {
			loadingSteps = false;
		}
	}

	async function loadInstances() {
		if (instances.length > 0) return;
		loadingInstances = true;
		try {
			const resp = await emr.send(new ListInstancesCommand({ ClusterId: selectedCluster?.Id ?? '' }));
			instances = resp.Instances ?? [];
		} catch (e) {
			toast.error('Failed to load instances: ' + String(e));
		} finally {
			loadingInstances = false;
		}
	}

	async function loadGroups() {
		if (instanceGroups.length > 0) return;
		loadingGroups = true;
		try {
			const resp = await emr.send(new ListInstanceGroupsCommand({ ClusterId: selectedCluster?.Id ?? '' }));
			instanceGroups = resp.InstanceGroups ?? [];
		} catch (e) {
			toast.error('Failed to load instance groups: ' + String(e));
		} finally {
			loadingGroups = false;
		}
	}

	async function loadFleets() {
		if (instanceFleets.length > 0) return;
		loadingFleets = true;
		try {
			const resp = await emr.send(new ListInstanceFleetsCommand({ ClusterId: selectedCluster?.Id ?? '' }));
			instanceFleets = resp.InstanceFleets ?? [];
		} catch (e) {
			toast.error('Failed to load instance fleets: ' + String(e));
		} finally {
			loadingFleets = false;
		}
	}

	async function loadBootstrap() {
		if (bootstrapActions.length > 0) return;
		loadingBootstrap = true;
		try {
			const resp = await emr.send(new ListBootstrapActionsCommand({ ClusterId: selectedCluster?.Id ?? '' }));
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			bootstrapActions = (resp as any).BootstrapActions ?? [];
		} catch (e) {
			toast.error('Failed to load bootstrap actions: ' + String(e));
		} finally {
			loadingBootstrap = false;
		}
	}

	async function loadNotebooks() {
		if (notebookExecutions.length > 0) return;
		loadingNotebooks = true;
		try {
			const resp = await emr.send(new ListNotebookExecutionsCommand({}));
			notebookExecutions = resp.NotebookExecutions ?? [];
		} catch (e) {
			toast.error('Failed to load notebook executions: ' + String(e));
		} finally {
			loadingNotebooks = false;
		}
	}

	async function loadStudios() {
		if (studios.length > 0) return;
		loadingStudios = true;
		try {
			const resp = await emr.send(new ListStudiosCommand({}));
			studios = resp.Studios ?? [];
		} catch (e) {
			toast.error('Failed to load studios: ' + String(e));
		} finally {
			loadingStudios = false;
		}
	}

	async function handleTabChange(tab: 'overview' | 'steps' | 'instances' | 'instance-groups' | 'modifications' | 'autoscaling' | 'notebooks' | 'studios' | 'bootstrap-actions' | 'instance-fleets') {
		activeTab = tab;
		if (!selectedCluster) return;
		if (tab === 'steps') await loadSteps();
		if (tab === 'instances') await loadInstances();
		if (tab === 'instance-groups') await loadGroups();
		if (tab === 'instance-fleets') await loadFleets();
		if (tab === 'bootstrap-actions') await loadBootstrap();
		if (tab === 'notebooks') await loadNotebooks();
		if (tab === 'studios') await loadStudios();
	}

	async function modifyCluster() {
		if (!selectedCluster) return;
		modifying = true;
		try {
			await emr.send(new ModifyClusterCommand({
				ClusterId: selectedCluster.Id ?? '',
				StepConcurrencyLevel: modifyStepConcurrency
			}));
			toast.success('Cluster modified');
		} catch (e) {
			toast.error('Failed to modify cluster: ' + String(e));
		} finally {
			modifying = false;
		}
	}

	async function putAutoScaling() {
		if (!selectedCluster) return;
		try {
			await emr.send(new PutAutoScalingPolicyCommand({
				ClusterId: selectedCluster.Id ?? '',
				InstanceGroupId: 'ig-default',
				AutoScalingPolicy: {
					Constraints: { MinCapacity: autoScalingMinCapacity, MaxCapacity: autoScalingMaxCapacity },
					Rules: []
				}
			}));
			toast.success('Auto scaling policy applied');
		} catch (e) {
			toast.error('Failed to apply auto scaling policy: ' + String(e));
		}
	}

	async function removeAutoScaling() {
		if (!selectedCluster) return;
		try {
			await emr.send(new RemoveAutoScalingPolicyCommand({
				ClusterId: selectedCluster.Id ?? '',
				InstanceGroupId: 'ig-default'
			}));
			toast.success('Auto scaling policy removed');
		} catch (e) {
			toast.error('Failed to remove auto scaling policy: ' + String(e));
		}
	}

	async function putManagedScaling() {
		if (!selectedCluster) return;
		try {
			await emr.send(new PutManagedScalingPolicyCommand({
				ClusterId: selectedCluster.Id ?? '',
				ManagedScalingPolicy: {
					ComputeLimits: {
						UnitType: 'InstanceFleetUnits',
						MinimumCapacityUnits: autoScalingMinCapacity,
						MaximumCapacityUnits: autoScalingMaxCapacity,
						MaximumOnDemandCapacityUnits: managedMaxOnDemand
					}
				}
			}));
			toast.success('Managed scaling policy applied');
		} catch (e) {
			toast.error('Failed to apply managed scaling: ' + String(e));
		}
	}

	async function removeManagedScaling() {
		if (!selectedCluster) return;
		try {
			await emr.send(new RemoveManagedScalingPolicyCommand({ ClusterId: selectedCluster.Id ?? '' }));
			toast.success('Managed scaling policy removed');
		} catch (e) {
			toast.error('Failed to remove managed scaling: ' + String(e));
		}
	}

	async function addStep() {
		if (!selectedCluster || !stepName.trim() || !stepJar.trim()) return;
		addingStep = true;
		try {
			const args = stepArgs.trim() ? stepArgs.split('\n').filter((a) => a.trim()) : [];
			await emr.send(new AddJobFlowStepsCommand({
				JobFlowId: selectedCluster.Id ?? '',
				Steps: [{
					Name: stepName.trim(),
					ActionOnFailure: stepActionOnFailure,
					HadoopJarStep: {
						Jar: stepJar.trim(),
						Args: args
					}
				}]
			}));
			toast.success(`Step "${stepName}" added`);
			showAddStep = false;
			stepName = '';
			stepArgs = '';
			steps = [];
			await handleTabChange('steps');
		} catch (e) {
			toast.error('Failed to add step: ' + String(e));
		} finally {
			addingStep = false;
		}
	}

	async function terminateCluster(id: string, name: string) {
		if (!await confirmDestructive({ title: 'Terminate Cluster', message: `Terminate EMR cluster "${name}"? All running jobs will be aborted.`, confirmLabel: 'Terminate' })) return;
		try {
			await emr.send(new TerminateJobFlowsCommand({ JobFlowIds: [id] }));
			toast.success(`Cluster "${name}" termination initiated`);
			if (selectedCluster?.Id === id) selectedCluster = null;
			await loadClusters();
		} catch (e) {
			toast.error('Failed to terminate cluster: ' + String(e));
		}
	}

	function formatDate(d: Date | undefined): string {
		if (!d) return '-';
		return d.toLocaleString();
	}

	onMount(loadClusters);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Database class="w-7 h-7 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon EMR</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Managed Hadoop / Spark clusters</p>
			</div>
		</div>
		<button onclick={loadClusters} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	{#if selectedCluster}
		<!-- Cluster Detail -->
		<div class="flex items-center justify-between">
			<div class="flex items-center gap-2 text-sm">
				<button onclick={() => { selectedCluster = null; steps = []; instances = []; instanceGroups = []; instanceFleets = []; }} class="text-orange-600 hover:underline">Clusters</button>
				<ChevronRight class="w-4 h-4 text-gray-400" />
				<span class="text-gray-600 dark:text-gray-300 font-medium">{selectedCluster.Name}</span>
				<span class={`ml-2 px-2 py-0.5 rounded text-xs font-medium bg-${clusterStatusColor(selectedCluster.Status?.State)}-100 text-${clusterStatusColor(selectedCluster.Status?.State)}-700`}>
					{selectedCluster.Status?.State}
				</span>
			</div>
			<button onclick={() => terminateCluster(selectedCluster?.Id ?? '', selectedCluster?.Name ?? '')} class="text-sm text-red-500 hover:underline">Terminate</button>
		</div>

		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['overview', 'Overview'], ['steps', 'Steps'], ['instances', 'Instances'], ['instance-groups', 'Instance Groups'], ['modifications', 'Modifications'], ['autoscaling', 'Autoscaling'], ['notebooks', 'Notebooks'], ['studios', 'Studios'], ['bootstrap-actions', 'Bootstrap Actions'], ['instance-fleets', 'Instance Fleets']] as [tab, label]}
				<button
					onclick={() => handleTabChange(tab as 'overview' | 'steps' | 'instances' | 'instance-groups' | 'modifications' | 'autoscaling' | 'notebooks' | 'studios' | 'bootstrap-actions' | 'instance-fleets')}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-orange-500 text-orange-600 dark:text-orange-400' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
				>
					{label}
				</button>
			{/each}
		</div>

		{#if activeTab === 'overview'}
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#each [
					{ label: 'Cluster ID', value: selectedCluster.Id },
					{ label: 'Release Label', value: selectedCluster.ReleaseLabel },
					{ label: 'Status', value: selectedCluster.Status?.State },
					{ label: 'Log URI', value: selectedCluster.LogUri ?? '-' },
					{ label: 'Master DNS', value: selectedCluster.MasterPublicDnsName ?? 'Not available' },
					{ label: 'Auto-Terminate', value: selectedCluster.AutoTerminate ? 'Yes' : 'No' },
					{ label: 'Normalized Hours', value: String(selectedCluster.NormalizedInstanceHours ?? 0) },
					{ label: 'Created', value: formatDate(selectedCluster.Status?.Timeline?.CreationDateTime) }
				] as row}
					<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
						<div class="text-xs text-gray-500 font-medium">{row.label}</div>
						<div class="text-sm text-gray-900 dark:text-white mt-1 truncate font-mono">{row.value}</div>
					</div>
				{/each}
			</div>

			{#if selectedCluster.Applications && selectedCluster.Applications.length > 0}
				<div>
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Applications</h3>
					<div class="flex flex-wrap gap-2">
						{#each selectedCluster.Applications as app}
							<span class="px-3 py-1 rounded-full text-xs font-medium bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400">{app.Name} {app.Version ?? ''}</span>
						{/each}
					</div>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'steps'}
			<div class="flex justify-end mb-2">
				<button onclick={() => (showAddStep = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm font-medium">
					<Plus class="w-4 h-4" /> Add Step
				</button>
			</div>
			{#if loadingSteps}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else if steps.length === 0}
				<div class="text-center py-12 text-gray-500"><Play class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No steps found</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Step Name</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Action on Failure</th>
								<th class="px-4 py-3 text-left">Created</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each steps as step}
								<tr>
									<td class="px-4 py-3 font-medium">{step.Name}</td>
									<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium bg-${stepStatusColor(step.Status?.State)}-100 text-${stepStatusColor(step.Status?.State)}-700`}>{step.Status?.State}</span></td>
									<td class="px-4 py-3 text-xs text-gray-500">{step.ActionOnFailure}</td>
									<td class="px-4 py-3 text-xs text-gray-500">{formatDate(step.Status?.Timeline?.CreationDateTime)}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'instances'}
			{#if loadingInstances}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else if instances.length === 0}
				<div class="text-center py-12 text-gray-500"><Server class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No instances found</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Instance ID</th>
								<th class="px-4 py-3 text-left">State</th>
								<th class="px-4 py-3 text-left">Instance Type</th>
								<th class="px-4 py-3 text-left">Private IP</th>
								<th class="px-4 py-3 text-left">Public IP</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each instances as inst}
								<tr>
									<td class="px-4 py-3 font-mono text-xs">{inst.Ec2InstanceId ?? inst.Id}</td>
									<td class="px-4 py-3"><span class="px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-700">{inst.Status?.State}</span></td>
									<td class="px-4 py-3 text-xs text-gray-500">{inst.InstanceType ?? '-'}</td>
									<td class="px-4 py-3 font-mono text-xs">{inst.PrivateIpAddress ?? '-'}</td>
									<td class="px-4 py-3 font-mono text-xs">{inst.PublicIpAddress ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'instance-groups'}
			{#if loadingGroups}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else if instanceGroups.length === 0}
				<div class="text-center py-12 text-gray-500"><Server class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No instance groups found</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Name</th>
								<th class="px-4 py-3 text-left">Type</th>
								<th class="px-4 py-3 text-left">Instance Type</th>
								<th class="px-4 py-3 text-left">Market</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-right">Requested</th>
								<th class="px-4 py-3 text-right">Running</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each instanceGroups as ig}
								<tr>
									<td class="px-4 py-3 font-medium">{ig.Name ?? ig.Id ?? '-'}</td>
									<td class="px-4 py-3 text-xs text-gray-500">{ig.InstanceGroupType ?? '-'}</td>
									<td class="px-4 py-3 font-mono text-xs">{ig.InstanceType ?? '-'}</td>
									<td class="px-4 py-3 text-xs text-gray-500">{ig.Market ?? '-'}</td>
									<td class="px-4 py-3"><span class="px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300">{ig.Status?.State ?? '-'}</span></td>
									<td class="px-4 py-3 text-right text-gray-600 dark:text-gray-400">{ig.RequestedInstanceCount ?? 0}</td>
									<td class="px-4 py-3 text-right text-gray-600 dark:text-gray-400">{ig.RunningInstanceCount ?? 0}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

	{#if activeTab === 'modifications'}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6 space-y-4 max-w-md">
				<div class="flex items-center gap-2 mb-2">
					<Settings class="w-5 h-5 text-orange-500" />
					<h3 class="text-sm font-semibold text-gray-800 dark:text-gray-200">Modify Cluster</h3>
				</div>
				<div>
					<label for="step-concurrency" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Step Concurrency Level</label>
					<input id="step-concurrency" type="number" min="1" max="256" bind:value={modifyStepConcurrency} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					<p class="text-xs text-gray-400 mt-1">Number of steps that can run concurrently (1–256)</p>
				</div>
				<button onclick={modifyCluster} disabled={modifying} class="px-4 py-2 rounded-lg bg-orange-600 text-white text-sm font-medium hover:bg-orange-700 disabled:opacity-50">
					{modifying ? 'Applying...' : 'Apply Changes'}
				</button>
			</div>
		{/if}

		{#if activeTab === 'autoscaling'}
			<div class="grid grid-cols-1 md:grid-cols-2 gap-6">
				<!-- Instance Group Auto Scaling -->
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6 space-y-4">
					<h3 class="text-sm font-semibold text-gray-800 dark:text-gray-200">Instance Group Auto Scaling</h3>
					<div>
						<label for="as-min" class="block text-xs text-gray-500 mb-1">Min Capacity</label>
						<input id="as-min" type="number" min="1" bind:value={autoScalingMinCapacity} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					</div>
					<div>
						<label for="as-max" class="block text-xs text-gray-500 mb-1">Max Capacity</label>
						<input id="as-max" type="number" min="1" bind:value={autoScalingMaxCapacity} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					</div>
					<div class="flex gap-2">
						<button onclick={putAutoScaling} class="flex-1 px-4 py-2 rounded-lg bg-orange-600 text-white text-sm font-medium hover:bg-orange-700">Apply Policy</button>
						<button onclick={removeAutoScaling} class="px-4 py-2 rounded-lg border border-red-300 text-red-600 text-sm font-medium hover:bg-red-50 dark:hover:bg-red-900/20">Remove</button>
					</div>
				</div>
				<!-- Managed Scaling -->
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6 space-y-4">
					<h3 class="text-sm font-semibold text-gray-800 dark:text-gray-200">Managed Scaling Policy</h3>
					<div>
						<label for="ms-min" class="block text-xs text-gray-500 mb-1">Min Capacity Units</label>
						<input id="ms-min" type="number" min="1" bind:value={autoScalingMinCapacity} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					</div>
					<div>
						<label for="ms-max" class="block text-xs text-gray-500 mb-1">Max Capacity Units</label>
						<input id="ms-max" type="number" min="1" bind:value={autoScalingMaxCapacity} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					</div>
					<div>
						<label for="ms-od" class="block text-xs text-gray-500 mb-1">Max On-Demand Units</label>
						<input id="ms-od" type="number" min="0" bind:value={managedMaxOnDemand} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					</div>
					<div class="flex gap-2">
						<button onclick={putManagedScaling} class="flex-1 px-4 py-2 rounded-lg bg-orange-600 text-white text-sm font-medium hover:bg-orange-700">Apply</button>
						<button onclick={removeManagedScaling} class="px-4 py-2 rounded-lg border border-red-300 text-red-600 text-sm font-medium hover:bg-red-50 dark:hover:bg-red-900/20">Remove</button>
					</div>
				</div>
			</div>
		{/if}

		{#if activeTab === 'notebooks'}
			{#if loadingNotebooks}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else if notebookExecutions.length === 0}
				<div class="text-center py-12 text-gray-500"><BookOpen class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No notebook executions found</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Execution ID</th>
								<th class="px-4 py-3 text-left">Notebook ID</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Start Time</th>
								<th class="px-4 py-3 text-left">End Time</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each notebookExecutions as nb}
								<tr>
									<td class="px-4 py-3 font-mono text-xs">{nb.NotebookExecutionId ?? '-'}</td>
									<td class="px-4 py-3 font-mono text-xs">{nb.EditorId ?? '-'}</td>
									<td class="px-4 py-3"><span class="px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-700">{nb.Status ?? '-'}</span></td>
									<td class="px-4 py-3 text-xs text-gray-500">{formatDate(nb.StartTime)}</td>
									<td class="px-4 py-3 text-xs text-gray-500">{formatDate(nb.EndTime)}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'studios'}
			{#if loadingStudios}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else if studios.length === 0}
				<div class="text-center py-12 text-gray-500"><Layers class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No studios found</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Studio ID</th>
								<th class="px-4 py-3 text-left">Name</th>
								<th class="px-4 py-3 text-left">Auth Mode</th>
								<th class="px-4 py-3 text-left">VPC ID</th>
								<th class="px-4 py-3 text-left">URL</th>
								<th class="px-4 py-3 text-left">Created</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each studios as studio}
								<tr>
									<td class="px-4 py-3 font-mono text-xs">{studio.StudioId ?? '-'}</td>
									<td class="px-4 py-3 font-medium">{studio.Name ?? '-'}</td>
									<td class="px-4 py-3 text-xs text-gray-500">{studio.AuthMode ?? '-'}</td>
									<td class="px-4 py-3 font-mono text-xs">{studio.VpcId ?? '-'}</td>
									<td class="px-4 py-3 text-xs"><a href={studio.Url ?? '#'} class="text-orange-600 hover:underline truncate block max-w-xs" target="_blank" rel="noopener noreferrer">{studio.Url ?? '-'}</a></td>
									<td class="px-4 py-3 text-xs text-gray-500">{formatDate(studio.CreationTime)}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'bootstrap-actions'}
			{#if loadingBootstrap}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else if bootstrapActions.length === 0}
				<div class="text-center py-12 text-gray-500"><Server class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No bootstrap actions found</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Name</th>
								<th class="px-4 py-3 text-left">Script Path</th>
								<th class="px-4 py-3 text-left">Args</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each bootstrapActions as ba}
								<tr>
									<td class="px-4 py-3 font-medium">{ba.Name ?? '-'}</td>
									<td class="px-4 py-3 font-mono text-xs">{ba.ScriptBootstrapAction?.Path ?? '-'}</td>
									<td class="px-4 py-3 text-xs text-gray-500">{(ba.ScriptBootstrapAction?.Args ?? []).join(' ') || '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'instance-fleets'}
			{#if loadingFleets}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else if instanceFleets.length === 0}
				<div class="text-center py-12 text-gray-500"><Layers class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No instance fleets found</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Fleet ID</th>
								<th class="px-4 py-3 text-left">Name</th>
								<th class="px-4 py-3 text-left">Type</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Target On-Demand</th>
								<th class="px-4 py-3 text-left">Target Spot</th>
								<th class="px-4 py-3 text-left">Provisioned On-Demand</th>
								<th class="px-4 py-3 text-left">Provisioned Spot</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each instanceFleets as fleet}
								<tr>
									<td class="px-4 py-3 font-mono text-xs">{fleet.Id ?? '-'}</td>
									<td class="px-4 py-3 font-medium">{fleet.Name ?? '-'}</td>
									<td class="px-4 py-3 text-xs text-gray-500">{fleet.InstanceFleetType ?? '-'}</td>
									<td class="px-4 py-3"><span class="px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-700">{fleet.Status?.State ?? '-'}</span></td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-center">{fleet.TargetOnDemandCapacity ?? 0}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-center">{fleet.TargetSpotCapacity ?? 0}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-center">{fleet.ProvisionedOnDemandCapacity ?? 0}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-center">{fleet.ProvisionedSpotCapacity ?? 0}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

	{:else}
		<!-- Cluster List -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search clusters..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredClusters.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Database class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No active clusters found</p>
				<p class="text-sm mt-1">Launch an EMR cluster to run big data workloads</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Cluster Name</th>
							<th class="px-4 py-3 text-left">State</th>
							<th class="px-4 py-3 text-left">Normalized Hours</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredClusters as cluster}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button onclick={() => selectCluster(cluster.Id ?? '')} class="text-orange-600 dark:text-orange-400 hover:underline font-medium">{cluster.Name}</button>
								</td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${clusterStatusColor(cluster.Status?.State)}-100 text-${clusterStatusColor(cluster.Status?.State)}-700`}>
										{cluster.Status?.State}
									</span>
								</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{cluster.NormalizedInstanceHours ?? 0}</td>
								<td class="px-4 py-3">
									<button onclick={() => terminateCluster(cluster.Id ?? '', cluster.Name ?? '')} class="text-red-500 hover:text-red-700 p-1">
										<XCircle class="w-4 h-4" />
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

<!-- Add Step Modal -->
{#if showAddStep && selectedCluster}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Add Step</h2>
			<div>
				<label for="step-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Step Name</label>
				<input id="step-name" bind:value={stepName} type="text" placeholder="My Spark Job" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="step-jar" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">JAR Location</label>
				<input id="step-jar" bind:value={stepJar} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono text-xs" />
			</div>
			<div>
				<label for="step-args" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Arguments (one per line)</label>
				<textarea id="step-args" bind:value={stepArgs} rows={4} placeholder="s3://my-bucket/script.sh" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
			<div>
				<label for="step-action" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Action on Failure</label>
				<select id="step-action" bind:value={stepActionOnFailure} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="CONTINUE">Continue</option>
					<option value="TERMINATE_CLUSTER">Terminate Cluster</option>
				</select>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showAddStep = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={addStep} disabled={addingStep || !stepName.trim() || !stepJar.trim()} class="flex-1 px-4 py-2 rounded-lg bg-orange-600 text-white text-sm font-medium hover:bg-orange-700 disabled:opacity-50">
					{addingStep ? 'Adding...' : 'Add Step'}
				</button>
			</div>
		</div>
	</div>
{/if}
