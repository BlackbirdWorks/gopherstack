<script lang="ts">
import { onMount } from 'svelte';
import { getECSClient } from '$lib/aws-client';
import {
	ListClustersCommand,
	ListServicesCommand,
	ListTasksCommand,
	ListTaskDefinitionsCommand,
	DescribeTaskDefinitionCommand,
	ListContainerInstancesCommand,
	DescribeContainerInstancesCommand,
	DescribeServicesCommand,
	UpdateServiceCommand,
	DescribeTaskSetsCommand,
	DescribeCapacityProvidersCommand,
	type TaskDefinition,
	type ContainerInstance,
	type TaskSet,
	type CapacityProvider,
	type Service
} from '@aws-sdk/client-ecs';
import { toast } from 'svelte-sonner';
import { Container, Plus, RefreshCw, Search, Layers, Activity, Server, ChevronRight, List, Box, Cpu, LayoutGrid, Zap } from 'lucide-svelte';

const ecs = getECSClient();

let clusters = $state<string[]>([]);
let loading = $state(true);
let selectedCluster = $state<string | null>(null);
let services = $state<string[]>([]);
let servicesLoading = $state(false);
let serviceDetails = $state<Record<string, Service>>({});
let editingService = $state<string | null>(null);
let svcDesiredCount = $state(0);
let svcTaskDef = $state('');
let svcForceDeploy = $state(false);
let updatingService = $state(false);
let tasks = $state<string[]>([]);
let tasksLoading = $state(false);
let taskDefs = $state<string[]>([]);
let taskDefsLoading = $state(false);
let selectedTaskDef = $state<TaskDefinition | null>(null);
let taskDefLoading = $state(false);
let containerInstances = $state<ContainerInstance[]>([]);
let containerInstancesLoading = $state(false);
let taskSets = $state<TaskSet[]>([]);
let taskSetsLoading = $state(false);
let taskSetsLoadedForCluster = $state<string | null>(null);
let capacityProviders = $state<CapacityProvider[]>([]);
let capacityProvidersLoading = $state(false);
let search = $state('');
let servicesByCluster = $state<Record<string, string[]>>({});
let tasksByCluster = $state<Record<string, string[]>>({});
let activeTab = $state<'services' | 'tasks' | 'taskdefs' | 'instances' | 'tasksets' | 'capacity'>('services');

onMount(async () => {
	await loadClusters();
	await loadTaskDefs();
});

async function loadClusters() {
	try {
		loading = true;
		const data = await ecs.send(new ListClustersCommand({}));
		clusters = data.clusterArns || [];
		if (clusters.length > 0) {
			selectedCluster = clusters[0];
			await Promise.all([
				loadServices(clusters[0]),
				loadTasks(clusters[0]),
				loadContainerInstances(clusters[0])
			]);
		}
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load clusters');
	} finally {
		loading = false;
	}
}

async function loadServices(clusterArn: string) {
	try {
		servicesLoading = true;
		const data = await ecs.send(new ListServicesCommand({ cluster: clusterArn }));
		const arns = data.serviceArns || [];
		services = arns;
		servicesByCluster = { ...servicesByCluster, [clusterArn]: arns };
		serviceDetails = {};
		if (arns.length > 0) {
			// DescribeServices accepts up to 10 per call
			const detail: Record<string, Service> = {};
			for (let i = 0; i < arns.length; i += 10) {
				const chunk = arns.slice(i, i + 10);
				const res = await ecs.send(new DescribeServicesCommand({ cluster: clusterArn, services: chunk }));
				for (const s of res.services ?? []) {
					if (s.serviceArn) detail[s.serviceArn] = s;
				}
			}
			serviceDetails = detail;
		}
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load services');
	} finally {
		servicesLoading = false;
	}
}

function startEditService(serviceArn: string) {
	const svc = serviceDetails[serviceArn];
	editingService = serviceArn;
	svcDesiredCount = svc?.desiredCount ?? 0;
	svcTaskDef = svc?.taskDefinition ?? '';
	svcForceDeploy = false;
}

async function updateService(serviceArn: string) {
	if (!selectedCluster) return;
	updatingService = true;
	try {
		await ecs.send(new UpdateServiceCommand({
			cluster: selectedCluster,
			service: serviceName(serviceArn),
			desiredCount: svcDesiredCount,
			taskDefinition: svcTaskDef.trim() || undefined,
			forceNewDeployment: svcForceDeploy
		}));
		toast.success('Service updated');
		editingService = null;
		await loadServices(selectedCluster);
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to update service');
	} finally {
		updatingService = false;
	}
}

async function loadTasks(clusterArn: string) {
	try {
		tasksLoading = true;
		const data = await ecs.send(new ListTasksCommand({ cluster: clusterArn }));
		const arns = data.taskArns || [];
		tasks = arns;
		tasksByCluster = { ...tasksByCluster, [clusterArn]: arns };
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load tasks');
	} finally {
		tasksLoading = false;
	}
}

async function loadTaskDefs() {
	try {
		taskDefsLoading = true;
		const data = await ecs.send(new ListTaskDefinitionsCommand({ status: 'ACTIVE', maxResults: 100 }));
		taskDefs = data.taskDefinitionArns || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load task definitions');
	} finally {
		taskDefsLoading = false;
	}
}

async function loadContainerInstances(clusterArn: string) {
	try {
		containerInstancesLoading = true;
		const list = await ecs.send(new ListContainerInstancesCommand({ cluster: clusterArn }));
		const arns = list.containerInstanceArns || [];
		if (arns.length > 0) {
			const desc = await ecs.send(new DescribeContainerInstancesCommand({ cluster: clusterArn, containerInstances: arns }));
			containerInstances = desc.containerInstances || [];
		} else {
			containerInstances = [];
		}
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load container instances');
	} finally {
		containerInstancesLoading = false;
	}
}

async function loadCapacityProviders(clusterArn: string) {
	try {
		capacityProvidersLoading = true;
		// Get capacity providers attached to the cluster
		const clustersData = await ecs.send(new ListClustersCommand({}));
		const clusterList = clustersData.clusterArns || [];
		if (clusterList.length === 0) {
			capacityProviders = [];
			return;
		}
		// Describe capacity providers - pass empty to get all registered ones
		const cpData = await ecs.send(new DescribeCapacityProvidersCommand({}));
		capacityProviders = cpData.capacityProviders || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load capacity providers');
	} finally {
		capacityProvidersLoading = false;
	}
}

async function handleTabChange(tabId: string) {
	activeTab = tabId as typeof activeTab;
	search = '';
	if (tabId === 'tasksets' && selectedCluster && taskSetsLoadedForCluster !== selectedCluster) {
		await loadTaskSets(selectedCluster);
	}
	if (tabId === 'capacity' && selectedCluster) {
		await loadCapacityProviders(selectedCluster);
	}
}

async function loadTaskSets(clusterArn: string) {
	try {
		taskSetsLoading = true;
		// List services first, then describe task sets for each service in parallel
		const svcs = await ecs.send(new ListServicesCommand({ cluster: clusterArn }));
		const serviceArns = svcs.serviceArns || [];
		if (serviceArns.length === 0) {
			taskSets = [];
			return;
		}
		const results = await Promise.allSettled(
			serviceArns.map((arn) =>
				ecs.send(new DescribeTaskSetsCommand({ cluster: clusterArn, service: arn }))
			)
		);
		const failedRequests = results.filter((result) => result.status === 'rejected');
		taskSets = results.flatMap((result) =>
			result.status === 'fulfilled' ? result.value.taskSets || [] : []
		);
		if (failedRequests.length > 0) {
			toast.error(
				failedRequests.length === serviceArns.length
					? 'Failed to load task sets'
					: 'Some task sets could not be loaded'
			);
		}
		taskSetsLoadedForCluster = clusterArn;
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load task sets');
	} finally {
		taskSetsLoading = false;
	}
}

async function selectCluster(arn: string) {
	selectedCluster = arn;
	taskSetsLoadedForCluster = null;
	taskSets = [];
	const loads: Promise<void>[] = [loadServices(arn), loadTasks(arn), loadContainerInstances(arn)];
	if (activeTab === 'tasksets') {
		loads.push(loadTaskSets(arn));
	}
	await Promise.all(loads);
}

async function selectTaskDef(arn: string) {
	try {
		taskDefLoading = true;
		const data = await ecs.send(new DescribeTaskDefinitionCommand({ taskDefinition: arn }));
		selectedTaskDef = data.taskDefinition || null;
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load task definition');
	} finally {
		taskDefLoading = false;
	}
}

async function reload() {
	await loadClusters();
	await loadTaskDefs();
	if (selectedCluster) {
		await loadTaskSets(selectedCluster);
	}
}

function clusterName(arn: string) {
	return arn.split('/').pop() || arn;
}

function serviceName(arn: string) {
	return arn.split('/').pop() || arn;
}

function taskId(arn: string) {
	return arn.split('/').pop()?.slice(0, 12) || arn;
}

function taskDefFamily(arn: string) {
	// arn:aws:ecs:region:account:task-definition/family:revision
	const part = arn.split('/').pop() || arn;
	return part.split(':')[0];
}

function taskDefRevision(arn: string) {
	const part = arn.split('/').pop() || '';
	return part.split(':')[1] || '';
}

function clusterRegion(arn: string) {
	const parts = arn.split(':');
	return parts[3] || '—';
}

function clusterAccount(arn: string) {
	const parts = arn.split(':');
	return parts[4] || '—';
}

function instanceId(arn: string) {
	return arn.split('/').pop()?.slice(0, 12) || arn;
}

function taskSetId(arn: string) {
	return arn.split('/').pop() || arn;
}

function copyToClipboard(text: string) {
	navigator.clipboard.writeText(text).then(() => toast.success('Copied to clipboard'));
}

let filteredServices = $derived(services.filter(s =>
	!search || serviceName(s).toLowerCase().includes(search.toLowerCase())
));

let filteredTasks = $derived(tasks.filter(t =>
	!search || taskId(t).toLowerCase().includes(search.toLowerCase())
));

let filteredTaskDefs = $derived(taskDefs.filter(td =>
	!search || taskDefFamily(td).toLowerCase().includes(search.toLowerCase())
));

let filteredContainerInstances = $derived(containerInstances.filter(ci =>
	!search || (ci.ec2InstanceId || '').toLowerCase().includes(search.toLowerCase()) ||
	(ci.containerInstanceArn || '').toLowerCase().includes(search.toLowerCase())
));

let filteredTaskSets = $derived(taskSets.filter(ts =>
	!search || (ts.taskSetArn || '').toLowerCase().includes(search.toLowerCase()) ||
	(ts.id || '').toLowerCase().includes(search.toLowerCase())
));

let filteredCapacityProviders = $derived(capacityProviders.filter(cp =>
	!search || (cp.name || '').toLowerCase().includes(search.toLowerCase()) ||
	(cp.capacityProviderArn || '').toLowerCase().includes(search.toLowerCase())
));

let totalServices = $derived(Object.values(servicesByCluster).reduce((acc, s) => acc + s.length, 0));
let totalTasks = $derived(Object.values(tasksByCluster).reduce((acc, t) => acc + t.length, 0));

// Group task defs by family
let taskDefFamilies = $derived(() => {
	const families = new Set<string>();
	for (const td of taskDefs) families.add(taskDefFamily(td));
	return families.size;
});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Container class="w-6 h-6 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">ECS Clusters</h1>
				<p class="text-slate-600 dark:text-slate-300">Elastic Container Service</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button
				onclick={reload}
				class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300"
				title="Refresh"
			>
				<RefreshCw class="w-4 h-4" />
			</button>
			<button
				onclick={() => toast.info('Use the AWS console or CLI to create ECS clusters.')}
				class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"
			>
				<Plus class="w-4 h-4" />
				Create Cluster
			</button>
		</div>
	</div>

	<!-- Stats cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Layers class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{clusters.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Clusters</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<Activity class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{totalServices}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Services</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Server class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{totalTasks}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Running Tasks</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Box class="w-5 h-5 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{taskDefs.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Task Definitions</p>
			</div>
		</div>
	</div>

	{#if loading}
		<div class="text-center py-12 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
			<p>Loading clusters...</p>
		</div>
	{:else if clusters.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
			<Container class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
			<p class="text-slate-600 dark:text-slate-300 text-lg mb-1">No ECS clusters found</p>
			<p class="text-slate-400 dark:text-slate-500 text-sm mb-4">Create a cluster to start deploying containers.</p>
			<button
				onclick={() => toast.info('Use the AWS console or CLI to create ECS clusters.')}
				class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700"
			>
				Create Cluster
			</button>
		</div>
	{:else}
		<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
			<!-- Cluster list panel -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<h2 class="font-semibold text-slate-900 dark:text-white mb-3 flex items-center gap-2">
					<Layers class="w-4 h-4 text-purple-500" />
					Clusters ({clusters.length})
				</h2>
				<div class="space-y-1">
					{#each clusters as cluster}
						{@const name = clusterName(cluster)}
						{@const region = clusterRegion(cluster)}
						{@const account = clusterAccount(cluster)}
						{@const clusterServices = servicesByCluster[cluster] ?? []}
						{@const clusterTasks = tasksByCluster[cluster] ?? []}
						<button
							onclick={() => selectCluster(cluster)}
							class="w-full text-left px-3 py-3 rounded-lg transition-colors {selectedCluster === cluster
								? 'bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 ring-1 ring-indigo-300 dark:ring-indigo-700'
								: 'hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300'}"
						>
							<div class="flex items-center justify-between">
								<p class="font-semibold truncate">{name}</p>
								<ChevronRight class="w-4 h-4 shrink-0 opacity-50" />
							</div>
							<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{region} · {account}</p>
							<div class="flex gap-2 mt-1">
								{#if clusterServices.length > 0}
									<span class="text-xs text-green-600 dark:text-green-400">
										{clusterServices.length} {clusterServices.length === 1 ? 'svc' : 'svcs'}
									</span>
								{/if}
								{#if clusterTasks.length > 0}
									<span class="text-xs text-blue-600 dark:text-blue-400">
										{clusterTasks.length} {clusterTasks.length === 1 ? 'task' : 'tasks'}
									</span>
								{/if}
							</div>
						</button>
					{/each}
				</div>
			</div>

			<!-- Right panel with tabs -->
			<div class="lg:col-span-2 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
				<!-- Tab bar -->
				<div class="flex overflow-x-auto border-b border-slate-200 dark:border-slate-700 px-4">
					{#each [
						{ id: 'services', label: 'Services', icon: Activity, count: services.length },
						{ id: 'tasks', label: 'Tasks', icon: Server, count: tasks.length },
						{ id: 'taskdefs', label: 'Task Defs', icon: Box, count: taskDefs.length },
						{ id: 'instances', label: 'Instances', icon: Cpu, count: containerInstances.length },
						{ id: 'tasksets', label: 'Task Sets', icon: LayoutGrid, count: taskSets.length },
						{ id: 'capacity', label: 'Capacity', icon: Zap, count: capacityProviders.length }
					] as tab}
						<button
							onclick={() => handleTabChange(tab.id)}
							class="flex items-center gap-1.5 px-3 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === tab.id
								? 'border-indigo-500 text-indigo-600 dark:text-indigo-400'
								: 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
						>
							<tab.icon class="w-4 h-4" />
							{tab.label}
							<span class="ml-1 text-xs bg-slate-100 dark:bg-slate-700 rounded-full px-1.5">{tab.count}</span>
						</button>
					{/each}
				</div>

				<div class="p-4">
					<!-- Search -->
					<div class="relative mb-4">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input
							type="text"
							placeholder="Search..."
							bind:value={search}
							class="w-full pl-9 pr-4 py-2 text-sm border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
						/>
					</div>

					<!-- Services tab -->
					{#if activeTab === 'services'}
						{#if servicesLoading}
							<div class="text-center py-8 text-slate-500 dark:text-slate-400 text-sm">
								<div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500 mb-2"></div>
								<p>Loading services...</p>
							</div>
						{:else if filteredServices.length === 0}
							<div class="text-center py-10">
								<Activity class="w-10 h-10 mx-auto text-slate-300 dark:text-slate-600 mb-2 opacity-50" />
								<p class="text-slate-500 dark:text-slate-400 text-sm">No services found in this cluster</p>
							</div>
						{:else}
							<div class="space-y-2">
								{#each filteredServices as service}
									{@const name = serviceName(service)}
									{@const svc = serviceDetails[service]}
									<div class="p-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg border border-slate-200 dark:border-slate-600 hover:border-indigo-300 dark:hover:border-indigo-700 transition-colors">
										<div class="flex items-center justify-between">
											<p class="font-semibold text-slate-900 dark:text-white text-sm">{name}</p>
											<div class="flex items-center gap-2">
												<span class="text-xs px-2 py-0.5 rounded-full {svc?.status === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300'} font-medium">{svc?.status ?? 'Active'}</span>
												{#if svc}<span class="text-xs text-slate-500 dark:text-slate-400">{svc.runningCount ?? 0}/{svc.desiredCount ?? 0} running</span>{/if}
												<button onclick={() => editingService === service ? (editingService = null) : startEditService(service)} class="text-xs px-2 py-0.5 rounded border border-indigo-200 dark:border-indigo-800 text-indigo-600 dark:text-indigo-400 hover:bg-indigo-50 dark:hover:bg-indigo-900/20">{editingService === service ? 'Cancel' : 'Update'}</button>
											</div>
										</div>
										<p class="text-xs text-slate-400 dark:text-slate-500 font-mono mt-1 truncate">{svc?.taskDefinition ?? service}</p>
										{#if editingService === service}
											<div class="mt-3 pt-3 border-t border-slate-200 dark:border-slate-600 space-y-2">
												<div class="flex items-center gap-2 flex-wrap">
													<label class="text-xs text-slate-500 dark:text-slate-400" for="svc-desired-{name}">Desired count</label>
													<input id="svc-desired-{name}" type="number" min="0" bind:value={svcDesiredCount} class="w-20 px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
												</div>
												<div>
													<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="svc-taskdef-{name}">Task definition</label>
													<input id="svc-taskdef-{name}" type="text" bind:value={svcTaskDef} placeholder="family:revision" class="w-full px-2 py-1 text-xs font-mono border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
												</div>
												<label class="flex items-center gap-1.5 text-xs text-slate-500 dark:text-slate-400">
													<input type="checkbox" bind:checked={svcForceDeploy} /> Force new deployment
												</label>
												<button disabled={updatingService} onclick={() => updateService(service)} class="w-full py-1.5 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700 disabled:opacity-50">{updatingService ? 'Updating...' : 'Apply Update'}</button>
											</div>
										{/if}
									</div>
								{/each}
							</div>
							<p class="text-xs text-slate-400 mt-3 text-right">{filteredServices.length} services</p>
						{/if}

					<!-- Tasks tab -->
					{:else if activeTab === 'tasks'}
						{#if tasksLoading}
							<div class="text-center py-8 text-slate-500 dark:text-slate-400 text-sm">
								<div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500 mb-2"></div>
								<p>Loading tasks...</p>
							</div>
						{:else if filteredTasks.length === 0}
							<div class="text-center py-10">
								<List class="w-10 h-10 mx-auto text-slate-300 dark:text-slate-600 mb-2 opacity-50" />
								<p class="text-slate-500 dark:text-slate-400 text-sm">No running tasks found in this cluster</p>
							</div>
						{:else}
							<div class="space-y-2">
								{#each filteredTasks as task}
									{@const id = taskId(task)}
									<div class="p-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg border border-slate-200 dark:border-slate-600">
										<div class="flex items-center justify-between">
											<p class="font-semibold text-slate-900 dark:text-white text-sm font-mono">{id}</p>
											<span class="text-xs px-2 py-0.5 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 font-medium">RUNNING</span>
										</div>
										<div class="flex items-center justify-between mt-1">
											<p class="text-xs text-slate-400 dark:text-slate-500 font-mono truncate">{task}</p>
											<div class="flex items-center gap-2 ml-2">
												<a href="https://console.aws.amazon.com/cloudwatch/home#logsV2:log-groups" target="_blank" rel="noopener noreferrer" class="text-xs text-indigo-500 hover:text-indigo-700 dark:hover:text-indigo-300">View Logs ↗</a>
												<button onclick={() => copyToClipboard(task)} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">
													<span class="text-xs">Copy</span>
												</button>
											</div>
										</div>
									</div>
								{/each}
							</div>
							<p class="text-xs text-slate-400 mt-3 text-right">{filteredTasks.length} tasks</p>
						{/if}

					<!-- Task Definitions tab -->
					{:else if activeTab === 'taskdefs'}
						{#if taskDefsLoading}
							<div class="text-center py-8 text-slate-500 dark:text-slate-400 text-sm">
								<div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500 mb-2"></div>
								<p>Loading task definitions...</p>
							</div>
						{:else if filteredTaskDefs.length === 0}
							<div class="text-center py-10">
								<Box class="w-10 h-10 mx-auto text-slate-300 dark:text-slate-600 mb-2 opacity-50" />
								<p class="text-slate-500 dark:text-slate-400 text-sm">No task definitions found</p>
							</div>
						{:else}
							<div class="grid grid-cols-1 gap-2">
								{#each filteredTaskDefs as td}
									{@const family = taskDefFamily(td)}
									{@const rev = taskDefRevision(td)}
									<div
										class="p-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg border border-slate-200 dark:border-slate-600 hover:border-indigo-300 dark:hover:border-indigo-700 cursor-pointer transition-colors"
										onclick={() => selectTaskDef(td)}
										role="button"
										tabindex="0"
										onkeydown={(e) => e.key === 'Enter' && selectTaskDef(td)}
									>
										<div class="flex items-center justify-between">
											<p class="font-semibold text-slate-900 dark:text-white text-sm">{family}</p>
											<span class="text-xs px-2 py-0.5 rounded-full bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300">rev {rev}</span>
										</div>
										{#if selectedTaskDef && selectedTaskDef.family === family && !taskDefLoading}
											<div class="mt-2 pt-2 border-t border-slate-200 dark:border-slate-600 text-xs text-slate-600 dark:text-slate-400 space-y-1">
												<p>CPU: {selectedTaskDef.cpu || '—'} · Memory: {selectedTaskDef.memory || '—'}</p>
												<p>Network: {selectedTaskDef.networkMode || 'bridge'}</p>
												<p>Containers: {selectedTaskDef.containerDefinitions?.length ?? 0}</p>
												{#if (selectedTaskDef.containerDefinitions ?? []).length > 0}
													<div class="flex flex-wrap gap-1 mt-1">
														{#each (selectedTaskDef.containerDefinitions ?? []) as cdef}
															<span class="px-1.5 py-0.5 bg-slate-200 dark:bg-slate-600 rounded text-xs">{cdef.name}</span>
														{/each}
													</div>
												{/if}
											</div>
										{:else if taskDefLoading && selectedTaskDef?.family === family}
											<p class="text-xs text-slate-400 mt-1 animate-pulse">Loading details...</p>
										{/if}
									</div>
								{/each}
							</div>
							<p class="text-xs text-slate-400 mt-3 text-right">{filteredTaskDefs.length} definitions</p>
						{/if}

					<!-- Container Instances tab -->
					{:else if activeTab === 'instances'}
						{#if containerInstancesLoading}
							<div class="text-center py-8 text-slate-500 dark:text-slate-400 text-sm">
								<div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500 mb-2"></div>
								<p>Loading container instances...</p>
							</div>
						{:else if filteredContainerInstances.length === 0}
							<div class="text-center py-10">
								<Cpu class="w-10 h-10 mx-auto text-slate-300 dark:text-slate-600 mb-2 opacity-50" />
								<p class="text-slate-500 dark:text-slate-400 text-sm">No container instances found in this cluster</p>
							</div>
						{:else}
							<div class="space-y-2">
								{#each filteredContainerInstances as ci}
									{@const ciId = instanceId(ci.containerInstanceArn || '')}
									<div class="p-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg border border-slate-200 dark:border-slate-600">
										<div class="flex items-center justify-between">
											<p class="font-semibold text-slate-900 dark:text-white text-sm font-mono">{ciId}</p>
											<span class="text-xs px-2 py-0.5 rounded-full {ci.status === 'ACTIVE'
												? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300'
												: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300'} font-medium">
												{ci.status || 'UNKNOWN'}
											</span>
										</div>
										<div class="grid grid-cols-2 gap-x-4 mt-1 text-xs text-slate-500 dark:text-slate-400">
											<p>EC2: {ci.ec2InstanceId || '—'}</p>
											<p>Agent: {ci.agentConnected ? '✓ Connected' : '✗ Disconnected'}</p>
											<p>Running tasks: {ci.runningTasksCount ?? 0}</p>
											<p>Pending tasks: {ci.pendingTasksCount ?? 0}</p>
										</div>
										{#if ci.agentUpdateStatus}
											<p class="text-xs text-amber-600 dark:text-amber-400 mt-1">Agent update: {ci.agentUpdateStatus}</p>
										{/if}
									</div>
								{/each}
							</div>
							<p class="text-xs text-slate-400 mt-3 text-right">{filteredContainerInstances.length} instances</p>
						{/if}

					<!-- Task Sets tab -->
					{:else if activeTab === 'tasksets'}
						{#if taskSetsLoading}
							<div class="text-center py-8 text-slate-500 dark:text-slate-400 text-sm">
								<div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500 mb-2"></div>
								<p>Loading task sets...</p>
							</div>
						{:else if filteredTaskSets.length === 0}
							<div class="text-center py-10">
								<LayoutGrid class="w-10 h-10 mx-auto text-slate-300 dark:text-slate-600 mb-2 opacity-50" />
								<p class="text-slate-500 dark:text-slate-400 text-sm">No task sets found in this cluster</p>
							</div>
						{:else}
							<div class="space-y-2">
								{#each filteredTaskSets as ts}
									{@const tsId = taskSetId(ts.taskSetArn || '')}
									<div class="p-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg border border-slate-200 dark:border-slate-600">
										<div class="flex items-center justify-between">
											<p class="font-semibold text-slate-900 dark:text-white text-sm font-mono">{tsId}</p>
											<span class="text-xs px-2 py-0.5 rounded-full {ts.status === 'ACTIVE'
												? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300'
												: 'bg-slate-100 dark:bg-slate-600 text-slate-600 dark:text-slate-300'} font-medium">
												{ts.status || 'UNKNOWN'}
											</span>
										</div>
										<div class="grid grid-cols-2 gap-x-4 mt-1 text-xs text-slate-500 dark:text-slate-400">
											<p>Launch: {ts.launchType || 'FARGATE'}</p>
											<p>Scale: {ts.scale?.value ?? 100}% ({ts.scale?.unit || 'PERCENT'})</p>
											{#if ts.taskDefinition}
												<p class="col-span-2 truncate">Task def: {ts.taskDefinition}</p>
											{/if}
										</div>
									</div>
								{/each}
							</div>
							<p class="text-xs text-slate-400 mt-3 text-right">{filteredTaskSets.length} task sets</p>
						{/if}

					<!-- Capacity Providers tab -->
					{:else if activeTab === 'capacity'}
						{#if capacityProvidersLoading}
							<div class="text-center py-8 text-slate-500 dark:text-slate-400 text-sm">
								<div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500 mb-2"></div>
								<p>Loading capacity providers...</p>
							</div>
						{:else if filteredCapacityProviders.length === 0}
							<div class="text-center py-10">
								<Zap class="w-10 h-10 mx-auto text-slate-300 dark:text-slate-600 mb-2 opacity-50" />
								<p class="text-slate-500 dark:text-slate-400 text-sm">No capacity providers found</p>
								<p class="text-slate-400 dark:text-slate-500 text-xs mt-1">Create a capacity provider and attach it to the cluster.</p>
							</div>
						{:else}
							<div class="space-y-2">
								{#each filteredCapacityProviders as cp}
									<div class="p-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg border border-slate-200 dark:border-slate-600">
										<div class="flex items-center justify-between">
											<p class="font-semibold text-slate-900 dark:text-white text-sm">{cp.name || '—'}</p>
											<span class="text-xs px-2 py-0.5 rounded-full {cp.status === 'ACTIVE'
												? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300'
												: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300'} font-medium">
												{cp.status || 'UNKNOWN'}
											</span>
										</div>
										<p class="text-xs text-slate-400 dark:text-slate-500 font-mono mt-1 truncate">{cp.capacityProviderArn || '—'}</p>
										{#if cp.updateStatus}
											<p class="text-xs text-amber-600 dark:text-amber-400 mt-1">Update status: {cp.updateStatus}</p>
										{/if}
									</div>
								{/each}
							</div>
							<p class="text-xs text-slate-400 mt-3 text-right">{filteredCapacityProviders.length} capacity providers</p>
						{/if}
					{/if}
				</div>
			</div>
		</div>
	{/if}
</div>
