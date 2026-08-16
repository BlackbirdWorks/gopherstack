<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { multiRegionList } from '$lib/multi-region';
	import { isAllRegions, currentRegion } from '$lib/region.svelte';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
	import { getECSClient } from '$lib/aws-client';
	import {
		ListClustersCommand,
		DescribeClustersCommand,
		CreateClusterCommand,
		UpdateClusterCommand,
		DeleteClusterCommand,
		PutClusterCapacityProvidersCommand,
		ListServicesCommand,
		DescribeServicesCommand,
		CreateServiceCommand,
		UpdateServiceCommand,
		DeleteServiceCommand,
		ListTasksCommand,
		DescribeTasksCommand,
		RunTaskCommand,
		StopTaskCommand,
		ListTaskDefinitionsCommand,
		DescribeTaskDefinitionCommand,
		RegisterTaskDefinitionCommand,
		DeregisterTaskDefinitionCommand,
		DeleteTaskDefinitionsCommand,
		ListContainerInstancesCommand,
		DescribeContainerInstancesCommand,
		DeregisterContainerInstanceCommand,
		UpdateContainerInstancesStateCommand,
		UpdateContainerAgentCommand,
		DescribeCapacityProvidersCommand,
		CreateCapacityProviderCommand,
		UpdateCapacityProviderCommand,
		DeleteCapacityProviderCommand,
		DescribeTaskSetsCommand,
		CreateTaskSetCommand,
		UpdateTaskSetCommand,
		DeleteTaskSetCommand,
		UpdateServicePrimaryTaskSetCommand,
		ListAccountSettingsCommand,
		PutAccountSettingCommand,
		PutAccountSettingDefaultCommand,
		DeleteAccountSettingCommand,
		type Cluster,
		type Service,
		type Task,
		type TaskDefinition,
		type ContainerInstance,
		type CapacityProvider,
		type TaskSet,
		type Setting,
		type SettingName,
		type ContainerDefinition,
		type KeyValuePair
	} from '@aws-sdk/client-ecs';
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
	import { Container, Plus, Trash2, Eye, Pencil, Play, Square, Check } from 'lucide-svelte';

	const client = regionalClient(getECSClient);

	// The SDK puts the AWS error code on err.name and status on
	// err.$metadata.httpStatusCode; err.message alone is usually just the
	// human-readable text. Combine them so both the toast and the inline
	// error banner show the actual code, not just a generic message.
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

	// Search only over named fields that are actually meaningful for each
	// resource, never the whole serialized object.
	function matches(q: string, ...vals: (string | undefined)[]): boolean {
		if (!q) return true;
		const needle = q.toLowerCase();
		return vals.some((v) => (v ?? '').toLowerCase().includes(needle));
	}

	function parseCommaList(s: string): string[] {
		return s
			.split(',')
			.map((x) => x.trim())
			.filter((x) => x.length > 0);
	}

	// Parses "key=value" lines (one per line) into KeyValuePair[]/Tag-shaped
	// records, used for both container environment variables and resource
	// tags across this page's create forms.
	function parseKeyValueLines(s: string): KeyValuePair[] {
		return s
			.split('\n')
			.map((line) => line.trim())
			.filter((line) => line.length > 0)
			.map((line) => {
				const idx = line.indexOf('=');
				if (idx < 0) return { name: line, value: '' };
				return { name: line.slice(0, idx).trim(), value: line.slice(idx + 1).trim() };
			});
	}

	function arnName(arn: string | undefined): string {
		return arn?.split('/').pop() ?? '';
	}

	function clusterShortName(arn: string | undefined): string {
		return arnName(arn);
	}

	function taskDefFamily(arn: string | undefined): string {
		const part = arnName(arn);
		return part.split(':')[0] ?? part;
	}

	function taskDefRevision(arn: string | undefined): string {
		const part = arnName(arn);
		return part.split(':')[1] ?? '';
	}

	function taskShortId(arn: string | undefined): string {
		return arnName(arn).slice(0, 12);
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	type TabId =
		| 'clusters'
		| 'services'
		| 'tasks'
		| 'taskDefinitions'
		| 'containerInstances'
		| 'capacityProviders'
		| 'taskSets'
		| 'accountSettings';

	const tabs: TabDef[] = [
		{ id: 'clusters', label: 'Clusters' },
		{ id: 'services', label: 'Services' },
		{ id: 'tasks', label: 'Tasks' },
		{ id: 'taskDefinitions', label: 'Task Definitions' },
		{ id: 'containerInstances', label: 'Container Instances' },
		{ id: 'capacityProviders', label: 'Capacity Providers' },
		{ id: 'taskSets', label: 'Task Sets' },
		{ id: 'accountSettings', label: 'Account Settings' }
	];

	// Clusters is the parent resource for Services/Tasks/Container Instances --
	// the same shared-selector pattern accessanalyzer/directoryservice use for
	// their scoped tabs. Task Sets is doubly-scoped (cluster AND a specific
	// service within it), so it gets its own service sub-selector further down
	// rather than being folded into this list.
	const clusterScopedTabs: TabId[] = ['services', 'tasks', 'containerInstances'];

	// Every child tab's calls (services/tasks/containerInstances/taskSets)
	// must go to the SELECTED cluster's own region, not the picker's region
	// -- in All mode the cluster driving the drill-down can be from any
	// fanned region, unrelated to whatever the picker currently shows.
	type Regioned<T> = T & { region: string };

	let activeTab = $state<TabId>('clusters');
	let searchQuery = $state('');

	// ==================== Clusters ====================

	let clusters = $state<Regioned<Cluster>[]>([]);
	let clustersNextToken = $state<string | undefined>();
	let loadingMoreClusters = $state(false);

	let selectedClusterArn = $state('');
	let selectedClusterRegion = $state('');
	const selectedCluster = $derived(clusters.find((c) => c.clusterArn === selectedClusterArn));

	// In All mode, fans ListClusters+DescribeClusters out across every region
	// with data and tags each row; pagination (nextToken) is inherently
	// single-region, so "Load More" only appends within the currently
	// selected region.
	async function fetchClusters(reset: boolean): Promise<void> {
		if (reset && isAllRegions()) {
			const result = await multiRegionList(
				async (region) => {
					const rc = getECSClient(region);
					const list = await rc.send(new ListClustersCommand({ maxResults: 20 }));
					const arns = list.clusterArns ?? [];
					if (arns.length === 0) return { clusters: [] as Cluster[] };
					const desc = await rc.send(new DescribeClustersCommand({ clusters: arns, include: ['TAGS', 'SETTINGS'] }));
					return { clusters: desc.clusters ?? [] };
				},
				(r) => r.clusters
			);
			clusters = result.items.map(({ region, item }) => ({ ...item, region }) as Regioned<Cluster>);
			clustersNextToken = undefined;
			if (result.errors.length > 0) {
				toast.error(`Failed to load clusters from ${result.errors.length} region(s)`);
			}
		} else {
			const list = await client().send(
				new ListClustersCommand({ nextToken: reset ? undefined : clustersNextToken, maxResults: 20 })
			);
			const arns = list.clusterArns ?? [];
			clustersNextToken = list.nextToken;
			let page: Cluster[] = [];
			if (arns.length > 0) {
				const desc = await client().send(
					new DescribeClustersCommand({ clusters: arns, include: ['TAGS', 'SETTINGS'] })
				);
				page = desc.clusters ?? [];
			}
			const region = currentRegion();
			const tagged = page.map((c) => ({ ...c, region }) as Regioned<Cluster>);
			clusters = reset ? tagged : [...clusters, ...tagged];
		}
		if (!selectedClusterArn && clusters.length > 0) {
			selectedClusterArn = clusters[0].clusterArn ?? '';
			selectedClusterRegion = clusters[0].region;
		}
	}

	async function loadMoreClusters(): Promise<void> {
		loadingMoreClusters = true;
		try {
			await fetchClusters(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreClusters = false;
		}
	}

	function onClusterSelect(arn: string): void {
		selectedClusterArn = arn;
		selectedClusterRegion = clusters.find((c) => c.clusterArn === arn)?.region ?? currentRegion();
		taskSetServiceArn = '';
		taskSets = [];
		if (clusterScopedTabs.includes(activeTab) || activeTab === 'taskSets') {
			tabLoader.refresh(activeTab);
		}
	}

	let createClusterModal = $state<Modal | null>(null);
	let creatingCluster = $state(false);
	let createClusterError = $state<string | null>(null);
	let newClusterName = $state('');
	let newClusterContainerInsights = $state(false);
	let newClusterCapacityProviders = $state('');

	function openCreateClusterModal(): void {
		createClusterError = null;
		newClusterName = '';
		newClusterContainerInsights = false;
		newClusterCapacityProviders = '';
		createClusterModal?.open();
	}

	async function submitCreateCluster(): Promise<void> {
		if (!newClusterName) {
			createClusterError = 'Cluster name is required.';
			return;
		}
		creatingCluster = true;
		createClusterError = null;
		try {
			await client().send(
				new CreateClusterCommand({
					clusterName: newClusterName,
					settings: [{ name: 'containerInsights', value: newClusterContainerInsights ? 'enabled' : 'disabled' }],
					capacityProviders:
						parseCommaList(newClusterCapacityProviders).length > 0
							? parseCommaList(newClusterCapacityProviders)
							: undefined
				})
			);
			toast.success('Cluster created');
			createClusterModal?.close();
			await tabLoader.refresh('clusters');
		} catch (e) {
			const msg = describeError(e);
			createClusterError = msg;
			toast.error(msg);
		} finally {
			creatingCluster = false;
		}
	}

	let editClusterModal = $state<Modal | null>(null);
	let updatingCluster = $state(false);
	let updateClusterError = $state<string | null>(null);
	let editClusterTarget = $state<Regioned<Cluster> | null>(null);
	let editClusterContainerInsights = $state(false);
	let editClusterCapacityProviders = $state('');

	function openEditClusterModal(c: Regioned<Cluster>): void {
		editClusterTarget = c;
		editClusterContainerInsights = (c.settings ?? []).some(
			(s) => s.name === 'containerInsights' && (s.value === 'enabled' || s.value === 'enhanced')
		);
		editClusterCapacityProviders = (c.capacityProviders ?? []).join(', ');
		updateClusterError = null;
		editClusterModal?.open();
	}

	// UpdateClusterCommand's real request shape (UpdateClusterRequest in the
	// installed SDK) is only cluster/settings/configuration/
	// serviceConnectDefaults -- it has NO capacityProviders or
	// defaultCapacityProviderStrategy field at all (confirmed against the
	// compiled types; TypeScript rejects it: "Object literal may only
	// specify known properties, and 'capacityProviders' does not exist in
	// type 'UpdateClusterCommandInput'"). This backend's updateClusterInput
	// (handler_clusters.go) accepts both anyway -- a real wire-shape
	// mismatch, since the real API manages a cluster's capacity-provider
	// association exclusively through the separate PutClusterCapacityProviders
	// operation. This form therefore issues both real ops: UpdateCluster for
	// settings, PutClusterCapacityProviders for the association list.
	async function submitEditCluster(): Promise<void> {
		if (!editClusterTarget?.clusterArn) return;
		updatingCluster = true;
		updateClusterError = null;
		try {
			await getECSClient(editClusterTarget.region).send(
				new UpdateClusterCommand({
					cluster: editClusterTarget.clusterArn,
					settings: [{ name: 'containerInsights', value: editClusterContainerInsights ? 'enabled' : 'disabled' }]
				})
			);
			const providers = parseCommaList(editClusterCapacityProviders);
			if (providers.length > 0) {
				await getECSClient(editClusterTarget.region).send(
					new PutClusterCapacityProvidersCommand({
						cluster: editClusterTarget.clusterArn,
						capacityProviders: providers,
						defaultCapacityProviderStrategy: undefined
					})
				);
			}
			toast.success('Cluster updated');
			editClusterModal?.close();
			await tabLoader.refresh('clusters');
		} catch (e) {
			const msg = describeError(e);
			updateClusterError = msg;
			toast.error(msg);
		} finally {
			updatingCluster = false;
		}
	}

	async function handleDeleteCluster(c: Regioned<Cluster>): Promise<void> {
		if (!c.clusterArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete cluster',
			message: `Delete cluster ${c.clusterName}? This cascades to its services, tasks, and container instance registrations.`
		});
		if (!confirmed) return;
		try {
			await getECSClient(c.region).send(new DeleteClusterCommand({ cluster: c.clusterArn }));
			toast.success('Cluster deleted');
			if (selectedClusterArn === c.clusterArn) {
				selectedClusterArn = '';
				selectedClusterRegion = '';
			}
			await tabLoader.refresh('clusters');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let clusterDetailModal = $state<Modal | null>(null);
	let viewedCluster = $state<Regioned<Cluster> | null>(null);

	function openClusterDetail(c: Regioned<Cluster>): void {
		viewedCluster = c;
		clusterDetailModal?.open();
	}

	// ==================== Services (cluster-scoped) ====================

	let services = $state<Service[]>([]);
	let servicesNextToken = $state<string | undefined>();
	let loadingMoreServices = $state(false);

	async function fetchServices(reset: boolean): Promise<void> {
		if (!selectedClusterArn) {
			services = [];
			servicesNextToken = undefined;
			return;
		}
		const list = await getECSClient(selectedClusterRegion).send(
			new ListServicesCommand({
				cluster: selectedClusterArn,
				nextToken: reset ? undefined : servicesNextToken,
				maxResults: 20
			})
		);
		const arns = list.serviceArns ?? [];
		servicesNextToken = list.nextToken;
		let page: Service[] = [];
		// DescribeServices accepts up to 10 services per call.
		for (let i = 0; i < arns.length; i += 10) {
			const chunk = arns.slice(i, i + 10);
			const desc = await getECSClient(selectedClusterRegion).send(
				new DescribeServicesCommand({ cluster: selectedClusterArn, services: chunk, include: ['TAGS'] })
			);
			page = [...page, ...(desc.services ?? [])];
		}
		services = reset ? page : [...services, ...page];
	}

	async function loadMoreServices(): Promise<void> {
		loadingMoreServices = true;
		try {
			await fetchServices(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreServices = false;
		}
	}

	let createServiceModal = $state<Modal | null>(null);
	let creatingService = $state(false);
	let createServiceError = $state<string | null>(null);
	let newServiceName = $state('');
	let newServiceTaskDef = $state('');
	let newServiceDesiredCount = $state(1);
	let newServiceLaunchType = $state<'FARGATE' | 'EC2' | 'EXTERNAL'>('FARGATE');

	function openCreateServiceModal(): void {
		createServiceError = selectedClusterArn ? null : 'Select a cluster first.';
		newServiceName = '';
		newServiceTaskDef = '';
		newServiceDesiredCount = 1;
		newServiceLaunchType = 'FARGATE';
		createServiceModal?.open();
	}

	async function submitCreateService(): Promise<void> {
		if (!selectedClusterArn) {
			createServiceError = 'Select a cluster first.';
			return;
		}
		if (!newServiceName || !newServiceTaskDef) {
			createServiceError = 'Service name and task definition are required.';
			return;
		}
		creatingService = true;
		createServiceError = null;
		try {
			await getECSClient(selectedClusterRegion).send(
				new CreateServiceCommand({
					cluster: selectedClusterArn,
					serviceName: newServiceName,
					taskDefinition: newServiceTaskDef,
					desiredCount: newServiceDesiredCount,
					launchType: newServiceLaunchType
				})
			);
			toast.success('Service creation started');
			createServiceModal?.close();
			await tabLoader.refresh('services');
		} catch (e) {
			const msg = describeError(e);
			createServiceError = msg;
			toast.error(msg);
		} finally {
			creatingService = false;
		}
	}

	let editServiceModal = $state<Modal | null>(null);
	let updatingService = $state(false);
	let updateServiceError = $state<string | null>(null);
	let editServiceTarget = $state<Service | null>(null);
	let editServiceDesiredCount = $state(0);
	let editServiceTaskDef = $state('');
	let editServiceForceDeploy = $state(false);

	function openEditServiceModal(s: Service): void {
		editServiceTarget = s;
		editServiceDesiredCount = s.desiredCount ?? 0;
		editServiceTaskDef = s.taskDefinition ?? '';
		editServiceForceDeploy = false;
		updateServiceError = null;
		editServiceModal?.open();
	}

	async function submitEditService(): Promise<void> {
		if (!selectedClusterArn || !editServiceTarget?.serviceName) return;
		updatingService = true;
		updateServiceError = null;
		try {
			await getECSClient(selectedClusterRegion).send(
				new UpdateServiceCommand({
					cluster: selectedClusterArn,
					service: editServiceTarget.serviceName,
					desiredCount: editServiceDesiredCount,
					taskDefinition: editServiceTaskDef.trim() || undefined,
					forceNewDeployment: editServiceForceDeploy
				})
			);
			toast.success('Service updated');
			editServiceModal?.close();
			await tabLoader.refresh('services');
		} catch (e) {
			const msg = describeError(e);
			updateServiceError = msg;
			toast.error(msg);
		} finally {
			updatingService = false;
		}
	}

	async function handleDeleteService(s: Service): Promise<void> {
		if (!selectedClusterArn || !s.serviceName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete service',
			message: `Delete service ${s.serviceName}? Running tasks will be stopped.`
		});
		if (!confirmed) return;
		try {
			await getECSClient(selectedClusterRegion).send(
				new DeleteServiceCommand({ cluster: selectedClusterArn, service: s.serviceName, force: true })
			);
			toast.success('Service deletion started');
			await tabLoader.refresh('services');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let serviceDetailModal = $state<Modal | null>(null);
	let viewedService = $state<Service | null>(null);

	function openServiceDetail(s: Service): void {
		viewedService = s;
		serviceDetailModal?.open();
	}

	// ==================== Tasks (cluster-scoped) ====================
	//
	// RunTask/StopTask are the real create/delete-equivalent verbs -- ECS has
	// no UpdateTask operation at all, so this family has no update action.

	let tasks = $state<Task[]>([]);
	let tasksNextToken = $state<string | undefined>();
	let loadingMoreTasks = $state(false);

	async function fetchTasks(reset: boolean): Promise<void> {
		if (!selectedClusterArn) {
			tasks = [];
			tasksNextToken = undefined;
			return;
		}
		const list = await getECSClient(selectedClusterRegion).send(
			new ListTasksCommand({
				cluster: selectedClusterArn,
				nextToken: reset ? undefined : tasksNextToken,
				maxResults: 20
			})
		);
		const arns = list.taskArns ?? [];
		tasksNextToken = list.nextToken;
		let page: Task[] = [];
		if (arns.length > 0) {
			const desc = await getECSClient(selectedClusterRegion).send(
				new DescribeTasksCommand({ cluster: selectedClusterArn, tasks: arns })
			);
			page = desc.tasks ?? [];
		}
		tasks = reset ? page : [...tasks, ...page];
	}

	async function loadMoreTasks(): Promise<void> {
		loadingMoreTasks = true;
		try {
			await fetchTasks(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTasks = false;
		}
	}

	let runTaskModal = $state<Modal | null>(null);
	let runningTask = $state(false);
	let runTaskError = $state<string | null>(null);
	let runTaskDef = $state('');
	let runTaskCount = $state(1);
	let runTaskLaunchType = $state<'FARGATE' | 'EC2' | 'EXTERNAL'>('FARGATE');
	let runTaskStartedBy = $state('');

	function openRunTaskModal(): void {
		runTaskError = selectedClusterArn ? null : 'Select a cluster first.';
		runTaskDef = '';
		runTaskCount = 1;
		runTaskLaunchType = 'FARGATE';
		runTaskStartedBy = '';
		runTaskModal?.open();
	}

	async function submitRunTask(): Promise<void> {
		if (!selectedClusterArn) {
			runTaskError = 'Select a cluster first.';
			return;
		}
		if (!runTaskDef) {
			runTaskError = 'Task definition is required.';
			return;
		}
		runningTask = true;
		runTaskError = null;
		try {
			await getECSClient(selectedClusterRegion).send(
				new RunTaskCommand({
					cluster: selectedClusterArn,
					taskDefinition: runTaskDef,
					count: runTaskCount,
					launchType: runTaskLaunchType,
					startedBy: runTaskStartedBy || undefined
				})
			);
			toast.success('Task started');
			runTaskModal?.close();
			await tabLoader.refresh('tasks');
		} catch (e) {
			const msg = describeError(e);
			runTaskError = msg;
			toast.error(msg);
		} finally {
			runningTask = false;
		}
	}

	async function handleStopTask(t: Task): Promise<void> {
		if (!selectedClusterArn || !t.taskArn) return;
		const confirmed = await confirmDestructive({
			title: 'Stop task',
			message: `Stop task ${taskShortId(t.taskArn)}?`
		});
		if (!confirmed) return;
		try {
			await getECSClient(selectedClusterRegion).send(
				new StopTaskCommand({
					cluster: selectedClusterArn,
					task: t.taskArn,
					reason: 'Stopped from gopherstack console'
				})
			);
			toast.success('Task stop requested');
			await tabLoader.refresh('tasks');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let taskDetailModal = $state<Modal | null>(null);
	let viewedTask = $state<Task | null>(null);

	function openTaskDetail(t: Task): void {
		viewedTask = t;
		taskDetailModal?.open();
	}

	// ==================== Task Definitions (global, family/revision) ====================
	//
	// Task definitions are REGISTERED and DEREGISTERED, not created/deleted --
	// the real API has no "update" for an existing revision either; the
	// closest analog is registering a new revision under the same family,
	// which is exposed here as its own action rather than mislabeled "update".
	//
	// RegisterTaskDefinitionInput's real containerDefinitions shape
	// (aws-sdk-go-v2/service/ecs types.ContainerDefinition) has far more
	// fields than this backend's ContainerDefinition model actually stores
	// (services/ecs/models.go): no links, hostname, user, workingDirectory,
	// dnsServers/dnsSearchDomains, extraHosts, dockerSecurityOptions,
	// dockerLabels, ulimits, linuxParameters, interactive/pseudoTerminal,
	// start/stopTimeout, systemControls, credentialSpecs, restartPolicy, or
	// versionConsistency -- a real client submitting those fields today would
	// have them silently discarded (Go's encoding/json ignores unknown
	// fields). What genuinely round-trips and is covered below: name, image,
	// cpu, memory/memoryReservation, essential, entryPoint, command,
	// environment, and portMappings. The richer fields above are a reporting
	// finding, not something this form fakes support for.

	let taskDefArns = $state<string[]>([]);
	let taskDefsNextToken = $state<string | undefined>();
	let loadingMoreTaskDefs = $state(false);
	let selectedTaskDef = $state<TaskDefinition | null>(null);
	let taskDefDetailLoading = $state(false);

	async function fetchTaskDefs(reset: boolean): Promise<void> {
		const data = await client().send(
			new ListTaskDefinitionsCommand({
				status: 'ACTIVE',
				sort: 'DESC',
				nextToken: reset ? undefined : taskDefsNextToken,
				maxResults: 20
			})
		);
		const arns = data.taskDefinitionArns ?? [];
		taskDefsNextToken = data.nextToken;
		taskDefArns = reset ? arns : [...taskDefArns, ...arns];
	}

	async function loadMoreTaskDefs(): Promise<void> {
		loadingMoreTaskDefs = true;
		try {
			await fetchTaskDefs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTaskDefs = false;
		}
	}

	async function selectTaskDef(arn: string): Promise<void> {
		try {
			taskDefDetailLoading = true;
			const data = await client().send(new DescribeTaskDefinitionCommand({ taskDefinition: arn }));
			selectedTaskDef = data.taskDefinition ?? null;
			taskDefDetailModal?.open();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			taskDefDetailLoading = false;
		}
	}

	let taskDefDetailModal = $state<Modal | null>(null);

	let registerTaskDefModal = $state<Modal | null>(null);
	let registeringTaskDef = $state(false);
	let registerTaskDefError = $state<string | null>(null);
	let newTaskDefFamily = $state('');
	let newTaskDefTaskRole = $state('');
	let newTaskDefExecutionRole = $state('');
	let newTaskDefNetworkMode = $state<'bridge' | 'host' | 'awsvpc' | 'none'>('awsvpc');
	let newTaskDefCpu = $state('256');
	let newTaskDefMemory = $state('512');
	let newTaskDefRequiresFargate = $state(true);
	let newTaskDefRequiresEc2 = $state(false);

	type NewContainerDef = {
		key: number;
		name: string;
		image: string;
		cpu: string;
		memory: string;
		essential: boolean;
		entryPoint: string;
		command: string;
		environment: string;
		portMappings: string;
	};

	let nextContainerDefKey = 0;

	function blankContainerDef(): NewContainerDef {
		nextContainerDefKey += 1;
		return {
			key: nextContainerDefKey,
			name: '',
			image: '',
			cpu: '',
			memory: '',
			essential: true,
			entryPoint: '',
			command: '',
			environment: '',
			portMappings: ''
		};
	}

	let newTaskDefContainers = $state<NewContainerDef[]>([blankContainerDef()]);

	function openRegisterTaskDefModal(): void {
		registerTaskDefError = null;
		newTaskDefFamily = '';
		newTaskDefTaskRole = '';
		newTaskDefExecutionRole = '';
		newTaskDefNetworkMode = 'awsvpc';
		newTaskDefCpu = '256';
		newTaskDefMemory = '512';
		newTaskDefRequiresFargate = true;
		newTaskDefRequiresEc2 = false;
		newTaskDefContainers = [blankContainerDef()];
		registerTaskDefModal?.open();
	}

	function addContainerDef(): void {
		newTaskDefContainers = [...newTaskDefContainers, blankContainerDef()];
	}

	function removeContainerDef(index: number): void {
		newTaskDefContainers = newTaskDefContainers.filter((_, i) => i !== index);
	}

	function buildContainerDefinitions(): ContainerDefinition[] {
		return newTaskDefContainers
			.filter((c) => c.name && c.image)
			.map((c) => ({
				name: c.name,
				image: c.image,
				cpu: c.cpu ? Number(c.cpu) : undefined,
				memory: c.memory ? Number(c.memory) : undefined,
				essential: c.essential,
				entryPoint: c.entryPoint ? parseCommaList(c.entryPoint) : undefined,
				command: c.command ? parseCommaList(c.command) : undefined,
				environment: c.environment ? parseKeyValueLines(c.environment) : undefined,
				portMappings: c.portMappings
					? parseCommaList(c.portMappings).map((pair) => {
							const [containerPort, hostPort] = pair.split(':').map((n) => Number(n.trim()));
							return { containerPort, hostPort: Number.isNaN(hostPort) ? undefined : hostPort };
						})
					: undefined
			}));
	}

	async function submitRegisterTaskDef(): Promise<void> {
		if (!newTaskDefFamily) {
			registerTaskDefError = 'Family is required.';
			return;
		}
		const containerDefinitions = buildContainerDefinitions();
		if (containerDefinitions.length === 0) {
			registerTaskDefError = 'At least one container with a name and image is required.';
			return;
		}
		const requiresCompatibilities: ('EC2' | 'FARGATE')[] = [
			...(newTaskDefRequiresFargate ? (['FARGATE'] as const) : []),
			...(newTaskDefRequiresEc2 ? (['EC2'] as const) : [])
		];
		registeringTaskDef = true;
		registerTaskDefError = null;
		try {
			await client().send(
				new RegisterTaskDefinitionCommand({
					family: newTaskDefFamily,
					taskRoleArn: newTaskDefTaskRole || undefined,
					executionRoleArn: newTaskDefExecutionRole || undefined,
					networkMode: newTaskDefNetworkMode,
					cpu: newTaskDefCpu || undefined,
					memory: newTaskDefMemory || undefined,
					requiresCompatibilities:
						requiresCompatibilities.length > 0 ? requiresCompatibilities : undefined,
					containerDefinitions
				})
			);
			toast.success('Task definition registered');
			registerTaskDefModal?.close();
			await tabLoader.refresh('taskDefinitions');
		} catch (e) {
			const msg = describeError(e);
			registerTaskDefError = msg;
			toast.error(msg);
		} finally {
			registeringTaskDef = false;
		}
	}

	async function handleDeregisterTaskDef(arn: string): Promise<void> {
		const confirmed = await confirmDestructive({
			title: 'Deregister task definition',
			message: `Deregister ${arnName(arn)}? This marks the revision INACTIVE; it can no longer be used to run new tasks or services, but existing ones are unaffected.`,
			confirmLabel: 'Deregister'
		});
		if (!confirmed) return;
		try {
			await client().send(new DeregisterTaskDefinitionCommand({ taskDefinition: arn }));
			toast.success('Task definition deregistered');
			await tabLoader.refresh('taskDefinitions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// DeleteTaskDefinitions is a distinct, real, permanent-removal operation
	// (as opposed to Deregister's soft INACTIVE flip) -- AWS requires the
	// revision to already be INACTIVE (i.e. deregistered) before it can be
	// permanently deleted.
	async function handleDeleteTaskDef(arn: string): Promise<void> {
		const confirmed = await confirmDestructive({
			title: 'Permanently delete task definition revision',
			message: `Permanently delete ${arnName(arn)}? This requires the revision to already be deregistered (INACTIVE) and cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteTaskDefinitionsCommand({ taskDefinitions: [arn] }));
			toast.success('Task definition deleted');
			await tabLoader.refresh('taskDefinitions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Container Instances (cluster-scoped) ====================
	//
	// RegisterContainerInstance has NO practical create form here for two
	// independent reasons: (1) in real AWS usage it is invoked exclusively by
	// the ECS container agent bootstrapping on an EC2 instance, never
	// manually from the console; and (2) this backend's wire shape is a real
	// bug -- handler_container_instances.go's registerContainerInstanceInput
	// takes `ec2InstanceId` directly, a field that does not exist anywhere on
	// the real RegisterContainerInstanceRequest (confirmed against the
	// installed @aws-sdk/client-ecs types: the real request only carries
	// `instanceIdentityDocument` + `instanceIdentityDocumentSignature`, the
	// signed JSON blob from the EC2 metadata service, from which AWS itself
	// parses the instance ID -- there is no direct ec2InstanceId input field
	// at all). A form built against the real typed SDK could never populate
	// the field this backend actually reads. Reported, not worked around.

	let containerInstances = $state<ContainerInstance[]>([]);
	let containerInstancesNextToken = $state<string | undefined>();
	let loadingMoreContainerInstances = $state(false);

	async function fetchContainerInstances(reset: boolean): Promise<void> {
		if (!selectedClusterArn) {
			containerInstances = [];
			containerInstancesNextToken = undefined;
			return;
		}
		const list = await getECSClient(selectedClusterRegion).send(
			new ListContainerInstancesCommand({
				cluster: selectedClusterArn,
				nextToken: reset ? undefined : containerInstancesNextToken,
				maxResults: 20
			})
		);
		const arns = list.containerInstanceArns ?? [];
		containerInstancesNextToken = list.nextToken;
		let page: ContainerInstance[] = [];
		if (arns.length > 0) {
			const desc = await getECSClient(selectedClusterRegion).send(
				new DescribeContainerInstancesCommand({
					cluster: selectedClusterArn,
					containerInstances: arns,
					include: ['TAGS']
				})
			);
			page = desc.containerInstances ?? [];
		}
		containerInstances = reset ? page : [...containerInstances, ...page];
	}

	async function loadMoreContainerInstances(): Promise<void> {
		loadingMoreContainerInstances = true;
		try {
			await fetchContainerInstances(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreContainerInstances = false;
		}
	}

	async function handleDeregisterContainerInstance(ci: ContainerInstance): Promise<void> {
		if (!selectedClusterArn || !ci.containerInstanceArn) return;
		const confirmed = await confirmDestructive({
			title: 'Deregister container instance',
			message: `Deregister ${ci.ec2InstanceId ?? arnName(ci.containerInstanceArn)}? Any tasks still running on it will be stopped.`,
			confirmLabel: 'Deregister'
		});
		if (!confirmed) return;
		try {
			await getECSClient(selectedClusterRegion).send(
				new DeregisterContainerInstanceCommand({
					cluster: selectedClusterArn,
					containerInstance: ci.containerInstanceArn,
					force: true
				})
			);
			toast.success('Container instance deregistered');
			await tabLoader.refresh('containerInstances');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let drainModal = $state<Modal | null>(null);
	let draining = $state(false);
	let drainError = $state<string | null>(null);
	let drainTarget = $state<ContainerInstance | null>(null);
	let drainStatus = $state<'ACTIVE' | 'DRAINING'>('DRAINING');

	function openDrainModal(ci: ContainerInstance): void {
		drainTarget = ci;
		drainStatus = ci.status === 'DRAINING' ? 'ACTIVE' : 'DRAINING';
		drainError = null;
		drainModal?.open();
	}

	async function submitDrain(): Promise<void> {
		if (!selectedClusterArn || !drainTarget?.containerInstanceArn) return;
		draining = true;
		drainError = null;
		try {
			await getECSClient(selectedClusterRegion).send(
				new UpdateContainerInstancesStateCommand({
					cluster: selectedClusterArn,
					containerInstances: [drainTarget.containerInstanceArn],
					status: drainStatus
				})
			);
			toast.success('Container instance state updated');
			drainModal?.close();
			await tabLoader.refresh('containerInstances');
		} catch (e) {
			const msg = describeError(e);
			drainError = msg;
			toast.error(msg);
		} finally {
			draining = false;
		}
	}

	async function handleUpdateAgent(ci: ContainerInstance): Promise<void> {
		if (!selectedClusterArn || !ci.containerInstanceArn) return;
		try {
			await getECSClient(selectedClusterRegion).send(
				new UpdateContainerAgentCommand({
					cluster: selectedClusterArn,
					containerInstance: ci.containerInstanceArn
				})
			);
			toast.success('Container agent update started');
			await tabLoader.refresh('containerInstances');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let containerInstanceDetailModal = $state<Modal | null>(null);
	let viewedContainerInstance = $state<ContainerInstance | null>(null);

	function openContainerInstanceDetail(ci: ContainerInstance): void {
		viewedContainerInstance = ci;
		containerInstanceDetailModal?.open();
	}

	// ==================== Capacity Providers (account-level, optional cluster filter) ====================

	let capacityProviders = $state<CapacityProvider[]>([]);
	let capacityProvidersNextToken = $state<string | undefined>();
	let loadingMoreCapacityProviders = $state(false);
	let filterCapacityProvidersByCluster = $state(false);

	async function fetchCapacityProviders(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeCapacityProvidersCommand({
				cluster: filterCapacityProvidersByCluster ? selectedClusterArn || undefined : undefined,
				include: ['TAGS'],
				nextToken: reset ? undefined : capacityProvidersNextToken,
				maxResults: 10
			})
		);
		const page = resp.capacityProviders ?? [];
		capacityProvidersNextToken = resp.nextToken;
		capacityProviders = reset ? page : [...capacityProviders, ...page];
	}

	async function loadMoreCapacityProviders(): Promise<void> {
		loadingMoreCapacityProviders = true;
		try {
			await fetchCapacityProviders(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCapacityProviders = false;
		}
	}

	function toggleCapacityProviderClusterFilter(): void {
		filterCapacityProvidersByCluster = !filterCapacityProvidersByCluster;
		tabLoader.refresh('capacityProviders');
	}

	let createCapacityProviderModal = $state<Modal | null>(null);
	let creatingCapacityProvider = $state(false);
	let createCapacityProviderError = $state<string | null>(null);
	let newCpName = $state('');
	let newCpAsgArn = $state('');
	let newCpManagedScalingStatus = $state<'ENABLED' | 'DISABLED'>('ENABLED');
	let newCpTargetCapacity = $state(100);
	let newCpManagedTermination = $state<'ENABLED' | 'DISABLED'>('ENABLED');
	let newCpManagedDraining = $state<'ENABLED' | 'DISABLED'>('ENABLED');

	function openCreateCapacityProviderModal(): void {
		createCapacityProviderError = null;
		newCpName = '';
		newCpAsgArn = '';
		newCpManagedScalingStatus = 'ENABLED';
		newCpTargetCapacity = 100;
		newCpManagedTermination = 'ENABLED';
		newCpManagedDraining = 'ENABLED';
		createCapacityProviderModal?.open();
	}

	async function submitCreateCapacityProvider(): Promise<void> {
		if (!newCpName || !newCpAsgArn) {
			createCapacityProviderError = 'Name and Auto Scaling group ARN are required.';
			return;
		}
		creatingCapacityProvider = true;
		createCapacityProviderError = null;
		try {
			await client().send(
				new CreateCapacityProviderCommand({
					name: newCpName,
					autoScalingGroupProvider: {
						autoScalingGroupArn: newCpAsgArn,
						managedScaling: {
							status: newCpManagedScalingStatus,
							targetCapacity: newCpTargetCapacity
						},
						managedTerminationProtection: newCpManagedTermination,
						managedDraining: newCpManagedDraining
					}
				})
			);
			toast.success('Capacity provider created');
			createCapacityProviderModal?.close();
			await tabLoader.refresh('capacityProviders');
		} catch (e) {
			const msg = describeError(e);
			createCapacityProviderError = msg;
			toast.error(msg);
		} finally {
			creatingCapacityProvider = false;
		}
	}

	// UpdateCapacityProviderCommand's real request shape
	// (UpdateCapacityProviderRequest in the installed SDK) has only
	// name/cluster/autoScalingGroupProvider/managedInstancesProvider -- NO
	// status and NO tags field. This backend's updateCapacityProviderInput
	// (handler_capacity_providers.go) additionally accepts `status` and
	// `tags`, neither of which exists on the real API's Update input at all
	// (status can only be driven to INACTIVE via Delete; tags are managed via
	// TagResource) -- a wire-shape mismatch worth reporting. This form sends
	// only the real fields. Separately, the real update's
	// autoScalingGroupProvider is the narrower AutoScalingGroupProviderUpdate
	// type (managedScaling/managedTerminationProtection/managedDraining only
	// -- no autoScalingGroupArn, since the underlying ASG can't be swapped by
	// Update), while this backend reuses the full Create-time
	// AutoScalingGroupProvider type for Update too; sending no ARN here is
	// therefore also the real-shaped request, and is reported as a second
	// finding under the same op.
	let editCapacityProviderModal = $state<Modal | null>(null);
	let updatingCapacityProvider = $state(false);
	let updateCapacityProviderError = $state<string | null>(null);
	let editCpTarget = $state<CapacityProvider | null>(null);
	let editCpManagedScalingStatus = $state<'ENABLED' | 'DISABLED'>('ENABLED');
	let editCpTargetCapacity = $state(100);
	let editCpManagedTermination = $state<'ENABLED' | 'DISABLED'>('ENABLED');
	let editCpManagedDraining = $state<'ENABLED' | 'DISABLED'>('ENABLED');

	function openEditCapacityProviderModal(cp: CapacityProvider): void {
		editCpTarget = cp;
		editCpManagedScalingStatus = cp.autoScalingGroupProvider?.managedScaling?.status === 'DISABLED' ? 'DISABLED' : 'ENABLED';
		editCpTargetCapacity = cp.autoScalingGroupProvider?.managedScaling?.targetCapacity ?? 100;
		editCpManagedTermination = cp.autoScalingGroupProvider?.managedTerminationProtection === 'DISABLED' ? 'DISABLED' : 'ENABLED';
		editCpManagedDraining = cp.autoScalingGroupProvider?.managedDraining === 'DISABLED' ? 'DISABLED' : 'ENABLED';
		updateCapacityProviderError = null;
		editCapacityProviderModal?.open();
	}

	async function submitEditCapacityProvider(): Promise<void> {
		if (!editCpTarget?.name) return;
		updatingCapacityProvider = true;
		updateCapacityProviderError = null;
		try {
			await client().send(
				new UpdateCapacityProviderCommand({
					name: editCpTarget.name,
					autoScalingGroupProvider: {
						managedScaling: {
							status: editCpManagedScalingStatus,
							targetCapacity: editCpTargetCapacity
						},
						managedTerminationProtection: editCpManagedTermination,
						managedDraining: editCpManagedDraining
					}
				})
			);
			toast.success('Capacity provider updated');
			editCapacityProviderModal?.close();
			await tabLoader.refresh('capacityProviders');
		} catch (e) {
			const msg = describeError(e);
			updateCapacityProviderError = msg;
			toast.error(msg);
		} finally {
			updatingCapacityProvider = false;
		}
	}

	async function handleDeleteCapacityProvider(cp: CapacityProvider): Promise<void> {
		if (!cp.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete capacity provider',
			message: `Delete capacity provider ${cp.name}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteCapacityProviderCommand({ capacityProvider: cp.name }));
			toast.success('Capacity provider deleted');
			await tabLoader.refresh('capacityProviders');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let capacityProviderDetailModal = $state<Modal | null>(null);
	let viewedCapacityProvider = $state<CapacityProvider | null>(null);

	function openCapacityProviderDetail(cp: CapacityProvider): void {
		viewedCapacityProvider = cp;
		capacityProviderDetailModal?.open();
	}

	// ==================== Task Sets (cluster + service scoped) ====================
	//
	// DescribeTaskSetsRequest/Response have no nextToken member at all in the
	// installed SDK -- the real API genuinely returns every task set for the
	// given service in one response, so there is nothing to paginate here.

	let taskSetServiceArn = $state('');
	let taskSets = $state<TaskSet[]>([]);

	async function fetchTaskSets(): Promise<void> {
		if (!selectedClusterArn) {
			taskSets = [];
			return;
		}
		// Task Sets is doubly-scoped (cluster + service); make sure the
		// service list used for the sub-selector is populated even if the
		// user never visited the Services tab directly.
		await tabLoader.load('services');
		if (!taskSetServiceArn && services.length > 0) {
			taskSetServiceArn = services[0].serviceArn ?? '';
		}
		if (!taskSetServiceArn) {
			taskSets = [];
			return;
		}
		const resp = await getECSClient(selectedClusterRegion).send(
			new DescribeTaskSetsCommand({
				cluster: selectedClusterArn,
				service: taskSetServiceArn,
				include: ['TAGS']
			})
		);
		taskSets = resp.taskSets ?? [];
	}

	function onTaskSetServiceSelect(arn: string): void {
		taskSetServiceArn = arn;
		tabLoader.refresh('taskSets');
	}

	let createTaskSetModal = $state<Modal | null>(null);
	let creatingTaskSet = $state(false);
	let createTaskSetError = $state<string | null>(null);
	let newTaskSetTaskDef = $state('');
	let newTaskSetExternalId = $state('');
	let newTaskSetScaleValue = $state(100);
	let newTaskSetLaunchType = $state<'FARGATE' | 'EC2' | 'EXTERNAL'>('FARGATE');

	function openCreateTaskSetModal(): void {
		createTaskSetError = taskSetServiceArn ? null : 'Select a service first.';
		newTaskSetTaskDef = '';
		newTaskSetExternalId = '';
		newTaskSetScaleValue = 100;
		newTaskSetLaunchType = 'FARGATE';
		createTaskSetModal?.open();
	}

	async function submitCreateTaskSet(): Promise<void> {
		if (!selectedClusterArn || !taskSetServiceArn) {
			createTaskSetError = 'Select a cluster and service first.';
			return;
		}
		if (!newTaskSetTaskDef) {
			createTaskSetError = 'Task definition is required.';
			return;
		}
		creatingTaskSet = true;
		createTaskSetError = null;
		try {
			await getECSClient(selectedClusterRegion).send(
				new CreateTaskSetCommand({
					cluster: selectedClusterArn,
					service: taskSetServiceArn,
					taskDefinition: newTaskSetTaskDef,
					externalId: newTaskSetExternalId || undefined,
					launchType: newTaskSetLaunchType,
					scale: { value: newTaskSetScaleValue, unit: 'PERCENT' }
				})
			);
			toast.success('Task set created');
			createTaskSetModal?.close();
			await tabLoader.refresh('taskSets');
		} catch (e) {
			const msg = describeError(e);
			createTaskSetError = msg;
			toast.error(msg);
		} finally {
			creatingTaskSet = false;
		}
	}

	let editTaskSetModal = $state<Modal | null>(null);
	let updatingTaskSet = $state(false);
	let updateTaskSetError = $state<string | null>(null);
	let editTaskSetTarget = $state<TaskSet | null>(null);
	let editTaskSetScaleValue = $state(100);

	function openEditTaskSetModal(ts: TaskSet): void {
		editTaskSetTarget = ts;
		editTaskSetScaleValue = ts.scale?.value ?? 100;
		updateTaskSetError = null;
		editTaskSetModal?.open();
	}

	async function submitEditTaskSet(): Promise<void> {
		if (!selectedClusterArn || !taskSetServiceArn || !editTaskSetTarget?.taskSetArn) return;
		updatingTaskSet = true;
		updateTaskSetError = null;
		try {
			await getECSClient(selectedClusterRegion).send(
				new UpdateTaskSetCommand({
					cluster: selectedClusterArn,
					service: taskSetServiceArn,
					taskSet: editTaskSetTarget.taskSetArn,
					scale: { value: editTaskSetScaleValue, unit: 'PERCENT' }
				})
			);
			toast.success('Task set updated');
			editTaskSetModal?.close();
			await tabLoader.refresh('taskSets');
		} catch (e) {
			const msg = describeError(e);
			updateTaskSetError = msg;
			toast.error(msg);
		} finally {
			updatingTaskSet = false;
		}
	}

	async function handleDeleteTaskSet(ts: TaskSet): Promise<void> {
		if (!selectedClusterArn || !taskSetServiceArn || !ts.taskSetArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete task set',
			message: `Delete task set ${ts.id ?? arnName(ts.taskSetArn)}?`
		});
		if (!confirmed) return;
		try {
			await getECSClient(selectedClusterRegion).send(
				new DeleteTaskSetCommand({
					cluster: selectedClusterArn,
					service: taskSetServiceArn,
					taskSet: ts.taskSetArn,
					force: true
				})
			);
			toast.success('Task set deleted');
			await tabLoader.refresh('taskSets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleSetPrimaryTaskSet(ts: TaskSet): Promise<void> {
		if (!selectedClusterArn || !taskSetServiceArn || !ts.taskSetArn) return;
		try {
			await getECSClient(selectedClusterRegion).send(
				new UpdateServicePrimaryTaskSetCommand({
					cluster: selectedClusterArn,
					service: taskSetServiceArn,
					primaryTaskSet: ts.taskSetArn
				})
			);
			toast.success('Primary task set updated');
			await tabLoader.refresh('taskSets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let taskSetDetailModal = $state<Modal | null>(null);
	let viewedTaskSet = $state<TaskSet | null>(null);

	function openTaskSetDetail(ts: TaskSet): void {
		viewedTaskSet = ts;
		taskSetDetailModal?.open();
	}

	// ==================== Account Settings (account-level) ====================
	//
	// PutAccountSetting is a genuine upsert -- the real API has no separate
	// Create vs Update for a (name, principal) pair, so this family exposes a
	// single "Put" action rather than inventing a create/update split the API
	// doesn't have. DeleteAccountSetting resets a setting back to the
	// account default.

	let accountSettings = $state<Setting[]>([]);
	let accountSettingsNextToken = $state<string | undefined>();
	let loadingMoreAccountSettings = $state(false);

	async function fetchAccountSettings(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListAccountSettingsCommand({
				effectiveSettings: true,
				nextToken: reset ? undefined : accountSettingsNextToken,
				maxResults: 10
			})
		);
		const page = resp.settings ?? [];
		accountSettingsNextToken = resp.nextToken;
		accountSettings = reset ? page : [...accountSettings, ...page];
	}

	async function loadMoreAccountSettings(): Promise<void> {
		loadingMoreAccountSettings = true;
		try {
			await fetchAccountSettings(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAccountSettings = false;
		}
	}

	let putSettingModal = $state<Modal | null>(null);
	let puttingSetting = $state(false);
	let putSettingError = $state<string | null>(null);
	let newSettingName = $state('containerInsights');
	let newSettingValue = $state('enabled');
	let newSettingPrincipalArn = $state('');
	let newSettingAsDefault = $state(false);

	function openPutSettingModal(): void {
		putSettingError = null;
		newSettingName = 'containerInsights';
		newSettingValue = 'enabled';
		newSettingPrincipalArn = '';
		newSettingAsDefault = false;
		putSettingModal?.open();
	}

	async function submitPutSetting(): Promise<void> {
		if (!newSettingName || !newSettingValue) {
			putSettingError = 'Name and value are required.';
			return;
		}
		puttingSetting = true;
		putSettingError = null;
		try {
			// The name field is free text here (it's just as easy to mistype
			// an account setting name as any other AWS input), so it's typed
			// as a plain string and cast to the SDK's SettingName union only
			// at the call site -- the real API 400s on an unknown name just
			// like any other invalid enum value sent over the wire.
			if (newSettingAsDefault) {
				await client().send(
					new PutAccountSettingDefaultCommand({
						name: newSettingName as SettingName,
						value: newSettingValue
					})
				);
			} else {
				await client().send(
					new PutAccountSettingCommand({
						name: newSettingName as SettingName,
						value: newSettingValue,
						principalArn: newSettingPrincipalArn || undefined
					})
				);
			}
			toast.success('Account setting saved');
			putSettingModal?.close();
			await tabLoader.refresh('accountSettings');
		} catch (e) {
			const msg = describeError(e);
			putSettingError = msg;
			toast.error(msg);
		} finally {
			puttingSetting = false;
		}
	}

	async function handleDeleteSetting(s: Setting): Promise<void> {
		if (!s.name) return;
		const confirmed = await confirmDestructive({
			title: 'Reset account setting',
			message: `Reset ${s.name} to the account default?`,
			confirmLabel: 'Reset'
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteAccountSettingCommand({ name: s.name, principalArn: s.principalArn })
			);
			toast.success('Account setting reset');
			await tabLoader.refresh('accountSettings');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Tab loader / wiring ====================

	const tabLoader = createTabLoader<TabId>({
		clusters: () => fetchClusters(true).catch(rethrowDescribed),
		services: () => fetchServices(true).catch(rethrowDescribed),
		tasks: () => fetchTasks(true).catch(rethrowDescribed),
		taskDefinitions: () => fetchTaskDefs(true).catch(rethrowDescribed),
		containerInstances: () => fetchContainerInstances(true).catch(rethrowDescribed),
		capacityProviders: () => fetchCapacityProviders(true).catch(rethrowDescribed),
		taskSets: () => fetchTaskSets().catch(rethrowDescribed),
		accountSettings: () => fetchAccountSettings(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// Clusters is the parent resource for Services/Tasks/Container
	// Instances/Task Sets: on a region change the previously selected cluster
	// ARN belongs to the old region and must not be reused, so reload
	// clusters first (which re-selects a cluster for the new region) before
	// reloading whichever tab is active.
	onRegionChange(() => {
		selectedClusterArn = '';
		selectedClusterRegion = '';
		taskSetServiceArn = '';
		clusters = [];
		clustersNextToken = undefined;
		void tabLoader.refresh('clusters').then(() => {
			if (activeTab !== 'clusters') {
				tabLoader.refresh(activeTab);
			}
		});
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredClusters = $derived(
		clusters.filter((c) => matches(searchQuery, c.clusterName, c.clusterArn, c.status))
	);
	const filteredServices = $derived(
		services.filter((s) => matches(searchQuery, s.serviceName, s.taskDefinition, s.status))
	);
	const filteredTasks = $derived(
		tasks.filter((t) => matches(searchQuery, t.taskArn, t.taskDefinitionArn, t.lastStatus, t.group))
	);
	const filteredTaskDefArns = $derived(
		taskDefArns.filter((arn) => matches(searchQuery, taskDefFamily(arn), arn))
	);
	const filteredContainerInstances = $derived(
		containerInstances.filter((ci) =>
			matches(searchQuery, ci.ec2InstanceId, ci.containerInstanceArn, ci.status)
		)
	);
	const filteredCapacityProviders = $derived(
		capacityProviders.filter((cp) => matches(searchQuery, cp.name, cp.status, cp.capacityProviderArn))
	);
	const filteredTaskSets = $derived(
		taskSets.filter((ts) => matches(searchQuery, ts.id, ts.taskSetArn, ts.status, ts.taskDefinition))
	);
	const filteredAccountSettings = $derived(
		accountSettings.filter((s) => matches(searchQuery, s.name, s.value, s.principalArn))
	);
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Container}
		title="Amazon ECS"
		description="Clusters, services, tasks, task definitions, and capacity providers"
		onRefresh={handleRefresh}
		color="purple"
	>
		{#snippet actions()}
			{#if activeTab === 'clusters'}
				<button
					onclick={openCreateClusterModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create cluster
				</button>
			{:else if activeTab === 'services'}
				<button
					onclick={openCreateServiceModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create service
				</button>
			{:else if activeTab === 'tasks'}
				<button
					onclick={openRunTaskModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm"
				>
					<Play class="w-4 h-4" /> Run task
				</button>
			{:else if activeTab === 'taskDefinitions'}
				<button
					onclick={openRegisterTaskDefModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Register task definition
				</button>
			{:else if activeTab === 'capacityProviders'}
				<button
					onclick={openCreateCapacityProviderModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create capacity provider
				</button>
			{:else if activeTab === 'taskSets'}
				<button
					onclick={openCreateTaskSetModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create task set
				</button>
			{:else if activeTab === 'accountSettings'}
				<button
					onclick={openPutSettingModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Put setting
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="purple" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if clusterScopedTabs.includes(activeTab)}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="cluster-select" class="text-sm text-gray-500 dark:text-gray-400">Cluster</label>
					<select
						id="cluster-select"
						value={selectedClusterArn}
						onchange={(e) => onClusterSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if clusters.length === 0}
							<option value="">No clusters</option>
						{/if}
						{#each clusters as c (c.region + '::' + c.clusterArn)}
							<option value={c.clusterArn}>{clusterShortName(c.clusterArn)} — {c.region}</option>
						{/each}
					</select>
					{#if selectedClusterRegion}
						<RegionChip region={selectedClusterRegion} />
					{/if}
				</div>
			{:else if activeTab === 'taskSets'}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="ts-cluster-select" class="text-sm text-gray-500 dark:text-gray-400">Cluster</label>
					<select
						id="ts-cluster-select"
						value={selectedClusterArn}
						onchange={(e) => onClusterSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if clusters.length === 0}
							<option value="">No clusters</option>
						{/if}
						{#each clusters as c (c.region + '::' + c.clusterArn)}
							<option value={c.clusterArn}>{clusterShortName(c.clusterArn)} — {c.region}</option>
						{/each}
					</select>
					{#if selectedClusterRegion}
						<RegionChip region={selectedClusterRegion} />
					{/if}
					<label for="ts-service-select" class="text-sm text-gray-500 dark:text-gray-400">Service</label>
					<select
						id="ts-service-select"
						value={taskSetServiceArn}
						onchange={(e) => onTaskSetServiceSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if services.length === 0}
							<option value="">No services</option>
						{/if}
						{#each services as s (s.serviceArn)}
							<option value={s.serviceArn}>{s.serviceName}</option>
						{/each}
					</select>
				</div>
			{:else if activeTab === 'capacityProviders'}
				<label class="flex items-center gap-2 text-sm text-gray-500 dark:text-gray-400">
					<input
						type="checkbox"
						checked={filterCapacityProvidersByCluster}
						onchange={toggleCapacityProviderClusterFilter}
					/>
					Filter to cluster: {selectedCluster ? clusterShortName(selectedCluster.clusterArn) : '(none selected)'}
				</label>
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

			{#if activeTab === 'clusters'}
				{#snippet clusterRegionCell(c: Regioned<Cluster>)}
					<RegionChip region={c.region} />
				{/snippet}
				{#snippet clusterStatusCell(c: Regioned<Cluster>)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(c.status === 'ACTIVE')}">{c.status ?? '—'}</span>
				{/snippet}
				{#snippet clusterCountsCell(c: Regioned<Cluster>)}
					{c.runningTasksCount ?? 0} running / {c.activeServicesCount ?? 0} svcs
				{/snippet}
				{#snippet clusterActionsCell(c: Regioned<Cluster>)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openClusterDetail(c)} title="View" aria-label="View cluster {c.clusterName}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openEditClusterModal(c)} title="Update" aria-label="Update cluster {c.clusterName}" class="text-gray-400 hover:text-purple-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteCluster(c)} title="Delete" aria-label="Delete cluster {c.clusterName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const clusterColumns = defineColumns<Regioned<Cluster>>([
					{ key: 'clusterName', label: 'Name' },
					{ key: 'region', label: 'Region', render: clusterRegionCell },
					{ key: 'status', label: 'Status', render: clusterStatusCell },
					{ key: 'counts', label: 'Tasks / Services', render: clusterCountsCell },
					{ key: 'registeredContainerInstancesCount', label: 'Instances' },
					{ key: 'actions', label: '', render: clusterActionsCell }
				])}
				<DataTable
					rows={filteredClusters}
					rowKey={(c) => c.region + '::' + (c.clusterArn ?? '')}
					columns={clusterColumns}
					loading={tabLoader.isLoading('clusters')}
					emptyMessage="No clusters found"
				/>
				<LoadMore hasMore={!!clustersNextToken} loading={loadingMoreClusters} onLoadMore={loadMoreClusters} />
			{:else if activeTab === 'services'}
				{#snippet svcStatusCell(s: Service)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(s.status === 'ACTIVE')}">{s.status ?? '—'}</span>
				{/snippet}
				{#snippet svcCountsCell(s: Service)}
					{s.runningCount ?? 0}/{s.desiredCount ?? 0} running
				{/snippet}
				{#snippet svcActionsCell(s: Service)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openServiceDetail(s)} title="View" aria-label="View service {s.serviceName}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openEditServiceModal(s)} title="Update" aria-label="Update service {s.serviceName}" class="text-gray-400 hover:text-purple-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteService(s)} title="Delete" aria-label="Delete service {s.serviceName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const serviceColumns = defineColumns<Service>([
					{ key: 'serviceName', label: 'Name' },
					{ key: 'taskDefinition', label: 'Task Definition' },
					{ key: 'status', label: 'Status', render: svcStatusCell },
					{ key: 'counts', label: 'Running/Desired', render: svcCountsCell },
					{ key: 'actions', label: '', render: svcActionsCell }
				])}
				<DataTable
					rows={filteredServices}
					rowKey={(s) => s.serviceArn ?? ''}
					columns={serviceColumns}
					loading={tabLoader.isLoading('services')}
					emptyMessage={selectedClusterArn ? 'No services found in this cluster' : 'Select a cluster to see its services'}
				/>
				<LoadMore hasMore={!!servicesNextToken} loading={loadingMoreServices} onLoadMore={loadMoreServices} />
			{:else if activeTab === 'tasks'}
				{#snippet taskIdCell(t: Task)}
					<span class="font-mono">{taskShortId(t.taskArn)}</span>
				{/snippet}
				{#snippet taskStatusCell(t: Task)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(t.lastStatus === 'RUNNING')}">{t.lastStatus ?? '—'}</span>
				{/snippet}
				{#snippet taskActionsCell(t: Task)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openTaskDetail(t)} title="View" aria-label="View task {taskShortId(t.taskArn)}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleStopTask(t)} title="Stop" aria-label="Stop task {taskShortId(t.taskArn)}" class="text-gray-400 hover:text-red-500"><Square class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const taskColumns = defineColumns<Task>([
					{ key: 'taskArn', label: 'Task', render: taskIdCell },
					{ key: 'taskDefinitionArn', label: 'Task Definition' },
					{ key: 'lastStatus', label: 'Status', render: taskStatusCell },
					{ key: 'launchType', label: 'Launch Type' },
					{ key: 'actions', label: '', render: taskActionsCell }
				])}
				<DataTable
					rows={filteredTasks}
					rowKey={(t) => t.taskArn ?? ''}
					columns={taskColumns}
					loading={tabLoader.isLoading('tasks')}
					emptyMessage={selectedClusterArn ? 'No tasks found in this cluster' : 'Select a cluster to see its tasks'}
				/>
				<LoadMore hasMore={!!tasksNextToken} loading={loadingMoreTasks} onLoadMore={loadMoreTasks} />
			{:else if activeTab === 'taskDefinitions'}
				{#snippet tdFamilyCell(arn: string)}
					{taskDefFamily(arn)}
				{/snippet}
				{#snippet tdRevisionCell(arn: string)}
					rev {taskDefRevision(arn)}
				{/snippet}
				{#snippet tdActionsCell(arn: string)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => selectTaskDef(arn)} title="View" aria-label="View task definition {taskDefFamily(arn)}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeregisterTaskDef(arn)} title="Deregister" aria-label="Deregister task definition {taskDefFamily(arn)}" class="text-gray-400 hover:text-red-500"><Square class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteTaskDef(arn)} title="Delete permanently" aria-label="Permanently delete task definition {taskDefFamily(arn)}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const taskDefColumns = defineColumns<string>([
					{ key: 'family', label: 'Family', render: tdFamilyCell },
					{ key: 'revision', label: 'Revision', render: tdRevisionCell },
					{ key: 'actions', label: '', render: tdActionsCell }
				])}
				<DataTable
					rows={filteredTaskDefArns}
					rowKey={(arn) => arn}
					columns={taskDefColumns}
					loading={tabLoader.isLoading('taskDefinitions')}
					emptyMessage="No task definitions found"
				/>
				<LoadMore hasMore={!!taskDefsNextToken} loading={loadingMoreTaskDefs} onLoadMore={loadMoreTaskDefs} />
			{:else if activeTab === 'containerInstances'}
				{#snippet ciStatusCell(ci: ContainerInstance)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(ci.status === 'ACTIVE')}">{ci.status ?? '—'}</span>
				{/snippet}
				{#snippet ciTasksCell(ci: ContainerInstance)}
					{ci.runningTasksCount ?? 0} running / {ci.pendingTasksCount ?? 0} pending
				{/snippet}
				{#snippet ciAgentCell(ci: ContainerInstance)}
					{ci.agentConnected ? 'Connected' : 'Disconnected'}
				{/snippet}
				{#snippet ciActionsCell(ci: ContainerInstance)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openContainerInstanceDetail(ci)} title="View" aria-label="View container instance {ci.ec2InstanceId}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openDrainModal(ci)} title="Update state" aria-label="Update state for container instance {ci.ec2InstanceId}" class="text-gray-400 hover:text-purple-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleUpdateAgent(ci)} title="Update agent" aria-label="Update agent for container instance {ci.ec2InstanceId}" class="text-gray-400 hover:text-purple-500"><Check class="w-4 h-4" /></button>
						<button onclick={() => handleDeregisterContainerInstance(ci)} title="Deregister" aria-label="Deregister container instance {ci.ec2InstanceId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const ciColumns = defineColumns<ContainerInstance>([
					{ key: 'ec2InstanceId', label: 'EC2 Instance' },
					{ key: 'status', label: 'Status', render: ciStatusCell },
					{ key: 'tasks', label: 'Tasks', render: ciTasksCell },
					{ key: 'agentConnected', label: 'Agent', render: ciAgentCell },
					{ key: 'actions', label: '', render: ciActionsCell }
				])}
				<DataTable
					rows={filteredContainerInstances}
					rowKey={(ci) => ci.containerInstanceArn ?? ''}
					columns={ciColumns}
					loading={tabLoader.isLoading('containerInstances')}
					emptyMessage={selectedClusterArn ? 'No container instances found in this cluster' : 'Select a cluster to see its container instances'}
				/>
				<p class="text-xs text-slate-400 dark:text-slate-500">
					No "Register" action here: RegisterContainerInstance is invoked by the ECS agent
					on instance bootstrap in real AWS usage, and this backend's accepted shape
					(ec2InstanceId) does not match the real API's instanceIdentityDocument-based
					request -- see code comment above.
				</p>
				<LoadMore hasMore={!!containerInstancesNextToken} loading={loadingMoreContainerInstances} onLoadMore={loadMoreContainerInstances} />
			{:else if activeTab === 'capacityProviders'}
				{#snippet cpStatusCell(cp: CapacityProvider)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(cp.status === 'ACTIVE')}">{cp.status ?? '—'}</span>
				{/snippet}
				{#snippet cpAsgCell(cp: CapacityProvider)}
					{cp.autoScalingGroupProvider?.autoScalingGroupArn ?? '—'}
				{/snippet}
				{#snippet cpActionsCell(cp: CapacityProvider)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openCapacityProviderDetail(cp)} title="View" aria-label="View capacity provider {cp.name}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openEditCapacityProviderModal(cp)} title="Update" aria-label="Update capacity provider {cp.name}" class="text-gray-400 hover:text-purple-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteCapacityProvider(cp)} title="Delete" aria-label="Delete capacity provider {cp.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const cpColumns = defineColumns<CapacityProvider>([
					{ key: 'name', label: 'Name' },
					{ key: 'status', label: 'Status', render: cpStatusCell },
					{ key: 'asg', label: 'Auto Scaling Group', render: cpAsgCell },
					{ key: 'actions', label: '', render: cpActionsCell }
				])}
				<DataTable
					rows={filteredCapacityProviders}
					rowKey={(cp) => cp.capacityProviderArn ?? cp.name ?? ''}
					columns={cpColumns}
					loading={tabLoader.isLoading('capacityProviders')}
					emptyMessage="No capacity providers found"
				/>
				<LoadMore hasMore={!!capacityProvidersNextToken} loading={loadingMoreCapacityProviders} onLoadMore={loadMoreCapacityProviders} />
			{:else if activeTab === 'taskSets'}
				{#snippet tsStatusCell(ts: TaskSet)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(ts.status === 'ACTIVE' || ts.status === 'PRIMARY')}">{ts.status ?? '—'}</span>
				{/snippet}
				{#snippet tsScaleCell(ts: TaskSet)}
					{ts.scale?.value ?? 100}{ts.scale?.unit === 'PERCENT' || !ts.scale?.unit ? '%' : ''}
				{/snippet}
				{#snippet tsActionsCell(ts: TaskSet)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openTaskSetDetail(ts)} title="View" aria-label="View task set {ts.id}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openEditTaskSetModal(ts)} title="Update" aria-label="Update task set {ts.id}" class="text-gray-400 hover:text-purple-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleSetPrimaryTaskSet(ts)} title="Set as primary" aria-label="Set task set {ts.id} as primary" class="text-gray-400 hover:text-purple-500"><Check class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteTaskSet(ts)} title="Delete" aria-label="Delete task set {ts.id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const taskSetColumns = defineColumns<TaskSet>([
					{ key: 'id', label: 'ID' },
					{ key: 'taskDefinition', label: 'Task Definition' },
					{ key: 'status', label: 'Status', render: tsStatusCell },
					{ key: 'scale', label: 'Scale', render: tsScaleCell },
					{ key: 'actions', label: '', render: tsActionsCell }
				])}
				<DataTable
					rows={filteredTaskSets}
					rowKey={(ts) => ts.taskSetArn ?? ''}
					columns={taskSetColumns}
					loading={tabLoader.isLoading('taskSets')}
					emptyMessage={taskSetServiceArn ? 'No task sets found for this service' : 'Select a cluster and service to see its task sets'}
				/>
			{:else if activeTab === 'accountSettings'}
				{#snippet asActionsCell(s: Setting)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => handleDeleteSetting(s)} title="Reset to default" aria-label="Reset setting {s.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const settingColumns = defineColumns<Setting>([
					{ key: 'name', label: 'Name' },
					{ key: 'value', label: 'Value' },
					{ key: 'principalArn', label: 'Principal' },
					{ key: 'actions', label: '', render: asActionsCell }
				])}
				<DataTable
					rows={filteredAccountSettings}
					rowKey={(s) => `${s.name ?? ''}:${s.principalArn ?? ''}`}
					columns={settingColumns}
					loading={tabLoader.isLoading('accountSettings')}
					emptyMessage="No account settings found"
				/>
				<LoadMore hasMore={!!accountSettingsNextToken} loading={loadingMoreAccountSettings} onLoadMore={loadMoreAccountSettings} />
			{/if}
		</div>
	</div>
</div>

<!-- ==================== Clusters modals ==================== -->

<Modal bind:this={createClusterModal} title="Create Cluster">
	{#snippet headerAction()}
		<WriteRegionHint />
	{/snippet}
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="cluster-name" class="text-sm text-slate-600 dark:text-slate-300">Cluster name</label>
				<input id="cluster-name" bind:value={newClusterName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newClusterContainerInsights} />
				Enable Container Insights
			</label>
			<div>
				<label for="cluster-cps" class="text-sm text-slate-600 dark:text-slate-300">Capacity providers (comma-separated, optional)</label>
				<input id="cluster-cps" bind:value={newClusterCapacityProviders} placeholder="FARGATE, FARGATE_SPOT" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createClusterError}
				<p class="text-sm text-red-600 dark:text-red-400">{createClusterError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createClusterModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateCluster} disabled={creatingCluster} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingCluster ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editClusterModal} title="Update Cluster">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating <span class="font-medium">{editClusterTarget?.clusterName}</span></p>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={editClusterContainerInsights} />
				Enable Container Insights
			</label>
			<div>
				<label for="edit-cluster-cps" class="text-sm text-slate-600 dark:text-slate-300">Capacity providers (comma-separated)</label>
				<input id="edit-cluster-cps" bind:value={editClusterCapacityProviders} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if updateClusterError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateClusterError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editClusterModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditCluster} disabled={updatingCluster} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{updatingCluster ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={clusterDetailModal} title="Cluster">
	{#snippet children()}
		{#if viewedCluster}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.clusterName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedCluster.clusterArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Capacity providers</dt><dd class="text-slate-900 dark:text-white">{(viewedCluster.capacityProviders ?? []).join(', ') || '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Running tasks</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.runningTasksCount ?? 0}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Pending tasks</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.pendingTasksCount ?? 0}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Active services</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.activeServicesCount ?? 0}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Registered instances</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.registeredContainerInstancesCount ?? 0}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Settings</dt><dd class="text-slate-900 dark:text-white">{(viewedCluster.settings ?? []).map((s) => `${s.name}=${s.value}`).join(', ') || '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Tags</dt><dd class="text-slate-900 dark:text-white">{(viewedCluster.tags ?? []).map((t) => `${t.key}=${t.value}`).join(', ') || '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => clusterDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Services modals ==================== -->

<Modal bind:this={createServiceModal} title="Create Service">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">In cluster <span class="font-medium">{selectedCluster ? clusterShortName(selectedCluster.clusterArn) : '(none selected)'}</span></p>
			<div>
				<label for="svc-name" class="text-sm text-slate-600 dark:text-slate-300">Service name</label>
				<input id="svc-name" bind:value={newServiceName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="svc-taskdef" class="text-sm text-slate-600 dark:text-slate-300">Task definition (family:revision or family)</label>
				<input id="svc-taskdef" bind:value={newServiceTaskDef} placeholder="my-app:3" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="flex gap-3">
				<div class="flex-1">
					<label for="svc-desired" class="text-sm text-slate-600 dark:text-slate-300">Desired count</label>
					<input id="svc-desired" type="number" min="0" bind:value={newServiceDesiredCount} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div class="flex-1">
					<label for="svc-launch" class="text-sm text-slate-600 dark:text-slate-300">Launch type</label>
					<select id="svc-launch" bind:value={newServiceLaunchType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="FARGATE">FARGATE</option>
						<option value="EC2">EC2</option>
						<option value="EXTERNAL">EXTERNAL</option>
					</select>
				</div>
			</div>
			{#if createServiceError}
				<p class="text-sm text-red-600 dark:text-red-400">{createServiceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createServiceModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateService} disabled={creatingService} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingService ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editServiceModal} title="Update Service">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating <span class="font-medium">{editServiceTarget?.serviceName}</span></p>
			<div>
				<label for="edit-svc-desired" class="text-sm text-slate-600 dark:text-slate-300">Desired count</label>
				<input id="edit-svc-desired" type="number" min="0" bind:value={editServiceDesiredCount} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="edit-svc-taskdef" class="text-sm text-slate-600 dark:text-slate-300">Task definition</label>
				<input id="edit-svc-taskdef" bind:value={editServiceTaskDef} class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={editServiceForceDeploy} />
				Force new deployment
			</label>
			{#if updateServiceError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateServiceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editServiceModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditService} disabled={updatingService} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{updatingService ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={serviceDetailModal} title="Service">
	{#snippet children()}
		{#if viewedService}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedService.serviceName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedService.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Task definition</dt><dd class="break-all text-slate-900 dark:text-white">{viewedService.taskDefinition ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Launch type</dt><dd class="text-slate-900 dark:text-white">{viewedService.launchType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Desired / Running / Pending</dt><dd class="text-slate-900 dark:text-white">{viewedService.desiredCount ?? 0} / {viewedService.runningCount ?? 0} / {viewedService.pendingCount ?? 0}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedService.createdAt)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => serviceDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Tasks modals ==================== -->

<Modal bind:this={runTaskModal} title="Run Task">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">In cluster <span class="font-medium">{selectedCluster ? clusterShortName(selectedCluster.clusterArn) : '(none selected)'}</span></p>
			<div>
				<label for="run-taskdef" class="text-sm text-slate-600 dark:text-slate-300">Task definition</label>
				<input id="run-taskdef" bind:value={runTaskDef} placeholder="my-app:3" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="flex gap-3">
				<div class="flex-1">
					<label for="run-count" class="text-sm text-slate-600 dark:text-slate-300">Count</label>
					<input id="run-count" type="number" min="1" bind:value={runTaskCount} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div class="flex-1">
					<label for="run-launch" class="text-sm text-slate-600 dark:text-slate-300">Launch type</label>
					<select id="run-launch" bind:value={runTaskLaunchType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="FARGATE">FARGATE</option>
						<option value="EC2">EC2</option>
						<option value="EXTERNAL">EXTERNAL</option>
					</select>
				</div>
			</div>
			<div>
				<label for="run-startedby" class="text-sm text-slate-600 dark:text-slate-300">Started by (optional)</label>
				<input id="run-startedby" bind:value={runTaskStartedBy} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if runTaskError}
				<p class="text-sm text-red-600 dark:text-red-400">{runTaskError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => runTaskModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitRunTask} disabled={runningTask} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{runningTask ? 'Starting…' : 'Run'}</button>
	{/snippet}
</Modal>

<Modal bind:this={taskDetailModal} title="Task">
	{#snippet children()}
		{#if viewedTask}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Task ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedTask.taskArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Task definition</dt><dd class="break-all text-slate-900 dark:text-white">{viewedTask.taskDefinitionArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedTask.lastStatus ?? '—'} (desired {viewedTask.desiredStatus ?? '—'})</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Launch type</dt><dd class="text-slate-900 dark:text-white">{viewedTask.launchType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Started at</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedTask.startedAt)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Containers</dt><dd class="text-slate-900 dark:text-white">{(viewedTask.containers ?? []).map((c) => c.name).join(', ') || '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => taskDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Task Definitions modals ==================== -->

<Modal bind:this={registerTaskDefModal} title="Register Task Definition">
	{#snippet children()}
		<div class="space-y-3 max-h-[60vh] overflow-y-auto pr-1">
			<div>
				<label for="td-family" class="text-sm text-slate-600 dark:text-slate-300">Family</label>
				<input id="td-family" bind:value={newTaskDefFamily} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="flex gap-3">
				<div class="flex-1">
					<label for="td-cpu" class="text-sm text-slate-600 dark:text-slate-300">CPU units</label>
					<input id="td-cpu" bind:value={newTaskDefCpu} placeholder="256" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div class="flex-1">
					<label for="td-memory" class="text-sm text-slate-600 dark:text-slate-300">Memory (MiB)</label>
					<input id="td-memory" bind:value={newTaskDefMemory} placeholder="512" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			<div>
				<label for="td-network" class="text-sm text-slate-600 dark:text-slate-300">Network mode</label>
				<select id="td-network" bind:value={newTaskDefNetworkMode} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="awsvpc">awsvpc</option>
					<option value="bridge">bridge</option>
					<option value="host">host</option>
					<option value="none">none</option>
				</select>
			</div>
			<div class="flex gap-3">
				<div class="flex-1">
					<label for="td-taskrole" class="text-sm text-slate-600 dark:text-slate-300">Task role ARN (optional)</label>
					<input id="td-taskrole" bind:value={newTaskDefTaskRole} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div class="flex-1">
					<label for="td-exerole" class="text-sm text-slate-600 dark:text-slate-300">Execution role ARN (optional)</label>
					<input id="td-exerole" bind:value={newTaskDefExecutionRole} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			<div class="flex gap-4">
				<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
					<input type="checkbox" bind:checked={newTaskDefRequiresFargate} /> FARGATE
				</label>
				<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
					<input type="checkbox" bind:checked={newTaskDefRequiresEc2} /> EC2
				</label>
			</div>

			<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
				<div class="flex items-center justify-between mb-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-200">Container definitions</p>
					<button type="button" onclick={addContainerDef} class="text-xs text-purple-600 dark:text-purple-400 hover:underline">+ Add container</button>
				</div>
				<p class="text-xs text-slate-400 dark:text-slate-500 mb-2">
					Only name, image, cpu, memory, essential, entry point, command, environment, and
					port mappings round-trip against this backend -- richer fields (links, ulimits,
					linux parameters, health checks, etc.) are silently dropped, see code comment.
				</p>
				{#each newTaskDefContainers as cdef, i (cdef.key)}
					<div class="mb-3 p-3 rounded-lg border border-slate-200 dark:border-slate-600 space-y-2">
						<div class="flex items-center justify-between">
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400">Container {i + 1}</p>
							{#if newTaskDefContainers.length > 1}
								<button type="button" onclick={() => removeContainerDef(i)} class="text-xs text-red-500 hover:underline">Remove</button>
							{/if}
						</div>
						<div class="flex gap-2">
							<input placeholder="name" bind:value={cdef.name} class="flex-1 px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							<input placeholder="image" bind:value={cdef.image} class="flex-1 px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						</div>
						<div class="flex gap-2">
							<input placeholder="cpu" bind:value={cdef.cpu} class="w-20 px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							<input placeholder="memory" bind:value={cdef.memory} class="w-24 px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							<label class="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400">
								<input type="checkbox" bind:checked={cdef.essential} /> essential
							</label>
						</div>
						<input placeholder="entry point (comma-separated)" bind:value={cdef.entryPoint} class="w-full px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input placeholder="command (comma-separated)" bind:value={cdef.command} class="w-full px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input placeholder="port mappings, e.g. 80:8080, 443:8443" bind:value={cdef.portMappings} class="w-full px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<textarea placeholder="environment, one KEY=value per line" bind:value={cdef.environment} rows="2" class="w-full px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
					</div>
				{/each}
			</div>
			{#if registerTaskDefError}
				<p class="text-sm text-red-600 dark:text-red-400">{registerTaskDefError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => registerTaskDefModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitRegisterTaskDef} disabled={registeringTaskDef} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{registeringTaskDef ? 'Registering…' : 'Register'}</button>
	{/snippet}
</Modal>

<Modal bind:this={taskDefDetailModal} title="Task Definition">
	{#snippet children()}
		{#if taskDefDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if selectedTaskDef}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Family</dt><dd class="text-slate-900 dark:text-white">{selectedTaskDef.family ?? '—'} (rev {selectedTaskDef.revision ?? '—'})</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{selectedTaskDef.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">CPU / Memory</dt><dd class="text-slate-900 dark:text-white">{selectedTaskDef.cpu ?? '—'} / {selectedTaskDef.memory ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Network mode</dt><dd class="text-slate-900 dark:text-white">{selectedTaskDef.networkMode ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Containers</dt>
					<dd class="text-slate-900 dark:text-white space-y-1">
						{#each selectedTaskDef.containerDefinitions ?? [] as cdef (cdef.name)}
							<div class="text-xs">{cdef.name}: {cdef.image} (cpu {cdef.cpu ?? '—'}, mem {cdef.memory ?? '—'}, essential {cdef.essential ? 'yes' : 'no'})</div>
						{/each}
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => taskDefDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Container Instances modals ==================== -->

<Modal bind:this={drainModal} title="Update Container Instance State">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Instance <span class="font-medium">{drainTarget?.ec2InstanceId}</span></p>
			<div>
				<label for="drain-status" class="text-sm text-slate-600 dark:text-slate-300">Status</label>
				<select id="drain-status" bind:value={drainStatus} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="ACTIVE">ACTIVE</option>
					<option value="DRAINING">DRAINING</option>
				</select>
			</div>
			{#if drainError}
				<p class="text-sm text-red-600 dark:text-red-400">{drainError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => drainModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitDrain} disabled={draining} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{draining ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={containerInstanceDetailModal} title="Container Instance">
	{#snippet children()}
		{#if viewedContainerInstance}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">EC2 instance</dt><dd class="text-slate-900 dark:text-white">{viewedContainerInstance.ec2InstanceId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedContainerInstance.containerInstanceArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedContainerInstance.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Agent</dt><dd class="text-slate-900 dark:text-white">{viewedContainerInstance.agentConnected ? 'Connected' : 'Disconnected'} ({viewedContainerInstance.agentUpdateStatus ?? 'up to date'})</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Running / Pending tasks</dt><dd class="text-slate-900 dark:text-white">{viewedContainerInstance.runningTasksCount ?? 0} / {viewedContainerInstance.pendingTasksCount ?? 0}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Registered</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedContainerInstance.registeredAt)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => containerInstanceDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Capacity Providers modals ==================== -->

<Modal bind:this={createCapacityProviderModal} title="Create Capacity Provider">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="cp-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="cp-name" bind:value={newCpName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="cp-asg" class="text-sm text-slate-600 dark:text-slate-300">Auto Scaling group ARN</label>
				<input id="cp-asg" bind:value={newCpAsgArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="flex gap-3">
				<div class="flex-1">
					<label for="cp-scaling-status" class="text-sm text-slate-600 dark:text-slate-300">Managed scaling</label>
					<select id="cp-scaling-status" bind:value={newCpManagedScalingStatus} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="ENABLED">ENABLED</option>
						<option value="DISABLED">DISABLED</option>
					</select>
				</div>
				<div class="flex-1">
					<label for="cp-target" class="text-sm text-slate-600 dark:text-slate-300">Target capacity %</label>
					<input id="cp-target" type="number" min="1" max="100" bind:value={newCpTargetCapacity} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			<div class="flex gap-3">
				<div class="flex-1">
					<label for="cp-termination" class="text-sm text-slate-600 dark:text-slate-300">Managed termination protection</label>
					<select id="cp-termination" bind:value={newCpManagedTermination} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="ENABLED">ENABLED</option>
						<option value="DISABLED">DISABLED</option>
					</select>
				</div>
				<div class="flex-1">
					<label for="cp-draining" class="text-sm text-slate-600 dark:text-slate-300">Managed draining</label>
					<select id="cp-draining" bind:value={newCpManagedDraining} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="ENABLED">ENABLED</option>
						<option value="DISABLED">DISABLED</option>
					</select>
				</div>
			</div>
			{#if createCapacityProviderError}
				<p class="text-sm text-red-600 dark:text-red-400">{createCapacityProviderError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createCapacityProviderModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateCapacityProvider} disabled={creatingCapacityProvider} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingCapacityProvider ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editCapacityProviderModal} title="Update Capacity Provider">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating <span class="font-medium">{editCpTarget?.name}</span></p>
			<div class="flex gap-3">
				<div class="flex-1">
					<label for="edit-cp-scaling-status" class="text-sm text-slate-600 dark:text-slate-300">Managed scaling</label>
					<select id="edit-cp-scaling-status" bind:value={editCpManagedScalingStatus} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="ENABLED">ENABLED</option>
						<option value="DISABLED">DISABLED</option>
					</select>
				</div>
				<div class="flex-1">
					<label for="edit-cp-target" class="text-sm text-slate-600 dark:text-slate-300">Target capacity %</label>
					<input id="edit-cp-target" type="number" min="1" max="100" bind:value={editCpTargetCapacity} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			<div class="flex gap-3">
				<div class="flex-1">
					<label for="edit-cp-termination" class="text-sm text-slate-600 dark:text-slate-300">Managed termination protection</label>
					<select id="edit-cp-termination" bind:value={editCpManagedTermination} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="ENABLED">ENABLED</option>
						<option value="DISABLED">DISABLED</option>
					</select>
				</div>
				<div class="flex-1">
					<label for="edit-cp-draining" class="text-sm text-slate-600 dark:text-slate-300">Managed draining</label>
					<select id="edit-cp-draining" bind:value={editCpManagedDraining} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="ENABLED">ENABLED</option>
						<option value="DISABLED">DISABLED</option>
					</select>
				</div>
			</div>
			{#if updateCapacityProviderError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateCapacityProviderError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editCapacityProviderModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditCapacityProvider} disabled={updatingCapacityProvider} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{updatingCapacityProvider ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={capacityProviderDetailModal} title="Capacity Provider">
	{#snippet children()}
		{#if viewedCapacityProvider}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedCapacityProvider.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedCapacityProvider.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Auto Scaling group</dt><dd class="break-all text-slate-900 dark:text-white">{viewedCapacityProvider.autoScalingGroupProvider?.autoScalingGroupArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Managed scaling</dt><dd class="text-slate-900 dark:text-white">{viewedCapacityProvider.autoScalingGroupProvider?.managedScaling?.status ?? '—'} (target {viewedCapacityProvider.autoScalingGroupProvider?.managedScaling?.targetCapacity ?? '—'}%)</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Managed termination protection</dt><dd class="text-slate-900 dark:text-white">{viewedCapacityProvider.autoScalingGroupProvider?.managedTerminationProtection ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Managed draining</dt><dd class="text-slate-900 dark:text-white">{viewedCapacityProvider.autoScalingGroupProvider?.managedDraining ?? '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => capacityProviderDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Task Sets modals ==================== -->

<Modal bind:this={createTaskSetModal} title="Create Task Set">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For service <span class="font-medium">{services.find((s) => s.serviceArn === taskSetServiceArn)?.serviceName ?? '(none selected)'}</span></p>
			<div>
				<label for="ts-taskdef" class="text-sm text-slate-600 dark:text-slate-300">Task definition</label>
				<input id="ts-taskdef" bind:value={newTaskSetTaskDef} placeholder="my-app:3" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ts-external-id" class="text-sm text-slate-600 dark:text-slate-300">External ID (optional)</label>
				<input id="ts-external-id" bind:value={newTaskSetExternalId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="flex gap-3">
				<div class="flex-1">
					<label for="ts-launch" class="text-sm text-slate-600 dark:text-slate-300">Launch type</label>
					<select id="ts-launch" bind:value={newTaskSetLaunchType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="FARGATE">FARGATE</option>
						<option value="EC2">EC2</option>
						<option value="EXTERNAL">EXTERNAL</option>
					</select>
				</div>
				<div class="flex-1">
					<label for="ts-scale" class="text-sm text-slate-600 dark:text-slate-300">Scale %</label>
					<input id="ts-scale" type="number" min="0" max="100" bind:value={newTaskSetScaleValue} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			{#if createTaskSetError}
				<p class="text-sm text-red-600 dark:text-red-400">{createTaskSetError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createTaskSetModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateTaskSet} disabled={creatingTaskSet} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingTaskSet ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editTaskSetModal} title="Update Task Set">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating <span class="font-medium">{editTaskSetTarget?.id}</span></p>
			<div>
				<label for="edit-ts-scale" class="text-sm text-slate-600 dark:text-slate-300">Scale %</label>
				<input id="edit-ts-scale" type="number" min="0" max="100" bind:value={editTaskSetScaleValue} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if updateTaskSetError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateTaskSetError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editTaskSetModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditTaskSet} disabled={updatingTaskSet} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{updatingTaskSet ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={taskSetDetailModal} title="Task Set">
	{#snippet children()}
		{#if viewedTaskSet}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedTaskSet.id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedTaskSet.taskSetArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedTaskSet.status ?? '—'} (stability {viewedTaskSet.stabilityStatus ?? '—'})</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Task definition</dt><dd class="break-all text-slate-900 dark:text-white">{viewedTaskSet.taskDefinition ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Launch type</dt><dd class="text-slate-900 dark:text-white">{viewedTaskSet.launchType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Scale</dt><dd class="text-slate-900 dark:text-white">{viewedTaskSet.scale?.value ?? 100}{viewedTaskSet.scale?.unit === 'PERCENT' || !viewedTaskSet.scale?.unit ? '%' : ''}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => taskSetDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Account Settings modals ==================== -->

<Modal bind:this={putSettingModal} title="Put Account Setting">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-xs text-slate-400 dark:text-slate-500">
				The real API has no separate create vs. update for account settings --
				Put always upserts the (name, principal) pair.
			</p>
			<div>
				<label for="setting-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="setting-name" bind:value={newSettingName} placeholder="containerInsights" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="setting-value" class="text-sm text-slate-600 dark:text-slate-300">Value</label>
				<input id="setting-value" bind:value={newSettingValue} placeholder="enabled" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if !newSettingAsDefault}
				<div>
					<label for="setting-principal" class="text-sm text-slate-600 dark:text-slate-300">Principal ARN (optional; defaults to the authenticated user)</label>
					<input id="setting-principal" bind:value={newSettingPrincipalArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newSettingAsDefault} />
				Set as the account-wide default instead (PutAccountSettingDefault)
			</label>
			{#if putSettingError}
				<p class="text-sm text-red-600 dark:text-red-400">{putSettingError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => putSettingModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitPutSetting} disabled={puttingSetting} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{puttingSetting ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
