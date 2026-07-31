<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getEMRClient } from '$lib/aws-client';
	import {
		ListClustersCommand,
		DescribeClusterCommand,
		RunJobFlowCommand,
		TerminateJobFlowsCommand,
		ModifyClusterCommand,
		SetTerminationProtectionCommand,
		SetVisibleToAllUsersCommand,
		SetKeepJobFlowAliveWhenNoStepsCommand,
		SetUnhealthyNodeReplacementCommand,
		ListStepsCommand,
		AddJobFlowStepsCommand,
		CancelStepsCommand,
		DescribeStepCommand,
		ListInstanceGroupsCommand,
		AddInstanceGroupsCommand,
		ModifyInstanceGroupsCommand,
		ListInstanceFleetsCommand,
		AddInstanceFleetCommand,
		ModifyInstanceFleetCommand,
		ListInstancesCommand,
		ListBootstrapActionsCommand,
		ListSecurityConfigurationsCommand,
		CreateSecurityConfigurationCommand,
		DeleteSecurityConfigurationCommand,
		DescribeSecurityConfigurationCommand,
		ListStudiosCommand,
		CreateStudioCommand,
		UpdateStudioCommand,
		DeleteStudioCommand,
		DescribeStudioCommand,
		ListNotebookExecutionsCommand,
		StartNotebookExecutionCommand,
		StopNotebookExecutionCommand,
		DescribeNotebookExecutionCommand,
		type ClusterSummary,
		type Cluster,
		type StepSummary,
		type Step,
		type Instance,
		type InstanceFleet,
		type InstanceGroup,
		type StudioSummary,
		type Studio,
		type NotebookExecutionSummary,
		type NotebookExecution,
		type SecurityConfigurationSummary,
		type Command as BootstrapCommand,
		type InstanceGroupConfig
	} from '@aws-sdk/client-emr';
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
	import { Database, Plus, Trash2, Eye, Pencil, XCircle, StopCircle } from 'lucide-svelte';

	const client = regionalClient(getEMRClient);

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

	// Replaces the JSON.stringify(x).toLowerCase().includes(...) antipattern:
	// search only over the named fields that are actually meaningful for each
	// resource.
	function matches(q: string, ...vals: (string | undefined)[]): boolean {
		if (!q) return true;
		const needle = q.toLowerCase();
		return vals.some((v) => (v ?? '').toLowerCase().includes(needle));
	}

	// Parses "KEY=VALUE" lines (one tag per line) into the Tag[] shape RunJobFlow/CreateStudio expect.
	function parseTagLines(s: string): { Key: string; Value: string }[] {
		return s
			.split('\n')
			.map((line) => line.trim())
			.filter((line) => line.length > 0)
			.map((line) => {
				const eq = line.indexOf('=');
				return eq === -1
					? { Key: line, Value: '' }
					: { Key: line.slice(0, eq).trim(), Value: line.slice(eq + 1).trim() };
			});
	}

	function parseCommaList(s: string): string[] {
		return s
			.split(',')
			.map((x) => x.trim())
			.filter((x) => x.length > 0);
	}

	type TabId =
		| 'clusters'
		| 'steps'
		| 'instanceGroups'
		| 'instanceFleets'
		| 'instances'
		| 'bootstrapActions'
		| 'securityConfigurations'
		| 'studios'
		| 'notebookExecutions';

	// NOTE (backend wire-shape bug, not fixed here -- this pass is UI-only):
	// services/emr/models.go's ClusterSummary struct has a ReleaseLabel field
	// that the real @aws-sdk/client-emr ClusterSummary type does NOT define
	// (confirmed against the installed SDK's compiled types: real
	// ClusterSummary only has Id/Name/Status/NormalizedInstanceHours/
	// ClusterArn/OutpostArn). ListClusters therefore emits a field a real
	// client's typed model has no slot for -- caught here because
	// `svelte-check` rejected `c.ReleaseLabel` against the real
	// `ClusterSummary` type. The Clusters table below uses the real
	// NormalizedInstanceHours field instead (which this backend simply never
	// populates -- an acceptable "optional field the client sees as
	// undefined" omission, unlike the fabricated ReleaseLabel).
	const tabs: TabDef[] = [
		{ id: 'clusters', label: 'Clusters' },
		{ id: 'steps', label: 'Steps' },
		{ id: 'instanceGroups', label: 'Instance Groups' },
		{ id: 'instanceFleets', label: 'Instance Fleets' },
		{ id: 'instances', label: 'Instances' },
		{ id: 'bootstrapActions', label: 'Bootstrap Actions' },
		{ id: 'securityConfigurations', label: 'Security Configurations' },
		{ id: 'studios', label: 'Studios' },
		{ id: 'notebookExecutions', label: 'Notebook Executions' }
	];

	// Steps, Instance Groups, Instance Fleets, Instances and Bootstrap Actions
	// all belong to exactly one cluster on the real API (ListSteps,
	// ListInstanceGroups, ListInstanceFleets, ListInstances and
	// ListBootstrapActions all require a ClusterId) -- the same
	// parent-selector pattern accessanalyzer uses for its analyzer-scoped
	// tabs and directoryservice uses for its directory-scoped tabs.
	// Security Configurations, Studios and Notebook Executions are
	// account/region-level resources with no cluster relationship in their
	// List operations, so they are NOT cluster-scoped.
	const clusterScopedTabs: TabId[] = [
		'steps',
		'instanceGroups',
		'instanceFleets',
		'instances',
		'bootstrapActions'
	];

	let activeTab = $state<TabId>('clusters');
	let searchQuery = $state('');

	let selectedClusterId = $state('');

	let clusters = $state<ClusterSummary[]>([]);
	let clustersMarker = $state<string | undefined>();
	let loadingMoreClusters = $state(false);

	let steps = $state<StepSummary[]>([]);
	let stepsMarker = $state<string | undefined>();
	let loadingMoreSteps = $state(false);

	let instanceGroups = $state<InstanceGroup[]>([]);
	let instanceGroupsMarker = $state<string | undefined>();
	let loadingMoreInstanceGroups = $state(false);

	let instanceFleets = $state<InstanceFleet[]>([]);
	let instanceFleetsMarker = $state<string | undefined>();
	let loadingMoreInstanceFleets = $state(false);

	let instances = $state<Instance[]>([]);
	let instancesMarker = $state<string | undefined>();
	let loadingMoreInstances = $state(false);

	let bootstrapActions = $state<BootstrapCommand[]>([]);
	let bootstrapActionsMarker = $state<string | undefined>();
	let loadingMoreBootstrapActions = $state(false);

	let securityConfigs = $state<SecurityConfigurationSummary[]>([]);
	let securityConfigsMarker = $state<string | undefined>();
	let loadingMoreSecurityConfigs = $state(false);

	let studios = $state<StudioSummary[]>([]);
	let studiosMarker = $state<string | undefined>();
	let loadingMoreStudios = $state(false);

	let notebookExecutions = $state<NotebookExecutionSummary[]>([]);
	let notebookExecutionsMarker = $state<string | undefined>();
	let loadingMoreNotebookExecutions = $state(false);

	async function fetchClusters(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListClustersCommand({ Marker: reset ? undefined : clustersMarker })
		);
		clusters = reset ? (resp.Clusters ?? []) : [...clusters, ...(resp.Clusters ?? [])];
		clustersMarker = resp.Marker;
		if (!selectedClusterId && clusters.length > 0) {
			selectedClusterId = clusters[0].Id ?? '';
		}
	}

	async function fetchSteps(reset: boolean): Promise<void> {
		if (!selectedClusterId) {
			steps = [];
			stepsMarker = undefined;
			return;
		}
		const resp = await client().send(
			new ListStepsCommand({
				ClusterId: selectedClusterId,
				Marker: reset ? undefined : stepsMarker
			})
		);
		steps = reset ? (resp.Steps ?? []) : [...steps, ...(resp.Steps ?? [])];
		stepsMarker = resp.Marker;
	}

	async function fetchInstanceGroups(reset: boolean): Promise<void> {
		if (!selectedClusterId) {
			instanceGroups = [];
			instanceGroupsMarker = undefined;
			return;
		}
		const resp = await client().send(
			new ListInstanceGroupsCommand({ ClusterId: selectedClusterId })
		);
		instanceGroups = resp.InstanceGroups ?? [];
		instanceGroupsMarker = resp.Marker;
	}

	async function fetchInstanceFleets(reset: boolean): Promise<void> {
		if (!selectedClusterId) {
			instanceFleets = [];
			instanceFleetsMarker = undefined;
			return;
		}
		const resp = await client().send(
			new ListInstanceFleetsCommand({ ClusterId: selectedClusterId })
		);
		instanceFleets = resp.InstanceFleets ?? [];
		instanceFleetsMarker = resp.Marker;
	}

	async function fetchInstances(reset: boolean): Promise<void> {
		if (!selectedClusterId) {
			instances = [];
			instancesMarker = undefined;
			return;
		}
		const resp = await client().send(
			new ListInstancesCommand({
				ClusterId: selectedClusterId,
				Marker: reset ? undefined : instancesMarker
			})
		);
		instances = reset ? (resp.Instances ?? []) : [...instances, ...(resp.Instances ?? [])];
		instancesMarker = resp.Marker;
	}

	async function fetchBootstrapActions(reset: boolean): Promise<void> {
		if (!selectedClusterId) {
			bootstrapActions = [];
			bootstrapActionsMarker = undefined;
			return;
		}
		const resp = await client().send(
			new ListBootstrapActionsCommand({
				ClusterId: selectedClusterId,
				Marker: reset ? undefined : bootstrapActionsMarker
			})
		);
		bootstrapActions = reset
			? (resp.BootstrapActions ?? [])
			: [...bootstrapActions, ...(resp.BootstrapActions ?? [])];
		bootstrapActionsMarker = resp.Marker;
	}

	async function fetchSecurityConfigs(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListSecurityConfigurationsCommand({ Marker: reset ? undefined : securityConfigsMarker })
		);
		securityConfigs = reset
			? (resp.SecurityConfigurations ?? [])
			: [...securityConfigs, ...(resp.SecurityConfigurations ?? [])];
		securityConfigsMarker = resp.Marker;
	}

	async function fetchStudios(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListStudiosCommand({ Marker: reset ? undefined : studiosMarker })
		);
		studios = reset ? (resp.Studios ?? []) : [...studios, ...(resp.Studios ?? [])];
		studiosMarker = resp.Marker;
	}

	async function fetchNotebookExecutions(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListNotebookExecutionsCommand({
				Marker: reset ? undefined : notebookExecutionsMarker
			})
		);
		notebookExecutions = reset
			? (resp.NotebookExecutions ?? [])
			: [...notebookExecutions, ...(resp.NotebookExecutions ?? [])];
		notebookExecutionsMarker = resp.Marker;
	}

	const tabLoader = createTabLoader<TabId>({
		clusters: () => fetchClusters(true).catch(rethrowDescribed),
		steps: () => fetchSteps(true).catch(rethrowDescribed),
		instanceGroups: () => fetchInstanceGroups(true).catch(rethrowDescribed),
		instanceFleets: () => fetchInstanceFleets(true).catch(rethrowDescribed),
		instances: () => fetchInstances(true).catch(rethrowDescribed),
		bootstrapActions: () => fetchBootstrapActions(true).catch(rethrowDescribed),
		securityConfigurations: () => fetchSecurityConfigs(true).catch(rethrowDescribed),
		studios: () => fetchStudios(true).catch(rethrowDescribed),
		notebookExecutions: () => fetchNotebookExecutions(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	function onClusterSelect(id: string): void {
		selectedClusterId = id;
		if (clusterScopedTabs.includes(activeTab)) {
			tabLoader.refresh(activeTab);
		}
	}

	// Clusters is the parent resource for the five cluster-scoped tabs: on a
	// region change the previously selected cluster ID belongs to the old
	// region and must not be reused, so reload clusters first (which
	// re-selects a cluster for the new region) before reloading whichever tab
	// is active.
	onRegionChange(() => {
		selectedClusterId = '';
		clusters = [];
		clustersMarker = undefined;
		void tabLoader.refresh('clusters').then(() => {
			if (activeTab !== 'clusters') {
				tabLoader.refresh(activeTab);
			}
		});
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredClusters = $derived(
		clusters.filter((c) => matches(searchQuery, c.Name, c.Id, c.Status?.State))
	);
	const filteredSteps = $derived(
		steps.filter((s) => matches(searchQuery, s.Name, s.Id, s.Status?.State))
	);
	const filteredInstanceGroups = $derived(
		instanceGroups.filter((g) =>
			matches(searchQuery, g.Name, g.Id, g.InstanceGroupType, g.InstanceType, g.Market)
		)
	);
	const filteredInstanceFleets = $derived(
		instanceFleets.filter((f) => matches(searchQuery, f.Name, f.Id, f.InstanceFleetType))
	);
	const filteredInstances = $derived(
		instances.filter((i) =>
			matches(searchQuery, i.Ec2InstanceId, i.Id, i.InstanceType, i.PrivateIpAddress, i.PublicIpAddress)
		)
	);
	const filteredBootstrapActions = $derived(
		bootstrapActions.filter((b) => matches(searchQuery, b.Name, b.ScriptPath))
	);
	const filteredSecurityConfigs = $derived(
		securityConfigs.filter((s) => matches(searchQuery, s.Name))
	);
	const filteredStudios = $derived(
		studios.filter((s) => matches(searchQuery, s.Name, s.StudioId, s.VpcId))
	);
	const filteredNotebookExecutions = $derived(
		notebookExecutions.filter((n) =>
			matches(searchQuery, n.NotebookExecutionName, n.NotebookExecutionId, n.EditorId, n.Status)
		)
	);

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
	async function loadMoreSteps(): Promise<void> {
		loadingMoreSteps = true;
		try {
			await fetchSteps(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSteps = false;
		}
	}
	async function loadMoreInstanceGroups(): Promise<void> {
		loadingMoreInstanceGroups = true;
		try {
			await fetchInstanceGroups(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreInstanceGroups = false;
		}
	}
	async function loadMoreInstanceFleets(): Promise<void> {
		loadingMoreInstanceFleets = true;
		try {
			await fetchInstanceFleets(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreInstanceFleets = false;
		}
	}
	async function loadMoreInstances(): Promise<void> {
		loadingMoreInstances = true;
		try {
			await fetchInstances(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreInstances = false;
		}
	}
	async function loadMoreBootstrapActions(): Promise<void> {
		loadingMoreBootstrapActions = true;
		try {
			await fetchBootstrapActions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreBootstrapActions = false;
		}
	}
	async function loadMoreSecurityConfigs(): Promise<void> {
		loadingMoreSecurityConfigs = true;
		try {
			await fetchSecurityConfigs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSecurityConfigs = false;
		}
	}
	async function loadMoreStudios(): Promise<void> {
		loadingMoreStudios = true;
		try {
			await fetchStudios(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreStudios = false;
		}
	}
	async function loadMoreNotebookExecutions(): Promise<void> {
		loadingMoreNotebookExecutions = true;
		try {
			await fetchNotebookExecutions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreNotebookExecutions = false;
		}
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// ==================== Clusters: launch (RunJobFlow) / terminate / edit settings / detail ====================
	//
	// EMR's real verbs are RunJobFlow (launch) and TerminateJobFlows (shut
	// down) -- not "create"/"delete". RunJobFlow takes a large nested
	// Instances structure; the backend (services/emr/clusters.go
	// buildNewCluster) genuinely stores Instances.InstanceGroups (or
	// InstanceFleets, mutually exclusive -- fleets win if both are set),
	// Ec2KeyName/Ec2SubnetId/EmrManagedMasterSecurityGroup/
	// EmrManagedSlaveSecurityGroup/ServiceAccessSecurityGroup/
	// AdditionalMaster|SlaveSecurityGroups/Ec2SubnetIds, and
	// KeepJobFlowAliveWhenNoSteps/TerminationProtected. It silently drops the
	// legacy uniform-config shortcut (MasterInstanceType/SlaveInstanceType/
	// InstanceCount), the deprecated HadoopVersion, Instances.Placement (EC2
	// AZ placement -- distinct from the top-level PlacementGroupConfigs,
	// which DOES round-trip), and Instances.UnhealthyNodeReplacement (there
	// is no matching field on RunJobFlowInstances at all -- only the
	// standalone SetUnhealthyNodeReplacement operation can set it, after
	// creation). This form only builds the InstanceGroups shape, which is
	// what genuinely round-trips.
	let createClusterModal = $state<Modal | null>(null);
	let creatingCluster = $state(false);
	let createClusterError = $state<string | null>(null);
	let newClusterName = $state('');
	let newClusterReleaseLabel = $state('emr-7.3.0');
	let newClusterLogUri = $state('');
	let newClusterServiceRole = $state('EMR_DefaultRole');
	let newClusterJobFlowRole = $state('EMR_EC2_DefaultRole');
	let newMasterInstanceType = $state('m5.xlarge');
	let newCoreInstanceType = $state('m5.xlarge');
	let newCoreInstanceCount = $state(2);
	let newClusterKeepAlive = $state(true);
	let newClusterTerminationProtected = $state(false);
	let newClusterVisibleToAllUsers = $state(true);
	let newClusterTags = $state('');

	function openCreateClusterModal(): void {
		createClusterError = null;
		newClusterName = '';
		newClusterReleaseLabel = 'emr-7.3.0';
		newClusterLogUri = '';
		newClusterServiceRole = 'EMR_DefaultRole';
		newClusterJobFlowRole = 'EMR_EC2_DefaultRole';
		newMasterInstanceType = 'm5.xlarge';
		newCoreInstanceType = 'm5.xlarge';
		newCoreInstanceCount = 2;
		newClusterKeepAlive = true;
		newClusterTerminationProtected = false;
		newClusterVisibleToAllUsers = true;
		newClusterTags = '';
		createClusterModal?.open();
	}

	async function submitCreateCluster(): Promise<void> {
		if (!newClusterName.trim()) {
			createClusterError = 'Name is required.';
			return;
		}
		creatingCluster = true;
		createClusterError = null;
		try {
			const instanceGroupsInput: InstanceGroupConfig[] = [
				{
					Name: 'Master',
					InstanceRole: 'MASTER',
					Market: 'ON_DEMAND',
					InstanceType: newMasterInstanceType.trim(),
					InstanceCount: 1
				}
			];
			if (newCoreInstanceCount > 0) {
				instanceGroupsInput.push({
					Name: 'Core',
					InstanceRole: 'CORE',
					Market: 'ON_DEMAND',
					InstanceType: newCoreInstanceType.trim(),
					InstanceCount: newCoreInstanceCount
				});
			}
			const tags = parseTagLines(newClusterTags);
			await client().send(
				new RunJobFlowCommand({
					Name: newClusterName.trim(),
					ReleaseLabel: newClusterReleaseLabel.trim() || undefined,
					LogUri: newClusterLogUri.trim() || undefined,
					ServiceRole: newClusterServiceRole.trim() || undefined,
					JobFlowRole: newClusterJobFlowRole.trim() || undefined,
					Tags: tags.length > 0 ? tags : undefined,
					VisibleToAllUsers: newClusterVisibleToAllUsers,
					Instances: {
						InstanceGroups: instanceGroupsInput,
						KeepJobFlowAliveWhenNoSteps: newClusterKeepAlive,
						TerminationProtected: newClusterTerminationProtected
					}
				})
			);
			toast.success(`Cluster "${newClusterName}" launch started`);
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

	async function handleTerminateCluster(c: ClusterSummary): Promise<void> {
		if (!c.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Terminate cluster',
			message: `Terminate EMR cluster "${c.Name ?? c.Id}"? All running steps will be aborted.`,
			confirmLabel: 'Terminate'
		});
		if (!confirmed) return;
		try {
			await client().send(new TerminateJobFlowsCommand({ JobFlowIds: [c.Id] }));
			toast.success(`Cluster "${c.Name ?? c.Id}" termination started`);
			if (selectedClusterId === c.Id) {
				selectedClusterId = '';
			}
			await tabLoader.refresh('clusters');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// Edit settings composes the five real per-cluster mutate operations this
	// backend implements (ModifyCluster for StepConcurrencyLevel, plus the
	// four boolean Set* toggles) into one modal/submit, since all five are
	// scoped to JobFlowIds/ClusterId and none has a dedicated "detail" of its
	// own to justify a separate modal per operation.
	let editClusterModal = $state<Modal | null>(null);
	let editingCluster = $state(false);
	let editClusterError = $state<string | null>(null);
	let editClusterTarget = $state<ClusterSummary | null>(null);
	let editStepConcurrency = $state(1);
	let editTerminationProtected = $state(false);
	let editVisibleToAllUsers = $state(true);
	let editKeepAlive = $state(true);
	let editUnhealthyNodeReplacement = $state(false);

	async function openEditClusterModal(c: ClusterSummary): Promise<void> {
		if (!c.Id) return;
		editClusterTarget = c;
		editClusterError = null;
		editClusterModal?.open();
		try {
			const resp = await client().send(new DescribeClusterCommand({ ClusterId: c.Id }));
			const cluster = resp.Cluster;
			editStepConcurrency = cluster?.StepConcurrencyLevel ?? 1;
			editTerminationProtected = cluster?.TerminationProtected ?? false;
			editVisibleToAllUsers = cluster?.VisibleToAllUsers ?? true;
			editKeepAlive = !(cluster?.AutoTerminate ?? false);
			editUnhealthyNodeReplacement = cluster?.UnhealthyNodeReplacement ?? false;
		} catch (e) {
			editClusterError = describeError(e);
		}
	}

	async function submitEditCluster(): Promise<void> {
		const id = editClusterTarget?.Id;
		if (!id) return;
		editingCluster = true;
		editClusterError = null;
		try {
			await Promise.all([
				client().send(
					new ModifyClusterCommand({ ClusterId: id, StepConcurrencyLevel: editStepConcurrency })
				),
				client().send(
					new SetTerminationProtectionCommand({
						JobFlowIds: [id],
						TerminationProtected: editTerminationProtected
					})
				),
				client().send(
					new SetVisibleToAllUsersCommand({
						JobFlowIds: [id],
						VisibleToAllUsers: editVisibleToAllUsers
					})
				),
				client().send(
					new SetKeepJobFlowAliveWhenNoStepsCommand({
						JobFlowIds: [id],
						KeepJobFlowAliveWhenNoSteps: editKeepAlive
					})
				),
				client().send(
					new SetUnhealthyNodeReplacementCommand({
						JobFlowIds: [id],
						UnhealthyNodeReplacement: editUnhealthyNodeReplacement
					})
				)
			]);
			toast.success('Cluster settings updated');
			editClusterModal?.close();
			await tabLoader.refresh('clusters');
		} catch (e) {
			const msg = describeError(e);
			editClusterError = msg;
			toast.error(msg);
		} finally {
			editingCluster = false;
		}
	}

	let clusterDetailModal = $state<Modal | null>(null);
	let viewedCluster = $state<Cluster | null>(null);
	let clusterDetailLoading = $state(false);
	let clusterDetailError = $state<string | null>(null);

	async function openClusterDetail(c: ClusterSummary): Promise<void> {
		if (!c.Id) return;
		viewedCluster = null;
		clusterDetailError = null;
		clusterDetailModal?.open();
		clusterDetailLoading = true;
		try {
			const resp = await client().send(new DescribeClusterCommand({ ClusterId: c.Id }));
			viewedCluster = resp.Cluster ?? null;
		} catch (e) {
			clusterDetailError = describeError(e);
		} finally {
			clusterDetailLoading = false;
		}
	}

	// ==================== Steps: add / cancel / detail (no update) ====================
	//
	// Steps have no update operation on the real API -- once added, a step's
	// only other lifecycle transition is CancelSteps, and only while still
	// PENDING.

	let addStepModal = $state<Modal | null>(null);
	let addingStep = $state(false);
	let addStepError = $state<string | null>(null);
	let newStepName = $state('');
	let newStepJar = $state('s3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar');
	let newStepArgs = $state('');
	let newStepActionOnFailure = $state<'CONTINUE' | 'TERMINATE_CLUSTER' | 'CANCEL_AND_WAIT'>(
		'CONTINUE'
	);

	function openAddStepModal(): void {
		addStepError = selectedClusterId ? null : 'Select a cluster first.';
		newStepName = '';
		newStepJar = 's3://eu-west-1.elasticmapreduce/libs/script-runner/script-runner.jar';
		newStepArgs = '';
		newStepActionOnFailure = 'CONTINUE';
		addStepModal?.open();
	}

	async function submitAddStep(): Promise<void> {
		if (!selectedClusterId) {
			addStepError = 'Select a cluster first.';
			return;
		}
		if (!newStepName.trim() || !newStepJar.trim()) {
			addStepError = 'Step name and JAR location are required.';
			return;
		}
		addingStep = true;
		addStepError = null;
		try {
			const args = newStepArgs.trim() ? newStepArgs.split('\n').filter((a) => a.trim()) : [];
			await client().send(
				new AddJobFlowStepsCommand({
					JobFlowId: selectedClusterId,
					Steps: [
						{
							Name: newStepName.trim(),
							ActionOnFailure: newStepActionOnFailure,
							HadoopJarStep: { Jar: newStepJar.trim(), Args: args }
						}
					]
				})
			);
			toast.success(`Step "${newStepName}" added`);
			addStepModal?.close();
			await tabLoader.refresh('steps');
		} catch (e) {
			const msg = describeError(e);
			addStepError = msg;
			toast.error(msg);
		} finally {
			addingStep = false;
		}
	}

	async function handleCancelStep(s: StepSummary): Promise<void> {
		if (!s.Id || !selectedClusterId) return;
		const confirmed = await confirmDestructive({
			title: 'Cancel step',
			message: `Cancel step "${s.Name ?? s.Id}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new CancelStepsCommand({ ClusterId: selectedClusterId, StepIds: [s.Id] })
			);
			toast.success('Step cancellation submitted');
			await tabLoader.refresh('steps');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let stepDetailModal = $state<Modal | null>(null);
	let viewedStep = $state<Step | null>(null);
	let stepDetailLoading = $state(false);
	let stepDetailError = $state<string | null>(null);

	async function openStepDetail(s: StepSummary): Promise<void> {
		if (!s.Id || !selectedClusterId) return;
		viewedStep = null;
		stepDetailError = null;
		stepDetailModal?.open();
		stepDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeStepCommand({ ClusterId: selectedClusterId, StepId: s.Id })
			);
			viewedStep = resp.Step ?? null;
		} catch (e) {
			stepDetailError = describeError(e);
		} finally {
			stepDetailLoading = false;
		}
	}

	// ==================== Instance Groups: add / resize (update) / detail (no delete) ====================
	//
	// The real API has no operation to remove an instance group -- resize it
	// to 0 (ModifyInstanceGroups) or terminate the whole cluster instead.
	// AddInstanceGroups can only add CORE or TASK groups; MASTER is fixed at
	// cluster launch.

	let addInstanceGroupModal = $state<Modal | null>(null);
	let addingInstanceGroup = $state(false);
	let addInstanceGroupError = $state<string | null>(null);
	let newGroupName = $state('');
	let newGroupRole = $state<'CORE' | 'TASK'>('TASK');
	let newGroupMarket = $state<'ON_DEMAND' | 'SPOT'>('ON_DEMAND');
	let newGroupInstanceType = $state('m5.xlarge');
	let newGroupInstanceCount = $state(1);
	let newGroupBidPrice = $state('');

	function openAddInstanceGroupModal(): void {
		addInstanceGroupError = selectedClusterId ? null : 'Select a cluster first.';
		newGroupName = '';
		newGroupRole = 'TASK';
		newGroupMarket = 'ON_DEMAND';
		newGroupInstanceType = 'm5.xlarge';
		newGroupInstanceCount = 1;
		newGroupBidPrice = '';
		addInstanceGroupModal?.open();
	}

	async function submitAddInstanceGroup(): Promise<void> {
		if (!selectedClusterId) {
			addInstanceGroupError = 'Select a cluster first.';
			return;
		}
		addingInstanceGroup = true;
		addInstanceGroupError = null;
		try {
			await client().send(
				new AddInstanceGroupsCommand({
					JobFlowId: selectedClusterId,
					InstanceGroups: [
						{
							Name: newGroupName.trim() || undefined,
							InstanceRole: newGroupRole,
							Market: newGroupMarket,
							BidPrice: newGroupMarket === 'SPOT' ? newGroupBidPrice.trim() || undefined : undefined,
							InstanceType: newGroupInstanceType.trim(),
							InstanceCount: newGroupInstanceCount
						}
					]
				})
			);
			toast.success('Instance group added');
			addInstanceGroupModal?.close();
			await tabLoader.refresh('instanceGroups');
		} catch (e) {
			const msg = describeError(e);
			addInstanceGroupError = msg;
			toast.error(msg);
		} finally {
			addingInstanceGroup = false;
		}
	}

	let resizeGroupModal = $state<Modal | null>(null);
	let resizingGroup = $state(false);
	let resizeGroupError = $state<string | null>(null);
	let resizeGroupTarget = $state<InstanceGroup | null>(null);
	let resizeGroupCount = $state(1);

	function openResizeGroupModal(g: InstanceGroup): void {
		resizeGroupTarget = g;
		resizeGroupCount = g.RequestedInstanceCount ?? 1;
		resizeGroupError = null;
		resizeGroupModal?.open();
	}

	async function submitResizeGroup(): Promise<void> {
		if (!resizeGroupTarget?.Id || !selectedClusterId) return;
		resizingGroup = true;
		resizeGroupError = null;
		try {
			await client().send(
				new ModifyInstanceGroupsCommand({
					ClusterId: selectedClusterId,
					InstanceGroups: [
						{ InstanceGroupId: resizeGroupTarget.Id, InstanceCount: resizeGroupCount }
					]
				})
			);
			toast.success('Instance group resized');
			resizeGroupModal?.close();
			await tabLoader.refresh('instanceGroups');
		} catch (e) {
			const msg = describeError(e);
			resizeGroupError = msg;
			toast.error(msg);
		} finally {
			resizingGroup = false;
		}
	}

	let groupDetailModal = $state<Modal | null>(null);
	let viewedGroup = $state<InstanceGroup | null>(null);

	function openGroupDetail(g: InstanceGroup): void {
		viewedGroup = g;
		groupDetailModal?.open();
	}

	// ==================== Instance Fleets: add / resize (update) / detail (no delete) ====================
	//
	// Like instance groups, there is no operation to remove an instance
	// fleet on the real API.

	let addFleetModal = $state<Modal | null>(null);
	let addingFleet = $state(false);
	let addFleetError = $state<string | null>(null);
	let newFleetName = $state('');
	let newFleetType = $state<'MASTER' | 'CORE' | 'TASK'>('TASK');
	let newFleetOnDemand = $state(0);
	let newFleetSpot = $state(1);

	function openAddFleetModal(): void {
		addFleetError = selectedClusterId ? null : 'Select a cluster first.';
		newFleetName = '';
		newFleetType = 'TASK';
		newFleetOnDemand = 0;
		newFleetSpot = 1;
		addFleetModal?.open();
	}

	async function submitAddFleet(): Promise<void> {
		if (!selectedClusterId) {
			addFleetError = 'Select a cluster first.';
			return;
		}
		addingFleet = true;
		addFleetError = null;
		try {
			await client().send(
				new AddInstanceFleetCommand({
					ClusterId: selectedClusterId,
					InstanceFleet: {
						Name: newFleetName.trim() || undefined,
						InstanceFleetType: newFleetType,
						TargetOnDemandCapacity: newFleetOnDemand,
						TargetSpotCapacity: newFleetSpot
					}
				})
			);
			toast.success('Instance fleet added');
			addFleetModal?.close();
			await tabLoader.refresh('instanceFleets');
		} catch (e) {
			const msg = describeError(e);
			addFleetError = msg;
			toast.error(msg);
		} finally {
			addingFleet = false;
		}
	}

	let modifyFleetModal = $state<Modal | null>(null);
	let modifyingFleet = $state(false);
	let modifyFleetError = $state<string | null>(null);
	let modifyFleetTarget = $state<InstanceFleet | null>(null);
	let modifyFleetOnDemand = $state(0);
	let modifyFleetSpot = $state(0);

	function openModifyFleetModal(f: InstanceFleet): void {
		modifyFleetTarget = f;
		modifyFleetOnDemand = f.TargetOnDemandCapacity ?? 0;
		modifyFleetSpot = f.TargetSpotCapacity ?? 0;
		modifyFleetError = null;
		modifyFleetModal?.open();
	}

	async function submitModifyFleet(): Promise<void> {
		if (!modifyFleetTarget?.Id || !selectedClusterId) return;
		modifyingFleet = true;
		modifyFleetError = null;
		try {
			await client().send(
				new ModifyInstanceFleetCommand({
					ClusterId: selectedClusterId,
					InstanceFleet: {
						InstanceFleetId: modifyFleetTarget.Id,
						TargetOnDemandCapacity: modifyFleetOnDemand,
						TargetSpotCapacity: modifyFleetSpot
					}
				})
			);
			toast.success('Instance fleet updated');
			modifyFleetModal?.close();
			await tabLoader.refresh('instanceFleets');
		} catch (e) {
			const msg = describeError(e);
			modifyFleetError = msg;
			toast.error(msg);
		} finally {
			modifyingFleet = false;
		}
	}

	let fleetDetailModal = $state<Modal | null>(null);
	let viewedFleet = $state<InstanceFleet | null>(null);

	function openFleetDetail(f: InstanceFleet): void {
		viewedFleet = f;
		fleetDetailModal?.open();
	}

	// ==================== Instances: detail only (no create/update/delete) ====================
	//
	// Instances are a byproduct of an instance group's or fleet's requested
	// capacity -- there is no operation to create, modify or terminate an
	// individual instance directly through the EMR API (that's an EC2/ASG
	// concern). Note also: ListInstances' InstanceFleetId filter is a
	// documented no-op against this backend (services/emr/instances.go
	// instanceGroupMatchesParams only ever checks InstanceGroupId/
	// InstanceGroupTypes) -- ticket gopherstack-dqd8 already tracks this.

	let instanceDetailModal = $state<Modal | null>(null);
	let viewedInstance = $state<Instance | null>(null);

	function openInstanceDetail(i: Instance): void {
		viewedInstance = i;
		instanceDetailModal?.open();
	}

	// ==================== Bootstrap Actions: read-only (no create/update/delete) ====================
	//
	// Bootstrap actions can only be specified at cluster launch time via
	// RunJobFlow's BootstrapActions field -- there is no Add/Modify/Delete
	// bootstrap action operation anywhere in the real API. ListBootstrapActions
	// is the only operation this family has.

	let bootstrapDetailModal = $state<Modal | null>(null);
	let viewedBootstrap = $state<BootstrapCommand | null>(null);

	function openBootstrapDetail(b: BootstrapCommand): void {
		viewedBootstrap = b;
		bootstrapDetailModal?.open();
	}

	// ==================== Security Configurations: create / delete / detail (no update) ====================
	//
	// Security configurations are immutable once created -- the real API has
	// no UpdateSecurityConfiguration operation.

	let createSecConfigModal = $state<Modal | null>(null);
	let creatingSecConfig = $state(false);
	let createSecConfigError = $state<string | null>(null);
	let newSecConfigName = $state('');
	let newSecConfigJSON = $state('{\n  "EncryptionConfiguration": {}\n}');

	function openCreateSecConfigModal(): void {
		createSecConfigError = null;
		newSecConfigName = '';
		newSecConfigJSON = '{\n  "EncryptionConfiguration": {}\n}';
		createSecConfigModal?.open();
	}

	async function submitCreateSecConfig(): Promise<void> {
		if (!newSecConfigName.trim()) {
			createSecConfigError = 'Name is required.';
			return;
		}
		try {
			JSON.parse(newSecConfigJSON);
		} catch {
			createSecConfigError = 'Security configuration must be valid JSON.';
			return;
		}
		creatingSecConfig = true;
		createSecConfigError = null;
		try {
			await client().send(
				new CreateSecurityConfigurationCommand({
					Name: newSecConfigName.trim(),
					SecurityConfiguration: newSecConfigJSON
				})
			);
			toast.success(`Security configuration "${newSecConfigName}" created`);
			createSecConfigModal?.close();
			await tabLoader.refresh('securityConfigurations');
		} catch (e) {
			const msg = describeError(e);
			createSecConfigError = msg;
			toast.error(msg);
		} finally {
			creatingSecConfig = false;
		}
	}

	async function handleDeleteSecConfig(sc: SecurityConfigurationSummary): Promise<void> {
		if (!sc.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete security configuration',
			message: `Delete security configuration "${sc.Name}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteSecurityConfigurationCommand({ Name: sc.Name }));
			toast.success('Security configuration deleted');
			await tabLoader.refresh('securityConfigurations');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let secConfigDetailModal = $state<Modal | null>(null);
	let viewedSecConfigName = $state('');
	let viewedSecConfigJSON = $state('');
	let viewedSecConfigCreated = $state<Date | undefined>();
	let secConfigDetailLoading = $state(false);
	let secConfigDetailError = $state<string | null>(null);

	async function openSecConfigDetail(sc: SecurityConfigurationSummary): Promise<void> {
		if (!sc.Name) return;
		viewedSecConfigName = sc.Name;
		viewedSecConfigJSON = '';
		viewedSecConfigCreated = undefined;
		secConfigDetailError = null;
		secConfigDetailModal?.open();
		secConfigDetailLoading = true;
		try {
			const resp = await client().send(new DescribeSecurityConfigurationCommand({ Name: sc.Name }));
			viewedSecConfigJSON = resp.SecurityConfiguration ?? '';
			viewedSecConfigCreated = resp.CreationDateTime;
		} catch (e) {
			secConfigDetailError = describeError(e);
		} finally {
			secConfigDetailLoading = false;
		}
	}

	// ==================== Studios: create / update / delete / detail ====================
	//
	// CreateStudio's Description parameter is accepted by the real wire shape
	// but this backend's handler (services/emr/handler_studios.go
	// createStudioInput) never declares a Description field at all, so it is
	// silently dropped -- a Studio created with a Description here always
	// comes back with Description empty. UpdateStudio DOES apply Description
	// correctly (its handler parses and forwards it), so Description is only
	// offered on the Update form, not Create. UpdateStudio's SubnetIds
	// parameter is likewise accepted on the wire but never applied (the
	// handler hardcodes "" instead of forwarding in.SubnetIDs to the
	// backend) -- omitted here since it would not round-trip.

	let createStudioModal = $state<Modal | null>(null);
	let creatingStudio = $state(false);
	let createStudioError = $state<string | null>(null);
	let newStudioName = $state('');
	let newStudioAuthMode = $state<'SSO' | 'IAM'>('SSO');
	let newStudioVpcId = $state('');
	let newStudioSubnetIds = $state('');
	let newStudioServiceRole = $state('');
	let newStudioEngineSG = $state('');
	let newStudioWorkspaceSG = $state('');
	let newStudioDefaultS3Location = $state('');

	function openCreateStudioModal(): void {
		createStudioError = null;
		newStudioName = '';
		newStudioAuthMode = 'SSO';
		newStudioVpcId = '';
		newStudioSubnetIds = '';
		newStudioServiceRole = '';
		newStudioEngineSG = '';
		newStudioWorkspaceSG = '';
		newStudioDefaultS3Location = '';
		createStudioModal?.open();
	}

	async function submitCreateStudio(): Promise<void> {
		if (!newStudioName.trim() || !newStudioVpcId.trim() || !newStudioDefaultS3Location.trim()) {
			createStudioError = 'Name, VPC ID and default S3 location are required.';
			return;
		}
		creatingStudio = true;
		createStudioError = null;
		try {
			await client().send(
				new CreateStudioCommand({
					Name: newStudioName.trim(),
					AuthMode: newStudioAuthMode,
					VpcId: newStudioVpcId.trim(),
					SubnetIds: parseCommaList(newStudioSubnetIds),
					ServiceRole: newStudioServiceRole.trim(),
					EngineSecurityGroupId: newStudioEngineSG.trim(),
					WorkspaceSecurityGroupId: newStudioWorkspaceSG.trim(),
					DefaultS3Location: newStudioDefaultS3Location.trim()
				})
			);
			toast.success(`Studio "${newStudioName}" created`);
			createStudioModal?.close();
			await tabLoader.refresh('studios');
		} catch (e) {
			const msg = describeError(e);
			createStudioError = msg;
			toast.error(msg);
		} finally {
			creatingStudio = false;
		}
	}

	let editStudioModal = $state<Modal | null>(null);
	let editingStudio = $state(false);
	let editStudioError = $state<string | null>(null);
	let editStudioTarget = $state<StudioSummary | null>(null);
	let editStudioName = $state('');
	let editStudioDescription = $state('');
	let editStudioDefaultS3Location = $state('');

	function openEditStudioModal(s: StudioSummary): void {
		editStudioTarget = s;
		editStudioName = s.Name ?? '';
		editStudioDescription = s.Description ?? '';
		editStudioDefaultS3Location = '';
		editStudioError = null;
		editStudioModal?.open();
	}

	async function submitEditStudio(): Promise<void> {
		if (!editStudioTarget?.StudioId) return;
		editingStudio = true;
		editStudioError = null;
		try {
			await client().send(
				new UpdateStudioCommand({
					StudioId: editStudioTarget.StudioId,
					Name: editStudioName.trim() || undefined,
					Description: editStudioDescription.trim() || undefined,
					DefaultS3Location: editStudioDefaultS3Location.trim() || undefined
				})
			);
			toast.success('Studio updated');
			editStudioModal?.close();
			await tabLoader.refresh('studios');
		} catch (e) {
			const msg = describeError(e);
			editStudioError = msg;
			toast.error(msg);
		} finally {
			editingStudio = false;
		}
	}

	async function handleDeleteStudio(s: StudioSummary): Promise<void> {
		if (!s.StudioId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete studio',
			message: `Delete studio "${s.Name ?? s.StudioId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteStudioCommand({ StudioId: s.StudioId }));
			toast.success('Studio deleted');
			await tabLoader.refresh('studios');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let studioDetailModal = $state<Modal | null>(null);
	let viewedStudio = $state<Studio | null>(null);
	let studioDetailLoading = $state(false);
	let studioDetailError = $state<string | null>(null);

	async function openStudioDetail(s: StudioSummary): Promise<void> {
		if (!s.StudioId) return;
		viewedStudio = null;
		studioDetailError = null;
		studioDetailModal?.open();
		studioDetailLoading = true;
		try {
			const resp = await client().send(new DescribeStudioCommand({ StudioId: s.StudioId }));
			viewedStudio = resp.Studio ?? null;
		} catch (e) {
			studioDetailError = describeError(e);
		} finally {
			studioDetailLoading = false;
		}
	}

	// ==================== Notebook Executions: start / stop / detail (no delete) ====================
	//
	// SEVERE WIRE-SHAPE MISMATCH (backend bug, not fixed here -- this pass is
	// UI-only): the real StartNotebookExecutionInput's cluster reference is a
	// top-level field named "ExecutionEngine" (ExecutionEngineConfig type,
	// {Id, Type, ...}) -- confirmed against the installed
	// @aws-sdk/client-emr StartNotebookExecutionInput. This backend's handler
	// (services/emr/handler_notebook_executions.go startNotebookExecutionInput)
	// instead declares the field with JSON tag "ExecutionEngineConfig", which
	// does not match any field a real client ever sends. A real
	// StartNotebookExecutionCommand's {ExecutionEngine: {Id: clusterId}} is
	// silently dropped by this backend's JSON unmarshal, so
	// NotebookExecution.ExecutionEngineID is *always* empty here regardless
	// of what is sent -- notebook executions never actually get associated
	// with the cluster a real caller specified. This form still sends the
	// real, correct "ExecutionEngine" field (that's what a real client must
	// send); the resulting empty association on read-back is the backend's
	// bug to fix, not something to work around in the UI. ServiceRole is
	// also required by the real input type but is not modeled on
	// NotebookExecution at all -- accepted and discarded.

	let startNotebookModal = $state<Modal | null>(null);
	let startingNotebook = $state(false);
	let startNotebookError = $state<string | null>(null);
	let newNotebookEditorId = $state('');
	let newNotebookClusterId = $state('');
	let newNotebookServiceRole = $state('EMR_Notebooks_DefaultRole');
	let newNotebookName = $state('');
	let newNotebookParams = $state('');

	function openStartNotebookModal(): void {
		startNotebookError = null;
		newNotebookEditorId = '';
		newNotebookClusterId = selectedClusterId;
		newNotebookServiceRole = 'EMR_Notebooks_DefaultRole';
		newNotebookName = '';
		newNotebookParams = '';
		startNotebookModal?.open();
	}

	async function submitStartNotebook(): Promise<void> {
		if (!newNotebookEditorId.trim() || !newNotebookClusterId || !newNotebookServiceRole.trim()) {
			startNotebookError = 'Editor ID, cluster and service role are required.';
			return;
		}
		startingNotebook = true;
		startNotebookError = null;
		try {
			await client().send(
				new StartNotebookExecutionCommand({
					EditorId: newNotebookEditorId.trim(),
					ExecutionEngine: { Id: newNotebookClusterId },
					ServiceRole: newNotebookServiceRole.trim(),
					NotebookExecutionName: newNotebookName.trim() || undefined,
					NotebookParams: newNotebookParams.trim() || undefined
				})
			);
			toast.success('Notebook execution started');
			startNotebookModal?.close();
			await tabLoader.refresh('notebookExecutions');
		} catch (e) {
			const msg = describeError(e);
			startNotebookError = msg;
			toast.error(msg);
		} finally {
			startingNotebook = false;
		}
	}

	async function handleStopNotebook(n: NotebookExecutionSummary): Promise<void> {
		if (!n.NotebookExecutionId) return;
		const confirmed = await confirmDestructive({
			title: 'Stop notebook execution',
			message: `Stop notebook execution "${n.NotebookExecutionName ?? n.NotebookExecutionId}"?`,
			confirmLabel: 'Stop',
			dangerous: false
		});
		if (!confirmed) return;
		try {
			await client().send(
				new StopNotebookExecutionCommand({ NotebookExecutionId: n.NotebookExecutionId })
			);
			toast.success('Notebook execution stop requested');
			await tabLoader.refresh('notebookExecutions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let notebookDetailModal = $state<Modal | null>(null);
	let viewedNotebook = $state<NotebookExecution | null>(null);
	let notebookDetailLoading = $state(false);
	let notebookDetailError = $state<string | null>(null);

	async function openNotebookDetail(n: NotebookExecutionSummary): Promise<void> {
		if (!n.NotebookExecutionId) return;
		viewedNotebook = null;
		notebookDetailError = null;
		notebookDetailModal?.open();
		notebookDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeNotebookExecutionCommand({ NotebookExecutionId: n.NotebookExecutionId })
			);
			viewedNotebook = resp.NotebookExecution ?? null;
		} catch (e) {
			notebookDetailError = describeError(e);
		} finally {
			notebookDetailLoading = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Database}
		title="Amazon EMR"
		description="Managed Hadoop / Spark clusters"
		onRefresh={handleRefresh}
		color="orange"
	>
		{#snippet actions()}
			{#if activeTab === 'clusters'}
				<button
					onclick={openCreateClusterModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Launch cluster
				</button>
			{:else if activeTab === 'steps'}
				<button
					onclick={openAddStepModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Add step
				</button>
			{:else if activeTab === 'instanceGroups'}
				<button
					onclick={openAddInstanceGroupModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Add instance group
				</button>
			{:else if activeTab === 'instanceFleets'}
				<button
					onclick={openAddFleetModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Add instance fleet
				</button>
			{:else if activeTab === 'securityConfigurations'}
				<button
					onclick={openCreateSecConfigModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create security configuration
				</button>
			{:else if activeTab === 'studios'}
				<button
					onclick={openCreateStudioModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create studio
				</button>
			{:else if activeTab === 'notebookExecutions'}
				<button
					onclick={openStartNotebookModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Start notebook execution
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="orange" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if clusterScopedTabs.includes(activeTab)}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="cluster-select" class="text-sm text-gray-500 dark:text-gray-400">Cluster</label>
					<select
						id="cluster-select"
						value={selectedClusterId}
						onchange={(e) => onClusterSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if clusters.length === 0}
							<option value="">No clusters</option>
						{/if}
						{#each clusters as c (c.Id)}
							<option value={c.Id}>{c.Name} ({c.Status?.State})</option>
						{/each}
					</select>
				</div>
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
				{#snippet clusterStatusCell(c: ClusterSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(c.Status?.State === 'RUNNING' || c.Status?.State === 'WAITING')}"
						>{c.Status?.State ?? '—'}</span
					>
				{/snippet}
				{#snippet clusterCreatedCell(c: ClusterSummary)}
					{formatDate(c.Status?.Timeline?.CreationDateTime)}
				{/snippet}
				{#snippet clusterHoursCell(c: ClusterSummary)}
					{c.NormalizedInstanceHours ?? 0}
				{/snippet}
				{#snippet clusterActionsCell(c: ClusterSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openClusterDetail(c)}
							title="View"
							aria-label="View cluster {c.Name}"
							class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditClusterModal(c)}
							title="Edit settings"
							aria-label="Edit cluster {c.Name}"
							class="text-gray-400 hover:text-orange-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleTerminateCluster(c)}
							title="Terminate"
							aria-label="Terminate cluster {c.Name}"
							class="text-gray-400 hover:text-red-500"><XCircle class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const clusterColumns = defineColumns<ClusterSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'status', label: 'Status', render: clusterStatusCell },
					{ key: 'normalizedInstanceHours', label: 'Normalized Hours', render: clusterHoursCell },
					{ key: 'created', label: 'Created', render: clusterCreatedCell },
					{ key: 'actions', label: '', render: clusterActionsCell }
				])}
				<DataTable
					rows={filteredClusters}
					rowKey={(c) => c.Id ?? ''}
					columns={clusterColumns}
					loading={tabLoader.isLoading('clusters')}
					emptyMessage="No clusters found"
				/>
				<LoadMore
					hasMore={!!clustersMarker}
					loading={loadingMoreClusters}
					onLoadMore={loadMoreClusters}
				/>
			{:else if activeTab === 'steps'}
				{#snippet stepStatusCell(s: StepSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(s.Status?.State === 'COMPLETED')}"
						>{s.Status?.State ?? '—'}</span
					>
				{/snippet}
				{#snippet stepActionsCell(s: StepSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openStepDetail(s)}
							title="View"
							aria-label="View step {s.Name}"
							class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button
						>
						{#if s.Status?.State === 'PENDING'}
							<button
								onclick={() => handleCancelStep(s)}
								title="Cancel"
								aria-label="Cancel step {s.Name}"
								class="text-gray-400 hover:text-red-500"><XCircle class="w-4 h-4" /></button
							>
						{/if}
					</div>
				{/snippet}
				{@const stepColumns = defineColumns<StepSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'status', label: 'Status', render: stepStatusCell },
					{ key: 'ActionOnFailure', label: 'Action on Failure' },
					{ key: 'actions', label: '', render: stepActionsCell }
				])}
				<DataTable
					rows={filteredSteps}
					rowKey={(s) => s.Id ?? ''}
					columns={stepColumns}
					loading={tabLoader.isLoading('steps')}
					emptyMessage={selectedClusterId ? 'No steps found' : 'Select a cluster to see its steps'}
				/>
				<LoadMore hasMore={!!stepsMarker} loading={loadingMoreSteps} onLoadMore={loadMoreSteps} />
			{:else if activeTab === 'instanceGroups'}
				{#snippet groupStatusCell(g: InstanceGroup)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(g.Status?.State === 'RUNNING')}"
						>{g.Status?.State ?? '—'}</span
					>
				{/snippet}
				{#snippet groupCountsCell(g: InstanceGroup)}
					{g.RunningInstanceCount ?? 0} / {g.RequestedInstanceCount ?? 0}
				{/snippet}
				{#snippet groupActionsCell(g: InstanceGroup)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openGroupDetail(g)}
							title="View"
							aria-label="View instance group {g.Name}"
							class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openResizeGroupModal(g)}
							title="Resize"
							aria-label="Resize instance group {g.Name}"
							class="text-gray-400 hover:text-orange-500"><Pencil class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const groupColumns = defineColumns<InstanceGroup>([
					{ key: 'Name', label: 'Name' },
					{ key: 'InstanceGroupType', label: 'Type' },
					{ key: 'InstanceType', label: 'Instance Type' },
					{ key: 'Market', label: 'Market' },
					{ key: 'status', label: 'Status', render: groupStatusCell },
					{ key: 'counts', label: 'Running / Requested', render: groupCountsCell },
					{ key: 'actions', label: '', render: groupActionsCell }
				])}
				<DataTable
					rows={filteredInstanceGroups}
					rowKey={(g) => g.Id ?? ''}
					columns={groupColumns}
					loading={tabLoader.isLoading('instanceGroups')}
					emptyMessage={selectedClusterId
						? 'No instance groups found'
						: 'Select a cluster to see its instance groups'}
				/>
				<p class="text-xs text-gray-400">
					EMR has no operation to remove an instance group directly -- resize it to 0 or terminate
					the cluster instead.
				</p>
			{:else if activeTab === 'instanceFleets'}
				{#snippet fleetStatusCell(f: InstanceFleet)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(f.Status?.State === 'RUNNING')}"
						>{f.Status?.State ?? '—'}</span
					>
				{/snippet}
				{#snippet fleetActionsCell(f: InstanceFleet)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openFleetDetail(f)}
							title="View"
							aria-label="View instance fleet {f.Name}"
							class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openModifyFleetModal(f)}
							title="Modify capacity"
							aria-label="Modify instance fleet {f.Name}"
							class="text-gray-400 hover:text-orange-500"><Pencil class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const fleetColumns = defineColumns<InstanceFleet>([
					{ key: 'Name', label: 'Name' },
					{ key: 'InstanceFleetType', label: 'Type' },
					{ key: 'status', label: 'Status', render: fleetStatusCell },
					{ key: 'TargetOnDemandCapacity', label: 'Target On-Demand' },
					{ key: 'TargetSpotCapacity', label: 'Target Spot' },
					{ key: 'actions', label: '', render: fleetActionsCell }
				])}
				<DataTable
					rows={filteredInstanceFleets}
					rowKey={(f) => f.Id ?? ''}
					columns={fleetColumns}
					loading={tabLoader.isLoading('instanceFleets')}
					emptyMessage={selectedClusterId
						? 'No instance fleets found'
						: 'Select a cluster to see its instance fleets'}
				/>
				<p class="text-xs text-gray-400">
					EMR has no operation to remove an instance fleet directly.
				</p>
			{:else if activeTab === 'instances'}
				{#snippet instanceStatusCell(i: Instance)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(i.Status?.State === 'RUNNING')}"
						>{i.Status?.State ?? '—'}</span
					>
				{/snippet}
				{#snippet instanceActionsCell(i: Instance)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openInstanceDetail(i)}
							title="View"
							aria-label="View instance {i.Ec2InstanceId}"
							class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const instanceColumns = defineColumns<Instance>([
					{ key: 'Ec2InstanceId', label: 'EC2 Instance ID' },
					{ key: 'InstanceType', label: 'Instance Type' },
					{ key: 'status', label: 'Status', render: instanceStatusCell },
					{ key: 'PrivateIpAddress', label: 'Private IP' },
					{ key: 'actions', label: '', render: instanceActionsCell }
				])}
				<DataTable
					rows={filteredInstances}
					rowKey={(i) => i.Id ?? ''}
					columns={instanceColumns}
					loading={tabLoader.isLoading('instances')}
					emptyMessage={selectedClusterId
						? 'No instances found'
						: 'Select a cluster to see its instances'}
				/>
				<LoadMore
					hasMore={!!instancesMarker}
					loading={loadingMoreInstances}
					onLoadMore={loadMoreInstances}
				/>
				<p class="text-xs text-gray-400">
					Read-only: an EMR instance is created and terminated only as a byproduct of its
					instance group's or fleet's requested capacity.
				</p>
			{:else if activeTab === 'bootstrapActions'}
				{#snippet bootstrapActionsCell(b: BootstrapCommand)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openBootstrapDetail(b)}
							title="View"
							aria-label="View bootstrap action {b.Name}"
							class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const bootstrapColumns = defineColumns<BootstrapCommand>([
					{ key: 'Name', label: 'Name' },
					{ key: 'ScriptPath', label: 'Script Path' },
					{ key: 'actions', label: '', render: bootstrapActionsCell }
				])}
				<DataTable
					rows={filteredBootstrapActions}
					rowKey={(b) => `${b.Name}-${b.ScriptPath}`}
					columns={bootstrapColumns}
					loading={tabLoader.isLoading('bootstrapActions')}
					emptyMessage={selectedClusterId
						? 'No bootstrap actions found'
						: 'Select a cluster to see its bootstrap actions'}
				/>
				<LoadMore
					hasMore={!!bootstrapActionsMarker}
					loading={loadingMoreBootstrapActions}
					onLoadMore={loadMoreBootstrapActions}
				/>
				<p class="text-xs text-gray-400">
					Read-only: bootstrap actions can only be specified when a cluster is launched -- there
					is no Add/Modify/Delete operation for them afterward.
				</p>
			{:else if activeTab === 'securityConfigurations'}
				{#snippet secConfigCreatedCell(s: SecurityConfigurationSummary)}
					{formatDate(s.CreationDateTime)}
				{/snippet}
				{#snippet secConfigActionsCell(s: SecurityConfigurationSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openSecConfigDetail(s)}
							title="View"
							aria-label="View security configuration {s.Name}"
							class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteSecConfig(s)}
							title="Delete"
							aria-label="Delete security configuration {s.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const secConfigColumns = defineColumns<SecurityConfigurationSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'created', label: 'Created', render: secConfigCreatedCell },
					{ key: 'actions', label: '', render: secConfigActionsCell }
				])}
				<DataTable
					rows={filteredSecurityConfigs}
					rowKey={(s) => s.Name ?? ''}
					columns={secConfigColumns}
					loading={tabLoader.isLoading('securityConfigurations')}
					emptyMessage="No security configurations found"
				/>
				<LoadMore
					hasMore={!!securityConfigsMarker}
					loading={loadingMoreSecurityConfigs}
					onLoadMore={loadMoreSecurityConfigs}
				/>
			{:else if activeTab === 'studios'}
				{#snippet studioActionsCell(s: StudioSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openStudioDetail(s)}
							title="View"
							aria-label="View studio {s.Name}"
							class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditStudioModal(s)}
							title="Edit"
							aria-label="Edit studio {s.Name}"
							class="text-gray-400 hover:text-orange-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteStudio(s)}
							title="Delete"
							aria-label="Delete studio {s.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const studioColumns = defineColumns<StudioSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'AuthMode', label: 'Auth Mode' },
					{ key: 'VpcId', label: 'VPC ID' },
					{ key: 'actions', label: '', render: studioActionsCell }
				])}
				<DataTable
					rows={filteredStudios}
					rowKey={(s) => s.StudioId ?? ''}
					columns={studioColumns}
					loading={tabLoader.isLoading('studios')}
					emptyMessage="No studios found"
				/>
				<LoadMore
					hasMore={!!studiosMarker}
					loading={loadingMoreStudios}
					onLoadMore={loadMoreStudios}
				/>
			{:else if activeTab === 'notebookExecutions'}
				{#snippet notebookStatusCell(n: NotebookExecutionSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(n.Status === 'FINISHED')}"
						>{n.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet notebookActionsCell(n: NotebookExecutionSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openNotebookDetail(n)}
							title="View"
							aria-label="View notebook execution {n.NotebookExecutionName}"
							class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button
						>
						{#if n.Status === 'RUNNING' || n.Status === 'STARTING'}
							<button
								onclick={() => handleStopNotebook(n)}
								title="Stop"
								aria-label="Stop notebook execution {n.NotebookExecutionName}"
								class="text-gray-400 hover:text-red-500"><StopCircle class="w-4 h-4" /></button
							>
						{/if}
					</div>
				{/snippet}
				{@const notebookColumns = defineColumns<NotebookExecutionSummary>([
					{ key: 'NotebookExecutionName', label: 'Name' },
					{ key: 'EditorId', label: 'Editor ID' },
					{ key: 'status', label: 'Status', render: notebookStatusCell },
					{ key: 'actions', label: '', render: notebookActionsCell }
				])}
				<DataTable
					rows={filteredNotebookExecutions}
					rowKey={(n) => n.NotebookExecutionId ?? ''}
					columns={notebookColumns}
					loading={tabLoader.isLoading('notebookExecutions')}
					emptyMessage="No notebook executions found"
				/>
				<LoadMore
					hasMore={!!notebookExecutionsMarker}
					loading={loadingMoreNotebookExecutions}
					onLoadMore={loadMoreNotebookExecutions}
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createClusterModal} title="Launch Cluster">
	{#snippet children()}
		<div class="space-y-3 max-h-[60vh] overflow-y-auto pr-1">
			<div>
				<label for="cluster-name" class="text-sm text-slate-600 dark:text-slate-300">Cluster Name</label>
				<input
					id="cluster-name"
					bind:value={newClusterName}
					placeholder="my-cluster"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="cluster-release" class="text-sm text-slate-600 dark:text-slate-300"
					>Release Label</label
				>
				<input
					id="cluster-release"
					bind:value={newClusterReleaseLabel}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				/>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="cluster-master-type" class="text-sm text-slate-600 dark:text-slate-300"
						>Master Instance Type</label
					>
					<input
						id="cluster-master-type"
						bind:value={newMasterInstanceType}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
					/>
				</div>
				<div>
					<label for="cluster-core-type" class="text-sm text-slate-600 dark:text-slate-300"
						>Core Instance Type</label
					>
					<input
						id="cluster-core-type"
						bind:value={newCoreInstanceType}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
					/>
				</div>
			</div>
			<div>
				<label for="cluster-core-count" class="text-sm text-slate-600 dark:text-slate-300"
					>Core Instance Count (0 to launch master-only)</label
				>
				<input
					id="cluster-core-count"
					type="number"
					min="0"
					bind:value={newCoreInstanceCount}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="cluster-log-uri" class="text-sm text-slate-600 dark:text-slate-300"
					>Log URI</label
				>
				<input
					id="cluster-log-uri"
					bind:value={newClusterLogUri}
					placeholder="s3://my-bucket/logs/"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				/>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="cluster-service-role" class="text-sm text-slate-600 dark:text-slate-300"
						>Service Role</label
					>
					<input
						id="cluster-service-role"
						bind:value={newClusterServiceRole}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
					/>
				</div>
				<div>
					<label for="cluster-jobflow-role" class="text-sm text-slate-600 dark:text-slate-300"
						>EC2 Instance Profile</label
					>
					<input
						id="cluster-jobflow-role"
						bind:value={newClusterJobFlowRole}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
					/>
				</div>
			</div>
			<div class="space-y-2">
				<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
					<input type="checkbox" bind:checked={newClusterKeepAlive} />
					Keep cluster alive when no steps are running
				</label>
				<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
					<input type="checkbox" bind:checked={newClusterTerminationProtected} />
					Termination protected
				</label>
				<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
					<input type="checkbox" bind:checked={newClusterVisibleToAllUsers} />
					Visible to all IAM principals in the account
				</label>
			</div>
			<div>
				<label for="cluster-tags" class="text-sm text-slate-600 dark:text-slate-300"
					>Tags (one KEY=VALUE per line)</label
				>
				<textarea
					id="cluster-tags"
					bind:value={newClusterTags}
					rows={2}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				></textarea>
			</div>
			{#if createClusterError}
				<p class="text-sm text-red-600 dark:text-red-400">{createClusterError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createClusterModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateCluster}
			disabled={creatingCluster}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{creatingCluster ? 'Launching…' : 'Launch'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editClusterModal} title="Edit Cluster Settings">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-500 dark:text-slate-400">{editClusterTarget?.Name}</p>
			<div>
				<label for="edit-step-concurrency" class="text-sm text-slate-600 dark:text-slate-300"
					>Step Concurrency Level</label
				>
				<input
					id="edit-step-concurrency"
					type="number"
					min="1"
					max="256"
					bind:value={editStepConcurrency}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={editKeepAlive} />
				Keep cluster alive when no steps are running
			</label>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={editTerminationProtected} />
				Termination protected
			</label>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={editVisibleToAllUsers} />
				Visible to all IAM principals in the account
			</label>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={editUnhealthyNodeReplacement} />
				Replace unhealthy nodes automatically
			</label>
			{#if editClusterError}
				<p class="text-sm text-red-600 dark:text-red-400">{editClusterError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editClusterModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditCluster}
			disabled={editingCluster}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{editingCluster ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={clusterDetailModal} title="Cluster">
	{#snippet children()}
		{#if clusterDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedCluster}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedCluster.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedCluster.ClusterArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">State</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.Status?.State ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Release Label</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.ReleaseLabel ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Log URI</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.LogUri ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Instance Collection Type</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.InstanceCollectionType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Step Concurrency Level</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.StepConcurrencyLevel ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Auto Terminate</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.AutoTerminate ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Termination Protected</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.TerminationProtected ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Visible to All Users</dt><dd class="text-slate-900 dark:text-white">{viewedCluster.VisibleToAllUsers ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedCluster.Status?.Timeline?.CreationDateTime)}</dd></div>
				{#if viewedCluster.Applications && viewedCluster.Applications.length > 0}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Applications</dt>
						<dd class="text-slate-900 dark:text-white">{viewedCluster.Applications.map((a) => `${a.Name} ${a.Version ?? ''}`.trim()).join(', ')}</dd>
					</div>
				{/if}
				{#if viewedCluster.Tags && viewedCluster.Tags.length > 0}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Tags</dt>
						<dd class="text-slate-900 dark:text-white">{viewedCluster.Tags.map((t) => `${t.Key}=${t.Value}`).join(', ')}</dd>
					</div>
				{/if}
			</dl>
		{/if}
		{#if clusterDetailError}
			<p class="mt-2 text-sm text-red-600 dark:text-red-400">{clusterDetailError}</p>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => clusterDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={addStepModal} title="Add Step">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="step-name" class="text-sm text-slate-600 dark:text-slate-300">Step Name</label>
				<input
					id="step-name"
					bind:value={newStepName}
					placeholder="My Spark Job"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="step-jar" class="text-sm text-slate-600 dark:text-slate-300">JAR Location</label>
				<input
					id="step-jar"
					bind:value={newStepJar}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono text-xs"
				/>
			</div>
			<div>
				<label for="step-args" class="text-sm text-slate-600 dark:text-slate-300"
					>Arguments (one per line)</label
				>
				<textarea
					id="step-args"
					bind:value={newStepArgs}
					rows={3}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				></textarea>
			</div>
			<div>
				<label for="step-action" class="text-sm text-slate-600 dark:text-slate-300"
					>Action on Failure</label
				>
				<select
					id="step-action"
					bind:value={newStepActionOnFailure}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="CONTINUE">Continue</option>
					<option value="CANCEL_AND_WAIT">Cancel and Wait</option>
					<option value="TERMINATE_CLUSTER">Terminate Cluster</option>
				</select>
			</div>
			{#if addStepError}
				<p class="text-sm text-red-600 dark:text-red-400">{addStepError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => addStepModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitAddStep}
			disabled={addingStep}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{addingStep ? 'Adding…' : 'Add'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={stepDetailModal} title="Step">
	{#snippet children()}
		{#if stepDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedStep}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedStep.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedStep.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedStep.Status?.State ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Action on Failure</dt><dd class="text-slate-900 dark:text-white">{viewedStep.ActionOnFailure ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">JAR</dt><dd class="font-mono text-xs break-all text-slate-900 dark:text-white">{viewedStep.Config?.Jar ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Args</dt><dd class="font-mono text-xs break-all text-slate-900 dark:text-white">{(viewedStep.Config?.Args ?? []).join(' ') || '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedStep.Status?.Timeline?.CreationDateTime)}</dd></div>
			</dl>
		{/if}
		{#if stepDetailError}
			<p class="mt-2 text-sm text-red-600 dark:text-red-400">{stepDetailError}</p>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => stepDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={addInstanceGroupModal} title="Add Instance Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="group-name" class="text-sm text-slate-600 dark:text-slate-300">Group Name</label>
				<input
					id="group-name"
					bind:value={newGroupName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="group-role" class="text-sm text-slate-600 dark:text-slate-300">Role</label>
					<select
						id="group-role"
						bind:value={newGroupRole}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="CORE">Core</option>
						<option value="TASK">Task</option>
					</select>
				</div>
				<div>
					<label for="group-market" class="text-sm text-slate-600 dark:text-slate-300">Market</label>
					<select
						id="group-market"
						bind:value={newGroupMarket}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="ON_DEMAND">On-Demand</option>
						<option value="SPOT">Spot</option>
					</select>
				</div>
			</div>
			{#if newGroupMarket === 'SPOT'}
				<div>
					<label for="group-bid" class="text-sm text-slate-600 dark:text-slate-300">Bid Price (USD)</label>
					<input
						id="group-bid"
						bind:value={newGroupBidPrice}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			{/if}
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="group-instance-type" class="text-sm text-slate-600 dark:text-slate-300"
						>Instance Type</label
					>
					<input
						id="group-instance-type"
						bind:value={newGroupInstanceType}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
					/>
				</div>
				<div>
					<label for="group-count" class="text-sm text-slate-600 dark:text-slate-300"
						>Instance Count</label
					>
					<input
						id="group-count"
						type="number"
						min="1"
						bind:value={newGroupInstanceCount}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			{#if addInstanceGroupError}
				<p class="text-sm text-red-600 dark:text-red-400">{addInstanceGroupError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => addInstanceGroupModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitAddInstanceGroup}
			disabled={addingInstanceGroup}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{addingInstanceGroup ? 'Adding…' : 'Add'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={resizeGroupModal} title="Resize Instance Group">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-500 dark:text-slate-400">{resizeGroupTarget?.Name}</p>
			<div>
				<label for="resize-count" class="text-sm text-slate-600 dark:text-slate-300"
					>Target Instance Count</label
				>
				<input
					id="resize-count"
					type="number"
					min="0"
					bind:value={resizeGroupCount}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if resizeGroupError}
				<p class="text-sm text-red-600 dark:text-red-400">{resizeGroupError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => resizeGroupModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitResizeGroup}
			disabled={resizingGroup}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{resizingGroup ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={groupDetailModal} title="Instance Group">
	{#snippet children()}
		{#if viewedGroup}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedGroup.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedGroup.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedGroup.InstanceGroupType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Market</dt><dd class="text-slate-900 dark:text-white">{viewedGroup.Market ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Instance Type</dt><dd class="text-slate-900 dark:text-white">{viewedGroup.InstanceType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Bid Price</dt><dd class="text-slate-900 dark:text-white">{viewedGroup.BidPrice ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedGroup.Status?.State ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Requested / Running</dt><dd class="text-slate-900 dark:text-white">{viewedGroup.RequestedInstanceCount ?? 0} / {viewedGroup.RunningInstanceCount ?? 0}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => groupDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={addFleetModal} title="Add Instance Fleet">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="fleet-name" class="text-sm text-slate-600 dark:text-slate-300">Fleet Name</label>
				<input
					id="fleet-name"
					bind:value={newFleetName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="fleet-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select
					id="fleet-type"
					bind:value={newFleetType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="MASTER">Master</option>
					<option value="CORE">Core</option>
					<option value="TASK">Task</option>
				</select>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="fleet-ondemand" class="text-sm text-slate-600 dark:text-slate-300"
						>Target On-Demand Capacity</label
					>
					<input
						id="fleet-ondemand"
						type="number"
						min="0"
						bind:value={newFleetOnDemand}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="fleet-spot" class="text-sm text-slate-600 dark:text-slate-300"
						>Target Spot Capacity</label
					>
					<input
						id="fleet-spot"
						type="number"
						min="0"
						bind:value={newFleetSpot}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			{#if addFleetError}
				<p class="text-sm text-red-600 dark:text-red-400">{addFleetError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => addFleetModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitAddFleet}
			disabled={addingFleet}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{addingFleet ? 'Adding…' : 'Add'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={modifyFleetModal} title="Modify Instance Fleet">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-500 dark:text-slate-400">{modifyFleetTarget?.Name}</p>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="modify-fleet-ondemand" class="text-sm text-slate-600 dark:text-slate-300"
						>Target On-Demand Capacity</label
					>
					<input
						id="modify-fleet-ondemand"
						type="number"
						min="0"
						bind:value={modifyFleetOnDemand}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="modify-fleet-spot" class="text-sm text-slate-600 dark:text-slate-300"
						>Target Spot Capacity</label
					>
					<input
						id="modify-fleet-spot"
						type="number"
						min="0"
						bind:value={modifyFleetSpot}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			{#if modifyFleetError}
				<p class="text-sm text-red-600 dark:text-red-400">{modifyFleetError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => modifyFleetModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitModifyFleet}
			disabled={modifyingFleet}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{modifyingFleet ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={fleetDetailModal} title="Instance Fleet">
	{#snippet children()}
		{#if viewedFleet}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedFleet.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedFleet.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedFleet.InstanceFleetType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedFleet.Status?.State ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Target On-Demand / Spot</dt><dd class="text-slate-900 dark:text-white">{viewedFleet.TargetOnDemandCapacity ?? 0} / {viewedFleet.TargetSpotCapacity ?? 0}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Provisioned On-Demand / Spot</dt><dd class="text-slate-900 dark:text-white">{viewedFleet.ProvisionedOnDemandCapacity ?? 0} / {viewedFleet.ProvisionedSpotCapacity ?? 0}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => fleetDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={instanceDetailModal} title="Instance">
	{#snippet children()}
		{#if viewedInstance}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Id</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedInstance.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">EC2 Instance ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedInstance.Ec2InstanceId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedInstance.Status?.State ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Instance Type</dt><dd class="text-slate-900 dark:text-white">{viewedInstance.InstanceType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Private IP</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedInstance.PrivateIpAddress ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Public IP</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedInstance.PublicIpAddress ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Instance Group ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedInstance.InstanceGroupId ?? '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => instanceDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={bootstrapDetailModal} title="Bootstrap Action">
	{#snippet children()}
		{#if viewedBootstrap}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedBootstrap.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Script Path</dt><dd class="font-mono text-xs break-all text-slate-900 dark:text-white">{viewedBootstrap.ScriptPath ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Args</dt><dd class="font-mono text-xs break-all text-slate-900 dark:text-white">{(viewedBootstrap.Args ?? []).join(' ') || '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => bootstrapDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createSecConfigModal} title="Create Security Configuration">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="secconfig-name" class="text-sm text-slate-600 dark:text-slate-300">Configuration Name</label>
				<input
					id="secconfig-name"
					bind:value={newSecConfigName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="secconfig-json" class="text-sm text-slate-600 dark:text-slate-300"
					>Security Configuration (JSON)</label
				>
				<textarea
					id="secconfig-json"
					bind:value={newSecConfigJSON}
					rows={6}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono text-xs"
				></textarea>
			</div>
			{#if createSecConfigError}
				<p class="text-sm text-red-600 dark:text-red-400">{createSecConfigError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createSecConfigModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateSecConfig}
			disabled={creatingSecConfig}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{creatingSecConfig ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={secConfigDetailModal} title="Security Configuration">
	{#snippet children()}
		{#if secConfigDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedSecConfigName}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedSecConfigCreated)}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Configuration</dt>
					<dd class="font-mono text-xs break-all whitespace-pre-wrap text-slate-900 dark:text-white">{viewedSecConfigJSON}</dd>
				</div>
			</dl>
		{/if}
		{#if secConfigDetailError}
			<p class="mt-2 text-sm text-red-600 dark:text-red-400">{secConfigDetailError}</p>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => secConfigDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createStudioModal} title="Create Studio">
	{#snippet children()}
		<div class="space-y-3 max-h-[60vh] overflow-y-auto pr-1">
			<div>
				<label for="studio-name" class="text-sm text-slate-600 dark:text-slate-300">Studio Name</label>
				<input
					id="studio-name"
					bind:value={newStudioName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="studio-auth" class="text-sm text-slate-600 dark:text-slate-300">Auth Mode</label>
				<select
					id="studio-auth"
					bind:value={newStudioAuthMode}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SSO">IAM Identity Center (SSO)</option>
					<option value="IAM">IAM</option>
				</select>
			</div>
			<div>
				<label for="studio-vpc" class="text-sm text-slate-600 dark:text-slate-300">VPC ID</label>
				<input
					id="studio-vpc"
					bind:value={newStudioVpcId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				/>
			</div>
			<div>
				<label for="studio-subnets" class="text-sm text-slate-600 dark:text-slate-300"
					>Subnet IDs (comma-separated)</label
				>
				<input
					id="studio-subnets"
					bind:value={newStudioSubnetIds}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				/>
			</div>
			<div>
				<label for="studio-service-role" class="text-sm text-slate-600 dark:text-slate-300"
					>Service Role</label
				>
				<input
					id="studio-service-role"
					bind:value={newStudioServiceRole}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				/>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="studio-engine-sg" class="text-sm text-slate-600 dark:text-slate-300"
						>Engine Security Group</label
					>
					<input
						id="studio-engine-sg"
						bind:value={newStudioEngineSG}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
					/>
				</div>
				<div>
					<label for="studio-workspace-sg" class="text-sm text-slate-600 dark:text-slate-300"
						>Workspace Security Group</label
					>
					<input
						id="studio-workspace-sg"
						bind:value={newStudioWorkspaceSG}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
					/>
				</div>
			</div>
			<div>
				<label for="studio-s3" class="text-sm text-slate-600 dark:text-slate-300"
					>Default S3 Location</label
				>
				<input
					id="studio-s3"
					bind:value={newStudioDefaultS3Location}
					placeholder="s3://my-bucket/studio/"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				/>
			</div>
			{#if createStudioError}
				<p class="text-sm text-red-600 dark:text-red-400">{createStudioError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createStudioModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateStudio}
			disabled={creatingStudio}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{creatingStudio ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editStudioModal} title="Edit Studio">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-studio-name" class="text-sm text-slate-600 dark:text-slate-300">Edit Studio Name</label>
				<input
					id="edit-studio-name"
					bind:value={editStudioName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-studio-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<textarea
					id="edit-studio-desc"
					bind:value={editStudioDescription}
					rows={2}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			<div>
				<label for="edit-studio-s3" class="text-sm text-slate-600 dark:text-slate-300"
					>Default S3 Location</label
				>
				<input
					id="edit-studio-s3"
					bind:value={editStudioDefaultS3Location}
					placeholder="(leave blank to keep current)"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				/>
			</div>
			{#if editStudioError}
				<p class="text-sm text-red-600 dark:text-red-400">{editStudioError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editStudioModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditStudio}
			disabled={editingStudio}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{editingStudio ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={studioDetailModal} title="Studio">
	{#snippet children()}
		{#if studioDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedStudio}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedStudio.StudioId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedStudio.StudioArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedStudio.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedStudio.Description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Auth Mode</dt><dd class="text-slate-900 dark:text-white">{viewedStudio.AuthMode ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">VPC ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedStudio.VpcId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">URL</dt><dd class="break-all text-slate-900 dark:text-white"><a href={viewedStudio.Url ?? '#'} class="text-orange-600 hover:underline" target="_blank" rel="noopener noreferrer">{viewedStudio.Url ?? '—'}</a></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedStudio.CreationTime)}</dd></div>
			</dl>
		{/if}
		{#if studioDetailError}
			<p class="mt-2 text-sm text-red-600 dark:text-red-400">{studioDetailError}</p>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => studioDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={startNotebookModal} title="Start Notebook Execution">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="notebook-editor" class="text-sm text-slate-600 dark:text-slate-300"
					>Editor (Notebook) ID</label
				>
				<input
					id="notebook-editor"
					bind:value={newNotebookEditorId}
					placeholder="e-XXXXXXXXXXXXXXXXXXXXXXXXXXX"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				/>
			</div>
			<div>
				<label for="notebook-cluster" class="text-sm text-slate-600 dark:text-slate-300">Cluster</label>
				<select
					id="notebook-cluster"
					bind:value={newNotebookClusterId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Select a cluster</option>
					{#each clusters as c (c.Id)}
						<option value={c.Id}>{c.Name}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="notebook-service-role" class="text-sm text-slate-600 dark:text-slate-300"
					>Service Role</label
				>
				<input
					id="notebook-service-role"
					bind:value={newNotebookServiceRole}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono"
				/>
			</div>
			<div>
				<label for="notebook-name" class="text-sm text-slate-600 dark:text-slate-300"
					>Execution Name</label
				>
				<input
					id="notebook-name"
					bind:value={newNotebookName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="notebook-params" class="text-sm text-slate-600 dark:text-slate-300"
					>Notebook Params (JSON)</label
				>
				<textarea
					id="notebook-params"
					bind:value={newNotebookParams}
					rows={2}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono text-xs"
				></textarea>
			</div>
			{#if startNotebookError}
				<p class="text-sm text-red-600 dark:text-red-400">{startNotebookError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => startNotebookModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitStartNotebook}
			disabled={startingNotebook}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50"
			>{startingNotebook ? 'Starting…' : 'Start'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={notebookDetailModal} title="Notebook Execution">
	{#snippet children()}
		{#if notebookDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedNotebook}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedNotebook.NotebookExecutionId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedNotebook.NotebookExecutionName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Editor ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedNotebook.EditorId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Execution Engine (Cluster) ID</dt><dd class="font-mono text-slate-900 dark:text-white">{viewedNotebook.ExecutionEngine?.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedNotebook.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Start Time</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedNotebook.StartTime)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">End Time</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedNotebook.EndTime)}</dd></div>
			</dl>
		{/if}
		{#if notebookDetailError}
			<p class="mt-2 text-sm text-red-600 dark:text-red-400">{notebookDetailError}</p>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => notebookDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>
