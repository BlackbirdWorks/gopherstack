<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getApplicationAutoScalingClient } from '$lib/aws-client';
	import {
		DescribeScalableTargetsCommand,
		RegisterScalableTargetCommand,
		DeregisterScalableTargetCommand,
		DescribeScalingPoliciesCommand,
		PutScalingPolicyCommand,
		DeleteScalingPolicyCommand,
		DescribeScheduledActionsCommand,
		PutScheduledActionCommand,
		DeleteScheduledActionCommand,
		DescribeScalingActivitiesCommand,
		ListTagsForResourceCommand,
		TagResourceCommand,
		UntagResourceCommand,
		GetPredictiveScalingForecastCommand,
		ServiceNamespace,
		ScalableDimension,
		type ScalableTarget,
		type ScalingPolicy,
		type ScheduledAction,
		type ScalingActivity,
		type PredefinedMetricSpecification
	} from '@aws-sdk/client-application-auto-scaling';
	import { toast } from 'svelte-sonner';
	import {
		Activity,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		ChevronRight,
		Tag,
		Clock,
		TrendingUp,
		Pause,
		Play,
		BarChart2,
		Settings2
	} from 'lucide-svelte';

	const aas = getApplicationAutoScalingClient();

	// ── State ────────────────────────────────────────────────────────────────
	let loading = $state(false);
	let targets = $state<ScalableTarget[]>([]);
	let selectedTarget = $state<ScalableTarget | null>(null);
	let activeTab = $state<'policies' | 'scheduled' | 'activities' | 'tags' | 'forecast'>('policies');
	let searchQuery = $state('');
	let namespaceFilter = $state<string>('');

	// Policies
	let policies = $state<ScalingPolicy[]>([]);
	let loadingPolicies = $state(false);

	// Scaling Activities
	let activities = $state<ScalingActivity[]>([]);
	let loadingActivities = $state(false);

	// Scheduled Actions
	let scheduledActions = $state<ScheduledAction[]>([]);
	let loadingScheduled = $state(false);

	// Tags
	let tags = $state<Record<string, string>>({});
	let loadingTags = $state(false);
	let tagKey = $state('');
	let tagValue = $state('');
	let addingTag = $state(false);

	// Forecast
	let forecastPolicyName = $state('');
	let forecastStartTime = $state('');
	let forecastEndTime = $state('');
	let loadingForecast = $state(false);
	let forecastCapacity = $state<number[]>([]);
	let forecastTimestamps = $state<string[]>([]);

	// Register target form
	let showRegister = $state(false);
	let registering = $state(false);
	let regNamespace = $state<string>(ServiceNamespace.ECS);
	let regResourceId = $state('service/default/my-service');
	let regDimension = $state('ecs:service:DesiredCount');
	let regMin = $state(1);
	let regMax = $state(10);

	// Create policy form
	let showCreatePolicy = $state(false);
	let creatingPolicy = $state(false);
	let policyName = $state('');
	let policyType = $state<'TargetTrackingScaling' | 'StepScaling'>('TargetTrackingScaling');
	let targetValue = $state(75);
	let policyMetricType = $state('ECSServiceAverageCPUUtilization');

	// StepScaling fields. The single StepAdjustment row covers the common
	// "scale up by N when threshold crossed" use-case; users wanting more
	// complex multi-step policies can hand-craft via the SDK.
	let stepAdjustmentType = $state<'ChangeInCapacity' | 'PercentChangeInCapacity' | 'ExactCapacity'>(
		'ChangeInCapacity'
	);
	let stepScalingAdjustment = $state(1);
	let stepCooldown = $state(60);

	// Predefined Application Auto Scaling metric types grouped by service namespace.
	// Each namespace's first entry is used as the default when the user opens the
	// create-policy form for a target in that namespace.
	const metricTypesByNamespace: Record<string, string[]> = {
		ecs: ['ECSServiceAverageCPUUtilization', 'ECSServiceAverageMemoryUtilization', 'ALBRequestCountPerTarget'],
		dynamodb: ['DynamoDBReadCapacityUtilization', 'DynamoDBWriteCapacityUtilization'],
		rds: ['RDSReaderAverageCPUUtilization', 'RDSReaderAverageDatabaseConnections'],
		ec2: [
			'EC2SpotFleetRequestAverageCPUUtilization',
			'EC2SpotFleetRequestAverageNetworkIn',
			'EC2SpotFleetRequestAverageNetworkOut'
		],
		sagemaker: [
			'SageMakerVariantInvocationsPerInstance',
			'SageMakerVariantConcurrentRequestsPerModel',
			'SageMakerInferenceComponentInvocationsPerCopy',
			'SageMakerInferenceComponentConcurrentRequestsPerCopyHighResolution'
		],
		appstream: ['AppStreamAverageCapacityUtilization'],
		elasticache: [
			'ElastiCachePrimaryEngineCPUUtilization',
			'ElastiCacheReplicaEngineCPUUtilization',
			'ElastiCacheDatabaseMemoryUsageCountedForEvictPercentage'
		],
		comprehend: ['ComprehendInferenceUtilization'],
		lambda: ['LambdaProvisionedConcurrencyUtilization'],
		cassandra: ['CassandraReadCapacityUtilization', 'CassandraWriteCapacityUtilization'],
		kafka: ['KafkaBrokerStorageUtilization'],
		neptune: ['NeptuneReaderAverageCPUUtilization'],
		'custom-resource': []
	};

	function metricTypesFor(ns: string | undefined): string[] {
		if (!ns) return metricTypesByNamespace.ecs;
		return metricTypesByNamespace[ns] ?? [];
	}

	// Create scheduled action form
	let showCreateScheduled = $state(false);
	let creatingScheduled = $state(false);
	let scheduledName = $state('');
	let scheduledSchedule = $state('cron(0 12 * * ? *)');
	let scheduledMinCap = $state<number | undefined>();
	let scheduledMaxCap = $state<number | undefined>();

	// Namespace options
	const namespaceOptions = [
		{ value: ServiceNamespace.ECS, label: 'ECS' },
		{ value: ServiceNamespace.EMR, label: 'EMR' },
		{ value: ServiceNamespace.EC2, label: 'EC2' },
		{ value: ServiceNamespace.APPSTREAM, label: 'AppStream' },
		{ value: ServiceNamespace.DYNAMODB, label: 'DynamoDB' },
		{ value: ServiceNamespace.RDS, label: 'RDS' },
		{ value: ServiceNamespace.SAGEMAKER, label: 'SageMaker' },
		{ value: ServiceNamespace.CUSTOM_RESOURCE, label: 'Custom Resource' },
		{ value: ServiceNamespace.COMPREHEND, label: 'Comprehend' },
		{ value: ServiceNamespace.LAMBDA, label: 'Lambda' },
		{ value: ServiceNamespace.CASSANDRA, label: 'Cassandra' },
		{ value: ServiceNamespace.KAFKA, label: 'Kafka' },
		{ value: ServiceNamespace.ELASTICACHE, label: 'ElastiCache' },
		{ value: ServiceNamespace.NEPTUNE, label: 'Neptune' }
	];

	// Derived
	const filteredTargets = $derived(
		targets.filter((t) => {
			const matchNS = !namespaceFilter || t.ServiceNamespace === namespaceFilter;
			const matchSearch =
				!searchQuery ||
				(t.ResourceId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(t.ScalableDimension ?? '').toLowerCase().includes(searchQuery.toLowerCase());
			return matchNS && matchSearch;
		})
	);

	const suspendedTarget = $derived(
		selectedTarget?.SuspendedState
			? selectedTarget.SuspendedState.DynamicScalingInSuspended ||
				selectedTarget.SuspendedState.DynamicScalingOutSuspended ||
				selectedTarget.SuspendedState.ScheduledScalingSuspended
			: false
	);

	// ── Data loaders ─────────────────────────────────────────────────────────
	async function loadTargets() {
		loading = true;
		try {
			const res = await aas.send(
				new DescribeScalableTargetsCommand({
					ServiceNamespace: (namespaceFilter as ServiceNamespace) || ServiceNamespace.ECS
				})
			);
			targets = res.ScalableTargets ?? [];
		} catch (e) {
			toast.error('Failed to load scalable targets: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadAllNamespaces() {
		loading = true;
		const all: ScalableTarget[] = [];
		await Promise.allSettled(
			namespaceOptions.map(async ({ value }) => {
				try {
					const res = await aas.send(
						new DescribeScalableTargetsCommand({ ServiceNamespace: value as ServiceNamespace })
					);
					all.push(...(res.ScalableTargets ?? []));
				} catch {
					// namespace may have no targets
				}
			})
		);
		targets = all;
		loading = false;
	}

	async function selectTarget(t: ScalableTarget) {
		selectedTarget = t;
		activeTab = 'policies';
		policies = [];
		scheduledActions = [];
		activities = [];
		tags = {};
		forecastCapacity = [];

		// Default the predefined metric type to the first one valid for this
		// namespace so Target Tracking policies don't fail with "metric not valid
		// for ServiceNamespace".
		const opts = metricTypesFor(t.ServiceNamespace);
		if (opts.length > 0) policyMetricType = opts[0];

		await loadPolicies();
	}

	async function loadPolicies() {
		if (!selectedTarget) return;
		loadingPolicies = true;
		try {
			const res = await aas.send(
				new DescribeScalingPoliciesCommand({
					ServiceNamespace: selectedTarget.ServiceNamespace,
					ResourceId: selectedTarget.ResourceId,
					ScalableDimension: selectedTarget.ScalableDimension
				})
			);
			policies = res.ScalingPolicies ?? [];
		} catch (e) {
			toast.error('Failed to load policies: ' + String(e));
		} finally {
			loadingPolicies = false;
		}
	}

	async function loadScheduledActions() {
		if (!selectedTarget) return;
		loadingScheduled = true;
		try {
			const res = await aas.send(
				new DescribeScheduledActionsCommand({
					ServiceNamespace: selectedTarget.ServiceNamespace,
					ResourceId: selectedTarget.ResourceId,
					ScalableDimension: selectedTarget.ScalableDimension
				})
			);
			scheduledActions = res.ScheduledActions ?? [];
		} catch (e) {
			toast.error('Failed to load scheduled actions: ' + String(e));
		} finally {
			loadingScheduled = false;
		}
	}

	async function loadActivities() {
		if (!selectedTarget) return;
		loadingActivities = true;
		try {
			const res = await aas.send(
				new DescribeScalingActivitiesCommand({
					ServiceNamespace: selectedTarget.ServiceNamespace,
					ResourceId: selectedTarget.ResourceId,
					ScalableDimension: selectedTarget.ScalableDimension,
					IncludeNotScaledActivities: true
				})
			);
			activities = res.ScalingActivities ?? [];
		} catch (e) {
			toast.error('Failed to load scaling activities: ' + String(e));
		} finally {
			loadingActivities = false;
		}
	}

	async function loadTags() {
		if (!selectedTarget?.ScalableTargetARN) return;
		loadingTags = true;
		try {
			const res = await aas.send(
				new ListTagsForResourceCommand({ ResourceARN: selectedTarget.ScalableTargetARN })
			);
			tags = res.Tags ?? {};
		} catch (e) {
			toast.error('Failed to load tags: ' + String(e));
		} finally {
			loadingTags = false;
		}
	}

	async function handleTabChange(tab: typeof activeTab) {
		activeTab = tab;
		if (tab === 'policies' && policies.length === 0) await loadPolicies();
		if (tab === 'scheduled' && scheduledActions.length === 0) await loadScheduledActions();
		if (tab === 'activities') await loadActivities();
		if (tab === 'tags') await loadTags();
	}

	// ── Feature 1: Register Scalable Target ──────────────────────────────────
	async function registerTarget() {
		if (!regResourceId.trim()) return;
		registering = true;
		try {
			await aas.send(
				new RegisterScalableTargetCommand({
					ServiceNamespace: regNamespace as ServiceNamespace,
					ResourceId: regResourceId.trim(),
					ScalableDimension: regDimension.trim() as ScalableDimension,
					MinCapacity: regMin,
					MaxCapacity: regMax
				})
			);
			toast.success('Scalable target registered');
			showRegister = false;
			await loadAllNamespaces();
		} catch (e) {
			toast.error('Failed to register target: ' + String(e));
		} finally {
			registering = false;
		}
	}

	// ── Feature 2: Deregister Scalable Target ─────────────────────────────────
	async function deregisterTarget(t: ScalableTarget) {
		const confirmed = await confirmDestructive({
			message: `Deregister "${t.ResourceId}"? This will also delete all associated scaling policies and scheduled actions.`
		});
		if (!confirmed) return;
		try {
			await aas.send(
				new DeregisterScalableTargetCommand({
					ServiceNamespace: t.ServiceNamespace,
					ResourceId: t.ResourceId,
					ScalableDimension: t.ScalableDimension
				})
			);
			toast.success('Scalable target deregistered');
			if (selectedTarget?.ScalableTargetARN === t.ScalableTargetARN) selectedTarget = null;
			await loadAllNamespaces();
		} catch (e) {
			toast.error('Failed to deregister: ' + String(e));
		}
	}

	// ── Feature 3: Suspend / Resume Scaling ──────────────────────────────────
	async function toggleSuspend() {
		if (!selectedTarget) return;
		const suspend = !suspendedTarget;
		try {
			await aas.send(
				new RegisterScalableTargetCommand({
					ServiceNamespace: selectedTarget.ServiceNamespace,
					ResourceId: selectedTarget.ResourceId,
					ScalableDimension: selectedTarget.ScalableDimension as ScalableDimension,
					MinCapacity: selectedTarget.MinCapacity,
					MaxCapacity: selectedTarget.MaxCapacity,
					SuspendedState: {
						DynamicScalingInSuspended: suspend,
						DynamicScalingOutSuspended: suspend,
						ScheduledScalingSuspended: suspend
					}
				})
			);
			toast.success(suspend ? 'Scaling suspended' : 'Scaling resumed');
			await loadAllNamespaces();
			const updated = targets.find(
				(t) => t.ScalableTargetARN === selectedTarget!.ScalableTargetARN
			);
			if (updated) selectedTarget = updated;
		} catch (e) {
			toast.error('Failed to update suspension state: ' + String(e));
		}
	}

	// ── Feature 4: Create Scaling Policy ─────────────────────────────────────
	async function createPolicy() {
		if (!selectedTarget || !policyName.trim()) return;
		creatingPolicy = true;
		try {
			await aas.send(
				new PutScalingPolicyCommand({
					ServiceNamespace: selectedTarget.ServiceNamespace,
					ResourceId: selectedTarget.ResourceId,
					ScalableDimension: selectedTarget.ScalableDimension,
					PolicyName: policyName.trim(),
					PolicyType: policyType,
					TargetTrackingScalingPolicyConfiguration:
						policyType === 'TargetTrackingScaling'
							? {
									TargetValue: targetValue,
									PredefinedMetricSpecification: {
										PredefinedMetricType: policyMetricType as PredefinedMetricSpecification['PredefinedMetricType']
									}
								}
							: undefined,
					StepScalingPolicyConfiguration:
						policyType === 'StepScaling'
							? {
									AdjustmentType: stepAdjustmentType,
									Cooldown: stepCooldown,
									StepAdjustments: [
										{
											MetricIntervalLowerBound: 0,
											ScalingAdjustment: stepScalingAdjustment
										}
									]
								}
							: undefined
				})
			);
			toast.success(`Policy "${policyName}" created`);
			showCreatePolicy = false;
			policyName = '';
			await loadPolicies();
		} catch (e) {
			toast.error('Failed to create policy: ' + String(e));
		} finally {
			creatingPolicy = false;
		}
	}

	// ── Feature 5: Delete Scaling Policy ─────────────────────────────────────
	async function deletePolicy(p: ScalingPolicy) {
		const confirmed = await confirmDestructive({
			message: `Delete policy "${p.PolicyName}"? This action cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await aas.send(
				new DeleteScalingPolicyCommand({
					ServiceNamespace: p.ServiceNamespace,
					ResourceId: p.ResourceId,
					ScalableDimension: p.ScalableDimension,
					PolicyName: p.PolicyName
				})
			);
			toast.success(`Policy "${p.PolicyName}" deleted`);
			await loadPolicies();
		} catch (e) {
			toast.error('Failed to delete policy: ' + String(e));
		}
	}

	// ── Feature 6: Create Scheduled Action ───────────────────────────────────
	async function createScheduledAction() {
		if (!selectedTarget || !scheduledName.trim()) return;
		creatingScheduled = true;
		try {
			await aas.send(
				new PutScheduledActionCommand({
					ServiceNamespace: selectedTarget.ServiceNamespace,
					ResourceId: selectedTarget.ResourceId,
					ScalableDimension: selectedTarget.ScalableDimension,
					ScheduledActionName: scheduledName.trim(),
					Schedule: scheduledSchedule.trim(),
					ScalableTargetAction: {
						MinCapacity: scheduledMinCap,
						MaxCapacity: scheduledMaxCap
					}
				})
			);
			toast.success(`Scheduled action "${scheduledName}" created`);
			showCreateScheduled = false;
			scheduledName = '';
			await loadScheduledActions();
		} catch (e) {
			toast.error('Failed to create scheduled action: ' + String(e));
		} finally {
			creatingScheduled = false;
		}
	}

	// ── Feature 7: Delete Scheduled Action ───────────────────────────────────
	async function deleteScheduledAction(a: ScheduledAction) {
		const confirmed = await confirmDestructive({
			message: `Delete scheduled action "${a.ScheduledActionName}"? This action cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await aas.send(
				new DeleteScheduledActionCommand({
					ServiceNamespace: a.ServiceNamespace!,
					ResourceId: a.ResourceId!,
					ScalableDimension: a.ScalableDimension,
					ScheduledActionName: a.ScheduledActionName!
				})
			);
			toast.success(`Scheduled action deleted`);
			await loadScheduledActions();
		} catch (e) {
			toast.error('Failed to delete scheduled action: ' + String(e));
		}
	}

	// ── Feature 8: Add Tag ────────────────────────────────────────────────────
	async function addTag() {
		if (!selectedTarget?.ScalableTargetARN || !tagKey.trim()) return;
		addingTag = true;
		try {
			await aas.send(
				new TagResourceCommand({
					ResourceARN: selectedTarget.ScalableTargetARN,
					Tags: { [tagKey.trim()]: tagValue.trim() }
				})
			);
			toast.success(`Tag "${tagKey}" added`);
			tagKey = '';
			tagValue = '';
			await loadTags();
		} catch (e) {
			toast.error('Failed to add tag: ' + String(e));
		} finally {
			addingTag = false;
		}
	}

	// ── Feature 9: Remove Tag ─────────────────────────────────────────────────
	async function removeTag(key: string) {
		if (!selectedTarget?.ScalableTargetARN) return;
		try {
			await aas.send(
				new UntagResourceCommand({
					ResourceARN: selectedTarget.ScalableTargetARN,
					TagKeys: [key]
				})
			);
			toast.success(`Tag "${key}" removed`);
			await loadTags();
		} catch (e) {
			toast.error('Failed to remove tag: ' + String(e));
		}
	}

	// ── Feature 10: Predictive Scaling Forecast ───────────────────────────────
	async function loadForecast() {
		if (!selectedTarget || !forecastPolicyName.trim()) return;
		loadingForecast = true;
		try {
			const res = await aas.send(
				new GetPredictiveScalingForecastCommand({
					ServiceNamespace: selectedTarget.ServiceNamespace,
					ResourceId: selectedTarget.ResourceId,
					ScalableDimension: selectedTarget.ScalableDimension,
					PolicyName: forecastPolicyName.trim(),
					StartTime: new Date(forecastStartTime),
					EndTime: new Date(forecastEndTime)
				})
			);
			forecastTimestamps = (res.CapacityForecast?.Timestamps ?? []).map((t) =>
				new Date(t).toLocaleString()
			);
			forecastCapacity = res.CapacityForecast?.Values ?? [];
			if (forecastCapacity.length === 0) toast.info('No forecast data returned');
		} catch (e) {
			toast.error('Failed to load forecast: ' + String(e));
		} finally {
			loadingForecast = false;
		}
	}

	const forecastMax = $derived(Math.max(...forecastCapacity, 1));

	onMount(() => loadAllNamespaces());
</script>

<div class="flex h-full overflow-hidden">
	<!-- ── Left panel: target list ──────────────────────────────────────────── -->
	<div class="flex w-80 flex-shrink-0 flex-col border-r border-slate-200 dark:border-slate-700">
		<div class="border-b border-slate-200 p-4 dark:border-slate-700">
			<div class="mb-3 flex items-center justify-between">
				<h2 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Scalable Targets</h2>
				<div class="flex gap-1">
					<button
						onclick={() => (showRegister = true)}
						class="flex items-center gap-1 rounded bg-indigo-600 px-2 py-1 text-xs text-white hover:bg-indigo-700"
					>
						<Plus size={12} />
						Register
					</button>
					<button
						onclick={loadAllNamespaces}
						class="rounded p-1 text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-700"
						title="Refresh"
					>
						<RefreshCw size={14} class={loading ? 'animate-spin' : ''} />
					</button>
				</div>
			</div>

			<!-- Search -->
			<div class="relative mb-2">
				<Search
					size={13}
					class="absolute top-1/2 left-2 -translate-y-1/2 text-slate-400"
				/>
				<input
					bind:value={searchQuery}
					placeholder="Search resource ID…"
					class="w-full rounded border border-slate-200 py-1.5 pr-2 pl-7 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
				/>
			</div>

			<!-- Namespace filter -->
			<select
				bind:value={namespaceFilter}
				onchange={loadAllNamespaces}
				class="w-full rounded border border-slate-200 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
			>
				<option value="">All namespaces</option>
				{#each namespaceOptions as opt}
					<option value={opt.value}>{opt.label}</option>
				{/each}
			</select>
		</div>

		<!-- Target list -->
		<div class="flex-1 overflow-y-auto">
			{#if loading}
				<div class="flex justify-center p-6">
					<RefreshCw size={18} class="animate-spin text-slate-400" />
				</div>
			{:else if filteredTargets.length === 0}
				<div class="p-4 text-center text-xs text-slate-500">No targets found</div>
			{:else}
				{#each filteredTargets as t}
					<button
						onclick={() => selectTarget(t)}
						class="flex w-full items-start gap-2 border-b border-slate-100 px-4 py-3 text-left hover:bg-slate-50 dark:border-slate-700 dark:hover:bg-slate-800 {selectedTarget?.ScalableTargetARN ===
						t.ScalableTargetARN
							? 'bg-indigo-50 dark:bg-indigo-900/20'
							: ''}"
					>
						<Activity size={14} class="mt-0.5 flex-shrink-0 text-indigo-500" />
						<div class="min-w-0 flex-1">
							<div class="truncate text-xs font-medium text-slate-700 dark:text-slate-200">
								{t.ResourceId}
							</div>
							<div class="mt-0.5 flex items-center gap-1">
								<span
									class="rounded bg-indigo-100 px-1 py-0.5 text-[10px] text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300"
								>
									{t.ServiceNamespace}
								</span>
								{#if t.SuspendedState?.DynamicScalingInSuspended || t.SuspendedState?.DynamicScalingOutSuspended || t.SuspendedState?.ScheduledScalingSuspended}
									<span
										class="rounded bg-amber-100 px-1 py-0.5 text-[10px] text-amber-700 dark:bg-amber-900/40 dark:text-amber-300"
									>
										Suspended
									</span>
								{/if}
							</div>
							<div class="mt-0.5 text-[10px] text-slate-400">
								{t.MinCapacity} – {t.MaxCapacity} capacity
							</div>
						</div>
						<ChevronRight size={12} class="flex-shrink-0 text-slate-400" />
					</button>
				{/each}
			{/if}
		</div>
	</div>

	<!-- ── Right panel: target details ──────────────────────────────────────── -->
	<div class="flex flex-1 flex-col overflow-hidden">
		{#if !selectedTarget}
			<div class="flex flex-1 items-center justify-center text-sm text-slate-400">
				Select a scalable target to view details
			</div>
		{:else}
			<!-- Header -->
			<div class="border-b border-slate-200 px-6 py-4 dark:border-slate-700">
				<div class="flex items-start justify-between">
					<div>
						<h2 class="text-base font-semibold text-slate-800 dark:text-slate-100">
							{selectedTarget.ResourceId}
						</h2>
						<div class="mt-1 flex flex-wrap items-center gap-2">
							<span class="text-xs text-slate-500">{selectedTarget.ServiceNamespace}</span>
							<span class="text-slate-300">·</span>
							<span class="text-xs text-slate-500">{selectedTarget.ScalableDimension}</span>
							<span class="text-slate-300">·</span>
							<span class="text-xs text-slate-500">
								Capacity: {selectedTarget.MinCapacity} – {selectedTarget.MaxCapacity}
							</span>
						</div>
					</div>
					<div class="flex gap-2">
						<!-- Suspend / Resume toggle -->
						<button
							onclick={toggleSuspend}
							class="flex items-center gap-1 rounded border px-3 py-1.5 text-xs {suspendedTarget
								? 'border-green-300 text-green-600 hover:bg-green-50 dark:border-green-700 dark:text-green-400'
								: 'border-amber-300 text-amber-600 hover:bg-amber-50 dark:border-amber-700 dark:text-amber-400'}"
						>
							{#if suspendedTarget}
								<Play size={12} />
								Resume Scaling
							{:else}
								<Pause size={12} />
								Suspend Scaling
							{/if}
						</button>
						<button
							onclick={() => deregisterTarget(selectedTarget!)}
							class="flex items-center gap-1 rounded border border-red-300 px-3 py-1.5 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400"
						>
							<Trash2 size={12} />
							Deregister
						</button>
					</div>
				</div>
			</div>

			<!-- Tabs -->
			<div class="flex border-b border-slate-200 dark:border-slate-700">
				{#snippet tabBtn(id: typeof activeTab, label: string)}
					<button
						onclick={() => handleTabChange(id)}
						class="border-b-2 px-4 py-2.5 text-xs font-medium transition-colors {activeTab === id
							? 'border-indigo-600 text-indigo-600 dark:text-indigo-400'
							: 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200'}"
					>
						{label}
					</button>
				{/snippet}
				{@render tabBtn('policies', 'Scaling Policies')}
				{@render tabBtn('scheduled', 'Scheduled Actions')}
				{@render tabBtn('activities', 'Activities')}
				{@render tabBtn('tags', 'Tags')}
				{@render tabBtn('forecast', 'Forecast')}
			</div>

			<!-- Tab content -->
			<div class="flex-1 overflow-y-auto p-4">
				<!-- Policies tab -->
				{#if activeTab === 'policies'}
					<div class="mb-3 flex items-center justify-between">
						<span class="text-xs font-medium text-slate-600 dark:text-slate-300">
							{policies.length} {policies.length === 1 ? 'policy' : 'policies'}
						</span>
						<button
							onclick={() => (showCreatePolicy = true)}
							class="flex items-center gap-1 rounded bg-indigo-600 px-2.5 py-1 text-xs text-white hover:bg-indigo-700"
						>
							<Plus size={12} />
							New Policy
						</button>
					</div>

					{#if loadingPolicies}
						<div class="flex justify-center py-8">
							<RefreshCw size={16} class="animate-spin text-slate-400" />
						</div>
					{:else if policies.length === 0}
						<div class="rounded-lg border border-dashed border-slate-300 p-8 text-center dark:border-slate-600">
							<TrendingUp size={24} class="mx-auto mb-2 text-slate-300" />
							<p class="text-xs text-slate-500">No scaling policies yet</p>
						</div>
					{:else}
						<div class="space-y-2">
							{#each policies as p}
								<div
									class="rounded-lg border border-slate-200 bg-white p-3 dark:border-slate-700 dark:bg-slate-800"
								>
									<div class="flex items-start justify-between">
										<div>
											<div class="text-xs font-semibold text-slate-700 dark:text-slate-200">
												{p.PolicyName}
											</div>
											<div class="mt-1 flex gap-2">
												<span
													class="rounded bg-blue-100 px-1.5 py-0.5 text-[10px] text-blue-700 dark:bg-blue-900/30 dark:text-blue-300"
												>
													{p.PolicyType}
												</span>
												{#if p.TargetTrackingScalingPolicyConfiguration?.TargetValue}
													<span class="text-[10px] text-slate-500">
														Target: {p.TargetTrackingScalingPolicyConfiguration.TargetValue}%
													</span>
												{/if}
											</div>
										</div>
										<button
											onclick={() => deletePolicy(p)}
											class="rounded p-1 text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20"
											title="Delete policy"
										>
											<Trash2 size={13} />
										</button>
									</div>
									{#if p.Alarms && p.Alarms.length > 0}
										<div class="mt-2 text-[10px] text-slate-400">
											Alarms: {p.Alarms.map((a) => a.AlarmName).join(', ')}
										</div>
									{/if}
								</div>
							{/each}
						</div>
					{/if}

					<!-- Create policy form -->
					{#if showCreatePolicy}
						<div
							class="mt-4 rounded-lg border border-indigo-200 bg-indigo-50/50 p-4 dark:border-indigo-800 dark:bg-indigo-900/10"
						>
							<h4 class="mb-3 text-xs font-semibold text-slate-700 dark:text-slate-200">
								New Scaling Policy
							</h4>
							<div class="space-y-2">
								<input
									bind:value={policyName}
									placeholder="Policy name"
									class="w-full rounded border border-slate-200 px-3 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
								/>
								<select
									bind:value={policyType}
									class="w-full rounded border border-slate-200 px-3 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
								>
									<option value="TargetTrackingScaling">Target Tracking Scaling</option>
									<option value="StepScaling">Step Scaling</option>
								</select>
								{#if policyType === 'TargetTrackingScaling'}
									{@const metricOptions = metricTypesFor(selectedTarget?.ServiceNamespace)}
									{#if metricOptions.length > 0}
										<select
											bind:value={policyMetricType}
											class="w-full rounded border border-slate-200 px-3 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
										>
											{#each metricOptions as opt (opt)}
												<option value={opt}>{opt}</option>
											{/each}
										</select>
									{:else}
										<input
											bind:value={policyMetricType}
											placeholder="Predefined metric type"
											class="w-full rounded border border-slate-200 px-3 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
										/>
									{/if}
									<div class="flex items-center gap-2">
										<label class="w-24 text-xs text-slate-600 dark:text-slate-300">Target %</label>
										<input
											bind:value={targetValue}
											type="number"
											min="1"
											max="100"
											class="w-24 rounded border border-slate-200 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
										/>
									</div>
								{:else if policyType === 'StepScaling'}
									<select
										bind:value={stepAdjustmentType}
										class="w-full rounded border border-slate-200 px-3 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
									>
										<option value="ChangeInCapacity">ChangeInCapacity</option>
										<option value="PercentChangeInCapacity">PercentChangeInCapacity</option>
										<option value="ExactCapacity">ExactCapacity</option>
									</select>
									<div class="flex items-center gap-2">
										<label class="w-24 text-xs text-slate-600 dark:text-slate-300">Adjustment</label>
										<input
											bind:value={stepScalingAdjustment}
											type="number"
											class="w-24 rounded border border-slate-200 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
										/>
									</div>
									<div class="flex items-center gap-2">
										<label class="w-24 text-xs text-slate-600 dark:text-slate-300">Cooldown (s)</label>
										<input
											bind:value={stepCooldown}
											type="number"
											min="0"
											class="w-24 rounded border border-slate-200 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
										/>
									</div>
								{/if}
							</div>
							<div class="mt-3 flex gap-2">
								<button
									onclick={createPolicy}
									disabled={creatingPolicy}
									class="rounded bg-indigo-600 px-3 py-1.5 text-xs text-white hover:bg-indigo-700 disabled:opacity-50"
								>
									{creatingPolicy ? 'Creating…' : 'Create'}
								</button>
								<button
									onclick={() => (showCreatePolicy = false)}
									class="rounded border border-slate-200 px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-300"
								>
									Cancel
								</button>
							</div>
						</div>
					{/if}

				<!-- Scheduled Actions tab -->
				{:else if activeTab === 'scheduled'}
					<div class="mb-3 flex items-center justify-between">
						<span class="text-xs font-medium text-slate-600 dark:text-slate-300">
							{scheduledActions.length} scheduled {scheduledActions.length === 1 ? 'action' : 'actions'}
						</span>
						<button
							onclick={() => (showCreateScheduled = true)}
							class="flex items-center gap-1 rounded bg-indigo-600 px-2.5 py-1 text-xs text-white hover:bg-indigo-700"
						>
							<Plus size={12} />
							New Action
						</button>
					</div>

					{#if loadingScheduled}
						<div class="flex justify-center py-8">
							<RefreshCw size={16} class="animate-spin text-slate-400" />
						</div>
					{:else if scheduledActions.length === 0}
						<div class="rounded-lg border border-dashed border-slate-300 p-8 text-center dark:border-slate-600">
							<Clock size={24} class="mx-auto mb-2 text-slate-300" />
							<p class="text-xs text-slate-500">No scheduled actions yet</p>
						</div>
					{:else}
						<div class="space-y-2">
							{#each scheduledActions as a}
								<div
									class="rounded-lg border border-slate-200 bg-white p-3 dark:border-slate-700 dark:bg-slate-800"
								>
									<div class="flex items-start justify-between">
										<div>
											<div class="text-xs font-semibold text-slate-700 dark:text-slate-200">
												{a.ScheduledActionName}
											</div>
											<div class="mt-1 font-mono text-[10px] text-slate-500">{a.Schedule}</div>
											{#if a.ScalableTargetAction}
												<div class="mt-1 text-[10px] text-slate-500">
													Min: {a.ScalableTargetAction.MinCapacity ?? '–'} / Max: {a
														.ScalableTargetAction.MaxCapacity ?? '–'}
												</div>
											{/if}
										</div>
										<button
											onclick={() => deleteScheduledAction(a)}
											class="rounded p-1 text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20"
											title="Delete action"
										>
											<Trash2 size={13} />
										</button>
									</div>
								</div>
							{/each}
						</div>
					{/if}

					{#if showCreateScheduled}
						<div
							class="mt-4 rounded-lg border border-indigo-200 bg-indigo-50/50 p-4 dark:border-indigo-800 dark:bg-indigo-900/10"
						>
							<h4 class="mb-3 text-xs font-semibold text-slate-700 dark:text-slate-200">
								New Scheduled Action
							</h4>
							<div class="space-y-2">
								<input
									bind:value={scheduledName}
									placeholder="Action name"
									class="w-full rounded border border-slate-200 px-3 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
								/>
								<input
									bind:value={scheduledSchedule}
									placeholder="Schedule (e.g. cron(0 12 * * ? *))"
									class="w-full rounded border border-slate-200 px-3 py-1.5 font-mono text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
								/>
								<div class="flex gap-2">
									<input
										bind:value={scheduledMinCap}
										type="number"
										placeholder="Min capacity"
										class="w-1/2 rounded border border-slate-200 px-3 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
									/>
									<input
										bind:value={scheduledMaxCap}
										type="number"
										placeholder="Max capacity"
										class="w-1/2 rounded border border-slate-200 px-3 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
									/>
								</div>
							</div>
							<div class="mt-3 flex gap-2">
								<button
									onclick={createScheduledAction}
									disabled={creatingScheduled}
									class="rounded bg-indigo-600 px-3 py-1.5 text-xs text-white hover:bg-indigo-700 disabled:opacity-50"
								>
									{creatingScheduled ? 'Creating…' : 'Create'}
								</button>
								<button
									onclick={() => (showCreateScheduled = false)}
									class="rounded border border-slate-200 px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-300"
								>
									Cancel
								</button>
							</div>
						</div>
					{/if}

				<!-- Activities tab -->
				{:else if activeTab === 'activities'}
					<div class="mb-3 flex items-center justify-between">
						<p class="text-xs text-slate-500 dark:text-slate-400">
							{activities.length} scaling {activities.length === 1 ? 'activity' : 'activities'}
						</p>
						<button
							onclick={loadActivities}
							class="flex items-center gap-1 rounded border border-slate-200 px-3 py-1.5 text-xs text-slate-600 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-300"
						>
							<RefreshCw size={12} /> Refresh
						</button>
					</div>
					{#if loadingActivities}
						<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-slate-400" /></div>
					{:else if activities.length === 0}
						<div class="flex flex-col items-center justify-center py-12 text-slate-400">
							<Clock class="mb-2 h-10 w-10 opacity-30" />
							<p class="text-sm">No scaling activities recorded</p>
						</div>
					{:else}
						<ol class="relative border-l border-slate-200 dark:border-slate-700 ml-3 space-y-5">
						{#each activities as act}
							<li class="ml-5">
								<span class="absolute -left-[7px] mt-1 h-3 w-3 rounded-full {act.StatusCode === 'Successful' ? 'bg-green-500' : act.StatusCode === 'Failed' ? 'bg-red-500' : 'bg-amber-500'}"></span>
								<div class="flex flex-wrap items-center gap-2">
									<span class="text-xs font-medium text-slate-900 dark:text-white">{act.Description ?? act.ActivityId}</span>
									<span class="rounded px-1.5 py-0.5 text-[10px] font-medium {act.StatusCode === 'Successful' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : act.StatusCode === 'Failed' ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' : 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-400'}">{act.StatusCode}</span>
								</div>
								{#if act.Cause}
									<p class="mt-1 text-xs text-slate-500 dark:text-slate-400">{act.Cause}</p>
								{/if}
								{#if act.StatusMessage}
									<p class="mt-1 text-xs text-slate-400 dark:text-slate-500">{act.StatusMessage}</p>
								{/if}
								<p class="mt-1 text-[11px] text-slate-400">
									{act.StartTime ? new Date(act.StartTime).toLocaleString() : '—'}
									{#if act.EndTime}→ {new Date(act.EndTime).toLocaleString()}{/if}
								</p>
							</li>
						{/each}
						</ol>
					{/if}

				<!-- Tags tab -->
				{:else if activeTab === 'tags'}
					{#if loadingTags}
						<div class="flex justify-center py-8">
							<RefreshCw size={16} class="animate-spin text-slate-400" />
						</div>
					{:else}
						<div class="mb-4 space-y-2">
							{#if Object.keys(tags).length === 0}
								<div class="rounded-lg border border-dashed border-slate-300 p-8 text-center dark:border-slate-600">
									<Tag size={24} class="mx-auto mb-2 text-slate-300" />
									<p class="text-xs text-slate-500">No tags on this target</p>
								</div>
							{:else}
								{#each Object.entries(tags) as [k, v]}
									<div
										class="flex items-center justify-between rounded-lg border border-slate-200 bg-white px-3 py-2 dark:border-slate-700 dark:bg-slate-800"
									>
										<div class="flex gap-3">
											<span class="text-xs font-medium text-slate-700 dark:text-slate-200">{k}</span>
											<span class="text-xs text-slate-500">{v}</span>
										</div>
										<button
											onclick={() => removeTag(k)}
											class="rounded p-1 text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20"
											title="Remove tag"
										>
											<Trash2 size={12} />
										</button>
									</div>
								{/each}
							{/if}
						</div>

						<!-- Add tag -->
						<div class="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-700 dark:bg-slate-800/50">
							<div class="mb-2 text-xs font-medium text-slate-600 dark:text-slate-300">Add Tag</div>
							<div class="flex gap-2">
								<input
									bind:value={tagKey}
									placeholder="Key"
									class="w-1/3 rounded border border-slate-200 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
								/>
								<input
									bind:value={tagValue}
									placeholder="Value"
									class="flex-1 rounded border border-slate-200 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
								/>
								<button
									onclick={addTag}
									disabled={addingTag || !tagKey.trim()}
									class="rounded bg-indigo-600 px-3 py-1.5 text-xs text-white hover:bg-indigo-700 disabled:opacity-50"
								>
									Add
								</button>
							</div>
						</div>
					{/if}

				<!-- Forecast tab -->
				{:else if activeTab === 'forecast'}
					<div class="mb-4 rounded-lg border border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-800/50">
						<div class="mb-2 text-xs font-medium text-slate-600 dark:text-slate-300">
							Predictive Scaling Forecast
						</div>
						<div class="space-y-2">
							<input
								bind:value={forecastPolicyName}
								placeholder="PredictiveScaling policy name"
								class="w-full rounded border border-slate-200 px-3 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
							/>
							<div class="flex gap-2">
								<div class="flex-1">
									<label class="mb-1 block text-[10px] text-slate-500">Start Time</label>
									<input
										bind:value={forecastStartTime}
										type="datetime-local"
										class="w-full rounded border border-slate-200 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
									/>
								</div>
								<div class="flex-1">
									<label class="mb-1 block text-[10px] text-slate-500">End Time</label>
									<input
										bind:value={forecastEndTime}
										type="datetime-local"
										class="w-full rounded border border-slate-200 px-2 py-1.5 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
									/>
								</div>
							</div>
							<button
								onclick={loadForecast}
								disabled={loadingForecast ||
								!forecastPolicyName.trim() ||
								!forecastStartTime ||
								!forecastEndTime}
								class="flex items-center gap-1 rounded bg-indigo-600 px-3 py-1.5 text-xs text-white hover:bg-indigo-700 disabled:opacity-50"
							>
								<BarChart2 size={12} />
								{loadingForecast ? 'Loading…' : 'Load Forecast'}
							</button>
						</div>
					</div>

					{#if forecastCapacity.length > 0}
						<div class="rounded-lg border border-slate-200 bg-white p-4 dark:border-slate-700 dark:bg-slate-800">
							<div class="mb-3 text-xs font-medium text-slate-600 dark:text-slate-300">
								Capacity Forecast ({forecastCapacity.length} data points)
							</div>
							<!-- Mini bar chart -->
							<div class="flex h-32 items-end gap-0.5 overflow-x-auto">
								{#each forecastCapacity as val, i}
									<div class="flex flex-col items-center" title="{forecastTimestamps[i]}: {val}">
										<div
											class="min-w-2 rounded-t bg-indigo-400 dark:bg-indigo-500"
											style="height: {(val / forecastMax) * 100}%; min-height: 2px; width: {Math.max(
												6,
												Math.floor(600 / forecastCapacity.length)
											)}px"
										></div>
									</div>
								{/each}
							</div>
							<div class="mt-2 flex justify-between text-[10px] text-slate-400">
								<span>{forecastTimestamps[0] ?? ''}</span>
								<span>Max: {forecastMax}</span>
								<span>{forecastTimestamps[forecastTimestamps.length - 1] ?? ''}</span>
							</div>
						</div>
					{/if}
				{/if}
			</div>
		{/if}
	</div>
</div>

<!-- Register Target modal -->
{#if showRegister}
	<div
		class="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm"
		role="dialog"
		aria-modal="true"
	>
		<div
			class="w-full max-w-md rounded-xl border border-slate-200 bg-white p-6 shadow-xl dark:border-slate-700 dark:bg-slate-900"
		>
			<div class="mb-4 flex items-center gap-2">
				<Settings2 size={16} class="text-indigo-500" />
				<h3 class="text-sm font-semibold text-slate-800 dark:text-slate-100">
					Register Scalable Target
				</h3>
			</div>
			<div class="space-y-3">
				<div>
					<label class="mb-1 block text-xs text-slate-600 dark:text-slate-300">Service Namespace</label>
					<select
						bind:value={regNamespace}
						onchange={() => {
							if (regNamespace === ServiceNamespace.ECS) {
								regDimension = 'ecs:service:DesiredCount';
								regResourceId = 'service/default/my-service';
							} else if (regNamespace === ServiceNamespace.DYNAMODB) {
								regDimension = 'dynamodb:table:ReadCapacityUnits';
								regResourceId = 'table/my-table';
							} else if (regNamespace === ServiceNamespace.LAMBDA) {
								regDimension = 'lambda:function:ProvisionedConcurrency';
								regResourceId = 'function:my-function:prod';
							}
						}}
						class="w-full rounded border border-slate-200 px-3 py-2 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
					>
						{#each namespaceOptions as opt}
							<option value={opt.value}>{opt.label}</option>
						{/each}
					</select>
				</div>
				<div>
					<label class="mb-1 block text-xs text-slate-600 dark:text-slate-300">Resource ID</label>
					<input
						bind:value={regResourceId}
						placeholder="e.g. service/default/my-service"
						class="w-full rounded border border-slate-200 px-3 py-2 text-xs font-mono dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
					/>
				</div>
				<div>
					<label class="mb-1 block text-xs text-slate-600 dark:text-slate-300">Scalable Dimension</label>
					<input
						bind:value={regDimension}
						placeholder="e.g. ecs:service:DesiredCount"
						class="w-full rounded border border-slate-200 px-3 py-2 text-xs font-mono dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
					/>
				</div>
				<div class="flex gap-3">
					<div class="flex-1">
						<label class="mb-1 block text-xs text-slate-600 dark:text-slate-300">Min Capacity</label>
						<input
							bind:value={regMin}
							type="number"
							min="0"
							class="w-full rounded border border-slate-200 px-3 py-2 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
						/>
					</div>
					<div class="flex-1">
						<label class="mb-1 block text-xs text-slate-600 dark:text-slate-300">Max Capacity</label>
						<input
							bind:value={regMax}
							type="number"
							min="1"
							class="w-full rounded border border-slate-200 px-3 py-2 text-xs dark:border-slate-600 dark:bg-slate-800 dark:text-slate-200"
						/>
					</div>
				</div>
			</div>
			<div class="mt-5 flex justify-end gap-2">
				<button
					onclick={() => (showRegister = false)}
					class="rounded border border-slate-200 px-4 py-2 text-xs text-slate-600 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300"
				>
					Cancel
				</button>
				<button
					onclick={registerTarget}
					disabled={registering}
					class="rounded bg-indigo-600 px-4 py-2 text-xs text-white hover:bg-indigo-700 disabled:opacity-50"
				>
					{registering ? 'Registering…' : 'Register'}
				</button>
			</div>
		</div>
	</div>
{/if}
