<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getCostExplorerClient } from '$lib/aws-client';
	import {
		ListCostCategoryDefinitionsCommand,
		CreateCostCategoryDefinitionCommand,
		DeleteCostCategoryDefinitionCommand,
		GetAnomalyMonitorsCommand,
		CreateAnomalyMonitorCommand,
		DeleteAnomalyMonitorCommand,
		GetAnomaliesCommand,
		GetAnomalySubscriptionsCommand,
		CreateAnomalySubscriptionCommand,
		DeleteAnomalySubscriptionCommand,
		GetCostForecastCommand,
		GetReservationPurchaseRecommendationCommand,
		type AnomalyMonitor,
		type Anomaly,
		type AnomalySubscription,
		type CostCategoryReference
	} from '@aws-sdk/client-cost-explorer';
	import { toast } from 'svelte-sonner';
	import {
		DollarSign,
		AlertTriangle,
		Bell,
		List,
		RefreshCw,
		Plus,
		Trash2,
		Eye,
		Filter,
		TrendingUp,
		Server
	} from 'lucide-svelte';

	const ce = regionalClient(getCostExplorerClient);

	type Tab = 'categories' | 'monitors' | 'subscriptions' | 'anomalies' | 'forecasting' | 'reservations';

	let activeTab = $state<Tab>('categories');

	// Forecasting
	let forecastStart = $state(new Date().toISOString().slice(0, 10));
	let forecastEnd = $state(new Date(Date.now() + 30 * 86400000).toISOString().slice(0, 10));
	let forecastMetric = $state<'BLENDED_COST' | 'UNBLENDED_COST' | 'AMORTIZED_COST' | 'NET_AMORTIZED_COST'>('BLENDED_COST');
	let forecastGranularity = $state<'DAILY' | 'MONTHLY'>('MONTHLY');
	let forecastResult = $state<{ Total?: { Amount?: string; Unit?: string } } | null>(null);
	let loadingForecast = $state(false);

	// Reservations
	let reservationService = $state('Amazon EC2');
	let reservationLookback = $state<'SEVEN_DAYS' | 'THIRTY_DAYS' | 'SIXTY_DAYS'>('THIRTY_DAYS');
	let reservationTerm = $state<'ONE_YEAR' | 'THREE_YEARS'>('ONE_YEAR');
	let reservationPayment = $state<'NO_UPFRONT' | 'PARTIAL_UPFRONT' | 'ALL_UPFRONT'>('NO_UPFRONT');
	let reservationRecommendations = $state<unknown[]>([]);
	let loadingReservations = $state(false);
	let loading = $state(false);

	// Cost Categories
	let categories = $state<CostCategoryReference[]>([]);
	let newCategoryName = $state('');
	let newCategoryRuleVersion = $state('CostCategoryExpression.v1');
	let creatingCategory = $state(false);

	// Anomaly Monitors
	let monitors = $state<AnomalyMonitor[]>([]);
	let newMonitorName = $state('');
	let newMonitorType = $state<'DIMENSIONAL' | 'CUSTOM'>('DIMENSIONAL');
	let creatingMonitor = $state(false);

	// Anomaly Subscriptions
	let subscriptions = $state<AnomalySubscription[]>([]);
	let newSubName = $state('');
	let newSubFrequency = $state<'DAILY' | 'IMMEDIATE' | 'WEEKLY'>('DAILY');
	let newSubThreshold = $state(100);
	let creatingSubscription = $state(false);

	// Anomalies
	let anomalies = $state<Anomaly[]>([]);
	let monitorFilter = $state('');
	let feedbackFilter = $state<'ALL' | 'YES' | 'NO' | 'PLANNED_ACTIVITY'>('ALL');
	let loadingAnomalies = $state(false);

	const filteredAnomalies = $derived(
		anomalies.filter((a) => {
			const matchMonitor = monitorFilter === '' || (a.MonitorArn ?? '').includes(monitorFilter);
			return matchMonitor;
		})
	);

	function anomalyScoreBadge(score?: number) {
		const s = score ?? 0;
		if (s >= 0.9)
			return { color: 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900', label: 'High' };
		if (s >= 0.5)
			return {
				color: 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900',
				label: 'Medium'
			};
		return {
			color: 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-900',
			label: 'Low'
		};
	}

	async function loadCategories() {
		loading = true;
		try {
			const res = await ce().send(new ListCostCategoryDefinitionsCommand({}));
			categories = res.CostCategoryReferences ?? [];
		} catch (e) {
			toast.error(`Failed to load cost categories: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function createCategory() {
		if (!newCategoryName.trim()) return;
		creatingCategory = true;
		try {
			await ce().send(
				new CreateCostCategoryDefinitionCommand({
					Name: newCategoryName.trim(),
					RuleVersion: newCategoryRuleVersion as 'CostCategoryExpression.v1',
					Rules: []
				})
			);
			toast.success(`Cost category '${newCategoryName}' created`);
			newCategoryName = '';
			await loadCategories();
		} catch (e) {
			toast.error(`Failed to create category: ${e}`);
		} finally {
			creatingCategory = false;
		}
	}

	async function deleteCategory(arn: string, name: string) {
		try {
			await ce().send(new DeleteCostCategoryDefinitionCommand({ CostCategoryArn: arn }));
			toast.success(`Deleted category '${name}'`);
			await loadCategories();
		} catch (e) {
			toast.error(`Failed to delete category: ${e}`);
		}
	}

	async function loadMonitors() {
		loading = true;
		try {
			const res = await ce().send(new GetAnomalyMonitorsCommand({}));
			monitors = res.AnomalyMonitors ?? [];
		} catch (e) {
			toast.error(`Failed to load anomaly monitors: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function createMonitor() {
		if (!newMonitorName.trim()) return;
		creatingMonitor = true;
		try {
			await ce().send(
				new CreateAnomalyMonitorCommand({
					AnomalyMonitor: {
						MonitorName: newMonitorName.trim(),
						MonitorType: newMonitorType
					}
				})
			);
			toast.success(`Monitor '${newMonitorName}' created`);
			newMonitorName = '';
			await loadMonitors();
		} catch (e) {
			toast.error(`Failed to create monitor: ${e}`);
		} finally {
			creatingMonitor = false;
		}
	}

	async function deleteMonitor(arn: string, name: string) {
		try {
			await ce().send(new DeleteAnomalyMonitorCommand({ MonitorArn: arn }));
			toast.success(`Deleted monitor '${name}'`);
			await loadMonitors();
		} catch (e) {
			toast.error(`Failed to delete monitor: ${e}`);
		}
	}

	async function loadSubscriptions() {
		loading = true;
		try {
			const res = await ce().send(new GetAnomalySubscriptionsCommand({}));
			subscriptions = res.AnomalySubscriptions ?? [];
		} catch (e) {
			toast.error(`Failed to load subscriptions: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function createSubscription() {
		if (!newSubName.trim()) return;
		creatingSubscription = true;
		try {
			await ce().send(
				new CreateAnomalySubscriptionCommand({
					AnomalySubscription: {
						SubscriptionName: newSubName.trim(),
						Frequency: newSubFrequency,
						MonitorArnList: [],
						Subscribers: [],
						ThresholdExpression: undefined
					}
				})
			);
			toast.success(`Subscription '${newSubName}' created`);
			newSubName = '';
			await loadSubscriptions();
		} catch (e) {
			toast.error(`Failed to create subscription: ${e}`);
		} finally {
			creatingSubscription = false;
		}
	}

	async function deleteSubscription(arn: string, name: string) {
		try {
			await ce().send(new DeleteAnomalySubscriptionCommand({ SubscriptionArn: arn }));
			toast.success(`Deleted subscription '${name}'`);
			await loadSubscriptions();
		} catch (e) {
			toast.error(`Failed to delete subscription: ${e}`);
		}
	}

	async function loadAnomalies() {
		loadingAnomalies = true;
		try {
			const params =
				feedbackFilter === 'ALL'
					? { DateInterval: { StartDate: '2020-01-01', EndDate: '2099-12-31' } }
					: {
							DateInterval: { StartDate: '2020-01-01', EndDate: '2099-12-31' },
							Feedback: feedbackFilter
						};
			const res = await ce().send(new GetAnomaliesCommand(params));
			anomalies = res.Anomalies ?? [];
		} catch (e) {
			toast.error(`Failed to load anomalies: ${e}`);
		} finally {
			loadingAnomalies = false;
		}
	}

	async function loadForecast() {
		loadingForecast = true;
		try {
			const res = await ce().send(
				new GetCostForecastCommand({
					TimePeriod: { Start: forecastStart, End: forecastEnd },
					Metric: forecastMetric,
					Granularity: forecastGranularity
				})
			);
			forecastResult = res.Total ? { Total: res.Total } : { Total: { Amount: '0.00', Unit: 'USD' } };
		} catch (e) {
			toast.error(`Failed to load forecast: ${e}`);
		} finally {
			loadingForecast = false;
		}
	}

	async function loadReservations() {
		loadingReservations = true;
		try {
			const res = await ce().send(
				new GetReservationPurchaseRecommendationCommand({
					Service: reservationService,
					LookbackPeriodInDays: reservationLookback,
					TermInYears: reservationTerm,
					PaymentOption: reservationPayment
				})
			);
			reservationRecommendations = res.Recommendations ?? [];
		} catch (e) {
			toast.error(`Failed to load recommendations: ${e}`);
		} finally {
			loadingReservations = false;
		}
	}

	async function switchTab(tab: Tab) {
		activeTab = tab;
		if (tab === 'categories') await loadCategories();
		else if (tab === 'monitors') await loadMonitors();
		else if (tab === 'subscriptions') await loadSubscriptions();
		else if (tab === 'anomalies') await loadAnomalies();
		else if (tab === 'forecasting') await loadForecast();
		else if (tab === 'reservations') await loadReservations();
	}

	onRegionChange(() => {
		loadCategories();
	});
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center gap-3">
		<DollarSign class="h-7 w-7 text-primary" />
		<div>
			<h1 class="text-2xl font-bold">Cost Explorer</h1>
			<p class="text-sm text-muted-foreground">
				Manage cost categories, anomaly monitors, subscriptions, and detected anomalies.
			</p>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex gap-1 border-b">
		{#each [
			{ id: 'categories' as const, label: 'Cost Categories', icon: List },
			{ id: 'monitors' as const, label: 'Anomaly Monitors', icon: AlertTriangle },
			{ id: 'subscriptions' as const, label: 'Subscriptions', icon: Bell },
			{ id: 'anomalies' as const, label: 'Anomalies', icon: Eye },
			{ id: 'forecasting' as const, label: 'Forecasting', icon: TrendingUp },
			{ id: 'reservations' as const, label: 'Reservations', icon: Server }
		] as tab}
			<button
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors flex items-center gap-2 {activeTab ===
				tab.id
					? 'border-primary text-primary'
					: 'border-transparent text-muted-foreground hover:text-foreground'}"
				onclick={() => switchTab(tab.id)}
			>
				<tab.icon class="h-4 w-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Cost Categories Tab -->
	{#if activeTab === 'categories'}
		<div class="space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Cost Category Definitions</h2>
				<button
					class="flex items-center gap-2 px-3 py-1.5 text-sm rounded-md bg-muted hover:bg-muted/80"
					onclick={loadCategories}
					disabled={loading}
				>
					<RefreshCw class="h-4 w-4 {loading ? 'animate-spin' : ''}" />
					Refresh
				</button>
			</div>

			<!-- Create form -->
			<div class="border rounded-lg p-4 space-y-3 bg-muted/30">
				<h3 class="text-sm font-semibold flex items-center gap-2">
					<Plus class="h-4 w-4" /> New Cost Category
				</h3>
				<div class="flex gap-3">
					<input
						class="flex-1 px-3 py-1.5 text-sm border rounded-md bg-background"
						placeholder="Category name"
						bind:value={newCategoryName}
					/>
					<select
						class="px-3 py-1.5 text-sm border rounded-md bg-background"
						bind:value={newCategoryRuleVersion}
					>
						<option value="CostCategoryExpression.v1">v1</option>
					</select>
					<button
						class="px-4 py-1.5 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
						onclick={createCategory}
						disabled={creatingCategory || !newCategoryName.trim()}
					>
						{creatingCategory ? 'Creating…' : 'Create'}
					</button>
				</div>
			</div>

			<!-- List -->
			{#if loading}
				<p class="text-sm text-muted-foreground">Loading…</p>
			{:else if categories.length === 0}
				<p class="text-sm text-muted-foreground">No cost categories defined.</p>
			{:else}
				<div class="border rounded-lg overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="text-left px-4 py-2 font-medium">Name</th>
								<th class="text-left px-4 py-2 font-medium">Effective Start</th>
								<th class="text-left px-4 py-2 font-medium">ARN</th>
								<th class="px-4 py-2"></th>
							</tr>
						</thead>
						<tbody>
							{#each categories as cat}
								<tr class="border-t hover:bg-muted/30">
									<td class="px-4 py-2 font-mono text-xs">{cat.Name ?? '—'}</td>
									<td class="px-4 py-2 text-muted-foreground">{cat.EffectiveStart ?? '—'}</td>
									<td class="px-4 py-2 font-mono text-xs text-muted-foreground truncate max-w-xs"
										>{cat.CostCategoryArn ?? '—'}</td
									>
									<td class="px-4 py-2 text-right">
										<button
											class="text-destructive hover:text-destructive/70"
											onclick={() =>
												deleteCategory(cat.CostCategoryArn ?? '', cat.Name ?? '')}
											title="Delete"
										>
											<Trash2 class="h-4 w-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>

	<!-- Anomaly Monitors Tab -->
	{:else if activeTab === 'monitors'}
		<div class="space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Anomaly Monitors</h2>
				<button
					class="flex items-center gap-2 px-3 py-1.5 text-sm rounded-md bg-muted hover:bg-muted/80"
					onclick={loadMonitors}
					disabled={loading}
				>
					<RefreshCw class="h-4 w-4 {loading ? 'animate-spin' : ''}" />
					Refresh
				</button>
			</div>

			<!-- Create form -->
			<div class="border rounded-lg p-4 space-y-3 bg-muted/30">
				<h3 class="text-sm font-semibold flex items-center gap-2">
					<Plus class="h-4 w-4" /> New Anomaly Monitor
				</h3>
				<div class="flex gap-3">
					<input
						class="flex-1 px-3 py-1.5 text-sm border rounded-md bg-background"
						placeholder="Monitor name"
						bind:value={newMonitorName}
					/>
					<select
						class="px-3 py-1.5 text-sm border rounded-md bg-background"
						bind:value={newMonitorType}
					>
						<option value="DIMENSIONAL">DIMENSIONAL</option>
						<option value="CUSTOM">CUSTOM</option>
					</select>
					<button
						class="px-4 py-1.5 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
						onclick={createMonitor}
						disabled={creatingMonitor || !newMonitorName.trim()}
					>
						{creatingMonitor ? 'Creating…' : 'Create'}
					</button>
				</div>
			</div>

			<!-- List -->
			{#if loading}
				<p class="text-sm text-muted-foreground">Loading…</p>
			{:else if monitors.length === 0}
				<p class="text-sm text-muted-foreground">No anomaly monitors configured.</p>
			{:else}
				<div class="border rounded-lg overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="text-left px-4 py-2 font-medium">Name</th>
								<th class="text-left px-4 py-2 font-medium">Type</th>
								<th class="text-left px-4 py-2 font-medium">ARN</th>
								<th class="px-4 py-2"></th>
							</tr>
						</thead>
						<tbody>
							{#each monitors as mon}
								<tr class="border-t hover:bg-muted/30">
									<td class="px-4 py-2 font-medium">{mon.MonitorName ?? '—'}</td>
									<td class="px-4 py-2">
										<span class="px-2 py-0.5 rounded-full text-xs bg-muted font-mono"
											>{mon.MonitorType ?? '—'}</span
										>
									</td>
									<td class="px-4 py-2 font-mono text-xs text-muted-foreground truncate max-w-xs"
										>{mon.MonitorArn ?? '—'}</td
									>
									<td class="px-4 py-2 text-right">
										<button
											class="text-destructive hover:text-destructive/70"
											onclick={() => deleteMonitor(mon.MonitorArn ?? '', mon.MonitorName ?? '')}
											title="Delete"
										>
											<Trash2 class="h-4 w-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>

	<!-- Subscriptions Tab -->
	{:else if activeTab === 'subscriptions'}
		<div class="space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Anomaly Subscriptions</h2>
				<button
					class="flex items-center gap-2 px-3 py-1.5 text-sm rounded-md bg-muted hover:bg-muted/80"
					onclick={loadSubscriptions}
					disabled={loading}
				>
					<RefreshCw class="h-4 w-4 {loading ? 'animate-spin' : ''}" />
					Refresh
				</button>
			</div>

			<!-- Create form -->
			<div class="border rounded-lg p-4 space-y-3 bg-muted/30">
				<h3 class="text-sm font-semibold flex items-center gap-2">
					<Plus class="h-4 w-4" /> New Subscription
				</h3>
				<div class="flex flex-wrap gap-3">
					<input
						class="flex-1 min-w-48 px-3 py-1.5 text-sm border rounded-md bg-background"
						placeholder="Subscription name"
						bind:value={newSubName}
					/>
					<select
						class="px-3 py-1.5 text-sm border rounded-md bg-background"
						bind:value={newSubFrequency}
					>
						<option value="DAILY">DAILY</option>
						<option value="IMMEDIATE">IMMEDIATE</option>
						<option value="WEEKLY">WEEKLY</option>
					</select>
					<input
						type="number"
						class="w-32 px-3 py-1.5 text-sm border rounded-md bg-background"
						placeholder="Threshold ($)"
						bind:value={newSubThreshold}
						min="0"
					/>
					<button
						class="px-4 py-1.5 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
						onclick={createSubscription}
						disabled={creatingSubscription || !newSubName.trim()}
					>
						{creatingSubscription ? 'Creating…' : 'Create'}
					</button>
				</div>
			</div>

			<!-- List -->
			{#if loading}
				<p class="text-sm text-muted-foreground">Loading…</p>
			{:else if subscriptions.length === 0}
				<p class="text-sm text-muted-foreground">No anomaly subscriptions configured.</p>
			{:else}
				<div class="border rounded-lg overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="text-left px-4 py-2 font-medium">Name</th>
								<th class="text-left px-4 py-2 font-medium">Frequency</th>
								<th class="text-left px-4 py-2 font-medium">Threshold</th>
								<th class="px-4 py-2"></th>
							</tr>
						</thead>
						<tbody>
							{#each subscriptions as sub}
								<tr class="border-t hover:bg-muted/30">
									<td class="px-4 py-2 font-medium">{sub.SubscriptionName ?? '—'}</td>
									<td class="px-4 py-2">
										<span class="px-2 py-0.5 rounded-full text-xs bg-muted font-mono"
											>{sub.Frequency ?? '—'}</span
										>
									</td>
									<td class="px-4 py-2 text-muted-foreground">
										{sub.Threshold != null ? `$${sub.Threshold}` : '—'}
									</td>
									<td class="px-4 py-2 text-right">
										<button
											class="text-destructive hover:text-destructive/70"
											onclick={() =>
												deleteSubscription(
													sub.SubscriptionArn ?? '',
													sub.SubscriptionName ?? ''
												)}
											title="Delete"
										>
											<Trash2 class="h-4 w-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>

	<!-- Anomalies Tab -->
	{:else if activeTab === 'anomalies'}
		<div class="space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Detected Anomalies</h2>
				<button
					class="flex items-center gap-2 px-3 py-1.5 text-sm rounded-md bg-muted hover:bg-muted/80"
					onclick={loadAnomalies}
					disabled={loadingAnomalies}
				>
					<RefreshCw class="h-4 w-4 {loadingAnomalies ? 'animate-spin' : ''}" />
					Refresh
				</button>
			</div>

			<!-- Filters -->
			<div class="flex flex-wrap gap-3 items-center text-sm">
				<Filter class="h-4 w-4 text-muted-foreground" />
				<input
					class="px-3 py-1.5 border rounded-md bg-background"
					placeholder="Filter by monitor ARN"
					bind:value={monitorFilter}
				/>
				<select
					class="px-3 py-1.5 border rounded-md bg-background"
					bind:value={feedbackFilter}
					onchange={loadAnomalies}
				>
					<option value="ALL">All feedback</option>
					<option value="YES">Confirmed (YES)</option>
					<option value="NO">Not an anomaly (NO)</option>
					<option value="PLANNED_ACTIVITY">Planned activity</option>
				</select>
			</div>

			{#if loadingAnomalies}
				<p class="text-sm text-muted-foreground">Loading…</p>
			{:else if filteredAnomalies.length === 0}
				<p class="text-sm text-muted-foreground">No anomalies found.</p>
			{:else}
				<div class="border rounded-lg overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="text-left px-4 py-2 font-medium">ID</th>
								<th class="text-left px-4 py-2 font-medium">Start Date</th>
								<th class="text-left px-4 py-2 font-medium">End Date</th>
								<th class="text-left px-4 py-2 font-medium">Score</th>
								<th class="text-left px-4 py-2 font-medium">Impact</th>
								<th class="text-left px-4 py-2 font-medium">Feedback</th>
							</tr>
						</thead>
						<tbody>
							{#each filteredAnomalies as a}
								{@const badge = anomalyScoreBadge(a.AnomalyScore?.MaxScore)}
								<tr class="border-t hover:bg-muted/30">
									<td class="px-4 py-2 font-mono text-xs">{a.AnomalyId ?? '—'}</td>
									<td class="px-4 py-2 text-muted-foreground">{a.AnomalyStartDate ?? '—'}</td>
									<td class="px-4 py-2 text-muted-foreground">{a.AnomalyEndDate ?? '—'}</td>
									<td class="px-4 py-2">
										<span class="px-2 py-0.5 rounded-full text-xs font-medium {badge.color}"
											>{badge.label}</span
										>
									</td>
									<td class="px-4 py-2">
										{#if a.Impact?.TotalActualSpend != null}
											${a.Impact.TotalActualSpend.toFixed(2)}
										{:else}
											—
										{/if}
									</td>
									<td class="px-4 py-2 text-muted-foreground">{a.Feedback ?? '—'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Forecasting Tab -->
	{#if activeTab === 'forecasting'}
		<div class="space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Cost Forecast</h2>
			</div>
			<div class="border rounded-lg p-4 space-y-4 bg-muted/30">
				<h3 class="text-sm font-semibold flex items-center gap-2">
					<TrendingUp class="h-4 w-4" /> Forecast Parameters
				</h3>
				<div class="grid grid-cols-2 gap-3">
					<div class="space-y-1">
						<label class="text-xs text-muted-foreground">Start Date</label>
						<input
							type="date"
							class="w-full px-3 py-1.5 text-sm border rounded-md bg-background"
							bind:value={forecastStart}
						/>
					</div>
					<div class="space-y-1">
						<label class="text-xs text-muted-foreground">End Date</label>
						<input
							type="date"
							class="w-full px-3 py-1.5 text-sm border rounded-md bg-background"
							bind:value={forecastEnd}
						/>
					</div>
					<div class="space-y-1">
						<label class="text-xs text-muted-foreground">Metric</label>
						<select
							class="w-full px-3 py-1.5 text-sm border rounded-md bg-background"
							bind:value={forecastMetric}
						>
							<option value="BLENDED_COST">Blended Cost</option>
							<option value="UNBLENDED_COST">Unblended Cost</option>
							<option value="AMORTIZED_COST">Amortized Cost</option>
							<option value="NET_AMORTIZED_COST">Net Amortized Cost</option>
						</select>
					</div>
					<div class="space-y-1">
						<label class="text-xs text-muted-foreground">Granularity</label>
						<select
							class="w-full px-3 py-1.5 text-sm border rounded-md bg-background"
							bind:value={forecastGranularity}
						>
							<option value="DAILY">Daily</option>
							<option value="MONTHLY">Monthly</option>
						</select>
					</div>
				</div>
				<button
					class="px-4 py-2 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90"
					onclick={loadForecast}
					disabled={loadingForecast}
				>
					{loadingForecast ? 'Loading...' : 'Get Forecast'}
				</button>
			</div>
			{#if forecastResult}
				<div class="border rounded-lg p-4 bg-muted/10">
					<h3 class="text-sm font-semibold mb-3">Forecast Result</h3>
					<div class="text-2xl font-bold text-primary">
						{forecastResult.Total?.Unit ?? 'USD'}
						{forecastResult.Total?.Amount ?? '0.00'}
					</div>
					<p class="text-xs text-muted-foreground mt-1">
						Total forecasted cost for {forecastStart} to {forecastEnd}
					</p>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Reservations Tab -->
	{#if activeTab === 'reservations'}
		<div class="space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Reservation Purchase Recommendations</h2>
			</div>
			<div class="border rounded-lg p-4 space-y-4 bg-muted/30">
				<h3 class="text-sm font-semibold flex items-center gap-2">
					<Server class="h-4 w-4" /> Recommendation Parameters
				</h3>
				<div class="grid grid-cols-2 gap-3">
					<div class="space-y-1">
						<label class="text-xs text-muted-foreground">Service</label>
						<select
							class="w-full px-3 py-1.5 text-sm border rounded-md bg-background"
							bind:value={reservationService}
						>
							<option value="Amazon EC2">Amazon EC2</option>
							<option value="Amazon RDS">Amazon RDS</option>
							<option value="Amazon ElastiCache">Amazon ElastiCache</option>
							<option value="Amazon Redshift">Amazon Redshift</option>
							<option value="Amazon OpenSearch">Amazon OpenSearch</option>
						</select>
					</div>
					<div class="space-y-1">
						<label class="text-xs text-muted-foreground">Lookback Period</label>
						<select
							class="w-full px-3 py-1.5 text-sm border rounded-md bg-background"
							bind:value={reservationLookback}
						>
							<option value="SEVEN_DAYS">7 Days</option>
							<option value="THIRTY_DAYS">30 Days</option>
							<option value="SIXTY_DAYS">60 Days</option>
						</select>
					</div>
					<div class="space-y-1">
						<label class="text-xs text-muted-foreground">Term</label>
						<select
							class="w-full px-3 py-1.5 text-sm border rounded-md bg-background"
							bind:value={reservationTerm}
						>
							<option value="ONE_YEAR">1 Year</option>
							<option value="THREE_YEARS">3 Years</option>
						</select>
					</div>
					<div class="space-y-1">
						<label class="text-xs text-muted-foreground">Payment Option</label>
						<select
							class="w-full px-3 py-1.5 text-sm border rounded-md bg-background"
							bind:value={reservationPayment}
						>
							<option value="NO_UPFRONT">No Upfront</option>
							<option value="PARTIAL_UPFRONT">Partial Upfront</option>
							<option value="ALL_UPFRONT">All Upfront</option>
						</select>
					</div>
				</div>
				<button
					class="px-4 py-2 text-sm rounded-md bg-primary text-primary-foreground hover:bg-primary/90"
					onclick={loadReservations}
					disabled={loadingReservations}
				>
					{loadingReservations ? 'Loading...' : 'Get Recommendations'}
				</button>
			</div>
			{#if reservationRecommendations.length === 0}
				<p class="text-sm text-muted-foreground text-center py-8">
					No reservation recommendations found. Click "Get Recommendations" to fetch.
				</p>
			{:else}
				<div class="border rounded-lg overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="text-left px-4 py-2 font-medium">Account Scope</th>
								<th class="text-left px-4 py-2 font-medium">Term</th>
								<th class="text-left px-4 py-2 font-medium">Payment Option</th>
								<th class="text-left px-4 py-2 font-medium">Recommendations</th>
							</tr>
						</thead>
						<tbody>
							{#each reservationRecommendations as rec}
								{@const r = rec as Record<string, unknown>}
								<tr class="border-t hover:bg-muted/30">
									<td class="px-4 py-2">{String(r['AccountScope'] ?? '—')}</td>
									<td class="px-4 py-2">{String(r['TermInYears'] ?? '—')}</td>
									<td class="px-4 py-2">{String(r['PaymentOption'] ?? '—')}</td>
									<td class="px-4 py-2">{((r['RecommendationDetails'] as unknown[]) ?? []).length} items</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}
</div>
