<script lang="ts">
	import { onMount } from 'svelte';
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
		Filter
	} from 'lucide-svelte';

	const ce = getCostExplorerClient();

	type Tab = 'categories' | 'monitors' | 'subscriptions' | 'anomalies';

	let activeTab = $state<Tab>('categories');
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
			const res = await ce.send(new ListCostCategoryDefinitionsCommand({}));
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
			await ce.send(
				new CreateCostCategoryDefinitionCommand({
					Name: newCategoryName.trim(),
					RuleVersion: newCategoryRuleVersion,
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
			await ce.send(new DeleteCostCategoryDefinitionCommand({ CostCategoryArn: arn }));
			toast.success(`Deleted category '${name}'`);
			await loadCategories();
		} catch (e) {
			toast.error(`Failed to delete category: ${e}`);
		}
	}

	async function loadMonitors() {
		loading = true;
		try {
			const res = await ce.send(new GetAnomalyMonitorsCommand({}));
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
			await ce.send(
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
			await ce.send(new DeleteAnomalyMonitorCommand({ MonitorArn: arn }));
			toast.success(`Deleted monitor '${name}'`);
			await loadMonitors();
		} catch (e) {
			toast.error(`Failed to delete monitor: ${e}`);
		}
	}

	async function loadSubscriptions() {
		loading = true;
		try {
			const res = await ce.send(new GetAnomalySubscriptionsCommand({}));
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
			await ce.send(
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
			await ce.send(new DeleteAnomalySubscriptionCommand({ SubscriptionArn: arn }));
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
			const res = await ce.send(new GetAnomaliesCommand(params));
			anomalies = res.Anomalies ?? [];
		} catch (e) {
			toast.error(`Failed to load anomalies: ${e}`);
		} finally {
			loadingAnomalies = false;
		}
	}

	async function switchTab(tab: Tab) {
		activeTab = tab;
		if (tab === 'categories') await loadCategories();
		else if (tab === 'monitors') await loadMonitors();
		else if (tab === 'subscriptions') await loadSubscriptions();
		else if (tab === 'anomalies') await loadAnomalies();
	}

	onMount(() => {
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
			{ id: 'anomalies' as const, label: 'Anomalies', icon: Eye }
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
									<td class="px-4 py-2 text-muted-foreground">{a.Feedback?.FeedbackType ?? '—'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}
</div>
