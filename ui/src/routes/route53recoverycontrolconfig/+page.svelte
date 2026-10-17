<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '#lib/region-effect.svelte.ts';
	import { getRoute53RecoveryControlConfigClient } from '#lib/aws-client.ts';
	import { currentRegion } from '#lib/region.svelte.ts';
	import { describeError, statusBadgeClass, withClusterEndpoints } from '#lib/arc.ts';
	import {
		ListClustersCommand,
		CreateClusterCommand,
		DeleteClusterCommand,
		ListControlPanelsCommand,
		CreateControlPanelCommand,
		DeleteControlPanelCommand,
		ListRoutingControlsCommand,
		CreateRoutingControlCommand,
		DeleteRoutingControlCommand,
		ListSafetyRulesCommand,
		CreateSafetyRuleCommand,
		DeleteSafetyRuleCommand,
		type Cluster,
		type ControlPanel,
		type RoutingControl,
		type Rule
	} from '@aws-sdk/client-route53-recovery-control-config';
	import {
		ListRoutingControlsCommand as ListStatesCommand,
		UpdateRoutingControlStateCommand
	} from '@aws-sdk/client-route53-recovery-cluster';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { createTabLoader } from '#lib/tab-loader.svelte.ts';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '#lib/components/Tabs.svelte';
	import SearchInput from '#lib/components/SearchInput.svelte';
	import RegionChip from '#lib/components/RegionChip.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { ToggleRight, Plus, Trash2 } from 'lucide-svelte';

	const client = regionalClient(getRoute53RecoveryControlConfigClient);

	type TabId = 'clusters' | 'panels' | 'controls' | 'rules';
	type FlatRule = {
		kind: 'ASSERTION' | 'GATING';
		arn: string;
		name: string;
		status?: string;
		threshold?: number;
		type?: string;
		inverted?: boolean;
		waitMs?: number;
		controls: string[];
		targets: string[];
	};

	const tabs: TabDef[] = [
		{ id: 'clusters', label: 'Clusters' },
		{ id: 'panels', label: 'Control Panels' },
		{ id: 'controls', label: 'Routing Controls' },
		{ id: 'rules', label: 'Safety Rules' }
	];

	const btn =
		'flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 text-sm';
	const cancelBtn =
		'rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800';
	const primaryBtn =
		'rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50';
	const input =
		'px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full';
	const th = 'px-4 py-2 font-medium';
	const td = 'px-4 py-3 text-gray-700 dark:text-gray-300';

	let activeTab = $state<TabId>('clusters');
	let searchQuery = $state('');

	let clusters = $state<Cluster[]>([]);
	let panels = $state<ControlPanel[]>([]);
	let controls = $state<RoutingControl[]>([]);
	let rules = $state<FlatRule[]>([]);
	let selectedPanelArn = $state('');
	let states = $state<Record<string, string>>({});
	let statesError = $state<string | null>(null);
	let flipError = $state<string | null>(null);
	let flipping = $state<string | null>(null);
	let expandedCluster = $state<string | null>(null);

	async function fetchClusters(): Promise<void> {
		const out: Cluster[] = [];
		let token: string | undefined;
		do {
			const resp = await client().send(new ListClustersCommand({ NextToken: token }));
			out.push(...(resp.Clusters ?? []));
			token = resp.NextToken;
		} while (token);
		clusters = out;
	}

	async function fetchPanels(): Promise<void> {
		const out: ControlPanel[] = [];
		let token: string | undefined;
		do {
			const resp = await client().send(new ListControlPanelsCommand({ NextToken: token }));
			out.push(...(resp.ControlPanels ?? []));
			token = resp.NextToken;
		} while (token);
		panels = out;
		if (!panels.some((p) => p.ControlPanelArn === selectedPanelArn)) {
			selectedPanelArn = panels[0]?.ControlPanelArn ?? '';
		}
	}

	function clusterOf(panelArn: string): Cluster | undefined {
		const panel = panels.find((p) => p.ControlPanelArn === panelArn);
		return clusters.find((c) => c.ClusterArn === panel?.ClusterArn);
	}

	async function loadStates(): Promise<void> {
		statesError = null;
		const cluster = clusterOf(selectedPanelArn);
		if (!cluster) {
			states = {};
			return;
		}
		try {
			const resp = await withClusterEndpoints(
				cluster.ClusterEndpoints ?? [],
				currentRegion(),
				(c) => c.send(new ListStatesCommand({ ControlPanelArn: selectedPanelArn }))
			);
			const next: Record<string, string> = {};
			for (const rc of resp.RoutingControls ?? []) {
				if (rc.RoutingControlArn) next[rc.RoutingControlArn] = rc.RoutingControlState ?? '';
			}
			states = next;
		} catch (e) {
			states = {};
			statesError = describeError(e);
		}
	}

	async function listControls(panelArn: string): Promise<RoutingControl[]> {
		const out: RoutingControl[] = [];
		let token: string | undefined;
		do {
			const resp = await client().send(
				new ListRoutingControlsCommand({ ControlPanelArn: panelArn, NextToken: token })
			);
			out.push(...(resp.RoutingControls ?? []));
			token = resp.NextToken;
		} while (token);
		return out;
	}

	async function fetchControls(): Promise<void> {
		if (panels.length === 0) await fetchPanels();
		if (clusters.length === 0) await fetchClusters();
		if (!selectedPanelArn) {
			controls = [];
			return;
		}
		controls = await listControls(selectedPanelArn);
		await loadStates();
	}

	function flatten(r: Rule): FlatRule | null {
		if (r.ASSERTION) {
			const a = r.ASSERTION;
			return {
				kind: 'ASSERTION',
				arn: a.SafetyRuleArn ?? '',
				name: a.Name ?? '',
				status: a.Status,
				threshold: a.RuleConfig?.Threshold,
				type: a.RuleConfig?.Type,
				inverted: a.RuleConfig?.Inverted,
				waitMs: a.WaitPeriodMs,
				controls: a.AssertedControls ?? [],
				targets: []
			};
		}
		if (r.GATING) {
			const g = r.GATING;
			return {
				kind: 'GATING',
				arn: g.SafetyRuleArn ?? '',
				name: g.Name ?? '',
				status: g.Status,
				threshold: g.RuleConfig?.Threshold,
				type: g.RuleConfig?.Type,
				inverted: g.RuleConfig?.Inverted,
				waitMs: g.WaitPeriodMs,
				controls: g.GatingControls ?? [],
				targets: g.TargetControls ?? []
			};
		}
		return null;
	}

	async function fetchRules(): Promise<void> {
		if (panels.length === 0) await fetchPanels();
		if (!selectedPanelArn) {
			rules = [];
			return;
		}
		if (controls.length === 0) {
			controls = await listControls(selectedPanelArn);
		}
		const out: FlatRule[] = [];
		let token: string | undefined;
		do {
			const resp = await client().send(
				new ListSafetyRulesCommand({ ControlPanelArn: selectedPanelArn, NextToken: token })
			);
			for (const r of resp.SafetyRules ?? []) {
				const f = flatten(r);
				if (f) out.push(f);
			}
			token = resp.NextToken;
		} while (token);
		rules = out;
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	const tabLoader = createTabLoader<TabId>({
		clusters: () => fetchClusters().catch(rethrowDescribed),
		panels: () => fetchPanels().catch(rethrowDescribed),
		controls: () => fetchControls().catch(rethrowDescribed),
		rules: () => fetchRules().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	async function onPanelSelect(arn: string): Promise<void> {
		selectedPanelArn = arn;
		controls = [];
		rules = [];
		flipError = null;
		await tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		untrack(() => {
			selectedPanelArn = '';
			panels = [];
			controls = [];
			rules = [];
			clusters = [];
			void tabLoader.refreshLoaded(activeTab);
		});
	});

	const q = $derived(searchQuery.toLowerCase());
	const filteredClusters = $derived(
		clusters.filter((c) => (c.Name ?? '').toLowerCase().includes(q))
	);
	const filteredPanels = $derived(
		panels.filter((p) => (p.Name ?? '').toLowerCase().includes(q))
	);
	const filteredControls = $derived(
		controls.filter((c) => (c.Name ?? '').toLowerCase().includes(q))
	);
	const filteredRules = $derived(rules.filter((r) => r.name.toLowerCase().includes(q)));
	const activeTabError = $derived(tabLoader.getError(activeTab));

	function shortName(arn: string): string {
		return controls.find((c) => c.RoutingControlArn === arn)?.Name ?? arn.split('/').at(-1) ?? arn;
	}

	async function flip(rc: RoutingControl): Promise<void> {
		const arn = rc.RoutingControlArn;
		const cluster = clusterOf(selectedPanelArn);
		if (!arn || !cluster) return;
		const next = states[arn] === 'On' ? 'Off' : 'On';
		flipping = arn;
		flipError = null;
		try {
			await withClusterEndpoints(cluster.ClusterEndpoints ?? [], currentRegion(), (c) =>
				c.send(
					new UpdateRoutingControlStateCommand({
						RoutingControlArn: arn,
						RoutingControlState: next
					})
				)
			);
			toast.success(`${rc.Name} set to ${next}`);
			await loadStates();
		} catch (e) {
			const msg = describeError(e);
			flipError = msg;
			toast.error(msg);
		} finally {
			flipping = null;
		}
	}

	// --- create / delete ---

	let createClusterModal = $state<Modal | null>(null);
	let newClusterName = $state('');
	let newClusterNetwork = $state('IPV4');
	let createPanelModal = $state<Modal | null>(null);
	let newPanelName = $state('');
	let newPanelCluster = $state('');
	let createControlModal = $state<Modal | null>(null);
	let newControlName = $state('');
	let createRuleModal = $state<Modal | null>(null);
	let ruleKind = $state<'ASSERTION' | 'GATING'>('ASSERTION');
	let ruleName = $state('');
	let ruleType = $state('ATLEAST');
	let ruleThreshold = $state(1);
	let ruleInverted = $state(false);
	let ruleWaitMs = $state(0);
	let ruleControls = $state<string[]>([]);
	let ruleTargets = $state<string[]>([]);
	let saving = $state(false);
	let modalError = $state<string | null>(null);

	function openModal(m: Modal | null): void {
		modalError = null;
		m?.open();
	}

	async function submit(modal: Modal | null, tab: TabId, work: () => Promise<unknown>): Promise<void> {
		saving = true;
		modalError = null;
		try {
			await work();
			modal?.close();
			await tabLoader.refresh(tab);
		} catch (e) {
			modalError = describeError(e);
			toast.error(modalError);
		} finally {
			saving = false;
		}
	}

	const submitCluster = () =>
		submit(createClusterModal, 'clusters', async () => {
			await client().send(
				new CreateClusterCommand({
					ClusterName: newClusterName,
					NetworkType: newClusterNetwork as 'IPV4' | 'DUALSTACK'
				})
			);
			newClusterName = '';
			toast.success('Cluster created');
		});

	const submitPanel = () =>
		submit(createPanelModal, 'panels', async () => {
			await client().send(
				new CreateControlPanelCommand({
					ControlPanelName: newPanelName,
					ClusterArn: newPanelCluster
				})
			);
			newPanelName = '';
			toast.success('Control panel created');
		});

	const submitControl = () =>
		submit(createControlModal, 'controls', async () => {
			const cluster = clusterOf(selectedPanelArn);
			await client().send(
				new CreateRoutingControlCommand({
					RoutingControlName: newControlName,
					ClusterArn: cluster?.ClusterArn,
					ControlPanelArn: selectedPanelArn
				})
			);
			newControlName = '';
			toast.success('Routing control created');
		});

	const submitRule = () =>
		submit(createRuleModal, 'rules', async () => {
			const RuleConfig = {
				Type: ruleType as 'ATLEAST' | 'AND' | 'OR',
				Threshold: Number(ruleThreshold),
				Inverted: ruleInverted
			};
			if (ruleKind === 'ASSERTION') {
				await client().send(
					new CreateSafetyRuleCommand({
						AssertionRule: {
							Name: ruleName,
							ControlPanelArn: selectedPanelArn,
							AssertedControls: ruleControls,
							RuleConfig,
							WaitPeriodMs: Number(ruleWaitMs)
						}
					})
				);
			} else {
				await client().send(
					new CreateSafetyRuleCommand({
						GatingRule: {
							Name: ruleName,
							ControlPanelArn: selectedPanelArn,
							GatingControls: ruleControls,
							TargetControls: ruleTargets,
							RuleConfig,
							WaitPeriodMs: Number(ruleWaitMs)
						}
					})
				);
			}
			ruleName = '';
			toast.success('Safety rule created');
		});

	async function remove(
		title: string,
		message: string,
		tab: TabId,
		work: () => Promise<unknown>
	): Promise<void> {
		if (!(await confirmDestructive({ title, message }))) return;
		try {
			await work();
			toast.success(`${title} succeeded`);
			await tabLoader.refresh(tab);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	const delCluster = (c: Cluster) =>
		remove('Delete cluster', `Delete cluster ${c.Name}?`, 'clusters', () =>
			client().send(new DeleteClusterCommand({ ClusterArn: c.ClusterArn }))
		);
	const delPanel = (p: ControlPanel) =>
		remove('Delete control panel', `Delete control panel ${p.Name}?`, 'panels', () =>
			client().send(new DeleteControlPanelCommand({ ControlPanelArn: p.ControlPanelArn }))
		);
	const delControl = (c: RoutingControl) =>
		remove('Delete routing control', `Delete routing control ${c.Name}?`, 'controls', () =>
			client().send(new DeleteRoutingControlCommand({ RoutingControlArn: c.RoutingControlArn }))
		);
	const delRule = (r: FlatRule) =>
		remove('Delete safety rule', `Delete safety rule ${r.name}?`, 'rules', () =>
			client().send(new DeleteSafetyRuleCommand({ SafetyRuleArn: r.arn }))
		);

	function toggleIn(list: string[], arn: string, on: boolean): string[] {
		return on ? [...list.filter((a) => a !== arn), arn] : list.filter((a) => a !== arn);
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={ToggleRight}
		title="Route 53 Recovery Control"
		description="Clusters, control panels, routing controls and safety rules"
		onRefresh={handleRefresh}
		color="emerald"
	>
		{#snippet actions()}
			{#if activeTab === 'clusters'}
				<button onclick={() => openModal(createClusterModal)} class={btn}>
					<Plus class="w-4 h-4" /> Create cluster
				</button>
			{:else if activeTab === 'panels'}
				<button
					onclick={() => {
						newPanelCluster = clusters[0]?.ClusterArn ?? '';
						openModal(createPanelModal);
					}}
					class={btn}
				>
					<Plus class="w-4 h-4" /> Create control panel
				</button>
			{:else if activeTab === 'controls'}
				<button
					onclick={() => openModal(createControlModal)}
					disabled={!selectedPanelArn}
					class="{btn} disabled:opacity-50"
				>
					<Plus class="w-4 h-4" /> Create routing control
				</button>
			{:else}
				<button
					onclick={() => openModal(createRuleModal)}
					disabled={!selectedPanelArn}
					class="{btn} disabled:opacity-50"
				>
					<Plus class="w-4 h-4" /> Create safety rule
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="emerald" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTab === 'controls' || activeTab === 'rules'}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="panel-select" class="text-sm text-gray-500 dark:text-gray-400"
						>Control panel</label
					>
					<select
						id="panel-select"
						value={selectedPanelArn}
						onchange={(e) => onPanelSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md"
					>
						{#if panels.length === 0}
							<option value="">No control panels</option>
						{/if}
						{#each panels as p (p.ControlPanelArn)}
							<option value={p.ControlPanelArn}>{p.Name}</option>
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

			{#if activeTab === 'controls' && flipError}
				<div
					role="alert"
					data-testid="flip-error"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Routing control state change rejected</p>
					<p>{flipError}</p>
				</div>
			{/if}

			{#if activeTab === 'controls' && statesError}
				<div
					role="alert"
					class="rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-4 py-3 text-sm text-amber-700 dark:text-amber-300"
				>
					<p class="font-medium">Live state unavailable</p>
					<p>{statesError}</p>
				</div>
			{/if}

			{#if tabLoader.isLoading(activeTab)}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'clusters'}
				{#if filteredClusters.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No clusters found</div>
				{:else}
					<div class="overflow-x-auto">
						<table class="w-full text-sm">
							<thead>
								<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
									<th class={th}>Name</th><th class={th}>Status</th><th class={th}>Network</th>
									<th class={th}>Region</th><th class={th}>Endpoints</th><th class={th}></th>
								</tr>
							</thead>
							<tbody>
								{#each filteredClusters as c (c.ClusterArn)}
									<tr class="border-b border-gray-100 dark:border-gray-800">
										<td class={td}>
											<div class="font-medium">{c.Name}</div>
											<div class="text-xs text-gray-400 break-all">{c.ClusterArn}</div>
										</td>
										<td class={td}>
											<span class="text-xs px-2 py-1 rounded-full {statusBadgeClass(c.Status)}">{c.Status ?? '—'}</span>
										</td>
										<td class={td}>{c.NetworkType ?? '—'}</td>
										<td class={td}><RegionChip region={currentRegion()} /></td>
										<td class={td}>
											<button
												class="text-emerald-600 hover:underline"
												onclick={() => (expandedCluster = expandedCluster === c.ClusterArn ? null : (c.ClusterArn ?? null))}
												aria-label="Toggle endpoints for {c.Name}"
											>
												{(c.ClusterEndpoints ?? []).length} endpoints
											</button>
										</td>
										<td class={td}>
											<button
												onclick={() => delCluster(c)}
												title="Delete"
												aria-label="Delete cluster {c.Name}"
												class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
											>
										</td>
									</tr>
									{#if expandedCluster === c.ClusterArn}
										<tr class="border-b border-gray-100 dark:border-gray-800 bg-slate-50 dark:bg-slate-900/40">
											<td colspan="6" class="px-4 py-3">
												<table class="w-full text-xs">
													<thead>
														<tr class="text-left text-gray-500"><th class="py-1">Region</th><th class="py-1">Endpoint</th></tr>
													</thead>
													<tbody>
														{#each c.ClusterEndpoints ?? [] as ep (ep.Region)}
															<tr><td class="py-1">{ep.Region}</td><td class="py-1 break-all font-mono">{ep.Endpoint}</td></tr>
														{/each}
													</tbody>
												</table>
											</td>
										</tr>
									{/if}
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			{:else if activeTab === 'panels'}
				{#if filteredPanels.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No control panels found</div>
				{:else}
					<div class="overflow-x-auto">
						<table class="w-full text-sm">
							<thead>
								<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
									<th class={th}>Name</th><th class={th}>Status</th><th class={th}>Routing controls</th>
									<th class={th}>Default</th><th class={th}>Cluster</th><th class={th}></th>
								</tr>
							</thead>
							<tbody>
								{#each filteredPanels as p (p.ControlPanelArn)}
									<tr class="border-b border-gray-100 dark:border-gray-800">
										<td class={td}>
											<div class="font-medium">{p.Name}</div>
											<div class="text-xs text-gray-400 break-all">{p.ControlPanelArn}</div>
										</td>
										<td class={td}>
											<span class="text-xs px-2 py-1 rounded-full {statusBadgeClass(p.Status)}">{p.Status ?? '—'}</span>
										</td>
										<td class={td}>{p.RoutingControlCount ?? 0}</td>
										<td class={td}>{p.DefaultControlPanel ? 'Yes' : 'No'}</td>
										<td class="{td} text-xs break-all">{clusters.find((c) => c.ClusterArn === p.ClusterArn)?.Name ?? p.ClusterArn}</td>
										<td class={td}>
											<button
												onclick={() => delPanel(p)}
												title="Delete"
												aria-label="Delete control panel {p.Name}"
												class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
											>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			{:else if activeTab === 'controls'}
				{#if filteredControls.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No routing controls found</div>
				{:else}
					<div class="overflow-x-auto">
						<table class="w-full text-sm">
							<thead>
								<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
									<th class={th}>Name</th><th class={th}>Config status</th><th class={th}>Live state</th>
									<th class={th}>Change state</th><th class={th}></th>
								</tr>
							</thead>
							<tbody>
								{#each filteredControls as rc (rc.RoutingControlArn)}
									{@const state = states[rc.RoutingControlArn ?? '']}
									<tr class="border-b border-gray-100 dark:border-gray-800">
										<td class={td}>
											<div class="font-medium">{rc.Name}</div>
											<div class="text-xs text-gray-400 break-all">{rc.RoutingControlArn}</div>
										</td>
										<td class={td}>
											<span class="text-xs px-2 py-1 rounded-full {statusBadgeClass(rc.Status)}">{rc.Status ?? '—'}</span>
										</td>
										<td class={td}>
											<span
												data-testid="state-{rc.Name}"
												class="text-xs px-2 py-1 rounded-full {statusBadgeClass(state)}"
												>{state || 'Unknown'}</span
											>
										</td>
										<td class={td}>
											<button
												onclick={() => flip(rc)}
												disabled={!state || flipping === rc.RoutingControlArn}
												aria-label="Turn {rc.Name} {state === 'On' ? 'Off' : 'On'}"
												class="px-3 py-1 rounded-lg border border-emerald-600 text-emerald-700 dark:text-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-900/20 text-xs disabled:opacity-50"
											>
												Turn {state === 'On' ? 'Off' : 'On'}
											</button>
										</td>
										<td class={td}>
											<button
												onclick={() => delControl(rc)}
												title="Delete"
												aria-label="Delete routing control {rc.Name}"
												class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
											>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			{:else if filteredRules.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">No safety rules found</div>
			{:else}
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead>
							<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
								<th class={th}>Name</th><th class={th}>Kind</th><th class={th}>Status</th>
								<th class={th}>Rule</th><th class={th}>Controls</th><th class={th}>Wait</th><th class={th}></th>
							</tr>
						</thead>
						<tbody>
							{#each filteredRules as r (r.arn)}
								<tr class="border-b border-gray-100 dark:border-gray-800">
									<td class={td}>
										<div class="font-medium">{r.name}</div>
										<div class="text-xs text-gray-400 break-all">{r.arn}</div>
									</td>
									<td class={td}>{r.kind === 'ASSERTION' ? 'Assertion' : 'Gating'}</td>
									<td class={td}>
										<span class="text-xs px-2 py-1 rounded-full {statusBadgeClass(r.status)}">{r.status ?? '—'}</span>
									</td>
									<td class={td}>{r.type}{r.type === 'ATLEAST' ? ` ${r.threshold}` : ''}{r.inverted ? ' (inverted)' : ''}</td>
									<td class="{td} text-xs">
										{r.kind === 'GATING' ? 'Gating: ' : 'Asserted: '}{r.controls.map(shortName).join(', ')}
										{#if r.kind === 'GATING'}<br />Targets: {r.targets.map(shortName).join(', ')}{/if}
									</td>
									<td class={td}>{r.waitMs ?? 0} ms</td>
									<td class={td}>
										<button
											onclick={() => delRule(r)}
											title="Delete"
											aria-label="Delete safety rule {r.name}"
											class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
										>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	</div>
</div>

{#snippet errorLine()}
	{#if modalError}
		<p class="text-sm text-red-600 dark:text-red-400">{modalError}</p>
	{/if}
{/snippet}

<Modal bind:this={createClusterModal} title="Create Cluster">
	{#snippet children()}
		<div class="space-y-3">
			<label class="block text-sm" for="cluster-name">Cluster name</label>
			<input id="cluster-name" class={input} bind:value={newClusterName} />
			<label class="block text-sm" for="cluster-network">Network type</label>
			<select id="cluster-network" class={input} bind:value={newClusterNetwork}>
				<option value="IPV4">IPV4</option>
				<option value="DUALSTACK">DUALSTACK</option>
			</select>
			{@render errorLine()}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createClusterModal?.close()} class={cancelBtn}>Cancel</button>
		<button type="button" onclick={submitCluster} disabled={saving || !newClusterName} class={primaryBtn}
			>{saving ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createPanelModal} title="Create Control Panel">
	{#snippet children()}
		<div class="space-y-3">
			<label class="block text-sm" for="panel-name">Control panel name</label>
			<input id="panel-name" class={input} bind:value={newPanelName} />
			<label class="block text-sm" for="panel-cluster">Cluster</label>
			<select id="panel-cluster" class={input} bind:value={newPanelCluster}>
				{#each clusters as c (c.ClusterArn)}
					<option value={c.ClusterArn}>{c.Name}</option>
				{/each}
			</select>
			{@render errorLine()}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createPanelModal?.close()} class={cancelBtn}>Cancel</button>
		<button type="button" onclick={submitPanel} disabled={saving || !newPanelName || !newPanelCluster} class={primaryBtn}
			>{saving ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createControlModal} title="Create Routing Control">
	{#snippet children()}
		<div class="space-y-3">
			<label class="block text-sm" for="control-name">Routing control name</label>
			<input id="control-name" class={input} bind:value={newControlName} />
			{@render errorLine()}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createControlModal?.close()} class={cancelBtn}>Cancel</button>
		<button type="button" onclick={submitControl} disabled={saving || !newControlName} class={primaryBtn}
			>{saving ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createRuleModal} title="Create Safety Rule">
	{#snippet children()}
		<div class="space-y-3">
			<label class="block text-sm" for="rule-kind">Rule kind</label>
			<select id="rule-kind" class={input} bind:value={ruleKind}>
				<option value="ASSERTION">Assertion rule</option>
				<option value="GATING">Gating rule</option>
			</select>
			<label class="block text-sm" for="rule-name">Name</label>
			<input id="rule-name" class={input} bind:value={ruleName} />
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label class="block text-sm" for="rule-type">Rule type</label>
					<select id="rule-type" class={input} bind:value={ruleType}>
						<option value="ATLEAST">ATLEAST</option>
						<option value="AND">AND</option>
						<option value="OR">OR</option>
					</select>
				</div>
				<div>
					<label class="block text-sm" for="rule-threshold">Threshold</label>
					<input id="rule-threshold" type="number" min="0" class={input} bind:value={ruleThreshold} />
				</div>
			</div>
			<label class="flex items-center gap-2 text-sm">
				<input type="checkbox" bind:checked={ruleInverted} /> Inverted
			</label>
			<label class="block text-sm" for="rule-wait">Wait period (ms)</label>
			<input id="rule-wait" type="number" min="0" class={input} bind:value={ruleWaitMs} />
			<fieldset>
				<legend class="text-sm">{ruleKind === 'ASSERTION' ? 'Asserted controls' : 'Gating controls'}</legend>
				{#each controls as rc (rc.RoutingControlArn)}
					<label class="flex items-center gap-2 text-sm">
						<input
							type="checkbox"
							checked={ruleControls.includes(rc.RoutingControlArn ?? '')}
							onchange={(e) => (ruleControls = toggleIn(ruleControls, rc.RoutingControlArn ?? '', (e.target as HTMLInputElement).checked))}
						/>
						{rc.Name}
					</label>
				{/each}
			</fieldset>
			{#if ruleKind === 'GATING'}
				<fieldset>
					<legend class="text-sm">Target controls</legend>
					{#each controls as rc (rc.RoutingControlArn)}
						<label class="flex items-center gap-2 text-sm">
							<input
								type="checkbox"
								checked={ruleTargets.includes(rc.RoutingControlArn ?? '')}
								onchange={(e) => (ruleTargets = toggleIn(ruleTargets, rc.RoutingControlArn ?? '', (e.target as HTMLInputElement).checked))}
							/>
							{rc.Name}
						</label>
					{/each}
				</fieldset>
			{/if}
			{@render errorLine()}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createRuleModal?.close()} class={cancelBtn}>Cancel</button>
		<button type="button" onclick={submitRule} disabled={saving || !ruleName || ruleControls.length === 0} class={primaryBtn}
			>{saving ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>
