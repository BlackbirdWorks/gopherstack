<script lang="ts">
	import { onMount } from 'svelte';
	import { getEventBridgeClient } from '$lib/aws-client';
	import {
		ListEventBusesCommand,
		CreateEventBusCommand,
		DeleteEventBusCommand,
		ListRulesCommand,
		PutRuleCommand,
		DeleteRuleCommand,
		PutEventsCommand,
		type EventBus,
		type Rule,
		type PutRuleCommandInput
	} from '@aws-sdk/client-eventbridge';
	import { toast } from 'svelte-sonner';
	import { Zap, Search, RefreshCw, Plus, Trash2, Send, List, Bus } from 'lucide-svelte';

	const eb = getEventBridgeClient();

	let loading = $state(false);
	let buses = $state<EventBus[]>([]);
	let searchQuery = $state('');
	let selectedBus = $state<EventBus | null>(null);
	let rules = $state<Rule[]>([]);
	let loadingRules = $state(false);
	let activeTab = $state<'buses' | 'rules'>('buses');

	// Create bus modal
	let showCreateBusModal = $state(false);
	let creatingBus = $state(false);
	let newBusName = $state('');

	// Create rule modal
	let showCreateRuleModal = $state(false);
	let creatingRule = $state(false);
	let newRuleName = $state('');
	let newRuleType = $state<'schedule' | 'pattern'>('schedule');
	let newRuleSchedule = $state('rate(5 minutes)');
	let newRulePattern = $state('{\n  "source": ["my.app"]\n}');
	let newRuleState = $state<'ENABLED' | 'DISABLED'>('ENABLED');

	// Put events modal
	let showPutEventsModal = $state(false);
	let puttingEvents = $state(false);
	let evtSource = $state('my.application');
	let evtDetailType = $state('UserAction');
	let evtDetail = $state('{\n  "action": "login",\n  "userId": "u-123"\n}');
	let evtBusName = $state('default');

	const filteredBuses = $derived(
		buses.filter((b) => (b.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function loadBuses() {
		loading = true;
		try {
			const res = await eb.send(new ListEventBusesCommand({ Limit: 100 }));
			buses = res.EventBuses ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load event buses: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createBus() {
		if (!newBusName.trim()) return;
		creatingBus = true;
		try {
			await eb.send(new CreateEventBusCommand({ Name: newBusName.trim() }));
			toast.success(`Event bus "${newBusName.trim()}" created`);
			showCreateBusModal = false;
			newBusName = '';
			await loadBuses();
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creatingBus = false;
		}
	}

	async function deleteBus(name: string) {
		if (!confirm(`Delete event bus "${name}"?`)) return;
		try {
			await eb.send(new DeleteEventBusCommand({ Name: name }));
			toast.success(`Event bus "${name}" deleted`);
			if (selectedBus?.Name === name) { selectedBus = null; rules = []; }
			await loadBuses();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function selectBus(bus: EventBus) {
		selectedBus = bus;
		activeTab = 'rules';
		await loadRules(bus.Name ?? 'default');
	}

	async function loadRules(busName: string) {
		loadingRules = true;
		try {
			const res = await eb.send(new ListRulesCommand({ EventBusName: busName, Limit: 100 }));
			rules = res.Rules ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load rules: ${(err as Error).message}`);
		} finally {
			loadingRules = false;
		}
	}

	async function createRule() {
		if (!newRuleName.trim() || !selectedBus) return;
		creatingRule = true;
		try {
			const params: PutRuleCommandInput = {
				Name: newRuleName.trim(),
				EventBusName: selectedBus.Name,
				State: newRuleState
			};
			if (newRuleType === 'schedule') {
				params.ScheduleExpression = newRuleSchedule;
			} else {
				params.EventPattern = newRulePattern;
			}
			await eb.send(new PutRuleCommand(params));
			toast.success(`Rule "${newRuleName.trim()}" created`);
			showCreateRuleModal = false;
			newRuleName = '';
			await loadRules(selectedBus.Name ?? 'default');
		} catch (err: unknown) {
			toast.error(`Create rule failed: ${(err as Error).message}`);
		} finally {
			creatingRule = false;
		}
	}

	async function deleteRule(ruleName: string) {
		if (!selectedBus || !confirm(`Delete rule "${ruleName}"?`)) return;
		try {
			await eb.send(new DeleteRuleCommand({ Name: ruleName, EventBusName: selectedBus.Name }));
			toast.success(`Rule "${ruleName}" deleted`);
			await loadRules(selectedBus.Name ?? 'default');
		} catch (err: unknown) {
			toast.error(`Delete rule failed: ${(err as Error).message}`);
		}
	}

	async function putEvents() {
		puttingEvents = true;
		try {
			let detail = evtDetail;
			try { JSON.parse(detail); } catch { detail = '{}'; }
			await eb.send(new PutEventsCommand({
				Entries: [{
					Source: evtSource,
					DetailType: evtDetailType,
					Detail: detail,
					EventBusName: evtBusName
				}]
			}));
			toast.success('Event sent successfully');
			showPutEventsModal = false;
		} catch (err: unknown) {
			toast.error(`Put events failed: ${(err as Error).message}`);
		} finally {
			puttingEvents = false;
		}
	}

	onMount(() => { loadBuses(); });
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-pink-100 dark:bg-pink-900/30 rounded-lg">
				<Zap class="w-6 h-6 text-pink-600 dark:text-pink-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">EventBridge</h1>
				<p class="text-slate-600 dark:text-slate-300">Event buses, rules, and routing</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={() => loadBuses()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
			<button onclick={() => { showPutEventsModal = true; }} class="px-3 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-2 text-sm">
				<Send class="w-4 h-4" />Put Events
			</button>
			<button onclick={() => { showCreateBusModal = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />Create Bus
			</button>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex border-b border-slate-200 dark:border-slate-700">
		{#each [{ id: 'buses', label: 'Event Buses', icon: Bus }, { id: 'rules', label: 'Rules', icon: List }] as tab}
			<button
				onclick={() => { activeTab = tab.id as 'buses' | 'rules'; }}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors {activeTab === tab.id ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
			>
				<tab.icon class="w-4 h-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	{#if activeTab === 'buses'}
		<!-- Search -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input type="text" bind:value={searchQuery} placeholder="Search event buses..." class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
		</div>

		{#if loading}
			<div class="text-center py-12">
				<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
				<p class="text-slate-500 dark:text-slate-400">Loading event buses...</p>
			</div>
		{:else if filteredBuses.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Zap class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
				<p class="text-slate-500 dark:text-slate-400">No event buses found</p>
			</div>
		{:else}
			<div class="grid gap-3">
				{#each filteredBuses as bus}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center justify-between">
						<div>
							<p class="font-medium text-slate-900 dark:text-white">{bus.Name}</p>
							<p class="text-xs text-slate-500 dark:text-slate-400 font-mono mt-0.5 truncate max-w-md">{bus.Arn}</p>
						</div>
						<div class="flex items-center gap-2">
							<button
								id="view-rules-{bus.Name}"
								onclick={() => selectBus(bus)}
								class="px-3 py-1.5 text-sm bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 rounded-lg hover:bg-indigo-200 dark:hover:bg-indigo-900/50"
							>View Rules</button>
							{#if bus.Name !== 'default'}
								<button onclick={() => deleteBus(bus.Name ?? '')} class="p-1.5 text-slate-400 hover:text-red-500">
									<Trash2 class="w-4 h-4" />
								</button>
							{/if}
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{:else}
		<!-- Rules Tab -->
		<div class="space-y-4">
			<div class="flex items-center justify-between">
				<div>
					{#if selectedBus}
						<p class="text-sm text-slate-600 dark:text-slate-400">Rules for: <span class="font-semibold text-slate-900 dark:text-white">{selectedBus.Name}</span></p>
					{:else}
						<p class="text-sm text-slate-500 dark:text-slate-400">Select an event bus to view its rules</p>
					{/if}
				</div>
				{#if selectedBus}
					<button onclick={() => { showCreateRuleModal = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2 text-sm">
						<Plus class="w-4 h-4" />Create Rule
					</button>
				{/if}
			</div>

			{#if loadingRules}
				<div class="text-center py-8">
					<div class="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-indigo-500"></div>
				</div>
			{:else if rules.length === 0 && selectedBus}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
					<List class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">No rules found for this bus</p>
				</div>
			{:else}
				<div class="grid gap-3">
					{#each rules as rule}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-start justify-between">
							<div class="min-w-0 flex-1">
								<div class="flex items-center gap-2 mb-1">
									<p class="font-medium text-slate-900 dark:text-white">{rule.Name}</p>
									<span class="px-2 py-0.5 text-xs rounded-full {rule.State === 'ENABLED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400'}">{rule.State}</span>
								</div>
								{#if rule.ScheduleExpression}
									<p class="text-sm text-slate-600 dark:text-slate-400 font-mono">Schedule: {rule.ScheduleExpression}</p>
								{/if}
								{#if rule.EventPattern}
									<p class="text-xs text-slate-500 dark:text-slate-400 font-mono truncate">Pattern: {rule.EventPattern}</p>
								{/if}
							</div>
							<button onclick={() => deleteRule(rule.Name ?? '')} class="ml-3 p-1.5 text-slate-400 hover:text-red-500">
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	{/if}
</div>

<!-- Create Bus Modal -->
{#if showCreateBusModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Event Bus</h2>
			<form onsubmit={(e) => { e.preventDefault(); createBus(); }} class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Bus Name</label>
					<input type="text" bind:value={newBusName} placeholder="e.g. my-app-events" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateBusModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingBus} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingBus ? 'Creating...' : 'Create Bus'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Rule Modal -->
{#if showCreateRuleModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Rule</h2>
			<form onsubmit={(e) => { e.preventDefault(); createRule(); }} class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Rule Name</label>
					<input type="text" bind:value={newRuleName} placeholder="e.g. process-orders-daily" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div class="flex gap-4">
					{#each [['schedule', 'Schedule Expression'], ['pattern', 'Event Pattern']] as [val, lbl]}
						<label class="flex items-center gap-2 cursor-pointer">
							<input type="radio" bind:group={newRuleType} value={val} class="accent-indigo-600" />
							<span class="text-sm text-slate-700 dark:text-slate-300">{lbl}</span>
						</label>
					{/each}
				</div>
				{#if newRuleType === 'schedule'}
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Schedule Expression</label>
						<input type="text" bind:value={newRuleSchedule} placeholder="e.g. rate(5 minutes)" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono" />
					</div>
				{:else}
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Event Pattern (JSON)</label>
						<textarea bind:value={newRulePattern} rows={4} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"></textarea>
					</div>
				{/if}
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">State</label>
					<select bind:value={newRuleState} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
						<option value="ENABLED">ENABLED</option>
						<option value="DISABLED">DISABLED</option>
					</select>
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateRuleModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingRule} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingRule ? 'Creating...' : 'Create Rule'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Put Events Modal -->
{#if showPutEventsModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Put Events</h2>
			<form onsubmit={(e) => { e.preventDefault(); putEvents(); }} class="space-y-4">
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Source</label>
						<input type="text" bind:value={evtSource} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Detail Type</label>
						<input type="text" bind:value={evtDetailType} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Event Bus Name</label>
					<input type="text" bind:value={evtBusName} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Detail (JSON)</label>
					<textarea bind:value={evtDetail} rows={5} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"></textarea>
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showPutEventsModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={puttingEvents} class="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 flex items-center gap-2">
						<Send class="w-4 h-4" />{puttingEvents ? 'Sending...' : 'Put Events'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
