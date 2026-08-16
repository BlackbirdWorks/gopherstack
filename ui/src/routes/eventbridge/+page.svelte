<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { multiRegionList } from '$lib/multi-region';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
	import { getEventBridgeClient } from '$lib/aws-client';
	import {
		ListEventBusesCommand,
		CreateEventBusCommand,
		DeleteEventBusCommand,
		ListRulesCommand,
		PutRuleCommand,
		DeleteRuleCommand,
		EnableRuleCommand,
		DisableRuleCommand,
		PutEventsCommand,
		ListArchivesCommand,
		CreateArchiveCommand,
		DeleteArchiveCommand,
		ListConnectionsCommand,
		CreateConnectionCommand,
		DeleteConnectionCommand,
		TestEventPatternCommand,
		ListTargetsByRuleCommand,
		PutTargetsCommand,
		RemoveTargetsCommand,
		StartReplayCommand,
		DescribeArchiveCommand,
		type EventBus,
		type Rule,
		type Archive,
		type Connection,
		type Target,
		type PutRuleCommandInput,
		ConnectionAuthorizationType
	} from '@aws-sdk/client-eventbridge';
	import { toast } from 'svelte-sonner';
	import { Zap, Search, RefreshCw, Plus, Trash2, Send, List, Bus, ArchiveIcon, Link, BookOpen, FlaskConical, Play, Pause, Database } from 'lucide-svelte';

	const eb = regionalClient(getEventBridgeClient);
	type Tab = 'buses' | 'rules' | 'archives' | 'connections' | 'docs';

	// Every bus/archive/connection row carries the region its List call was
	// made against. Row and detail actions must use THIS region, not the
	// page's shared `eb()` client -- in All mode the same name can
	// legitimately exist in two different regions.
	type Regioned<T> = T & { region: string };

	let loading = $state(false);
	let buses = $state<Regioned<EventBus>[]>([]);
	let searchQuery = $state('');
	let selectedBus = $state<Regioned<EventBus> | null>(null);
	let rules = $state<Rule[]>([]);
	let loadingRules = $state(false);
	// Rule targets editor
	let expandedRule = $state<string | null>(null);
	let ruleTargets = $state<Target[]>([]);
	let loadingTargets = $state(false);
	let newTargetArn = $state('');
	let newTargetId = $state('');
	let addingTarget = $state(false);
	// Archive replay
	let replayArchive = $state<string | null>(null);
	let replayName = $state('');
	let replayStart = $state('');
	let replayEnd = $state('');
	let replayBusArn = $state('');
	let startingReplay = $state(false);
	let activeTab = $state<Tab>('buses');
	let archives = $state<Regioned<Archive>[]>([]);
	let loadingArchives = $state(false);
	let showCreateArchiveModal = $state(false);
	let creatingArchive = $state(false);
	let newArchiveName = $state('');
	let newArchiveSource = $state('');
	let newArchiveRetention = $state(0);
	let newArchivePattern = $state('');
	let connections = $state<Regioned<Connection>[]>([]);
	let loadingConnections = $state(false);
	let showCreateConnectionModal = $state(false);
	let creatingConnection = $state(false);
	let newConnectionName = $state('');
	let newConnectionAuthType = $state<string>('API_KEY');
	let showTestPatternModal = $state(false);
	let testPattern = $state('{"source":["my.app"]}');
	let testEvent = $state('{"source":"my.app","detail-type":"Login","detail":{}}');
	let testPatternResult = $state<boolean | null>(null);
	let testingPattern = $state(false);
	let showCreateBusModal = $state(false);
	let creatingBus = $state(false);
	let newBusName = $state('');
	let showCreateRuleModal = $state(false);
	let creatingRule = $state(false);
	let newRuleName = $state('');
	let newRuleType = $state<'schedule' | 'pattern'>('schedule');
	let newRuleSchedule = $state('rate(5 minutes)');
	let newRulePattern = $state('{"source":["my.app"]}');
	let newRuleState = $state<'ENABLED' | 'DISABLED'>('ENABLED');
	let showPutEventsModal = $state(false);
	let puttingEvents = $state(false);
	let evtSource = $state('my.application');
	let evtDetailType = $state('UserAction');
	let evtDetail = $state('{"action":"login"}');
	let evtBusName = $state('default');
	let seedingDemo = $state(false);

	const filteredBuses = $derived(buses.filter((b) => (b.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	// Fans List*Command out across every region with data in All mode
	// (single-region mode collapses to exactly one call).
	async function loadBuses() {
		loading = true;
		try {
			const result = await multiRegionList(
				(region) => getEventBridgeClient(region).send(new ListEventBusesCommand({ Limit: 100 })),
				(r) => r.EventBuses ?? []
			);
			buses = result.items.map(({ region, item }) => ({ ...item, region }));
			if (result.errors.length > 0) toast.error(`Failed to load event buses from ${result.errors.length} region(s)`);
		}
		catch (err: unknown) { toast.error(`Failed to load event buses: ${(err as Error).message}`); }
		finally { loading = false; }
	}
	async function loadArchives() {
		loadingArchives = true;
		try {
			const result = await multiRegionList(
				(region) => getEventBridgeClient(region).send(new ListArchivesCommand({ Limit: 100 })),
				(r) => r.Archives ?? []
			);
			archives = result.items.map(({ region, item }) => ({ ...item, region }));
			if (result.errors.length > 0) toast.error(`Failed to load archives from ${result.errors.length} region(s)`);
		}
		catch (err: unknown) { toast.error(`Failed to load archives: ${(err as Error).message}`); }
		finally { loadingArchives = false; }
	}
	async function loadConnections() {
		loadingConnections = true;
		try {
			const result = await multiRegionList(
				(region) => getEventBridgeClient(region).send(new ListConnectionsCommand({ Limit: 100 })),
				(r) => r.Connections ?? []
			);
			connections = result.items.map(({ region, item }) => ({ ...item, region }));
			if (result.errors.length > 0) toast.error(`Failed to load connections from ${result.errors.length} region(s)`);
		}
		catch (err: unknown) { toast.error(`Failed to load connections: ${(err as Error).message}`); }
		finally { loadingConnections = false; }
	}
	async function createBus() {
		if (!newBusName.trim()) return;
		creatingBus = true;
		try { await eb().send(new CreateEventBusCommand({ Name: newBusName.trim() })); toast.success(`Event bus "${newBusName.trim()}" created`); showCreateBusModal = false; newBusName = ''; await loadBuses(); }
		catch (err: unknown) { toast.error(`Create failed: ${(err as Error).message}`); }
		finally { creatingBus = false; }
	}
	async function deleteBus(name: string, region: string) {
		if (!(await confirmDestructive({ title: 'Delete Event Bus', message: `Delete event bus "${name}"?` }))) return;
		try { await getEventBridgeClient(region).send(new DeleteEventBusCommand({ Name: name })); toast.success(`Event bus "${name}" deleted`); if (selectedBus?.Name === name) { selectedBus = null; rules = []; } await loadBuses(); }
		catch (err: unknown) { toast.error(`Delete failed: ${(err as Error).message}`); }
	}
	async function selectBus(bus: Regioned<EventBus>) { selectedBus = bus; activeTab = 'rules'; await loadRules(bus.Name ?? 'default'); }
	async function loadRules(busName: string) {
		if (!selectedBus) return;
		loadingRules = true;
		try { const res = await getEventBridgeClient(selectedBus.region).send(new ListRulesCommand({ EventBusName: busName, Limit: 100 })); rules = res.Rules ?? []; }
		catch (err: unknown) { toast.error(`Failed to load rules: ${(err as Error).message}`); }
		finally { loadingRules = false; }
	}
	async function toggleRuleTargets(rule: Rule) {
		if (!selectedBus) return;
		if (expandedRule === rule.Name) { expandedRule = null; return; }
		expandedRule = rule.Name ?? null;
		ruleTargets = [];
		newTargetArn = '';
		newTargetId = '';
		loadingTargets = true;
		try {
			const res = await getEventBridgeClient(selectedBus.region).send(new ListTargetsByRuleCommand({ Rule: rule.Name, EventBusName: selectedBus?.Name }));
			ruleTargets = res.Targets ?? [];
		} catch (err: unknown) { toast.error(`Failed to load targets: ${(err as Error).message}`); }
		finally { loadingTargets = false; }
	}
	async function addTarget(ruleName: string) {
		if (!newTargetArn.trim()) { toast.error('Target ARN is required'); return; }
		if (!selectedBus) return;
		addingTarget = true;
		try {
			const id = newTargetId.trim() || `target-${Date.now()}`;
			const res = await getEventBridgeClient(selectedBus.region).send(new PutTargetsCommand({ Rule: ruleName, EventBusName: selectedBus?.Name, Targets: [{ Id: id, Arn: newTargetArn.trim() }] }));
			if ((res.FailedEntryCount ?? 0) > 0) {
				toast.error(`Failed: ${res.FailedEntries?.[0]?.ErrorMessage ?? 'unknown error'}`);
			} else {
				toast.success('Target added');
				newTargetArn = '';
				newTargetId = '';
				await toggleRuleTargets({ Name: ruleName });
				expandedRule = ruleName;
			}
		} catch (err: unknown) { toast.error(`Add target failed: ${(err as Error).message}`); }
		finally { addingTarget = false; }
	}
	async function removeTarget(ruleName: string, id: string) {
		if (!selectedBus) return;
		try {
			await getEventBridgeClient(selectedBus.region).send(new RemoveTargetsCommand({ Rule: ruleName, EventBusName: selectedBus?.Name, Ids: [id] }));
			toast.success('Target removed');
			ruleTargets = ruleTargets.filter((t) => t.Id !== id);
		} catch (err: unknown) { toast.error(`Remove target failed: ${(err as Error).message}`); }
	}
	let replayArchiveArn = $state('');
	let replayRegion = $state('');
	async function openReplay(archive: Regioned<Archive>) {
		replayArchive = archive.ArchiveName ?? null;
		replayRegion = archive.region;
		replayName = `${archive.ArchiveName}-replay-${Date.now()}`.replaceAll(/[^A-Za-z0-9._-]/g, '-');
		replayBusArn = archive.EventSourceArn ?? '';
		replayArchiveArn = '';
		const now = new Date();
		replayEnd = now.toISOString().slice(0, 16);
		replayStart = new Date(now.getTime() - 24 * 3600 * 1000).toISOString().slice(0, 16);
		try {
			const res = await getEventBridgeClient(archive.region).send(new DescribeArchiveCommand({ ArchiveName: archive.ArchiveName }));
			replayArchiveArn = res.ArchiveArn ?? '';
		} catch {
			// fall back to manual entry
		}
	}
	async function startReplay() {
		if (!replayArchive || !replayName.trim() || !replayBusArn.trim() || !replayArchiveArn.trim() || !replayStart || !replayEnd) {
			toast.error('All replay fields (including archive ARN) are required');
			return;
		}
		startingReplay = true;
		try {
			await getEventBridgeClient(replayRegion).send(new StartReplayCommand({
				ReplayName: replayName.trim(),
				EventSourceArn: replayArchiveArn.trim(),
				Destination: { Arn: replayBusArn.trim() },
				EventStartTime: new Date(replayStart),
				EventEndTime: new Date(replayEnd)
			}));
			toast.success(`Replay "${replayName.trim()}" started`);
			replayArchive = null;
		} catch (err: unknown) {
			toast.error(`Start replay failed: ${(err as Error).message}`);
		} finally {
			startingReplay = false;
		}
	}
	async function createRule() {
		if (!newRuleName.trim() || !selectedBus) return;
		creatingRule = true;
		try {
			const params: PutRuleCommandInput = { Name: newRuleName.trim(), EventBusName: selectedBus.Name, State: newRuleState };
			if (newRuleType === 'schedule') { params.ScheduleExpression = newRuleSchedule; } else { params.EventPattern = newRulePattern; }
			await getEventBridgeClient(selectedBus.region).send(new PutRuleCommand(params)); toast.success(`Rule "${newRuleName.trim()}" created`); showCreateRuleModal = false; newRuleName = ''; await loadRules(selectedBus.Name ?? 'default');
		} catch (err: unknown) { toast.error(`Create rule failed: ${(err as Error).message}`); }
		finally { creatingRule = false; }
	}
	async function deleteRule(ruleName: string) {
		if (!selectedBus || !(await confirmDestructive({ title: 'Delete EventBridge Rule', message: `Delete rule "${ruleName}"?` }))) return;
		try { await getEventBridgeClient(selectedBus.region).send(new DeleteRuleCommand({ Name: ruleName, EventBusName: selectedBus.Name })); toast.success(`Rule "${ruleName}" deleted`); await loadRules(selectedBus.Name ?? 'default'); }
		catch (err: unknown) { toast.error(`Delete rule failed: ${(err as Error).message}`); }
	}
	async function toggleRule(rule: Rule) {
		if (!selectedBus) return;
		try {
			if (rule.State === 'ENABLED') { await getEventBridgeClient(selectedBus.region).send(new DisableRuleCommand({ Name: rule.Name, EventBusName: selectedBus.Name })); toast.success(`Rule "${rule.Name}" disabled`); }
			else { await getEventBridgeClient(selectedBus.region).send(new EnableRuleCommand({ Name: rule.Name, EventBusName: selectedBus.Name })); toast.success(`Rule "${rule.Name}" enabled`); }
			await loadRules(selectedBus.Name ?? 'default');
		} catch (err: unknown) { toast.error(`Toggle failed: ${(err as Error).message}`); }
	}
	async function putEvents() {
		puttingEvents = true;
		try {
			let detail = evtDetail; try { JSON.parse(detail); } catch { detail = '{}'; }
			await eb().send(new PutEventsCommand({ Entries: [{ Source: evtSource, DetailType: evtDetailType, Detail: detail, EventBusName: evtBusName }] }));
			toast.success('Event sent successfully'); showPutEventsModal = false;
		} catch (err: unknown) { toast.error(`Put events failed: ${(err as Error).message}`); }
		finally { puttingEvents = false; }
	}
	async function createArchive() {
		if (!newArchiveName.trim() || !newArchiveSource.trim()) return;
		creatingArchive = true;
		try {
			await eb().send(new CreateArchiveCommand({ ArchiveName: newArchiveName.trim(), EventSourceArn: newArchiveSource.trim(), RetentionDays: newArchiveRetention > 0 ? newArchiveRetention : undefined, EventPattern: newArchivePattern.trim() || undefined }));
			toast.success(`Archive "${newArchiveName.trim()}" created`); showCreateArchiveModal = false; newArchiveName = ''; newArchiveSource = ''; newArchiveRetention = 0; newArchivePattern = ''; await loadArchives();
		} catch (err: unknown) { toast.error(`Create archive failed: ${(err as Error).message}`); }
		finally { creatingArchive = false; }
	}
	async function deleteArchive(name: string, region: string) {
		if (!(await confirmDestructive({ title: 'Delete Archive', message: `Delete archive "${name}"?` }))) return;
		try { await getEventBridgeClient(region).send(new DeleteArchiveCommand({ ArchiveName: name })); toast.success(`Archive "${name}" deleted`); await loadArchives(); }
		catch (err: unknown) { toast.error(`Delete archive failed: ${(err as Error).message}`); }
	}
	async function createConnection() {
		if (!newConnectionName.trim()) return;
		creatingConnection = true;
		try {
			await eb().send(new CreateConnectionCommand({ Name: newConnectionName.trim(), AuthorizationType: newConnectionAuthType as ConnectionAuthorizationType, AuthParameters: { ApiKeyAuthParameters: { ApiKeyName: 'x-api-key', ApiKeyValue: 'placeholder' } } }));
			toast.success(`Connection "${newConnectionName.trim()}" created`); showCreateConnectionModal = false; newConnectionName = ''; await loadConnections();
		} catch (err: unknown) { toast.error(`Create connection failed: ${(err as Error).message}`); }
		finally { creatingConnection = false; }
	}
	async function deleteConnection(name: string, region: string) {
		if (!(await confirmDestructive({ title: 'Delete Connection', message: `Delete connection "${name}"?` }))) return;
		try { await getEventBridgeClient(region).send(new DeleteConnectionCommand({ Name: name })); toast.success(`Connection "${name}" deleted`); await loadConnections(); }
		catch (err: unknown) { toast.error(`Delete connection failed: ${(err as Error).message}`); }
	}
	async function testEventPattern() {
		testingPattern = true; testPatternResult = null;
		try {
			try { JSON.parse(testEvent); } catch { toast.error('Event must be valid JSON'); return; }
			const res = await eb().send(new TestEventPatternCommand({ EventPattern: testPattern, Event: testEvent }));
			testPatternResult = res.Result ?? false;
			toast.info(testPatternResult ? 'Pattern matches event' : 'Pattern does not match event');
		} catch (err: unknown) { toast.error(`Test failed: ${(err as Error).message}`); }
		finally { testingPattern = false; }
	}
	async function loadDemoData() {
		seedingDemo = true;
		try {
			await eb().send(new CreateEventBusCommand({ Name: 'demo-app-events' })).catch(() => {});
			await eb().send(new PutRuleCommand({ Name: 'demo-order-rule', EventBusName: 'demo-app-events', EventPattern: JSON.stringify({ source: ['demo.shop'], 'detail-type': ['OrderPlaced'] }), State: 'ENABLED', Description: 'Fires when an order is placed' })).catch(() => {});
			await eb().send(new PutEventsCommand({ Entries: [{ Source: 'demo.shop', DetailType: 'OrderPlaced', Detail: JSON.stringify({ orderId: 'ord-001', amount: 42.0 }), EventBusName: 'demo-app-events' }] })).catch(() => {});
			toast.success('Demo data seeded'); await loadBuses();
		} catch (err: unknown) { toast.error(`Seed failed: ${(err as Error).message}`); }
		finally { seedingDemo = false; }
	}
	onRegionChange(() => { loadBuses(); });

	const tabs: { id: Tab; label: string; icon: typeof Zap }[] = [
		{ id: 'buses', label: 'Event Buses', icon: Bus },
		{ id: 'rules', label: 'Rules', icon: List },
		{ id: 'archives', label: 'Archives', icon: ArchiveIcon },
		{ id: 'connections', label: 'Connections', icon: Link },
		{ id: 'docs', label: 'Docs', icon: BookOpen }
	];
	const apiOperations = [
		{ op: 'CreateEventBus', desc: 'Create a custom event bus' },
		{ op: 'DeleteEventBus', desc: 'Delete a custom event bus' },
		{ op: 'DescribeEventBus', desc: 'Retrieve event bus details' },
		{ op: 'ListEventBuses', desc: 'List all event buses' },
		{ op: 'UpdateEventBus', desc: 'Update event bus description' },
		{ op: 'PutRule', desc: 'Create or update a routing rule' },
		{ op: 'DeleteRule', desc: 'Delete a routing rule' },
		{ op: 'DescribeRule', desc: 'Retrieve rule details' },
		{ op: 'ListRules', desc: 'List rules for an event bus' },
		{ op: 'EnableRule', desc: 'Enable a disabled rule' },
		{ op: 'DisableRule', desc: 'Disable an active rule' },
		{ op: 'PutTargets', desc: 'Add or replace targets for a rule (max 5)' },
		{ op: 'RemoveTargets', desc: 'Remove targets from a rule' },
		{ op: 'ListTargetsByRule', desc: 'List targets for a rule' },
		{ op: 'ListRuleNamesByTarget', desc: 'List rules matching a target ARN' },
		{ op: 'PutEvents', desc: 'Send custom events to an event bus' },
		{ op: 'TestEventPattern', desc: 'Test whether an event matches a pattern' },
		{ op: 'CreateArchive', desc: 'Create an event archive (name max 48 chars)' },
		{ op: 'DeleteArchive', desc: 'Delete an archive' },
		{ op: 'DescribeArchive', desc: 'Retrieve archive details' },
		{ op: 'ListArchives', desc: 'List archives' },
		{ op: 'UpdateArchive', desc: 'Update archive configuration' },
		{ op: 'StartReplay', desc: 'Replay events from an archive' },
		{ op: 'DescribeReplay', desc: 'Retrieve replay details' },
		{ op: 'ListReplays', desc: 'List replay jobs' },
		{ op: 'CancelReplay', desc: 'Cancel an in-progress replay' },
		{ op: 'CreateConnection', desc: 'Create an API connection (API_KEY/BASIC/OAUTH)' },
		{ op: 'DeleteConnection', desc: 'Delete a connection' },
		{ op: 'DescribeConnection', desc: 'Retrieve connection details' },
		{ op: 'ListConnections', desc: 'List connections' },
		{ op: 'UpdateConnection', desc: 'Update connection auth configuration' },
		{ op: 'DeauthorizeConnection', desc: 'Remove connection authorisation' },
		{ op: 'CreateApiDestination', desc: 'Create an API destination' },
		{ op: 'DeleteApiDestination', desc: 'Delete an API destination' },
		{ op: 'DescribeApiDestination', desc: 'Retrieve API destination details' },
		{ op: 'ListApiDestinations', desc: 'List API destinations' },
		{ op: 'UpdateApiDestination', desc: 'Update an API destination' },
		{ op: 'CreateEndpoint', desc: 'Create a global endpoint' },
		{ op: 'DeleteEndpoint', desc: 'Delete a global endpoint' },
		{ op: 'DescribeEndpoint', desc: 'Retrieve endpoint details' },
		{ op: 'ListEndpoints', desc: 'List global endpoints' },
		{ op: 'UpdateEndpoint', desc: 'Update endpoint configuration' },
		{ op: 'PutPermission', desc: 'Add a resource-based policy statement' },
		{ op: 'RemovePermission', desc: 'Remove a resource-based policy statement' },
		{ op: 'ListEventSources', desc: 'List partner event sources' },
		{ op: 'DescribeEventSource', desc: 'Describe a partner event source' },
		{ op: 'ActivateEventSource', desc: 'Activate a pending partner event source' },
		{ op: 'DeactivateEventSource', desc: 'Deactivate an active partner event source' },
		{ op: 'ListPartnerEventSources', desc: 'List partner event source names' },
		{ op: 'DescribePartnerEventSource', desc: 'Describe a partner event source by name' },
		{ op: 'DeletePartnerEventSource', desc: 'Delete a partner event source' },
		{ op: 'PutPartnerEvents', desc: 'Send events from a partner event source' }
	];
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-pink-100 dark:bg-pink-900/30 rounded-lg"><Zap class="w-6 h-6 text-pink-600 dark:text-pink-400" /></div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">EventBridge</h1>
				<p class="text-slate-600 dark:text-slate-300">Event buses, rules, archives, and connections</p>
			</div>
		</div>
		<div class="flex items-center gap-2 flex-wrap justify-end">
			<button onclick={() => { if (activeTab==='buses') loadBuses(); else if (activeTab==='archives') loadArchives(); else if (activeTab==='connections') loadConnections(); }} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {loading||loadingArchives||loadingConnections ? 'animate-spin' : ''}" />
			</button>
			<button onclick={() => loadDemoData()} disabled={seedingDemo} class="px-3 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 flex items-center gap-2 text-sm disabled:opacity-50">
				<Database class="w-4 h-4" />{seedingDemo ? 'Seeding...' : 'Load Demo'}
			</button>
			<button onclick={() => { showTestPatternModal = true; }} class="px-3 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 flex items-center gap-2 text-sm">
				<FlaskConical class="w-4 h-4" />Test Pattern
			</button>
			<button onclick={() => { showPutEventsModal = true; }} class="px-3 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-2 text-sm">
				<Send class="w-4 h-4" />Put Events
			</button>
			{#if activeTab === 'buses'}
				<button onclick={() => { showCreateBusModal = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"><Plus class="w-4 h-4" />Create Bus</button>
			{:else if activeTab === 'rules' && selectedBus}
				<button onclick={() => { showCreateRuleModal = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"><Plus class="w-4 h-4" />Create Rule</button>
			{:else if activeTab === 'archives'}
				<button onclick={() => { showCreateArchiveModal = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"><Plus class="w-4 h-4" />Create Archive</button>
			{:else if activeTab === 'connections'}
				<button onclick={() => { showCreateConnectionModal = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"><Plus class="w-4 h-4" />Create Connection</button>
			{/if}
		</div>
	</div>

	<div class="flex border-b border-slate-200 dark:border-slate-700 overflow-x-auto">
		{#each tabs as tab}
			<button onclick={() => { activeTab = tab.id; if (tab.id==='archives' && archives.length===0) loadArchives(); if (tab.id==='connections' && connections.length===0) loadConnections(); }}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors whitespace-nowrap {activeTab === tab.id ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}">
				<tab.icon class="w-4 h-4" />{tab.label}
			</button>
		{/each}
	</div>

	{#if activeTab === 'buses'}
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input type="text" bind:value={searchQuery} placeholder="Search event buses..." class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
		</div>
		{#if loading}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div><p class="text-slate-500 dark:text-slate-400">Loading event buses...</p></div>
		{:else if filteredBuses.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Zap class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
				<p class="text-slate-500 dark:text-slate-400">No event buses found</p>
				<p class="text-sm text-slate-400 dark:text-slate-500 mt-1">Click <strong>Load Demo</strong> to seed example data.</p>
			</div>
		{:else}
			<div class="grid gap-3">
				{#each filteredBuses as bus}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center justify-between">
						<div class="min-w-0 flex-1">
							<div class="flex items-center gap-2">
								<p class="font-medium text-slate-900 dark:text-white">{bus.Name}</p>
								<RegionChip region={bus.region} />
							</div>
							<p class="text-xs text-slate-500 dark:text-slate-400 font-mono mt-0.5 truncate max-w-md">{bus.Arn}</p>
							{#if bus.Description}<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{bus.Description}</p>{/if}
						</div>
						<div class="flex items-center gap-2 ml-4">
							<button id="view-rules-{bus.Name}" onclick={() => selectBus(bus)} class="px-3 py-1.5 text-sm bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 rounded-lg hover:bg-indigo-200 dark:hover:bg-indigo-900/50">View Rules</button>
							{#if bus.Name !== 'default'}<button onclick={() => deleteBus(bus.Name ?? '', bus.region)} class="p-1.5 text-slate-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>{/if}
						</div>
					</div>
				{/each}
			</div>
		{/if}

	{:else if activeTab === 'rules'}
		<div class="space-y-4">
			<div>
				{#if selectedBus}<p class="text-sm text-slate-600 dark:text-slate-400">Rules for: <span class="font-semibold text-slate-900 dark:text-white">{selectedBus.Name}</span> <RegionChip region={selectedBus.region} /></p>
				{:else}<p class="text-sm text-slate-500 dark:text-slate-400">Select an event bus from the <strong>Event Buses</strong> tab to view its rules.</p>{/if}
			</div>
			{#if loadingRules}
				<div class="text-center py-8"><div class="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-indigo-500"></div></div>
			{:else if rules.length === 0 && selectedBus}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center"><List class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" /><p class="text-slate-500 dark:text-slate-400">No rules found for this bus</p></div>
			{:else}
				<div class="grid gap-3">
					{#each rules as rule}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
							<div class="flex items-start justify-between">
								<div class="min-w-0 flex-1">
									<div class="flex items-center gap-2 mb-1">
										<p class="font-medium text-slate-900 dark:text-white">{rule.Name}</p>
										<span class="px-2 py-0.5 text-xs rounded-full {rule.State === 'ENABLED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400'}">{rule.State}</span>
									</div>
									{#if rule.Description}<p class="text-xs text-slate-500 dark:text-slate-400 mb-1">{rule.Description}</p>{/if}
									{#if rule.ScheduleExpression}<p class="text-sm text-slate-600 dark:text-slate-400 font-mono">Schedule: {rule.ScheduleExpression}</p>{/if}
									{#if rule.EventPattern}<p class="text-xs text-slate-500 dark:text-slate-400 font-mono truncate">Pattern: {rule.EventPattern}</p>{/if}
								</div>
								<div class="flex items-center gap-2 ml-3">
									<button onclick={() => toggleRuleTargets(rule)} class="text-xs px-2 py-1 rounded border border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">{expandedRule === rule.Name ? 'Hide Targets' : 'Targets'}</button>
									<button onclick={() => toggleRule(rule)} class="p-1.5 text-slate-400 hover:text-indigo-500" title={rule.State === 'ENABLED' ? 'Disable rule' : 'Enable rule'}>
										{#if rule.State === 'ENABLED'}<Pause class="w-4 h-4" />{:else}<Play class="w-4 h-4" />{/if}
									</button>
									<button onclick={() => deleteRule(rule.Name ?? '')} class="p-1.5 text-slate-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
								</div>
							</div>
							{#if expandedRule === rule.Name}
								<div class="mt-3 pt-3 border-t border-slate-200 dark:border-slate-700">
									{#if loadingTargets}
										<p class="text-xs text-slate-500">Loading targets...</p>
									{:else}
										{#if ruleTargets.length === 0}
											<p class="text-xs text-slate-500 mb-2">No targets configured.</p>
										{:else}
											<div class="space-y-1 mb-2">
												{#each ruleTargets as t}
													<div class="flex items-center justify-between gap-2 text-xs bg-slate-50 dark:bg-slate-700/40 rounded p-2">
														<span class="font-mono text-slate-700 dark:text-slate-300 truncate flex-1">{t.Id}: {t.Arn}</span>
														<button onclick={() => removeTarget(rule.Name ?? '', t.Id ?? '')} class="shrink-0 text-red-500 hover:text-red-700">Remove</button>
													</div>
												{/each}
											</div>
										{/if}
										<div class="flex flex-wrap items-end gap-2">
											<div class="flex-1 min-w-[12rem]">
												<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="tgt-arn-{rule.Name}">Target ARN (Lambda/SQS/SNS/etc.)</label>
												<input id="tgt-arn-{rule.Name}" type="text" bind:value={newTargetArn} placeholder="arn:aws:..." class="w-full px-2 py-1 text-xs font-mono border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
											</div>
											<div>
												<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="tgt-id-{rule.Name}">Target ID</label>
												<input id="tgt-id-{rule.Name}" type="text" bind:value={newTargetId} placeholder="auto" class="w-28 px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
											</div>
											<button disabled={addingTarget} onclick={() => addTarget(rule.Name ?? '')} class="px-3 py-1 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700 disabled:opacity-50">{addingTarget ? 'Adding...' : 'Add Target'}</button>
										</div>
									{/if}
								</div>
							{/if}
						</div>
					{/each}
				</div>
			{/if}
		</div>

	{:else if activeTab === 'archives'}
		{#if loadingArchives}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div><p class="text-slate-500 dark:text-slate-400">Loading archives...</p></div>
		{:else if archives.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<ArchiveIcon class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
				<p class="text-slate-500 dark:text-slate-400">No archives found</p>
				<p class="text-sm text-slate-400 dark:text-slate-500 mt-1">Create an archive to replay events later.</p>
			</div>
		{:else}
			<div class="grid gap-3">
				{#each archives as archive}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
						<div class="flex items-start justify-between">
							<div class="min-w-0 flex-1">
								<div class="flex items-center gap-2 mb-1">
									<p class="font-medium text-slate-900 dark:text-white">{archive.ArchiveName}</p>
									<RegionChip region={archive.region} />
									<span class="px-2 py-0.5 text-xs rounded-full {archive.State === 'ENABLED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400'}">{archive.State}</span>
								</div>
								<p class="text-xs text-slate-500 dark:text-slate-400 font-mono truncate">Source: {archive.EventSourceArn}</p>
								<p class="text-xs text-slate-400 dark:text-slate-500">Retention: {archive.RetentionDays ? `${archive.RetentionDays} days` : 'Indefinite'} · Events: {archive.EventCount ?? 0}</p>
							</div>
							<div class="flex items-center gap-2 ml-3">
								<button onclick={() => replayArchive === archive.ArchiveName ? (replayArchive = null) : openReplay(archive)} class="text-xs px-2 py-1 rounded border border-indigo-200 dark:border-indigo-800 text-indigo-600 dark:text-indigo-400 hover:bg-indigo-50 dark:hover:bg-indigo-900/20">{replayArchive === archive.ArchiveName ? 'Cancel' : 'Replay'}</button>
								<button onclick={() => deleteArchive(archive.ArchiveName ?? '', archive.region)} class="p-1.5 text-slate-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
							</div>
						</div>
						{#if replayArchive === archive.ArchiveName}
							<div class="mt-3 pt-3 border-t border-slate-200 dark:border-slate-700 space-y-2">
								<div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
									<div>
										<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="rp-name">Replay name</label>
										<input id="rp-name" type="text" bind:value={replayName} class="w-full px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
									</div>
									<div>
										<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="rp-arn">Archive ARN</label>
										<input id="rp-arn" type="text" bind:value={replayArchiveArn} placeholder="arn:aws:events:...:archive/..." class="w-full px-2 py-1 text-xs font-mono border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
									</div>
									<div>
										<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="rp-dest">Destination bus ARN</label>
										<input id="rp-dest" type="text" bind:value={replayBusArn} class="w-full px-2 py-1 text-xs font-mono border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
									</div>
									<div class="grid grid-cols-2 gap-2">
										<div>
											<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="rp-start">Start</label>
											<input id="rp-start" type="datetime-local" bind:value={replayStart} class="w-full px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
										</div>
										<div>
											<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="rp-end">End</label>
											<input id="rp-end" type="datetime-local" bind:value={replayEnd} class="w-full px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
										</div>
									</div>
								</div>
								<button disabled={startingReplay} onclick={startReplay} class="px-3 py-1.5 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700 disabled:opacity-50">{startingReplay ? 'Starting...' : 'Start Replay'}</button>
							</div>
						{/if}
					</div>
				{/each}
			</div>
		{/if}

	{:else if activeTab === 'connections'}
		{#if loadingConnections}
			<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div><p class="text-slate-500 dark:text-slate-400">Loading connections...</p></div>
		{:else if connections.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<Link class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
				<p class="text-slate-500 dark:text-slate-400">No connections found</p>
				<p class="text-sm text-slate-400 dark:text-slate-500 mt-1">Connections provide authentication for API destinations.</p>
			</div>
		{:else}
			<div class="grid gap-3">
				{#each connections as connection}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-start justify-between">
						<div class="min-w-0 flex-1">
							<div class="flex items-center gap-2 mb-1">
								<p class="font-medium text-slate-900 dark:text-white">{connection.Name}</p>
								<RegionChip region={connection.region} />
								<span class="px-2 py-0.5 text-xs rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">{connection.ConnectionState ?? 'AUTHORIZED'}</span>
							</div>
							<p class="text-xs text-slate-500 dark:text-slate-400">Auth: {connection.AuthorizationType}</p>
						</div>
						<button onclick={() => deleteConnection(connection.Name ?? '', connection.region)} class="ml-3 p-1.5 text-slate-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/each}
			</div>
		{/if}

	{:else if activeTab === 'docs'}
		<div class="space-y-4">
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
				<h2 class="text-lg font-semibold text-slate-900 dark:text-white mb-3">EventBridge Operations</h2>
				<p class="text-sm text-slate-600 dark:text-slate-400 mb-4">All operations are emulated locally via the gopherstack in-memory EventBridge backend.</p>
				<div class="grid gap-1">
					{#each apiOperations as { op, desc }}
						<div class="flex items-start gap-3 py-1.5 border-b border-slate-100 dark:border-slate-700 last:border-0">
							<code class="text-xs font-mono text-indigo-600 dark:text-indigo-400 min-w-52 shrink-0">{op}</code>
							<p class="text-xs text-slate-600 dark:text-slate-400">{desc}</p>
						</div>
					{/each}
				</div>
			</div>
			<div class="bg-amber-50 dark:bg-amber-900/20 rounded-lg border border-amber-200 dark:border-amber-800 p-4">
				<p class="text-sm text-amber-800 dark:text-amber-300 font-medium">AWS Realism Notes</p>
				<ul class="text-xs text-amber-700 dark:text-amber-400 mt-2 space-y-1 list-disc list-inside">
					<li>Bus names may not start with the reserved prefix <code>aws.</code></li>
					<li>A rule must have either a ScheduleExpression or EventPattern — not both.</li>
					<li>Archive names are limited to 48 characters.</li>
					<li>Maximum 5 targets per rule (AWS default service quota).</li>
					<li>Connections require AuthorizationType: API_KEY, BASIC, or OAUTH_CLIENT_CREDENTIALS.</li>
					<li>StartReplay requires EventStartTime to be before EventEndTime.</li>
				</ul>
			</div>
		</div>
	{/if}
</div>

{#if showCreateBusModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-xl font-bold text-slate-900 dark:text-white">Create Event Bus</h2>
				<WriteRegionHint />
			</div>
			<form onsubmit={(e) => { e.preventDefault(); createBus(); }} class="space-y-4">
				<div>
					<label for="eb-bus-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Bus Name</label>
					<input id="eb-bus-name" type="text" bind:value={newBusName} placeholder="e.g. my-app-events" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
					<p class="text-xs text-slate-400 dark:text-slate-500 mt-1">Cannot start with "aws." (reserved prefix).</p>
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateBusModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingBus} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">{creatingBus ? 'Creating...' : 'Create Bus'}</button>
				</div>
			</form>
		</div>
	</div>
{/if}

{#if showCreateRuleModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Rule</h2>
			<form onsubmit={(e) => { e.preventDefault(); createRule(); }} class="space-y-4">
				<div>
					<label for="eb-rule-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Rule Name</label>
					<input id="eb-rule-name" type="text" bind:value={newRuleName} placeholder="e.g. process-orders-daily" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div class="flex gap-4">
					{#each [['schedule', 'Schedule Expression'], ['pattern', 'Event Pattern']] as [val, lbl]}
						<label class="flex items-center gap-2 cursor-pointer"><input type="radio" bind:group={newRuleType} value={val} class="accent-indigo-600" /><span class="text-sm text-slate-700 dark:text-slate-300">{lbl}</span></label>
					{/each}
				</div>
				{#if newRuleType === 'schedule'}
					<div><label for="eb-schedule" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Schedule Expression</label><input id="eb-schedule" type="text" bind:value={newRuleSchedule} placeholder="e.g. rate(5 minutes)" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono" /></div>
				{:else}
					<div><label for="eb-pattern" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Event Pattern (JSON)</label><textarea id="eb-pattern" bind:value={newRulePattern} rows={4} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"></textarea></div>
				{/if}
				<div>
					<label for="eb-rule-state" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">State</label>
					<select id="eb-rule-state" bind:value={newRuleState} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"><option value="ENABLED">ENABLED</option><option value="DISABLED">DISABLED</option></select>
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateRuleModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingRule} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">{creatingRule ? 'Creating...' : 'Create Rule'}</button>
				</div>
			</form>
		</div>
	</div>
{/if}

{#if showCreateArchiveModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-xl font-bold text-slate-900 dark:text-white">Create Archive</h2>
				<WriteRegionHint />
			</div>
			<form onsubmit={(e) => { e.preventDefault(); createArchive(); }} class="space-y-4">
				<div><label for="eb-archive-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Archive Name <span class="text-slate-400">(max 48 chars)</span></label><input id="eb-archive-name" type="text" bind:value={newArchiveName} maxlength={48} placeholder="e.g. my-event-archive" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required /></div>
				<div><label for="eb-archive-source" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Event Source ARN</label><input id="eb-archive-source" type="text" bind:value={newArchiveSource} placeholder="arn:aws:events:us-east-1:123456789012:event-bus/default" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm" required /></div>
				<div><label for="eb-archive-retention" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Retention Days <span class="text-slate-400">(0 = indefinite)</span></label><input id="eb-archive-retention" type="number" bind:value={newArchiveRetention} min={0} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" /></div>
				<div><label for="eb-archive-pattern" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Event Pattern (optional)</label><textarea id="eb-archive-pattern" bind:value={newArchivePattern} rows={3} placeholder={'{"source":["my.app"]}'} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"></textarea></div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateArchiveModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingArchive} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">{creatingArchive ? 'Creating...' : 'Create Archive'}</button>
				</div>
			</form>
		</div>
	</div>
{/if}

{#if showCreateConnectionModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-xl font-bold text-slate-900 dark:text-white">Create Connection</h2>
				<WriteRegionHint />
			</div>
			<form onsubmit={(e) => { e.preventDefault(); createConnection(); }} class="space-y-4">
				<div><label for="eb-conn-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Connection Name</label><input id="eb-conn-name" type="text" bind:value={newConnectionName} placeholder="e.g. my-webhook-auth" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required /></div>
				<div>
					<label for="eb-conn-auth" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Authorization Type</label>
					<select id="eb-conn-auth" bind:value={newConnectionAuthType} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"><option value="API_KEY">API_KEY</option><option value="BASIC">BASIC</option><option value="OAUTH_CLIENT_CREDENTIALS">OAUTH_CLIENT_CREDENTIALS</option></select>
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateConnectionModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingConnection} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">{creatingConnection ? 'Creating...' : 'Create Connection'}</button>
				</div>
			</form>
		</div>
	</div>
{/if}

{#if showPutEventsModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Put Events</h2>
			<form onsubmit={(e) => { e.preventDefault(); putEvents(); }} class="space-y-4">
				<div class="grid grid-cols-2 gap-4">
					<div><label for="eb-source" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Source</label><input id="eb-source" type="text" bind:value={evtSource} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" /></div>
					<div><label for="eb-detail-type" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Detail Type</label><input id="eb-detail-type" type="text" bind:value={evtDetailType} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" /></div>
				</div>
				<div><label for="eb-evt-bus" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Event Bus Name</label><input id="eb-evt-bus" type="text" bind:value={evtBusName} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" /></div>
				<div><label for="eb-detail" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Detail (JSON)</label><textarea id="eb-detail" bind:value={evtDetail} rows={5} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"></textarea></div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showPutEventsModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={puttingEvents} class="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 flex items-center gap-2"><Send class="w-4 h-4" />{puttingEvents ? 'Sending...' : 'Put Events'}</button>
				</div>
			</form>
		</div>
	</div>
{/if}

{#if showTestPatternModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-2xl">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Test Event Pattern</h2>
			<div class="grid grid-cols-2 gap-4 mb-4">
				<div><label for="eb-test-pattern" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Event Pattern (JSON)</label><textarea id="eb-test-pattern" bind:value={testPattern} rows={8} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-purple-500 font-mono text-sm resize-none"></textarea></div>
				<div><label for="eb-test-event" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Event (JSON)</label><textarea id="eb-test-event" bind:value={testEvent} rows={8} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-purple-500 font-mono text-sm resize-none"></textarea></div>
			</div>
			{#if testPatternResult !== null}
				<div class="mb-4 p-3 rounded-lg {testPatternResult ? 'bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-800 text-green-700 dark:text-green-300' : 'bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 text-red-700 dark:text-red-300'}">{testPatternResult ? '✓ Pattern matches the event' : '✗ Pattern does not match the event'}</div>
			{/if}
			<div class="flex justify-end gap-3">
				<button type="button" onclick={() => { showTestPatternModal = false; testPatternResult = null; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Close</button>
				<button onclick={() => testEventPattern()} disabled={testingPattern} class="px-4 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 disabled:opacity-50 flex items-center gap-2"><FlaskConical class="w-4 h-4" />{testingPattern ? 'Testing...' : 'Test Pattern'}</button>
			</div>
		</div>
	</div>
{/if}
