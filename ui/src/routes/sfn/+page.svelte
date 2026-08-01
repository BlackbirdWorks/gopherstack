<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getSFNClient } from '$lib/aws-client';
	import {
		ListStateMachinesCommand,
		DescribeStateMachineCommand,
		ListExecutionsCommand,
		DescribeExecutionCommand,
		StartExecutionCommand,
		StopExecutionCommand,
		DeleteStateMachineCommand,
		CreateStateMachineCommand,
		UpdateStateMachineCommand,
		GetExecutionHistoryCommand,
		RedriveExecutionCommand,
		ValidateStateMachineDefinitionCommand,
		CreateActivityCommand,
		DeleteActivityCommand,
		ListActivitiesCommand,
		ListStateMachineVersionsCommand,
		PublishStateMachineVersionCommand,
		type StateMachineListItem,
		type DescribeStateMachineCommandOutput,
		type ExecutionListItem,
		type DescribeExecutionOutput,
		type HistoryEvent,
		type ActivityListItem,
		type StateMachineType
	} from '@aws-sdk/client-sfn';
	import { toast } from 'svelte-sonner';
	import {
		Zap, Search, RefreshCw, Plus, Trash2,
		Activity, ChevronRight, ListFilter, Play, Square,
		Terminal, Code, X, CheckCircle2, GitBranch,
		AlertCircle, Timer, BarChart3, BookOpen, Cpu,
		Settings, Copy, ArrowRight
	} from 'lucide-svelte';

	const sfn = regionalClient(getSFNClient);

	// Page-level tab
	let pageTab = $state<'workflows' | 'activities' | 'metrics' | 'docs'>('workflows');

	// State Machines
	let loading = $state(false);
	let searchQuery = $state('');
	let typeFilter = $state<'ALL' | 'STANDARD' | 'EXPRESS'>('ALL');
	let stateMachines = $state<StateMachineListItem[]>([]);
	let selectedSM = $state<DescribeStateMachineCommandOutput | null>(null);
	let smTab = $state<'executions' | 'definition' | 'versions'>('executions');

	// Executions
	let executions = $state<ExecutionListItem[]>([]);
	let execStatusFilter = $state<'ALL' | 'RUNNING' | 'SUCCEEDED' | 'FAILED' | 'ABORTED'>('ALL');
	let selectedExecution = $state<DescribeExecutionOutput | null>(null);
	let loadingExecutions = $state(false);
	let loadingDetail = $state(false);

	// History
	let historyEvents = $state<HistoryEvent[]>([]);
	let loadingHistory = $state(false);
	let historyNextToken = $state<string>('');
	let historyHasMore = $state(false);
	let redriving = $state(false);
	// ASL validation
	let validationResult = $state<{ ok: boolean; message: string } | null>(null);
	let validating = $state(false);

	type TimelineState = { name: string; entered?: Date; exited?: Date; status: 'succeeded' | 'failed' | 'running' };
	const executionTimeline = $derived(() => {
		const states = new Map<string, TimelineState>();
		const order: string[] = [];
		for (const ev of historyEvents) {
			const enterName = ev.stateEnteredEventDetails?.name;
			const exitName = ev.stateExitedEventDetails?.name;
			if (enterName && !states.has(enterName)) {
				states.set(enterName, { name: enterName, entered: ev.timestamp, status: 'running' });
				order.push(enterName);
			}
			if (exitName && states.has(exitName)) {
				const s = states.get(exitName)!;
				s.exited = ev.timestamp;
				s.status = 'succeeded';
			}
			if ((ev.type ?? '').includes('Failed')) {
				// mark the most recent running state as failed
				for (let i = order.length - 1; i >= 0; i--) {
					const s = states.get(order[i])!;
					if (s.status === 'running') {
						s.status = 'failed';
						s.exited = ev.timestamp;
						break;
					}
				}
			}
		}
		return order.map((n) => states.get(n)!);
	});

	// Versions
	interface VersionSummary { stateMachineVersionArn?: string; description?: string; creationDate?: Date; }
	let versions = $state<VersionSummary[]>([]);
	let loadingVersions = $state(false);

	// Activities
	let activities = $state<ActivityListItem[]>([]);
	let loadingActivities = $state(false);
	let newActivityName = $state('');
	let creatingActivity = $state(false);

	// Create SM modal
	let showCreateModal = $state(false);
	let newSMName = $state('');
	let newSMType = $state<'STANDARD' | 'EXPRESS'>('STANDARD');
	let newSMRoleArn = $state('arn:aws:iam::000000000000:role/StepFunctionsRole');
	let newSMDefinition = $state(JSON.stringify({
		Comment: 'My state machine',
		StartAt: 'HelloWorld',
		States: { HelloWorld: { Type: 'Pass', Result: 'Hello, World!', End: true } }
	}, null, 2));
	let creating = $state(false);

	// Edit SM modal
	let showEditModal = $state(false);
	let editDefinition = $state('');
	let editRoleArn = $state('');
	let updating = $state(false);

	// Start Execution modal
	let showStartModal = $state(false);
	let executionInput = $state('{}');
	let executionName = $state('');
	let starting = $state(false);

	// Derived
	const filteredSMs = $derived(stateMachines.filter(sm => {
		const nm = sm.name?.toLowerCase().includes(searchQuery.toLowerCase());
		return nm && (typeFilter === 'ALL' || sm.type === typeFilter);
	}));
	const filteredExecutions = $derived(execStatusFilter === 'ALL'
		? executions
		: executions.filter(e => e.status === execStatusFilter));
	const execMetrics = $derived((() => {
		const c = { RUNNING: 0, SUCCEEDED: 0, FAILED: 0, ABORTED: 0 };
		for (const ex of executions) {
			const s = (ex.status ?? '') as keyof typeof c;
			if (s in c) c[s]++;
		}
		return c;
	})());

	// Helpers
	function statusDot(s?: string) {
		const m: Record<string, string> = { RUNNING: 'bg-emerald-400 animate-pulse', SUCCEEDED: 'bg-emerald-500', FAILED: 'bg-rose-500', ABORTED: 'bg-amber-500' };
		return m[s?.toUpperCase() ?? ''] ?? 'bg-slate-500';
	}
	function statusColor(s?: string) {
		const m: Record<string, string> = { RUNNING: 'text-emerald-400', SUCCEEDED: 'text-emerald-500', FAILED: 'text-rose-500', ABORTED: 'text-amber-500' };
		return m[s?.toUpperCase() ?? ''] ?? 'text-slate-400';
	}
	function eventColor(t: string) {
		if (t.includes('Started') || t.includes('Entered')) return 'text-blue-400';
		if (t.includes('Succeeded') || t.includes('Exited')) return 'text-emerald-400';
		if (t.includes('Failed') || t.includes('Aborted')) return 'text-rose-400';
		if (t.includes('Scheduled')) return 'text-amber-400';
		return 'text-slate-400';
	}
	function duration(start?: Date, stop?: Date) {
		if (!start) return '—';
		const ms = (stop ?? new Date()).getTime() - start.getTime();
		if (ms < 1000) return ms + 'ms';
		if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
		return Math.floor(ms / 60000) + 'm ' + Math.floor((ms % 60000) / 1000) + 's';
	}
	function fmtJSON(s: string) {
		try { return JSON.stringify(JSON.parse(s), null, 2); } catch { return s; }
	}
	async function cp(text: string) {
		try { await navigator.clipboard.writeText(text); toast.success('Copied'); } catch { toast.error('Copy failed'); }
	}

	// State Machine Actions
	async function loadStateMachines() {
		loading = true;
		try {
			const r = await sfn().send(new ListStateMachinesCommand({}));
			stateMachines = r.stateMachines ?? [];
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { loading = false; }
	}
	async function selectSM(arn: string) {
		loadingDetail = true; selectedExecution = null; historyEvents = []; versions = [];
		try {
			const [d, x] = await Promise.all([
				sfn().send(new DescribeStateMachineCommand({ stateMachineArn: arn })),
				sfn().send(new ListExecutionsCommand({ stateMachineArn: arn, maxResults: 50 }))
			]);
			selectedSM = d; executions = x.executions ?? [];
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { loadingDetail = false; }
	}
	async function refreshExecs() {
		if (!selectedSM?.stateMachineArn) return;
		loadingExecutions = true;
		try {
			const r = await sfn().send(new ListExecutionsCommand({ stateMachineArn: selectedSM.stateMachineArn, maxResults: 50 }));
			executions = r.executions ?? [];
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { loadingExecutions = false; }
	}
	async function selectExecution(arn: string) {
		try {
			selectedExecution = await sfn().send(new DescribeExecutionCommand({ executionArn: arn }));
			historyEvents = [];
		} catch (e: unknown) { toast.error((e as Error).message); }
	}
	async function loadHistory(token?: string) {
		if (!selectedExecution?.executionArn) return;
		loadingHistory = true;
		try {
			const r = await sfn().send(new GetExecutionHistoryCommand({
				executionArn: selectedExecution.executionArn,
				maxResults: 50, nextToken: token
			}));
			historyEvents = token ? [...historyEvents, ...(r.events ?? [])] : (r.events ?? []);
			historyNextToken = r.nextToken ?? '';
			historyHasMore = !!r.nextToken;
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { loadingHistory = false; }
	}
	async function stopExec(arn: string) {
		if (!await confirmDestructive({ title: 'Stop Execution', message: 'Stop this execution?' })) return;
		try {
			await sfn().send(new StopExecutionCommand({ executionArn: arn, error: 'ManualStop', cause: 'User requested' }));
			toast.success('Stop requested');
			if (selectedExecution?.executionArn === arn) {
				selectedExecution = await sfn().send(new DescribeExecutionCommand({ executionArn: arn }));
			}
			await refreshExecs();
		} catch (e: unknown) { toast.error((e as Error).message); }
	}
	async function redriveExec(arn: string) {
		redriving = true;
		try {
			await sfn().send(new RedriveExecutionCommand({ executionArn: arn }));
			toast.success('Redrive requested');
			if (selectedExecution?.executionArn === arn) {
				selectedExecution = await sfn().send(new DescribeExecutionCommand({ executionArn: arn }));
			}
			await refreshExecs();
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { redriving = false; }
	}
	async function validateDefinition() {
		validating = true;
		validationResult = null;
		try {
			const r = await sfn().send(new ValidateStateMachineDefinitionCommand({ definition: editDefinition }));
			if (r.result === 'OK') {
				validationResult = { ok: true, message: 'Definition is valid.' };
			} else {
				const diags = (r.diagnostics ?? []).map((d) => `${d.severity}: ${d.message}${d.location ? ` (${d.location})` : ''}`).join('\n');
				validationResult = { ok: false, message: diags || 'Definition is invalid.' };
			}
		} catch (e: unknown) {
			validationResult = { ok: false, message: (e as Error).message };
		} finally {
			validating = false;
		}
	}
	async function startExecution() {
		if (!selectedSM) return;
		starting = true;
		try {
			const r = await sfn().send(new StartExecutionCommand({
				stateMachineArn: selectedSM.stateMachineArn,
				name: executionName || undefined,
				input: executionInput
			}));
			toast.success('Started: ' + r.executionArn?.split(':').pop());
			showStartModal = false; executionName = ''; executionInput = '{}';
			await refreshExecs();
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { starting = false; }
	}
	async function createSM() {
		creating = true;
		try {
			await sfn().send(new CreateStateMachineCommand({
				name: newSMName, definition: newSMDefinition, roleArn: newSMRoleArn, type: newSMType as StateMachineType
			}));
			toast.success('Created ' + newSMName);
			showCreateModal = false; newSMName = '';
			await loadStateMachines();
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { creating = false; }
	}
	function openEdit() {
		if (!selectedSM) return;
		editDefinition = selectedSM.definition ?? '';
		editRoleArn = selectedSM.roleArn ?? '';
		showEditModal = true;
	}
	async function updateSM() {
		if (!selectedSM?.stateMachineArn) return;
		updating = true;
		try {
			await sfn().send(new UpdateStateMachineCommand({
				stateMachineArn: selectedSM.stateMachineArn,
				definition: editDefinition, roleArn: editRoleArn
			}));
			toast.success('Updated');
			showEditModal = false;
			await selectSM(selectedSM.stateMachineArn);
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { updating = false; }
	}
	async function deleteSM(arn?: string) {
		if (!arn || !await confirmDestructive({ title: 'Delete State Machine', message: 'Delete this state machine?' })) return;
		try {
			await sfn().send(new DeleteStateMachineCommand({ stateMachineArn: arn }));
			toast.success('Delete initiated');
			if (selectedSM?.stateMachineArn === arn) selectedSM = null;
			await loadStateMachines();
		} catch (e: unknown) { toast.error((e as Error).message); }
	}
	async function loadVersions() {
		if (!selectedSM?.stateMachineArn) return;
		loadingVersions = true;
		try {
			const r = await sfn().send(new ListStateMachineVersionsCommand({ stateMachineArn: selectedSM.stateMachineArn }));
			versions = (r as { stateMachineVersions?: VersionSummary[] }).stateMachineVersions ?? [];
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { loadingVersions = false; }
	}
	async function publishVersion() {
		if (!selectedSM?.stateMachineArn) return;
		try {
			await sfn().send(new PublishStateMachineVersionCommand({
				stateMachineArn: selectedSM.stateMachineArn,
				description: 'v' + (versions.length + 1) + ' - ' + new Date().toISOString().slice(0, 10)
			}));
			toast.success('Version published'); await loadVersions();
		} catch (e: unknown) { toast.error((e as Error).message); }
	}
	async function seedDemo() {
		const demos = [
			{name:'order-processing-demo',type:'STANDARD' as const,def:JSON.stringify({Comment:'Order processing',StartAt:'Validate',States:{Validate:{Type:'Pass',Next:'Payment'},Payment:{Type:'Pass',Next:'Fulfill'},Fulfill:{Type:'Pass',Next:'Notify'},Notify:{Type:'Pass',End:true}}})},
			{name:'fan-out-demo',type:'STANDARD' as const,def:JSON.stringify({Comment:'Fan-out',StartAt:'Parallel',States:{Parallel:{Type:'Parallel',Branches:[{StartAt:'A',States:{A:{Type:'Pass',End:true}}},{StartAt:'B',States:{B:{Type:'Pass',End:true}}}],End:true}}})},
			{name:'map-iterator-demo',type:'STANDARD' as const,def:JSON.stringify({Comment:'Map demo',StartAt:'MapItems',States:{MapItems:{Type:'Map',Iterator:{StartAt:'Process',States:{Process:{Type:'Pass',End:true}}},MaxConcurrency:5,End:true}}})},
			{name:'express-demo',type:'EXPRESS' as const,def:JSON.stringify({Comment:'Express workflow',StartAt:'Fast',States:{Fast:{Type:'Pass',End:true}}})}
		];
		let c = 0;
		for (const d of demos) {
			try {
				await sfn().send(new CreateStateMachineCommand({name:d.name,definition:d.def,roleArn:'arn:aws:iam::000000000000:role/StepFunctionsRole',type:d.type}));
				c++;
			} catch {
				// skip: state machine may already exist
			}
		}
		toast.success('Seeded ' + c + ' demo workflow(s)');
		await loadStateMachines();
	}

	// Activity Actions
	async function loadActivities() {
		loadingActivities = true;
		try {
			activities = (await sfn().send(new ListActivitiesCommand({}))).activities ?? [];
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { loadingActivities = false; }
	}
	async function createActivity() {
		if (!newActivityName.trim()) return;
		creatingActivity = true;
		try {
			await sfn().send(new CreateActivityCommand({ name: newActivityName.trim() }));
			toast.success('Created ' + newActivityName); newActivityName = '';
			await loadActivities();
		} catch (e: unknown) { toast.error((e as Error).message); }
		finally { creatingActivity = false; }
	}
	async function deleteActivity(arn: string) {
		if (!await confirmDestructive({ title: 'Delete Activity', message: 'Delete this activity?' })) return;
		try {
			await sfn().send(new DeleteActivityCommand({ activityArn: arn }));
			toast.success('Deleted'); await loadActivities();
		} catch (e: unknown) { toast.error((e as Error).message); }
	}

	async function switchPageTab(tab: typeof pageTab) {
		pageTab = tab;
		if (tab === 'activities') await loadActivities();
	}
	async function switchSMTab(tab: typeof smTab) {
		smTab = tab;
		if (tab === 'versions' && selectedSM) await loadVersions();
	}

	// A state machine ARN (and its executions, history, and versions) is only
	// unique within a region, so a selection from the old region must not
	// survive a region switch -- clear it and fall back to the list. `pageTab`
	// is read with `untrack` since switchPageTab() already reloads activities
	// directly on tab switch; letting the effect also depend on it would
	// double-fetch on every subsequent region change.
	onRegionChange(() => {
		selectedSM = null;
		selectedExecution = null;
		executions = [];
		historyEvents = [];
		versions = [];
		activities = [];
		loadStateMachines();
		if (untrack(() => pageTab) === 'activities') {
			loadActivities();
		}
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-pink-500/20 rounded-xl"><Zap class="w-8 h-8 text-pink-500" /></div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-pink-600 to-purple-600 dark:from-pink-400 dark:to-purple-400 bg-clip-text text-transparent italic">Step Functions</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Orchestrate serverless workflows using visual state machines.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button onclick={seedDemo} class="px-4 py-2 bg-purple-600/80 hover:bg-purple-600 text-white rounded-xl font-black uppercase text-[10px] tracking-widest active:scale-95 transition-all flex items-center gap-1.5">
				<Zap class="w-3.5 h-3.5" /> Demo Data
			</button>
			<button onclick={() => showCreateModal = true} class="px-4 py-2 bg-pink-600 hover:bg-pink-700 text-white rounded-xl font-black uppercase text-[10px] tracking-widest shadow-lg active:scale-95 transition-all flex items-center gap-1.5">
				<Plus class="w-4 h-4" /> New Workflow
			</button>
			<button onclick={loadStateMachines} class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95">
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
		</div>
	</div>

	<!-- Page Tabs -->
	<div class="flex gap-1 p-1 bg-slate-100/50 dark:bg-slate-800/50 rounded-xl w-fit border border-slate-200/50 dark:border-slate-700/50">
		{#each [['workflows','Workflows'],['activities','Activities'],['metrics','Metrics'],['docs','Docs']] as [tab, label]}
			<button onclick={() => switchPageTab(tab as any)} class="px-4 py-2 rounded-lg text-[11px] font-black uppercase tracking-widest transition-all {pageTab === tab ? 'bg-white dark:bg-slate-700 text-pink-600 dark:text-pink-400 shadow' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-300'}">{label}</button>
		{/each}
	</div>

	<!-- WORKFLOWS TAB -->
	{#if pageTab === 'workflows'}
	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- SM List -->
		<div class="lg:col-span-3">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-3 border-b border-slate-200 dark:border-slate-700/50 space-y-2">
					<div class="relative">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input type="text" bind:value={searchQuery} placeholder="Search workflows..." class="w-full pl-9 pr-3 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-pink-500 outline-none" />
					</div>
					<div class="flex gap-1">
						{#each ['ALL','STANDARD','EXPRESS'] as f}
							<button onclick={() => typeFilter = f as any} class="flex-1 py-1 text-[9px] font-black uppercase rounded-lg transition-all {typeFilter === f ? 'bg-pink-500 text-white' : 'bg-slate-100 dark:bg-slate-700 text-slate-500'}">{f}</button>
						{/each}
					</div>
				</div>
				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#if loading && !stateMachines.length}
						{#each Array(3) as _}<div class="p-4 animate-pulse"><div class="h-8 bg-slate-200/50 dark:bg-slate-700/30 rounded-lg"></div></div>{/each}
					{:else}
						{#each filteredSMs as sm}
							<div role="button" tabindex="0" onclick={() => sm.stateMachineArn && selectSM(sm.stateMachineArn)} onkeydown={(e) => e.key === 'Enter' && sm.stateMachineArn && selectSM(sm.stateMachineArn)}
								class="p-3 hover:bg-pink-500/5 cursor-pointer transition-all border-l-4 {selectedSM?.stateMachineArn === sm.stateMachineArn ? 'border-pink-500 bg-pink-500/10' : 'border-transparent'}">
								<div class="flex items-center justify-between mb-0.5">
									<div class="font-black text-slate-900 dark:text-white uppercase text-xs truncate max-w-[130px] italic">{sm.name}</div>
									<span class="text-[8px] font-black px-1.5 py-0.5 rounded {sm.type === 'EXPRESS' ? 'bg-purple-500/20 text-purple-500' : 'bg-pink-500/10 text-pink-500'}">{sm.type}</span>
								</div>
								<div class="text-[8px] text-slate-400 font-mono truncate">{sm.stateMachineArn?.split(':').pop()}</div>
							</div>
						{/each}
						{#if !filteredSMs.length}<div class="p-10 text-center text-slate-400 text-xs italic">{searchQuery ? 'No matches.' : 'No state machines found.'}</div>{/if}
					{/if}
				</div>
			</div>
		</div>

		<!-- Detail -->
		<div class="lg:col-span-9 space-y-4">
			{#if selectedSM}
				<!-- SM header card -->
				<div class="p-5 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
					<div class="flex justify-between items-start mb-4">
						<div>
							<h2 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">{selectedSM.name}</h2>
							<div class="flex flex-wrap gap-2 mt-1">
								<span class="px-2 py-0.5 rounded bg-pink-500/10 text-pink-600 text-[10px] font-black uppercase">{selectedSM.status}</span>
								<span class="px-2 py-0.5 rounded bg-slate-100 dark:bg-slate-700 text-[10px] font-black uppercase">{selectedSM.type}</span>
							</div>
							{#if selectedSM.roleArn}
							<div class="flex items-center gap-1.5 mt-2">
								<span class="text-[9px] text-slate-400 uppercase font-bold">Role:</span>
								<span class="text-[9px] font-mono text-slate-500 dark:text-slate-400 truncate max-w-xs">{selectedSM.roleArn}</span>
								<button onclick={() => cp(selectedSM?.roleArn ?? '')} class="text-slate-400 hover:text-pink-500 flex-shrink-0" title="Copy Role ARN"><Copy class="w-3 h-3" /></button>
							</div>
							{/if}
						</div>
						<div class="flex gap-2">
							<button onclick={() => showStartModal = true} class="px-3 py-2 bg-pink-600 hover:bg-pink-700 text-white rounded-xl text-[10px] font-black uppercase tracking-widest flex items-center gap-1.5 active:scale-95 transition-all">
								<Play class="w-3.5 h-3.5 fill-current" /> Execute
							</button>
							<button onclick={openEdit} class="p-2.5 bg-slate-100 dark:bg-slate-700 hover:text-pink-500 rounded-xl transition-all" title="Edit"><Settings class="w-4 h-4" /></button>
							<button onclick={() => deleteSM(selectedSM?.stateMachineArn)} class="p-2.5 bg-slate-100 dark:bg-slate-700 hover:text-rose-500 rounded-xl transition-all" title="Delete"><Trash2 class="w-4 h-4" /></button>
						</div>
					</div>
					<div class="flex gap-1">
						{#each [['executions','Executions'],['definition','Definition'],['versions','Versions']] as [tab, label]}
							<button onclick={() => switchSMTab(tab as any)} class="px-3 py-1.5 text-[10px] font-black uppercase tracking-widest rounded-lg transition-all {smTab === tab ? 'bg-pink-500 text-white' : 'text-slate-500 hover:bg-slate-100 dark:hover:bg-slate-700'}">{label}</button>
						{/each}
					</div>
				</div>

				{#if smTab === 'executions'}
				<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
					<!-- Execution list -->
					<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
						<div class="p-3 border-b border-slate-200 dark:border-slate-700/50 flex items-center justify-between">
							<h3 class="text-[10px] font-black text-slate-500 uppercase tracking-widest italic flex items-center gap-1.5"><Timer class="w-3.5 h-3.5" /> Executions</h3>
							<div class="flex items-center gap-1">
								<button onclick={refreshExecs} class="p-1 text-slate-400 hover:text-pink-500"><RefreshCw class="w-3.5 h-3.5 {loadingExecutions ? 'animate-spin' : ''}" /></button>
								{#each ['ALL','RUNNING','SUCCEEDED','FAILED','ABORTED'] as f}
									<button onclick={() => execStatusFilter = f as any} class="px-1 py-0.5 text-[7px] font-black uppercase rounded transition-all {execStatusFilter === f ? 'bg-pink-500 text-white' : 'bg-slate-100 dark:bg-slate-700 text-slate-500'}">{f.slice(0,3)}</button>
								{/each}
							</div>
						</div>
						<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[450px] overflow-y-auto">
							{#each filteredExecutions as exec}
								<div role="button" tabindex="0" onclick={() => selectExecution(exec.executionArn!)} onkeydown={(e) => e.key === 'Enter' && selectExecution(exec.executionArn!)}
									class="p-3 hover:bg-pink-500/5 cursor-pointer flex items-center justify-between {selectedExecution?.executionArn === exec.executionArn ? 'bg-pink-500/10' : ''}">
									<div class="flex items-center gap-2 min-w-0">
										<div class="w-2 h-2 rounded-full shrink-0 {statusDot(exec.status)}"></div>
										<div class="min-w-0">
											<div class="text-[11px] font-black uppercase tracking-tight truncate italic">{exec.name}</div>
											<div class="text-[9px] text-slate-400">{exec.startDate?.toLocaleString()}</div>
										</div>
									</div>
									<div class="flex items-center gap-1 shrink-0">
										{#if exec.status === 'RUNNING'}
											<button onclick={(ev) => { ev.stopPropagation(); stopExec(exec.executionArn!); }} class="p-1 bg-rose-500/10 hover:bg-rose-500/20 text-rose-500 rounded-lg" title="Stop"><Square class="w-3 h-3 fill-current" /></button>
										{/if}
										<ChevronRight class="w-3.5 h-3.5 text-slate-300" />
									</div>
								</div>
							{:else}
								<div class="p-10 text-center text-slate-400 text-xs italic">No executions.</div>
							{/each}
						</div>
					</div>
					<!-- Execution detail -->
					<div class="bg-slate-900 dark:bg-black rounded-2xl border border-slate-800 min-h-[450px] flex flex-col overflow-hidden">
						{#if selectedExecution}
							<div class="p-3 border-b border-slate-800 bg-slate-800/50 flex justify-between items-center">
								<div class="flex items-center gap-2">
									<Activity class="w-4 h-4 {statusColor(selectedExecution.status)}" />
									<div>
										<div class="text-xs font-black text-white uppercase italic">{selectedExecution.name}</div>
										<div class="text-[9px] font-bold {statusColor(selectedExecution.status)} uppercase">{selectedExecution.status}</div>
									</div>
								</div>
								<div class="flex gap-1.5 items-center">
									{#if selectedExecution.status === 'RUNNING'}
										<button onclick={() => stopExec(selectedExecution!.executionArn!)} class="p-1.5 bg-rose-500/20 text-rose-400 hover:bg-rose-500/30 rounded-lg" title="Stop"><Square class="w-3.5 h-3.5 fill-current" /></button>
									{/if}
									{#if selectedExecution.status === 'FAILED' || selectedExecution.status === 'TIMED_OUT' || selectedExecution.status === 'ABORTED'}
										<button disabled={redriving} onclick={() => redriveExec(selectedExecution!.executionArn!)} class="px-2 py-1 bg-amber-500/20 text-amber-400 hover:bg-amber-500/30 rounded-lg text-[9px] font-black uppercase tracking-widest disabled:opacity-50" title="Redrive failed execution">{redriving ? '...' : 'Redrive'}</button>
									{/if}
									<button onclick={() => loadHistory()} class="p-1.5 bg-slate-700 hover:bg-slate-600 text-slate-300 rounded-lg" title="Load history"><ListFilter class="w-3.5 h-3.5" /></button>
									<button onclick={() => selectedExecution = null} class="text-slate-500 hover:text-white"><X class="w-4 h-4" /></button>
								</div>
							</div>
							<div class="flex-1 overflow-y-auto p-4 space-y-3">
								<div class="grid grid-cols-2 gap-2">
									<div class="p-2.5 bg-slate-800/50 rounded-xl border border-slate-800">
										<div class="text-[8px] font-black text-slate-500 uppercase mb-1">Started</div>
										<div class="text-[9px] text-slate-300 font-mono">{selectedExecution.startDate?.toLocaleString()}</div>
									</div>
									<div class="p-2.5 bg-slate-800/50 rounded-xl border border-slate-800">
										<div class="text-[8px] font-black text-slate-500 uppercase mb-1">Duration</div>
										<div class="text-[9px] text-slate-300 font-mono">{duration(selectedExecution.startDate, selectedExecution.stopDate)}</div>
									</div>
								</div>
								<div>
									<div class="flex justify-between items-center mb-1">
										<div class="text-[8px] font-black text-slate-500 uppercase flex items-center gap-1"><ArrowRight class="w-3 h-3" /> Input</div>
										<button onclick={() => cp(selectedExecution?.input ?? '')} class="text-slate-600 hover:text-slate-400"><Copy class="w-3 h-3" /></button>
									</div>
									<div class="bg-black/60 p-3 rounded-xl border border-slate-800 max-h-28 overflow-y-auto">
										<pre class="text-[10px] font-mono text-emerald-400 italic">{fmtJSON(selectedExecution.input || '{}')}</pre>
									</div>
								</div>
								{#if selectedExecution.output}
								<div>
									<div class="flex justify-between items-center mb-1">
										<div class="text-[8px] font-black text-slate-500 uppercase flex items-center gap-1"><CheckCircle2 class="w-3 h-3" /> Output</div>
										<button onclick={() => cp(selectedExecution?.output ?? '')} class="text-slate-600 hover:text-slate-400"><Copy class="w-3 h-3" /></button>
									</div>
									<div class="bg-black/60 p-3 rounded-xl border border-slate-800 max-h-28 overflow-y-auto">
										<pre class="text-[10px] font-mono text-emerald-400 italic">{fmtJSON(selectedExecution.output)}</pre>
									</div>
								</div>
								{/if}
								{#if selectedExecution.error}
								<div class="p-3 bg-rose-950/40 rounded-xl border border-rose-900/50">
									<div class="text-[8px] font-black text-rose-400 uppercase flex items-center gap-1"><AlertCircle class="w-3 h-3" /> Error</div>
									<div class="text-[10px] text-rose-300 font-mono mt-1">{selectedExecution.error}</div>
									{#if selectedExecution.cause}<div class="text-[9px] text-rose-400/70 mt-1">{selectedExecution.cause}</div>{/if}
								</div>
								{/if}
								{#if executionTimeline().length > 0}
								<div>
									<div class="text-[8px] font-black text-slate-500 uppercase mb-2 flex items-center gap-1"><Activity class="w-3 h-3" /> State Timeline ({executionTimeline().length})</div>
									<div class="space-y-1">
										{#each executionTimeline() as st}
											<div class="flex items-center gap-2 p-1.5 rounded-lg border {st.status === 'failed' ? 'bg-rose-950/40 border-rose-900/50' : st.status === 'running' ? 'bg-amber-950/30 border-amber-900/40' : 'bg-emerald-950/30 border-emerald-900/40'}">
												<span class="w-2 h-2 rounded-full shrink-0 {st.status === 'failed' ? 'bg-rose-400' : st.status === 'running' ? 'bg-amber-400 animate-pulse' : 'bg-emerald-400'}"></span>
												<span class="text-[10px] font-mono text-slate-200 truncate flex-1">{st.name}</span>
												<span class="text-[8px] text-slate-500 font-mono shrink-0">{duration(st.entered, st.exited)}</span>
											</div>
										{/each}
									</div>
								</div>
								{/if}
								{#if historyEvents.length > 0}
								<div>
									<div class="text-[8px] font-black text-slate-500 uppercase mb-2 flex items-center gap-1"><ListFilter class="w-3 h-3" /> Events ({historyEvents.length})</div>
									<div class="space-y-1 max-h-[280px] overflow-y-auto">
										{#each historyEvents as ev}
											<div class="flex items-start gap-2 p-1.5 bg-slate-800/30 rounded-lg">
												<span class="text-[8px] {eventColor(ev.type ?? '')} shrink-0 font-mono">#{ev.id}</span>
												<div class="min-w-0">
													<div class="text-[9px] font-bold {eventColor(ev.type ?? '')}">{ev.type}</div>
													{#if ev.stateEnteredEventDetails?.name}<div class="text-[8px] text-slate-500">→ {ev.stateEnteredEventDetails.name}</div>{/if}
													{#if ev.stateExitedEventDetails?.name}<div class="text-[8px] text-slate-500">← {ev.stateExitedEventDetails.name}</div>{/if}
												</div>
											</div>
										{/each}
									</div>
									{#if historyHasMore}
									<button onclick={() => loadHistory(historyNextToken)} class="w-full py-2 text-[8px] font-black text-slate-400 uppercase tracking-widest hover:text-slate-200">{loadingHistory ? 'Loading...' : 'Load More'}</button>
									{/if}
								</div>
								{/if}
							</div>
						{:else}
							<div class="flex-1 flex flex-col items-center justify-center opacity-30 p-8 text-center">
								<Terminal class="w-10 h-10 text-slate-600 mb-3" />
								<p class="text-[10px] text-slate-600 italic">Select an execution to view details.</p>
							</div>
						{/if}
					</div>
				</div>

				{:else if smTab === 'definition'}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-5">
					<div class="flex items-center justify-between mb-3">
						<h3 class="text-[10px] font-black text-slate-500 uppercase tracking-widest italic flex items-center gap-2"><Code class="w-4 h-4" /> ASL Definition</h3>
						<div class="flex gap-2">
							<button onclick={openEdit} class="px-3 py-1.5 bg-pink-600 hover:bg-pink-700 text-white rounded-lg text-[10px] font-black uppercase tracking-widest">Edit</button>
							<button onclick={() => cp(selectedSM?.definition ?? '')} class="p-1.5 bg-slate-100 dark:bg-slate-700 text-slate-500 hover:text-pink-500 rounded-lg"><Copy class="w-4 h-4" /></button>
						</div>
					</div>
					<div class="bg-slate-900 rounded-xl border border-slate-800 p-4 max-h-[500px] overflow-y-auto">
						<pre class="text-[10px] font-mono text-pink-400 leading-relaxed">{fmtJSON(selectedSM.definition ?? '{}')}</pre>
					</div>
				</div>

				{:else if smTab === 'versions'}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
					<div class="p-4 border-b border-slate-200 dark:border-slate-700/50 flex items-center justify-between">
						<h3 class="text-[10px] font-black text-slate-500 uppercase tracking-widest italic flex items-center gap-2"><GitBranch class="w-4 h-4" /> Versions</h3>
						<button onclick={publishVersion} class="px-3 py-1.5 bg-purple-600 hover:bg-purple-700 text-white rounded-lg text-[10px] font-black uppercase tracking-widest flex items-center gap-1.5"><Plus class="w-3.5 h-3.5" /> Publish</button>
					</div>
					{#if loadingVersions}
						<div class="p-8 flex justify-center"><RefreshCw class="w-5 h-5 text-pink-500 animate-spin" /></div>
					{:else if versions.length === 0}
						<div class="p-10 text-center text-slate-400 text-xs italic">No published versions. Click Publish to snapshot the current definition.</div>
					{:else}
						<div class="divide-y divide-slate-100 dark:divide-slate-700/50">
							{#each versions as v}
								<div class="p-4 flex items-center justify-between">
									<div>
										<div class="text-xs font-black text-slate-900 dark:text-white font-mono">{v.stateMachineVersionArn?.split(':').pop()}</div>
										<div class="text-[9px] text-slate-400">{v.creationDate?.toLocaleString?.() ?? ''}</div>
										{#if v.description}<div class="text-[9px] text-slate-500 italic">{v.description}</div>{/if}
									</div>
									<button onclick={() => cp(v.stateMachineVersionArn ?? '')} class="text-slate-400 hover:text-pink-500"><Copy class="w-3.5 h-3.5" /></button>
								</div>
							{/each}
						</div>
					{/if}
				</div>
				{/if}

			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-2xl p-20 text-center flex flex-col items-center gap-4">
					<div class="p-6 bg-slate-50 dark:bg-slate-800 rounded-3xl"><Zap class="w-12 h-12 text-slate-300 dark:text-slate-600" /></div>
					<h3 class="text-xl font-bold text-slate-900 dark:text-white uppercase italic">Workflow Orchestration</h3>
					<p class="text-slate-500 text-sm max-w-sm">Select a state machine from the list, or create a new one to get started.</p>
					<div class="flex gap-3">
						<button onclick={() => showCreateModal = true} class="px-4 py-2 bg-pink-600 text-white rounded-xl font-black uppercase text-[10px] tracking-widest flex items-center gap-1.5"><Plus class="w-4 h-4" />New Workflow</button>
						<button onclick={seedDemo} class="px-4 py-2 bg-purple-600/80 text-white rounded-xl font-black uppercase text-[10px] tracking-widest flex items-center gap-1.5"><Zap class="w-4 h-4" />Demo Data</button>
					</div>
				</div>
			{/if}
		</div>
	</div>

	<!-- ACTIVITIES TAB -->
	{:else if pageTab === 'activities'}
	<div class="space-y-4">
		<div class="p-5 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
			<h3 class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-4 italic flex items-center gap-2"><Cpu class="w-4 h-4" /> Create Activity</h3>
			<div class="flex gap-3">
				<input type="text" bind:value={newActivityName} placeholder="my-worker-activity" class="flex-1 px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-pink-500 text-sm" />
				<button onclick={createActivity} disabled={creatingActivity || !newActivityName.trim()} class="px-4 py-2.5 bg-pink-600 hover:bg-pink-700 text-white rounded-xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">{creatingActivity ? 'Creating...' : 'Create'}</button>
			</div>
		</div>
		<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
			<div class="p-4 border-b border-slate-200 dark:border-slate-700/50 flex items-center justify-between">
				<h3 class="text-[10px] font-black text-slate-500 uppercase tracking-widest italic flex items-center gap-2"><Cpu class="w-4 h-4" /> Activities ({activities.length})</h3>
				<button onclick={loadActivities} class="p-2 text-slate-400 hover:text-pink-500"><RefreshCw class="w-4 h-4 {loadingActivities ? 'animate-spin' : ''}" /></button>
			</div>
			{#if loadingActivities}
				<div class="p-8 flex justify-center"><RefreshCw class="w-5 h-5 text-pink-500 animate-spin" /></div>
			{:else if activities.length === 0}
				<div class="p-10 text-center text-slate-400 text-xs italic">No activities. Create one to enable activity-based task workers.</div>
			{:else}
				<div class="divide-y divide-slate-100 dark:divide-slate-700/50">
					{#each activities as act}
						<div class="p-4 flex items-center justify-between">
							<div>
								<div class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-tight italic">{act.name}</div>
								<div class="text-[9px] text-slate-400 font-mono mt-0.5">{act.activityArn}</div>
								<div class="text-[9px] text-slate-500 mt-0.5">Created: {act.creationDate?.toLocaleString()}</div>
							</div>
							<div class="flex gap-2">
								<button onclick={() => cp(act.activityArn ?? '')} class="p-2 text-slate-400 hover:text-pink-500"><Copy class="w-4 h-4" /></button>
								<button onclick={() => deleteActivity(act.activityArn!)} class="p-2 text-slate-400 hover:text-rose-500"><Trash2 class="w-4 h-4" /></button>
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	</div>

	<!-- METRICS TAB -->
	{:else if pageTab === 'metrics'}
	<div class="space-y-6">
		<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
			{#each [
				['Total Workflows', stateMachines.length, 'text-pink-500'],
				['Standard', stateMachines.filter(s => s.type === 'STANDARD').length, 'text-blue-500'],
				['Express', stateMachines.filter(s => s.type === 'EXPRESS').length, 'text-purple-500'],
				['Activities', activities.length, 'text-amber-500']
			] as [label, count, color]}
				<div class="p-5 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
					<div class="text-3xl font-black {color}">{count}</div>
					<div class="text-[10px] font-black text-slate-500 uppercase tracking-widest mt-1">{label}</div>
				</div>
			{/each}
		</div>
		{#if selectedSM && executions.length > 0}
		<div class="p-5 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
			<h3 class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-4 italic flex items-center gap-2"><BarChart3 class="w-4 h-4" /> Executions — {selectedSM.name}</h3>
			<div class="grid grid-cols-2 md:grid-cols-5 gap-3">
				{#each [['Running',execMetrics.RUNNING,'text-emerald-400'],['Succeeded',execMetrics.SUCCEEDED,'text-emerald-500'],['Failed',execMetrics.FAILED,'text-rose-500'],['Aborted',execMetrics.ABORTED,'text-amber-500'],['Total',executions.length,'text-slate-300']] as [l,c,col]}
					<div class="p-3 bg-slate-50 dark:bg-slate-900/30 rounded-xl border border-slate-100 dark:border-slate-700/50 text-center">
						<div class="text-2xl font-black {col}">{c}</div>
						<div class="text-[8px] font-black text-slate-500 uppercase mt-0.5">{l}</div>
					</div>
				{/each}
			</div>
			<div class="mt-4 space-y-1">
				<div class="flex justify-between text-[9px] text-slate-500 uppercase font-black"><span>Success Rate</span><span>{Math.round((execMetrics.SUCCEEDED / executions.length) * 100)}%</span></div>
				<div class="h-2 bg-slate-200 dark:bg-slate-700 rounded-full overflow-hidden">
					<div class="h-full bg-emerald-500 rounded-full transition-all duration-500" style="width:{Math.round((execMetrics.SUCCEEDED / executions.length) * 100)}%"></div>
				</div>
			</div>
		</div>
		{:else}
		<div class="p-8 text-center text-slate-400 text-sm italic">Select a state machine in the Workflows tab to see execution metrics here.</div>
		{/if}
	</div>

	<!-- DOCS TAB -->
	{:else if pageTab === 'docs'}
	<div class="space-y-6">
		<div class="p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
			<h3 class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-5 italic flex items-center gap-2"><BookOpen class="w-4 h-4" /> Supported Operations</h3>
			<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
				{#each [
					{g:'State Machines', ops:[['CreateStateMachine','Create STANDARD or EXPRESS state machine'],['UpdateStateMachine','Update definition, role ARN'],['DeleteStateMachine','Delete SM and execution history'],['DescribeStateMachine','Get SM details'],['ListStateMachines','List all SMs (paginated)'],['ValidateStateMachineDefinition','Validate ASL without creating SM']]},
					{g:'Versions & Aliases', ops:[['PublishStateMachineVersion','Snapshot current definition'],['DescribeStateMachineVersion','Get version details'],['DeleteStateMachineVersion','Delete a version'],['ListStateMachineVersions','List all versions'],['CreateStateMachineAlias','Create routing alias'],['UpdateStateMachineAlias','Update alias weights'],['DeleteStateMachineAlias','Delete alias'],['DescribeStateMachineAlias','Get alias details'],['ListStateMachineAliases','List all aliases']]},
					{g:'Executions', ops:[['StartExecution','Start STANDARD execution'],['StartSyncExecution','Run EXPRESS synchronously'],['StopExecution','Abort running execution'],['RedriveExecution','Re-run FAILED/ABORTED execution'],['DescribeExecution','Get execution status/I/O'],['DescribeStateMachineForExecution','Get SM definition at exec start'],['ListExecutions','List executions with status filter'],['GetExecutionHistory','Get paginated execution events']]},
					{g:'Activities', ops:[['CreateActivity','Register activity worker'],['DeleteActivity','Delete activity'],['DescribeActivity','Get activity metadata'],['ListActivities','List all activities'],['GetActivityTask','Long-poll for pending task (60s)'],['SendTaskSuccess','Report task success'],['SendTaskFailure','Report task failure'],['SendTaskHeartbeat','Keep task alive']]},
					{g:'Tags', ops:[['TagResource','Add/update tags'],['UntagResource','Remove tags'],['ListTagsForResource','List resource tags']]}
				] as {g, ops}}
					<div class="rounded-xl border border-slate-200 dark:border-slate-700/50 overflow-hidden">
						<div class="px-4 py-2.5 bg-slate-50 dark:bg-slate-900/40 border-b border-slate-200 dark:border-slate-700/50">
							<h4 class="text-[10px] font-black text-slate-600 dark:text-slate-300 uppercase tracking-widest">{g}</h4>
						</div>
						<div class="divide-y divide-slate-100 dark:divide-slate-800">
							{#each ops as [op, desc]}
								<div class="px-4 py-2 flex items-start gap-3">
									<code class="text-[10px] font-mono font-black text-pink-600 dark:text-pink-400 shrink-0">{op}</code>
									<span class="text-[10px] text-slate-500">{desc}</span>
								</div>
							{/each}
						</div>
					</div>
				{/each}
			</div>
		</div>
		<div class="p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
			<h3 class="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-4 italic flex items-center gap-2"><Code class="w-4 h-4" /> ASL State Types</h3>
			<div class="grid grid-cols-2 md:grid-cols-4 gap-3">
				{#each [['Task','Lambda, SQS, SNS, DynamoDB, Activity'],['Pass','Transform data'],['Choice','Branch on conditions'],['Wait','Delay for duration/timestamp'],['Succeed','Terminal success'],['Fail','Terminal with error'],['Parallel','Concurrent branches'],['Map','Iterate array with concurrency']] as [t, d]}
					<div class="p-3 rounded-xl border border-slate-200 dark:border-slate-700/50 bg-slate-50/50 dark:bg-slate-900/20">
						<div class="text-[11px] font-black text-pink-600 dark:text-pink-400 uppercase mb-1">{t}</div>
						<div class="text-[9px] text-slate-500">{d}</div>
					</div>
				{/each}
			</div>
		</div>
	</div>
	{/if}
</div>

<!-- Start Execution Modal -->
{#if showStartModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
	<div role="none" onclick={() => showStartModal = false} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"></div>
	<div class="relative w-full max-w-lg bg-white dark:bg-slate-800 rounded-3xl shadow-2xl border border-pink-500/20">
		<div class="p-6">
			<h3 class="text-xl font-black text-slate-900 dark:text-white mb-5 uppercase italic">Execute — {selectedSM?.name}</h3>
			<form onsubmit={(e) => { e.preventDefault(); startExecution(); }} class="space-y-4">
				<div>
					<label class="block text-[10px] font-black text-slate-500 uppercase mb-1.5">Name <span class="text-slate-400 normal-case font-normal">(optional)</span></label>
					<input bind:value={executionName} type="text" placeholder="my-execution" class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-pink-500 text-sm" />
				</div>
				<div>
					<label class="block text-[10px] font-black text-slate-500 uppercase mb-1.5">Input JSON</label>
					<textarea bind:value={executionInput} rows="7" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-2xl outline-none focus:ring-2 focus:ring-pink-500 font-mono text-xs"></textarea>
				</div>
				<div class="flex gap-3">
					<button type="button" onclick={() => showStartModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 rounded-2xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
					<button type="submit" disabled={starting} class="flex-1 px-4 py-3 bg-pink-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg disabled:opacity-50">{starting ? 'Starting...' : 'Start'}</button>
				</div>
			</form>
		</div>
	</div>
</div>
{/if}

<!-- Create SM Modal -->
{#if showCreateModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
	<div role="none" onclick={() => showCreateModal = false} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"></div>
	<div class="relative w-full max-w-2xl bg-white dark:bg-slate-800 rounded-3xl shadow-2xl border border-pink-500/20">
		<div class="p-6">
			<h3 class="text-xl font-black text-slate-900 dark:text-white mb-5 uppercase italic">Create State Machine</h3>
			<form onsubmit={(e) => { e.preventDefault(); createSM(); }} class="space-y-4">
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label class="block text-[10px] font-black text-slate-500 uppercase mb-1.5">Name</label>
						<input bind:value={newSMName} type="text" placeholder="my-workflow" required class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-pink-500 text-sm" />
					</div>
					<div>
						<label class="block text-[10px] font-black text-slate-500 uppercase mb-1.5">Type</label>
						<select bind:value={newSMType} class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-pink-500 text-sm">
							<option>STANDARD</option><option>EXPRESS</option>
						</select>
					</div>
				</div>
				<div>
					<label class="block text-[10px] font-black text-slate-500 uppercase mb-1.5">Role ARN</label>
					<input bind:value={newSMRoleArn} type="text" class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-pink-500 text-sm font-mono" />
				</div>
				<div>
					<label class="block text-[10px] font-black text-slate-500 uppercase mb-1.5">ASL Definition</label>
					<textarea bind:value={newSMDefinition} rows="10" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-2xl outline-none focus:ring-2 focus:ring-pink-500 font-mono text-xs"></textarea>
				</div>
				<div class="flex gap-3">
					<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 rounded-2xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
					<button type="submit" disabled={creating} class="flex-1 px-4 py-3 bg-pink-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">{creating ? 'Creating...' : 'Create'}</button>
				</div>
			</form>
		</div>
	</div>
</div>
{/if}

<!-- Edit SM Modal -->
{#if showEditModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
	<div role="none" onclick={() => showEditModal = false} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"></div>
	<div class="relative w-full max-w-2xl bg-white dark:bg-slate-800 rounded-3xl shadow-2xl border border-pink-500/20">
		<div class="p-6">
			<h3 class="text-xl font-black text-slate-900 dark:text-white mb-5 uppercase italic">Edit — {selectedSM?.name}</h3>
			<form onsubmit={(e) => { e.preventDefault(); updateSM(); }} class="space-y-4">
				<div>
					<label class="block text-[10px] font-black text-slate-500 uppercase mb-1.5">Role ARN</label>
					<input bind:value={editRoleArn} type="text" class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-pink-500 text-sm font-mono" />
				</div>
				<div>
					<div class="flex items-center justify-between mb-1.5">
						<label class="block text-[10px] font-black text-slate-500 uppercase">ASL Definition</label>
						<button type="button" disabled={validating} onclick={validateDefinition} class="px-2 py-1 bg-indigo-600 text-white rounded-lg text-[9px] font-black uppercase tracking-widest disabled:opacity-50">{validating ? 'Validating...' : 'Validate'}</button>
					</div>
					<textarea bind:value={editDefinition} rows="12" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-2xl outline-none focus:ring-2 focus:ring-pink-500 font-mono text-xs"></textarea>
					{#if validationResult}
						<div class="mt-2 p-2 rounded-lg text-[10px] font-mono whitespace-pre-wrap {validationResult.ok ? 'bg-emerald-950/40 text-emerald-300 border border-emerald-900/50' : 'bg-rose-950/40 text-rose-300 border border-rose-900/50'}">{validationResult.message}</div>
					{/if}
				</div>
				<div class="flex gap-3">
					<button type="button" onclick={() => showEditModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 rounded-2xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
					<button type="submit" disabled={updating} class="flex-1 px-4 py-3 bg-pink-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">{updating ? 'Saving...' : 'Save'}</button>
				</div>
			</form>
		</div>
	</div>
</div>
{/if}

<style>
	::-webkit-scrollbar { width: 5px; height: 5px; }
	::-webkit-scrollbar-track { background: transparent; }
	::-webkit-scrollbar-thumb { background: rgba(148,163,184,0.15); border-radius: 10px; }
	::-webkit-scrollbar-thumb:hover { background: rgba(148,163,184,0.3); }
</style>
