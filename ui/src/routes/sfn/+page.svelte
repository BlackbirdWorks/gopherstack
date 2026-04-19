<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getSFNClient } from '$lib/aws-client';
	import {
		ListStateMachinesCommand,
		DescribeStateMachineCommand,
		ListExecutionsCommand,
		DescribeExecutionCommand,
		StartExecutionCommand,
		DeleteStateMachineCommand,
		type StateMachineListItem,
		type DescribeStateMachineCommandOutput,
		type ExecutionListItem,
		type DescribeExecutionOutput
	} from '@aws-sdk/client-sfn';
	import { toast } from 'svelte-sonner';
	import { 
		Zap, Search, RefreshCw, Plus, Trash2, 
		Activity, Info, Box, Clock, Shield, 
		ChevronRight, ListFilter, Play, Database,
		Terminal, Code, X, ArrowRight, CheckCircle2,
		AlertCircle, Timer
	} from 'lucide-svelte';

	const sfn = getSFNClient();

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let stateMachines = $state<StateMachineListItem[]>([]);
	let selectedSM = $state<DescribeStateMachineCommandOutput | null>(null);
	let executions = $state<ExecutionListItem[]>([]);
	let selectedExecution = $state<DescribeExecutionOutput | null>(null);
	let loadingExecutions = $state(false);
	let loadingDetail = $state(false);

	// Modal State
	let showStartModal = $state(false);
	let executionInput = $state('{}');
	let starting = $state(false);

	// Derived
	const filteredSMs = $derived(
		stateMachines.filter(sm => sm.name?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function loadStateMachines() {
		loading = true;
		try {
			const res = await sfn.send(new ListStateMachinesCommand({}));
			stateMachines = res.stateMachines ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load state machines: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectSM(arn: string) {
		loadingDetail = true;
		selectedExecution = null;
		try {
			const detailRes = await sfn.send(new DescribeStateMachineCommand({ stateMachineArn: arn }));
			selectedSM = detailRes;

			loadingExecutions = true;
			const execRes = await sfn.send(new ListExecutionsCommand({ stateMachineArn: arn, maxResults: 20 }));
			executions = execRes.executions ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load details: ${(err as Error).message}`);
		} finally {
			loadingDetail = false;
			loadingExecutions = false;
		}
	}

	async function selectExecution(arn: string) {
		try {
			const res = await sfn.send(new DescribeExecutionCommand({ executionArn: arn }));
			selectedExecution = res;
		} catch (err: unknown) {
			toast.error(`Failed to load execution detail: ${(err as Error).message}`);
		}
	}

	async function startExecution() {
		if (!selectedSM) return;
		starting = true;
		try {
			const res = await sfn.send(new StartExecutionCommand({
				stateMachineArn: selectedSM.stateMachineArn,
				input: executionInput
			}));
			toast.success(`Execution started: ${res.executionArn?.split(':').pop()}`);
			showStartModal = false;
			await selectSM(selectedSM.stateMachineArn!);
		} catch (err: unknown) {
			toast.error(`Start failed: ${(err as Error).message}`);
		} finally {
			starting = false;
		}
	}

	async function deleteSM(arn: string | undefined) {
		if (!arn || !await confirmDestructive(`Delete state machine?`)) return;
		try {
			await sfn.send(new DeleteStateMachineCommand({ stateMachineArn: arn }));
			toast.success(`Delete initiated`);
			if (selectedSM?.stateMachineArn === arn) selectedSM = null;
			await loadStateMachines();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function getStatusColor(status: string | undefined): string {
		if (!status) return 'bg-slate-500';
		const s = status.toUpperCase();
		if (s === 'SUCCEEDED' || s === 'RUNNING') return 'bg-emerald-500';
		if (s === 'FAILED' || s === 'ABORTED' || s === 'TIMED_OUT') return 'bg-rose-500';
		return 'bg-slate-500';
	}

	onMount(() => {
		loadStateMachines();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-pink-500/20 rounded-xl">
				<Zap class="w-8 h-8 text-pink-500" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-pink-600 to-purple-600 dark:from-pink-400 dark:to-purple-400 bg-clip-text text-transparent italic">Step Functions</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Orchestrate serverless workflows with visual state machines.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={loadStateMachines}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95"
				title="Refresh data"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- SM List -->
		<div class="lg:col-span-3 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Search workflows..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-pink-500 outline-none transition-all"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#if loading && !stateMachines.length}
						{#each Array(3) as _}
							<div class="p-4 animate-pulse"><div class="h-10 bg-slate-200/50 dark:bg-slate-700/30 rounded-lg"></div></div>
						{/each}
					{:else}
						{#each filteredSMs as sm}
							<div 
								role="button"
								tabindex="0"
								onclick={() => sm.stateMachineArn && selectSM(sm.stateMachineArn)}
								onkeydown={(e) => e.key === 'Enter' && sm.stateMachineArn && selectSM(sm.stateMachineArn)}
								class="p-4 group/sm hover:bg-pink-500/5 dark:hover:bg-pink-500/10 cursor-pointer transition-all {selectedSM?.stateMachineArn === sm.stateMachineArn ? 'bg-pink-500/10 border-l-4 border-pink-500' : 'border-l-4 border-transparent'}"
							>
								<div class="flex items-center justify-between mb-1">
									<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-xs truncate max-w-[150px]">{sm.name}</div>
									<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest">{sm.type}</span>
								</div>
								<div class="text-[8px] text-slate-400 font-mono truncate">{sm.stateMachineArn}</div>
							</div>
						{/each}

						{#if !stateMachines.length}
							<div class="p-12 text-center text-slate-400 text-sm italic font-bold">No state machines found.</div>
						{/if}
					{/if}
				</div>
			</div>
		</div>

		<!-- Detail View -->
		<div class="lg:col-span-9 space-y-6">
			{#if selectedSM}
				<div class="grid grid-cols-1 lg:grid-cols-2 gap-6 items-start">
					<!-- Definition & Executions -->
					<div class="space-y-6">
						<!-- Control Header -->
						<div class="p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
							<div class="flex justify-between items-start mb-6">
								<div>
									<h2 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">{selectedSM.name}</h2>
									<div class="flex items-center gap-2 mt-1">
										<span class="px-2 py-0.5 rounded bg-pink-500/10 text-pink-600 text-[10px] font-black uppercase">{selectedSM.status}</span>
										<span class="text-xs text-slate-400">•</span>
										<span class="text-[10px] text-slate-400 font-mono italic">{selectedSM.roleArn?.split('/').pop()}</span>
									</div>
								</div>
								<div class="flex gap-2">
									<button 
										onclick={() => showStartModal = true}
										class="px-4 py-2 bg-pink-600 hover:bg-pink-700 text-white rounded-xl font-black uppercase text-[10px] tracking-widest shadow-lg shadow-pink-600/20 active:scale-95 transition-all flex items-center gap-2"
									>
										<Play class="w-4 h-4 fill-current" />
										Execute
									</button>
									<button 
										onclick={() => deleteSM(selectedSM?.stateMachineArn)}
										class="p-2.5 bg-slate-100 dark:bg-slate-700 text-slate-400 hover:text-red-500 rounded-xl transition-all"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>

							<!-- ASL Explorer -->
							<div class="space-y-3">
								<h3 class="text-[10px] font-black text-slate-500 uppercase tracking-widest flex items-center gap-2">
									<Code class="w-4 h-4" />
									ASL Definition
								</h3>
								<div class="bg-slate-900 dark:bg-black rounded-2xl border border-slate-800 p-4 max-h-[300px] overflow-y-auto">
									<pre class="text-[10px] font-mono text-pink-400 leading-relaxed selection:bg-pink-500/20 italic">{selectedSM.definition}</pre>
								</div>
							</div>
						</div>

						<!-- Executions List -->
						<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
							<div class="p-4 bg-slate-50/50 dark:bg-slate-900/40 border-b border-slate-100 dark:border-slate-700/50 flex justify-between items-center">
								<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic">
									<Timer class="w-4 h-4" />
									Recent Executions
								</h3>
							</div>
							<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[400px] overflow-y-auto">
								{#if loadingExecutions}
									<div class="p-12 flex justify-center"><RefreshCw class="w-6 h-6 text-pink-500 animate-spin" /></div>
								{:else}
									{#each executions as exec}
										<div 
											role="button"
											tabindex="0"
											onclick={() => selectExecution(exec.executionArn!)}
											onkeydown={(e) => e.key === 'Enter' && selectExecution(exec.executionArn!)}
											class="p-4 hover:bg-pink-500/5 transition-all cursor-pointer flex items-center justify-between {selectedExecution?.executionArn === exec.executionArn ? 'bg-pink-500/10' : ''}"
										>
											<div class="flex items-center gap-3">
												<div class="w-2 h-2 rounded-full {getStatusColor(exec.status)} animate-pulse"></div>
												<div>
													<div class="text-[11px] font-black text-slate-900 dark:text-white uppercase tracking-tight">{exec.name}</div>
													<div class="text-[9px] text-slate-400 font-mono">{exec.startDate?.toLocaleString()}</div>
												</div>
											</div>
											<ChevronRight class="w-4 h-4 text-slate-300" />
										</div>
									{:else}
										<div class="p-12 text-center text-slate-400 text-xs italic">No execution history.</div>
									{/each}
								{/if}
							</div>
						</div>
					</div>

					<!-- Execution Monitor -->
					<div class="bg-slate-900 dark:bg-black rounded-3xl shadow-2xl border border-slate-800 overflow-hidden flex flex-col h-[750px] animate-in slide-in-from-right-8 duration-500">
						{#if selectedExecution}
							<div class="p-6 border-b border-slate-800 flex justify-between items-center bg-slate-800/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-emerald-500" />
									<div>
										<h4 class="text-xs font-black text-white uppercase tracking-widest italic">{selectedExecution.name}</h4>
										<span class="text-[9px] font-bold text-emerald-500 uppercase">{selectedExecution.status}</span>
									</div>
								</div>
								<button onclick={() => selectedExecution = null} class="text-slate-500 hover:text-white"><X class="w-4 h-4" /></button>
							</div>

							<div class="flex-1 overflow-y-auto p-6 space-y-8">
								<!-- Input -->
								<div class="space-y-3">
									<h5 class="text-[9px] font-black text-slate-500 uppercase tracking-widest flex items-center gap-2">
										<ArrowRight class="w-3.5 h-3.5" />
										Execution Input
									</h5>
									<div class="bg-black/50 p-4 rounded-2xl border border-slate-800 shadow-inner">
										<pre class="text-[10px] font-mono text-emerald-400 italic">{JSON.stringify(JSON.parse(selectedExecution.input || '{}'), null, 2)}</pre>
									</div>
								</div>

								<!-- Output -->
								{#if selectedExecution.output}
									<div class="space-y-3">
										<h5 class="text-[9px] font-black text-slate-500 uppercase tracking-widest flex items-center gap-2">
											<CheckCircle2 class="w-3.5 h-3.5" />
											Execution Output
										</h5>
										<div class="bg-black/50 p-4 rounded-2xl border border-slate-800 shadow-inner">
											<pre class="text-[10px] font-mono text-emerald-400 italic">{JSON.stringify(JSON.parse(selectedExecution.output || '{}'), null, 2)}</pre>
										</div>
									</div>
								{/if}

								<!-- Timeline (Simulated/Brief) -->
								<div class="space-y-3">
									<h5 class="text-[9px] font-black text-slate-500 uppercase tracking-widest flex items-center gap-2">
										<Timer class="w-3.5 h-3.5" />
										Metric History
									</h5>
									<div class="grid grid-cols-2 gap-4">
										<div class="p-4 bg-slate-800/30 rounded-2xl border border-slate-800">
											<div class="text-[8px] font-black text-slate-500 uppercase mb-1">Started</div>
											<div class="text-[10px] text-slate-300 font-mono">{selectedExecution.startDate?.toLocaleTimeString()}</div>
										</div>
										<div class="p-4 bg-slate-800/30 rounded-2xl border border-slate-800">
											<div class="text-[8px] font-black text-slate-500 uppercase mb-1">Duration</div>
											<div class="text-[10px] text-slate-300 font-mono">
												{selectedExecution.stopDate ? Math.round((selectedExecution.stopDate.getTime() - selectedExecution.startDate!.getTime()) / 1000) : '—'}s
											</div>
										</div>
									</div>
								</div>
							</div>
						{:else}
							<div class="flex-1 flex flex-col items-center justify-center opacity-30 p-12 text-center">
								<Terminal class="w-16 h-16 text-slate-600 mb-6" />
								<h4 class="text-xs font-black text-slate-500 uppercase tracking-[0.2em] mb-2">Execution Monitor Offline</h4>
								<p class="text-[10px] text-slate-600 italic max-w-xs">Select an execution from the list to view its trace, input payloads, and final results in real-time.</p>
							</div>
						{/if}
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-2xl p-24 text-center flex flex-col items-center gap-4">
					<div class="p-6 bg-slate-50 dark:bg-slate-800 rounded-3xl">
						<Zap class="w-12 h-12 text-slate-300 dark:text-slate-600" />
					</div>
					<h3 class="text-xl font-bold text-slate-900 dark:text-white uppercase tracking-tight italic">Workflow Orchestration</h3>
					<p class="text-slate-500 dark:text-slate-400 text-sm max-w-sm">Build distributed applications, automate business processes, and orchestrate serverless workflows with minimal code.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Start Modal -->
{#if showStartModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div role="none" onclick={() => showStartModal = false} onkeydown={(e) => e.key === 'Escape' && (showStartModal = false)} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"></div>
		<div class="relative w-full max-w-lg bg-white dark:bg-slate-800 rounded-3xl shadow-2xl border border-pink-500/20 overflow-hidden animate-in zoom-in-95">
			<div class="p-8">
				<h3 class="text-2xl font-black text-slate-900 dark:text-white mb-6 uppercase tracking-tight italic">New Workflow Execution</h3>
				
				<form onsubmit={(e) => { e.preventDefault(); startExecution(); }} class="space-y-6">
					<div>
						<label for="sfn-execution-input" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1">Input JSON</label>
						<textarea 
							id="sfn-execution-input"
							bind:value={executionInput}
							rows="8"
							class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-2xl outline-none focus:ring-2 focus:ring-pink-500 transition-all font-mono text-xs italic"
							placeholder="Execution input JSON"
						></textarea>
					</div>

					<div class="flex gap-4">
						<button type="button" onclick={() => showStartModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all">Cancel</button>
						<button type="submit" disabled={starting} class="flex-1 px-4 py-3 bg-pink-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg shadow-pink-600/20 active:scale-95 disabled:opacity-50 transition-all">
							{starting ? 'Initializing...' : 'Start Workflow'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<style>
	/* Custom scrollbar */
	::-webkit-scrollbar {
		width: 6px;
	}
	::-webkit-scrollbar-track {
		background: transparent;
	}
	::-webkit-scrollbar-thumb {
		background: rgba(148, 163, 184, 0.1);
		border-radius: 10px;
	}
	::-webkit-scrollbar-thumb:hover {
		background: rgba(148, 163, 184, 0.2);
	}
</style>
