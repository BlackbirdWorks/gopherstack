<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { multiRegionList } from '$lib/multi-region';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
	import { getCodePipelineClient } from '$lib/aws-client';
	import {
		ListPipelinesCommand,
		GetPipelineCommand,
		GetPipelineStateCommand,
		DeletePipelineCommand,
		CreatePipelineCommand,
		StartPipelineExecutionCommand,
		ListPipelineExecutionsCommand,
		ListWebhooksCommand,
		PutApprovalResultCommand,
		ListActionExecutionsCommand,
		type PipelineDeclaration,
		type GetPipelineStateCommandOutput,
		type StageState,
		type PipelineExecutionSummary,
		type ListWebhookItem,
		type ActionExecutionDetail
	} from '@aws-sdk/client-codepipeline';
	import { toast } from 'svelte-sonner';
	import { 
		Workflow, Search, RefreshCw, Plus, Trash2, 
		Activity, Info, Box, Clock, ShieldCheck,
		ChevronRight, ChevronDown, ListFilter, Globe,
		Layers, Share2, Play, CheckCircle2, 
		XCircle, AlertCircle, Timer, Zap,
		ArrowRight, ExternalLink, Shield,
		GitBranch, GitCommit, Settings,
		ArrowDown, Layout, Code2, Server,
		Database, Terminal
	} from 'lucide-svelte';

	const codepipeline = regionalClient(getCodePipelineClient);

	// Every row carries the region its List/Get call was made against.
	// Detail/action calls must build a client for THAT region -- in All mode
	// the same resource name can legitimately exist in two different regions.
	type Regioned<T> = T & { region: string };

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let pipelines = $state<Regioned<PipelineDeclaration>[]>([]);
	let selectedPipeline = $state<Regioned<PipelineDeclaration> | null>(null);
	let pipelineState = $state<GetPipelineStateCommandOutput | null>(null);
	let loadingDetails = $state(false);

	// Execution/webhook state
	let executions = $state<PipelineExecutionSummary[]>([]);
	let webhooks = $state<ListWebhookItem[]>([]);
	let activeTab = $state<'stages' | 'executions' | 'webhooks'>('stages');
	let startingExecution = $state(false);

	// Modal State
	let showCreateModal = $state(false);
	let newPipelineName = $state('');
	let roleArn = $state('arn:aws:iam::000000000000:role/service-role/codepipeline-role');
	let creating = $state(false);

	// Derived
	const filteredPipelines = $derived(
		pipelines.filter(p => p.name?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function listPipelinesInRegion(region: string): Promise<PipelineDeclaration[]> {
		const client = getCodePipelineClient(region);
		const listRes = await client.send(new ListPipelinesCommand({}));
		const pNames = listRes.pipelines?.map(p => p.name).filter(Boolean) as string[];
		if (pNames.length === 0) return [];
		const details = await Promise.all(
			pNames.map(name => client.send(new GetPipelineCommand({ name })))
		);
		return details.map(d => d.pipeline).filter(Boolean) as PipelineDeclaration[];
	}

	async function loadPipelines() {
		loading = true;
		try {
			const result = await multiRegionList(listPipelinesInRegion, (items) => items);
			pipelines = result.items.map(({ region, item }) => ({ ...item, region }));
			if (result.errors.length > 0) toast.error(`Failed to load pipelines from ${result.errors.length} region(s)`);
		} catch (err: unknown) {
			toast.error(`Failed to load pipelines: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectPipeline(pipeline: Regioned<PipelineDeclaration>) {
		selectedPipeline = pipeline;
		pipelineState = null;
		executions = [];
		webhooks = [];
		activeTab = 'stages';
		loadingDetails = true;
		try {
			const client = getCodePipelineClient(pipeline.region);
			const [stateRes, execRes, webhookRes] = await Promise.all([
				client.send(new GetPipelineStateCommand({ name: pipeline.name })),
				client.send(new ListPipelineExecutionsCommand({ pipelineName: pipeline.name })).catch(() => ({ pipelineExecutionSummaries: [] })),
				client.send(new ListWebhooksCommand({})).catch(() => ({ webhooks: [] }))
			]);
			pipelineState = stateRes;
			executions = execRes.pipelineExecutionSummaries ?? [];
			webhooks = (webhookRes.webhooks ?? []).filter(w => w.definition?.targetPipeline === pipeline.name);
		} catch (err: unknown) {
			toast.error(`Failed to load state: ${(err as Error).message}`);
		} finally {
			loadingDetails = false;
		}
	}

	// Per-execution action timeline (with durations) via ListActionExecutions.
	let expandedExecId = $state<string | null>(null);
	let actionExecutions = $state<ActionExecutionDetail[]>([]);
	let loadingActions = $state(false);

	function fmtDuration(start?: Date, end?: Date): string {
		if (!start) return '—';
		const endMs = (end ?? new Date()).getTime();
		const ms = endMs - start.getTime();
		if (ms < 0) return '—';
		if (ms < 1000) return `${ms}ms`;
		const s = Math.round(ms / 1000);
		if (s < 60) return `${s}s`;
		const m = Math.floor(s / 60);
		return `${m}m ${s % 60}s`;
	}

	async function toggleExecutionTimeline(execId: string | undefined) {
		if (!execId || !selectedPipeline?.name) return;
		if (expandedExecId === execId) {
			expandedExecId = null;
			return;
		}
		expandedExecId = execId;
		actionExecutions = [];
		loadingActions = true;
		try {
			const res = await getCodePipelineClient(selectedPipeline.region).send(
				new ListActionExecutionsCommand({
					pipelineName: selectedPipeline.name,
					filter: { pipelineExecutionId: execId }
				})
			);
			actionExecutions = (res.actionExecutionDetails ?? []).toSorted((a, b) => {
				const ta = a.startTime?.getTime() ?? 0;
				const tb = b.startTime?.getTime() ?? 0;
				return ta - tb;
			});
		} catch (err: unknown) {
			toast.error(`Failed to load action timeline: ${(err as Error).message}`);
		} finally {
			loadingActions = false;
		}
	}

	async function startExecution() {
		if (!selectedPipeline?.name) return;
		startingExecution = true;
		try {
			const res = await getCodePipelineClient(selectedPipeline.region).send(new StartPipelineExecutionCommand({ name: selectedPipeline.name }));
			toast.success(`Execution started: ${res.pipelineExecutionId}`);
			await selectPipeline(selectedPipeline);
		} catch (err: unknown) {
			toast.error(`Failed to start: ${(err as Error).message}`);
		} finally {
			startingExecution = false;
		}
	}

	async function approveAction(stageName: string, actionName: string, status: 'Approved' | 'Rejected') {
		if (!selectedPipeline?.name) return;
		try {
			await getCodePipelineClient(selectedPipeline.region).send(new PutApprovalResultCommand({
				pipelineName: selectedPipeline.name,
				stageName,
				actionName,
				result: { summary: `${status} via UI`, status },
				token: 'stub-token'
			}));
			toast.success(`Action ${status.toLowerCase()}`);
		} catch (err: unknown) {
			toast.error(`Approval failed: ${(err as Error).message}`);
		}
	}

	async function createPipeline() {
		if (!newPipelineName.trim()) return;
		creating = true;
		try {
			await codepipeline().send(new CreatePipelineCommand({
				pipeline: {
					name: newPipelineName.trim(),
					roleArn,
					artifactStore: {
						type: 'S3',
						location: `codepipeline-assets-${newPipelineName.trim()}`
					},
					stages: [
						{ name: 'Source', actions: [] },
						{ name: 'Build', actions: [] },
						{ name: 'Deploy', actions: [] }
					]
				}
			}));
			toast.success(`Pipeline "${newPipelineName}" created`);
			showCreateModal = false;
			newPipelineName = '';
			await loadPipelines();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deletePipeline(pipeline: Regioned<PipelineDeclaration> | null) {
		const name = pipeline?.name;
		if (!name || !await confirmDestructive({ title: 'Delete Pipeline', message: `Delete pipeline "${name}"? This cannot be undone.` })) return;
		try {
			await getCodePipelineClient(pipeline.region).send(new DeletePipelineCommand({ name }));
			toast.success(`Pipeline deleted`);
			if (selectedPipeline && selectedPipeline.name === name && selectedPipeline.region === pipeline.region) selectedPipeline = null;
			await loadPipelines();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function getStatusIcon(status: string | undefined) {
		if (status === 'Succeeded') return CheckCircle2;
		if (status === 'Failed') return XCircle;
		if (status === 'InProgress') return RefreshCw;
		return AlertCircle;
	}

	function getStatusColor(status: string | undefined): string {
		if (status === 'Succeeded') return 'text-emerald-500';
		if (status === 'Failed') return 'text-rose-500';
		if (status === 'InProgress') return 'text-amber-500 animate-spin';
		return 'text-slate-400';
	}

	onRegionChange(loadPipelines);
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-violet-600/20 rounded-xl shadow-inner">
				<Workflow class="w-8 h-8 text-violet-600" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-violet-600 to-fuchsia-600 dark:from-violet-400 dark:to-fuchsia-400 bg-clip-text text-transparent italic tracking-tight">CodePipeline Engine</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Continuous delivery orchestration across multi-stage release environments.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<WriteRegionHint />
			<button
				onclick={loadPipelines}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Refresh data"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button 
				onclick={() => showCreateModal = true}
				class="flex items-center gap-2 px-5 py-2.5 bg-violet-600 hover:bg-violet-700 text-white rounded-xl font-black shadow-lg shadow-violet-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				Deploy Pipeline
			</button>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Pipeline List -->
		<div class="lg:col-span-3 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Search pipelines..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-violet-500 outline-none transition-all italic font-bold"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#if loading && !pipelines.length}
						{#each Array(3) as _}
							<div class="p-4 animate-pulse"><div class="h-10 bg-slate-200/50 dark:bg-slate-700/30 rounded-lg"></div></div>
						{/each}
					{:else}
						{#each filteredPipelines as pipeline}
							<div 
								role="button"
								tabindex="0"
								onclick={() => selectPipeline(pipeline)}
								onkeydown={(e) => e.key === 'Enter' && selectPipeline(pipeline)}
								class="p-4 flex items-center justify-between hover:bg-violet-500/5 dark:hover:bg-violet-500/10 cursor-pointer transition-all {selectedPipeline && selectedPipeline.name === pipeline.name && selectedPipeline.region === pipeline.region ? 'bg-violet-500/10 border-l-4 border-violet-500 shadow-inner' : 'border-l-4 border-transparent'}"
							>
								<div class="flex items-center gap-3">
									<Layers class="w-4 h-4 text-violet-600" />
									<div>
										<div class="flex items-center gap-2">
											<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-xs truncate max-w-[150px]">{pipeline.name}</div>
											<RegionChip region={pipeline.region} />
										</div>
										<div class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic">{pipeline.stages?.length || 0} Stages Provisioned</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300" />
							</div>
						{/each}

						{#if !pipelines.length}
							<div class="p-12 text-center text-slate-400 text-sm italic font-bold">No pipelines provisioned.</div>
						{/if}
					{/if}
				</div>
			</div>
		</div>

		<!-- Detail View -->
		<div class="lg:col-span-9 space-y-6">
			{#if selectedPipeline}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in slide-in-from-right-4 duration-300">
					<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-violet-500/5 to-fuchsia-500/5 flex justify-between items-start">
						<div>
							<h2 class="text-3xl font-black text-slate-900 dark:text-white mb-2 uppercase tracking-tighter italic leading-none">{selectedPipeline.name}</h2>
							<div class="flex items-center gap-3 mt-4">
								<RegionChip region={selectedPipeline.region} />
								<div class="px-2 py-0.5 rounded-lg bg-violet-500/10 text-violet-600 text-[9px] font-black uppercase tracking-widest border border-violet-500/20">
									V1_TOPOGRAPHY
								</div>
								<div class="px-2 py-0.5 rounded-lg bg-emerald-500/10 text-emerald-600 text-[9px] font-black uppercase tracking-widest border border-emerald-500/20">
									ACTIVE
								</div>
							</div>
						</div>
						<button
							onclick={() => deletePipeline(selectedPipeline)}
							class="p-2.5 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-2xl transition-all border border-rose-500/20 shadow-xl"
							title="Discard Pipeline"
						>
							<Trash2 class="w-4 h-4" />
						</button>
					</div>

					<!-- Toolbar: start execution + tabs -->
					<div class="px-8 py-3 border-b border-slate-100 dark:border-slate-700/50 flex items-center justify-between gap-4">
						<div class="flex gap-2">
							<button onclick={() => (activeTab = 'stages')} class="px-3 py-1.5 text-xs font-bold rounded-lg transition-colors {activeTab === 'stages' ? 'bg-violet-600 text-white' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300'}">Stage Flow</button>
							<button onclick={() => (activeTab = 'executions')} class="px-3 py-1.5 text-xs font-bold rounded-lg transition-colors {activeTab === 'executions' ? 'bg-violet-600 text-white' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300'}">Executions ({executions.length})</button>
							<button onclick={() => (activeTab = 'webhooks')} class="px-3 py-1.5 text-xs font-bold rounded-lg transition-colors {activeTab === 'webhooks' ? 'bg-violet-600 text-white' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300'}">Webhooks ({webhooks.length})</button>
						</div>
						<button
							onclick={startExecution}
							disabled={startingExecution}
							class="flex items-center gap-1.5 px-3 py-1.5 text-xs font-bold rounded-lg bg-emerald-600 hover:bg-emerald-700 text-white transition-colors disabled:opacity-60"
						>
							<Play class="w-3 h-3" />
							{startingExecution ? 'Starting...' : 'Start Execution'}
						</button>
					</div>

					<div class="p-8">
						{#if activeTab === 'executions'}
							<div class="space-y-2">
								{#if executions.length === 0}
									<p class="text-center py-8 text-sm text-slate-500 dark:text-slate-400 italic">No executions found</p>
								{:else}
									{#each executions as exec}
										<div class="rounded-xl bg-slate-50 dark:bg-slate-700/50">
											<button
												type="button"
												onclick={() => toggleExecutionTimeline(exec.pipelineExecutionId)}
												class="flex w-full items-center justify-between p-3 text-left"
											>
												<span class="flex items-center gap-2 font-mono text-xs text-slate-600 dark:text-slate-300">
													{#if expandedExecId === exec.pipelineExecutionId}
														<ChevronDown class="w-3.5 h-3.5" />
													{:else}
														<ChevronRight class="w-3.5 h-3.5" />
													{/if}
													{exec.pipelineExecutionId}
												</span>
												<span class="text-xs px-2 py-0.5 rounded-full {exec.status === 'Succeeded' ? 'bg-emerald-100 text-emerald-700' : exec.status === 'Failed' ? 'bg-red-100 text-red-700' : 'bg-amber-100 text-amber-700'}">{exec.status}</span>
											</button>
											{#if expandedExecId === exec.pipelineExecutionId}
												<div class="border-t border-slate-200 dark:border-slate-600 p-3">
													{#if loadingActions}
														<p class="text-center py-3 text-xs text-slate-500 italic">Loading timeline…</p>
													{:else if actionExecutions.length === 0}
														<p class="text-center py-3 text-xs text-slate-500 italic">No action executions recorded</p>
													{:else}
														<div class="space-y-2">
															{#each actionExecutions as ae}
																<div class="flex items-center justify-between gap-3 rounded-lg bg-white dark:bg-slate-800 px-3 py-2">
																	<div class="min-w-0">
																		<div class="text-xs font-bold text-slate-800 dark:text-slate-200 truncate">{ae.stageName} / {ae.actionName}</div>
																		<div class="text-[10px] text-slate-400 font-mono">
																			{ae.startTime ? ae.startTime.toLocaleTimeString() : '—'}
																			{#if ae.input?.actionTypeId?.provider}· {ae.input.actionTypeId.provider}{/if}
																		</div>
																	</div>
																	<div class="flex items-center gap-2 shrink-0">
																		<span class="flex items-center gap-1 text-[10px] text-slate-500 font-mono">
																			<Timer class="w-3 h-3" /> {fmtDuration(ae.startTime, ae.lastUpdateTime)}
																		</span>
																		<span class="text-[10px] px-2 py-0.5 rounded-full {ae.status === 'Succeeded' ? 'bg-emerald-100 text-emerald-700' : ae.status === 'Failed' ? 'bg-red-100 text-red-700' : 'bg-amber-100 text-amber-700'}">{ae.status}</span>
																	</div>
																</div>
															{/each}
														</div>
													{/if}
												</div>
											{/if}
										</div>
									{/each}
								{/if}
							</div>
						{:else if activeTab === 'webhooks'}
							<div class="space-y-2">
								{#if webhooks.length === 0}
									<p class="text-center py-8 text-sm text-slate-500 dark:text-slate-400 italic">No webhooks configured for this pipeline</p>
								{:else}
									{#each webhooks as wh}
										<div class="p-3 rounded-xl bg-slate-50 dark:bg-slate-700/50">
											<div class="flex items-center justify-between">
												<span class="font-mono text-xs text-slate-900 dark:text-white">{wh.definition?.name}</span>
												<span class="text-xs text-slate-500 dark:text-slate-400">{wh.definition?.targetAction}</span>
											</div>
										</div>
									{/each}
								{/if}
							</div>
						{:else}
						<!-- Stage Flow -->
						<div class="relative">
							<div class="absolute inset-y-0 left-[11px] w-0.5 bg-slate-100 dark:bg-slate-700/50 z-0"></div>
							
							<div class="space-y-12 relative z-10">
								{#each selectedPipeline.stages || [] as stage, i}
									{@const stageState = pipelineState?.stageStates?.find((s) => s.stageName === stage.name)}
									<div class="space-y-4">
										<!-- Stage Header -->
										<div class="flex items-center gap-4">
											<div class="w-6 h-6 rounded-full {stageState?.latestExecution?.status === 'Succeeded' ? 'bg-emerald-500' : 'bg-slate-200 dark:bg-slate-700'} border-4 border-white dark:border-slate-800 flex items-center justify-center">
												{#if stageState?.latestExecution?.status === 'Succeeded'}
													<CheckCircle2 class="w-3 h-3 text-white" />
												{:else}
													<div class="w-1.5 h-1.5 rounded-full bg-slate-400"></div>
												{/if}
											</div>
											<div class="flex items-baseline gap-2">
												<h3 class="text-xs font-black text-slate-900 dark:text-white uppercase tracking-widest italic">{stage.name}</h3>
												<span class="text-[8px] text-slate-400 font-bold uppercase tracking-widest">Stage {i + 1}</span>
											</div>
										</div>

										<!-- Actions Grid -->
										<div class="ml-10 grid grid-cols-1 md:grid-cols-2 gap-4">
											{#each stage.actions || [] as action}
												{@const actionState = stageState?.actionStates?.find((as) => as.actionName === action.name)}
												{@const StatusIcon = getStatusIcon(actionState?.latestExecution?.status)}
												<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/action hover:border-violet-500/30 transition-all">
													<div class="flex justify-between items-start mb-4">
														<div class="flex items-center gap-3">
															<div class="p-2 bg-slate-900 dark:bg-black rounded-xl">
																<Settings class="w-3.5 h-3.5 text-violet-500" />
															</div>
															<div>
																<div class="text-[10px] font-black text-slate-900 dark:text-white uppercase italic tracking-tighter">{action.name}</div>
																<div class="text-[8px] text-slate-500 uppercase font-black">{action.actionTypeId?.provider}</div>
															</div>
														</div>
														<StatusIcon class="w-4 h-4 {getStatusColor(actionState?.latestExecution?.status)}" />
													</div>
													
													<div class="flex items-center justify-between pt-4 border-t border-slate-100 dark:border-slate-700/50 text-[8px] font-black uppercase text-slate-400 italic">
														<span>Latest Execution</span>
														<span class="text-slate-600 dark:text-slate-300 font-mono tracking-tighter">{actionState?.latestExecution?.summary || 'Never triggered'}</span>
													</div>
												</div>
											{/each}

											{#if !stage.actions?.length}
												<div class="p-8 text-center bg-slate-50/50 dark:bg-slate-800/10 border-2 border-dashed border-slate-100 dark:border-slate-700/50 rounded-[2.5rem] md:col-span-2">
													<p class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">No operational actions defined for this stage.</p>
												</div>
											{/if}
										</div>

										<!-- Transition Arrow -->
										{#if i < (selectedPipeline.stages?.length || 0) - 1}
											<div class="ml-[6px] py-4">
												<ArrowDown class="w-3 h-3 text-slate-200 dark:text-slate-700" />
											</div>
										{/if}
									</div>
								{/each}
							</div>
						</div>
						{/if}
					</div>

					<!-- Monitoring Panel -->
					<div class="p-8 border-t border-slate-100 dark:border-slate-700/50 bg-slate-50/50 dark:bg-slate-900/40 grid grid-cols-1 md:grid-cols-3 gap-6">
						<div class="flex items-center gap-4 group/mon">
							<div class="p-3 bg-violet-500/10 rounded-2xl group-hover/mon:scale-110 transition-transform">
								<Activity class="w-5 h-5 text-violet-500" />
							</div>
							<div>
								<div class="text-[8px] font-black text-slate-500 uppercase tracking-widest mb-0.5">Throughput</div>
								<div class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">OPTIONAL</div>
							</div>
						</div>
						<div class="flex items-center gap-4 group/mon">
							<div class="p-3 bg-fuchsia-500/10 rounded-2xl group-hover/mon:scale-110 transition-transform">
								<Clock class="w-5 h-5 text-fuchsia-500" />
							</div>
							<div>
								<div class="text-[8px] font-black text-slate-500 uppercase tracking-widest mb-0.5">Mean Time</div>
								<div class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">240s</div>
							</div>
						</div>
						<div class="flex items-center gap-4 group/mon">
							<div class="p-3 bg-emerald-500/10 rounded-2xl group-hover/mon:scale-110 transition-transform">
								<Shield class="w-5 h-5 text-emerald-500" />
							</div>
							<div>
								<div class="text-[8px] font-black text-slate-500 uppercase tracking-widest mb-0.5">Artifact Store</div>
								<div class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-tighter italic truncate max-w-[150px]">{selectedPipeline.artifactStore?.location}</div>
							</div>
						</div>
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-[3rem] p-32 text-center flex flex-col items-center gap-6">
					<div class="p-8 bg-slate-50 dark:bg-slate-800 rounded-[2.5rem]">
						<Workflow class="w-16 h-16 text-slate-200 dark:text-slate-700" />
					</div>
					<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">Release Topography Engine</h3>
					<p class="text-slate-500 dark:text-slate-400 text-sm max-w-sm italic tracking-tight font-medium lowercase">Provision intricate release pipelines, coordinate cross-stage transitions, and monitor multi-action execution statuses with high-resolution engine observability.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div role="none" onclick={() => showCreateModal = false} onkeydown={(e) => e.key === 'Escape' && (showCreateModal = false)} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2.5rem] shadow-2xl border border-violet-500/20 overflow-hidden animate-in zoom-in-95">
			<div class="p-8">
				<h3 class="text-2xl font-black text-slate-900 dark:text-white mb-6 uppercase tracking-tighter italic leading-none">Assemble Pipeline</h3>
				
				<form onsubmit={(e) => { e.preventDefault(); createPipeline(); }} class="space-y-6">
					<div>
						<label for="pName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Pipeline Identifier</label>
						<input 
							id="pName"
							type="text" 
							bind:value={newPipelineName}
							placeholder="e.g. core-services-delivery-prod"
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-violet-500 transition-all font-mono text-xs italic"
							required
						/>
					</div>

					<div class="flex gap-4 pt-4">
						<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-4 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all">Abort</button>
						<button type="submit" disabled={creating} class="flex-1 px-4 py-4 bg-violet-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg shadow-violet-600/20 active:scale-95 disabled:opacity-50 transition-all">
							{creating ? 'Assembling...' : 'Deploy Pipeline'}
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
