<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getPipesClient } from '$lib/aws-client';
	import {
		ListPipesCommand,
		DescribePipeCommand,
		CreatePipeCommand,
		UpdatePipeCommand,
		DeletePipeCommand,
		StartPipeCommand,
		StopPipeCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Pipe,
		type DescribePipeResponse
	} from '@aws-sdk/client-pipes';
	import { toast } from 'svelte-sonner';
	import {
		GitBranch,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Play,
		Square,
		ChevronRight,
		ArrowRight,
		Activity,
		Clock,
		Edit,
		Tag,
		X,
		Filter,
		SortAsc,
		AlertTriangle,
		Database,
		Zap,
		Box
	} from 'lucide-svelte';

	const pipesClient = getPipesClient();

	// --- State ---
	let loading = $state(false);
	let pipeList = $state<Pipe[]>([]);
	let nextToken = $state<string>();
	let hasMore = $state(false);
	let searchQuery = $state('');
	let stateFilter = $state('');
	let sortBy = $state<'name' | 'created' | 'state'>('name');
	let selectedPipe = $state<DescribePipeResponse | null>(null);
	let loadingDetails = $state(false);
	let activeDetailTab = $state<'overview' | 'tags' | 'config'>('overview');

	// Action loading states
	let actionInFlight = $state<string | null>(null);

	// Create modal
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newPipeName = $state('');
	let newPipeSource = $state('arn:aws:sqs:us-east-1:000000000000:source-queue');
	let newPipeTarget = $state('arn:aws:lambda:us-east-1:000000000000:function:target-fn');
	let newPipeRoleArn = $state('arn:aws:iam::000000000000:role/pipes-role');
	let newPipeDescription = $state('');
	let newPipeDesiredState = $state<'RUNNING' | 'STOPPED'>('RUNNING');
	let newPipeBatchSize = $state(10);
	let newPipeEnrichment = $state('');
	let newPipeInputTemplate = $state('');

	// Edit modal
	let showEditModal = $state(false);
	let editing = $state(false);
	let editTarget = $state('');
	let editRoleArn = $state('');
	let editDescription = $state('');
	let editDesiredState = $state('');

	// Tag modal
	let showTagModal = $state(false);
	let newTagKey = $state('');
	let newTagValue = $state('');
	let taggingInFlight = $state(false);

	// Stats
	const runningCount = $derived(pipeList.filter((p) => p.CurrentState === 'RUNNING').length);
	const stoppedCount = $derived(pipeList.filter((p) => p.CurrentState === 'STOPPED').length);
	const failedCount = $derived(
		pipeList.filter((p) => p.CurrentState?.includes('FAILED')).length
	);

	// Filtered + sorted list
	const filteredPipes = $derived(
		[...pipeList]
			.filter((p) => {
				const q = searchQuery.toLowerCase();
				if (q && !p.Name?.toLowerCase().includes(q)) return false;
				if (stateFilter && p.CurrentState !== stateFilter) return false;
				return true;
			})
			.toSorted((a, b) => {
				if (sortBy === 'name') return (a.Name ?? '').localeCompare(b.Name ?? '');
				if (sortBy === 'state') return (a.CurrentState ?? '').localeCompare(b.CurrentState ?? '');
				if (sortBy === 'created')
					return (b.CreationTime?.getTime() ?? 0) - (a.CreationTime?.getTime() ?? 0);
				return 0;
			})
	);

	// --- Helpers ---

	function arnLabel(arn: string | undefined): string {
		if (!arn) return '—';
		const parts = arn.split(':');
		if (parts.length >= 6) return parts.slice(5).join(':');
		return arn;
	}

	function arnService(arn: string | undefined): string {
		if (!arn) return '';
		const parts = arn.split(':');
		return parts[2] ?? '';
	}

	function stateColor(state: string | undefined): string {
		if (state === 'RUNNING') return 'text-emerald-500';
		if (state === 'STOPPED') return 'text-slate-400';
		if (state === 'STARTING' || state === 'STOPPING') return 'text-amber-500';
		if (state?.includes('FAILED')) return 'text-rose-500';
		return 'text-slate-400';
	}

	function stateBadgeClass(state: string | undefined): string {
		if (state === 'RUNNING') return 'bg-emerald-500/10 text-emerald-600 border-emerald-500/20';
		if (state === 'STOPPED') return 'bg-slate-500/10 text-slate-500 border-slate-500/20';
		if (state === 'STARTING' || state === 'STOPPING')
			return 'bg-amber-500/10 text-amber-600 border-amber-500/20';
		if (state?.includes('FAILED')) return 'bg-rose-500/10 text-rose-600 border-rose-500/20';
		return 'bg-slate-500/10 text-slate-400 border-slate-400/20';
	}

	function serviceIcon(arn: string | undefined) {
		const svc = arnService(arn);
		if (svc === 'sqs') return Database;
		if (svc === 'lambda') return Zap;
		if (svc === 'states') return GitBranch;
		if (svc === 'kinesis') return Activity;
		return Box;
	}

	// --- API ---

	async function loadPipes(append = false) {
		if (!append) {
			loading = true;
			pipeList = [];
		}
		try {
			const params: Record<string, unknown> = { Limit: 50 };
			if (append && nextToken) params.NextToken = nextToken;
			const res = await pipesClient.send(new ListPipesCommand(params));
			if (append) {
				pipeList = [...pipeList, ...(res.Pipes ?? [])];
			} else {
				pipeList = res.Pipes ?? [];
			}
			nextToken = res.NextToken;
			hasMore = !!res.NextToken;
		} catch (err: unknown) {
			toast.error(`Failed to load pipes: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectPipe(name: string | undefined) {
		if (!name) return;
		loadingDetails = true;
		selectedPipe = null;
		activeDetailTab = 'overview';
		try {
			const res = await pipesClient.send(new DescribePipeCommand({ Name: name }));
			selectedPipe = res;
		} catch (err: unknown) {
			toast.error(`Failed to load pipe details: ${(err as Error).message}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function createPipe() {
		if (!newPipeName.trim() || !newPipeSource.trim() || !newPipeTarget.trim()) return;
		creating = true;
		try {
			const input: Record<string, unknown> = {
				Name: newPipeName.trim(),
				Source: newPipeSource.trim(),
				Target: newPipeTarget.trim(),
				RoleArn: newPipeRoleArn.trim() || undefined,
				Description: newPipeDescription.trim() || undefined,
				DesiredState: newPipeDesiredState,
				SourceParameters: {
					SqsQueueParameters: { BatchSize: newPipeBatchSize }
				}
			};
			if (newPipeEnrichment.trim()) input.Enrichment = newPipeEnrichment.trim();
			if (newPipeInputTemplate.trim()) {
				input.TargetParameters = { InputTemplate: newPipeInputTemplate.trim() };
			}
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			await pipesClient.send(new CreatePipeCommand(input as any));
			toast.success(`Pipe "${newPipeName}" created`);
			showCreateModal = false;
			newPipeName = '';
			newPipeDescription = '';
			newPipeEnrichment = '';
			newPipeInputTemplate = '';
			await loadPipes();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	function openEditModal() {
		if (!selectedPipe) return;
		editTarget = selectedPipe.Target ?? '';
		editRoleArn = selectedPipe.RoleArn ?? '';
		editDescription = selectedPipe.Description ?? '';
		editDesiredState = selectedPipe.DesiredState ?? '';
		showEditModal = true;
	}

	async function updatePipe() {
		if (!selectedPipe?.Name) return;
		editing = true;
		try {
			await pipesClient.send(
				new UpdatePipeCommand({
					Name: selectedPipe.Name,
					Target: editTarget || undefined,
					RoleArn: editRoleArn || undefined,
					Description: editDescription,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					DesiredState: editDesiredState as any
				})
			);
			toast.success('Pipe updated');
			showEditModal = false;
			await selectPipe(selectedPipe.Name);
			await loadPipes();
		} catch (err: unknown) {
			toast.error(`Update failed: ${(err as Error).message}`);
		} finally {
			editing = false;
		}
	}

	async function deletePipe(name: string | undefined) {
		if (
			!name ||
			!(await confirmDestructive({
				title: 'Delete Pipe',
				message: `Delete pipe "${name}"? This cannot be undone.`
			}))
		)
			return;
		actionInFlight = 'delete';
		try {
			await pipesClient.send(new DeletePipeCommand({ Name: name }));
			toast.success('Pipe deleted');
			if (selectedPipe?.Name === name) selectedPipe = null;
			await loadPipes();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		} finally {
			actionInFlight = null;
		}
	}

	async function startPipe(name: string | undefined) {
		if (!name) return;
		actionInFlight = 'start';
		try {
			await pipesClient.send(new StartPipeCommand({ Name: name }));
			toast.success(`Pipe "${name}" started`);
			if (selectedPipe?.Name === name) await selectPipe(name);
			await loadPipes();
		} catch (err: unknown) {
			toast.error(`Start failed: ${(err as Error).message}`);
		} finally {
			actionInFlight = null;
		}
	}

	async function stopPipe(name: string | undefined) {
		if (!name) return;
		actionInFlight = 'stop';
		try {
			await pipesClient.send(new StopPipeCommand({ Name: name }));
			toast.success(`Pipe "${name}" stopped`);
			if (selectedPipe?.Name === name) await selectPipe(name);
			await loadPipes();
		} catch (err: unknown) {
			toast.error(`Stop failed: ${(err as Error).message}`);
		} finally {
			actionInFlight = null;
		}
	}

	async function addTag() {
		if (!selectedPipe?.Arn || !newTagKey.trim()) return;
		taggingInFlight = true;
		try {
			await pipesClient.send(
				new TagResourceCommand({
					resourceArn: selectedPipe.Arn,
					tags: { [newTagKey.trim()]: newTagValue.trim() }
				})
			);
			toast.success('Tag added');
			newTagKey = '';
			newTagValue = '';
			await selectPipe(selectedPipe.Name);
		} catch (err: unknown) {
			toast.error(`Tag failed: ${(err as Error).message}`);
		} finally {
			taggingInFlight = false;
		}
	}

	async function removeTag(key: string) {
		if (!selectedPipe?.Arn) return;
		try {
			await pipesClient.send(
				new UntagResourceCommand({
					resourceArn: selectedPipe.Arn,
					tagKeys: [key]
				})
			);
			toast.success('Tag removed');
			await selectPipe(selectedPipe.Name);
		} catch (err: unknown) {
			toast.error(`Remove tag failed: ${(err as Error).message}`);
		}
	}

	function setTab(tab: string) {
		activeDetailTab = tab as 'overview' | 'tags' | 'config';
	}

	function closeModalOnBackdrop(e: MouseEvent) {
		if (e.target === e.currentTarget) {
			showCreateModal = false;
			showEditModal = false;
			showTagModal = false;
		}
	}

	onMount(() => {
		loadPipes();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div
		class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl"
	>
		<div class="flex items-center gap-4">
			<div class="p-3 bg-rose-600/20 rounded-xl shadow-inner">
				<GitBranch class="w-8 h-8 text-rose-600" />
			</div>
			<div>
				<h1
					class="text-3xl font-bold bg-gradient-to-r from-rose-600 to-orange-500 dark:from-rose-400 dark:to-orange-400 bg-clip-text text-transparent italic tracking-tight"
				>
					EventBridge Pipes
				</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">
					Point-to-point integrations between event sources and targets with optional filtering and
					enrichment.
				</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button
				onclick={() => loadPipes()}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Refresh"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 px-5 py-2.5 bg-rose-600 hover:bg-rose-700 text-white rounded-xl font-black shadow-lg shadow-rose-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				New Pipe
			</button>
		</div>
	</div>

	<!-- Stats bar -->
	<div class="grid grid-cols-3 gap-4">
		{#each [
			{ label: 'Running', count: runningCount, color: 'text-emerald-600', bg: 'bg-emerald-500/10 border-emerald-500/20', state: 'RUNNING' },
			{ label: 'Stopped', count: stoppedCount, color: 'text-slate-500', bg: 'bg-slate-500/10 border-slate-500/20', state: 'STOPPED' },
			{ label: 'Failed', count: failedCount, color: 'text-rose-600', bg: 'bg-rose-500/10 border-rose-500/20', state: '' }
		] as stat}
			<button
				onclick={() => { stateFilter = stateFilter === stat.state ? '' : stat.state; }}
				class="p-4 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border rounded-2xl shadow-sm hover:shadow-md transition-all {stateFilter === stat.state ? stat.bg + ' border-2' : 'border-white/20 dark:border-slate-700/50'}"
			>
				<div class="text-2xl font-black {stat.color}">{stat.count}</div>
				<div class="text-[9px] font-black text-slate-400 uppercase tracking-widest mt-1">{stat.label}</div>
			</button>
		{/each}
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Pipe List -->
		<div class="lg:col-span-4 space-y-3">
			<!-- Search + Filter + Sort -->
			<div
				class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden"
			>
				<div class="p-3 space-y-2 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input
							type="text"
							bind:value={searchQuery}
							placeholder="Search pipes..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all italic font-bold"
						/>
					</div>
					<div class="flex gap-2">
						<div class="relative flex-1">
							<Filter class="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400" />
							<select
								bind:value={stateFilter}
								class="w-full pl-8 pr-3 py-1.5 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-xs font-bold focus:ring-2 focus:ring-rose-500 outline-none transition-all"
							>
								<option value="">All States</option>
								<option value="RUNNING">Running</option>
								<option value="STOPPED">Stopped</option>
								<option value="STARTING">Starting</option>
								<option value="STOPPING">Stopping</option>
								<option value="CREATE_FAILED">Create Failed</option>
							</select>
						</div>
						<div class="relative flex-1">
							<SortAsc class="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-slate-400" />
							<select
								bind:value={sortBy}
								class="w-full pl-8 pr-3 py-1.5 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-xs font-bold focus:ring-2 focus:ring-rose-500 outline-none transition-all"
							>
								<option value="name">Name</option>
								<option value="state">State</option>
								<option value="created">Newest</option>
							</select>
						</div>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[580px] overflow-y-auto">
					{#if loading && !pipeList.length}
						{#each Array(3) as _}
							<div class="p-4 animate-pulse">
								<div class="h-10 bg-slate-200/50 dark:bg-slate-700/30 rounded-lg"></div>
							</div>
						{/each}
					{:else}
						{#each filteredPipes as pipe}
							<div
								role="button"
								tabindex="0"
								onclick={() => selectPipe(pipe.Name)}
								onkeydown={(e) => e.key === 'Enter' && selectPipe(pipe.Name)}
								class="p-4 flex items-center justify-between hover:bg-rose-500/5 dark:hover:bg-rose-500/10 cursor-pointer transition-all {selectedPipe?.Name === pipe.Name ? 'bg-rose-500/10 border-l-4 border-rose-500 shadow-inner' : 'border-l-4 border-transparent'}"
							>
								<div class="flex items-center gap-3 min-w-0">
									<Activity class="w-4 h-4 {stateColor(pipe.CurrentState)} flex-shrink-0" />
									<div class="min-w-0">
										<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-xs truncate">
											{pipe.Name}
										</div>
										<div class="flex items-center gap-1.5 mt-0.5">
											<span class="text-[8px] text-slate-400 font-mono uppercase opacity-70">{pipe.CurrentState ?? 'UNKNOWN'}</span>
											{#if pipe.CurrentState !== pipe.DesiredState}
												<span class="text-[7px] bg-amber-100 dark:bg-amber-900/30 text-amber-600 px-1 rounded font-black uppercase">→{pipe.DesiredState}</span>
											{/if}
										</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300 flex-shrink-0" />
							</div>
						{/each}

						{#if !filteredPipes.length && !loading}
							<div class="p-12 text-center">
								<GitBranch class="w-10 h-10 text-slate-200 dark:text-slate-700 mx-auto mb-3" />
								<p class="text-slate-400 text-sm italic font-bold">No pipes found.</p>
								{#if !pipeList.length}
									<button
										onclick={() => (showCreateModal = true)}
										class="mt-3 text-xs text-rose-500 font-black underline"
									>Create your first pipe</button>
								{/if}
							</div>
						{/if}
					{/if}
				</div>

				{#if hasMore}
					<div class="p-3 border-t border-slate-100 dark:border-slate-700/50">
						<button
							onclick={() => loadPipes(true)}
							class="w-full py-2 text-xs font-black text-rose-500 hover:text-rose-700 uppercase tracking-widest transition-all"
						>
							Load More
						</button>
					</div>
				{/if}
			</div>
		</div>

		<!-- Detail View -->
		<div class="lg:col-span-8 space-y-6">
			{#if loadingDetails}
				<div
					class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-12 flex items-center justify-center"
				>
					<RefreshCw class="w-8 h-8 text-slate-400 animate-spin" />
				</div>
			{:else if selectedPipe}
				<div
					class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in slide-in-from-right-4 duration-300"
				>
					<!-- Detail header -->
					<div
						class="p-6 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-rose-500/5 to-orange-500/5"
					>
						<div class="flex justify-between items-start">
							<div>
								<h2 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none">
									{selectedPipe.Name}
								</h2>
								{#if selectedPipe.Description}
									<p class="text-slate-500 dark:text-slate-400 text-sm mt-1 italic">{selectedPipe.Description}</p>
								{/if}
								<div class="flex items-center gap-2 mt-3 flex-wrap">
									<div class="px-2 py-0.5 rounded-lg text-[9px] font-black uppercase tracking-widest border {stateBadgeClass(selectedPipe.CurrentState)}">
										{selectedPipe.CurrentState ?? 'UNKNOWN'}
									</div>
									{#if selectedPipe.DesiredState && selectedPipe.DesiredState !== selectedPipe.CurrentState}
										<div class="px-2 py-0.5 rounded-lg bg-amber-500/10 text-amber-600 text-[9px] font-black uppercase tracking-widest border border-amber-500/20">
											DESIRED: {selectedPipe.DesiredState}
										</div>
									{/if}
									{#if selectedPipe.Tags && Object.keys(selectedPipe.Tags).length > 0}
										<div class="px-2 py-0.5 rounded-lg bg-slate-500/10 text-slate-500 text-[9px] font-black uppercase tracking-widest border border-slate-500/20">
											{Object.keys(selectedPipe.Tags).length} tags
										</div>
									{/if}
								</div>
							</div>
							<div class="flex items-center gap-2">
								{#if selectedPipe.CurrentState === 'STOPPED' || selectedPipe.CurrentState === 'CREATE_FAILED'}
									<button
										onclick={() => startPipe(selectedPipe?.Name)}
										disabled={actionInFlight !== null}
										class="p-2 bg-emerald-500/10 text-emerald-600 hover:bg-emerald-500/20 rounded-xl transition-all border border-emerald-500/20 disabled:opacity-50"
										title="Start Pipe"
									>
										{#if actionInFlight === 'start'}
											<RefreshCw class="w-4 h-4 animate-spin" />
										{:else}
											<Play class="w-4 h-4" />
										{/if}
									</button>
								{:else if selectedPipe.CurrentState === 'RUNNING'}
									<button
										onclick={() => stopPipe(selectedPipe?.Name)}
										disabled={actionInFlight !== null}
										class="p-2 bg-amber-500/10 text-amber-600 hover:bg-amber-500/20 rounded-xl transition-all border border-amber-500/20 disabled:opacity-50"
										title="Stop Pipe"
									>
										{#if actionInFlight === 'stop'}
											<RefreshCw class="w-4 h-4 animate-spin" />
										{:else}
											<Square class="w-4 h-4" />
										{/if}
									</button>
								{/if}
								<button
									onclick={openEditModal}
									class="p-2 bg-slate-500/10 text-slate-600 dark:text-slate-300 hover:bg-slate-500/20 rounded-xl transition-all border border-slate-500/20"
									title="Edit Pipe"
								>
									<Edit class="w-4 h-4" />
								</button>
								<button
									onclick={() => deletePipe(selectedPipe?.Name)}
									disabled={actionInFlight !== null}
									class="p-2 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-xl transition-all border border-rose-500/20 disabled:opacity-50"
									title="Delete Pipe"
								>
									{#if actionInFlight === 'delete'}
										<RefreshCw class="w-4 h-4 animate-spin" />
									{:else}
										<Trash2 class="w-4 h-4" />
									{/if}
								</button>
							</div>
						</div>

						<!-- State reason -->
						{#if selectedPipe.StateReason}
							<div class="mt-4 p-3 bg-amber-50 dark:bg-amber-900/20 rounded-xl border border-amber-200 dark:border-amber-700/50 flex items-start gap-2">
								<AlertTriangle class="w-4 h-4 text-amber-500 flex-shrink-0 mt-0.5" />
								<div class="text-xs text-amber-700 dark:text-amber-300">{selectedPipe.StateReason}</div>
							</div>
						{/if}
					</div>

					<!-- Tabs -->
					<div class="flex border-b border-slate-100 dark:border-slate-700/50">
						{#each [['overview', 'Overview'], ['tags', 'Tags'], ['config', 'Config']] as [tab, label]}
							<button
								onclick={() => setTab(tab)}
								class="px-6 py-3 text-xs font-black uppercase tracking-widest transition-all {activeDetailTab === tab ? 'text-rose-600 border-b-2 border-rose-500' : 'text-slate-400 hover:text-slate-600'}"
							>
								{label}
							</button>
						{/each}
					</div>

					<div class="p-6">
						{#if activeDetailTab === 'overview'}
							<!-- Source → Enrichment → Target flow -->
							<div class="space-y-4">
								<div class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Data Flow</div>
								<div class="flex items-stretch gap-2">
									<div class="flex-1 p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 shadow-sm">
										<div class="flex items-center gap-1.5 mb-2">
											<Database class="w-3.5 h-3.5 text-rose-500" />
											<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest">Source</div>
										</div>
										<div class="text-xs font-mono text-slate-700 dark:text-slate-300 break-all leading-relaxed">{arnLabel(selectedPipe.Source)}</div>
										<div class="text-[8px] text-slate-400 mt-1 uppercase font-bold">{arnService(selectedPipe.Source)}</div>
									</div>
									<div class="flex flex-col items-center justify-center gap-1 flex-shrink-0">
										<ArrowRight class="w-5 h-5 text-rose-300" />
									</div>
									{#if selectedPipe.Enrichment}
										<div class="flex-1 p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-amber-100 dark:border-amber-700/30 shadow-sm">
											<div class="flex items-center gap-1.5 mb-2">
												<Activity class="w-3.5 h-3.5 text-amber-500" />
												<div class="text-[8px] font-black text-amber-500 uppercase tracking-widest">Enrichment</div>
											</div>
											<div class="text-xs font-mono text-slate-700 dark:text-slate-300 break-all leading-relaxed">{arnLabel(selectedPipe.Enrichment)}</div>
										</div>
										<div class="flex flex-col items-center justify-center gap-1 flex-shrink-0">
											<ArrowRight class="w-5 h-5 text-rose-300" />
										</div>
									{/if}
									<div class="flex-1 p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 shadow-sm">
										<div class="flex items-center gap-1.5 mb-2">
											<Zap class="w-3.5 h-3.5 text-emerald-500" />
											<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest">Target</div>
										</div>
										<div class="text-xs font-mono text-slate-700 dark:text-slate-300 break-all leading-relaxed">{arnLabel(selectedPipe.Target)}</div>
										<div class="text-[8px] text-slate-400 mt-1 uppercase font-bold">{arnService(selectedPipe.Target)}</div>
									</div>
								</div>

								<!-- Metadata grid -->
								<div class="grid grid-cols-2 gap-3 mt-4">
									<div class="p-3 bg-white/60 dark:bg-slate-900/60 rounded-xl border border-slate-100 dark:border-slate-700/50">
										<div class="flex items-center gap-1.5 mb-1">
											<Clock class="w-3 h-3 text-slate-400" />
											<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest">Created</div>
										</div>
										<div class="text-xs font-mono text-slate-600 dark:text-slate-300">
											{selectedPipe.CreationTime ? new Date(selectedPipe.CreationTime).toLocaleString() : '—'}
										</div>
									</div>
									<div class="p-3 bg-white/60 dark:bg-slate-900/60 rounded-xl border border-slate-100 dark:border-slate-700/50">
										<div class="flex items-center gap-1.5 mb-1">
											<Clock class="w-3 h-3 text-slate-400" />
											<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest">Modified</div>
										</div>
										<div class="text-xs font-mono text-slate-600 dark:text-slate-300">
											{selectedPipe.LastModifiedTime ? new Date(selectedPipe.LastModifiedTime).toLocaleString() : '—'}
										</div>
									</div>
								</div>

								{#if selectedPipe.Arn}
									<div class="p-3 bg-white/60 dark:bg-slate-900/60 rounded-xl border border-slate-100 dark:border-slate-700/50">
										<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1">ARN</div>
										<div class="text-xs font-mono text-slate-600 dark:text-slate-300 break-all">{selectedPipe.Arn}</div>
									</div>
								{/if}
							</div>
						{:else if activeDetailTab === 'tags'}
							<!-- Tag management -->
							<div class="space-y-4">
								<!-- Add tag form -->
								<div class="flex gap-2">
									<input
										type="text"
										bind:value={newTagKey}
										placeholder="Key"
										class="flex-1 px-3 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-xs font-mono focus:ring-2 focus:ring-rose-500 outline-none"
									/>
									<input
										type="text"
										bind:value={newTagValue}
										placeholder="Value"
										class="flex-1 px-3 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-xs font-mono focus:ring-2 focus:ring-rose-500 outline-none"
									/>
									<button
										onclick={addTag}
										disabled={!newTagKey.trim() || taggingInFlight}
										class="px-4 py-2 bg-rose-600 hover:bg-rose-700 disabled:opacity-50 text-white rounded-xl text-xs font-black transition-all"
									>
										{#if taggingInFlight}<RefreshCw class="w-3.5 h-3.5 animate-spin" />{:else}<Plus class="w-3.5 h-3.5" />{/if}
									</button>
								</div>

								{#if selectedPipe.Tags && Object.keys(selectedPipe.Tags).length > 0}
									<div class="space-y-2">
										{#each Object.entries(selectedPipe.Tags) as [k, v]}
											<div class="flex items-center justify-between p-3 bg-white/60 dark:bg-slate-900/60 rounded-xl border border-slate-100 dark:border-slate-700/50">
												<div class="flex items-center gap-3">
													<Tag class="w-3.5 h-3.5 text-slate-400" />
													<span class="text-xs font-mono text-slate-700 dark:text-slate-300 font-bold">{k}</span>
													<span class="text-slate-400 text-xs">→</span>
													<span class="text-xs font-mono text-slate-600 dark:text-slate-400">{v}</span>
												</div>
												<button
													onclick={() => removeTag(k)}
													class="p-1 hover:text-rose-500 text-slate-400 transition-colors rounded"
												>
													<X class="w-3.5 h-3.5" />
												</button>
											</div>
										{/each}
									</div>
								{:else}
									<div class="py-8 text-center text-slate-400 text-xs italic font-bold">No tags configured.</div>
								{/if}
							</div>
						{:else if activeDetailTab === 'config'}
							<!-- Source/Target parameters -->
							<div class="space-y-4">
								{#if selectedPipe.RoleArn}
									<div class="p-3 bg-white/60 dark:bg-slate-900/60 rounded-xl border border-slate-100 dark:border-slate-700/50">
										<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1">Role ARN</div>
										<div class="text-xs font-mono text-slate-600 dark:text-slate-300 break-all">{selectedPipe.RoleArn}</div>
									</div>
								{/if}
								<div class="p-3 bg-white/60 dark:bg-slate-900/60 rounded-xl border border-slate-100 dark:border-slate-700/50">
									<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1">Source Full ARN</div>
									<div class="text-xs font-mono text-slate-600 dark:text-slate-300 break-all">{selectedPipe.Source ?? '—'}</div>
								</div>
								<div class="p-3 bg-white/60 dark:bg-slate-900/60 rounded-xl border border-slate-100 dark:border-slate-700/50">
									<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1">Target Full ARN</div>
									<div class="text-xs font-mono text-slate-600 dark:text-slate-300 break-all">{selectedPipe.Target ?? '—'}</div>
								</div>
								{#if selectedPipe.SourceParameters?.SqsQueueParameters?.BatchSize}
									<div class="p-3 bg-white/60 dark:bg-slate-900/60 rounded-xl border border-slate-100 dark:border-slate-700/50">
										<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1">SQS Batch Size</div>
										<div class="text-sm font-black text-slate-700 dark:text-slate-300">{selectedPipe.SourceParameters.SqsQueueParameters.BatchSize}</div>
									</div>
								{/if}
								{#if selectedPipe.SourceParameters?.FilterCriteria?.Filters?.length}
									<div class="p-3 bg-white/60 dark:bg-slate-900/60 rounded-xl border border-slate-100 dark:border-slate-700/50">
										<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-2">Filter Criteria</div>
										{#each selectedPipe.SourceParameters.FilterCriteria.Filters as filter}
											<div class="text-xs font-mono text-slate-600 dark:text-slate-300 bg-slate-50 dark:bg-slate-800/50 p-2 rounded-lg">{filter.Pattern}</div>
										{/each}
									</div>
								{/if}
								{#if selectedPipe.TargetParameters?.InputTemplate}
									<div class="p-3 bg-white/60 dark:bg-slate-900/60 rounded-xl border border-slate-100 dark:border-slate-700/50">
										<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1">Input Template</div>
										<div class="text-xs font-mono text-slate-600 dark:text-slate-300 bg-slate-50 dark:bg-slate-800/50 p-2 rounded-lg">{selectedPipe.TargetParameters.InputTemplate}</div>
									</div>
								{/if}
							</div>
						{/if}
					</div>
				</div>
			{:else}
				<div
					class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-16 flex flex-col items-center justify-center text-center"
				>
					<GitBranch class="w-16 h-16 text-slate-200 dark:text-slate-700 mb-4" />
					<p class="text-slate-400 text-sm italic font-bold">Select a pipe to view details</p>
					<button
						onclick={() => (showCreateModal = true)}
						class="mt-4 text-xs text-rose-500 font-black hover:underline"
					>Or create a new pipe →</button>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Pipe Modal -->
{#if showCreateModal}
	<!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
	<div
		class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm"
		onclick={closeModalOnBackdrop}
		role="dialog"
		aria-modal="true"
	>
		<div
			class="bg-white dark:bg-slate-900 rounded-3xl shadow-2xl w-full max-w-lg border border-slate-200 dark:border-slate-700/50 overflow-hidden max-h-[90vh] overflow-y-auto"
		>
			<div class="p-6 border-b border-slate-100 dark:border-slate-800 bg-gradient-to-br from-rose-500/5 to-orange-500/5 sticky top-0">
				<h2 class="text-xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">
					Configure Pipe
				</h2>
				<p class="text-slate-500 dark:text-slate-400 text-xs mt-1">
					Connect an event source to a target with optional enrichment.
				</p>
			</div>
			<form onsubmit={(e) => { e.preventDefault(); createPipe(); }} class="p-6 space-y-4">
				<div>
					<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="pipe-name">Pipe Name *</label>
					<input id="pipe-name" type="text" bind:value={newPipeName} placeholder="e.g. sqs-to-lambda-processor"
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all" required />
				</div>
				<div>
					<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="pipe-source">Source ARN *</label>
					<input id="pipe-source" type="text" bind:value={newPipeSource} placeholder="arn:aws:sqs:us-east-1:000000000000:my-queue"
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm font-mono focus:ring-2 focus:ring-rose-500 outline-none transition-all" required />
				</div>
				<div>
					<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="pipe-target">Target ARN *</label>
					<input id="pipe-target" type="text" bind:value={newPipeTarget} placeholder="arn:aws:lambda:us-east-1:000000000000:function:my-fn"
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm font-mono focus:ring-2 focus:ring-rose-500 outline-none transition-all" required />
				</div>
				<div class="grid grid-cols-2 gap-3">
					<div>
						<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="pipe-role">Role ARN</label>
						<input id="pipe-role" type="text" bind:value={newPipeRoleArn} placeholder="arn:aws:iam::000000000000:role/r"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-xs font-mono focus:ring-2 focus:ring-rose-500 outline-none transition-all" />
					</div>
					<div>
						<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="pipe-batch">SQS Batch Size</label>
						<input id="pipe-batch" type="number" bind:value={newPipeBatchSize} min="1" max="10000"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all" />
					</div>
				</div>
				<div>
					<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="pipe-enrichment">Enrichment ARN (optional)</label>
					<input id="pipe-enrichment" type="text" bind:value={newPipeEnrichment} placeholder="arn:aws:apigateway:us-east-1:..."
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm font-mono focus:ring-2 focus:ring-rose-500 outline-none transition-all" />
				</div>
				<div>
					<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="pipe-template">Input Template (optional)</label>
<<<<<<< Updated upstream
					<input id="pipe-template" type="text" bind:value={newPipeInputTemplate} placeholder='eg. input template JSON'
=======
					<input id="pipe-template" type="text" bind:value={newPipeInputTemplate} placeholder="e.g. Static JSON or $.path expression"
>>>>>>> Stashed changes
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm font-mono focus:ring-2 focus:ring-rose-500 outline-none transition-all" />
				</div>
				<div class="grid grid-cols-2 gap-3">
					<div>
						<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="pipe-state">Initial State</label>
						<select id="pipe-state" bind:value={newPipeDesiredState}
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all">
							<option value="RUNNING">Running</option>
							<option value="STOPPED">Stopped</option>
						</select>
					</div>
					<div>
						<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="pipe-desc">Description</label>
						<input id="pipe-desc" type="text" bind:value={newPipeDescription} placeholder="Brief description"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all" />
					</div>
				</div>
				<div class="flex gap-3 pt-2">
					<button type="button" onclick={() => (showCreateModal = false)}
						class="flex-1 py-2.5 bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-black text-xs uppercase tracking-widest transition-all">
						Cancel
					</button>
					<button type="submit" disabled={creating || !newPipeName.trim() || !newPipeSource.trim() || !newPipeTarget.trim()}
						class="flex-1 py-2.5 bg-rose-600 hover:bg-rose-700 disabled:opacity-50 text-white rounded-xl font-black text-xs uppercase tracking-widest transition-all shadow-lg shadow-rose-600/20">
						{creating ? 'Creating…' : 'Create Pipe'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Edit Pipe Modal -->
{#if showEditModal && selectedPipe}
	<!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
	<div
		class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm"
		onclick={closeModalOnBackdrop}
		role="dialog"
		aria-modal="true"
	>
		<div class="bg-white dark:bg-slate-900 rounded-3xl shadow-2xl w-full max-w-lg border border-slate-200 dark:border-slate-700/50 overflow-hidden">
			<div class="p-6 border-b border-slate-100 dark:border-slate-800 bg-gradient-to-br from-slate-500/5 to-slate-500/5">
				<h2 class="text-xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">
					Edit Pipe — {selectedPipe.Name}
				</h2>
			</div>
			<form onsubmit={(e) => { e.preventDefault(); updatePipe(); }} class="p-6 space-y-4">
				<div>
					<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="edit-target">Target ARN</label>
					<input id="edit-target" type="text" bind:value={editTarget}
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm font-mono focus:ring-2 focus:ring-rose-500 outline-none transition-all" />
				</div>
				<div>
					<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="edit-role">Role ARN</label>
					<input id="edit-role" type="text" bind:value={editRoleArn}
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm font-mono focus:ring-2 focus:ring-rose-500 outline-none transition-all" />
				</div>
				<div>
					<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="edit-state">Desired State</label>
					<select id="edit-state" bind:value={editDesiredState}
						class="w-full px-3 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all">
						<option value="">— No change —</option>
						<option value="RUNNING">Running</option>
						<option value="STOPPED">Stopped</option>
					</select>
				</div>
				<div>
					<label class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5" for="edit-desc">Description</label>
					<input id="edit-desc" type="text" bind:value={editDescription} placeholder="Brief description"
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all" />
				</div>
				<div class="flex gap-3 pt-2">
					<button type="button" onclick={() => (showEditModal = false)}
						class="flex-1 py-2.5 bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 text-slate-700 dark:text-slate-300 rounded-xl font-black text-xs uppercase tracking-widest transition-all">
						Cancel
					</button>
					<button type="submit" disabled={editing}
						class="flex-1 py-2.5 bg-slate-800 hover:bg-slate-900 disabled:opacity-50 text-white rounded-xl font-black text-xs uppercase tracking-widest transition-all">
						{editing ? 'Saving…' : 'Save Changes'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
