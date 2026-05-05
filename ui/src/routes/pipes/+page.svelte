<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getPipesClient } from '$lib/aws-client';
	import {
		ListPipesCommand,
		DescribePipeCommand,
		CreatePipeCommand,
		DeletePipeCommand,
		StartPipeCommand,
		StopPipeCommand,
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
		Clock
	} from 'lucide-svelte';

	const pipes = getPipesClient();

	let loading = $state(false);
	let pipeList = $state<Pipe[]>([]);
	let searchQuery = $state('');
	let selectedPipe = $state<DescribePipeResponse | null>(null);
	let loadingDetails = $state(false);

	let showCreateModal = $state(false);
	let creating = $state(false);
	let newPipeName = $state('');
	let newPipeSource = $state('arn:aws:sqs:us-east-1:000000000000:source-queue');
	let newPipeTarget = $state('arn:aws:lambda:us-east-1:000000000000:function:target-fn');
	let newPipeRoleArn = $state('arn:aws:iam::000000000000:role/pipes-role');
	let newPipeDescription = $state('');

	const filteredPipes = $derived(
		pipeList.filter((p) => p.Name?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function loadPipes() {
		loading = true;
		try {
			const res = await pipes.send(new ListPipesCommand({}));
			pipeList = res.Pipes ?? [];
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
		try {
			const res = await pipes.send(new DescribePipeCommand({ Name: name }));
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
			await pipes.send(
				new CreatePipeCommand({
					Name: newPipeName.trim(),
					Source: newPipeSource.trim(),
					Target: newPipeTarget.trim(),
					RoleArn: newPipeRoleArn.trim(),
					Description: newPipeDescription.trim() || undefined
				})
			);
			toast.success(`Pipe "${newPipeName}" created`);
			showCreateModal = false;
			newPipeName = '';
			newPipeDescription = '';
			await loadPipes();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creating = false;
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
		try {
			await pipes.send(new DeletePipeCommand({ Name: name }));
			toast.success('Pipe deleted');
			if (selectedPipe?.Name === name) selectedPipe = null;
			await loadPipes();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function startPipe(name: string | undefined) {
		if (!name) return;
		try {
			await pipes.send(new StartPipeCommand({ Name: name }));
			toast.success(`Pipe "${name}" started`);
			if (selectedPipe?.Name === name) await selectPipe(name);
			await loadPipes();
		} catch (err: unknown) {
			toast.error(`Start failed: ${(err as Error).message}`);
		}
	}

	async function stopPipe(name: string | undefined) {
		if (!name) return;
		try {
			await pipes.send(new StopPipeCommand({ Name: name }));
			toast.success(`Pipe "${name}" stopped`);
			if (selectedPipe?.Name === name) await selectPipe(name);
			await loadPipes();
		} catch (err: unknown) {
			toast.error(`Stop failed: ${(err as Error).message}`);
		}
	}

	function stateColor(state: string | undefined): string {
		if (state === 'RUNNING') return 'text-emerald-500';
		if (state === 'STOPPED') return 'text-slate-400';
		if (state === 'STARTING' || state === 'STOPPING') return 'text-amber-500';
		if (state === 'CREATE_FAILED' || state === 'UPDATE_FAILED' || state === 'DELETE_FAILED')
			return 'text-rose-500';
		return 'text-slate-400';
	}

	function stateBadgeClass(state: string | undefined): string {
		if (state === 'RUNNING') return 'bg-emerald-500/10 text-emerald-600 border-emerald-500/20';
		if (state === 'STOPPED') return 'bg-slate-500/10 text-slate-500 border-slate-500/20';
		if (state === 'STARTING' || state === 'STOPPING')
			return 'bg-amber-500/10 text-amber-600 border-amber-500/20';
		return 'bg-rose-500/10 text-rose-600 border-rose-500/20';
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
				onclick={loadPipes}
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

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Pipe List -->
		<div class="lg:col-span-4 space-y-4">
			<div
				class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden"
			>
				<div
					class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50"
				>
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input
							type="text"
							bind:value={searchQuery}
							placeholder="Search pipes..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all italic font-bold"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
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
										<div
											class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-xs truncate"
										>
											{pipe.Name}
										</div>
										<div
											class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic"
										>
											{pipe.CurrentState ?? 'UNKNOWN'}
										</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300 flex-shrink-0" />
							</div>
						{/each}

						{#if !pipeList.length && !loading}
							<div class="p-12 text-center text-slate-400 text-sm italic font-bold">
								No pipes configured.
							</div>
						{/if}
					{/if}
				</div>
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
					<div
						class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-rose-500/5 to-orange-500/5 flex justify-between items-start"
					>
						<div>
							<h2
								class="text-3xl font-black text-slate-900 dark:text-white mb-2 uppercase tracking-tighter italic leading-none"
							>
								{selectedPipe.Name}
							</h2>
							{#if selectedPipe.Description}
								<p class="text-slate-500 dark:text-slate-400 text-sm mt-1 italic">
									{selectedPipe.Description}
								</p>
							{/if}
							<div class="flex items-center gap-3 mt-4">
								<div
									class="px-2 py-0.5 rounded-lg text-[9px] font-black uppercase tracking-widest border {stateBadgeClass(selectedPipe.CurrentState)}"
								>
									{selectedPipe.CurrentState ?? 'UNKNOWN'}
								</div>
								{#if selectedPipe.DesiredState && selectedPipe.DesiredState !== selectedPipe.CurrentState}
									<div
										class="px-2 py-0.5 rounded-lg bg-amber-500/10 text-amber-600 text-[9px] font-black uppercase tracking-widest border border-amber-500/20"
									>
										DESIRED: {selectedPipe.DesiredState}
									</div>
								{/if}
							</div>
						</div>
						<div class="flex items-center gap-2">
							{#if selectedPipe.CurrentState === 'STOPPED'}
								<button
									onclick={() => startPipe(selectedPipe?.Name)}
									class="p-2.5 bg-emerald-500/10 text-emerald-600 hover:bg-emerald-500/20 rounded-2xl transition-all border border-emerald-500/20"
									title="Start Pipe"
								>
									<Play class="w-4 h-4" />
								</button>
							{:else if selectedPipe.CurrentState === 'RUNNING'}
								<button
									onclick={() => stopPipe(selectedPipe?.Name)}
									class="p-2.5 bg-amber-500/10 text-amber-600 hover:bg-amber-500/20 rounded-2xl transition-all border border-amber-500/20"
									title="Stop Pipe"
								>
									<Square class="w-4 h-4" />
								</button>
							{/if}
							<button
								onclick={() => deletePipe(selectedPipe?.Name)}
								class="p-2.5 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-2xl transition-all border border-rose-500/20 shadow-xl"
								title="Delete Pipe"
							>
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
					</div>

					<div class="p-8 space-y-6">
						<!-- Source → Target flow -->
						<div>
							<div
								class="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-3 italic"
							>
								Data Flow
							</div>
							<div class="flex items-center gap-3">
								<div
									class="flex-1 p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 shadow-sm"
								>
									<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1">
										Source
									</div>
									<div
										class="text-xs font-mono text-slate-700 dark:text-slate-300 break-all leading-relaxed"
									>
										{selectedPipe.Source ?? '—'}
									</div>
								</div>
								<ArrowRight class="w-6 h-6 text-rose-400 flex-shrink-0" />
								<div
									class="flex-1 p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 shadow-sm"
								>
									<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1">
										Target
									</div>
									<div
										class="text-xs font-mono text-slate-700 dark:text-slate-300 break-all leading-relaxed"
									>
										{selectedPipe.Target ?? '—'}
									</div>
								</div>
							</div>
							{#if selectedPipe.Enrichment}
								<div class="mt-3 flex items-center gap-3">
									<div class="flex-none w-full">
										<div
											class="p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 shadow-sm"
										>
											<div
												class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1"
											>
												Enrichment
											</div>
											<div
												class="text-xs font-mono text-slate-700 dark:text-slate-300 break-all leading-relaxed"
											>
												{selectedPipe.Enrichment}
											</div>
										</div>
									</div>
								</div>
							{/if}
						</div>

						<!-- Metadata -->
						<div class="grid grid-cols-2 gap-4">
							<div
								class="p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 shadow-sm"
							>
								<div class="flex items-center gap-2 mb-2">
									<Clock class="w-3.5 h-3.5 text-slate-400" />
									<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest">
										Created
									</div>
								</div>
								<div class="text-xs font-mono text-slate-600 dark:text-slate-300">
									{selectedPipe.CreationTime
										? new Date(selectedPipe.CreationTime).toLocaleString()
										: '—'}
								</div>
							</div>
							<div
								class="p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 shadow-sm"
							>
								<div class="flex items-center gap-2 mb-2">
									<Clock class="w-3.5 h-3.5 text-slate-400" />
									<div class="text-[8px] font-black text-slate-400 uppercase tracking-widest">
										Last Modified
									</div>
								</div>
								<div class="text-xs font-mono text-slate-600 dark:text-slate-300">
									{selectedPipe.LastModifiedTime
										? new Date(selectedPipe.LastModifiedTime).toLocaleString()
										: '—'}
								</div>
							</div>
						</div>

						<!-- ARN & Role -->
						{#if selectedPipe.Arn || selectedPipe.RoleArn}
							<div class="space-y-3">
								{#if selectedPipe.Arn}
									<div
										class="p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 shadow-sm"
									>
										<div
											class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1"
										>
											ARN
										</div>
										<div
											class="text-xs font-mono text-slate-600 dark:text-slate-300 break-all"
										>
											{selectedPipe.Arn}
										</div>
									</div>
								{/if}
								{#if selectedPipe.RoleArn}
									<div
										class="p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 shadow-sm"
									>
										<div
											class="text-[8px] font-black text-slate-400 uppercase tracking-widest mb-1"
										>
											Role ARN
										</div>
										<div
											class="text-xs font-mono text-slate-600 dark:text-slate-300 break-all"
										>
											{selectedPipe.RoleArn}
										</div>
									</div>
								{/if}
							</div>
						{/if}

						{#if selectedPipe.StateReason}
							<div
								class="p-4 bg-amber-50 dark:bg-amber-900/20 rounded-2xl border border-amber-200 dark:border-amber-700/50"
							>
								<div class="text-[8px] font-black text-amber-600 uppercase tracking-widest mb-1">
									State Reason
								</div>
								<div class="text-xs text-amber-700 dark:text-amber-300 italic">
									{selectedPipe.StateReason}
								</div>
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
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Pipe Modal -->
{#if showCreateModal}
	<div
		class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-sm"
		role="dialog"
		aria-modal="true"
	>
		<div
			class="bg-white dark:bg-slate-900 rounded-3xl shadow-2xl w-full max-w-lg border border-slate-200 dark:border-slate-700/50 overflow-hidden"
		>
			<div
				class="p-6 border-b border-slate-100 dark:border-slate-800 bg-gradient-to-br from-rose-500/5 to-orange-500/5"
			>
				<h2 class="text-xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">
					Configure Pipe
				</h2>
				<p class="text-slate-500 dark:text-slate-400 text-xs mt-1">
					Create a new EventBridge Pipe to connect a source to a target.
				</p>
			</div>
			<form onsubmit={(e) => { e.preventDefault(); createPipe(); }} class="p-6 space-y-4">
				<div>
					<label
						class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5"
						for="pipe-name">Pipe Name</label
					>
					<input
						id="pipe-name"
						type="text"
						bind:value={newPipeName}
						placeholder="e.g. sqs-to-lambda-processor"
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all"
						required
					/>
				</div>
				<div>
					<label
						class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5"
						for="pipe-source">Source ARN</label
					>
					<input
						id="pipe-source"
						type="text"
						bind:value={newPipeSource}
						placeholder="arn:aws:sqs:us-east-1:000000000000:my-queue"
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm font-mono focus:ring-2 focus:ring-rose-500 outline-none transition-all"
						required
					/>
				</div>
				<div>
					<label
						class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5"
						for="pipe-target">Target ARN</label
					>
					<input
						id="pipe-target"
						type="text"
						bind:value={newPipeTarget}
						placeholder="arn:aws:lambda:us-east-1:000000000000:function:my-fn"
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm font-mono focus:ring-2 focus:ring-rose-500 outline-none transition-all"
						required
					/>
				</div>
				<div>
					<label
						class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5"
						for="pipe-role">Role ARN</label
					>
					<input
						id="pipe-role"
						type="text"
						bind:value={newPipeRoleArn}
						placeholder="arn:aws:iam::000000000000:role/pipes-role"
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm font-mono focus:ring-2 focus:ring-rose-500 outline-none transition-all"
					/>
				</div>
				<div>
					<label
						class="block text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1.5"
						for="pipe-desc">Description (optional)</label
					>
					<input
						id="pipe-desc"
						type="text"
						bind:value={newPipeDescription}
						placeholder="Brief description of this pipe"
						class="w-full px-4 py-2.5 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all"
					/>
				</div>
				<div class="flex gap-3 pt-2">
					<button
						type="button"
						onclick={() => (showCreateModal = false)}
						class="flex-1 py-2.5 bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-black text-xs uppercase tracking-widest transition-all"
					>
						Cancel
					</button>
					<button
						type="submit"
						disabled={creating || !newPipeName.trim() || !newPipeSource.trim() || !newPipeTarget.trim()}
						class="flex-1 py-2.5 bg-rose-600 hover:bg-rose-700 disabled:opacity-50 text-white rounded-xl font-black text-xs uppercase tracking-widest transition-all shadow-lg shadow-rose-600/20"
					>
						{creating ? 'Creating…' : 'Create Pipe'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
