<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getLambdaClient } from '$lib/aws-client';
	import {
		ListFunctionsCommand,
		InvokeCommand,
		DeleteFunctionCommand,
		type FunctionConfiguration,
		type InvocationResponse
	} from '@aws-sdk/client-lambda';
	import { toast } from 'svelte-sonner';
	import { 
		Zap, Search, RefreshCw, Plus, Trash2, Play, 
		Code, Cpu, Clock, Terminal, Globe, Sliders, ChevronRight, X
	} from 'lucide-svelte';

	const lambda = getLambdaClient();

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let functions = $state<FunctionConfiguration[]>([]);
	let selectedFunction = $state<FunctionConfiguration | null>(null);

	// Invocation State
	let showInvokeModal = $state(false);
	let invokePayload = $state('{\n  "key": "value"\n}');
	let invoking = $state(false);
	let invokeResponse = $state<InvocationResponse | null>(null);

	// Derived
	const filteredFunctions = $derived(
		functions.filter(f => f.FunctionName?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function loadFunctions() {
		loading = true;
		try {
			const res = await lambda.send(new ListFunctionsCommand({}));
			functions = res.Functions ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load functions: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function deleteFunction(name: string) {
		if (!await confirmDestructive({ title: 'Delete Function', message: `Delete function "${name}"? All versions, aliases, and event source mappings will be removed.` })) return;
		try {
			await lambda.send(new DeleteFunctionCommand({ FunctionName: name }));
			toast.success(`Function "${name}" deleted`);
			if (selectedFunction?.FunctionName === name) selectedFunction = null;
			await loadFunctions();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function invokeFunction() {
		if (!selectedFunction) return;
		invoking = true;
		invokeResponse = null;
		try {
			const payload = new TextEncoder().encode(invokePayload);
			const res = await lambda.send(new InvokeCommand({
				FunctionName: selectedFunction.FunctionName,
				Payload: payload
			}));
			invokeResponse = res;
			
			if (res.StatusCode === 200) {
				toast.success(`Successfully invoked ${selectedFunction.FunctionName}`);
			} else {
				toast.warning(`Invoked with status ${res.StatusCode}`);
			}
		} catch (err: unknown) {
			toast.error(`Invocation failed: ${(err as Error).message}`);
		} finally {
			invoking = false;
		}
	}

	function parseResponsePayload(payload: Uint8Array | undefined): string {
		if (!payload) return 'No payload returned';
		try {
			const decoded = new TextDecoder().decode(payload);
			return JSON.stringify(JSON.parse(decoded), null, 2);
		} catch {
			return new TextDecoder().decode(payload);
		}
	}

	onMount(() => {
		loadFunctions();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-orange-500/20 rounded-xl">
				<Zap class="w-8 h-8 text-orange-500" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-orange-600 to-amber-600 dark:from-orange-400 dark:to-amber-400 bg-clip-text text-transparent">Lambda Functions</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Deploy and run serverless code in response to events.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={loadFunctions}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95"
				title="Refresh data"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button 
				onclick={() => toast.info("Function creation wizard coming soon.")}
				class="flex items-center gap-2 px-5 py-2.5 bg-orange-600 hover:bg-orange-700 text-white rounded-xl font-medium shadow-lg shadow-orange-600/20 transition-all active:scale-95"
			>
				<Plus class="w-5 h-5" />
				Create Function
			</button>
		</div>
	</div>

	<div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
		{#each [
			{ label: 'Total Functions', value: functions.length, color: 'text-orange-500' },
			{ label: 'Node.js', value: functions.filter(f => (f.Runtime ?? '').includes('nodejs')).length, color: 'text-green-500' },
			{ label: 'Python', value: functions.filter(f => (f.Runtime ?? '').includes('python')).length, color: 'text-blue-500' },
			{ label: 'With Layers', value: functions.filter(f => (f.Layers?.length ?? 0) > 0).length, color: 'text-purple-500' }
		] as s}
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-xl p-4">
				<p class="text-2xl font-bold {s.color}">{s.value}</p>
				<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">{s.label}</p>
			</div>
		{/each}
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Main List -->
		<div class="lg:col-span-8 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Search functions..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-orange-500 outline-none transition-all"
						/>
					</div>
				</div>

				<div class="overflow-x-auto">
					<table class="w-full text-left border-collapse">
						<thead>
							<tr class="bg-slate-50/50 dark:bg-slate-900/20">
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Function Details</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Runtime</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Last Modified</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
							{#if loading && !functions.length}
								{#each Array(3) as _}
									<tr class="animate-pulse">
										<td colspan="4" class="px-6 py-8"><div class="h-12 bg-slate-200/50 dark:bg-slate-700/30 rounded-xl w-full"></div></td>
									</tr>
								{/each}
							{:else}
								{#each filteredFunctions as func}
									<tr 
										class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20 transition-all cursor-pointer {selectedFunction?.FunctionArn === func.FunctionArn ? 'bg-orange-500/5 dark:bg-orange-500/10' : ''}"
										onclick={() => selectedFunction = func}
									>
										<td class="px-6 py-4">
											<div class="flex items-center gap-3">
												<div class="p-2 bg-orange-500/10 rounded-lg">
													<Zap class="w-5 h-5 text-orange-600 dark:text-orange-400" />
												</div>
												<div>
													<a
														href={`/dashboard/lambda/function?name=${encodeURIComponent(func.FunctionName ?? '')}`}
														class="font-bold text-slate-900 dark:text-white hover:text-orange-600 dark:hover:text-orange-400"
														onclick={(e) => e.stopPropagation()}
													>
														{func.FunctionName}
													</a>
													<div class="text-[10px] text-slate-400 font-mono truncate max-w-[200px]">{func.FunctionArn}</div>
												</div>
											</div>
										</td>
										<td class="px-6 py-4">
											<div class="flex items-center gap-2">
												<Code class="w-3 h-3 text-slate-400" />
												<span class="text-xs font-medium text-slate-600 dark:text-slate-300 bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded-md">{func.Runtime}</span>
											</div>
										</td>
										<td class="px-6 py-4 text-sm text-slate-500 dark:text-slate-400">{func.LastModified}</td>
										<td class="px-6 py-4 text-right">
											<div class="flex items-center justify-end gap-2">
												<button 
													onclick={(e) => { e.stopPropagation(); selectedFunction = func; showInvokeModal = true; }}
													class="p-2 text-teal-600 hover:bg-teal-500/10 rounded-lg transition-colors" 
													title="Invoke"
												>
													<Play class="w-4 h-4" />
												</button>
												<button 
													onclick={(e) => { e.stopPropagation(); deleteFunction(func.FunctionName!); }}
													class="p-2 text-slate-400 hover:text-red-500 rounded-lg transition-colors" 
													title="Delete"
												>
													<Trash2 class="w-4 h-4" />
												</button>
												<ChevronRight class="w-4 h-4 text-slate-300" />
											</div>
										</td>
									</tr>
								{/each}

								{#if !functions.length}
									<tr>
										<td colspan="4" class="px-6 py-20 text-center">
											<div class="flex flex-col items-center gap-4">
												<div class="p-4 bg-slate-50 dark:bg-slate-900/40 rounded-full">
													<Zap class="w-12 h-12 text-slate-300 dark:text-slate-700" />
												</div>
												<div>
													<p class="text-lg font-medium text-slate-900 dark:text-white">No functions found</p>
													<p class="text-sm text-slate-500 dark:text-slate-400">Created functions will appear here.</p>
												</div>
											</div>
										</td>
									</tr>
								{/if}
							{/if}
						</tbody>
					</table>
				</div>
			</div>
		</div>

		<!-- Detail View / Side Panel -->
		<div class="lg:col-span-4 space-y-6">
			{#if selectedFunction}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
					<div class="p-6 border-b border-slate-200 dark:border-slate-700/50 bg-gradient-to-br from-orange-500/5 to-amber-500/5">
						<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-1">{selectedFunction.FunctionName}</h2>
						<p class="text-xs text-slate-500 dark:text-slate-400 font-mono break-all">{selectedFunction.FunctionArn}</p>
					</div>

					<div class="p-6 space-y-6">
						<!-- Config Grid -->
						<div class="grid grid-cols-2 gap-4">
							<div class="p-3 bg-slate-50 dark:bg-slate-900/40 rounded-xl border border-slate-200 dark:border-slate-700/50">
								<div class="flex items-center gap-2 text-slate-500 dark:text-slate-400 text-[10px] uppercase font-bold tracking-wider mb-1">
									<Cpu class="w-3 h-3" />
									Memory
								</div>
								<div class="text-sm font-bold text-slate-900 dark:text-white">{selectedFunction.MemorySize} MB</div>
							</div>
							<div class="p-3 bg-slate-50 dark:bg-slate-900/40 rounded-xl border border-slate-200 dark:border-slate-700/50">
								<div class="flex items-center gap-2 text-slate-500 dark:text-slate-400 text-[10px] uppercase font-bold tracking-wider mb-1">
									<Clock class="w-3 h-3" />
									Timeout
								</div>
								<div class="text-sm font-bold text-slate-900 dark:text-white">{selectedFunction.Timeout}s</div>
							</div>
						</div>

						<!-- Handler -->
						<div>
							<h3 class="flex items-center gap-2 text-xs font-bold text-slate-500 dark:text-slate-400 uppercase tracking-wider mb-3">
								<Globe class="w-3 h-3" />
								Runtime Configuration
							</h3>
							<div class="space-y-3">
								<div class="flex justify-between items-center text-sm">
									<span class="text-slate-500 dark:text-slate-400">Handler</span>
									<span class="font-mono text-xs text-slate-900 dark:text-white bg-white/50 dark:bg-slate-700 px-2 py-0.5 rounded border border-slate-200 dark:border-slate-600">{selectedFunction.Handler}</span>
								</div>
								<div class="flex justify-between items-center text-sm">
									<span class="text-slate-500 dark:text-slate-400">Architecture</span>
									<span class="font-mono text-xs text-slate-900 dark:text-white">{selectedFunction.Architectures?.[0] ?? 'x86_64'}</span>
								</div>
							</div>
						</div>

						<!-- Env Vars -->
						<div>
							<h3 class="flex items-center gap-2 text-xs font-bold text-slate-500 dark:text-slate-400 uppercase tracking-wider mb-3">
								<Terminal class="w-3 h-3" />
								Environment Variables
							</h3>
							{#if selectedFunction.Environment?.Variables}
								<div class="space-y-2 max-h-48 overflow-y-auto pr-2">
									{#each Object.entries(selectedFunction.Environment.Variables) as [key, value]}
										<div class="p-2 bg-slate-900/5 dark:bg-slate-900/40 rounded-lg border border-slate-200/50 dark:border-slate-700/50 flex flex-col">
											<span class="text-[10px] text-slate-400 uppercase font-mono">{key}</span>
											<span class="text-xs font-medium text-slate-700 dark:text-white line-clamp-2">{value}</span>
										</div>
									{/each}
								</div>
							{:else}
								<p class="text-xs text-slate-400 italic">No environment variables defined.</p>
							{/if}
						</div>

						<button 
							onclick={() => showInvokeModal = true}
							class="w-full flex items-center justify-center gap-2 py-3 bg-teal-600 hover:bg-teal-700 text-white rounded-xl font-bold shadow-lg shadow-teal-600/20 transition-all active:scale-[0.98]"
						>
							<Play class="w-4 h-4 fill-current" />
							Test / Invoke
						</button>
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-2xl p-12 text-center flex flex-col items-center gap-4">
					<div class="p-4 bg-slate-50 dark:bg-slate-800 rounded-2xl">
						<Sliders class="w-10 h-10 text-slate-300 dark:text-slate-600" />
					</div>
					<p class="text-slate-500 dark:text-slate-400 text-sm font-medium">Select a function to view configuration and test invocation results.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Invoke Modal -->
{#if showInvokeModal && selectedFunction}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div role="none" onclick={() => showInvokeModal = false} onkeydown={(e) => e.key === 'Escape' && (showInvokeModal = false)} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"></div>
		<div class="relative w-full max-w-2xl bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700 overflow-hidden">
			<div class="flex items-center justify-between p-6 border-b border-slate-200 dark:border-slate-700/50">
				<div>
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">Invoke {selectedFunction.FunctionName}</h3>
					<p class="text-xs text-slate-500">Configure function payload and view response.</p>
				</div>
				<button onclick={() => showInvokeModal = false} class="p-2 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-lg transition-colors">
					<X class="w-5 h-5 text-slate-400" />
				</button>
			</div>

			<div class="p-6 grid grid-cols-1 md:grid-cols-2 gap-6">
				<!-- Input -->
				<div class="space-y-2">
					<label for="lambda-invoke-payload" class="text-xs font-bold text-slate-500 uppercase tracking-widest">Input Payload (JSON)</label>
					<div class="relative group">
						<textarea 
							id="lambda-invoke-payload"
							bind:value={invokePayload}
							class="w-full h-80 p-4 font-mono text-sm bg-slate-50 dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 focus:ring-2 focus:ring-orange-500 outline-none transition-all resize-none shadow-inner"
						></textarea>
						<div class="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
							<Terminal class="w-4 h-4 text-slate-300" />
						</div>
					</div>
				</div>

				<!-- Output -->
				<div class="space-y-2">
					<p class="text-xs font-bold text-slate-500 uppercase tracking-widest">Execution Response</p>
					<div class="h-80 w-full p-4 font-mono text-sm bg-slate-950 text-emerald-400 rounded-xl overflow-auto shadow-inner border border-black group relative">
						{#if invoking}
							<div class="flex items-center gap-2 animate-pulse">
								<RefreshCw class="w-4 h-4 animate-spin" />
								Invoking function...
							</div>
						{:else if invokeResponse}
							<div class="space-y-2">
								<div class="flex items-center gap-2 {invokeResponse.FunctionError ? 'text-red-400' : 'text-emerald-400'}">
									<div class="w-2 h-2 rounded-full {invokeResponse.FunctionError ? 'bg-red-500' : 'bg-emerald-500'}"></div>
									Status: {invokeResponse.StatusCode}
									{invokeResponse.FunctionError ? '(' + invokeResponse.FunctionError + ')' : ''}
								</div>
								<div class="text-slate-400 text-[10px] border-b border-slate-800 pb-1 mb-2">Payload:</div>
								<pre class="whitespace-pre-wrap">{parseResponsePayload(invokeResponse.Payload)}</pre>
							</div>
						{:else}
							<div class="text-slate-600 italic h-full flex items-center justify-center text-center px-8">
								The execution output will be displayed here after invocation.
							</div>
						{/if}
					</div>
				</div>
			</div>

			<div class="p-6 bg-slate-50 dark:bg-slate-900/50 border-t border-slate-200 dark:border-slate-700/50 flex justify-end gap-3">
				<button 
					onclick={() => showInvokeModal = false}
					class="px-5 py-2.5 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 text-slate-700 dark:text-slate-300 rounded-xl font-medium hover:bg-slate-50 dark:hover:bg-slate-600 transition-all"
				>
					Close
				</button>
				<button 
					onclick={invokeFunction}
					disabled={invoking}
					class="flex items-center gap-2 px-8 py-2.5 bg-orange-600 hover:bg-orange-700 text-white rounded-xl font-bold shadow-lg shadow-orange-600/20 disabled:opacity-50 transition-all active:scale-[0.98]"
				>
					{#if invoking}
						<RefreshCw class="w-4 h-4 animate-spin" />
						Invoking...
					{:else}
						<Play class="w-4 h-4 fill-current" />
						Invoke Function
					{/if}
				</button>
			</div>
		</div>
	</div>
{/if}

<style>
	/* Custom scrollbar for glassmorphism look */
	::-webkit-scrollbar {
		width: 8px;
		height: 8px;
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
