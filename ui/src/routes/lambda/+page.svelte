<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { urlState } from '$lib/url-state.svelte';
	import LiveDot from '$lib/components/LiveDot.svelte';
	import { getLambdaClient } from '$lib/aws-client';
	import {
		ListFunctionsCommand,
		InvokeCommand,
		DeleteFunctionCommand,
		CreateFunctionCommand,
		ListLayersCommand,
		UpdateFunctionConfigurationCommand,
		UpdateFunctionCodeCommand,
		ListVersionsByFunctionCommand,
		PublishVersionCommand,
		ListAliasesCommand,
		CreateAliasCommand,
		DeleteAliasCommand,
		ListEventSourceMappingsCommand,
		CreateEventSourceMappingCommand,
		DeleteEventSourceMappingCommand,
		type FunctionConfiguration,
		type InvocationResponse,
		type LayersListItem,
		type AliasConfiguration,
		type EventSourceMappingConfiguration
	} from '@aws-sdk/client-lambda';
	import { toast } from 'svelte-sonner';
	import { 
		Zap, Search, RefreshCw, Plus, Trash2, Play, 
		Code, Cpu, Clock, Terminal, Globe, Sliders, ChevronRight, X
	} from 'lucide-svelte';

	const lambda = regionalClient(getLambdaClient);

	// State
	let loading = $state(false);
	// URL-backed (?q=..., ?runtime=...); see url-state.svelte.ts. Neither is
	// read inside the onRegionChange effect below, so no untrack() is
	// needed at that call site.
	const searchQueryParam = urlState<string>('q', '');
	let searchQuery = $derived(searchQueryParam.get());
	const runtimeFilterParam = urlState<string>('runtime', '');
	let runtimeFilter = $derived(runtimeFilterParam.get());
	let functions = $state<FunctionConfiguration[]>([]);
	let selectedFunction = $state<FunctionConfiguration | null>(null);
	let nextMarker = $state('');
	let hasNextPage = $state(false);
	let currentMarker = $state('');
	let markerHistory = $state<string[]>([]);

	// Invocation State
	let showInvokeModal = $state(false);
	let invokePayload = $state('{\n  "key": "value"\n}');
	let invoking = $state(false);
	let invokeResponse = $state<InvocationResponse | null>(null);
	let invokeType = $state<'RequestResponse' | 'Event' | 'DryRun'>('RequestResponse');

	// Invocation History
	interface InvocationRecord {
		functionName: string;
		timestamp: Date;
		statusCode: number | undefined;
		payload: string;
	}
	let invocationHistory = $state<InvocationRecord[]>([]);

	// Function detail tab
	let fnDetailTab = $state<'config' | 'versions' | 'aliases' | 'triggers' | 'code'>('config');

	// Versions
	let fnVersions = $state<FunctionConfiguration[]>([]);
	let versionsLoading = $state(false);
	let publishDesc = $state('');
	let publishing = $state(false);

	// Aliases
	let fnAliases = $state<AliasConfiguration[]>([]);
	let aliasesLoading = $state(false);
	let newAliasName = $state('');
	let newAliasFnVersion = $state('$LATEST');
	let creatingAlias = $state(false);

	// Event Source Mappings
	let fnEsms = $state<EventSourceMappingConfiguration[]>([]);
	let esmsLoading = $state(false);
	let newEsmEventArn = $state('');
	let newEsmBatchSize = $state(10);
	let creatingEsm = $state(false);

	// Code Update
	let updateCodeImageUri = $state('');
	let updatingCode = $state(false);
	let updateCodeZipFile = $state<File | null>(null);
	let updateCodeMode = $state<'image' | 'zip'>('image');

	// Layer Management
	let layers = $state<LayersListItem[]>([]);
	let layersLoading = $state(false);
	let showLayerTab = $state(false);

	// Env Var Editor
	let editingEnvVars = $state(false);
	let envVarDraft = $state<Record<string, string>>({});
	let savingEnvVars = $state(false);

	// Create Function Modal State
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newFnName = $state('');
	let newFnRuntime = $state('python3.12');
	let newFnRole = $state('arn:aws:iam::000000000000:role/lambda-role');
	let newFnHandler = $state('handler.handler');
	let newFnMemory = $state(128);
	let newFnTimeout = $state(30);
	let newFnDescription = $state('');

	// Derived
	const filteredFunctions = $derived(
		functions.filter(f => {
			const matchSearch = f.FunctionName?.toLowerCase().includes(searchQuery.toLowerCase());
			const matchRuntime = !runtimeFilter || (f.Runtime ?? '').includes(runtimeFilter);
			return matchSearch && matchRuntime;
		})
	);

	const runtimeImageMap: Record<string, string> = {
		'nodejs22.x': 'public.ecr.aws/lambda/nodejs:22',
		'nodejs20.x': 'public.ecr.aws/lambda/nodejs:20',
		'python3.12': 'public.ecr.aws/lambda/python:3.12',
		'python3.11': 'public.ecr.aws/lambda/python:3.11',
		'java21': 'public.ecr.aws/lambda/java:21',
		'go1.x': 'public.ecr.aws/lambda/provided:al2',
		'ruby3.3': 'public.ecr.aws/lambda/ruby:3.3',
		'dotnet8': 'public.ecr.aws/lambda/dotnet:8',
	};

	// Actions
	async function loadFunctions(marker = '') {
		loading = true;
		try {
			const res = await lambda().send(new ListFunctionsCommand({ Marker: marker || undefined }));
			functions = res.Functions ?? [];
			nextMarker = res.NextMarker ?? '';
			hasNextPage = !!res.NextMarker;
			currentMarker = marker;
		} catch (err: unknown) {
			toast.error(`Failed to load functions: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function deleteFunction(name: string) {
		if (!await confirmDestructive({ title: 'Delete Function', message: `Delete function "${name}"? All versions, aliases, and event source mappings will be removed.` })) return;
		try {
			await lambda().send(new DeleteFunctionCommand({ FunctionName: name }));
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
			const res = await lambda().send(new InvokeCommand({
				FunctionName: selectedFunction.FunctionName,
				InvocationType: invokeType,
				LogType: 'Tail',
				Payload: payload
			}));
			invokeResponse = res;
			invocationHistory = [
				{
					functionName: selectedFunction.FunctionName ?? '',
					timestamp: new Date(),
					statusCode: res.StatusCode,
					payload: invokePayload.slice(0, 100)
				},
				...invocationHistory.slice(0, 19)
			];

			if (res.StatusCode === 200 || res.StatusCode === 202) {
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

	async function createFunction() {
		if (!newFnName.trim()) {
			toast.error('Function name is required');
			return;
		}
		creating = true;
		try {
			const imageUri = runtimeImageMap[newFnRuntime] ?? 'public.ecr.aws/lambda/python:3.12';
			await lambda().send(new CreateFunctionCommand({
				FunctionName: newFnName.trim(),
				PackageType: 'Image',
				Code: { ImageUri: imageUri },
				Role: newFnRole,
				MemorySize: newFnMemory,
				Timeout: newFnTimeout,
				Description: newFnDescription || undefined,
			}));
			const createdName = newFnName.trim();
			toast.success(`Function "${createdName}" created`);
			showCreateModal = false;
			newFnName = '';
			newFnDescription = '';
			await loadFunctions();
			selectedFunction = functions.find(f => f.FunctionName === createdName) ?? null;
			if (selectedFunction) fnDetailTab = 'code';
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creating = false;
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

	function decodeLogResult(logResult: string | undefined): string {
		if (!logResult) return '';
		try {
			return atob(logResult);
		} catch {
			return logResult;
		}
	}

	async function loadVersions(fnName: string) {
		versionsLoading = true;
		try {
			const res = await lambda().send(new ListVersionsByFunctionCommand({ FunctionName: fnName }));
			fnVersions = res.Versions ?? [];
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to load versions');
		} finally {
			versionsLoading = false;
		}
	}

	async function publishVersion() {
		if (!selectedFunction?.FunctionName) return;
		publishing = true;
		try {
			await lambda().send(new PublishVersionCommand({ FunctionName: selectedFunction.FunctionName, Description: publishDesc || undefined }));
			toast.success('Version published');
			publishDesc = '';
			await loadVersions(selectedFunction.FunctionName);
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to publish');
		} finally {
			publishing = false;
		}
	}

	async function loadAliases(fnName: string) {
		aliasesLoading = true;
		try {
			const res = await lambda().send(new ListAliasesCommand({ FunctionName: fnName }));
			fnAliases = res.Aliases ?? [];
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to load aliases');
		} finally {
			aliasesLoading = false;
		}
	}

	async function createAlias() {
		if (!selectedFunction?.FunctionName || !newAliasName.trim()) return;
		creatingAlias = true;
		try {
			await lambda().send(new CreateAliasCommand({ FunctionName: selectedFunction.FunctionName, Name: newAliasName.trim(), FunctionVersion: newAliasFnVersion }));
			toast.success(`Alias "${newAliasName.trim()}" created`);
			newAliasName = '';
			await loadAliases(selectedFunction.FunctionName);
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to create alias');
		} finally {
			creatingAlias = false;
		}
	}

	async function deleteAlias(name: string) {
		if (!selectedFunction?.FunctionName || !await confirmDestructive({ title: 'Delete Alias', message: `Delete alias "${name}"?`, confirmLabel: 'Delete' })) return;
		try {
			await lambda().send(new DeleteAliasCommand({ FunctionName: selectedFunction.FunctionName, Name: name }));
			toast.success('Alias deleted');
			await loadAliases(selectedFunction.FunctionName);
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to delete alias');
		}
	}

	async function loadEsms(fnName: string) {
		esmsLoading = true;
		try {
			const res = await lambda().send(new ListEventSourceMappingsCommand({ FunctionName: fnName }));
			fnEsms = res.EventSourceMappings ?? [];
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to load event sources');
		} finally {
			esmsLoading = false;
		}
	}

	async function createEsm() {
		if (!selectedFunction?.FunctionArn || !newEsmEventArn.trim()) return;
		creatingEsm = true;
		try {
			await lambda().send(new CreateEventSourceMappingCommand({ FunctionName: selectedFunction.FunctionArn, EventSourceArn: newEsmEventArn.trim(), BatchSize: newEsmBatchSize, Enabled: true }));
			toast.success('Event source mapping created');
			newEsmEventArn = '';
			await loadEsms(selectedFunction.FunctionName!);
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to create event source');
		} finally {
			creatingEsm = false;
		}
	}

	async function deleteEsm(uuid: string) {
		if (!await confirmDestructive({ title: 'Delete Trigger', message: 'Remove this event source mapping?', confirmLabel: 'Remove' })) return;
		try {
			await lambda().send(new DeleteEventSourceMappingCommand({ UUID: uuid }));
			toast.success('Event source mapping removed');
			if (selectedFunction?.FunctionName) await loadEsms(selectedFunction.FunctionName);
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to delete event source');
		}
	}

	async function updateFunctionCode() {
		if (!selectedFunction?.FunctionName) return;
		if (updateCodeMode === 'image' && !updateCodeImageUri.trim()) return;
		if (updateCodeMode === 'zip' && !updateCodeZipFile) return;
		updatingCode = true;
		try {
			if (updateCodeMode === 'image') {
				await lambda().send(new UpdateFunctionCodeCommand({ FunctionName: selectedFunction.FunctionName, ImageUri: updateCodeImageUri.trim() }));
			} else {
				const buf = await updateCodeZipFile!.arrayBuffer();
				await lambda().send(new UpdateFunctionCodeCommand({ FunctionName: selectedFunction.FunctionName, ZipFile: new Uint8Array(buf) }));
			}
			toast.success('Function code updated');
			updateCodeImageUri = '';
			updateCodeZipFile = null;
			await loadFunctions();
		} catch (e) {
			toast.error(e instanceof Error ? e.message : 'Failed to update code');
		} finally {
			updatingCode = false;
		}
	}

	async function loadLayers() {
		layersLoading = true;
		try {
			const res = await lambda().send(new ListLayersCommand({}));
			layers = res.Layers ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load layers: ${(err as Error).message}`);
		} finally {
			layersLoading = false;
		}
	}

	function startEditEnvVars() {
		envVarDraft = { ...(selectedFunction?.Environment?.Variables) };
		editingEnvVars = true;
	}

	async function saveEnvVars() {
		if (!selectedFunction) return;
		savingEnvVars = true;
		try {
			await lambda().send(new UpdateFunctionConfigurationCommand({
				FunctionName: selectedFunction.FunctionName,
				Environment: { Variables: envVarDraft }
			}));
			toast.success('Environment variables saved');
			editingEnvVars = false;
			await loadFunctions();
		} catch (err: unknown) {
			toast.error(`Save failed: ${(err as Error).message}`);
		} finally {
			savingEnvVars = false;
		}
	}

	// selectedFunction and its versions/aliases/triggers, plus the list-page
	// pagination markers and the layers list, all reference resources or
	// tokens from whichever region they were fetched in — clear the whole
	// selection chain and reload, mirroring the original mount sequence
	// (both functions and layers load unconditionally).
	onRegionChange(() => {
		selectedFunction = null;
		fnVersions = [];
		fnAliases = [];
		fnEsms = [];
		markerHistory = [];
		nextMarker = '';
		currentMarker = '';
		hasNextPage = false;
		layers = [];
		loadFunctions();
		loadLayers();
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
				<div class="flex items-center gap-2">
					<h1 class="text-3xl font-bold bg-gradient-to-r from-orange-600 to-amber-600 dark:from-orange-400 dark:to-amber-400 bg-clip-text text-transparent">Lambda Functions</h1>
					<LiveDot service="lambda" />
				</div>
				<p class="text-sm text-muted-foreground text-slate-500 dark:text-slate-400 mt-0.5">{functions.length} function{functions.length !== 1 ? 's' : ''}</p>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Deploy and run serverless code in response to events.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={() => loadFunctions()}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95"
				title="Refresh data"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button 
				onclick={() => showCreateModal = true}
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
			{ label: 'Go', value: functions.filter(f => (f.Runtime ?? '').includes('go')).length, color: 'text-cyan-500' }
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
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50 space-y-2">
					<div class="flex gap-2">
						<div class="relative flex-1">
							<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
							<input
								type="text"
								value={searchQuery}
								oninput={(e) => searchQueryParam.set(e.currentTarget.value)}
								placeholder="Search functions..."
								class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-orange-500 outline-none transition-all"
							/>
						</div>
						<select
							value={runtimeFilter}
							onchange={(e) => runtimeFilterParam.set(e.currentTarget.value)}
							class="px-3 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-orange-500 outline-none transition-all"
						>
							<option value="">All runtimes</option>
							<option value="nodejs">Node.js</option>
							<option value="python">Python</option>
							<option value="java">Java</option>
							<option value="go">Go</option>
							<option value="ruby">Ruby</option>
							<option value="dotnet">dotnet</option>
							<option value="provided">Custom</option>
						</select>
					</div>
				</div>

				<div class="overflow-x-auto">
					<table class="w-full text-left border-collapse">
						<thead>
							<tr class="bg-slate-50/50 dark:bg-slate-900/20">
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Function Details</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Runtime</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Memory</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Timeout</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider">Last Modified</th>
								<th class="px-6 py-4 text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wider text-right">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
							{#if loading && !functions.length}
								{#each Array(3) as _}
									<tr class="animate-pulse">
										<td colspan="6" class="px-6 py-8"><div class="h-12 bg-slate-200/50 dark:bg-slate-700/30 rounded-xl w-full"></div></td>
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
													<div class="flex items-center gap-2">
														<a
															href={`/dashboard/lambda/function?name=${encodeURIComponent(func.FunctionName ?? '')}`}
															class="font-bold text-slate-900 dark:text-white hover:text-orange-600 dark:hover:text-orange-400"
															onclick={(e) => e.stopPropagation()}
														>
															{func.FunctionName}
														</a>
														{#if func.State}
															<span class="text-[10px] px-1.5 py-0.5 rounded-full font-medium {func.State === 'Active' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : func.State === 'Failed' ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400'}">
																{func.State}
															</span>
														{/if}
													</div>
													<div class="text-[10px] text-slate-400 font-mono truncate max-w-[200px]">{func.FunctionArn}</div>
												</div>
											</div>
										</td>
										<td class="px-6 py-4">
											<div class="flex items-center gap-2">
												<Code class="w-3 h-3 text-slate-400" />
												{#if func.PackageType === 'Image'}
													<span class="text-xs font-medium text-slate-600 dark:text-slate-300 bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-400 px-2 py-0.5 rounded-md">Container</span>
												{:else}
													<span class="text-xs font-medium text-slate-600 dark:text-slate-300 bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded-md">{func.Runtime ?? 'Custom'}</span>
												{/if}
											</div>
										</td>
										<td class="px-6 py-4 text-sm text-slate-500 dark:text-slate-400">{func.MemorySize ?? '-'} MB</td>
										<td class="px-6 py-4 text-sm text-slate-500 dark:text-slate-400">{func.Timeout ?? '-'}s</td>
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
										<td colspan="6" class="px-6 py-20 text-center">
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
			<!-- Pagination controls -->
			{#if hasNextPage || markerHistory.length > 0}
				<div class="flex items-center justify-between px-4 py-3 bg-white/30 dark:bg-slate-800/30 border border-white/20 dark:border-slate-700/50 rounded-xl">
					<button
						onclick={() => {
							const prev = markerHistory.pop();
							markerHistory = [...markerHistory];
							loadFunctions(prev ?? '');
						}}
						disabled={markerHistory.length === 0}
						class="px-4 py-2 text-sm font-medium rounded-lg bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 disabled:opacity-40 hover:bg-white dark:hover:bg-slate-700 transition-all"
					>
						← Previous
					</button>
					<button
						onclick={() => {
							markerHistory = [...markerHistory, currentMarker];
							loadFunctions(nextMarker);
						}}
						disabled={!hasNextPage}
						class="px-4 py-2 text-sm font-medium rounded-lg bg-orange-600 hover:bg-orange-700 text-white disabled:opacity-40 transition-all"
					>
						Next page →
					</button>
				</div>
			{/if}
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
							<div class="flex items-center justify-between mb-3">
								<h3 class="flex items-center gap-2 text-xs font-bold text-slate-500 dark:text-slate-400 uppercase tracking-wider">
									<Terminal class="w-3 h-3" />
									Environment Variables
								</h3>
								{#if !editingEnvVars}
									<button onclick={startEditEnvVars} class="text-xs text-orange-500 hover:text-orange-700">Edit</button>
								{/if}
							</div>
							{#if editingEnvVars}
								<div class="space-y-2 max-h-48 overflow-y-auto pr-2">
									{#each Object.entries(envVarDraft) as [k, v], i}
										<div class="flex gap-1">
											<input value={k} oninput={(e) => {
												// eslint-disable-next-line @typescript-eslint/no-explicit-any
												const newKey = (e.target as any).value;
												const entries = Object.entries(envVarDraft);
												entries[i] = [newKey, v];
												envVarDraft = Object.fromEntries(entries);
											}} class="flex-1 text-xs font-mono px-2 py-1 border rounded bg-white dark:bg-slate-700 border-slate-300 dark:border-slate-600" placeholder="KEY" />
											<input value={v} oninput={(e) => {
												// eslint-disable-next-line @typescript-eslint/no-explicit-any
												const newVal = (e.target as any).value;
												const entries = Object.entries(envVarDraft);
												entries[i] = [k, newVal];
												envVarDraft = Object.fromEntries(entries);
											}} class="flex-1 text-xs font-mono px-2 py-1 border rounded bg-white dark:bg-slate-700 border-slate-300 dark:border-slate-600" placeholder="value" />
											<button onclick={() => { const {[k]: _, ...rest} = envVarDraft; envVarDraft = rest; }} class="text-red-400 hover:text-red-600 px-1">×</button>
										</div>
									{/each}
									<button onclick={() => { envVarDraft = { ...envVarDraft, '': '' }; }} class="text-xs text-orange-500 hover:text-orange-700">+ Add variable</button>
								</div>
								<div class="flex gap-2 mt-2">
									<button onclick={saveEnvVars} disabled={savingEnvVars} class="flex-1 text-xs py-1.5 bg-orange-600 hover:bg-orange-700 text-white rounded-lg disabled:opacity-50">
										{savingEnvVars ? 'Saving…' : 'Save'}
									</button>
									<button onclick={() => editingEnvVars = false} class="flex-1 text-xs py-1.5 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">Cancel</button>
								</div>
							{:else if selectedFunction.Environment?.Variables}
								<div class="space-y-2 max-h-48 overflow-y-auto pr-2">
									{#each Object.entries(selectedFunction.Environment.Variables) as [key, value]}
										<div class="p-2 bg-slate-900/5 dark:bg-slate-900/40 rounded-lg border border-slate-200/50 dark:border-slate-700/50 flex flex-col">
											<span class="text-[10px] text-slate-400 uppercase font-mono">{key}</span>
											<span class="text-xs font-medium text-slate-700 dark:text-white line-clamp-2">{value}</span>
										</div>
									{/each}
								</div>
							{:else}
								<p class="text-xs text-slate-400 italic">No environment variables defined. <button class="text-orange-500 hover:underline" onclick={startEditEnvVars}>Add one</button></p>
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
					<!-- Function sub-tabs -->
					<div class="border-t border-slate-200 dark:border-slate-700/50">
						<div class="flex overflow-x-auto">
							{#each [['code', 'Code'], ['versions', 'Versions'], ['aliases', 'Aliases'], ['triggers', 'Triggers']] as [id, label]}
								<button
									onclick={() => {
										fnDetailTab = id as typeof fnDetailTab;
										if (id === 'versions' && selectedFunction?.FunctionName) loadVersions(selectedFunction.FunctionName);
										if (id === 'aliases' && selectedFunction?.FunctionName) loadAliases(selectedFunction.FunctionName);
										if (id === 'triggers' && selectedFunction?.FunctionName) loadEsms(selectedFunction.FunctionName);
									}}
									class="px-4 py-3 text-xs font-medium whitespace-nowrap border-b-2 transition-colors {fnDetailTab === id ? 'border-orange-500 text-orange-600 dark:text-orange-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-300'}"
								>{label}</button>
							{/each}
						</div>
						<div class="p-4">
							{#if fnDetailTab === 'code'}
								<h4 class="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">Update Function Code</h4>
								<div class="flex gap-1 p-1 bg-slate-100 dark:bg-slate-900 rounded-lg mb-3">
									<button onclick={() => updateCodeMode = 'image'} class="flex-1 py-1 text-xs font-semibold rounded-md transition-all {updateCodeMode === 'image' ? 'bg-white dark:bg-slate-700 text-orange-600 shadow' : 'text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}">Container Image</button>
									<button onclick={() => updateCodeMode = 'zip'} class="flex-1 py-1 text-xs font-semibold rounded-md transition-all {updateCodeMode === 'zip' ? 'bg-white dark:bg-slate-700 text-orange-600 shadow' : 'text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}">ZIP Upload</button>
								</div>
								{#if updateCodeMode === 'image'}
									<div class="flex gap-2">
										<input type="text" bind:value={updateCodeImageUri} placeholder="public.ecr.aws/lambda/python:3.12" class="flex-1 text-xs font-mono border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white" />
										<button onclick={updateFunctionCode} disabled={updatingCode || !updateCodeImageUri.trim()} class="text-white bg-orange-600 hover:bg-orange-700 font-medium rounded-lg text-xs px-3 py-2 disabled:opacity-50">{updatingCode ? 'Updating…' : 'Update'}</button>
									</div>
								{:else}
									<div class="space-y-2">
										<input type="file" accept=".zip,application/zip" onchange={(e) => { const f = (e.target as HTMLInputElement).files?.[0]; updateCodeZipFile = f ?? null; }} class="w-full text-xs file:mr-2 file:py-1 file:px-3 file:rounded-lg file:border-0 file:bg-orange-100 file:text-orange-700 dark:file:bg-orange-900/30 dark:file:text-orange-300 text-slate-500 dark:text-slate-400" />
										{#if updateCodeZipFile}<p class="text-[10px] text-slate-400">{updateCodeZipFile.name} ({(updateCodeZipFile.size / 1024).toFixed(1)} KB)</p>{/if}
										<button onclick={updateFunctionCode} disabled={updatingCode || !updateCodeZipFile} class="w-full text-white bg-orange-600 hover:bg-orange-700 font-medium rounded-lg text-xs px-3 py-2 disabled:opacity-50">{updatingCode ? 'Uploading…' : 'Upload ZIP'}</button>
									</div>
								{/if}
							{:else if fnDetailTab === 'versions'}
								{#if versionsLoading}
									<p class="text-xs text-slate-400 animate-pulse">Loading…</p>
								{:else}
									<div class="space-y-2 mb-3">
										{#each fnVersions as v}
											<div class="flex items-center justify-between p-2 bg-slate-50 dark:bg-slate-900/40 rounded-lg border border-slate-200 dark:border-slate-700/50 text-xs">
												<span class="font-mono font-bold text-slate-700 dark:text-slate-300">v{v.Version}</span>
												<span class="text-slate-400">{v.LastModified ? new Date(v.LastModified).toLocaleDateString() : '—'}</span>
												{#if v.Description}<span class="text-slate-500 truncate max-w-[100px]">{v.Description}</span>{/if}
											</div>
										{/each}
										{#if fnVersions.length === 0}<p class="text-xs text-slate-400">No versions published yet.</p>{/if}
									</div>
									<div class="flex gap-2 items-end">
										<input type="text" bind:value={publishDesc} placeholder="Version description (optional)" class="flex-1 text-xs border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white" />
										<button onclick={publishVersion} disabled={publishing} class="text-white bg-orange-600 hover:bg-orange-700 font-medium rounded-lg text-xs px-3 py-2 disabled:opacity-50">{publishing ? 'Publishing…' : 'Publish'}</button>
									</div>
								{/if}
							{:else if fnDetailTab === 'aliases'}
								{#if aliasesLoading}
									<p class="text-xs text-slate-400 animate-pulse">Loading…</p>
								{:else}
									<div class="space-y-2 mb-3">
										{#each fnAliases as alias}
											<div class="flex items-center justify-between p-2 bg-slate-50 dark:bg-slate-900/40 rounded-lg border border-slate-200 dark:border-slate-700/50 text-xs">
												<span class="font-mono font-bold text-slate-700 dark:text-slate-300">{alias.Name}</span>
												<span class="text-slate-400">→ v{alias.FunctionVersion}</span>
												<button onclick={() => alias.Name && deleteAlias(alias.Name)} class="text-red-400 hover:text-red-600">Delete</button>
											</div>
										{/each}
										{#if fnAliases.length === 0}<p class="text-xs text-slate-400">No aliases.</p>{/if}
									</div>
									<div class="flex gap-2 items-end flex-wrap">
										<input type="text" bind:value={newAliasName} placeholder="Alias name (e.g. live)" class="text-xs border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white w-28" />
										<input type="text" bind:value={newAliasFnVersion} placeholder="Version" class="text-xs border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white w-20" />
										<button onclick={createAlias} disabled={creatingAlias} class="text-white bg-orange-600 hover:bg-orange-700 font-medium rounded-lg text-xs px-3 py-2 disabled:opacity-50">{creatingAlias ? 'Creating…' : 'Create'}</button>
									</div>
								{/if}
							{:else if fnDetailTab === 'triggers'}
								{#if esmsLoading}
									<p class="text-xs text-slate-400 animate-pulse">Loading…</p>
								{:else}
									<div class="space-y-2 mb-3">
										{#each fnEsms as esm}
											<div class="p-2 bg-slate-50 dark:bg-slate-900/40 rounded-lg border border-slate-200 dark:border-slate-700/50 text-xs">
												<div class="flex items-center justify-between">
													<span class="font-mono text-slate-700 dark:text-slate-300 truncate max-w-[160px]" title={esm.EventSourceArn}>{esm.EventSourceArn?.split(':').pop() ?? esm.UUID}</span>
													<span class="ml-2 px-1.5 py-0.5 rounded-full {esm.State === 'Enabled' ? 'bg-green-100 text-green-700' : 'bg-slate-100 text-slate-500'}">{esm.State}</span>
													<button onclick={() => esm.UUID && deleteEsm(esm.UUID)} class="ml-2 text-red-400 hover:text-red-600">Remove</button>
												</div>
												<p class="text-slate-400 mt-0.5">Batch: {esm.BatchSize}</p>
											</div>
										{/each}
										{#if fnEsms.length === 0}<p class="text-xs text-slate-400">No event source mappings.</p>{/if}
									</div>
									<div class="space-y-2">
										<input type="text" bind:value={newEsmEventArn} placeholder="Event source ARN (SQS/Kinesis/DynamoDB)" class="w-full text-xs font-mono border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white" />
										<div class="flex gap-2 items-center">
											<label class="text-xs text-slate-500" for="new-esm-batch-size">Batch size:</label>
											<input id="new-esm-batch-size" type="number" bind:value={newEsmBatchSize} min="1" max="10000" class="w-20 text-xs border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white" />
											<button onclick={createEsm} disabled={creatingEsm || !newEsmEventArn.trim()} class="text-white bg-orange-600 hover:bg-orange-700 font-medium rounded-lg text-xs px-3 py-2 disabled:opacity-50">{creatingEsm ? 'Adding…' : 'Add Trigger'}</button>
										</div>
									</div>
								{/if}
							{/if}
						</div>
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

	<!-- Invocation History -->
	{#if invocationHistory.length > 0}
		<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6">
			<h2 class="text-sm font-bold text-slate-500 dark:text-slate-400 uppercase tracking-wider mb-4 flex items-center gap-2">
				<Terminal class="w-4 h-4" /> Invocation History
			</h2>
			<div class="space-y-2 max-h-48 overflow-y-auto">
				{#each invocationHistory as inv}
					<div class="flex items-center justify-between p-2 bg-slate-50 dark:bg-slate-900/40 rounded-lg border border-slate-200 dark:border-slate-700/50 text-xs">
						<span class="font-medium text-slate-900 dark:text-white">{inv.functionName}</span>
						<span class="font-mono text-slate-400">{inv.timestamp.toLocaleTimeString()}</span>
						<span class="px-2 py-0.5 rounded-full {(inv.statusCode ?? 0) < 300 ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'}">{inv.statusCode ?? '—'}</span>
					</div>
				{/each}
			</div>
		</div>
	{/if}

	<!-- Layer Management -->
	<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6">
		<div class="flex items-center justify-between mb-4">
			<h2 class="text-sm font-bold text-slate-500 dark:text-slate-400 uppercase tracking-wider flex items-center gap-2">
				<Code class="w-4 h-4" /> Lambda Layers
			</h2>
			<button onclick={() => { showLayerTab = !showLayerTab; if (showLayerTab && layers.length === 0) loadLayers(); }} class="text-xs text-orange-500 hover:text-orange-700">
				{showLayerTab ? 'Hide' : 'Show Layers'}
			</button>
		</div>
		{#if showLayerTab}
			{#if layersLoading}
				<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-orange-500"></div></div>
			{:else if layers.length === 0}
				<p class="text-xs text-slate-400 italic text-center py-4">No layers found</p>
			{:else}
				<div class="space-y-2">
					{#each layers as layer}
						<div class="p-3 bg-slate-50 dark:bg-slate-900/40 rounded-xl border border-slate-200 dark:border-slate-700/50">
							<div class="flex items-center justify-between">
								<span class="text-sm font-medium text-slate-900 dark:text-white">{layer.LayerName}</span>
								<span class="text-xs text-slate-400">v{layer.LatestMatchingVersion?.Version}</span>
							</div>
							{#if layer.LatestMatchingVersion?.Description}
								<p class="text-xs text-slate-500 mt-0.5">{layer.LatestMatchingVersion.Description}</p>
							{/if}
							<p class="text-xs text-slate-400 font-mono mt-1 truncate">{layer.LayerArn}</p>
						</div>
					{/each}
				</div>
			{/if}
		{/if}
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
				<div class="space-y-3">
					<!-- Invocation Type Selector -->
					<div>
						<p class="text-xs font-bold text-slate-500 uppercase tracking-widest mb-2">Invocation Type</p>
						<div class="flex gap-1 p-1 bg-slate-100 dark:bg-slate-900 rounded-xl">
							{#each (['RequestResponse', 'Event', 'DryRun'] as const) as t}
								<button
									onclick={() => invokeType = t}
									class="flex-1 py-1.5 text-xs font-semibold rounded-lg transition-all {invokeType === t ? 'bg-white dark:bg-slate-700 text-orange-600 dark:text-orange-400 shadow' : 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
								>{t}</button>
							{/each}
						</div>
					</div>
					<label for="lambda-invoke-payload" class="text-xs font-bold text-slate-500 uppercase tracking-widest">Input Payload (JSON)</label>
					<div class="relative group">
						<textarea 
							id="lambda-invoke-payload"
							bind:value={invokePayload}
							class="w-full h-64 p-4 font-mono text-sm bg-slate-50 dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 focus:ring-2 focus:ring-orange-500 outline-none transition-all resize-none shadow-inner"
						></textarea>
						<div class="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity">
							<Terminal class="w-4 h-4 text-slate-300" />
						</div>
					</div>
				</div>

				<!-- Output -->
				<div class="space-y-2">
					<p class="text-xs font-bold text-slate-500 uppercase tracking-widest">Execution Response</p>
					<div class="h-64 w-full p-4 font-mono text-sm bg-slate-950 text-emerald-400 rounded-xl overflow-auto shadow-inner border border-black group relative">
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
					{#if invokeResponse?.LogResult}
						<div>
							<p class="text-xs font-bold text-slate-500 uppercase tracking-widest mb-1">Log Output</p>
							<pre class="w-full p-3 font-mono text-xs bg-slate-950 text-slate-300 rounded-xl overflow-auto border border-black whitespace-pre-wrap max-h-32">{decodeLogResult(invokeResponse.LogResult)}</pre>
						</div>
					{/if}
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

<!-- Create Function Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div role="none" onclick={() => showCreateModal = false} onkeydown={(e) => e.key === 'Escape' && (showCreateModal = false)} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"></div>
		<div class="relative w-full max-w-lg bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700 overflow-hidden">
			<div class="flex items-center justify-between p-6 border-b border-slate-200 dark:border-slate-700/50">
				<div>
					<h3 class="text-xl font-bold text-slate-900 dark:text-white">Create Function</h3>
					<p class="text-xs text-slate-500">Configure a new Lambda function.</p>
				</div>
				<button onclick={() => showCreateModal = false} class="p-2 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-lg transition-colors">
					<X class="w-5 h-5 text-slate-400" />
				</button>
			</div>
			<div class="p-6 space-y-4">
				<div>
					<label class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1" for="create-fn-name">Function Name *</label>
					<input id="create-fn-name" type="text" bind:value={newFnName} placeholder="my-function" class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none" />
				</div>
				<div>
					<label class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1" for="create-fn-runtime">Runtime</label>
					<select id="create-fn-runtime" bind:value={newFnRuntime} class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none">
						{#each ['nodejs22.x', 'python3.12', 'python3.11', 'java21', 'go1.x', 'ruby3.3', 'dotnet8'] as rt}
							<option value={rt}>{rt}</option>
						{/each}
					</select>
				</div>
				<div>
					<label class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1" for="create-fn-role">IAM Role ARN</label>
					<input id="create-fn-role" type="text" bind:value={newFnRole} class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none" />
				</div>
				<div>
					<label class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1" for="create-fn-handler">Handler</label>
					<input id="create-fn-handler" type="text" bind:value={newFnHandler} class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none" />
				</div>
				<div class="grid grid-cols-2 gap-3">
					<div>
						<label class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1" for="create-fn-memory">Memory (MB)</label>
						<input id="create-fn-memory" type="number" bind:value={newFnMemory} min="128" max="10240" step="64" class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none" />
					</div>
					<div>
						<label class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1" for="create-fn-timeout">Timeout (s)</label>
						<input id="create-fn-timeout" type="number" bind:value={newFnTimeout} min="1" max="900" class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none" />
					</div>
				</div>
				<div>
					<label class="block text-xs font-bold text-slate-500 uppercase tracking-widest mb-1" for="create-fn-description">Description</label>
					<input id="create-fn-description" type="text" bind:value={newFnDescription} placeholder="Optional description" class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none" />
				</div>
			</div>
			<div class="p-6 bg-slate-50 dark:bg-slate-900/50 border-t border-slate-200 dark:border-slate-700/50 flex justify-end gap-3">
				<button onclick={() => showCreateModal = false} class="px-5 py-2.5 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 text-slate-700 dark:text-slate-300 rounded-xl font-medium hover:bg-slate-50 transition-all">Cancel</button>
				<button onclick={createFunction} disabled={creating} class="flex items-center gap-2 px-8 py-2.5 bg-orange-600 hover:bg-orange-700 text-white rounded-xl font-bold shadow-lg shadow-orange-600/20 disabled:opacity-50 transition-all active:scale-[0.98]">
					{#if creating}
						<RefreshCw class="w-4 h-4 animate-spin" />
						Creating...
					{:else}
						<Plus class="w-4 h-4" />
						Create
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
