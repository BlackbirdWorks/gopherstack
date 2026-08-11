<script lang="ts">
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { currentRegion } from '$lib/region.svelte';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import {
		GetFunctionCommand,
		UpdateFunctionConfigurationCommand,
		ListEventSourceMappingsCommand,
		CreateEventSourceMappingCommand,
		DeleteEventSourceMappingCommand,
		ListVersionsByFunctionCommand,
		PublishVersionCommand,
		ListAliasesCommand,
		CreateAliasCommand,
		DeleteAliasCommand,
		GetFunctionUrlConfigCommand,
		CreateFunctionUrlConfigCommand,
		DeleteFunctionUrlConfigCommand,
		DeleteFunctionCommand,
		InvokeCommand,
		type GetFunctionCommandOutput,
		type EventSourceMappingConfiguration,
		type AliasConfiguration,
		type FunctionConfiguration,
		type GetFunctionUrlConfigResponse,
		type InvocationResponse
	} from '@aws-sdk/client-lambda';
	import { getLambdaClient } from '$lib/aws-client';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { Play, Trash2, RefreshCw, X } from 'lucide-svelte';

	const lambda = regionalClient(getLambdaClient);

	let loading = $state(false);
	let errorMessage = $state('');
	let functionName = $derived(page.url.searchParams.get('name') ?? '');
	let response = $state<GetFunctionCommandOutput | null>(null);

	// Environment Variables
	let envVars = $state<{ key: string; value: string }[]>([]);
	let savingEnv = $state(false);

	// Event Source Mappings
	let esms = $state<EventSourceMappingConfiguration[]>([]);
	let loadingEsms = $state(false);
	let newEsmArn = $state('');
	let newEsmBatchSize = $state(10);
	let newEsmEnabled = $state(true);
	let addingEsm = $state(false);

	// Versions & Aliases
	let versions = $state<FunctionConfiguration[]>([]);
	let aliases = $state<AliasConfiguration[]>([]);
	let publishingVersion = $state(false);
	let versionDescription = $state('');
	let showVersions = $state(false);
	let showCreateAliasModal = $state(false);
	let newAliasName = $state('');
	let newAliasVersion = $state('$LATEST');
	let creatingAlias = $state(false);

	// Function URL
	let functionURL = $state<GetFunctionUrlConfigResponse | null>(null);
	let loadingURL = $state(false);

	// Invoke
	let showInvokeModal = $state(false);
	let invokePayload = $state('{\n  "key": "value"\n}');
	let invoking = $state(false);
	let invokeResponse = $state<InvocationResponse | null>(null);
	let invokeType = $state<'RequestResponse' | 'Event' | 'DryRun'>('RequestResponse');

	async function loadFunction(): Promise<void> {
		if (!functionName) {
			errorMessage = 'Function name is required';
			return;
		}
		loading = true;
		errorMessage = '';
		try {
			response = await lambda().send(new GetFunctionCommand({ FunctionName: functionName }));
			const vars = response.Configuration?.Environment?.Variables ?? {};
			envVars = Object.entries(vars).map(([key, value]) => ({ key, value }));
		} catch (err) {
			errorMessage = err instanceof Error ? err.message : 'Failed to load function details';
			response = null;
		} finally {
			loading = false;
		}
	}

	async function loadESMs(): Promise<void> {
		loadingEsms = true;
		try {
			const res = await lambda().send(new ListEventSourceMappingsCommand({ FunctionName: functionName }));
			esms = res.EventSourceMappings ?? [];
		} catch {
			esms = [];
		} finally {
			loadingEsms = false;
		}
	}

	async function loadVersions(): Promise<void> {
		try {
			const res = await lambda().send(new ListVersionsByFunctionCommand({ FunctionName: functionName }));
			versions = res.Versions ?? [];
		} catch {
			versions = [];
		}
	}

	async function loadAliases(): Promise<void> {
		try {
			const res = await lambda().send(new ListAliasesCommand({ FunctionName: functionName }));
			aliases = res.Aliases ?? [];
		} catch {
			aliases = [];
		}
	}

	async function createAlias(): Promise<void> {
		if (!newAliasName.trim()) { toast.error('Alias name is required'); return; }
		creatingAlias = true;
		try {
			await lambda().send(new CreateAliasCommand({
				FunctionName: functionName,
				Name: newAliasName.trim(),
				FunctionVersion: newAliasVersion || '$LATEST',
			}));
			toast.success(`Alias "${newAliasName.trim()}" created`);
			showCreateAliasModal = false;
			newAliasName = '';
			newAliasVersion = '$LATEST';
			await loadAliases();
		} catch (err) {
			toast.error(`Failed to create alias: ${(err as Error).message}`);
		} finally {
			creatingAlias = false;
		}
	}

	async function deleteAlias(aliasName: string): Promise<void> {
		try {
			await lambda().send(new DeleteAliasCommand({ FunctionName: functionName, Name: aliasName }));
			toast.success(`Alias "${aliasName}" deleted`);
			await loadAliases();
		} catch (err) {
			toast.error(`Failed to delete alias: ${(err as Error).message}`);
		}
	}

	async function loadFunctionURL(): Promise<void> {
		loadingURL = true;
		try {
			const res = await lambda().send(new GetFunctionUrlConfigCommand({ FunctionName: functionName }));
			functionURL = res;
		} catch {
			functionURL = null;
		} finally {
			loadingURL = false;
		}
	}

	async function saveEnvVars(): Promise<void> {
		savingEnv = true;
		try {
			const variables: Record<string, string> = {};
			for (const { key, value } of envVars) {
				if (key.trim()) variables[key.trim()] = value;
			}
			await lambda().send(new UpdateFunctionConfigurationCommand({
				FunctionName: functionName,
				Environment: { Variables: variables },
			}));
			toast.success('Environment variables saved');
		} catch (err) {
			toast.error(`Failed to save env vars: ${(err as Error).message}`);
		} finally {
			savingEnv = false;
		}
	}

	async function addESM(): Promise<void> {
		if (!newEsmArn.trim()) { toast.error('EventSourceARN is required'); return; }
		addingEsm = true;
		try {
			await lambda().send(new CreateEventSourceMappingCommand({
				FunctionName: functionName,
				EventSourceArn: newEsmArn.trim(),
				BatchSize: newEsmBatchSize,
				Enabled: newEsmEnabled,
				StartingPosition: 'TRIM_HORIZON',
			}));
			toast.success('Event source mapping added');
			newEsmArn = '';
			await loadESMs();
		} catch (err) {
			toast.error(`Failed to add ESM: ${(err as Error).message}`);
		} finally {
			addingEsm = false;
		}
	}

	async function deleteESM(uuid: string): Promise<void> {
		try {
			await lambda().send(new DeleteEventSourceMappingCommand({ UUID: uuid }));
			toast.success('ESM deleted');
			await loadESMs();
		} catch (err) {
			toast.error(`Failed to delete ESM: ${(err as Error).message}`);
		}
	}

	async function publishVersion(): Promise<void> {
		publishingVersion = true;
		try {
			await lambda().send(new PublishVersionCommand({ FunctionName: functionName, Description: versionDescription }));
			toast.success('Version published');
			versionDescription = '';
			await loadVersions();
		} catch (err) {
			toast.error(`Failed to publish version: ${(err as Error).message}`);
		} finally {
			publishingVersion = false;
		}
	}

	async function createFunctionURL(): Promise<void> {
		try {
			await lambda().send(new CreateFunctionUrlConfigCommand({ FunctionName: functionName, AuthType: 'NONE' }));
			toast.success('Function URL created');
			await loadFunctionURL();
		} catch (err) {
			toast.error(`Failed to create Function URL: ${(err as Error).message}`);
		}
	}

	async function deleteFunctionURL(): Promise<void> {
		try {
			await lambda().send(new DeleteFunctionUrlConfigCommand({ FunctionName: functionName }));
			toast.success('Function URL deleted');
			functionURL = null;
		} catch (err) {
			toast.error(`Failed to delete Function URL: ${(err as Error).message}`);
		}
	}

	async function invokeFunction(): Promise<void> {
		invoking = true;
		invokeResponse = null;
		try {
			const payload = new TextEncoder().encode(invokePayload);
			const res = await lambda().send(new InvokeCommand({
				FunctionName: functionName,
				InvocationType: invokeType,
				LogType: 'Tail',
				Payload: payload
			}));
			invokeResponse = res;
			if (res.StatusCode === 200 || res.StatusCode === 202) {
				toast.success(`Successfully invoked ${functionName}`);
			} else {
				toast.warning(`Invoked with status ${res.StatusCode}`);
			}
		} catch (err) {
			toast.error(`Invocation failed: ${(err as Error).message}`);
		} finally {
			invoking = false;
		}
	}

	async function deleteFunction(): Promise<void> {
		if (!await confirmDestructive({ title: 'Delete Function', message: `Delete function "${functionName}"? This action cannot be undone.` })) return;
		try {
			await lambda().send(new DeleteFunctionCommand({ FunctionName: functionName }));
			toast.success(`Function "${functionName}" deleted`);
			await goto('/dashboard/lambda');
		} catch (err) {
			toast.error(`Delete failed: ${(err as Error).message}`);
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

	onRegionChange(() => {
		void loadFunction();
		void loadESMs();
		void loadVersions();
		void loadAliases();
		void loadFunctionURL();
	});
</script>

<div class="space-y-6">
	<!-- Header with back link -->
	<div class="flex items-center justify-between p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div>
			<a href="/dashboard/lambda" class="text-sm text-orange-600 dark:text-orange-400 hover:underline mb-1 flex items-center gap-1">← Back to Lambda Functions</a>
			<h1 class="text-3xl font-bold bg-gradient-to-r from-orange-600 to-amber-600 dark:from-orange-400 dark:to-amber-400 bg-clip-text text-transparent">Lambda Function Details</h1>
			<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Inspect runtime configuration and package information.</p>
		</div>
		<div class="flex items-center gap-3">
			<a href="/dashboard/lambda" class="px-4 py-2 rounded-xl bg-white/60 dark:bg-slate-700/60 border border-slate-200 dark:border-slate-600 text-sm font-medium hover:bg-white dark:hover:bg-slate-700">
				Back to functions
			</a>
			{#if response?.Configuration}
				<button
					onclick={() => showInvokeModal = true}
					class="flex items-center gap-2 px-4 py-2 bg-teal-600 hover:bg-teal-700 text-white rounded-xl font-medium shadow-lg shadow-teal-600/20 transition-all active:scale-95"
				>
					<Play class="w-4 h-4" />
					Invoke
				</button>
				<button
					onclick={deleteFunction}
					class="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-xl font-medium shadow-lg shadow-red-600/20 transition-all active:scale-95"
				>
					<Trash2 class="w-4 h-4" />
					Delete Function
				</button>
			{/if}
		</div>
	</div>

	<!-- Config Section -->
	<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6 space-y-4">
		{#if loading}
			<p class="text-slate-500 dark:text-slate-400">Loading function details...</p>
		{:else if errorMessage}
			<p class="text-red-600 dark:text-red-400">{errorMessage}</p>
		{:else if response?.Configuration}
			<div class="flex items-center gap-2">
				<h2 class="text-2xl font-bold text-slate-900 dark:text-white">{response.Configuration.FunctionName}</h2>
				<RegionChip region={currentRegion()} />
			</div>
			{#if response.Configuration.Description}
				<p class="text-sm text-slate-500 dark:text-slate-400 italic">{response.Configuration.Description}</p>
			{/if}
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20">
					<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Package Type</p>
					<p class="text-base font-semibold text-slate-900 dark:text-white">{response.Configuration.PackageType ?? 'Unknown'}</p>
				</div>
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20">
					<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Runtime</p>
					<p class="text-base font-semibold text-slate-900 dark:text-white">{response.Configuration.Runtime ?? '-'}</p>
				</div>
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20">
					<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Handler</p>
					<p class="text-base font-semibold text-slate-900 dark:text-white">{response.Configuration.Handler ?? '-'}</p>
				</div>
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20">
					<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Memory / Timeout</p>
					<p class="text-base font-semibold text-slate-900 dark:text-white">{response.Configuration.MemorySize ?? 0} MB / {response.Configuration.Timeout ?? 0}s</p>
				</div>
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20">
					<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Version</p>
					<p class="text-base font-semibold text-slate-900 dark:text-white">{response.Configuration.Version ?? '$LATEST'}</p>
				</div>
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20">
					<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Code Size</p>
					<p class="text-base font-semibold text-slate-900 dark:text-white">{((response.Configuration.CodeSize ?? 0) / 1024).toFixed(1)} KB</p>
				</div>
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20">
					<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Layers</p>
					<p class="text-base font-semibold text-slate-900 dark:text-white">{response.Configuration.Layers?.length ?? 0}</p>
				</div>
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20 md:col-span-2">
					<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Role ARN</p>
					<p class="text-sm font-semibold text-slate-900 dark:text-white truncate">{response.Configuration.Role ?? '-'}</p>
				</div>
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20">
					<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Architecture</p>
					<p class="text-sm font-semibold text-slate-900 dark:text-white">{(response.Configuration.Architectures ?? ['x86_64']).join(', ')}</p>
				</div>
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20">
					<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Ephemeral Storage</p>
					<p class="text-sm font-semibold text-slate-900 dark:text-white">{response.Configuration.EphemeralStorage?.Size ?? 512} MB</p>
				</div>
				{#if response.Configuration.CodeSha256}
					<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20 md:col-span-2" title={response.Configuration.CodeSha256}>
						<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Code SHA256</p>
						<p class="text-sm font-semibold text-slate-900 dark:text-white font-mono truncate">{response.Configuration.CodeSha256.slice(0, 12)}…</p>
					</div>
				{/if}
				{#if response.Configuration.Description}
					<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-white/40 dark:bg-slate-900/20 md:col-span-3">
						<p class="text-xs uppercase tracking-wide text-slate-500 dark:text-slate-400">Description</p>
						<p class="text-sm font-semibold text-slate-900 dark:text-white">{response.Configuration.Description}</p>
					</div>
				{/if}
			</div>
		{:else}
			<p class="text-slate-500 dark:text-slate-400">Function not found.</p>
		{/if}
	</div>

	<!-- Environment Variables -->
	{#if response?.Configuration}
		<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6 space-y-4">
			<h2 class="text-lg font-bold text-slate-900 dark:text-white">Environment Variables</h2>
			<div class="space-y-2">
				{#each envVars as ev, i}
					<div class="flex gap-2 items-center">
						<input type="text" bind:value={ev.key} placeholder="KEY" class="flex-1 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm font-mono focus:ring-2 focus:ring-orange-500 outline-none" />
						<input type="text" bind:value={ev.value} placeholder="VALUE" class="flex-2 px-3 py-1.5 rounded-lg border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm font-mono focus:ring-2 focus:ring-orange-500 outline-none w-full" />
						<button onclick={() => envVars.splice(i, 1)} class="p-1.5 text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-lg transition-colors" title="Remove">×</button>
					</div>
				{/each}
			</div>
			<div class="flex gap-3">
				<button onclick={() => envVars.push({ key: '', value: '' })} class="px-4 py-2 text-sm border border-slate-200 dark:border-slate-600 rounded-xl hover:bg-slate-50 dark:hover:bg-slate-700 transition-colors">+ Add variable</button>
				<button onclick={saveEnvVars} disabled={savingEnv} class="px-4 py-2 text-sm bg-orange-600 hover:bg-orange-700 text-white rounded-xl font-medium disabled:opacity-50 transition-all">
					{savingEnv ? 'Saving...' : 'Save'}
				</button>
			</div>
		</div>

		<!-- Event Source Mappings -->
		<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6 space-y-4">
			<h2 class="text-lg font-bold text-slate-900 dark:text-white">Event Source Mappings</h2>
			{#if loadingEsms}
				<p class="text-sm text-slate-400">Loading...</p>
			{:else if esms.length > 0}
				<div class="overflow-x-auto">
					<table class="w-full text-sm text-left">
						<thead>
							<tr class="text-xs uppercase text-slate-500 border-b border-slate-200 dark:border-slate-700">
								<th class="py-2 pr-4">Source ARN</th>
								<th class="py-2 pr-4">State</th>
								<th class="py-2 pr-4">Batch Size</th>
								<th class="py-2"></th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700/50">
							{#each esms as esm}
								<tr class="hover:bg-slate-50/50 dark:hover:bg-slate-700/20">
									<td class="py-2 pr-4 font-mono text-xs truncate max-w-xs">{esm.EventSourceArn ?? '-'}</td>
									<td class="py-2 pr-4"><span class="px-2 py-0.5 rounded-full text-xs {esm.State === 'Enabled' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-slate-100 text-slate-500 dark:bg-slate-700 dark:text-slate-400'}">{esm.State}</span></td>
									<td class="py-2 pr-4">{esm.BatchSize ?? '-'}</td>
									<td class="py-2 text-right"><button onclick={() => deleteESM(esm.UUID ?? '')} class="p-1.5 text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 rounded-lg transition-colors text-xs">Delete</button></td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else}
				<p class="text-sm text-slate-400 italic">No event source mappings configured.</p>
			{/if}
			<!-- Add ESM -->
			<div class="border-t border-slate-200 dark:border-slate-700 pt-4 space-y-2">
				<p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Add Event Source</p>
				<div class="flex gap-2 flex-wrap">
					<input type="text" bind:value={newEsmArn} placeholder="arn:aws:kinesis:..." class="flex-1 min-w-[200px] px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none" />
					<input type="number" bind:value={newEsmBatchSize} min="1" max="10000" class="w-24 px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none" placeholder="Batch" />
					<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
						<input type="checkbox" bind:checked={newEsmEnabled} class="rounded" />
						Enabled
					</label>
					<button onclick={addESM} disabled={addingEsm} class="px-4 py-2 bg-orange-600 hover:bg-orange-700 text-white text-sm rounded-xl font-medium disabled:opacity-50 transition-all">
						{addingEsm ? 'Adding...' : 'Add'}
					</button>
				</div>
			</div>
		</div>

		<!-- Versions & Aliases -->
		<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
			<button
				onclick={() => showVersions = !showVersions}
				class="w-full flex items-center justify-between p-6 hover:bg-white/20 dark:hover:bg-slate-700/20 transition-colors"
			>
				<h2 class="text-lg font-bold text-slate-900 dark:text-white">Versions & Aliases</h2>
				<span class="text-slate-400 text-lg">{showVersions ? '▲' : '▼'}</span>
			</button>
			{#if showVersions}
				<div class="px-6 pb-6 space-y-4 border-t border-slate-200 dark:border-slate-700">
					<!-- Publish Version -->
					<div class="pt-4 flex gap-2 items-center">
						<input type="text" bind:value={versionDescription} placeholder="Version description (optional)" class="flex-1 px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none" />
						<button onclick={publishVersion} disabled={publishingVersion} class="px-4 py-2 bg-orange-600 hover:bg-orange-700 text-white text-sm rounded-xl font-medium disabled:opacity-50 transition-all">
							{publishingVersion ? 'Publishing...' : 'Publish Version'}
						</button>
					</div>
					<!-- Versions List -->
					{#if versions.length > 0}
						<div>
							<p class="text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">Published Versions</p>
							<div class="space-y-1">
								{#each versions as v}
									<div class="flex items-center justify-between p-2 bg-slate-50 dark:bg-slate-900/40 rounded-lg text-sm">
										<span class="font-mono font-bold text-orange-600 dark:text-orange-400">{v.Version}</span>
										<span class="text-slate-500 dark:text-slate-400 text-xs flex-1 px-3 truncate">{v.Description ?? ''}</span>
										<span class="text-xs text-slate-400">{v.LastModified ?? ''}</span>
									</div>
								{/each}
							</div>
						</div>
					{/if}
					<!-- Aliases List -->
					<div>
						<div class="flex items-center justify-between mb-2">
							<p class="text-xs font-bold text-slate-500 uppercase tracking-wider">Aliases</p>
							<button
								onclick={() => showCreateAliasModal = true}
								class="px-3 py-1 text-xs bg-orange-600 hover:bg-orange-700 text-white rounded-lg font-medium transition-all"
							>
								+ Create alias
							</button>
						</div>
						{#if aliases.length > 0}
							<div class="space-y-1">
								{#each aliases as a}
									<div class="flex items-center justify-between p-2 bg-slate-50 dark:bg-slate-900/40 rounded-lg text-sm">
										<span class="font-bold text-slate-900 dark:text-white">{a.Name}</span>
										<span class="font-mono text-xs text-orange-600 dark:text-orange-400 px-3">→ {a.FunctionVersion}</span>
										<span class="text-xs text-slate-400 flex-1 truncate">{a.Description ?? ''}</span>
										<button
											onclick={() => deleteAlias(a.Name ?? '')}
											class="p-1 text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 rounded text-xs ml-2"
											title="Delete alias"
										>
											<Trash2 class="w-3 h-3" />
										</button>
									</div>
								{/each}
							</div>
						{:else}
							<p class="text-xs text-slate-400 italic">No aliases defined.</p>
						{/if}
					</div>
				</div>
			{/if}
		</div>

		<!-- Function URL -->
		<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6 space-y-4">
			<h2 class="text-lg font-bold text-slate-900 dark:text-white">Function URL</h2>
			{#if loadingURL}
				<p class="text-sm text-slate-400">Loading...</p>
			{:else if functionURL?.FunctionUrl}
				<div class="flex items-center gap-3">
					<a href={functionURL.FunctionUrl} target="_blank" rel="noopener noreferrer" class="text-orange-600 dark:text-orange-400 hover:underline font-mono text-sm break-all">{functionURL.FunctionUrl}</a>
					<button onclick={deleteFunctionURL} class="px-3 py-1.5 text-xs bg-red-100 dark:bg-red-900/30 text-red-600 dark:text-red-400 rounded-lg hover:bg-red-200 dark:hover:bg-red-900/50 transition-colors font-medium">Delete</button>
				</div>
				<p class="text-xs text-slate-400">Auth type: {functionURL.AuthType ?? 'NONE'}</p>
			{:else}
				<p class="text-sm text-slate-400 italic mb-2">No Function URL configured.</p>
				<button onclick={createFunctionURL} class="px-4 py-2 bg-orange-600 hover:bg-orange-700 text-white text-sm rounded-xl font-medium transition-all">Create Function URL</button>
			{/if}
		</div>
	{/if}
</div>

<!-- Invoke Modal -->
{#if showInvokeModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
<div role="none" onclick={() => showInvokeModal = false} onkeydown={(e) => e.key === 'Escape' && (showInvokeModal = false)} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"></div>
<div class="relative w-full max-w-2xl bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700 overflow-hidden">
<div class="flex items-center justify-between p-6 border-b border-slate-200 dark:border-slate-700/50">
<div>
<h3 class="text-xl font-bold text-slate-900 dark:text-white">Invoke {functionName}</h3>
<p class="text-xs text-slate-500">Configure function payload and view response.</p>
</div>
<button onclick={() => showInvokeModal = false} class="p-2 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-lg transition-colors">
<X class="w-5 h-5 text-slate-400" />
</button>
</div>
<div class="p-6 grid grid-cols-1 md:grid-cols-2 gap-6">
<div class="space-y-3">
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
<label for="detail-invoke-payload" class="text-xs font-bold text-slate-500 uppercase tracking-widest">Input Payload (JSON)</label>
<textarea
id="detail-invoke-payload"
bind:value={invokePayload}
class="w-full h-56 p-4 font-mono text-sm bg-slate-50 dark:bg-slate-900 rounded-xl border border-slate-200 dark:border-slate-700 focus:ring-2 focus:ring-orange-500 outline-none transition-all resize-none shadow-inner"
></textarea>
</div>
<div class="space-y-2">
<p class="text-xs font-bold text-slate-500 uppercase tracking-widest">Execution Response</p>
<div class="h-56 w-full p-4 font-mono text-sm bg-slate-950 text-emerald-400 rounded-xl overflow-auto shadow-inner border border-black">
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

<!-- Create Alias Modal -->
{#if showCreateAliasModal}
<div class="fixed inset-0 bg-black/50 backdrop-blur-sm z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-slate-800 rounded-2xl shadow-2xl w-full max-w-md">
<div class="p-6 border-b border-slate-200 dark:border-slate-700">
<h2 class="text-xl font-bold text-slate-900 dark:text-white">Create Alias</h2>
</div>
<div class="p-6 space-y-4">
<div>
<label for="alias-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Alias name</label>
<input
id="alias-name"
type="text"
bind:value={newAliasName}
placeholder="e.g. live, staging"
class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none"
/>
</div>
<div>
<label for="alias-version" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Function version</label>
<input
id="alias-version"
type="text"
bind:value={newAliasVersion}
placeholder="$LATEST or version number"
class="w-full px-3 py-2 rounded-xl border border-slate-200 dark:border-slate-600 bg-white/50 dark:bg-slate-700/50 text-sm focus:ring-2 focus:ring-orange-500 outline-none"
/>
</div>
</div>
<div class="p-6 border-t border-slate-200 dark:border-slate-700 flex justify-end gap-3">
<button
onclick={() => { showCreateAliasModal = false; newAliasName = ''; newAliasVersion = '$LATEST'; }}
class="px-5 py-2.5 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 text-slate-700 dark:text-slate-300 rounded-xl font-medium hover:bg-slate-50 dark:hover:bg-slate-600 transition-all"
>
Cancel
</button>
<button
onclick={createAlias}
disabled={creatingAlias || !newAliasName.trim()}
class="px-5 py-2.5 bg-orange-600 hover:bg-orange-700 text-white rounded-xl font-medium disabled:opacity-50 transition-all"
>
{creatingAlias ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}
