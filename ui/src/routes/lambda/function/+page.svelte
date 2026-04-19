<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import {
		GetFunctionCommand,
		UpdateFunctionConfigurationCommand,
		ListEventSourceMappingsCommand,
		CreateEventSourceMappingCommand,
		DeleteEventSourceMappingCommand,
		ListVersionsByFunctionCommand,
		PublishVersionCommand,
		ListAliasesCommand,
		GetFunctionUrlConfigCommand,
		CreateFunctionUrlConfigCommand,
		DeleteFunctionUrlConfigCommand,
		type GetFunctionCommandOutput,
		type EventSourceMappingConfiguration,
		type AliasConfiguration,
		type FunctionConfiguration,
		type GetFunctionUrlConfigResponse
	} from '@aws-sdk/client-lambda';
	import { getLambdaClient } from '$lib/aws-client';
	import { toast } from 'svelte-sonner';

	const lambda = getLambdaClient();

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

	// Function URL
	let functionURL = $state<GetFunctionUrlConfigResponse | null>(null);
	let loadingURL = $state(false);

	async function loadFunction(): Promise<void> {
		if (!functionName) {
			errorMessage = 'Function name is required';
			return;
		}
		loading = true;
		errorMessage = '';
		try {
			response = await lambda.send(new GetFunctionCommand({ FunctionName: functionName }));
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
			const res = await lambda.send(new ListEventSourceMappingsCommand({ FunctionName: functionName }));
			esms = res.EventSourceMappings ?? [];
		} catch {
			esms = [];
		} finally {
			loadingEsms = false;
		}
	}

	async function loadVersions(): Promise<void> {
		try {
			const res = await lambda.send(new ListVersionsByFunctionCommand({ FunctionName: functionName }));
			versions = res.Versions ?? [];
		} catch {
			versions = [];
		}
	}

	async function loadAliases(): Promise<void> {
		try {
			const res = await lambda.send(new ListAliasesCommand({ FunctionName: functionName }));
			aliases = res.Aliases ?? [];
		} catch {
			aliases = [];
		}
	}

	async function loadFunctionURL(): Promise<void> {
		loadingURL = true;
		try {
			const res = await lambda.send(new GetFunctionUrlConfigCommand({ FunctionName: functionName }));
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
			await lambda.send(new UpdateFunctionConfigurationCommand({
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
			await lambda.send(new CreateEventSourceMappingCommand({
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
			await lambda.send(new DeleteEventSourceMappingCommand({ UUID: uuid }));
			toast.success('ESM deleted');
			await loadESMs();
		} catch (err) {
			toast.error(`Failed to delete ESM: ${(err as Error).message}`);
		}
	}

	async function publishVersion(): Promise<void> {
		publishingVersion = true;
		try {
			await lambda.send(new PublishVersionCommand({ FunctionName: functionName, Description: versionDescription }));
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
			await lambda.send(new CreateFunctionUrlConfigCommand({ FunctionName: functionName, AuthType: 'NONE' }));
			toast.success('Function URL created');
			await loadFunctionURL();
		} catch (err) {
			toast.error(`Failed to create Function URL: ${(err as Error).message}`);
		}
	}

	async function deleteFunctionURL(): Promise<void> {
		try {
			await lambda.send(new DeleteFunctionUrlConfigCommand({ FunctionName: functionName }));
			toast.success('Function URL deleted');
			functionURL = null;
		} catch (err) {
			toast.error(`Failed to delete Function URL: ${(err as Error).message}`);
		}
	}

	onMount(() => {
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
		<a href="/dashboard/lambda" class="px-4 py-2 rounded-xl bg-white/60 dark:bg-slate-700/60 border border-slate-200 dark:border-slate-600 text-sm font-medium hover:bg-white dark:hover:bg-slate-700">
			Back to functions
		</a>
	</div>

	<!-- Config Section -->
	<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6 space-y-4">
		{#if loading}
			<p class="text-slate-500 dark:text-slate-400">Loading function details...</p>
		{:else if errorMessage}
			<p class="text-red-600 dark:text-red-400">{errorMessage}</p>
		{:else if response?.Configuration}
			<h2 class="text-2xl font-bold text-slate-900 dark:text-white">{response.Configuration.FunctionName}</h2>
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
					{#if aliases.length > 0}
						<div>
							<p class="text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">Aliases</p>
							<div class="space-y-1">
								{#each aliases as a}
									<div class="flex items-center justify-between p-2 bg-slate-50 dark:bg-slate-900/40 rounded-lg text-sm">
										<span class="font-bold text-slate-900 dark:text-white">{a.Name}</span>
										<span class="font-mono text-xs text-orange-600 dark:text-orange-400 px-3">→ {a.FunctionVersion}</span>
										<span class="text-xs text-slate-400">{a.Description ?? ''}</span>
									</div>
								{/each}
							</div>
						</div>
					{/if}
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
