<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { GetFunctionCommand, type GetFunctionCommandOutput } from '@aws-sdk/client-lambda';
	import { getLambdaClient } from '$lib/aws-client';

	const lambda = getLambdaClient();

	let loading = $state(false);
	let errorMessage = $state('');
	let functionName = $derived(page.url.searchParams.get('name') ?? '');
	let response = $state<GetFunctionCommandOutput | null>(null);

	async function loadFunction(): Promise<void> {
		if (!functionName) {
			errorMessage = 'Function name is required';

			return;
		}

		loading = true;
		errorMessage = '';

		try {
			response = await lambda.send(
				new GetFunctionCommand({
					FunctionName: functionName
				})
			);
		} catch (err) {
			const message = err instanceof Error ? err.message : 'Failed to load function details';
			errorMessage = message;
			response = null;
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		void loadFunction();
	});
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div>
			<h1 class="text-3xl font-bold bg-gradient-to-r from-orange-600 to-amber-600 dark:from-orange-400 dark:to-amber-400 bg-clip-text text-transparent">Lambda Function Details</h1>
			<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Inspect runtime configuration and package information.</p>
		</div>
		<a href="/dashboard/lambda" class="px-4 py-2 rounded-xl bg-white/60 dark:bg-slate-700/60 border border-slate-200 dark:border-slate-600 text-sm font-medium hover:bg-white dark:hover:bg-slate-700">
			Back to functions
		</a>
	</div>

	<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6 space-y-4">
		{#if loading}
			<p class="text-slate-500 dark:text-slate-400">Loading function details...</p>
		{:else if errorMessage}
			<p class="text-red-600 dark:text-red-400">{errorMessage}</p>
		{:else if response?.Configuration}
			<h2 class="text-2xl font-bold text-slate-900 dark:text-white">{response.Configuration.FunctionName}</h2>
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
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
			</div>
		{:else}
			<p class="text-slate-500 dark:text-slate-400">Function not found.</p>
		{/if}
	</div>
</div>