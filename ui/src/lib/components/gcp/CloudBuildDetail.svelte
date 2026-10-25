<script lang="ts">
	import { onMount } from 'svelte';
	import { getBuildLog, type BuildInfo, type BuildLog, type CbScope } from '#lib/gcpcloudbuild.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { build: BuildInfo; scope: CbScope };
	let { build, scope }: Props = $props();

	let log = $state<BuildLog | null>(null);
	let error = $state('');

	onMount(async () => {
		try {
			log = await getBuildLog(scope, build.id);
		} catch (e) {
			error = describeGcpError(e);
		}
	});
</script>

<div class="space-y-2" data-testid="cloudbuild-detail-body">
	{#if build.statusDetail}<p class="text-gray-600 dark:text-gray-300">{build.statusDetail}</p>{/if}
	<p class="font-medium text-gray-500 dark:text-gray-400">Steps</p>
	{#if build.steps.length === 0}
		<p data-testid="cloudbuild-no-steps">No steps</p>
	{:else}
		<ol class="space-y-1" data-testid="cloudbuild-steps">
			{#each build.steps as step, i (i)}
				<li class="font-mono" data-testid="cloudbuild-step">
					<span class="mr-2 rounded bg-gray-100 px-1.5 py-0.5 dark:bg-slate-800">{step.status}</span>{step.id || `#${i}`} {step.name}{step.args.length ? ` ${step.args.join(' ')}` : ''}
				</li>
			{/each}
		</ol>
	{/if}
	<p class="font-medium text-gray-500 dark:text-gray-400">Log {build.logUrl ? `(${build.logUrl})` : ''}</p>
	{#if error}
		<p class="text-red-600" role="alert">{error}</p>
	{:else if log === null}
		<p>Loading log...</p>
	{:else if !log.available}
		<p data-testid="cloudbuild-log-missing">No log stored for this build yet.</p>
	{:else}
		<pre class="max-h-80 overflow-auto whitespace-pre-wrap break-all rounded-md bg-white p-2 font-mono dark:bg-slate-900" data-testid="cloudbuild-log">{log.log}</pre>
	{/if}
</div>
