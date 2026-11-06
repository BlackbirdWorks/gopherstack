<script lang="ts">
	import { getBuildLog, durationMs, formatDuration, type BuildInfo, type BuildLog, type CbScope } from '#lib/gcpcloudbuild.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { build: BuildInfo; scope: CbScope };
	let { build, scope }: Props = $props();

	let log = $state<BuildLog | null>(null);
	let error = $state('');

	$effect(() => {
		const id = build.id;
		void build.status;
		void build.finishTime;
		getBuildLog(scope, id).then(
			(l) => {
				log = l;
				error = '';
			},
			(e) => (error = describeGcpError(e))
		);
	});
</script>

<div class="space-y-2" data-testid="cloudbuild-detail-body">
	{#if build.statusDetail}<p class="text-gray-600 dark:text-gray-300">{build.statusDetail}</p>{/if}
	{#if build.approval}
		<p data-testid="cloudbuild-approval">Approval: <span class="font-mono">{build.approval}</span>{build.approvalComment ? ` (${build.approvalComment})` : ''}</p>
	{/if}
	<p class="font-medium text-gray-500 dark:text-gray-400">Steps</p>
	{#if build.steps.length === 0}
		<p data-testid="cloudbuild-no-steps">No steps</p>
	{:else}
		<ol class="space-y-1" data-testid="cloudbuild-steps">
			{#each build.steps as step, i (i)}
				<li class="font-mono" data-testid="cloudbuild-step">
					<span class="mr-2 rounded bg-gray-100 px-1.5 py-0.5 dark:bg-slate-800">{step.status}</span>{step.id || `#${i}`} {step.name}{step.args.length ? ` ${step.args.join(' ')}` : ''}
					{#if step.startTime}<span class="ml-2 text-gray-500" data-testid="cloudbuild-step-time">{formatDuration(durationMs(step.startTime, step.endTime))}</span>{/if}
					{#if step.exitCode !== null && step.exitCode !== 0}<span class="ml-2 text-red-600">exit {step.exitCode}</span>{/if}
				</li>
			{/each}
		</ol>
	{/if}

	{#if build.resultImages.length || build.images.length}
		<p class="font-medium text-gray-500 dark:text-gray-400">Images</p>
		<ul class="font-mono" data-testid="cloudbuild-images">
			{#each build.resultImages.length ? build.resultImages : build.images.map((name) => ({ name, digest: '' })) as img (img.name)}
				<li>{img.name}{img.digest ? ` @ ${img.digest}` : ''}</li>
			{/each}
		</ul>
	{/if}
	{#if build.stepOutputs.length}
		<p class="font-medium text-gray-500 dark:text-gray-400">Step outputs</p>
		<ul class="font-mono" data-testid="cloudbuild-outputs">{#each build.stepOutputs as o, i (i)}<li class="whitespace-pre-wrap break-all">{o}</li>{/each}</ul>
	{/if}
	{#if build.artifacts.length}
		<p class="font-medium text-gray-500 dark:text-gray-400">Artifacts</p>
		<ul class="font-mono" data-testid="cloudbuild-artifacts">{#each build.artifacts as a (a)}<li>{a}</li>{/each}</ul>
	{/if}
	{#if build.secrets.length}
		<p class="font-medium text-gray-500 dark:text-gray-400">Secrets (names only, values are never shown)</p>
		<ul class="font-mono" data-testid="cloudbuild-secrets">{#each build.secrets as s (s.name + s.kind)}<li>{s.name} <span class="text-gray-500">{s.kind}</span></li>{/each}</ul>
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
