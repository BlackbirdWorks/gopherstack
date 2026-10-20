<script lang="ts">
	import Modal from '#lib/components/Modal.svelte';
	import KeyValueEditor from '#lib/components/gcp/KeyValueEditor.svelte';
	import { GCP_GHOST, GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { validateRows, type MetaRow } from '#lib/gcpstorage.ts';
	import { rowsToEnv, validateEnvName, validateImage, validateMaxRetries, validateParallelism, validateRunId, validateTaskCount, type NewJob } from '#lib/gcprun.ts';

	let { busy, onCreate }: { busy: boolean; onCreate: (job: NewJob) => void } = $props();

	let modal = $state<Modal | null>(null);
	let fId = $state('');
	let fImage = $state('');
	let fTasks = $state('1');
	let fParallel = $state('');
	let fRetries = $state('3');
	let fCommand = $state('');
	let fArgs = $state('');
	let fEnv = $state<MetaRow[]>([]);
	let touched = $state(false);

	const errors = $derived([
		validateRunId(fId.trim()),
		validateImage(fImage),
		validateTaskCount(fTasks),
		validateParallelism(fParallel),
		validateMaxRetries(fRetries),
		validateRows(fEnv, validateEnvName)
	]);
	const invalid = $derived(errors.some(Boolean));
	const field = 'block space-y-1 text-sm';
	const label = 'font-medium text-gray-700 dark:text-gray-300';
	const err = 'block text-xs text-red-600 dark:text-red-400';

	export function open() {
		fId = '';
		fImage = '';
		fTasks = '1';
		fParallel = '';
		fRetries = '3';
		fCommand = '';
		fArgs = '';
		fEnv = [];
		touched = false;
		modal?.open();
	}

	export function close() {
		modal?.close('done');
	}

	function submit(event: Event) {
		event.preventDefault();
		touched = true;
		if (invalid) return;
		onCreate({ id: fId.trim(), image: fImage, taskCount: fTasks, parallelism: fParallel, maxRetries: fRetries, command: fCommand, args: fArgs, env: rowsToEnv(fEnv) });
	}
</script>

<Modal bind:this={modal} title="Create job" titleId="run-job-title">
	{#snippet children()}
		<form id="run-job-form" class="space-y-3" onsubmit={submit} novalidate>
			<label class={field}>
				<span class={label}>Job name</span>
				<input class={GCP_INPUT} bind:value={fId} placeholder="batch" autocomplete="off" aria-label="Job name" />
				{#if touched && errors[0]}<span class={err}>{errors[0]}</span>{/if}
			</label>
			<label class={field}>
				<span class={label}>Container image</span>
				<input class="{GCP_INPUT} font-mono" bind:value={fImage} placeholder="alpine:3.20" autocomplete="off" aria-label="Container image" />
				{#if touched && errors[1]}<span class={err}>{errors[1]}</span>{/if}
			</label>
			<div class="grid grid-cols-3 gap-3">
				<label class={field}>
					<span class={label}>Task count</span>
					<input class={GCP_INPUT} bind:value={fTasks} inputmode="numeric" aria-label="Task count" />
					{#if touched && errors[2]}<span class={err}>{errors[2]}</span>{/if}
				</label>
				<label class={field}>
					<span class={label}>Parallelism</span>
					<input class={GCP_INPUT} bind:value={fParallel} inputmode="numeric" placeholder="all" aria-label="Parallelism" />
					{#if touched && errors[3]}<span class={err}>{errors[3]}</span>{/if}
				</label>
				<label class={field}>
					<span class={label}>Max retries</span>
					<input class={GCP_INPUT} bind:value={fRetries} inputmode="numeric" aria-label="Max retries" />
					{#if touched && errors[4]}<span class={err}>{errors[4]}</span>{/if}
				</label>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<label class={field}>
					<span class={label}>Command (one per line)</span>
					<textarea class="{GCP_INPUT} font-mono" rows="2" bind:value={fCommand} aria-label="Command"></textarea>
				</label>
				<label class={field}>
					<span class={label}>Arguments (one per line)</span>
					<textarea class="{GCP_INPUT} font-mono" rows="2" bind:value={fArgs} aria-label="Arguments"></textarea>
				</label>
			</div>
			<KeyValueEditor bind:rows={fEnv} label="Environment variables" testId="run-job-env" validateKey={validateEnvName} hideSave />
			<p class="text-xs text-gray-500 dark:text-gray-400">Each task gets CLOUD_RUN_TASK_INDEX, CLOUD_RUN_TASK_COUNT and CLOUD_RUN_TASK_ATTEMPT.</p>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={GCP_GHOST} onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="run-job-form" class={GCP_PRIMARY} disabled={busy} data-testid="run-job-submit">{busy ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>
