<script lang="ts">
	import { toast } from 'svelte-sonner';
	import { Upload } from 'lucide-svelte';
	import { GCP_INPUT } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		LOAD_FORMATS,
		runLoad,
		validateBqId,
		validateGsUri,
		type JobOutcome
	} from '#lib/gcpbigquery.ts';

	let {
		project,
		datasets,
		dataset = '',
		onloaded
	}: { project: string; datasets: string[]; dataset?: string; onloaded: (dataset: string, table: string) => void } = $props();

	let uri = $state('');
	let format = $state('CSV');
	let autodetect = $state(true);
	let target = $state('');
	let table = $state('');
	let skipRows = $state(1);
	let append = $state(true);
	let busy = $state(false);
	let touched = $state(false);
	let outcome = $state<JobOutcome | null>(null);
	let error = $state('');

	const chosen = $derived(target || dataset || datasets[0] || '');
	const problem = $derived(
		validateGsUri(uri) ?? validateBqId(chosen, 'Dataset') ?? validateBqId(table.trim(), 'Table')
	);

	async function submit(event: Event) {
		event.preventDefault();
		touched = true;
		if (problem) return;
		busy = true;
		error = '';
		outcome = null;
		try {
			const t = table.trim();
			outcome = await runLoad(project, { uri, format, autodetect, dataset: chosen, table: t, skipRows, append });
			if (outcome.error) error = outcome.error;
			else {
				toast.success(`Loaded ${outcome.outputRows || '0'} row(s) into ${chosen}.${t}`);
				onloaded(chosen, t);
			}
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			busy = false;
		}
	}

	const label = 'block space-y-1 text-sm';
	const span = 'font-medium text-gray-700 dark:text-gray-300';
</script>

<form class="max-w-xl space-y-4" onsubmit={submit} novalidate data-testid="bq-load">
	<p class="text-sm text-gray-500 dark:text-gray-400">Load objects from the Cloud Storage emulator into a table (creates it when missing).</p>
	<label class={label}>
		<span class={span}>Source URI</span>
		<input class="{GCP_INPUT} font-mono" bind:value={uri} placeholder="gs://my-bucket/data/*.csv" autocomplete="off" aria-label="Source URI" data-testid="bq-load-uri" />
	</label>
	<div class="grid gap-4 sm:grid-cols-2">
		<label class={label}>
			<span class={span}>Format</span>
			<select class={GCP_INPUT} bind:value={format} aria-label="Source format" data-testid="bq-load-format">
				{#each LOAD_FORMATS as f (f.id)}<option value={f.id}>{f.label}</option>{/each}
			</select>
		</label>
		<label class={label}>
			<span class={span}>Dataset</span>
			<select class={GCP_INPUT} value={chosen} onchange={(e) => (target = e.currentTarget.value)} aria-label="Destination dataset" data-testid="bq-load-dataset">
				{#each datasets as d (d)}<option value={d}>{d}</option>{/each}
			</select>
		</label>
		<label class={label}>
			<span class={span}>Table</span>
			<input class="{GCP_INPUT} font-mono" bind:value={table} placeholder="imported" autocomplete="off" aria-label="Destination table" data-testid="bq-load-table" />
		</label>
		{#if format === 'CSV'}
			<label class={label}>
				<span class={span}>Header rows to skip</span>
				<input type="number" min="0" class={GCP_INPUT} bind:value={skipRows} aria-label="Header rows to skip" />
			</label>
		{/if}
	</div>
	<div class="flex flex-wrap gap-6 text-sm">
		<label class="flex items-center gap-2"><input type="checkbox" bind:checked={autodetect} data-testid="bq-load-autodetect" /> Auto-detect schema</label>
		<label class="flex items-center gap-2"><input type="checkbox" bind:checked={append} /> Append to existing rows</label>
	</div>
	{#if touched && problem}<p class="text-xs text-red-600 dark:text-red-400" data-testid="bq-load-problem">{problem}</p>{/if}
	<button type="submit" class="flex items-center gap-2 rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={busy} data-testid="bq-load-submit">
		<Upload class="h-4 w-4" /> {busy ? 'Loading…' : 'Load data'}
	</button>
	{#if error}
		<div class="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="bq-load-error">{error}</div>
	{:else if outcome}
		<div class="rounded-lg border border-green-200 bg-green-50 p-3 text-sm text-green-800 dark:border-green-900 dark:bg-green-950/40 dark:text-green-300" data-testid="bq-load-done">
			Job {outcome.id} {outcome.state}{outcome.outputRows ? ` — ${outcome.outputRows} row(s) loaded` : ''}
		</div>
	{/if}
</form>
