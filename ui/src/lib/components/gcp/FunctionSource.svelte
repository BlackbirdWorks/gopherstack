<script lang="ts">
	import { GCP_INPUT } from '#lib/gcp-action.ts';
	import { inlineTemplate, type SourceInput } from '#lib/gcpfunctions.ts';

	type Props = { runtime: string; source: SourceInput; touched: boolean; error: string | null; onEntry?: (entry: string) => void };
	let { runtime, source = $bindable(), touched, error, onEntry }: Props = $props();

	const tpl = $derived(inlineTemplate(runtime));
	let mode = $state<'inline' | 'zip'>('inline');
	let code = $state('');
	let zipName = $state('');

	$effect(() => {
		if (!tpl && mode === 'inline') choose('zip');
	});

	$effect(() => {
		if (tpl && mode === 'inline') {
			if (!code) code = tpl.code;
			source = { kind: 'inline', filename: tpl.filename, code };
		}
	});

	export function reset() {
		code = '';
		zipName = '';
		mode = 'inline';
		source = { kind: 'zip', data: new Uint8Array() };
	}

	async function pick(event: Event) {
		const file = (event.currentTarget as HTMLInputElement).files?.[0];
		if (!file) return;
		zipName = file.name;
		source = { kind: 'zip', data: new Uint8Array(await file.arrayBuffer()) };
	}

	function choose(next: 'inline' | 'zip') {
		mode = next;
		if (next === 'zip') source = { kind: 'zip', data: new Uint8Array() };
		else if (tpl) onEntry?.(tpl.entry);
	}
</script>

<fieldset class="space-y-2 text-sm">
	<legend class="font-medium text-gray-700 dark:text-gray-300">Source</legend>
	<div class="flex gap-4">
		<label class="flex items-center gap-1"><input type="radio" name="fn-src" checked={mode === 'inline'} disabled={!tpl} onchange={() => choose('inline')} data-testid="fn-src-inline" /> Inline editor</label>
		<label class="flex items-center gap-1"><input type="radio" name="fn-src" checked={mode === 'zip'} onchange={() => choose('zip')} data-testid="fn-src-zip" /> Upload .zip</label>
	</div>
	{#if mode === 'inline' && tpl}
		<label class="block space-y-1">
			<span class="font-mono text-xs text-gray-500 dark:text-gray-400">{tpl.filename}</span>
			<textarea class="{GCP_INPUT} h-40 font-mono text-xs" bind:value={code} spellcheck="false" aria-label="Source code" data-testid="fn-code"></textarea>
		</label>
	{:else}
		<input type="file" accept=".zip,application/zip" class="block text-xs" onchange={pick} aria-label="Source archive" data-testid="fn-zip" />
		{#if zipName}<p class="text-xs text-gray-500">{zipName} ({source.kind === 'zip' ? source.data.length : 0} bytes)</p>{/if}
		{#if !tpl}<p class="text-xs text-gray-500 dark:text-gray-400">The inline editor is available for Node.js and Python; Go sources need a zip.</p>{/if}
	{/if}
	{#if touched && error}<span class="block text-xs text-red-600 dark:text-red-400">{error}</span>{/if}
</fieldset>
