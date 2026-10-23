<script lang="ts">
	import Modal from '#lib/components/Modal.svelte';
	import FunctionSource from '#lib/components/gcp/FunctionSource.svelte';
	import { GCP_GHOST, GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import {
		inlineTemplate,
		validateEntryPoint,
		validateFunctionId,
		validateSource,
		validateTopic,
		type FunctionInfo,
		type NewFunction,
		type RuntimeInfo,
		type SourceInput,
		type TriggerKind
	} from '#lib/gcpfunctions.ts';

	type Props = {
		busy: boolean;
		runtimes: RuntimeInfo[];
		topics: string[];
		onCreate: (fn: NewFunction) => void;
		onUpdate: (fn: FunctionInfo, source: SourceInput) => void;
	};
	let { busy, runtimes, topics, onCreate, onUpdate }: Props = $props();

	let modal = $state<Modal | null>(null);
	let src = $state<FunctionSource | null>(null);
	let target = $state<FunctionInfo | null>(null);
	let fId = $state('');
	let fRuntime = $state('');
	let fEntry = $state('');
	let fTrigger = $state<TriggerKind>('http');
	let fTopic = $state('');
	let source = $state<SourceInput>({ kind: 'zip', data: new Uint8Array() });
	let touched = $state(false);

	const rtId = $derived(target?.runtime ?? fRuntime);
	const idError = $derived(target ? null : validateFunctionId(fId.trim()));
	const entryError = $derived(target ? null : validateEntryPoint(fEntry));
	const topicError = $derived(target ? null : validateTopic(fTrigger, fTopic));
	const srcError = $derived(validateSource(source));
	const invalid = $derived(Boolean(idError || entryError || topicError || srcError));

	function setRuntime(id: string) {
		fRuntime = id;
		const tpl = inlineTemplate(id);
		if (tpl) fEntry = tpl.entry;
	}

	function prepare(fn: FunctionInfo | null) {
		target = fn;
		fId = '';
		fEntry = '';
		fTrigger = 'http';
		fTopic = topics[0] ?? '';
		touched = false;
		setRuntime(fn?.runtime ?? runtimes.find((r) => r.id.startsWith('nodejs'))?.id ?? runtimes[0]?.id ?? '');
		src?.reset();
		modal?.open();
	}

	export const openCreate = () => prepare(null);
	export const openUpdate = (fn: FunctionInfo) => prepare(fn);
	export const close = () => modal?.close('done');

	function submit(event: Event) {
		event.preventDefault();
		touched = true;
		if (invalid) return;
		if (target) onUpdate(target, source);
		else onCreate({ id: fId.trim(), runtime: fRuntime, entryPoint: fEntry, trigger: fTrigger, topic: fTopic, source });
	}
</script>

<Modal bind:this={modal} title={target ? `Update source of ${target.id}` : 'Create function'} titleId="fn-form-title">
	{#snippet children()}
		<form id="fn-form" class="space-y-3" onsubmit={submit} novalidate>
			{#if !target}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Function name</span>
					<input class={GCP_INPUT} bind:value={fId} placeholder="hello" autocomplete="off" aria-label="Function name" />
					{#if touched && idError}<span class="block text-xs text-red-600 dark:text-red-400">{idError}</span>{/if}
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Runtime</span>
					<select class={GCP_INPUT} value={fRuntime} onchange={(e) => setRuntime(e.currentTarget.value)} aria-label="Runtime" data-testid="fn-runtime">
						{#each runtimes as r (r.id)}<option value={r.id}>{r.display} ({r.id})</option>{/each}
					</select>
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Entry point</span>
					<input class="{GCP_INPUT} font-mono" bind:value={fEntry} autocomplete="off" aria-label="Entry point" data-testid="fn-entry" />
					{#if touched && entryError}<span class="block text-xs text-red-600 dark:text-red-400">{entryError}</span>{/if}
				</label>
				<fieldset class="space-y-1 text-sm">
					<legend class="font-medium text-gray-700 dark:text-gray-300">Trigger</legend>
					<div class="flex gap-4">
						<label class="flex items-center gap-1"><input type="radio" name="fn-trigger" value="http" bind:group={fTrigger} data-testid="fn-trigger-http" /> HTTP</label>
						<label class="flex items-center gap-1"><input type="radio" name="fn-trigger" value="pubsub" bind:group={fTrigger} data-testid="fn-trigger-pubsub" /> Pub/Sub topic</label>
					</div>
					{#if fTrigger === 'pubsub'}
						{#if topics.length === 0}
							<p class="text-xs text-amber-700 dark:text-amber-300">No topics in this project; create one in the Pub/Sub dashboard first.</p>
						{:else}
							<select class={GCP_INPUT} bind:value={fTopic} aria-label="Pub/Sub topic" data-testid="fn-topic">
								{#each topics as t (t)}<option value={t}>{t}</option>{/each}
							</select>
						{/if}
						{#if touched && topicError}<span class="block text-xs text-red-600 dark:text-red-400">{topicError}</span>{/if}
					{/if}
				</fieldset>
			{/if}
			<FunctionSource bind:this={src} runtime={rtId} bind:source {touched} error={srcError} onEntry={(e) => { if (!target) fEntry = e; }} />
			{#if target}
				<p class="text-xs text-gray-500 dark:text-gray-400">Uploading new source redeploys the function; it returns to Deploying until ready.</p>
			{/if}
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={GCP_GHOST} onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="fn-form" class={GCP_PRIMARY} disabled={busy} data-testid="fn-submit">{busy ? 'Working…' : target ? 'Update' : 'Create'}</button>
	{/snippet}
</Modal>
