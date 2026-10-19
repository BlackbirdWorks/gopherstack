<script lang="ts">
	import { Plus } from 'lucide-svelte';
	import FirestoreValueEditor from './FirestoreValueEditor.svelte';
	import {
		fieldsToNodes,
		newNode,
		nodesFromJsonText,
		nodesToFields,
		validateFields,
		type FsCtx,
		type FsNode
	} from '#lib/gcpfirestore.ts';

	let {
		nodes = $bindable(),
		mode = $bindable(),
		rawText = $bindable(),
		ctx
	}: {
		nodes: FsNode[];
		mode: 'typed' | 'raw';
		rawText: string;
		ctx: FsCtx;
	} = $props();

	let switchError = $state('');

	function toRaw() {
		const problem = validateFields(nodes, ctx);
		if (problem) {
			switchError = `Fix the form first: ${problem}`;
			return;
		}
		rawText = JSON.stringify(nodesToFields(nodes, ctx), null, 2);
		switchError = '';
		mode = 'raw';
	}

	function toTyped() {
		const parsed = nodesFromJsonText(rawText);
		if ('error' in parsed) {
			switchError = `Fix the JSON first: ${parsed.error}`;
			return;
		}
		nodes = parsed.nodes.length > 0 ? parsed.nodes : fieldsToNodes({});
		switchError = '';
		mode = 'typed';
	}

	const tab = (active: boolean) =>
		`px-3 py-1 text-xs font-medium rounded-md ${active ? 'bg-blue-600 text-white' : 'border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-slate-800'}`;
</script>

<div class="space-y-3" data-testid="fs-doc-editor">
	<div class="flex items-center gap-2">
		<button type="button" class={tab(mode === 'typed')} onclick={toTyped} data-testid="fs-mode-typed">Fields</button>
		<button type="button" class={tab(mode === 'raw')} onclick={toRaw} data-testid="fs-mode-raw">Raw JSON</button>
		{#if switchError}<span class="text-xs text-red-600 dark:text-red-400" role="alert">{switchError}</span>{/if}
	</div>
	{#if mode === 'typed'}
		<div class="space-y-2">
			{#each nodes as _n, i (nodes[i].id)}
				<FirestoreValueEditor bind:node={nodes[i]} {ctx} onremove={() => nodes.splice(i, 1)} />
			{/each}
			{#if nodes.length === 0}<p class="text-xs text-gray-500 dark:text-gray-400">No fields</p>{/if}
			<button type="button" class="flex items-center gap-1 rounded-md border border-dashed border-gray-300 dark:border-gray-600 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => nodes.push(newNode())} data-testid="fs-add-field">
				<Plus class="h-3 w-3" /> Add field
			</button>
		</div>
	{:else}
		<textarea
			class="w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 font-mono text-xs focus:outline-none focus:ring-2 focus:ring-blue-500"
			rows="12"
			bind:value={rawText}
			aria-label="Document fields JSON"
			spellcheck="false"
			data-testid="fs-raw-json"
		></textarea>
		<p class="text-[11px] text-gray-500 dark:text-gray-400">Firestore REST Value JSON, e.g. {`{"age":{"integerValue":"30"}}`}</p>
	{/if}
</div>
