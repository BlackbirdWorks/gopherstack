<script lang="ts">
	import { prettyJson, tokenizeJson } from '#lib/azurelogic.ts';

	let { value, testid = 'json-view' }: { value: unknown; testid?: string } = $props();

	const tokens = $derived(tokenizeJson(prettyJson(value)));
	const tone: Record<string, string> = {
		key: 'text-sky-700 dark:text-sky-300',
		string: 'text-emerald-700 dark:text-emerald-300',
		number: 'text-amber-700 dark:text-amber-300',
		literal: 'text-purple-700 dark:text-purple-300',
		punct: 'text-gray-500 dark:text-gray-400',
		space: ''
	};
</script>

<pre class="max-h-96 overflow-auto rounded-md bg-gray-50 p-3 font-mono text-xs leading-relaxed dark:bg-slate-900" data-testid={testid}>{#each tokens as t, i (i)}<span class={tone[t.kind]}>{t.text}</span>{/each}</pre>
