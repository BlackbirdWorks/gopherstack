<script lang="ts">
	import { Play } from 'lucide-svelte';
	import { GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { invokeFunction, type FnScope, type FunctionInfo, type InvokeOutput } from '#lib/gcpfunctions.ts';

	type Props = { scope: FnScope; fn: FunctionInfo };
	let { scope, fn }: Props = $props();

	const METHODS = ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'];
	let method = $state('GET');
	let path = $state('/?name=gopher');
	let body = $state('');
	let headers = $state('');
	let running = $state(false);
	let error = $state('');
	let result = $state<InvokeOutput | null>(null);

	async function run() {
		running = true;
		error = '';
		try {
			result = await invokeFunction(scope, fn.id, { method, path, body, headers });
		} catch (e) {
			result = null;
			error = describeGcpError(e);
		} finally {
			running = false;
		}
	}
</script>

<div class="space-y-2" data-testid="fn-invoke">
	<h4 class="font-semibold text-gray-700 dark:text-gray-300">Invoke</h4>
	<div class="flex gap-2">
		<select class="{GCP_INPUT} w-28" bind:value={method} aria-label="Method" data-testid="fn-invoke-method">
			{#each METHODS as m (m)}<option>{m}</option>{/each}
		</select>
		<input class="{GCP_INPUT} font-mono" bind:value={path} aria-label="Path" data-testid="fn-invoke-path" />
		<button class="flex items-center gap-1 {GCP_PRIMARY}" disabled={running} onclick={run} data-testid="fn-invoke-run"><Play class="h-4 w-4" /> {running ? 'Calling…' : 'Invoke'}</button>
	</div>
	<div class="grid gap-2 md:grid-cols-2">
		<textarea class="{GCP_INPUT} h-20 font-mono text-xs" bind:value={headers} placeholder="Header-Name: value (one per line)" aria-label="Request headers"></textarea>
		<textarea class="{GCP_INPUT} h-20 font-mono text-xs" bind:value={body} placeholder="Request body" aria-label="Request body" data-testid="fn-invoke-body"></textarea>
	</div>
	{#if error}<p class="text-red-600 dark:text-red-400" role="alert" data-testid="fn-invoke-error">{error}</p>{/if}
	{#if result}
		<div class="rounded-md border border-gray-200 p-3 dark:border-gray-700" data-testid="fn-invoke-result">
			<p class="mb-1 font-mono"><span class="font-semibold" data-testid="fn-invoke-status">{result.status}</span></p>
			<pre class="max-h-24 overflow-auto text-[11px] text-gray-500 dark:text-gray-400">{Object.entries(result.headers).map(([k, v]) => `${k}: ${v}`).join('\n')}</pre>
			<pre class="mt-1 max-h-60 overflow-auto whitespace-pre-wrap break-all font-mono" data-testid="fn-invoke-response">{result.body}</pre>
		</div>
	{/if}
</div>
