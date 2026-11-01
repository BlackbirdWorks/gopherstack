<script lang="ts">
	import type { Snippet } from 'svelte';
	import { GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { TRY_METHODS, prettyBody, type TryResponse } from '#lib/ociapigateway.ts';
	import { parseHeaderLines, type HeaderPair } from '#lib/tryit.ts';

	type Props = {
		prefix: string;
		send: (req: { method: string; path: string; query: string; body: string; headers: HeaderPair[] }) => Promise<TryResponse>;
		method?: string;
		path?: string;
		disabled?: boolean;
		note?: string;
		fields?: Snippet;
		resetKey?: string;
	};

	let { prefix, send, method = $bindable('GET'), path = $bindable('/'), disabled = false, note = '', fields, resetKey = '' }: Props = $props();

	let query = $state('');
	let headerText = $state('');
	let reqBody = $state('');
	let sending = $state(false);
	let error = $state('');
	let result = $state<TryResponse | null>(null);

	$effect(() => {
		void resetKey;
		result = null;
		error = '';
	});

	async function go() {
		sending = true;
		error = '';
		try {
			result = await send({ method, path, query, body: reqBody, headers: parseHeaderLines(headerText) });
		} catch (e) {
			error = describeGcpError(e);
			result = null;
		} finally {
			sending = false;
		}
	}

	const pill = (ok: boolean) =>
		ok ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300';
</script>

<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="{prefix}-try">
	{#if note}<p class="text-xs text-gray-500">{note}</p>{/if}
	{@render fields?.()}
	<div class="flex flex-wrap items-end gap-3 text-sm">
		<label>Method
			<select class="{GCP_INPUT} !w-28 !py-1" bind:value={method} data-testid="{prefix}-try-method">{#each TRY_METHODS as m (m)}<option value={m}>{m}</option>{/each}</select>
		</label>
		<label class="min-w-64 flex-1">Path <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" bind:value={path} data-testid="{prefix}-try-path" /></label>
		<label class="w-56">Query <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder="a=1&b=2" bind:value={query} data-testid="{prefix}-try-query" /></label>
	</div>
	<label class="block text-sm"><span class="mb-1 block font-medium">Headers (Name: value per line)</span>
		<textarea class="{GCP_INPUT} font-mono text-xs" rows="2" spellcheck="false" bind:value={headerText} data-testid="{prefix}-try-headers"></textarea>
	</label>
	<label class="block text-sm"><span class="mb-1 block font-medium">Body</span>
		<textarea class="{GCP_INPUT} font-mono text-xs" rows="4" spellcheck="false" bind:value={reqBody} data-testid="{prefix}-try-body"></textarea>
	</label>
	<button class={GCP_PRIMARY} onclick={go} disabled={sending || disabled} data-testid="{prefix}-try-send">Send</button>
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="{prefix}-try-error">{error}</p>{/if}
	{#if result}
		<div class="space-y-2" data-testid="{prefix}-try-result">
			<p class="text-sm">Status <span class="rounded-full px-2 py-0.5 text-xs {pill(result.status < 400)}" data-testid="{prefix}-try-status">{result.status}</span>{#if result.truncated} <span class="text-xs text-amber-600">(body truncated)</span>{/if}</p>
			<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs" data-testid="{prefix}-try-response-headers">
				{#each Object.entries(result.headers) as [k, v] (k)}<dt class="text-gray-500">{k}</dt><dd class="font-mono break-all">{v}</dd>{/each}
			</dl>
			<pre class="max-h-96 overflow-auto rounded-md bg-gray-50 p-3 text-xs dark:bg-slate-800" data-testid="{prefix}-try-response-body">{prettyBody(result)}</pre>
		</div>
	{/if}
</div>
