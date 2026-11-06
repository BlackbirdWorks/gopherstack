<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw } from 'lucide-svelte';
	import { GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		INVOKE_METHODS,
		invokeEndpoint,
		listEndpoints,
		parseHeaders,
		statusTone,
		validateInvoke,
		type Endpoint,
		type InvokeReply
	} from '#lib/lambdaweb.ts';

	let { fn, region }: { fn: string; region: string } = $props();

	let endpoints = $state<Endpoint[]>([]);
	let endpoint = $state('');
	let method = $state<string>('GET');
	let path = $state('/');
	let headers = $state('');
	let body = $state('');
	let reply = $state<InvokeReply>();
	let loading = $state(true);
	let running = $state(false);
	let error = $state('');

	const invalid = $derived(validateInvoke(path, headers, body));
	const hasBody = $derived(method !== 'GET' && method !== 'HEAD');
	const tones = { good: 'text-emerald-700', warn: 'text-amber-700', bad: 'text-red-700' };

	async function load() {
		try {
			endpoints = await listEndpoints(fn, region);
			if (!endpoints.some((e) => e.name === endpoint)) endpoint = endpoints[0]?.name ?? '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function send() {
		if (!endpoint || invalid) return;
		running = true;
		reply = undefined;
		try {
			reply = await invokeEndpoint({
				region,
				function: fn,
				endpoint,
				method,
				path,
				headers: parseHeaders(headers).headers,
				body: hasBody ? body : ''
			});
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			running = false;
		}
	}

	onMount(load);

	const label = 'block text-xs text-gray-500';
	const mono = `${GCP_INPUT} mt-1 !py-1 font-mono text-xs`;
</script>

<div class="space-y-4" data-testid="lambdaweb-invoke">
	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if endpoints.length === 0}
		<p class="py-6 text-center text-gray-500" data-testid="lambdaweb-invoke-empty">Create an endpoint to invoke it here.</p>
	{:else}
		<form class="grid gap-3 md:grid-cols-2" onsubmit={(e) => { e.preventDefault(); void send(); }}>
			<label class={label}>Endpoint
				<select class={mono} bind:value={endpoint} data-testid="lambdaweb-invoke-endpoint">{#each endpoints as e (e.name)}<option value={e.name}>{e.name} ({e.auth})</option>{/each}</select>
			</label>
			<div class="grid grid-cols-[8rem_1fr] gap-3">
				<label class={label}>Method
					<select class={mono} bind:value={method} data-testid="lambdaweb-invoke-method">{#each INVOKE_METHODS as m (m)}<option value={m}>{m}</option>{/each}</select>
				</label>
				<label class={label}>Path
					<input class={mono} bind:value={path} placeholder="/" data-testid="lambdaweb-invoke-path" />
				</label>
			</div>
			<label class={label}>Headers (one per line, Name: value)
				<textarea class={mono} rows="3" bind:value={headers} placeholder="Accept: application/json" data-testid="lambdaweb-invoke-headers"></textarea>
			</label>
			<label class={label}>Body
				<textarea class={mono} rows="3" bind:value={body} disabled={!hasBody} data-testid="lambdaweb-invoke-body"></textarea>
			</label>
			<div class="flex items-center gap-3 md:col-span-2">
				<button type="submit" class={GCP_PRIMARY} disabled={running || invalid !== ''} data-testid="lambdaweb-invoke-send">{running ? 'Sending…' : 'Send request'}</button>
				{#if invalid}<span class="text-xs text-red-600" data-testid="lambdaweb-invoke-invalid">{invalid}</span>{/if}
			</div>
		</form>
		<p class="text-xs text-gray-500">The request is sent from the emulator to the endpoint domain. IamAuth endpoints need a signed request and answer 403 here.</p>

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="lambdaweb-invoke-error">{error}</p>{/if}

		{#if reply}
			<section class="space-y-2" data-testid="lambdaweb-reply">
				<p class="text-sm">Response <span class="font-mono font-semibold {tones[statusTone(reply.status)]}" data-testid="lambdaweb-reply-status">{reply.status}</span> · {reply.durationMs} ms · <span class="font-mono text-xs">{reply.domain}</span></p>
				<details>
					<summary class="cursor-pointer text-xs text-gray-500">Headers ({Object.keys(reply.headers).length})</summary>
					<pre class="mt-1 overflow-auto rounded-md bg-gray-50 p-2 font-mono text-xs dark:bg-slate-900" data-testid="lambdaweb-reply-headers">{Object.entries(reply.headers).map(([k, v]) => `${k}: ${v.join(', ')}`).join('\n')}</pre>
				</details>
				<pre class="max-h-96 overflow-auto rounded-md bg-gray-50 p-3 font-mono text-xs dark:bg-slate-900" data-testid="lambdaweb-reply-body">{reply.body || '(empty body)'}</pre>
				{#if reply.encoding === 'base64'}<p class="text-xs text-gray-500">The body is binary and shown as base64.</p>{/if}
				{#if reply.truncated}<p class="text-xs text-amber-700">The body was cut at 256 KiB.</p>{/if}
			</section>
		{/if}
	{/if}
</div>
