<script lang="ts">
	import { onMount } from 'svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { formatBytes } from '#lib/format.ts';
	import {
		buildQuery,
		buildTraceTree,
		distinctValues,
		emptyFilters,
		formatLatency,
		maxSeq,
		mergeNew,
		statusClass,
		type TraceFilters,
		type TraceList,
		type TraceSpan
	} from '#lib/apptrace.ts';

	const API = '/_gopherstack/apptrace';
	const PAGE_SIZE = 200;
	const POLL_MS = 2000;

	let filters = $state<TraceFilters>({ ...emptyFilters });
	let spans = $state<TraceSpan[]>([]);
	let total = $state(0);
	let capacity = $state(0);
	let recorded = $state(0);
	let live = $state(true);
	let error = $state<string | null>(null);
	let loaded = $state(false);

	let enabled = $state(true);
	let detailCapture = $state(false);

	let selected = $state<TraceSpan | null>(null);
	let trace = $state<TraceSpan[]>([]);
	let detailError = $state<string | null>(null);

	let serviceOptions = $derived(distinctValues(spans, 'service'));
	let regionOptions = $derived(distinctValues(spans, 'region'));
	let tree = $derived(buildTraceTree(trace));

	const statusStyles = {
		ok: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300',
		redirect: 'bg-sky-100 text-sky-800 dark:bg-sky-900/30 dark:text-sky-300',
		client: 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300',
		server: 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300'
	} as const;

	async function getJSON<T>(url: string): Promise<T> {
		const res = await fetch(url);
		if (!res.ok) throw new Error(`${url}: ${res.status}`);
		return (await res.json()) as T;
	}

	async function load() {
		try {
			const data = await getJSON<TraceList>(`${API}/requests${buildQuery(filters, { limit: PAGE_SIZE })}`);
			spans = data.items ?? [];
			total = data.total;
			capacity = data.capacity;
			recorded = data.recorded;
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		} finally {
			loaded = true;
		}
	}

	async function poll() {
		if (!live) return;
		try {
			const data = await getJSON<TraceList>(
				`${API}/requests${buildQuery(filters, { limit: PAGE_SIZE, afterSeq: maxSeq(spans) })}`
			);
			spans = mergeNew(spans, data.items ?? [], PAGE_SIZE);
			total += data.total;
			recorded = data.recorded;
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		}
	}

	async function loadConfig() {
		try {
			const cfg = await getJSON<{ enabled: boolean; detail: boolean }>(`${API}/config`);
			enabled = cfg.enabled;
			detailCapture = cfg.detail;
		} catch {
			// config is optional chrome; list errors are reported by load()
		}
	}

	async function putConfig(patch: { enabled?: boolean; detail?: boolean }) {
		try {
			const res = await fetch(`${API}/config`, {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(patch)
			});
			if (!res.ok) throw new Error('Failed to update capture settings');
			const cfg = await res.json();
			enabled = cfg.enabled;
			detailCapture = cfg.detail;
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		}
	}

	async function clearAll() {
		if (!(await confirmDestructive({ title: 'Clear Trace', message: 'Drop every recorded request?', confirmLabel: 'Clear', dangerous: false }))) return;
		try {
			const res = await fetch(`${API}/requests`, { method: 'DELETE' });
			if (!res.ok) throw new Error('Failed to clear trace');
			selected = null;
			trace = [];
			await load();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		}
	}

	async function select(id: string) {
		detailError = null;
		try {
			const data = await getJSON<{ span: TraceSpan; trace: TraceSpan[] }>(
				`${API}/requests/${encodeURIComponent(id)}`
			);
			selected = data.span;
			trace = data.trace;
		} catch (e) {
			selected = null;
			detailError = e instanceof Error ? e.message : 'Unknown error';
		}
	}

	let filterTimer: ReturnType<typeof setTimeout> | undefined;
	function filtersChanged() {
		clearTimeout(filterTimer);
		filterTimer = setTimeout(load, 250);
	}

	onMount(() => {
		load();
		loadConfig();
		const timer = setInterval(poll, POLL_MS);
		return () => {
			clearInterval(timer);
			clearTimeout(filterTimer);
		};
	});

	const inputCls =
		'px-3 py-1.5 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-800 dark:text-white text-sm';
</script>

{#snippet kv(title: string, map?: Record<string, string>)}
	<div>
		<h4 class="text-xs font-semibold uppercase text-slate-500 dark:text-slate-400 mb-1">{title}</h4>
		{#if map && Object.keys(map).length > 0}
			<dl class="text-xs font-mono grid grid-cols-[max-content_1fr] gap-x-3 gap-y-0.5">
				{#each Object.entries(map).sort(([a], [b]) => a.localeCompare(b)) as [k, v] (k)}
					<dt class="text-slate-500 dark:text-slate-400">{k}</dt>
					<dd class="break-all text-slate-900 dark:text-slate-100">{v}</dd>
				{/each}
			</dl>
		{:else}
			<p class="text-xs text-slate-400">none</p>
		{/if}
	</div>
{/snippet}

{#snippet body(title: string, text?: string, truncated?: boolean)}
	<div>
		<h4 class="text-xs font-semibold uppercase text-slate-500 dark:text-slate-400 mb-1">
			{title}{#if truncated}<span class="ml-2 normal-case text-amber-600">(truncated)</span>{/if}
		</h4>
		{#if text}
			<pre class="text-xs font-mono p-2 bg-slate-50 dark:bg-slate-900 rounded border border-slate-200 dark:border-slate-700 overflow-auto max-h-64 whitespace-pre-wrap break-all">{text}</pre>
		{:else}
			<p class="text-xs text-slate-400">empty</p>
		{/if}
	</div>
{/snippet}

<div class="space-y-4" id="apptrace-page">
	<div class="flex flex-wrap items-end justify-between gap-3">
		<div>
			<h1 class="text-3xl font-bold text-slate-900 dark:text-white">App Inspector</h1>
			<p class="text-slate-600 dark:text-slate-300">
				Every AWS API call through the router, with cross-service deliveries linked to their trigger.
			</p>
		</div>
		<div class="flex flex-wrap items-center gap-3 text-sm text-slate-700 dark:text-slate-300">
			<label class="flex items-center gap-1.5">
				<input id="apptrace-live" type="checkbox" bind:checked={live} /> Live
			</label>
			<label class="flex items-center gap-1.5">
				<input id="apptrace-enabled" type="checkbox" checked={enabled} onchange={(e) => putConfig({ enabled: e.currentTarget.checked })} /> Recording
			</label>
			<label class="flex items-center gap-1.5" title="Capture redacted headers and bodies for new requests">
				<input id="apptrace-detail" type="checkbox" checked={detailCapture} onchange={(e) => putConfig({ detail: e.currentTarget.checked })} /> Capture bodies
			</label>
			<button id="apptrace-refresh" onclick={load} class="px-3 py-1.5 border border-slate-300 dark:border-slate-600 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700">Refresh</button>
			<button id="apptrace-clear" onclick={clearAll} class="px-3 py-1.5 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-900/50 rounded-lg hover:bg-red-50 dark:hover:bg-red-900/20">Clear</button>
		</div>
	</div>

	{#if error}
		<div id="apptrace-error" class="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg text-sm text-red-700 dark:text-red-300">{error}</div>
	{/if}

	<div class="flex flex-wrap gap-2">
		<input id="apptrace-q" type="search" placeholder="Search service, operation, resource, id, principal…" bind:value={filters.q} oninput={filtersChanged} class="{inputCls} flex-1 min-w-64" />
		<select id="apptrace-service" bind:value={filters.service} onchange={filtersChanged} class={inputCls} aria-label="Service">
			<option value="">All services</option>
			{#each serviceOptions as s (s)}<option value={s}>{s}</option>{/each}
		</select>
		<input id="apptrace-operation" type="text" placeholder="Operation" bind:value={filters.operation} oninput={filtersChanged} class="{inputCls} w-40" />
		<select id="apptrace-region" bind:value={filters.region} onchange={filtersChanged} class={inputCls} aria-label="Region">
			<option value="">All regions</option>
			{#each regionOptions as r (r)}<option value={r}>{r}</option>{/each}
		</select>
		<select id="apptrace-status" bind:value={filters.status} onchange={filtersChanged} class={inputCls} aria-label="Status">
			<option value="">Any status</option>
			<option value="errors">Errors (4xx+5xx)</option>
			<option value="2xx">2xx</option>
			<option value="4xx">4xx</option>
			<option value="5xx">5xx</option>
		</select>
		<select id="apptrace-kind" bind:value={filters.kind} onchange={filtersChanged} class={inputCls} aria-label="Kind">
			<option value="">Requests and deliveries</option>
			<option value="request">Requests only</option>
			<option value="internal">Deliveries only</option>
		</select>
	</div>

	<p class="text-xs text-slate-500 dark:text-slate-400" id="apptrace-summary">
		{spans.length} shown of {total} matching · {recorded} recorded since start · ring capacity {capacity}
	</p>

	<div class="grid grid-cols-1 xl:grid-cols-2 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-auto max-h-[70vh]">
			<table class="w-full text-sm" id="apptrace-table">
				<thead class="sticky top-0 bg-slate-50 dark:bg-slate-900 text-left text-xs uppercase text-slate-500 dark:text-slate-400">
					<tr>
						<th class="px-3 py-2">Time</th>
						<th class="px-3 py-2">Service</th>
						<th class="px-3 py-2">Operation</th>
						<th class="px-3 py-2">Status</th>
						<th class="px-3 py-2 text-right">Latency</th>
						<th class="px-3 py-2 text-right">Size</th>
					</tr>
				</thead>
				<tbody>
					{#each spans as s (s.seq)}
						<tr
							data-testid="apptrace-row"
							class="border-t border-slate-100 dark:border-slate-700 cursor-pointer hover:bg-slate-50 dark:hover:bg-slate-700/50 {selected?.id === s.id ? 'bg-indigo-50 dark:bg-indigo-900/20' : ''}"
							onclick={() => select(s.id)}
						>
							<td class="px-3 py-1.5 whitespace-nowrap text-slate-600 dark:text-slate-400">{new Date(s.time).toLocaleTimeString()}</td>
							<td class="px-3 py-1.5">{#if s.kind === 'internal'}<span class="text-slate-400 mr-1" title="Triggered by request {s.parentId}">↳</span>{/if}{s.service}</td>
							<td class="px-3 py-1.5 font-medium text-slate-900 dark:text-white">{s.operation}</td>
							<td class="px-3 py-1.5">
								<span class="px-1.5 py-0.5 rounded text-xs font-semibold {statusStyles[statusClass(s.status)]}">{s.status}</span>
								{#if s.errorCode}<span class="ml-1 text-xs text-red-600 dark:text-red-400">{s.errorCode}</span>{/if}
							</td>
							<td class="px-3 py-1.5 text-right tabular-nums">{formatLatency(s.latencyMs)}</td>
							<td class="px-3 py-1.5 text-right tabular-nums text-slate-500">{formatBytes(s.requestBytes)} / {formatBytes(s.responseBytes)}</td>
						</tr>
					{:else}
						<tr><td colspan="6" class="px-3 py-8 text-center text-slate-500 dark:text-slate-400">{loaded ? 'No requests recorded yet. Make some AWS API calls.' : 'Loading…'}</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 space-y-4 overflow-auto max-h-[70vh]" id="apptrace-detail-panel">
			{#if detailError}
				<p class="text-sm text-red-600">{detailError}</p>
			{:else if !selected}
				<p class="text-sm text-slate-500 dark:text-slate-400">Select a request to inspect it.</p>
			{:else}
				<div>
					<h2 class="text-lg font-semibold text-slate-900 dark:text-white">{selected.service}: {selected.operation}</h2>
					<p class="text-xs font-mono text-slate-500 break-all">{selected.id}</p>
				</div>
				<dl class="text-sm grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
					<dt class="text-slate-500">Time</dt><dd>{new Date(selected.time).toISOString()}</dd>
					<dt class="text-slate-500">Status</dt><dd>{selected.status}{selected.errorCode ? ` (${selected.errorCode})` : ''}</dd>
					<dt class="text-slate-500">Latency</dt><dd>{formatLatency(selected.latencyMs)}</dd>
					<dt class="text-slate-500">Region</dt><dd>{selected.region || '—'}</dd>
					<dt class="text-slate-500">Account</dt><dd>{selected.account || '—'}</dd>
					<dt class="text-slate-500">Principal</dt><dd class="break-all">{selected.principal || '—'}</dd>
					<dt class="text-slate-500">Resource</dt><dd class="break-all">{selected.resource || '—'}</dd>
					{#if selected.method}<dt class="text-slate-500">Request</dt><dd class="font-mono">{selected.method} {selected.path}</dd>{/if}
					<dt class="text-slate-500">Sizes</dt><dd>{formatBytes(selected.requestBytes)} in / {formatBytes(selected.responseBytes)} out</dd>
				</dl>

				{#if tree.length > 1}
					<div>
						<h4 class="text-xs font-semibold uppercase text-slate-500 dark:text-slate-400 mb-1">Trace</h4>
						<ul class="text-sm" id="apptrace-trace">
							{#each tree as row (row.span.id)}
								<li style="padding-left: {row.depth * 1.25}rem">
									<button class="text-left hover:underline {row.span.id === selected.id ? 'font-bold' : ''}" onclick={() => select(row.span.id)}>
										{row.depth > 0 ? '↳ ' : ''}{row.span.service}: {row.span.operation}
										<span class="text-xs text-slate-500">{row.span.status} · {formatLatency(row.span.latencyMs)}</span>
									</button>
								</li>
							{/each}
						</ul>
					</div>
				{/if}

				{#if selected.detail}
					{@render kv('Request headers', selected.detail.requestHeaders)}
					{#if selected.detail.query}{@render body('Query string', selected.detail.query)}{/if}
					{@render body('Request body', selected.detail.requestBody, selected.detail.requestBodyTruncated)}
					{@render kv('Response headers', selected.detail.responseHeaders)}
					{@render body('Response body', selected.detail.responseBody, selected.detail.responseBodyTruncated)}
					<p class="text-xs text-slate-400">Secrets (Authorization, tokens, passwords, secret keys) are redacted.</p>
				{:else if selected.kind === 'request'}
					<p class="text-sm text-slate-500 dark:text-slate-400" id="apptrace-no-detail">
						Headers and bodies were not captured for this request. Enable "Capture bodies" above and repeat the call.
					</p>
				{/if}
			{/if}
		</div>
	</div>
</div>
