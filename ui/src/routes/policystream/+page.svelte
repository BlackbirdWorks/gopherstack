<script lang="ts">
	import { onMount } from 'svelte';
	import {
		decisionKind,
		decisionLabel,
		describeBoundary,
		describePolicy,
		emptyFilters,
		matchesFilters,
		mergeEvents,
		parseEvent,
		suggestionJSON,
		suggestPolicy,
		type DecisionKind,
		type StreamEvent,
		type StreamFilters
	} from '#lib/iamstream.ts';

	const API = '/_gopherstack/iam/policy-stream';
	const CAP = 1000;

	let events = $state<StreamEvent[]>([]);
	let filters = $state<StreamFilters>({ ...emptyFilters });
	let paused = $state(false);
	let connected = $state(false);
	let error = $state<string | null>(null);
	let suggestion = $state<ReturnType<typeof suggestPolicy> | null>(null);
	let copied = $state(false);
	let source: EventSource | null = null;

	let visible = $derived(events.filter((e) => matchesFilters(e, filters)));
	let deniedCount = $derived(visible.filter((e) => e.decision === 'deny').length);

	const badge: Record<DecisionKind, string> = {
		allow: 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300',
		'explicit-deny': 'bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300',
		'implicit-deny': 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300'
	};

	function onMessage(m: MessageEvent<string>) {
		const ev = parseEvent(m.data);
		if (ev) events = mergeEvents(events, [ev], CAP);
	}

	function connect() {
		disconnect();
		source = new EventSource(`${API}?follow=1`);
		source.onopen = () => {
			connected = true;
			error = null;
		};
		source.onmessage = onMessage;
		source.onerror = () => {
			connected = false;
			error = 'Stream disconnected, retrying…';
		};
	}

	function disconnect() {
		source?.close();
		source = null;
		connected = false;
	}

	function togglePause() {
		paused = !paused;
		if (paused) disconnect();
		else connect();
	}

	async function clearAll() {
		try {
			const res = await fetch(API, { method: 'DELETE' });
			if (!res.ok) throw new Error(`Clear failed: ${res.status}`);
			events = [];
			suggestion = null;
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		}
	}

	function suggest() {
		suggestion = suggestPolicy(visible);
		copied = false;
	}

	async function copy() {
		if (!suggestion) return;
		try {
			await navigator.clipboard.writeText(suggestionJSON(suggestion));
			copied = true;
		} catch {
			error = 'Clipboard unavailable; select the JSON and copy manually.';
		}
	}

	onMount(() => {
		connect();
		return disconnect;
	});

	const inputCls =
		'px-3 py-1.5 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-800 dark:text-white text-sm';
</script>

<div class="space-y-4" id="policystream-page">
	<div class="flex flex-wrap items-end justify-between gap-3">
		<div>
			<h1 class="text-3xl font-bold text-slate-900 dark:text-white">IAM Policy Stream</h1>
			<p class="text-slate-600 dark:text-slate-300">
				Live IAM authorization decisions with the policy and statement that decided each one. Requires <code>--enforce-iam</code> or <code>--iam-audit</code>.
			</p>
		</div>
		<div class="flex flex-wrap items-center gap-3 text-sm text-slate-700 dark:text-slate-300">
			<span id="policystream-status" class="flex items-center gap-1.5">
				<span class="inline-block w-2 h-2 rounded-full {connected ? 'bg-emerald-500' : 'bg-slate-400'}"></span>
				{paused ? 'Paused' : connected ? 'Live' : 'Connecting…'}
			</span>
			<button id="policystream-pause" onclick={togglePause} class="px-3 py-1.5 border border-slate-300 dark:border-slate-600 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700">{paused ? 'Resume' : 'Pause'}</button>
			<button id="policystream-clear" onclick={clearAll} class="px-3 py-1.5 text-red-700 dark:text-red-300 border border-red-200 dark:border-red-900/50 rounded-lg hover:bg-red-50 dark:hover:bg-red-900/20">Clear</button>
			<button id="policystream-suggest" onclick={suggest} disabled={deniedCount === 0} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">Suggest policy ({deniedCount})</button>
		</div>
	</div>

	{#if error}
		<div id="policystream-error" class="p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg text-sm text-red-700 dark:text-red-300">{error}</div>
	{/if}

	<div class="flex flex-wrap gap-2">
		<input id="policystream-principal" type="search" placeholder="Filter principal…" bind:value={filters.principal} class="{inputCls} flex-1 min-w-64" aria-label="Principal" />
		<input id="policystream-action" type="search" placeholder="Filter action (s3:GetObject)…" bind:value={filters.action} class="{inputCls} w-64" aria-label="Action" />
		<select id="policystream-decision" bind:value={filters.decision} class={inputCls} aria-label="Decision">
			<option value="">Any decision</option>
			<option value="allow">Allow</option>
			<option value="explicit-deny">Explicit deny</option>
			<option value="implicit-deny">Implicit deny</option>
		</select>
	</div>

	<p class="text-xs text-slate-500 dark:text-slate-400" id="policystream-summary">{visible.length} shown of {events.length} received</p>

	{#if suggestion}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-indigo-200 dark:border-indigo-900/50 p-4 space-y-2" id="policystream-suggestion">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Suggested least-privilege policy ({suggestion.consumed} denied requests)</h2>
				<div class="flex gap-2">
					<button id="policystream-copy" onclick={copy} class="px-3 py-1.5 border border-slate-300 dark:border-slate-600 rounded-lg text-sm hover:bg-slate-100 dark:hover:bg-slate-700">{copied ? 'Copied' : 'Copy'}</button>
					<button onclick={() => (suggestion = null)} class="px-3 py-1.5 border border-slate-300 dark:border-slate-600 rounded-lg text-sm hover:bg-slate-100 dark:hover:bg-slate-700">Close</button>
				</div>
			</div>
			<pre class="text-xs font-mono p-2 bg-slate-50 dark:bg-slate-900 rounded border border-slate-200 dark:border-slate-700 overflow-auto max-h-64" id="policystream-policy-json">{suggestionJSON(suggestion)}</pre>
			{#if suggestion.blocked.length > 0}
				<div class="text-sm text-amber-700 dark:text-amber-300" id="policystream-blocked">
					<p class="font-semibold">Not fixable with an identity Allow (explicit deny or permissions boundary):</p>
					<ul class="list-disc ml-5 font-mono text-xs">
						{#each suggestion.blocked as b (b)}<li>{b}</li>{/each}
					</ul>
				</div>
			{/if}
		</div>
	{/if}

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-auto max-h-[70vh]">
		<table class="w-full text-sm" id="policystream-table">
			<thead class="sticky top-0 bg-slate-50 dark:bg-slate-900 text-left text-xs uppercase text-slate-500 dark:text-slate-400">
				<tr>
					<th class="px-3 py-2">Time</th>
					<th class="px-3 py-2">Principal</th>
					<th class="px-3 py-2">Action</th>
					<th class="px-3 py-2">Resource</th>
					<th class="px-3 py-2">Decision</th>
					<th class="px-3 py-2">Deciding policy</th>
					<th class="px-3 py-2">Sid</th>
					<th class="px-3 py-2">Permissions boundary</th>
					<th class="px-3 py-2">Reason</th>
				</tr>
			</thead>
			<tbody>
				{#each visible as e (e.seq)}
					{@const kind = decisionKind(e)}
					<tr data-testid="policystream-row" class="border-t border-slate-100 dark:border-slate-700 align-top">
						<td class="px-3 py-1.5 whitespace-nowrap text-slate-600 dark:text-slate-400">{new Date(e.timestamp).toLocaleTimeString()}</td>
						<td class="px-3 py-1.5 font-mono text-xs break-all">{e.principal}</td>
						<td class="px-3 py-1.5 font-medium text-slate-900 dark:text-white whitespace-nowrap">{e.action}</td>
						<td class="px-3 py-1.5 font-mono text-xs break-all">{e.resource}</td>
						<td class="px-3 py-1.5 whitespace-nowrap">
							<span class="px-1.5 py-0.5 rounded text-xs font-semibold {badge[kind]}">{decisionLabel(kind)}</span>
							{#if !e.enforced}<span class="ml-1 text-xs text-slate-500" title="Audit mode: request was not blocked">audit</span>{/if}
						</td>
						<td class="px-3 py-1.5 font-mono text-xs break-all">{describePolicy(e.policy) || '—'}</td>
						<td class="px-3 py-1.5 font-mono text-xs">{e.matchedSid || '—'}</td>
						<td class="px-3 py-1.5 font-mono text-xs break-all">{describeBoundary(e.boundary) || '—'}</td>
						<td class="px-3 py-1.5 text-xs text-slate-600 dark:text-slate-400">{e.reason || '—'}</td>
					</tr>
				{:else}
					<tr><td colspan="9" class="px-3 py-8 text-center text-slate-500 dark:text-slate-400">No decisions yet. Make some AWS API calls with enforcement on.</td></tr>
				{/each}
			</tbody>
		</table>
	</div>
</div>
