<script lang="ts">
	import { onDestroy } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { RefreshCw } from 'lucide-svelte';
	import JsonView from './JsonView.svelte';
	import { attempt, copyText, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		isRequestTrigger,
		listTriggers,
		revealCallback,
		runTrigger,
		validatePayload,
		type Callback,
		type Invocation,
		type RunTarget,
		type Trigger
	} from '#lib/azurelogic.ts';

	let {
		target,
		definition,
		enabled,
		onRan
	}: { target: RunTarget; definition: Record<string, unknown>; enabled: boolean; onRan: (runId: string) => void } = $props();

	let triggers = $state<Trigger[]>([]);
	let loading = $state(true);
	let error = $state('');
	let payloads = $state<Record<string, string>>({});
	let revealed = $state<Record<string, Callback>>({});
	let results = $state<Record<string, Invocation>>({});
	let running = $state('');

	async function load() {
		try {
			triggers = await listTriggers(target, definition);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function run(t: Trigger) {
		const payload = payloads[t.name] ?? '';
		if (isRequestTrigger(t.type) && validatePayload(payload)) return;
		running = t.name;
		let reply: Invocation | undefined;
		const ok = await attempt(
			async () => {
				reply = await runTrigger(target, t, payload);
			},
			`Trigger ${t.name} ran`,
			`Failed to run ${t.name}`
		);
		running = '';
		if (!ok) return;
		if (reply) results = { ...results, [t.name]: reply };
		await load();
		onRan(reply?.runId ?? '');
	}

	async function reveal(t: Trigger) {
		try {
			revealed = { ...revealed, [t.name]: await revealCallback(target, t.name) };
		} catch (e) {
			toast.error(`Failed to get the callback URL: ${describeGcpError(e)}`);
		}
	}

	function hide(name: string) {
		const { [name]: _gone, ...rest } = revealed;
		revealed = rest;
	}

	onDestroy(() => {
		revealed = {};
	});

	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');

	$effect(() => {
		void target.workflow;
		void load();
	});
</script>

<div class="space-y-4" data-testid="logic-triggers">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="logic-triggers-error">Could not load triggers: {error}</p>{/if}
	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if triggers.length === 0}
		<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="logic-triggers-empty">This workflow has no triggers.</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="logic-trigger-table">
				<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Type</th><th class={cell}>State</th><th class={cell}>Schedule</th><th class={cell}>Last run</th><th class="{cell} text-right">Actions</th></tr></thead>
				{#each triggers as t (t.name)}
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						<tr data-testid="logic-trigger">
							<td class="{cell} font-mono text-xs" data-testid="logic-trigger-name">{t.name}</td>
							<td class="{cell} text-xs" data-testid="logic-trigger-type">{t.type || '—'}</td>
							<td class="{cell} text-xs">{t.state}</td>
							<td class="{cell} text-xs">{t.recurrence || (isRequestTrigger(t.type) ? 'on request' : '—')}{t.next ? ` · next ${when(t.next)}` : ''}</td>
							<td class="{cell} text-xs">{when(t.last)}</td>
							<td class="{cell} text-right whitespace-nowrap">
								<button class={GCP_MINI} disabled={!enabled || running === t.name || (isRequestTrigger(t.type) && validatePayload(payloads[t.name] ?? '') !== '')} onclick={() => run(t)} data-testid="logic-trigger-run">Run trigger</button>
								{#if isRequestTrigger(t.type)}
									{#if revealed[t.name]}
										<button class={GCP_MINI} onclick={() => hide(t.name)} data-testid="logic-callback-hide">Hide callback URL</button>
									{:else}
										<button class={GCP_MINI} onclick={() => reveal(t)} data-testid="logic-callback-reveal">Show callback URL</button>
									{/if}
								{/if}
							</td>
						</tr>
						{#if isRequestTrigger(t.type)}
							<tr>
								<td colspan="6" class="{cell} space-y-2">
									<label class="block text-xs text-gray-500">Request body (JSON, optional)
										<textarea class="{GCP_INPUT} mt-1 !py-1 font-mono text-xs" rows="2" placeholder={'{"name":"world"}'} bind:value={payloads[t.name]} data-testid="logic-trigger-payload"></textarea>
									</label>
									{#if validatePayload(payloads[t.name] ?? '')}<p class="text-xs text-red-600" data-testid="logic-payload-error">{validatePayload(payloads[t.name] ?? '')}</p>{/if}
									{#if revealed[t.name]}
										<div class="space-y-1 rounded-md border border-amber-300 bg-amber-50 p-2 text-xs dark:border-amber-800 dark:bg-amber-950/30" data-testid="logic-callback">
											<p class="font-medium">Callback URL ({revealed[t.name].method}) - contains a shared access signature, treat it as a secret</p>
											<p class="font-mono break-all" data-testid="logic-callback-url">{revealed[t.name].url}</p>
											<button class={GCP_MINI} onclick={() => copyText(revealed[t.name].url)} data-testid="logic-callback-copy">Copy</button>
										</div>
									{:else}
										<p class="font-mono text-xs text-gray-500" data-testid="logic-callback-masked">Callback URL hidden. The signature is only fetched when you choose to show it.</p>
									{/if}
									{#if results[t.name]}
										<div class="space-y-1 text-xs" data-testid="logic-trigger-result">
											<p>Response <span class="font-mono" data-testid="logic-result-status">{results[t.name].status}</span> · run <span class="font-mono" data-testid="logic-result-run">{results[t.name].runId || 'none'}</span></p>
											<JsonView value={results[t.name].body} testid="logic-result-body" />
										</div>
									{/if}
								</td>
							</tr>
						{/if}
					</tbody>
				{/each}
			</table>
		</div>
		{#if !enabled}<p class="text-xs text-gray-500" data-testid="logic-disabled-hint">The workflow is disabled; enable it to run triggers.</p>{/if}
	{/if}
</div>
