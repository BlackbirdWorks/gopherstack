<script lang="ts">
	import Modal from '#lib/components/Modal.svelte';
	import { GCP_GHOST, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { trafficRows, validateTraffic, type RevisionInfo, type ServiceInfo, type TrafficInput } from '#lib/gcprun.ts';

	let { busy, onSave }: { busy: boolean; onSave: (svc: ServiceInfo, rows: TrafficInput[]) => void } = $props();

	let modal = $state<Modal | null>(null);
	let svc = $state<ServiceInfo | null>(null);
	let rows = $state<(TrafficInput & { label: string })[]>([]);

	const problem = $derived(validateTraffic(rows));
	const total = $derived(rows.reduce((a, r) => a + (Number.isFinite(r.percent) ? r.percent : 0), 0));

	export function open(service: ServiceInfo, revisions: RevisionInfo[]) {
		svc = service;
		rows = trafficRows(service, revisions);
		modal?.open();
	}

	export function close() {
		modal?.close('done');
	}

	function evenSplit() {
		const active = rows.filter((r) => r.percent > 0);
		const set = active.length > 0 ? active : rows.slice(0, 1);
		const base = Math.floor(100 / set.length);
		const extra = 100 - base * set.length;
		rows = rows.map((r) => {
			const i = set.indexOf(r);
			return { ...r, percent: i < 0 ? 0 : base + (i < extra ? 1 : 0) };
		});
	}

	function submit(event: Event) {
		event.preventDefault();
		if (svc && !problem) onSave(svc, rows);
	}
</script>

<Modal bind:this={modal} title={svc ? `Split traffic for ${svc.id}` : 'Split traffic'} titleId="run-traffic-title">
	{#snippet children()}
		<form id="run-traffic-form" class="space-y-3" onsubmit={submit} novalidate>
			<ul class="space-y-2">
				{#each rows as r (r.label)}
					<li class="flex items-center gap-3 text-sm" data-testid="run-traffic-row">
						<span class="min-w-0 flex-1 truncate font-mono text-xs" title={r.label}>{r.label}</span>
						<input type="number" min="0" max="100" step="1" class="w-20 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 text-sm" bind:value={r.percent} aria-label="Percent for {r.label}" />
						<span class="text-gray-500">%</span>
					</li>
				{/each}
			</ul>
			<div class="flex items-center justify-between text-xs">
				<button type="button" class="underline" onclick={evenSplit} data-testid="run-traffic-even">Split evenly</button>
				<span class={problem ? 'text-red-600 dark:text-red-400' : 'text-green-700 dark:text-green-400'} data-testid="run-traffic-total">Total {total}%</span>
			</div>
			{#if problem}<p class="text-xs text-red-600 dark:text-red-400">{problem}</p>{/if}
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={GCP_GHOST} onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="run-traffic-form" class={GCP_PRIMARY} disabled={busy || Boolean(problem)} data-testid="run-traffic-submit">{busy ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
