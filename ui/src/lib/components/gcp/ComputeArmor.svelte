<script lang="ts">
	import { untrack } from 'svelte';
	import GcpResourceTable from '#lib/components/gcp/GcpResourceTable.svelte';
	import { listSecurityPolicies, type SecurityPolicy } from '#lib/gcpcompute.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { project: string; refresh: number };
	let { project, refresh }: Props = $props();

	let policies = $state<SecurityPolicy[]>([]);
	let loading = $state(true);
	let error = $state('');

	const byName = $derived(new Map(policies.map((p) => [p.name, p])));
	const rows = $derived(policies.map((p) => ({ id: p.name, cells: [p.name, p.type, p.description, String(p.rules.length)], raw: p.raw })));

	async function load() {
		loading = true;
		try {
			policies = await listSecurityPolicies(project);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		void project;
		void refresh;
		untrack(() => void load());
	});
</script>

<GcpResourceTable testId="compute-armor" noun="security policies" columns={['Name', 'Type', 'Description', 'Rules']} {rows} {loading} {error} onRetry={() => load()}>
	{#snippet detail(row)}
		{@const p = byName.get(row.id)}
		{#if p}
			<table class="w-full text-xs" data-testid="compute-armor-rules">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="py-1 pr-3">Priority</th><th class="py-1 pr-3">Action</th><th class="py-1 pr-3">Match</th><th class="py-1 pr-3">Description</th><th class="py-1">Preview</th></tr></thead>
				<tbody>
					{#each p.rules as r (r.priority)}
						<tr data-testid="compute-armor-rule"><td class="py-1 pr-3 font-mono">{r.priority}</td><td class="py-1 pr-3">{r.action}</td><td class="py-1 pr-3 font-mono break-all">{r.match || '—'}</td><td class="py-1 pr-3">{r.description || '—'}</td><td class="py-1">{r.preview ? 'yes' : 'no'}</td></tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{/snippet}
</GcpResourceTable>
