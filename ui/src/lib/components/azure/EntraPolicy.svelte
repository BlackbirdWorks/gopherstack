<script lang="ts">
	import { onMount } from 'svelte';
	import { listPolicyAssignments, listPolicyDefinitions, type PolicyAssignment, type PolicyDefinition } from '#lib/azureentra.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { scope: string; subscription: string };
	let { scope, subscription }: Props = $props();

	let defs = $state<PolicyDefinition[]>([]);
	let assigns = $state<PolicyAssignment[]>([]);
	let loading = $state(true);
	let error = $state('');
	let kind = $state('');

	const shown = $derived(kind ? defs.filter((d) => d.policyType === kind) : defs);

	async function load() {
		try {
			[defs, assigns] = await Promise.all([listPolicyDefinitions(`/subscriptions/${subscription}`), listPolicyAssignments(scope)]);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	const cell = 'px-4 py-3';
	onMount(load);
</script>

<div class="space-y-6" data-testid="entra-policy">
	{#if error}
		<p class="text-sm text-red-600" role="alert" data-testid="entra-policy-error">{error}</p>
	{:else if loading}
		<p class="text-sm text-gray-500">Loading…</p>
	{:else}
		<section class="space-y-2">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Assignments</h2>
			{#if assigns.length === 0}
				<p class="text-sm text-gray-500" data-testid="entra-policy-assign-empty">No policy assignments at this scope.</p>
			{:else}
				<table class="w-full text-sm" data-testid="entra-policy-assignments">
					<thead class="text-left text-gray-500"><tr><th class={cell}>Name</th><th class={cell}>Definition</th><th class={cell}>Scope</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each assigns as a (a.name)}
							<tr data-testid="entra-policy-assignment"><td class={cell}>{a.displayName || a.name}</td><td class="{cell} font-mono text-xs">{a.definition}</td><td class="{cell} font-mono text-xs break-all">{a.scope}</td></tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</section>
		<section class="space-y-2">
			<div class="flex items-center gap-3">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Definitions</h2>
				<select class="rounded-md border border-gray-300 bg-white px-2 py-1 text-xs dark:border-gray-600 dark:bg-slate-800" bind:value={kind} aria-label="Policy type" data-testid="entra-policy-kind">
					<option value="">All</option>
					<option value="BuiltIn">Built-in</option>
					<option value="Custom">Custom</option>
				</select>
				<span class="text-xs text-gray-500" data-testid="entra-policy-count">{shown.length} shown</span>
			</div>
			<table class="w-full text-sm" data-testid="entra-policy-definitions">
				<thead class="text-left text-gray-500"><tr><th class={cell}>Name</th><th class={cell}>Type</th><th class={cell}>Mode</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each shown as d (d.name)}
						<tr data-testid="entra-policy-definition"><td class={cell}>{d.displayName || d.name}</td><td class="{cell} text-xs">{d.policyType}</td><td class="{cell} text-xs">{d.mode || '—'}</td></tr>
					{/each}
				</tbody>
			</table>
		</section>
	{/if}
</div>
