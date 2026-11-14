<script lang="ts">
	import { onMount } from 'svelte';
	import { Workflow as WorkflowIcon, RefreshCw } from 'lucide-svelte';
	import LogicWorkflowDetail from './LogicWorkflowDetail.svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import { getSubscriptionId, listResourceGroups } from '#lib/azurearm.ts';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listWorkflows, setWorkflowEnabled, type Workflow } from '#lib/azurelogic.ts';

	let subscription = $state('');
	let groups = $state<string[]>([]);
	let group = $state('');
	let workflows = $state<Workflow[]>([]);
	let selected = $state('');
	let loading = $state(true);
	let error = $state('');

	const workflow = $derived(workflows.find((w) => w.name === selected));
	const target = $derived({ subscription, group, workflow: selected });

	async function loadWorkflows() {
		if (!group) {
			workflows = [];
			return;
		}
		try {
			workflows = await listWorkflows(subscription, group);
			error = '';
			if (!workflows.some((w) => w.name === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			groups = (await listResourceGroups(subscription)).map((g) => g.name).toSorted();
			if (!groups.includes(group)) group = groups[0] ?? '';
			await loadWorkflows();
		} catch (e) {
			error = describeGcpError(e);
			groups = [];
		} finally {
			loading = false;
		}
	}

	function pickGroup() {
		selected = '';
		void loadWorkflows();
	}

	function select(w: Workflow) {
		selected = selected === w.name ? '' : w.name;
	}

	async function toggle(w: Workflow) {
		const enable = w.state !== 'Enabled';
		const run = () => setWorkflowEnabled({ subscription, group, workflow: w.name }, enable);
		const verb = enable ? 'Enabled' : 'Disabled';
		const ok = enable
			? await attempt(run, `${verb} ${w.name}`, `Failed to enable ${w.name}`)
			: await confirmedAttempt(
					{ title: 'Disable workflow', message: `Disable "${w.name}"? Its triggers stop firing.`, confirmLabel: 'Disable' },
					run,
					`${verb} ${w.name}`,
					`Failed to disable ${w.name}`
				);
		if (ok) await loadWorkflows();
	}

	const cell = 'px-4 py-3';
	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="logic-dashboard">
	<PageHeader icon={WorkflowIcon} title="Logic Apps" description="Azure Logic Apps workflows, triggers and runs with action timelines" color="sky" onRefresh={load} />

	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if groups.length === 0}
		{#if error}
			<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="logic-error">
				<p class="font-medium">Could not load resource groups</p>
				<p>{error}</p>
				<button class="mt-2 underline" onclick={load}>Retry</button>
			</div>
		{:else}
			<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="logic-empty">No resource groups. Create one in Resource Manager first.</p>
		{/if}
	{:else}
		<div class="flex flex-wrap items-center gap-2 text-xs text-gray-500">
			<span>Subscription</span><span class="font-mono" data-testid="logic-subscription">{subscription}</span>
			<label for="logic-group" class="ml-4">Resource group</label>
			<select id="logic-group" class="{GCP_INPUT} !w-56 !py-1 text-xs" bind:value={group} onchange={pickGroup} data-testid="logic-group">
				{#each groups as g (g)}<option value={g}>{g}</option>{/each}
			</select>
		</div>

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="logic-list-error">{error}</p>{/if}

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="logic-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>State</th><th class={cell}>Location</th><th class={cell}>Version</th><th class={cell}>Changed</th><th class="{cell} text-right">Actions</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each workflows as w (w.name)}
						<tr class={selected === w.name ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="logic-workflow">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => select(w)} aria-expanded={selected === w.name} data-testid="logic-select">{w.name}</button></td>
							<td class="{cell} text-xs" data-testid="logic-state">{w.state}</td>
							<td class="{cell} text-xs">{w.location}</td>
							<td class="{cell} font-mono text-xs">{w.version}</td>
							<td class="{cell} text-xs">{when(w.changed)}</td>
							<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => toggle(w)} data-testid="logic-toggle">{w.state === 'Enabled' ? 'Disable' : 'Enable'}</button></td>
						</tr>
					{:else}
						<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="logic-workflows-empty">No workflows in this resource group.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if workflow}
			{#key selected}<LogicWorkflowDetail {workflow} {target} />{/key}
		{/if}
	{/if}
</div>
