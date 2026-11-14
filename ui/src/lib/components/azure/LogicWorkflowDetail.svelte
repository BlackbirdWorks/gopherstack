<script lang="ts">
	import JsonView from './JsonView.svelte';
	import LogicRuns from './LogicRuns.svelte';
	import LogicTriggers from './LogicTriggers.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import type { RunTarget, Workflow } from '#lib/azurelogic.ts';

	const TABS = [
		{ id: 'definition', label: 'Definition' },
		{ id: 'triggers', label: 'Triggers' },
		{ id: 'runs', label: 'Runs' }
	];

	let { workflow, target }: { workflow: Workflow; target: RunTarget } = $props();

	let tab = $state('definition');
	let focusRun = $state('');

	function ran(runId: string) {
		focusRun = runId;
		tab = 'runs';
	}
</script>

<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="logic-detail">
	<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{workflow.name}</h2>
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="sky" />
	{#if tab === 'definition'}
		<JsonView value={workflow.definition} testid="logic-definition" />
	{:else if tab === 'triggers'}
		<LogicTriggers {target} definition={workflow.definition} enabled={workflow.state === 'Enabled'} onRan={ran} />
	{:else}
		<LogicRuns {target} focus={focusRun} />
	{/if}
</div>
