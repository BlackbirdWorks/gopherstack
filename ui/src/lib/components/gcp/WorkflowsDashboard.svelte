<script lang="ts">
	import { onMount } from 'svelte';
	import { GitBranch, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { attempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import {
		cancelExecution,
		getWorkflow,
		listExecutions,
		listRevisions,
		listWorkflows,
		prettyJson,
		readStoredWfLocation,
		readStoredWfProject,
		runExecution,
		validateArgument,
		writeStoredWfScope,
		type ExecutionInfo,
		type WorkflowInfo
	} from '#lib/gcpworkflows.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'executions', label: 'Executions' },
		{ id: 'source', label: 'Source' },
		{ id: 'revisions', label: 'Revisions' }
	];

	let project = $state(readStoredWfProject());
	let location = $state(readStoredWfLocation());
	let workflows = $state<WorkflowInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let tab = $state('executions');
	let executions = $state<ExecutionInfo[]>([]);
	let revisions = $state<WorkflowInfo[]>([]);
	let source = $state<WorkflowInfo | null>(null);
	let sourceRevision = $state('');
	let childLoading = $state(false);
	let childError = $state('');
	let argument = $state('{\n  "name": "world"\n}');
	let running = $state(false);
	let expanded = $state('');

	async function load() {
		try {
			workflows = (await listWorkflows({ project, location })).toSorted((a, b) => a.id.localeCompare(b.id));
			error = '';
			if (!workflows.some((w) => w.name === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadChild() {
		if (!selected) return;
		childLoading = true;
		childError = '';
		try {
			if (tab === 'executions') executions = await listExecutions(selected);
			else if (tab === 'revisions') revisions = await listRevisions(selected);
			else source = await getWorkflow(selected, sourceRevision);
		} catch (e) {
			childError = describeGcpError(e);
		} finally {
			childLoading = false;
		}
	}

	function applyScope(p: string, l: string) {
		project = p;
		location = l;
		writeStoredWfScope(p, l);
		selected = '';
		loading = true;
		void load();
	}

	function selectWorkflow(row: ResourceRow) {
		selected = selected === row.id ? '' : row.id;
		sourceRevision = '';
		tab = 'executions';
		void loadChild();
	}

	function selectTab(id: string) {
		tab = id;
		void loadChild();
	}

	function viewRevision(rev: WorkflowInfo) {
		sourceRevision = rev.revisionId;
		tab = 'source';
		void loadChild();
	}

	async function run() {
		const invalid = validateArgument(argument);
		if (invalid) {
			childError = `Argument is not valid JSON: ${invalid}`;
			return;
		}
		running = true;
		if (await attempt(() => runExecution(selected, argument), 'Execution started', 'Failed to start execution')) await loadChild();
		running = false;
	}

	async function cancel(e: ExecutionInfo) {
		if (await attempt(() => cancelExecution(e.name), `Cancelled ${e.id}`, `Failed to cancel ${e.id}`)) await loadChild();
	}

	const rows = $derived(workflows.map((w) => ({ id: w.name, cells: [w.id, w.state, w.revisionId, w.serviceAccount || '—', w.updateTime], raw: w.raw })));
	const stateClass = (s: string) =>
		s === 'SUCCEEDED' ? 'bg-green-100 text-green-800' : s === 'FAILED' ? 'bg-red-100 text-red-800' : s === 'ACTIVE' ? 'bg-blue-100 text-blue-800' : 'bg-gray-100 text-gray-700';
	const cell = 'px-4 py-2';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="workflows-dashboard">
	<PageHeader icon={GitBranch} title="Workflows" description="Google Cloud Workflows: workflows, revisions, source and executions" color="blue" onRefresh={load} />
	<GcpScopeBar prefix="workflows" {project} {location} hint="Workflows are listed per project and location." onApply={applyScope} />
	<GcpResourceTable testId="workflows" noun="workflows" columns={['Workflow', 'State', 'Revision', 'Service account', 'Updated']} {rows} {loading} {error} {selected} onRetry={load} onSelect={selectWorkflow} />

	{#if selected}
		<div class="space-y-3" data-testid="workflows-detail">
			<Tabs tabs={TABS} active={tab} onSelect={selectTab} color="blue" />
			{#if childError}<p class="text-sm text-red-600" role="alert" data-testid="workflows-detail-error">{childError}</p>{/if}

			{#if tab === 'executions'}
				<div class="space-y-2 rounded-lg border border-gray-200 p-3 dark:border-gray-700" data-testid="workflows-run">
					<label class="block text-xs font-medium" for="workflows-arg">Run with argument (JSON, optional)</label>
					<textarea id="workflows-arg" class="{GCP_INPUT} h-24 font-mono text-xs" bind:value={argument} data-testid="workflows-arg"></textarea>
					<button class={GCP_PRIMARY} onclick={run} disabled={running} data-testid="workflows-run-button">Run execution</button>
				</div>
				{#if childLoading && executions.length === 0}
					<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
				{:else if executions.length === 0}
					<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="workflows-exec-empty">No executions</p>
				{:else}
					<table class="w-full text-sm" data-testid="workflows-executions">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Execution</th><th class={cell}>State</th><th class={cell}>Started</th><th class={cell}>Ended</th><th class="{cell} text-right">Actions</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each executions as e (e.name)}
								<tr data-testid="workflows-execution">
									<td class="{cell} font-mono text-xs">{e.id}</td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {stateClass(e.state)}" data-testid="workflows-exec-state">{e.state}</span></td>
									<td class="{cell} text-xs">{e.startTime}</td><td class="{cell} text-xs">{e.endTime || '—'}</td>
									<td class="{cell} text-right whitespace-nowrap space-x-1">
										<button class={GCP_MINI} onclick={() => (expanded = expanded === e.name ? '' : e.name)} data-testid="workflows-exec-details">Details</button>
										{#if e.state === 'ACTIVE'}<button class={GCP_MINI} onclick={() => cancel(e)} data-testid="workflows-exec-cancel">Cancel</button>{/if}
									</td>
								</tr>
								{#if expanded === e.name}
									<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="workflows-exec-detail">
										<td colspan="5" class="space-y-2 px-4 py-3 text-xs">
											<p class="font-medium">Argument</p><pre class="max-h-40 overflow-auto rounded-md bg-white p-2 font-mono dark:bg-slate-900">{prettyJson(e.argument) || '—'}</pre>
											{#if e.state === 'FAILED'}
												<p class="font-medium text-red-700">Error</p><pre class="max-h-40 overflow-auto rounded-md bg-white p-2 font-mono dark:bg-slate-900" data-testid="workflows-exec-error">{prettyJson(e.errorPayload) || e.errorContext || 'unknown error'}</pre>
												{#if e.errorContext}<p class="font-mono">{e.errorContext}</p>{/if}
											{:else}
												<p class="font-medium">Result</p><pre class="max-h-40 overflow-auto rounded-md bg-white p-2 font-mono dark:bg-slate-900" data-testid="workflows-exec-result">{prettyJson(e.result) || '—'}</pre>
											{/if}
										</td>
									</tr>
								{/if}
							{/each}
						</tbody>
					</table>
				{/if}
			{:else if tab === 'source'}
				{#if childLoading}
					<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
				{:else if source}
					<p class="text-xs text-gray-500">Revision {source.revisionId}{sourceRevision ? '' : ' (current)'}</p>
					<pre class="max-h-[28rem] overflow-auto rounded-md border border-gray-200 bg-white p-3 font-mono text-xs dark:border-gray-700 dark:bg-slate-900" data-testid="workflows-source">{source.source || '(no inline source)'}</pre>
				{/if}
			{:else if childLoading}
				<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
			{:else if revisions.length === 0}
				<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="workflows-rev-empty">No revisions</p>
			{:else}
				<table class="w-full text-sm" data-testid="workflows-revisions">
					<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Revision</th><th class={cell}>Created</th><th class="{cell} text-right">Source</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each revisions as r (r.revisionId)}
							<tr data-testid="workflows-revision"><td class="{cell} font-mono text-xs">{r.revisionId}</td><td class="{cell} text-xs">{r.updateTime || r.createTime}</td><td class="{cell} text-right"><button class={GCP_MINI} onclick={() => viewRevision(r)} data-testid="workflows-view-revision">View source</button></td></tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>
	{/if}
</div>
