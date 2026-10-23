<script lang="ts">
	import { Rocket, TriangleAlert } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import RunJobs from '#lib/components/gcp/RunJobs.svelte';
	import RunServices from '#lib/components/gcp/RunServices.svelte';
	import { readStoredRunLocation, readStoredRunProject, writeStoredRunScope } from '#lib/gcprun.ts';

	const TABS = [
		{ id: 'services', label: 'Services' },
		{ id: 'jobs', label: 'Jobs' }
	];

	let project = $state(readStoredRunProject());
	let location = $state(readStoredRunLocation());
	let projectInput = $state(readStoredRunProject());
	let locationInput = $state(readStoredRunLocation());
	let tab = $state('services');
	let tick = $state(0);
	let autoRefresh = $state(true);
	let noRuntime = $state(false);

	const scope = $derived({ project, location });
	const box = 'rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs';

	function applyScope(event: Event) {
		event.preventDefault();
		const p = projectInput.trim();
		const l = locationInput.trim();
		if (!p || !l) return;
		project = p;
		location = l;
		writeStoredRunScope(p, l);
	}

	function switchTab(id: string) {
		tab = id;
		noRuntime = false;
	}
</script>

<div class="space-y-6 pb-20" data-testid="run-dashboard">
	<PageHeader icon={Rocket} title="Cloud Run" description="Google Cloud Run: container-backed services, revisions and jobs" color="blue" onRefresh={() => (tick += 1)}>
		{#snippet actions()}
			<label class="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300">
				<input type="checkbox" bind:checked={autoRefresh} data-testid="run-auto-refresh" /> Auto-refresh
			</label>
		{/snippet}
	</PageHeader>

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyScope}>
		<label for="run-project">Project</label>
		<input id="run-project" class="w-48 {box}" bind:value={projectInput} data-testid="run-project" />
		<label for="run-location">Location</label>
		<input id="run-location" class="w-32 {box}" bind:value={locationInput} data-testid="run-location" />
		<button type="submit" class="rounded-md border border-gray-200 px-2 py-1 hover:bg-gray-50 disabled:opacity-50 dark:border-gray-700 dark:hover:bg-slate-800" disabled={!projectInput.trim() || !locationInput.trim() || (projectInput.trim() === project && locationInput.trim() === location)}>Switch</button>
		<span>Services and jobs are listed per project and location.</span>
	</form>

	{#if noRuntime}
		<div class="flex items-start gap-3 rounded-lg border border-amber-300 bg-amber-50 p-4 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-200" role="status" data-testid="run-no-runtime">
			<TriangleAlert class="mt-0.5 h-4 w-4 shrink-0" />
			<p><span class="font-medium">Container runtime unavailable.</span> Services are registered but return 503 from their URL, and job tasks fail immediately. Start Docker (or a compatible runtime) and redeploy.</p>
		</div>
	{/if}

	<Tabs tabs={TABS} active={tab} onSelect={switchTab} color="blue" />
	<div role="tabpanel" id="tabpanel-{tab}" aria-labelledby="tab-{tab}">
		{#if tab === 'services'}
			<RunServices {scope} {tick} {autoRefresh} onRuntime={(down) => (noRuntime = down)} />
		{:else}
			<RunJobs {scope} {tick} {autoRefresh} onRuntime={(down) => (noRuntime = down)} />
		{/if}
	</div>
</div>
