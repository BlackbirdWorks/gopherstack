<script lang="ts">
	import { Braces, TriangleAlert } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import FunctionsList from '#lib/components/gcp/FunctionsList.svelte';
	import { readStoredFnLocation, readStoredFnProject, writeStoredFnScope } from '#lib/gcpfunctions.ts';

	let project = $state(readStoredFnProject());
	let location = $state(readStoredFnLocation());
	let projectInput = $state(readStoredFnProject());
	let locationInput = $state(readStoredFnLocation());
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
		writeStoredFnScope(p, l);
	}
</script>

<div class="space-y-6 pb-20" data-testid="functions-dashboard">
	<PageHeader icon={Braces} title="Cloud Functions" description="Google Cloud Functions (2nd gen): source-deployed HTTP and Pub/Sub functions" color="blue" onRefresh={() => (tick += 1)}>
		{#snippet actions()}
			<label class="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300">
				<input type="checkbox" bind:checked={autoRefresh} data-testid="fn-auto-refresh" /> Auto-refresh
			</label>
		{/snippet}
	</PageHeader>

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyScope}>
		<label for="fn-project">Project</label>
		<input id="fn-project" class="w-48 {box}" bind:value={projectInput} data-testid="fn-project" />
		<label for="fn-location">Location</label>
		<input id="fn-location" class="w-32 {box}" bind:value={locationInput} data-testid="fn-location" />
		<button type="submit" class="rounded-md border border-gray-200 px-2 py-1 hover:bg-gray-50 disabled:opacity-50 dark:border-gray-700 dark:hover:bg-slate-800" disabled={!projectInput.trim() || !locationInput.trim() || (projectInput.trim() === project && locationInput.trim() === location)}>Switch</button>
		<span>Functions are listed per project and location.</span>
	</form>

	{#if noRuntime}
		<div class="flex items-start gap-3 rounded-lg border border-amber-300 bg-amber-50 p-4 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-200" role="status" data-testid="fn-no-runtime">
			<TriangleAlert class="mt-0.5 h-4 w-4 shrink-0" />
			<p><span class="font-medium">Container runtime unavailable.</span> Functions are registered but cannot serve requests or events. Start Docker (or a compatible runtime) and redeploy.</p>
		</div>
	{/if}

	<FunctionsList {scope} {tick} {autoRefresh} onRuntime={(down) => (noRuntime = down)} />
</div>
