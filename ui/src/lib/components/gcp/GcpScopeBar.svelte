<script lang="ts">
	type Props = {
		prefix: string;
		project: string;
		location?: string;
		hint: string;
		onApply: (project: string, location: string) => void;
	};
	let { prefix, project, location, hint, onApply }: Props = $props();

	let projectInput = $state('');
	let locationInput = $state('');
	$effect(() => {
		projectInput = project;
		locationInput = location ?? '';
	});

	const box = 'rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs';
	const unchanged = $derived(projectInput.trim() === project && (location === undefined || locationInput.trim() === location));
	const invalid = $derived(!projectInput.trim() || (location !== undefined && !locationInput.trim()));

	function submit(event: Event) {
		event.preventDefault();
		if (invalid || unchanged) return;
		onApply(projectInput.trim(), locationInput.trim());
	}
</script>

<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={submit}>
	<label for="{prefix}-project">Project</label>
	<input id="{prefix}-project" class="w-48 {box}" bind:value={projectInput} data-testid="{prefix}-project" />
	{#if location !== undefined}
		<label for="{prefix}-location">Location</label>
		<input id="{prefix}-location" class="w-32 {box}" bind:value={locationInput} data-testid="{prefix}-location" />
	{/if}
	<button type="submit" class="rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={invalid || unchanged}>Switch</button>
	<span>{hint}</span>
</form>
