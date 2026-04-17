<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';

	let loading = $state(false);
	let objectPaths = $state<string[]>([]);

	async function loadItems() {
		loading = true;
		try {
			const response = await fetch('/dashboard/api/mediastoredata/objects');
			if (!response.ok) {
				throw new Error(`request failed with status ${response.status}`);
			}

			const payload = await response.json() as { objects?: string[] };
			objectPaths = payload.objects ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to list items: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		void loadItems();
	});
</script>

<div class="space-y-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">MediaStore Data</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Object browser</p>
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		{#if loading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading objects...</p>
		{:else if objectPaths.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No objects stored.</p>
		{:else}
			<div class="space-y-2">
				{#each objectPaths as objectPath}
					<div class="rounded-lg border border-slate-200 p-3 text-sm dark:border-slate-700">
						<div class="font-medium text-slate-900 dark:text-white">{objectPath}</div>
					</div>
				{/each}
			</div>
		{/if}
	</div>
</div>
