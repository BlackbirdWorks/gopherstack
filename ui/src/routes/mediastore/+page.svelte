<script lang="ts">
	import { onMount } from 'svelte';
	import { getMediaStoreClient } from '$lib/aws-client';
	import { ListContainersCommand, CreateContainerCommand, type Container } from '@aws-sdk/client-mediastore';
	import { toast } from 'svelte-sonner';

	const mediaStore = getMediaStoreClient();

	let loading = $state(false);
	let creating = $state(false);
	let containers = $state<Container[]>([]);
	let containerName = $state('');

	async function loadContainers() {
		loading = true;
		try {
			const out = await mediaStore.send(new ListContainersCommand({ MaxResults: 100 }));
			containers = out.Containers ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to list containers: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createContainer() {
		if (!containerName.trim()) {
			return;
		}

		creating = true;
		try {
			await mediaStore.send(new CreateContainerCommand({ ContainerName: containerName.trim() }));
			toast.success(`Container "${containerName.trim()}" created`);
			containerName = '';
			await loadContainers();
		} catch (err: unknown) {
			toast.error(`Failed to create container: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	onMount(() => {
		void loadContainers();
	});
</script>

<div class="space-y-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">MediaStore</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Container management</p>
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="flex flex-col gap-3 sm:flex-row">
			<input
				type="text"
				bind:value={containerName}
				placeholder="Container name"
				class="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 dark:border-slate-600 dark:bg-slate-900 dark:text-white"
			/>
			<button
				type="button"
				onclick={createContainer}
				disabled={creating}
				class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>
				{creating ? 'Creating...' : 'Create Container'}
			</button>
		</div>
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		{#if loading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading containers...</p>
		{:else if containers.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No containers found</p>
		{:else}
			<div class="space-y-2">
				{#each containers as container}
					<div class="rounded-lg border border-slate-200 p-3 text-sm dark:border-slate-700">
						<div class="font-medium text-slate-900 dark:text-white">{container.Name}</div>
					</div>
				{/each}
			</div>
		{/if}
	</div>
</div>
