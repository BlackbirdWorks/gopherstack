<script lang="ts">
	import { onMount } from 'svelte';
	import { getResourceGroupsTaggingAPIClient } from '$lib/aws-client';
	import { GetResourcesCommand } from '@aws-sdk/client-resource-groups-tagging-api';
	import { toast } from 'svelte-sonner';

	const tagging = getResourceGroupsTaggingAPIClient();
	let resources = $state<Array<{ ResourceARN?: string; Tags?: Array<{ Key?: string; Value?: string }> }>>([]);

	async function loadResources() {
		try {
			const out = await tagging.send(new GetResourcesCommand({}));
			resources = out.ResourceTagMappingList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load tagged resources: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		void loadResources();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Resource Groups Tagging API</h1>
	</div>
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		{#if resources.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No tagged resources found</p>
		{:else}
			<div class="space-y-2">{#each resources as resource}<div class="rounded-lg border border-slate-200 p-3 dark:border-slate-700"><div class="font-medium text-slate-900 dark:text-white">{resource.ResourceARN}</div><div class="mt-1 text-xs text-slate-500 dark:text-slate-400">{#each resource.Tags ?? [] as tag}{tag.Key}={tag.Value} {/each}</div></div>{/each}</div>
		{/if}
	</div>
</div>
