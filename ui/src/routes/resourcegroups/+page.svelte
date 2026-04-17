<script lang="ts">
	import { onMount } from 'svelte';
	import { getResourceGroupsClient } from '$lib/aws-client';
	import { ListGroupsCommand } from '@aws-sdk/client-resource-groups';
	import { toast } from 'svelte-sonner';

	const resourceGroups = getResourceGroupsClient();
	let groups = $state<Array<{ Name?: string; Description?: string }>>([]);

	async function loadGroups() {
		try {
			const out = await resourceGroups.send(new ListGroupsCommand({}));
			groups = out.GroupIdentifiers ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load groups: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		void loadGroups();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Resource Groups</h1>
	</div>
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		{#if groups.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No groups found</p>
		{:else}
			<div class="space-y-2">{#each groups as group}<div class="rounded-lg border border-slate-200 p-3 dark:border-slate-700"><div class="font-medium text-slate-900 dark:text-white">{group.Name}</div><div class="text-xs text-slate-500 dark:text-slate-400">{group.Description}</div></div>{/each}</div>
		{/if}
	</div>
</div>
