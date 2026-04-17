<script lang="ts">
	import { onMount } from 'svelte';
	import { getSWFClient } from '$lib/aws-client';
	import { ListDomainsCommand } from '@aws-sdk/client-swf';
	import { toast } from 'svelte-sonner';

	const swf = getSWFClient();
	let domains = $state<Array<{ name?: string; description?: string }>>([]);

	async function loadDomains() {
		try {
			const out = await swf.send(new ListDomainsCommand({ registrationStatus: 'REGISTERED' }));
			domains = out.domainInfos ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load domains: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		void loadDomains();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Simple Workflow Service</h1>
	</div>
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		{#if domains.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No domains registered</p>
		{:else}
			<div class="space-y-2">{#each domains as domain}<div class="rounded-lg border border-slate-200 p-3 dark:border-slate-700"><div class="font-medium text-slate-900 dark:text-white">{domain.name}</div><div class="text-xs text-slate-500 dark:text-slate-400">{domain.description}</div></div>{/each}</div>
		{/if}
	</div>
</div>
