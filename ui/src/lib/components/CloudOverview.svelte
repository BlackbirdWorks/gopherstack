<script lang="ts">
	import { onMount } from 'svelte';
	import { CLOUDS, type Cloud } from '#lib/cloud.ts';
	import { backendInfo, loadBackendInfo, servedAzureServices, servedGcpServices, servedOciServices } from '#lib/cloud.svelte.ts';

	let { cloud }: { cloud: Exclude<Cloud, 'aws'> } = $props();

	const label = $derived(CLOUDS.find((c) => c.id === cloud)?.label ?? cloud);
	const services = $derived(cloud === 'gcp' ? servedGcpServices() : cloud === 'oci' ? servedOciServices() : servedAzureServices());
	const base = $derived(cloud);
	const info = $derived(backendInfo());

	onMount(() => {
		void loadBackendInfo();
	});
</script>

<div class="space-y-6 pb-20" data-testid="cloud-overview">
	<h1 class="text-3xl font-bold text-slate-900 dark:text-white">{label}</h1>
	{#if !info.loaded}
		<p class="text-slate-500 dark:text-slate-400">Loading {label} services...</p>
	{:else if info.services === null}
		<p class="text-slate-500 dark:text-slate-400">Could not reach the Gopherstack backend.</p>
	{:else if services.length === 0}
		<p class="text-slate-500 dark:text-slate-400">The backend is not serving any {label} services.</p>
	{:else}
		<div class="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
			{#each services as svc (svc.id)}
				{@const port = info.ports[svc.portLabel]}
				<a href="/dashboard/{base}/{svc.id}" class="block p-4 rounded-xl border border-slate-200 dark:border-white/10 bg-white dark:bg-slate-800 hover:border-indigo-400 transition-colors">
					<p class="font-semibold text-slate-900 dark:text-white">{svc.label}</p>
					<p class="text-sm text-slate-500 dark:text-slate-400">{svc.description}</p>
					<p class="mt-2 text-xs text-slate-500 dark:text-slate-400">
						{port ? `localhost:${port}` : 'port not reported'}
					</p>
				</a>
			{/each}
		</div>
	{/if}
</div>
