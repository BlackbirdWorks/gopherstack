<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { azureServiceById } from '#lib/cloud.ts';
	import ArmDashboard from '#lib/components/azure/ArmDashboard.svelte';
	import CosmosDashboard from '#lib/components/azure/CosmosDashboard.svelte';
	import BlobDashboard from '#lib/components/azure/BlobDashboard.svelte';
	import KeyVaultDashboard from '#lib/components/azure/KeyVaultDashboard.svelte';
	import QueueDashboard from '#lib/components/azure/QueueDashboard.svelte';
	import ServiceBusDashboard from '#lib/components/azure/ServiceBusDashboard.svelte';
	import TableDashboard from '#lib/components/azure/TableDashboard.svelte';
	import { backendInfo, loadBackendInfo, servedAzureServices } from '#lib/cloud.svelte.ts';

	const service = $derived(azureServiceById(page.params.service ?? ''));
	const served = $derived(service ? servedAzureServices().some((s) => s.id === service.id) : false);
	const info = $derived(backendInfo());
	const port = $derived(service ? info.ports[service.portLabel] : undefined);

	onMount(() => {
		void loadBackendInfo();
	});
</script>

{#if service?.id === 'arm'}
	<ArmDashboard />
{:else if service?.id === 'blob'}
	<BlobDashboard />
{:else if service?.id === 'queue'}
	<QueueDashboard />
{:else if service?.id === 'servicebus'}
	<ServiceBusDashboard />
{:else if service?.id === 'keyvault'}
	<KeyVaultDashboard />
{:else if service?.id === 'cosmosdb'}
	<CosmosDashboard />
{:else if service?.id === 'table'}
	<TableDashboard />
{:else}
<div class="space-y-4 pb-20" data-testid="azure-service-placeholder">
	{#if !service}
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Unknown Azure service</h1>
		<a href="/dashboard?cloud=azure" class="text-indigo-600 dark:text-indigo-400 underline">Back to Azure</a>
	{:else}
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">{service.label}</h1>
		<p class="text-slate-500 dark:text-slate-400">{service.description}. The dashboard page for this service is coming soon.</p>
		<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
			<dt class="font-semibold text-slate-700 dark:text-slate-300">Status</dt>
			<dd class="text-slate-600 dark:text-slate-400">
				{#if !info.loaded}Checking...{:else if info.services === null}Backend unreachable{:else if served}Running{:else}Not served by this backend{/if}
			</dd>
			<dt class="font-semibold text-slate-700 dark:text-slate-300">Endpoint</dt>
			<dd class="font-mono text-slate-600 dark:text-slate-400">{port ? `http://localhost:${port}` : 'not reported'}</dd>
		</dl>
		<a href="/dashboard?cloud=azure" class="inline-block text-indigo-600 dark:text-indigo-400 underline">Back to Azure</a>
	{/if}
</div>
{/if}
