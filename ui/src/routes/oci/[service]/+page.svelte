<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { ociServiceById } from '#lib/cloud.ts';
	import APIGatewayDashboard from '#lib/components/oci/APIGatewayDashboard.svelte';
	import ArtifactsDashboard from '#lib/components/oci/ArtifactsDashboard.svelte';
	import ContainerInstancesDashboard from '#lib/components/oci/ContainerInstancesDashboard.svelte';
	import ContainerEngineDashboard from '#lib/components/oci/ContainerEngineDashboard.svelte';
	import CoreDashboard from '#lib/components/oci/CoreDashboard.svelte';
	import EventsDashboard from '#lib/components/oci/EventsDashboard.svelte';
	import FunctionsDashboard from '#lib/components/oci/FunctionsDashboard.svelte';
	import IdentityDashboard from '#lib/components/oci/IdentityDashboard.svelte';
	import LoadBalancerDashboard from '#lib/components/oci/LoadBalancerDashboard.svelte';
	import LoggingDashboard from '#lib/components/oci/LoggingDashboard.svelte';
	import MonitoringDashboard from '#lib/components/oci/MonitoringDashboard.svelte';
	import MySQLDashboard from '#lib/components/oci/MySQLDashboard.svelte';
	import NoSQLDashboard from '#lib/components/oci/NoSQLDashboard.svelte';
	import NotificationsDashboard from '#lib/components/oci/NotificationsDashboard.svelte';
	import ObjectStorageDashboard from '#lib/components/oci/ObjectStorageDashboard.svelte';
	import QueueDashboard from '#lib/components/oci/QueueDashboard.svelte';
	import StreamingDashboard from '#lib/components/oci/StreamingDashboard.svelte';
	import VaultDashboard from '#lib/components/oci/VaultDashboard.svelte';
	import { backendInfo, loadBackendInfo, servedOciServices } from '#lib/cloud.svelte.ts';

	const service = $derived(ociServiceById(page.params.service ?? ''));
	const served = $derived(service ? servedOciServices().some((s) => s.id === service.id) : false);
	const info = $derived(backendInfo());
	const port = $derived(service ? info.ports[service.portLabel] : undefined);

	onMount(() => {
		void loadBackendInfo();
	});
</script>

{#if service?.id === 'objectstorage'}
	<ObjectStorageDashboard />
{:else if service?.id === 'vault'}
	<VaultDashboard />
{:else if service?.id === 'streaming'}
	<StreamingDashboard />
{:else if service?.id === 'identity'}
	<IdentityDashboard />
{:else if service?.id === 'functions'}
	<FunctionsDashboard />
{:else if service?.id === 'core'}
	<CoreDashboard />
{:else if service?.id === 'queue'}
	<QueueDashboard />
{:else if service?.id === 'notifications'}
	<NotificationsDashboard />
{:else if service?.id === 'containerengine'}
	<ContainerEngineDashboard />
{:else if service?.id === 'mysql'}
	<MySQLDashboard />
{:else if service?.id === 'logging'}
	<LoggingDashboard />
{:else if service?.id === 'monitoring'}
	<MonitoringDashboard />
{:else if service?.id === 'nosql'}
	<NoSQLDashboard />
{:else if service?.id === 'apigateway'}
	<APIGatewayDashboard />
{:else if service?.id === 'events'}
	<EventsDashboard />
{:else if service?.id === 'loadbalancer'}
	<LoadBalancerDashboard />
{:else if service?.id === 'artifacts'}
	<ArtifactsDashboard />
{:else if service?.id === 'containerinstances'}
	<ContainerInstancesDashboard />
{:else}
<div class="space-y-4 pb-20" data-testid="oci-service-placeholder">
	{#if !service}
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Unknown Oracle Cloud service</h1>
		<a href="/dashboard?cloud=oci" class="text-indigo-600 dark:text-indigo-400 underline">Back to Oracle Cloud</a>
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
		<a href="/dashboard?cloud=oci" class="inline-block text-indigo-600 dark:text-indigo-400 underline">Back to Oracle Cloud</a>
	{/if}
</div>
{/if}
