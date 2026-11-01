<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { gcpServiceById } from '#lib/cloud.ts';
	import StorageDashboard from '#lib/components/gcp/StorageDashboard.svelte';
	import PubSubDashboard from '#lib/components/gcp/PubSubDashboard.svelte';
	import FirestoreDashboard from '#lib/components/gcp/FirestoreDashboard.svelte';
	import KmsDashboard from '#lib/components/gcp/KmsDashboard.svelte';
	import CloudTasksDashboard from '#lib/components/gcp/CloudTasksDashboard.svelte';
	import IamDashboard from '#lib/components/gcp/IamDashboard.svelte';
	import SchedulerDashboard from '#lib/components/gcp/SchedulerDashboard.svelte';
	import RunDashboard from '#lib/components/gcp/RunDashboard.svelte';
	import FunctionsDashboard from '#lib/components/gcp/FunctionsDashboard.svelte';
	import BigQueryDashboard from '#lib/components/gcp/BigQueryDashboard.svelte';
	import SqlDashboard from '#lib/components/gcp/SqlDashboard.svelte';
	import LoggingDashboard from '#lib/components/gcp/LoggingDashboard.svelte';
	import BigtableDashboard from '#lib/components/gcp/BigtableDashboard.svelte';
	import EventarcDashboard from '#lib/components/gcp/EventarcDashboard.svelte';
	import SpannerDashboard from '#lib/components/gcp/SpannerDashboard.svelte';
	import ArtifactRegistryDashboard from '#lib/components/gcp/ArtifactRegistryDashboard.svelte';
	import CloudBuildDashboard from '#lib/components/gcp/CloudBuildDashboard.svelte';
	import RedisDashboard from '#lib/components/gcp/RedisDashboard.svelte';
	import MonitoringDashboard from '#lib/components/gcp/MonitoringDashboard.svelte';
	import WorkflowsDashboard from '#lib/components/gcp/WorkflowsDashboard.svelte';
	import DnsDashboard from '#lib/components/gcp/DnsDashboard.svelte';
	import ComputeDashboard from '#lib/components/gcp/ComputeDashboard.svelte';
	import DatastoreDashboard from '#lib/components/gcp/DatastoreDashboard.svelte';
	import ResourceManagerDashboard from '#lib/components/gcp/ResourceManagerDashboard.svelte';
	import ApiGatewayDashboard from '#lib/components/gcp/ApiGatewayDashboard.svelte';
	import BatchDashboard from '#lib/components/gcp/BatchDashboard.svelte';
	import IdentityPlatformDashboard from '#lib/components/gcp/IdentityPlatformDashboard.svelte';
	import KubernetesDashboard from '#lib/components/gcp/KubernetesDashboard.svelte';
	import SecretManagerDashboard from '#lib/components/gcp/SecretManagerDashboard.svelte';
	import { backendInfo, loadBackendInfo, servedGcpServices } from '#lib/cloud.svelte.ts';

	const service = $derived(gcpServiceById(page.params.service ?? ''));
	const served = $derived(service ? servedGcpServices().some((s) => s.id === service.id) : false);
	const info = $derived(backendInfo());
	const port = $derived(service ? info.ports[service.portLabel] : undefined);

	onMount(() => {
		void loadBackendInfo();
	});
</script>

{#if service?.id === 'storage'}
	<StorageDashboard />
{:else if service?.id === 'pubsub'}
	<PubSubDashboard />
{:else if service?.id === 'secretmanager'}
	<SecretManagerDashboard />
{:else if service?.id === 'kms'}
	<KmsDashboard />
{:else if service?.id === 'cloudtasks'}
	<CloudTasksDashboard />
{:else if service?.id === 'scheduler'}
	<SchedulerDashboard />
{:else if service?.id === 'run'}
	<RunDashboard />
{:else if service?.id === 'functions'}
	<FunctionsDashboard />
{:else if service?.id === 'iam'}
	<IamDashboard />
{:else if service?.id === 'bigquery'}
	<BigQueryDashboard />
{:else if service?.id === 'firestore'}
	<FirestoreDashboard />
{:else if service?.id === 'sql'}
	<SqlDashboard />
{:else if service?.id === 'logging'}
	<LoggingDashboard />
{:else if service?.id === 'bigtable'}
	<BigtableDashboard />
{:else if service?.id === 'eventarc'}
	<EventarcDashboard />
{:else if service?.id === 'spanner'}
	<SpannerDashboard />
{:else if service?.id === 'artifactregistry'}
	<ArtifactRegistryDashboard />
{:else if service?.id === 'cloudbuild'}
	<CloudBuildDashboard />
{:else if service?.id === 'redis'}
	<RedisDashboard />
{:else if service?.id === 'monitoring'}
	<MonitoringDashboard />
{:else if service?.id === 'workflows'}
	<WorkflowsDashboard />
{:else if service?.id === 'dns'}
	<DnsDashboard />
{:else if service?.id === 'compute'}
	<ComputeDashboard />
{:else if service?.id === 'datastore'}
	<DatastoreDashboard />
{:else if service?.id === 'container'}
	<KubernetesDashboard />
{:else if service?.id === 'identitytoolkit'}
	<IdentityPlatformDashboard />
{:else if service?.id === 'apigateway'}
	<ApiGatewayDashboard />
{:else if service?.id === 'batch'}
	<BatchDashboard />
{:else if service?.id === 'resourcemanager'}
	<ResourceManagerDashboard />
{:else}
<div class="space-y-4 pb-20" data-testid="gcp-service-placeholder">
	{#if !service}
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Unknown GCP service</h1>
		<a href="/dashboard?cloud=gcp" class="text-indigo-600 dark:text-indigo-400 underline">Back to GCP</a>
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
		<a href="/dashboard?cloud=gcp" class="inline-block text-indigo-600 dark:text-indigo-400 underline">Back to GCP</a>
	{/if}
</div>
{/if}
