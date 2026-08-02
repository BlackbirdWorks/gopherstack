<script lang="ts">
	// Load balancers -- family L (9 ops), with family M's TLS certificates (5
	// ops) folded into the detail view since GetLoadBalancerTlsCertificates
	// is itself scoped to a single loadBalancerName. A simplified
	// single-listener ALB analogue (services/lightsail/models.go).
	import {
		GetLoadBalancersCommand,
		CreateLoadBalancerCommand,
		DeleteLoadBalancerCommand,
		AttachInstancesToLoadBalancerCommand,
		DetachInstancesFromLoadBalancerCommand,
		UpdateLoadBalancerAttributeCommand,
		SetIpAddressTypeCommand,
		GetLoadBalancerMetricDataCommand,
		CreateLoadBalancerTlsCertificateCommand,
		DeleteLoadBalancerTlsCertificateCommand,
		AttachLoadBalancerTlsCertificateCommand,
		GetLoadBalancerTlsCertificatesCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type LoadBalancer,
		type LoadBalancerTlsCertificate,
		type LightsailClient
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, tagsToRecord } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let loadBalancers = $state<LoadBalancer[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchLBs(reset: boolean): Promise<void> {
		const resp = await client().send(new GetLoadBalancersCommand({ pageToken: reset ? undefined : nextToken }));
		loadBalancers = reset ? (resp.loadBalancers ?? []) : [...loadBalancers, ...(resp.loadBalancers ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchLBs(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchLBs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		loadBalancers.filter((lb) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (lb.name ?? '').toLowerCase().includes(q) || (lb.state ?? '').toLowerCase().includes(q);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createInstancePort = $state(80);
	let createHealthCheckPath = $state('/');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createInstancePort = 80;
		createHealthCheckPath = '/';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateLoadBalancerCommand({
					loadBalancerName: createName,
					instancePort: createInstancePort,
					healthCheckPath: createHealthCheckPath
				})
			);
			toast.success('Load balancer creation started');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteLB(lb: LoadBalancer): Promise<void> {
		if (!lb.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete load balancer',
			message: `Delete load balancer ${lb.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteLoadBalancerCommand({ loadBalancerName: lb.name }));
			toast.success('Load balancer deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<LoadBalancer | null>(null);
	let tlsCertificates = $state<LoadBalancerTlsCertificate[]>([]);
	let attachInstanceName = $state('');
	let newCertName = $state('');
	let newCertDomain = $state('');
	let metricsChecked = $state(false);
	let metricsEmpty = $state(true);

	async function openDetail(lb: LoadBalancer): Promise<void> {
		viewed = lb;
		tlsCertificates = [];
		attachInstanceName = '';
		newCertName = '';
		newCertDomain = '';
		metricsChecked = false;
		metricsEmpty = true;
		detailModal?.open();
		if (lb.name) {
			try {
				const resp = await client().send(new GetLoadBalancerTlsCertificatesCommand({ loadBalancerName: lb.name }));
				tlsCertificates = resp.tlsCertificates ?? [];
			} catch (e) {
				toast.error(describeError(e));
			}
		}
	}

	async function attachInstance(): Promise<void> {
		if (!viewed?.name || !attachInstanceName) return;
		try {
			await client().send(
				new AttachInstancesToLoadBalancerCommand({ loadBalancerName: viewed.name, instanceNames: [attachInstanceName] })
			);
			toast.success('Instance attached');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function detachInstance(name: string): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(
				new DetachInstancesFromLoadBalancerCommand({ loadBalancerName: viewed.name, instanceNames: [name] })
			);
			toast.success('Instance detached');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function toggleHttpsRedirection(enabled: boolean): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(
				new UpdateLoadBalancerAttributeCommand({
					loadBalancerName: viewed.name,
					attributeName: 'HttpsRedirectionEnabled',
					attributeValue: String(enabled)
				})
			);
			toast.success('Load balancer attribute updated');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function setIpType(ipType: 'ipv4' | 'dualstack'): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(
				new SetIpAddressTypeCommand({ resourceType: 'LoadBalancer', resourceName: viewed.name, ipAddressType: ipType })
			);
			toast.success('IP address type updated');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function createCert(): Promise<void> {
		if (!viewed?.name || !newCertName || !newCertDomain) return;
		try {
			await client().send(
				new CreateLoadBalancerTlsCertificateCommand({
					loadBalancerName: viewed.name,
					certificateName: newCertName,
					certificateDomainName: newCertDomain
				})
			);
			toast.success('TLS certificate requested');
			const resp = await client().send(new GetLoadBalancerTlsCertificatesCommand({ loadBalancerName: viewed.name }));
			tlsCertificates = resp.tlsCertificates ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function attachCert(name: string | undefined): Promise<void> {
		if (!viewed?.name || !name) return;
		try {
			await client().send(new AttachLoadBalancerTlsCertificateCommand({ loadBalancerName: viewed.name, certificateName: name }));
			toast.success('TLS certificate attached');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteCert(name: string | undefined): Promise<void> {
		if (!viewed?.name || !name) return;
		try {
			await client().send(new DeleteLoadBalancerTlsCertificateCommand({ loadBalancerName: viewed.name, certificateName: name }));
			toast.success('TLS certificate deleted');
			tlsCertificates = tlsCertificates.filter((c) => c.name !== name);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// GetLoadBalancerMetricData always returns an honest empty MetricData
	// series in this emulator -- never a fabricated chart.
	async function checkMetrics(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(
				new GetLoadBalancerMetricDataCommand({
					loadBalancerName: viewed.name,
					metricName: 'ClientTLSNegotiationErrorCount',
					period: 300,
					startTime: new Date(Date.now() - 3600_000),
					endTime: new Date(),
					unit: 'Count',
					statistics: ['Sum']
				})
			);
			metricsEmpty = (resp.metricData ?? []).length === 0;
			metricsChecked = true;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.name) return;
		await client().send(new TagResourceCommand({ resourceName: viewed.name, tags: [{ key, value }] }));
		viewed = { ...viewed, tags: [...(viewed.tags ?? []).filter((t) => t.key !== key), { key, value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.name) return;
		await client().send(new UntagResourceCommand({ resourceName: viewed.name, tagKeys: [key] }));
		viewed = { ...viewed, tags: (viewed.tags ?? []).filter((t) => t.key !== key) };
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
		Create load balancer
	</button>
</div>

{#snippet createdCell(lb: LoadBalancer)}
	{formatDate(lb.createdAt)}
{/snippet}
{#snippet rowActions(lb: LoadBalancer)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(lb)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteLB(lb)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(lb) => lb.name ?? ''}
	columns={defineColumns<LoadBalancer>([
		{ key: 'name', label: 'Name' },
		{ key: 'dnsName', label: 'DNS name' },
		{ key: 'state', label: 'State' },
		{ key: 'instancePort', label: 'Instance port' },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No load balancers found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create load balancer">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-lb-name">
				Name
				<input id="ls-lb-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-lb-port">
				Instance port
				<input id="ls-lb-port" type="number" bind:value={createInstancePort} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-lb-health">
				Health check path
				<input id="ls-lb-health" bind:value={createHealthCheckPath} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Load balancer {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.state ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Protocol</dt><dd>{viewed.protocol ?? '—'}</dd></div>
					<div><dt class="text-slate-500">TLS policy</dt><dd>{viewed.tlsPolicyName ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Attached instances</p>
					<ul class="text-sm space-y-1">
						{#each viewed.instanceHealthSummary ?? [] as h (h.instanceName)}
							<li class="flex items-center justify-between">
								<span>{h.instanceName} -- {h.instanceHealth ?? 'unknown'}</span>
								<button onclick={() => detachInstance(h.instanceName ?? '')} class="text-red-600 hover:underline text-xs">Detach</button>
							</li>
						{:else}
							<li class="text-slate-500">No attached instances</li>
						{/each}
					</ul>
					<div class="flex items-center gap-2">
						<input bind:value={attachInstanceName} placeholder="Instance name" aria-label="Instance to attach" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 flex-1" />
						<button onclick={attachInstance} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Attach</button>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Attributes</p>
					<div class="flex gap-2">
						<button onclick={() => toggleHttpsRedirection(true)} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Enable HTTPS redirect</button>
						<button onclick={() => toggleHttpsRedirection(false)} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Disable HTTPS redirect</button>
						<button onclick={() => setIpType('dualstack')} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Set dualstack</button>
						<button onclick={() => setIpType('ipv4')} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Set ipv4</button>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">TLS certificates</p>
					<ul class="text-sm space-y-1">
						{#each tlsCertificates as c (c.name)}
							<li class="flex items-center justify-between">
								<span>{c.name} -- {c.domainName} ({c.status})</span>
								<span class="flex gap-2">
									<button onclick={() => attachCert(c.name)} class="text-emerald-600 hover:underline text-xs">Attach</button>
									<button onclick={() => deleteCert(c.name)} class="text-red-600 hover:underline text-xs">Delete</button>
								</span>
							</li>
						{:else}
							<li class="text-slate-500">No TLS certificates</li>
						{/each}
					</ul>
					<div class="flex items-center gap-2">
						<input bind:value={newCertName} placeholder="Certificate name" aria-label="New certificate name" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700" />
						<input bind:value={newCertDomain} placeholder="Domain name" aria-label="New certificate domain" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700" />
						<button onclick={createCert} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Request</button>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Metrics</p>
					<button onclick={checkMetrics} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Check metric</button>
					{#if metricsChecked && metricsEmpty}
						<p class="text-xs text-slate-500">
							This emulator does not produce real metric datapoints -- GetLoadBalancerMetricData returned an honest empty series.
						</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={tagsToRecord(viewed.tags)} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
