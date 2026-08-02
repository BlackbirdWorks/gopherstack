<script lang="ts">
	// Container services -- family Q (5 ops), with family R's 7 deployment/
	// image ops folded into the detail view. This is state-machine
	// bookkeeping ONLY: services/lightsail/PARITY.md is explicit that no
	// image is ever pulled or run here. Deployments progress through the
	// real documented ContainerServiceState/ContainerServiceDeploymentState
	// values and image-version tagging is real and monotonic, but nothing
	// executes -- this panel says so rather than implying a real container
	// is running.
	//
	// Note: ContainerService's own name field is `containerServiceName`, not
	// `name` like every other Lightsail resource kind -- a confirmed wire
	// shape asymmetry (verified against @aws-sdk/client-lightsail's
	// installed TypeScript types).
	import {
		GetContainerServicesCommand,
		CreateContainerServiceCommand,
		UpdateContainerServiceCommand,
		DeleteContainerServiceCommand,
		CreateContainerServiceDeploymentCommand,
		GetContainerServiceDeploymentsCommand,
		CreateContainerServiceRegistryLoginCommand,
		RegisterContainerImageCommand,
		GetContainerImagesCommand,
		DeleteContainerImageCommand,
		GetContainerLogCommand,
		GetContainerServiceMetricDataCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type ContainerService,
		type ContainerServiceDeployment,
		type ContainerImage,
		type ContainerServicePowerName,
		type LightsailClient
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, tagsToRecord } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let services = $state<ContainerService[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			const resp = await client().send(new GetContainerServicesCommand({}));
			services = resp.containerServices ?? [];
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		services.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (s.containerServiceName ?? '').toLowerCase().includes(q) || (s.state ?? '').toLowerCase().includes(q);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createPower = $state<ContainerServicePowerName>('nano');
	let createScale = $state(1);
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createPower = 'nano';
		createScale = 1;
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(new CreateContainerServiceCommand({ serviceName: createName, power: createPower, scale: createScale }));
			toast.success('Container service creation started');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteService(s: ContainerService): Promise<void> {
		if (!s.containerServiceName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete container service',
			message: `Delete container service ${s.containerServiceName}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteContainerServiceCommand({ serviceName: s.containerServiceName }));
			toast.success('Container service deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function toggleDisabled(s: ContainerService, disabled: boolean): Promise<void> {
		if (!s.containerServiceName) return;
		try {
			await client().send(new UpdateContainerServiceCommand({ serviceName: s.containerServiceName, isDisabled: disabled }));
			toast.success(disabled ? 'Service disabled' : 'Service enabled');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<ContainerService | null>(null);
	let deployments = $state<ContainerServiceDeployment[]>([]);
	let images = $state<ContainerImage[]>([]);
	let deployImage = $state('');
	let deployContainerName = $state('web');
	let registryLogin = $state<{ username?: string; password?: string; registry?: string } | null>(null);
	let logLines = $state<string[]>([]);
	let metricsChecked = $state(false);
	let metricsEmpty = $state(true);

	async function openDetail(s: ContainerService): Promise<void> {
		viewed = s;
		deployments = [];
		images = [];
		deployImage = '';
		registryLogin = null;
		logLines = [];
		metricsChecked = false;
		metricsEmpty = true;
		detailModal?.open();
		if (!s.containerServiceName) return;
		try {
			const [dep, img] = await Promise.all([
				client().send(new GetContainerServiceDeploymentsCommand({ serviceName: s.containerServiceName })),
				client().send(new GetContainerImagesCommand({ serviceName: s.containerServiceName }))
			]);
			deployments = dep.deployments ?? [];
			images = img.containerImages ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function submitDeployment(): Promise<void> {
		if (!viewed?.containerServiceName || !deployImage || !deployContainerName) return;
		try {
			await client().send(
				new CreateContainerServiceDeploymentCommand({
					serviceName: viewed.containerServiceName,
					containers: { [deployContainerName]: { image: deployImage, ports: { '80': 'HTTP' } } },
					publicEndpoint: { containerName: deployContainerName, containerPort: 80 }
				})
			);
			toast.success('Deployment started -- bookkeeping only, no image is actually pulled or run');
			const dep = await client().send(new GetContainerServiceDeploymentsCommand({ serviceName: viewed.containerServiceName }));
			deployments = dep.deployments ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function fetchRegistryLogin(): Promise<void> {
		try {
			const resp = await client().send(new CreateContainerServiceRegistryLoginCommand({}));
			registryLogin = {
				username: resp.registryLogin?.username,
				password: resp.registryLogin?.password,
				registry: resp.registryLogin?.registry
			};
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function registerImage(): Promise<void> {
		if (!viewed?.containerServiceName || !deployImage) return;
		try {
			await client().send(
				new RegisterContainerImageCommand({ serviceName: viewed.containerServiceName, label: 'web', digest: deployImage })
			);
			toast.success('Image registered');
			const img = await client().send(new GetContainerImagesCommand({ serviceName: viewed.containerServiceName }));
			images = img.containerImages ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteImage(image: string | undefined): Promise<void> {
		if (!viewed?.containerServiceName || !image) return;
		try {
			await client().send(new DeleteContainerImageCommand({ serviceName: viewed.containerServiceName, image }));
			toast.success('Image deleted');
			images = images.filter((i) => i.image !== image);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function fetchLogs(): Promise<void> {
		if (!viewed?.containerServiceName || !deployContainerName) return;
		try {
			const resp = await client().send(
				new GetContainerLogCommand({ serviceName: viewed.containerServiceName, containerName: deployContainerName })
			);
			logLines = (resp.logEvents ?? []).map((l) => `${formatDate(l.createdAt)} -- ${l.message ?? ''}`);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// GetContainerServiceMetricData always returns an honest empty
	// MetricData series in this emulator -- never a fabricated chart.
	async function checkMetrics(): Promise<void> {
		if (!viewed?.containerServiceName) return;
		try {
			const resp = await client().send(
				new GetContainerServiceMetricDataCommand({
					serviceName: viewed.containerServiceName,
					metricName: 'CPUUtilization',
					period: 300,
					startTime: new Date(Date.now() - 3600_000),
					endTime: new Date(),
					statistics: ['Average']
				})
			);
			metricsEmpty = (resp.metricData ?? []).length === 0;
			metricsChecked = true;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.containerServiceName) return;
		await client().send(new TagResourceCommand({ resourceName: viewed.containerServiceName, tags: [{ key, value }] }));
		viewed = { ...viewed, tags: [...(viewed.tags ?? []).filter((t) => t.key !== key), { key, value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.containerServiceName) return;
		await client().send(new UntagResourceCommand({ resourceName: viewed.containerServiceName, tagKeys: [key] }));
		viewed = { ...viewed, tags: (viewed.tags ?? []).filter((t) => t.key !== key) };
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<p class="text-xs text-slate-500 dark:text-slate-400">
	Container services here are state-machine bookkeeping only -- no image is ever pulled or run. Deployment
	states and image-version tagging progress through real, documented values, but nothing executes.
</p>

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
		Create container service
	</button>
</div>

{#snippet createdCell(s: ContainerService)}
	{formatDate(s.createdAt)}
{/snippet}
{#snippet rowActions(s: ContainerService)}
	<div class="flex items-center gap-3 justify-end">
		{#if s.isDisabled}
			<button onclick={() => toggleDisabled(s, false)} class="text-emerald-600 hover:underline text-sm">Enable</button>
		{:else}
			<button onclick={() => toggleDisabled(s, true)} class="text-amber-600 hover:underline text-sm">Disable</button>
		{/if}
		<button onclick={() => openDetail(s)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteService(s)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(s) => s.containerServiceName ?? ''}
	columns={defineColumns<ContainerService>([
		{ key: 'containerServiceName', label: 'Name' },
		{ key: 'power', label: 'Power' },
		{ key: 'scale', label: 'Scale' },
		{ key: 'state', label: 'State' },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No container services found"
/>

<Modal bind:this={createModal} title="Create container service">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-cs-name">
				Name
				<input id="ls-cs-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-cs-power">
				Power
				<select id="ls-cs-power" bind:value={createPower} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700">
					<option value="nano">nano</option>
					<option value="micro">micro</option>
					<option value="small">small</option>
					<option value="medium">medium</option>
					<option value="large">large</option>
				</select>
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-cs-scale">
				Scale (node count)
				<input id="ls-cs-scale" type="number" bind:value={createScale} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Container service {viewed?.containerServiceName ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.state ?? '—'} {viewed.stateDetail?.code ? `(${viewed.stateDetail.code})` : ''}</dd></div>
					<div><dt class="text-slate-500">URL</dt><dd class="break-all">{viewed.url ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Deployments</p>
					<ul class="text-sm space-y-1">
						{#each deployments as d (d.version)}
							<li>v{d.version} -- {d.state}</li>
						{:else}
							<li class="text-slate-500">No deployments</li>
						{/each}
					</ul>
					<div class="flex items-center gap-2">
						<input bind:value={deployContainerName} placeholder="Container name" aria-label="Container name" class="w-24 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700" />
						<input bind:value={deployImage} placeholder="Image (e.g. :service.web.1)" aria-label="Container image" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 flex-1" />
						<button onclick={submitDeployment} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Deploy</button>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Images</p>
					<button onclick={registerImage} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Register image above as a version</button>
					<ul class="text-sm space-y-1">
						{#each images as img (img.image)}
							<li class="flex items-center justify-between">
								<span>{img.image}</span>
								<button onclick={() => deleteImage(img.image)} class="text-red-600 hover:underline text-xs">Delete</button>
							</li>
						{:else}
							<li class="text-slate-500">No registered images</li>
						{/each}
					</ul>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Registry login &amp; logs</p>
					<div class="flex gap-2">
						<button onclick={fetchRegistryLogin} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Get registry login</button>
						<button onclick={fetchLogs} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Fetch container logs</button>
					</div>
					{#if registryLogin}
						<p class="text-xs break-all">{registryLogin.registry} -- {registryLogin.username} / {registryLogin.password}</p>
					{/if}
					<ul class="text-xs space-y-1 max-h-24 overflow-y-auto">
						{#each logLines as line, i (i)}
							<li>{line}</li>
						{:else}
							<li class="text-slate-500">No log lines loaded</li>
						{/each}
					</ul>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Metrics</p>
					<button onclick={checkMetrics} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Check CPU utilization</button>
					{#if metricsChecked && metricsEmpty}
						<p class="text-xs text-slate-500">
							This emulator does not produce real metric datapoints -- GetContainerServiceMetricData returned an honest empty series.
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
