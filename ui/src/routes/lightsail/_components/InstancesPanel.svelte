<script lang="ts">
	// Instances -- family B (core lifecycle: create/delete/list/start/stop/
	// reboot/state), folded together with the detail-only families that only
	// ever act on an already-created instance: C (ports/access/host-keys), E
	// (metrics -- honest empty, see below -- and metadata options), F
	// (add-ons/auto-snapshots), and AA (GUI sessions). SetupInstanceHttps and
	// GetSetupHistory (also family C) are NOT surfaced here -- see the
	// route's page.test.ts / the restore report for why.
	//
	// InstanceState has NO typed SDK enum (services/lightsail/models.go's
	// Instance doc comment) -- this panel renders whatever `state.name`
	// string comes back rather than hard-coding a color map keyed to
	// EC2-style values the backend might not match exactly.
	import {
		GetInstancesCommand,
		GetInstanceCommand,
		CreateInstancesCommand,
		DeleteInstanceCommand,
		StartInstanceCommand,
		StopInstanceCommand,
		RebootInstanceCommand,
		GetInstancePortStatesCommand,
		OpenInstancePublicPortsCommand,
		CloseInstancePublicPortsCommand,
		GetInstanceAccessDetailsCommand,
		GetAutoSnapshotsCommand,
		DeleteAutoSnapshotCommand,
		EnableAddOnCommand,
		DisableAddOnCommand,
		GetInstanceMetricDataCommand,
		UpdateInstanceMetadataOptionsCommand,
		DeleteKnownHostKeysCommand,
		CreateGUISessionAccessDetailsCommand,
		StartGUISessionCommand,
		StopGUISessionCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Instance,
		type InstancePortState,
		type AutoSnapshotDetails,
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

	let instances = $state<Instance[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchInstances(reset: boolean): Promise<void> {
		const resp = await client().send(
			new GetInstancesCommand({ pageToken: reset ? undefined : nextToken })
		);
		instances = reset ? (resp.instances ?? []) : [...instances, ...(resp.instances ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchInstances(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchInstances(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		instances.filter((i) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(i.name ?? '').toLowerCase().includes(q) ||
				(i.blueprintId ?? '').toLowerCase().includes(q) ||
				(i.publicIpAddress ?? '').toLowerCase().includes(q) ||
				(i.state?.name ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createBlueprintId = $state('');
	let createBundleId = $state('');
	let createAvailabilityZone = $state('');
	let createKeyPairName = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createBlueprintId = '';
		createBundleId = '';
		createAvailabilityZone = '';
		createKeyPairName = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateInstancesCommand({
					instanceNames: [createName],
					availabilityZone: createAvailabilityZone,
					blueprintId: createBlueprintId,
					bundleId: createBundleId,
					keyPairName: createKeyPairName || undefined
				})
			);
			toast.success(`Instance ${createName} creation started`);
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteInstance(i: Instance): Promise<void> {
		if (!i.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete instance',
			message: `Delete instance ${i.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteInstanceCommand({ instanceName: i.name }));
			toast.success('Instance deletion started');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Lifecycle -------------------------------

	async function doStart(i: Instance): Promise<void> {
		if (!i.name) return;
		try {
			await client().send(new StartInstanceCommand({ instanceName: i.name }));
			toast.success('Start requested');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function doStop(i: Instance): Promise<void> {
		if (!i.name) return;
		try {
			await client().send(new StopInstanceCommand({ instanceName: i.name }));
			toast.success('Stop requested');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function doReboot(i: Instance): Promise<void> {
		if (!i.name) return;
		try {
			await client().send(new RebootInstanceCommand({ instanceName: i.name }));
			toast.success('Reboot requested');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Instance | null>(null);
	let detailError = $state<string | null>(null);
	let portStates = $state<InstancePortState[]>([]);
	let accessPassword = $state<string | null>(null);
	let accessCertKey = $state<string | null>(null);
	let autoSnapshots = $state<AutoSnapshotDetails[]>([]);
	let metricsChecked = $state(false);
	let metricsEmpty = $state(true);
	let metricsCount = $state(0);
	let guiSessionUrl = $state<string | null>(null);
	let guiSessionStatus = $state<string | null>(null);

	async function openDetail(i: Instance): Promise<void> {
		detailError = null;
		portStates = [];
		accessPassword = null;
		accessCertKey = null;
		autoSnapshots = [];
		metricsChecked = false;
		metricsEmpty = true;
		guiSessionUrl = null;
		guiSessionStatus = null;
		detailModal?.open();
		try {
			const resp = await client().send(new GetInstanceCommand({ instanceName: i.name }));
			viewed = resp.instance ?? i;
			await Promise.all([loadPorts(), loadAutoSnapshots()]);
		} catch (e) {
			detailError = describeError(e);
		}
	}

	async function loadPorts(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(new GetInstancePortStatesCommand({ instanceName: viewed.name }));
			portStates = resp.portStates ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function loadAutoSnapshots(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(new GetAutoSnapshotsCommand({ resourceName: viewed.name }));
			autoSnapshots = resp.autoSnapshots ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let newPortFrom = $state(80);
	let newPortTo = $state(80);
	let newPortProtocol = $state<'tcp' | 'udp' | 'all' | 'icmp' | 'icmpv6'>('tcp');

	async function openPort(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(
				new OpenInstancePublicPortsCommand({
					instanceName: viewed.name,
					portInfo: { fromPort: newPortFrom, toPort: newPortTo, protocol: newPortProtocol }
				})
			);
			toast.success('Port opened');
			await loadPorts();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function closePort(p: InstancePortState): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(
				new CloseInstancePublicPortsCommand({
					instanceName: viewed.name,
					portInfo: { fromPort: p.fromPort, toPort: p.toPort, protocol: p.protocol }
				})
			);
			toast.success('Port closed');
			await loadPorts();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function loadAccessDetails(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(new GetInstanceAccessDetailsCommand({ instanceName: viewed.name }));
			accessPassword = resp.accessDetails?.password ?? '';
			accessCertKey = resp.accessDetails?.certKey ?? '';
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function enableAutoSnapshot(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(
				new EnableAddOnCommand({
					resourceName: viewed.name,
					addOnRequest: { addOnType: 'AutoSnapshot', autoSnapshotAddOnRequest: { snapshotTimeOfDay: '06:00' } }
				})
			);
			toast.success('AutoSnapshot add-on enabled');
			const resp = await client().send(new GetInstanceCommand({ instanceName: viewed.name }));
			viewed = resp.instance ?? viewed;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function disableAutoSnapshot(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new DisableAddOnCommand({ resourceName: viewed.name, addOnType: 'AutoSnapshot' }));
			toast.success('AutoSnapshot add-on disabled');
			const resp = await client().send(new GetInstanceCommand({ instanceName: viewed.name }));
			viewed = resp.instance ?? viewed;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteOneAutoSnapshot(date: string | undefined): Promise<void> {
		if (!viewed?.name || !date) return;
		try {
			await client().send(new DeleteAutoSnapshotCommand({ resourceName: viewed.name, date }));
			toast.success('Auto snapshot deleted');
			await loadAutoSnapshots();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// GetInstanceMetricData is a real emulator call that always returns an
	// empty MetricData series (services/lightsail/PARITY.md: this emulator
	// has no real underlying telemetry signal to produce honestly) -- this
	// button proves the call round-trips, then shows an honest empty state
	// rather than a fabricated chart.
	async function checkMetrics(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(
				new GetInstanceMetricDataCommand({
					instanceName: viewed.name,
					metricName: 'CPUUtilization',
					period: 300,
					startTime: new Date(Date.now() - 3600_000),
					endTime: new Date(),
					unit: 'Percent',
					statistics: ['Average']
				})
			);
			metricsCount = (resp.metricData ?? []).length;
			metricsEmpty = metricsCount === 0;
			metricsChecked = true;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let metadataTokens = $state<'optional' | 'required'>('optional');

	async function updateMetadataOptions(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(
				new UpdateInstanceMetadataOptionsCommand({ instanceName: viewed.name, httpTokens: metadataTokens })
			);
			toast.success('Metadata options updated');
			const resp = await client().send(new GetInstanceCommand({ instanceName: viewed.name }));
			viewed = resp.instance ?? viewed;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function clearHostKeys(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new DeleteKnownHostKeysCommand({ instanceName: viewed.name }));
			toast.success('Known host keys cleared');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// GUI sessions (family AA) -- Lightsail for Research's browser-streamed
	// remote desktop. Narrow applicability (only research-capable
	// blueprints), but the three ops themselves take only a resourceName, so
	// they're surfaced here as plain action buttons rather than a full
	// dedicated tab.
	async function createGuiSession(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(new CreateGUISessionAccessDetailsCommand({ resourceName: viewed.name }));
			guiSessionStatus = resp.status ?? null;
			guiSessionUrl = resp.sessions?.[0]?.url ?? null;
			toast.success('GUI session access details requested');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function startGuiSession(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new StartGUISessionCommand({ resourceName: viewed.name }));
			toast.success('GUI session start requested');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function stopGuiSession(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new StopGUISessionCommand({ resourceName: viewed.name }));
			toast.success('GUI session stop requested');
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

	function stateClass(name: string | undefined): string {
		if (name === 'running') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (name === 'stopped' || name === 'terminated')
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
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
		Create instance
	</button>
</div>

{#snippet stateCell(i: Instance)}
	<span class="text-xs px-2 py-1 rounded-full {stateClass(i.state?.name)}">{i.state?.name ?? '—'}</span>
{/snippet}
{#snippet createdCell(i: Instance)}
	{formatDate(i.createdAt)}
{/snippet}
{#snippet rowActions(i: Instance)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => doStart(i)} class="text-green-600 hover:underline text-sm">Start</button>
		<button onclick={() => doStop(i)} class="text-amber-600 hover:underline text-sm">Stop</button>
		<button onclick={() => doReboot(i)} class="text-slate-600 hover:underline text-sm">Reboot</button>
		<button onclick={() => openDetail(i)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteInstance(i)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(i) => i.name ?? ''}
	columns={defineColumns<Instance>([
		{ key: 'name', label: 'Name' },
		{ key: 'blueprintId', label: 'Blueprint' },
		{ key: 'bundleId', label: 'Bundle' },
		{ key: 'publicIpAddress', label: 'Public IP' },
		{ key: 'state', label: 'State', render: stateCell },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No instances found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create instance">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-inst-name">
				Name
				<input id="ls-inst-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-inst-az">
				Availability zone (e.g. us-east-1a)
				<input id="ls-inst-az" bind:value={createAvailabilityZone} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-inst-blueprint">
				Blueprint ID -- see the Reference Data tab for valid values
				<input id="ls-inst-blueprint" bind:value={createBlueprintId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-inst-bundle">
				Bundle ID -- see the Reference Data tab for valid values
				<input id="ls-inst-bundle" bind:value={createBundleId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-inst-keypair">
				Key pair name (optional)
				<input id="ls-inst-keypair" bind:value={createKeyPairName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName || !createBlueprintId || !createBundleId} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Instance {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				{#if detailError}<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>{/if}
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.state?.name ?? '—'} (code {viewed.state?.code ?? '—'})</dd></div>
					<div><dt class="text-slate-500">Public IP</dt><dd>{viewed.publicIpAddress ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Private IP</dt><dd>{viewed.privateIpAddress ?? '—'}</dd></div>
					<div><dt class="text-slate-500">CPU / RAM</dt><dd>{viewed.hardware?.cpuCount ?? '—'} vCPU / {viewed.hardware?.ramSizeInGb ?? '—'} GB</dd></div>
					<div><dt class="text-slate-500">Username</dt><dd>{viewed.username ?? '—'}</dd></div>
					<div><dt class="text-slate-500">SSH key</dt><dd>{viewed.sshKeyName ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Firewall ports</p>
					<ul class="text-sm space-y-1">
						{#each portStates as p (p.fromPort + '-' + p.toPort + '-' + p.protocol)}
							<li class="flex items-center justify-between">
								<span>{p.protocol} {p.fromPort}-{p.toPort} ({p.state ?? 'open'})</span>
								<button onclick={() => closePort(p)} class="text-red-600 hover:underline text-xs">Close</button>
							</li>
						{:else}
							<li class="text-slate-500">No open ports</li>
						{/each}
					</ul>
					<div class="flex items-center gap-2">
						<input type="number" bind:value={newPortFrom} aria-label="From port" class="w-20 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700" />
						<input type="number" bind:value={newPortTo} aria-label="To port" class="w-20 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700" />
						<select bind:value={newPortProtocol} aria-label="Protocol" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700">
							<option value="tcp">tcp</option>
							<option value="udp">udp</option>
							<option value="all">all</option>
							<option value="icmp">icmp</option>
							<option value="icmpv6">icmpv6</option>
						</select>
						<button onclick={openPort} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Open port</button>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Access details</p>
					<button onclick={loadAccessDetails} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Fetch access details</button>
					{#if accessPassword !== null}
						<p class="text-xs text-slate-500 break-all">Password (RDP only, may be empty): {accessPassword || '(none)'}</p>
						<p class="text-xs text-slate-500 break-all">Cert key: {accessCertKey || '(none)'}</p>
					{/if}
					<button onclick={clearHostKeys} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Clear known host keys</button>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Add-ons &amp; auto snapshots</p>
					<div class="flex gap-2">
						<button onclick={enableAutoSnapshot} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Enable AutoSnapshot</button>
						<button onclick={disableAutoSnapshot} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Disable AutoSnapshot</button>
					</div>
					<ul class="text-sm space-y-1">
						{#each autoSnapshots as s (s.date)}
							<li class="flex items-center justify-between">
								<span>{s.date} -- {s.status}</span>
								<button onclick={() => deleteOneAutoSnapshot(s.date)} class="text-red-600 hover:underline text-xs">Delete</button>
							</li>
						{:else}
							<li class="text-slate-500">No automatic snapshots</li>
						{/each}
					</ul>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Metrics</p>
					<button onclick={checkMetrics} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Check CPU utilization</button>
					{#if metricsChecked}
						<p class="text-xs text-slate-500">
							{#if metricsEmpty}
								This emulator does not produce real metric datapoints -- GetInstanceMetricData returned an honest empty series.
							{:else}
								{metricsCount} datapoint(s) returned.
							{/if}
						</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Instance metadata options (IMDS)</p>
					<p class="text-xs text-slate-500">Current: {viewed.metadataOptions?.httpTokens ?? '—'}</p>
					<div class="flex items-center gap-2">
						<select bind:value={metadataTokens} aria-label="HTTP tokens" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700">
							<option value="optional">optional</option>
							<option value="required">required</option>
						</select>
						<button onclick={updateMetadataOptions} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Update</button>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">GUI session (Lightsail for Research)</p>
					<div class="flex gap-2">
						<button onclick={createGuiSession} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Get access details</button>
						<button onclick={startGuiSession} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Start</button>
						<button onclick={stopGuiSession} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Stop</button>
					</div>
					{#if guiSessionStatus}<p class="text-xs text-slate-500">Status: {guiSessionStatus}{guiSessionUrl ? ` -- ${guiSessionUrl}` : ''}</p>{/if}
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
