<script lang="ts">
	// Devices within a Global Network (services/networkmanager/PARITY.md
	// family C, 4 ops).
	import {
		GetDevicesCommand,
		CreateDeviceCommand,
		UpdateDeviceCommand,
		DeleteDeviceCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Device,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import GlobalNetworkSelect from './GlobalNetworkSelect.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let globalNetworkId = $state('');
	let devices = $state<Device[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchDevices(reset: boolean): Promise<void> {
		if (!globalNetworkId) {
			devices = [];
			return;
		}
		const resp = await client().send(
			new GetDevicesCommand({
				GlobalNetworkId: globalNetworkId,
				MaxResults: 50,
				NextToken: reset ? undefined : nextToken
			})
		);
		devices = reset ? (resp.Devices ?? []) : [...devices, ...(resp.Devices ?? [])];
		nextToken = resp.NextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchDevices(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchDevices(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	$effect(() => {
		void refresh();
	});

	const filtered = $derived(
		devices.filter((d) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(d.DeviceId ?? '').toLowerCase().includes(q) ||
				(d.Description ?? '').toLowerCase().includes(q) ||
				(d.Vendor ?? '').toLowerCase().includes(q) ||
				(d.Model ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createDescription = $state('');
	let createSiteId = $state('');
	let createType = $state('');
	let createVendor = $state('');
	let createModel = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createDescription = '';
		createSiteId = '';
		createType = '';
		createVendor = '';
		createModel = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!globalNetworkId) return;
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateDeviceCommand({
					GlobalNetworkId: globalNetworkId,
					Description: createDescription || undefined,
					SiteId: createSiteId || undefined,
					Type: createType || undefined,
					Vendor: createVendor || undefined,
					Model: createModel || undefined
				})
			);
			toast.success('Device created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteDevice(d: Device): Promise<void> {
		if (!d.DeviceId || !globalNetworkId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete device',
			message: `Delete device ${d.DeviceId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDeviceCommand({ GlobalNetworkId: globalNetworkId, DeviceId: d.DeviceId }));
			toast.success('Device deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Device | null>(null);
	let editDescription = $state('');
	let editSiteId = $state('');
	let detailBusy = $state(false);
	let detailError = $state<string | null>(null);

	function openDetail(d: Device): void {
		viewed = d;
		editDescription = d.Description ?? '';
		editSiteId = d.SiteId ?? '';
		detailError = null;
		detailModal?.open();
	}

	async function saveDevice(): Promise<void> {
		if (!viewed?.DeviceId || !globalNetworkId) return;
		detailBusy = true;
		detailError = null;
		try {
			const resp = await client().send(
				new UpdateDeviceCommand({
					GlobalNetworkId: globalNetworkId,
					DeviceId: viewed.DeviceId,
					Description: editDescription || undefined,
					SiteId: editSiteId || undefined
				})
			);
			viewed = resp.Device ?? viewed;
			toast.success('Device updated');
			await refresh();
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailBusy = false;
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.DeviceId) return;
		const arn = taggableArn('device', viewed.DeviceId, globalNetworkId);
		await client().send(new TagResourceCommand({ ResourceArn: arn, Tags: [{ Key: key, Value: value }] }));
		viewed = { ...viewed, Tags: [...(viewed.Tags ?? []).filter((t) => t.Key !== key), { Key: key, Value: value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.DeviceId) return;
		const arn = taggableArn('device', viewed.DeviceId, globalNetworkId);
		await client().send(new UntagResourceCommand({ ResourceArn: arn, TagKeys: [key] }));
		viewed = { ...viewed, Tags: (viewed.Tags ?? []).filter((t) => t.Key !== key) };
	}
</script>

<div class="flex flex-wrap items-end justify-between gap-3">
	<GlobalNetworkSelect {client} bind:value={globalNetworkId} id="nm-devices-gn" />
	<button
		onclick={openCreate}
		disabled={!globalNetworkId}
		class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
	>
		Create device
	</button>
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet createdCell(d: Device)}
	{formatDate(d.CreatedAt)}
{/snippet}
{#snippet rowActions(d: Device)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(d)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteDevice(d)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(d) => d.DeviceId ?? ''}
	columns={defineColumns<Device>([
		{ key: 'DeviceId', label: 'ID' },
		{ key: 'Description', label: 'Description' },
		{ key: 'Vendor', label: 'Vendor' },
		{ key: 'Model', label: 'Model' },
		{ key: 'SiteId', label: 'Site' },
		{ key: 'State', label: 'State' },
		{ key: 'CreatedAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No devices found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create device">
	{#snippet children()}
		<div class="grid grid-cols-2 gap-3">
			<label class="flex flex-col gap-1 text-sm col-span-2" for="nm-dev-desc">
				Description
				<input id="nm-dev-desc" bind:value={createDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-dev-site">
				Site ID
				<input id="nm-dev-site" bind:value={createSiteId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-dev-type">
				Type
				<input id="nm-dev-type" bind:value={createType} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-dev-vendor">
				Vendor
				<input id="nm-dev-vendor" bind:value={createVendor} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-dev-model">
				Model
				<input id="nm-dev-model" bind:value={createModel} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="col-span-2 text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Device {viewed?.DeviceId ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.DeviceArn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.State ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Type</dt><dd>{viewed.Type ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Vendor / Model</dt><dd>{viewed.Vendor ?? '—'} / {viewed.Model ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Serial number</dt><dd>{viewed.SerialNumber ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.CreatedAt)}</dd></div>
				</dl>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<label class="flex flex-col gap-1 text-sm" for="nm-dev-edit-desc">
						Description
						<input id="nm-dev-edit-desc" bind:value={editDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<label class="flex flex-col gap-1 text-sm" for="nm-dev-edit-site">
						Site ID
						<input id="nm-dev-edit-site" bind:value={editSiteId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<button onclick={saveDevice} disabled={detailBusy} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Save</button>
					{#if detailError}<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>{/if}
				</div>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={viewed.Tags ?? []} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
