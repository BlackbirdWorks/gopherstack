<script lang="ts">
	// vCenter clients (services/mgn/PARITY.md family H, 2 ops -- the smallest
	// family in the service): DescribeVcenterClients (the only non-tagging
	// GET in this whole 95-op surface), DeleteVcenterClient. No
	// CreateVcenterClient op exists anywhere -- a VcenterClient record is
	// created only by the on-prem vCenter connector appliance registering
	// itself, so this panel is list/delete only, same honest gap as Source
	// Servers.
	import {
		DescribeVcenterClientsCommand,
		DeleteVcenterClientCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type VcenterClient,
		type MgnClient
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { currentRegion } from '$lib/region.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

	type Props = { client: () => MgnClient; searchQuery: string };
	let { client, searchQuery }: Props = $props();

	let clients = $state<VcenterClient[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchClients(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeVcenterClientsCommand({ maxResults: 50, nextToken: reset ? undefined : nextToken })
		);
		clients = reset ? (resp.items ?? []) : [...clients, ...(resp.items ?? [])];
		nextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchClients(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchClients(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		clients.filter((c) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(c.hostname ?? '').toLowerCase().includes(q) ||
				(c.vcenterClientID ?? '').toLowerCase().includes(q) ||
				(c.datacenterName ?? '').toLowerCase().includes(q)
			);
		})
	);

	async function deleteClient(c: VcenterClient): Promise<void> {
		if (!c.vcenterClientID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete vCenter client',
			message: `Delete vCenter client ${c.hostname ?? c.vcenterClientID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteVcenterClientCommand({ vcenterClientID: c.vcenterClientID }));
			toast.success('vCenter client deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<VcenterClient | null>(null);

	function openDetail(c: VcenterClient): void {
		viewed = c;
		detailModal?.open();
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.vcenterClientID) return;
		const arn = taggableArn('vcenter-client', viewed.vcenterClientID, currentRegion());
		await client().send(new TagResourceCommand({ resourceArn: arn, tags: { [key]: value } }));
		viewed = { ...viewed, tags: { ...viewed.tags, [key]: value } };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.vcenterClientID) return;
		const arn = taggableArn('vcenter-client', viewed.vcenterClientID, currentRegion());
		await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
		const rest = { ...viewed.tags };
		delete rest[key];
		viewed = { ...viewed, tags: rest };
	}

	const columns = defineColumns<VcenterClient>([
		{ key: 'hostname', label: 'Hostname' },
		{ key: 'vcenterClientID', label: 'vCenter Client ID' },
		{ key: 'datacenterName', label: 'Datacenter' }
	]);
</script>

<div class="rounded-lg border border-blue-200 dark:border-blue-900 bg-blue-50 dark:bg-blue-950/30 px-4 py-3 text-sm text-blue-800 dark:text-blue-300">
	No AWS operation creates a vCenter client either -- it is registered by the
	on-prem vCenter connector appliance, which this emulator does not simulate.
	This tab is list/delete only.
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet rowActions(c: VcenterClient)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(c)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteClient(c)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable rows={filtered} rowKey={(c) => c.vcenterClientID ?? ''} columns={[...columns, { key: 'actions', label: '', render: rowActions }]} {loading} emptyMessage="No vCenter clients found" />
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={detailModal} title="vCenter client {viewed?.hostname ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">vCenter client ID</dt><dd>{viewed.vcenterClientID ?? '—'}</dd></div>
					<div><dt class="text-slate-500">vCenter UUID</dt><dd>{viewed.vcenterUUID ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Datacenter</dt><dd>{viewed.datacenterName ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Last seen</dt><dd>{formatDate(viewed.lastSeenDatetime)}</dd></div>
				</dl>
				<TagEditor tags={viewed.tags ?? {}} onAdd={addTag} onRemove={removeTag} />
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
