<script lang="ts">
	// Cloud WAN attachments -- the generic lifecycle shared by all 5
	// subtypes (services/networkmanager/PARITY.md family Q, 4 ops) plus each
	// subtype's own create op (families Q1-Q5, 12 ops) and attachment
	// routing policy labels (family P, 3 ops). Every subtype shares one base
	// Attachment shape, one 9-value AttachmentState, and the same
	// accept/reject/delete actions -- surfaced here as a single list with a
	// subtype selector for creation. Cross-account attachments land in
	// PENDING_ATTACHMENT_ACCEPTANCE; accept/reject resolve that.
	//
	// Each create subtype's own fields/validation/SDK call lives in its own
	// ./_components/Create*AttachmentFields.svelte (one per Q1-Q5 family),
	// so this file's submitCreate only ever dispatches to whichever one is
	// currently mounted rather than branching on kind itself. The detail
	// view (metadata, routing policy label, tags) lives in its own
	// ./_components/AttachmentDetailModal.svelte.
	import {
		ListAttachmentsCommand,
		AcceptAttachmentCommand,
		RejectAttachmentCommand,
		DeleteAttachmentCommand,
		type Attachment,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import AttachmentDetailModal from './AttachmentDetailModal.svelte';
	import CreateVpcAttachmentFields from './CreateVpcAttachmentFields.svelte';
	import CreateConnectAttachmentFields from './CreateConnectAttachmentFields.svelte';
	import CreateSiteToSiteVpnAttachmentFields from './CreateSiteToSiteVpnAttachmentFields.svelte';
	import CreateDirectConnectGatewayAttachmentFields from './CreateDirectConnectGatewayAttachmentFields.svelte';
	import CreateTransitGatewayRouteTableAttachmentFields from './CreateTransitGatewayRouteTableAttachmentFields.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let attachments = $state<Attachment[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchAttachments(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListAttachmentsCommand({ MaxResults: 50, NextToken: reset ? undefined : nextToken })
		);
		attachments = reset ? (resp.Attachments ?? []) : [...attachments, ...(resp.Attachments ?? [])];
		nextToken = resp.NextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchAttachments(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchAttachments(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		attachments.filter((a) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(a.AttachmentId ?? '').toLowerCase().includes(q) ||
				(a.AttachmentType ?? '').toLowerCase().includes(q) ||
				(a.CoreNetworkId ?? '').toLowerCase().includes(q) ||
				(a.SegmentName ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	type CreateKind = 'VPC' | 'CONNECT' | 'SITE_TO_SITE_VPN' | 'DIRECT_CONNECT_GATEWAY' | 'TRANSIT_GATEWAY_ROUTE_TABLE';

	type CreateFieldsRef = {
		reset: () => void;
		submit: () => Promise<string | null>;
	};

	let createModal = $state<Modal | null>(null);
	let createKind = $state<CreateKind>('VPC');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);
	// Bound to whichever Create*AttachmentFields.svelte is currently mounted
	// for `createKind` -- see those files for the per-subtype fields,
	// validation and SDK call this used to branch on inline.
	let activeCreateFields = $state<CreateFieldsRef | null>(null);

	function openCreate(): void {
		createKind = 'VPC';
		createError = null;
		activeCreateFields?.reset();
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			const validationError = await activeCreateFields?.submit();
			if (validationError) {
				createError = validationError;
				return;
			}
			toast.success('Attachment created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// -------------------------- Lifecycle actions ---------------------------

	async function acceptAttachment(a: Attachment): Promise<void> {
		if (!a.AttachmentId) return;
		try {
			await client().send(new AcceptAttachmentCommand({ AttachmentId: a.AttachmentId }));
			toast.success('Attachment accepted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function rejectAttachment(a: Attachment): Promise<void> {
		if (!a.AttachmentId) return;
		const confirmed = await confirmDestructive(`Reject attachment ${a.AttachmentId}?`);
		if (!confirmed) return;
		try {
			await client().send(new RejectAttachmentCommand({ AttachmentId: a.AttachmentId }));
			toast.success('Attachment rejected');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteAttachment(a: Attachment): Promise<void> {
		if (!a.AttachmentId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete attachment',
			message: `Delete attachment ${a.AttachmentId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAttachmentCommand({ AttachmentId: a.AttachmentId }));
			toast.success('Attachment deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<{ open: (a: Attachment) => Promise<void> } | null>(null);

	function stateClass(state: string | undefined): string {
		if (state === 'AVAILABLE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (state === 'PENDING_ATTACHMENT_ACCEPTANCE' || state === 'PENDING_NETWORK_UPDATE' || state === 'PENDING_TAG_ACCEPTANCE')
			return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400';
		if (state === 'REJECTED' || state === 'FAILED') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
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
		Create attachment
	</button>
</div>

{#snippet stateCell(a: Attachment)}
	<span class="text-xs px-2 py-1 rounded-full {stateClass(a.State)}">{a.State ?? '—'}</span>
{/snippet}
{#snippet createdCell(a: Attachment)}
	{formatDate(a.CreatedAt)}
{/snippet}
{#snippet rowActions(a: Attachment)}
	<div class="flex items-center gap-3 justify-end flex-wrap">
		{#if a.State === 'PENDING_ATTACHMENT_ACCEPTANCE'}
			<button onclick={() => acceptAttachment(a)} class="text-emerald-600 hover:underline text-sm">Accept</button>
			<button onclick={() => rejectAttachment(a)} class="text-amber-600 hover:underline text-sm">Reject</button>
		{/if}
		<button onclick={() => detailModal?.open(a)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteAttachment(a)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(a) => a.AttachmentId ?? ''}
	columns={defineColumns<Attachment>([
		{ key: 'AttachmentId', label: 'ID' },
		{ key: 'AttachmentType', label: 'Type' },
		{ key: 'CoreNetworkId', label: 'Core network' },
		{ key: 'SegmentName', label: 'Segment' },
		{ key: 'State', label: 'State', render: stateCell },
		{ key: 'CreatedAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No attachments found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create attachment">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="nm-att-kind">
				Attachment type
				<select id="nm-att-kind" bind:value={createKind} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="VPC">VPC</option>
					<option value="CONNECT">Connect</option>
					<option value="SITE_TO_SITE_VPN">Site-to-Site VPN</option>
					<option value="DIRECT_CONNECT_GATEWAY">Direct Connect Gateway</option>
					<option value="TRANSIT_GATEWAY_ROUTE_TABLE">Transit Gateway Route Table</option>
				</select>
			</label>

			{#if createKind === 'VPC'}
				<CreateVpcAttachmentFields bind:this={activeCreateFields} {client} />
			{:else if createKind === 'CONNECT'}
				<CreateConnectAttachmentFields bind:this={activeCreateFields} {client} />
			{:else if createKind === 'SITE_TO_SITE_VPN'}
				<CreateSiteToSiteVpnAttachmentFields bind:this={activeCreateFields} {client} />
			{:else if createKind === 'DIRECT_CONNECT_GATEWAY'}
				<CreateDirectConnectGatewayAttachmentFields bind:this={activeCreateFields} {client} />
			{:else}
				<CreateTransitGatewayRouteTableAttachmentFields bind:this={activeCreateFields} {client} />
			{/if}
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<AttachmentDetailModal bind:this={detailModal} {client} />
