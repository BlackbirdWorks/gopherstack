<script lang="ts">
	// Cloud WAN attachments -- the generic lifecycle shared by all 5
	// subtypes (services/networkmanager/PARITY.md family Q, 4 ops) plus each
	// subtype's own create op (families Q1-Q5, 12 ops) and attachment
	// routing policy labels (family P, 3 ops). Every subtype shares one base
	// Attachment shape, one 9-value AttachmentState, and the same
	// accept/reject/delete actions -- surfaced here as a single list with a
	// subtype selector for creation. Cross-account attachments land in
	// PENDING_ATTACHMENT_ACCEPTANCE; accept/reject resolve that.
	import {
		ListAttachmentsCommand,
		AcceptAttachmentCommand,
		RejectAttachmentCommand,
		DeleteAttachmentCommand,
		CreateVpcAttachmentCommand,
		CreateConnectAttachmentCommand,
		CreateSiteToSiteVpnAttachmentCommand,
		CreateDirectConnectGatewayAttachmentCommand,
		CreateTransitGatewayRouteTableAttachmentCommand,
		PutAttachmentRoutingPolicyLabelCommand,
		RemoveAttachmentRoutingPolicyLabelCommand,
		ListAttachmentRoutingPolicyAssociationsCommand,
		TagResourceCommand,
		UntagResourceCommand,
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
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

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

	let createModal = $state<Modal | null>(null);
	let createKind = $state<CreateKind>('VPC');
	let createCoreNetworkId = $state('');
	let createVpcArn = $state('');
	let createSubnetArns = $state('');
	let createEdgeLocation = $state('');
	let createTransportAttachmentId = $state('');
	let createVpnConnectionArn = $state('');
	let createDcgArn = $state('');
	let createEdgeLocations = $state('');
	let createPeeringId = $state('');
	let createTgwRouteTableArn = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createKind = 'VPC';
		createCoreNetworkId = '';
		createVpcArn = '';
		createSubnetArns = '';
		createEdgeLocation = '';
		createTransportAttachmentId = '';
		createVpnConnectionArn = '';
		createDcgArn = '';
		createEdgeLocations = '';
		createPeeringId = '';
		createTgwRouteTableArn = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			if (createKind === 'VPC') {
				if (!createCoreNetworkId.trim() || !createVpcArn.trim() || !createSubnetArns.trim()) {
					createError = 'Core network ID, VPC ARN and at least one subnet ARN are required.';
					return;
				}
				await client().send(
					new CreateVpcAttachmentCommand({
						CoreNetworkId: createCoreNetworkId.trim(),
						VpcArn: createVpcArn.trim(),
						SubnetArns: createSubnetArns.split(',').map((s) => s.trim()).filter(Boolean)
					})
				);
			} else if (createKind === 'CONNECT') {
				if (!createCoreNetworkId.trim() || !createEdgeLocation.trim() || !createTransportAttachmentId.trim()) {
					createError = 'Core network ID, edge location and transport attachment ID are required.';
					return;
				}
				await client().send(
					new CreateConnectAttachmentCommand({
						CoreNetworkId: createCoreNetworkId.trim(),
						EdgeLocation: createEdgeLocation.trim(),
						TransportAttachmentId: createTransportAttachmentId.trim(),
						Options: { Protocol: 'GRE' }
					})
				);
			} else if (createKind === 'SITE_TO_SITE_VPN') {
				if (!createCoreNetworkId.trim() || !createVpnConnectionArn.trim()) {
					createError = 'Core network ID and VPN connection ARN are required.';
					return;
				}
				await client().send(
					new CreateSiteToSiteVpnAttachmentCommand({
						CoreNetworkId: createCoreNetworkId.trim(),
						VpnConnectionArn: createVpnConnectionArn.trim()
					})
				);
			} else if (createKind === 'DIRECT_CONNECT_GATEWAY') {
				if (!createCoreNetworkId.trim() || !createDcgArn.trim() || !createEdgeLocations.trim()) {
					createError = 'Core network ID, Direct Connect gateway ARN and at least one edge location are required.';
					return;
				}
				await client().send(
					new CreateDirectConnectGatewayAttachmentCommand({
						CoreNetworkId: createCoreNetworkId.trim(),
						DirectConnectGatewayArn: createDcgArn.trim(),
						EdgeLocations: createEdgeLocations.split(',').map((s) => s.trim()).filter(Boolean)
					})
				);
			} else {
				if (!createPeeringId.trim() || !createTgwRouteTableArn.trim()) {
					createError = 'Peering ID and transit gateway route table ARN are required.';
					return;
				}
				await client().send(
					new CreateTransitGatewayRouteTableAttachmentCommand({
						PeeringId: createPeeringId.trim(),
						TransitGatewayRouteTableArn: createTgwRouteTableArn.trim()
					})
				);
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

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Attachment | null>(null);
	let routingLabel = $state('');
	let routingLabelBusy = $state(false);
	let routingLabelError = $state<string | null>(null);
	let currentLabels = $state<string[]>([]);

	async function openDetail(a: Attachment): Promise<void> {
		viewed = a;
		routingLabel = '';
		routingLabelError = null;
		currentLabels = [];
		detailModal?.open();
		if (a.CoreNetworkId && a.AttachmentId) {
			try {
				const resp = await client().send(
					new ListAttachmentRoutingPolicyAssociationsCommand({
						CoreNetworkId: a.CoreNetworkId,
						AttachmentId: a.AttachmentId
					})
				);
				currentLabels = (resp.AttachmentRoutingPolicyAssociations ?? [])
					.map((s) => s.RoutingPolicyLabel)
					.filter((label): label is string => !!label);
			} catch {
				// Non-fatal: routing policy labels are a secondary feature of
				// the detail view.
			}
		}
	}

	async function putRoutingLabel(): Promise<void> {
		if (!viewed?.AttachmentId || !viewed.CoreNetworkId || !routingLabel.trim()) return;
		routingLabelBusy = true;
		routingLabelError = null;
		try {
			await client().send(
				new PutAttachmentRoutingPolicyLabelCommand({
					AttachmentId: viewed.AttachmentId,
					CoreNetworkId: viewed.CoreNetworkId,
					RoutingPolicyLabel: routingLabel.trim()
				})
			);
			toast.success('Routing policy label applied');
			currentLabels = [...currentLabels, routingLabel.trim()];
			routingLabel = '';
		} catch (e) {
			routingLabelError = describeError(e);
		} finally {
			routingLabelBusy = false;
		}
	}

	async function removeRoutingLabel(): Promise<void> {
		if (!viewed?.AttachmentId || !viewed.CoreNetworkId) return;
		try {
			await client().send(
				new RemoveAttachmentRoutingPolicyLabelCommand({
					AttachmentId: viewed.AttachmentId,
					CoreNetworkId: viewed.CoreNetworkId
				})
			);
			toast.success('Routing policy label removed');
			currentLabels = [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.AttachmentId) return;
		const arn = taggableArn('attachment', viewed.AttachmentId);
		await client().send(new TagResourceCommand({ ResourceArn: arn, Tags: [{ Key: key, Value: value }] }));
		viewed = { ...viewed, Tags: [...(viewed.Tags ?? []).filter((t) => t.Key !== key), { Key: key, Value: value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.AttachmentId) return;
		const arn = taggableArn('attachment', viewed.AttachmentId);
		await client().send(new UntagResourceCommand({ ResourceArn: arn, TagKeys: [key] }));
		viewed = { ...viewed, Tags: (viewed.Tags ?? []).filter((t) => t.Key !== key) };
	}

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
		<button onclick={() => openDetail(a)} class="text-blue-600 hover:underline text-sm">View</button>
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
				<label class="flex flex-col gap-1 text-sm" for="nm-att-cn-vpc">Core network ID *
					<input id="nm-att-cn-vpc" bind:value={createCoreNetworkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm" for="nm-att-vpc-arn">VPC ARN *
					<input id="nm-att-vpc-arn" bind:value={createVpcArn} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm" for="nm-att-subnets">Subnet ARNs (comma-separated) *
					<input id="nm-att-subnets" bind:value={createSubnetArns} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
			{:else if createKind === 'CONNECT'}
				<label class="flex flex-col gap-1 text-sm" for="nm-att-cn-connect">Core network ID *
					<input id="nm-att-cn-connect" bind:value={createCoreNetworkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm" for="nm-att-edge">Edge location *
					<input id="nm-att-edge" bind:value={createEdgeLocation} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm" for="nm-att-transport">Transport attachment ID *
					<input id="nm-att-transport" bind:value={createTransportAttachmentId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
			{:else if createKind === 'SITE_TO_SITE_VPN'}
				<label class="flex flex-col gap-1 text-sm" for="nm-att-cn-vpn">Core network ID *
					<input id="nm-att-cn-vpn" bind:value={createCoreNetworkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm" for="nm-att-vpn-arn">VPN connection ARN *
					<input id="nm-att-vpn-arn" bind:value={createVpnConnectionArn} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
			{:else if createKind === 'DIRECT_CONNECT_GATEWAY'}
				<label class="flex flex-col gap-1 text-sm" for="nm-att-cn-dcg">Core network ID *
					<input id="nm-att-cn-dcg" bind:value={createCoreNetworkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm" for="nm-att-dcg-arn">Direct Connect gateway ARN * <span class="text-xs text-amber-600 dark:text-amber-400">(unvalidated -- no services/directconnect backend yet)</span>
					<input id="nm-att-dcg-arn" bind:value={createDcgArn} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm" for="nm-att-edges">Edge locations (comma-separated) *
					<input id="nm-att-edges" bind:value={createEdgeLocations} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
			{:else}
				<label class="flex flex-col gap-1 text-sm" for="nm-att-peering">Peering ID *
					<input id="nm-att-peering" bind:value={createPeeringId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm" for="nm-att-tgw-rt">Transit gateway route table ARN *
					<input id="nm-att-tgw-rt" bind:value={createTgwRouteTableArn} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
			{/if}
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Attachment {viewed?.AttachmentId ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">Type</dt><dd>{viewed.AttachmentType ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.State ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Core network</dt><dd>{viewed.CoreNetworkId ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Segment</dt><dd>{viewed.SegmentName ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Owner account</dt><dd>{viewed.OwnerAccountId ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.CreatedAt)}</dd></div>
				</dl>
				{#if (viewed.LastModificationErrors ?? []).length > 0}
					<div class="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-2 text-xs text-red-700 dark:text-red-300">
						{#each viewed.LastModificationErrors ?? [] as e (e.RequestId ?? e.Code)}
							<p>{e.Code}: {e.Message}</p>
						{/each}
					</div>
				{/if}
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Routing policy label</p>
					{#if currentLabels.length > 0}
						<p class="text-sm text-slate-600 dark:text-slate-300">Current: {currentLabels.join(', ')}</p>
						<button onclick={removeRoutingLabel} class="text-red-600 hover:underline text-xs">Remove label</button>
					{:else}
						<div class="flex gap-2">
							<input bind:value={routingLabel} placeholder="Label" class="flex-1 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							<button onclick={putRoutingLabel} disabled={routingLabelBusy} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Apply</button>
						</div>
					{/if}
					{#if routingLabelError}<p class="text-sm text-red-600 dark:text-red-400">{routingLabelError}</p>{/if}
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
