<script lang="ts">
	// Global-Networks-side associations -- five distinct families
	// (services/networkmanager/PARITY.md families E/G/H/I/J, 15 ops total)
	// that all bind a Device/Link to something else. Grouped into one tab
	// with an internal kind selector because the backend itself groups them
	// in one file (associations.go/handler_associations.go) and they share
	// the same shape (Device/Link plus one foreign key, a 4-value PENDING/
	// AVAILABLE/DELETING/DELETED state). "Connect peer" is the genuine
	// bridge to the Cloud WAN half: it binds an already-created Cloud WAN
	// ConnectPeer (see the Connect Peers tab) to an on-prem Device/Link.
	import {
		GetLinkAssociationsCommand,
		AssociateLinkCommand,
		DisassociateLinkCommand,
		GetCustomerGatewayAssociationsCommand,
		AssociateCustomerGatewayCommand,
		DisassociateCustomerGatewayCommand,
		GetTransitGatewayRegistrationsCommand,
		RegisterTransitGatewayCommand,
		DeregisterTransitGatewayCommand,
		GetTransitGatewayConnectPeerAssociationsCommand,
		AssociateTransitGatewayConnectPeerCommand,
		DisassociateTransitGatewayConnectPeerCommand,
		GetConnectPeerAssociationsCommand,
		AssociateConnectPeerCommand,
		DisassociateConnectPeerCommand,
		type LinkAssociation,
		type CustomerGatewayAssociation,
		type TransitGatewayRegistration,
		type TransitGatewayConnectPeerAssociation,
		type ConnectPeerAssociation,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab } from '$lib/components/Tabs.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import GlobalNetworkSelect from './GlobalNetworkSelect.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	type Kind = 'link' | 'customer-gateway' | 'transit-gateway' | 'tgw-connect-peer' | 'connect-peer';

	const kindTabs: Tab[] = [
		{ id: 'link', label: 'Link' },
		{ id: 'customer-gateway', label: 'Customer Gateway' },
		{ id: 'transit-gateway', label: 'Transit Gateway' },
		{ id: 'tgw-connect-peer', label: 'TGW Connect Peer' },
		{ id: 'connect-peer', label: 'Connect Peer (Cloud WAN bridge)' }
	];

	let globalNetworkId = $state('');
	let kind = $state<Kind>('link');
	let loading = $state(false);
	let error = $state<string | null>(null);

	let linkAssociations = $state<LinkAssociation[]>([]);
	let customerGatewayAssociations = $state<CustomerGatewayAssociation[]>([]);
	let tgwRegistrations = $state<TransitGatewayRegistration[]>([]);
	let tgwConnectPeerAssociations = $state<TransitGatewayConnectPeerAssociation[]>([]);
	let connectPeerAssociations = $state<ConnectPeerAssociation[]>([]);

	async function load(): Promise<void> {
		if (!globalNetworkId) return;
		loading = true;
		error = null;
		try {
			if (kind === 'link') {
				const resp = await client().send(new GetLinkAssociationsCommand({ GlobalNetworkId: globalNetworkId }));
				linkAssociations = resp.LinkAssociations ?? [];
			} else if (kind === 'customer-gateway') {
				const resp = await client().send(
					new GetCustomerGatewayAssociationsCommand({ GlobalNetworkId: globalNetworkId })
				);
				customerGatewayAssociations = resp.CustomerGatewayAssociations ?? [];
			} else if (kind === 'transit-gateway') {
				const resp = await client().send(
					new GetTransitGatewayRegistrationsCommand({ GlobalNetworkId: globalNetworkId })
				);
				tgwRegistrations = resp.TransitGatewayRegistrations ?? [];
			} else if (kind === 'tgw-connect-peer') {
				const resp = await client().send(
					new GetTransitGatewayConnectPeerAssociationsCommand({ GlobalNetworkId: globalNetworkId })
				);
				tgwConnectPeerAssociations = resp.TransitGatewayConnectPeerAssociations ?? [];
			} else {
				const resp = await client().send(new GetConnectPeerAssociationsCommand({ GlobalNetworkId: globalNetworkId }));
				connectPeerAssociations = resp.ConnectPeerAssociations ?? [];
			}
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	export async function refresh(): Promise<void> {
		await load();
	}

	$effect(() => {
		void load();
	});

	function switchKind(id: string): void {
		kind = id as Kind;
	}

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let formDeviceId = $state('');
	let formLinkId = $state('');
	// CustomerGatewayArn / TransitGatewayArn / TGWConnectPeerArn / ConnectPeerId, depending on `kind`.
	let formForeignId = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function foreignFieldLabel(): string {
		switch (kind) {
			case 'customer-gateway':
				return 'Customer gateway ARN';
			case 'transit-gateway':
				return 'Transit gateway ARN';
			case 'tgw-connect-peer':
				return 'Transit gateway Connect peer ARN';
			case 'connect-peer':
				return 'Connect peer ID';
			default:
				return '';
		}
	}

	function openCreate(): void {
		formDeviceId = '';
		formLinkId = '';
		formForeignId = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!globalNetworkId) return;
		createBusy = true;
		createError = null;
		try {
			if (kind === 'link') {
				if (!formDeviceId.trim() || !formLinkId.trim()) {
					createError = 'Device ID and Link ID are required.';
					return;
				}
				await client().send(
					new AssociateLinkCommand({
						GlobalNetworkId: globalNetworkId,
						DeviceId: formDeviceId.trim(),
						LinkId: formLinkId.trim()
					})
				);
			} else if (kind === 'customer-gateway') {
				if (!formForeignId.trim() || !formDeviceId.trim()) {
					createError = 'Customer gateway ARN and Device ID are required.';
					return;
				}
				await client().send(
					new AssociateCustomerGatewayCommand({
						GlobalNetworkId: globalNetworkId,
						CustomerGatewayArn: formForeignId.trim(),
						DeviceId: formDeviceId.trim(),
						LinkId: formLinkId || undefined
					})
				);
			} else if (kind === 'transit-gateway') {
				if (!formForeignId.trim()) {
					createError = 'Transit gateway ARN is required.';
					return;
				}
				await client().send(
					new RegisterTransitGatewayCommand({
						GlobalNetworkId: globalNetworkId,
						TransitGatewayArn: formForeignId.trim()
					})
				);
			} else if (kind === 'tgw-connect-peer') {
				if (!formForeignId.trim() || !formDeviceId.trim()) {
					createError = 'Connect peer ARN and Device ID are required.';
					return;
				}
				await client().send(
					new AssociateTransitGatewayConnectPeerCommand({
						GlobalNetworkId: globalNetworkId,
						TransitGatewayConnectPeerArn: formForeignId.trim(),
						DeviceId: formDeviceId.trim(),
						LinkId: formLinkId || undefined
					})
				);
			} else {
				if (!formForeignId.trim() || !formDeviceId.trim()) {
					createError = 'Connect peer ID and Device ID are required.';
					return;
				}
				await client().send(
					new AssociateConnectPeerCommand({
						GlobalNetworkId: globalNetworkId,
						ConnectPeerId: formForeignId.trim(),
						DeviceId: formDeviceId.trim(),
						LinkId: formLinkId || undefined
					})
				);
			}
			toast.success('Association created');
			createModal?.close();
			await load();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function disassociateLink(a: LinkAssociation): Promise<void> {
		if (!a.DeviceId || !a.LinkId) return;
		const confirmed = await confirmDestructive(`Disassociate link ${a.LinkId} from device ${a.DeviceId}?`);
		if (!confirmed) return;
		try {
			await client().send(
				new DisassociateLinkCommand({ GlobalNetworkId: globalNetworkId, DeviceId: a.DeviceId, LinkId: a.LinkId })
			);
			toast.success('Disassociated');
			await load();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function disassociateCustomerGateway(a: CustomerGatewayAssociation): Promise<void> {
		if (!a.CustomerGatewayArn) return;
		const confirmed = await confirmDestructive(`Disassociate customer gateway ${a.CustomerGatewayArn}?`);
		if (!confirmed) return;
		try {
			await client().send(
				new DisassociateCustomerGatewayCommand({
					GlobalNetworkId: globalNetworkId,
					CustomerGatewayArn: a.CustomerGatewayArn
				})
			);
			toast.success('Disassociated');
			await load();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deregisterTgw(r: TransitGatewayRegistration): Promise<void> {
		if (!r.TransitGatewayArn) return;
		const confirmed = await confirmDestructive(`Deregister transit gateway ${r.TransitGatewayArn}?`);
		if (!confirmed) return;
		try {
			await client().send(
				new DeregisterTransitGatewayCommand({ GlobalNetworkId: globalNetworkId, TransitGatewayArn: r.TransitGatewayArn })
			);
			toast.success('Deregistered');
			await load();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function disassociateTgwConnectPeer(a: TransitGatewayConnectPeerAssociation): Promise<void> {
		if (!a.TransitGatewayConnectPeerArn) return;
		const confirmed = await confirmDestructive(`Disassociate transit gateway Connect peer ${a.TransitGatewayConnectPeerArn}?`);
		if (!confirmed) return;
		try {
			await client().send(
				new DisassociateTransitGatewayConnectPeerCommand({
					GlobalNetworkId: globalNetworkId,
					TransitGatewayConnectPeerArn: a.TransitGatewayConnectPeerArn
				})
			);
			toast.success('Disassociated');
			await load();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function disassociateConnectPeer(a: ConnectPeerAssociation): Promise<void> {
		if (!a.ConnectPeerId) return;
		const confirmed = await confirmDestructive(`Disassociate Connect peer ${a.ConnectPeerId}?`);
		if (!confirmed) return;
		try {
			await client().send(
				new DisassociateConnectPeerCommand({ GlobalNetworkId: globalNetworkId, ConnectPeerId: a.ConnectPeerId })
			);
			toast.success('Disassociated');
			await load();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	function matchesSearch(...fields: (string | undefined)[]): boolean {
		const q = searchQuery.toLowerCase();
		if (!q) return true;
		return fields.some((f) => (f ?? '').toLowerCase().includes(q));
	}
</script>

<div class="space-y-3">
	<div class="flex flex-wrap items-end justify-between gap-3">
		<GlobalNetworkSelect {client} bind:value={globalNetworkId} id="nm-assoc-gn" />
		<button
			onclick={openCreate}
			disabled={!globalNetworkId}
			class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
		>
			Create association
		</button>
	</div>

	<Tabs tabs={kindTabs} active={kind} onSelect={switchKind} color="violet" />

	{#if error}
		<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
			<p class="font-medium">Failed to load data</p>
			<p>{error}</p>
		</div>
	{:else if loading}
		<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
	{:else if kind === 'link'}
		{@const rows = linkAssociations.filter((a) => matchesSearch(a.DeviceId, a.LinkId))}
		{#if rows.length === 0}
			<div class="text-center py-8 text-gray-500 dark:text-gray-400">No link associations found</div>
		{:else}
			<table class="w-full text-sm">
				<thead>
					<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class="px-4 py-2 font-medium">Device</th>
						<th class="px-4 py-2 font-medium">Link</th>
						<th class="px-4 py-2 font-medium">State</th>
						<th class="px-4 py-2 font-medium"></th>
					</tr>
				</thead>
				<tbody>
					{#each rows as a (a.DeviceId + '/' + a.LinkId)}
						<tr class="border-b border-gray-100 dark:border-gray-800">
							<td class="px-4 py-3">{a.DeviceId}</td>
							<td class="px-4 py-3">{a.LinkId}</td>
							<td class="px-4 py-3">{a.LinkAssociationState ?? '—'}</td>
							<td class="px-4 py-3 text-right">
								<button onclick={() => disassociateLink(a)} class="text-red-600 hover:underline text-sm">Disassociate</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{:else if kind === 'customer-gateway'}
		{@const rows = customerGatewayAssociations.filter((a) => matchesSearch(a.CustomerGatewayArn, a.DeviceId))}
		{#if rows.length === 0}
			<div class="text-center py-8 text-gray-500 dark:text-gray-400">No customer gateway associations found</div>
		{:else}
			<table class="w-full text-sm">
				<thead>
					<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class="px-4 py-2 font-medium">Customer gateway ARN</th>
						<th class="px-4 py-2 font-medium">Device</th>
						<th class="px-4 py-2 font-medium">Link</th>
						<th class="px-4 py-2 font-medium">State</th>
						<th class="px-4 py-2 font-medium"></th>
					</tr>
				</thead>
				<tbody>
					{#each rows as a (a.CustomerGatewayArn)}
						<tr class="border-b border-gray-100 dark:border-gray-800">
							<td class="px-4 py-3 break-all">{a.CustomerGatewayArn}</td>
							<td class="px-4 py-3">{a.DeviceId}</td>
							<td class="px-4 py-3">{a.LinkId ?? '—'}</td>
							<td class="px-4 py-3">{a.State ?? '—'}</td>
							<td class="px-4 py-3 text-right">
								<button onclick={() => disassociateCustomerGateway(a)} class="text-red-600 hover:underline text-sm">Disassociate</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{:else if kind === 'transit-gateway'}
		{@const rows = tgwRegistrations.filter((r) => matchesSearch(r.TransitGatewayArn))}
		{#if rows.length === 0}
			<div class="text-center py-8 text-gray-500 dark:text-gray-400">No transit gateway registrations found</div>
		{:else}
			<table class="w-full text-sm">
				<thead>
					<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class="px-4 py-2 font-medium">Transit gateway ARN</th>
						<th class="px-4 py-2 font-medium">State</th>
						<th class="px-4 py-2 font-medium"></th>
					</tr>
				</thead>
				<tbody>
					{#each rows as r (r.TransitGatewayArn)}
						<tr class="border-b border-gray-100 dark:border-gray-800">
							<td class="px-4 py-3 break-all">{r.TransitGatewayArn}</td>
							<td class="px-4 py-3">{r.State?.Code ?? '—'}</td>
							<td class="px-4 py-3 text-right">
								<button onclick={() => deregisterTgw(r)} class="text-red-600 hover:underline text-sm">Deregister</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{:else if kind === 'tgw-connect-peer'}
		{@const rows = tgwConnectPeerAssociations.filter((a) => matchesSearch(a.TransitGatewayConnectPeerArn, a.DeviceId))}
		{#if rows.length === 0}
			<div class="text-center py-8 text-gray-500 dark:text-gray-400">No transit gateway Connect peer associations found</div>
		{:else}
			<table class="w-full text-sm">
				<thead>
					<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class="px-4 py-2 font-medium">Connect peer ARN</th>
						<th class="px-4 py-2 font-medium">Device</th>
						<th class="px-4 py-2 font-medium">State</th>
						<th class="px-4 py-2 font-medium"></th>
					</tr>
				</thead>
				<tbody>
					{#each rows as a (a.TransitGatewayConnectPeerArn)}
						<tr class="border-b border-gray-100 dark:border-gray-800">
							<td class="px-4 py-3 break-all">{a.TransitGatewayConnectPeerArn}</td>
							<td class="px-4 py-3">{a.DeviceId}</td>
							<td class="px-4 py-3">{a.State ?? '—'}</td>
							<td class="px-4 py-3 text-right">
								<button onclick={() => disassociateTgwConnectPeer(a)} class="text-red-600 hover:underline text-sm">Disassociate</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{:else}
		{@const rows = connectPeerAssociations.filter((a) => matchesSearch(a.ConnectPeerId, a.DeviceId))}
		<p class="text-xs text-slate-500 dark:text-slate-400">
			Binds an existing Cloud WAN Connect peer (see the Connect Peers tab) to an on-prem device/link -- the
			real structural bridge between Global Networks and Cloud WAN.
		</p>
		{#if rows.length === 0}
			<div class="text-center py-8 text-gray-500 dark:text-gray-400">No Connect peer associations found</div>
		{:else}
			<table class="w-full text-sm">
				<thead>
					<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class="px-4 py-2 font-medium">Connect peer ID</th>
						<th class="px-4 py-2 font-medium">Device</th>
						<th class="px-4 py-2 font-medium">Link</th>
						<th class="px-4 py-2 font-medium">State</th>
						<th class="px-4 py-2 font-medium"></th>
					</tr>
				</thead>
				<tbody>
					{#each rows as a (a.ConnectPeerId)}
						<tr class="border-b border-gray-100 dark:border-gray-800">
							<td class="px-4 py-3">{a.ConnectPeerId}</td>
							<td class="px-4 py-3">{a.DeviceId}</td>
							<td class="px-4 py-3">{a.LinkId ?? '—'}</td>
							<td class="px-4 py-3">{a.State ?? '—'}</td>
							<td class="px-4 py-3 text-right">
								<button onclick={() => disassociateConnectPeer(a)} class="text-red-600 hover:underline text-sm">Disassociate</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{/if}
</div>

<Modal bind:this={createModal} title="Create {kindTabs.find((t) => t.id === kind)?.label} association">
	{#snippet children()}
		<div class="space-y-3">
			{#if kind !== 'transit-gateway'}
				<label class="flex flex-col gap-1 text-sm" for="nm-assoc-device">
					Device ID *
					<input id="nm-assoc-device" bind:value={formDeviceId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm" for="nm-assoc-link">
					Link ID {kind === 'link' ? '*' : '(optional)'}
					<input id="nm-assoc-link" bind:value={formLinkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
			{/if}
			{#if kind !== 'link'}
				<label class="flex flex-col gap-1 text-sm" for="nm-assoc-foreign">
					{foreignFieldLabel()} *
					<input id="nm-assoc-foreign" bind:value={formForeignId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
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
