<script lang="ts">
	// Global-Networks-side associations -- five distinct families
	// (services/networkmanager/PARITY.md families E/G/H/I/J, 15 ops total)
	// that all bind a Device/Link to something else. Grouped into one tab
	// because the backend itself groups them in one file (associations.go/
	// handler_associations.go) and they share the same shape (Device/Link
	// plus one foreign key, a 4-value PENDING/AVAILABLE/DELETING/DELETED
	// state) -- but each kind's own load/create/delete logic now lives in
	// its own ./_components/*AssociationPanel.svelte, one level down, same
	// as +page.svelte's own selector-plus-composition split over its tabs.
	// This file owns only the one thing genuinely shared across all five
	// kinds: which Global Network is picked (switching kind must NOT reset
	// that pick, so GlobalNetworkSelect's state has to live here, above the
	// kind switch, rather than in each child). "Connect peer" is the genuine
	// bridge to the Cloud WAN half: it binds an already-created Cloud WAN
	// ConnectPeer (see the Connect Peers tab) to an on-prem Device/Link, with
	// an inline note (in its own panel) pointing at the Cloud WAN Connect
	// Peers tab for the other side.
	import type { NetworkManagerClient } from '@aws-sdk/client-networkmanager';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab } from '$lib/components/Tabs.svelte';
	import GlobalNetworkSelect from './GlobalNetworkSelect.svelte';
	import LinkAssociationPanel from './LinkAssociationPanel.svelte';
	import CustomerGatewayAssociationPanel from './CustomerGatewayAssociationPanel.svelte';
	import TransitGatewayRegistrationPanel from './TransitGatewayRegistrationPanel.svelte';
	import TgwConnectPeerAssociationPanel from './TgwConnectPeerAssociationPanel.svelte';
	import ConnectPeerAssociationPanel from './ConnectPeerAssociationPanel.svelte';

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

	function switchKind(id: string): void {
		kind = id as Kind;
	}

	// Bound to whichever kind's own panel is currently mounted, so the
	// shared PageHeader Refresh button (forwarded from +page.svelte via this
	// component's own exported refresh()) can reload it without this file
	// needing to branch on kind itself.
	let activeKindRef = $state<{ refresh: () => Promise<void> } | null>(null);

	export async function refresh(): Promise<void> {
		await activeKindRef?.refresh();
	}
</script>

<div class="space-y-3">
	<GlobalNetworkSelect {client} bind:value={globalNetworkId} id="nm-assoc-gn" />

	<Tabs tabs={kindTabs} active={kind} onSelect={switchKind} color="violet" />

	{#if kind === 'link'}
		<LinkAssociationPanel bind:this={activeKindRef} {client} {globalNetworkId} {searchQuery} />
	{:else if kind === 'customer-gateway'}
		<CustomerGatewayAssociationPanel bind:this={activeKindRef} {client} {globalNetworkId} {searchQuery} />
	{:else if kind === 'transit-gateway'}
		<TransitGatewayRegistrationPanel bind:this={activeKindRef} {client} {globalNetworkId} {searchQuery} />
	{:else if kind === 'tgw-connect-peer'}
		<TgwConnectPeerAssociationPanel bind:this={activeKindRef} {client} {globalNetworkId} {searchQuery} />
	{:else}
		<ConnectPeerAssociationPanel bind:this={activeKindRef} {client} {globalNetworkId} {searchQuery} />
	{/if}
</div>
